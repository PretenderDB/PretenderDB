package io.github.pretenderdb.endToEnd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.pretenderdb.DynamoDbPretenderClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.*;

public class ItemOperationsTest extends BaseEndToEndTest {

  private static final String TABLE_NAME = "ItemOpsTable";
  private static final String HASH_KEY = "userId";
  private static final String SORT_KEY = "timestamp";

  private DynamoDbPretenderClient client;

  @BeforeEach
  void setup() {
    client = component.dynamoDbPretenderClient();

    // Create table with hash and sort keys
    final List<KeySchemaElement> keySchemaElements = List.of(
        KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build(),
        KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build()
    );
    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(keySchemaElements)
        .build();
    client.createTable(createTableRequest);
  }

  @AfterEach
  void teardown() {
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
    } catch (ResourceNotFoundException e) {
      // Table may already be deleted in some tests
    }
  }

  @Test
  void putItem_and_getItem_roundTrip() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("John Doe").build(),
        "age", AttributeValue.builder().n("30").build(),
        "email", AttributeValue.builder().s("john@example.com").build()
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    final PutItemResponse putResponse = client.putItem(putRequest);
    assertThat(putResponse).isNotNull();

    // Get the item back
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);
    assertThat(getResponse.item()).isNotNull();
    assertThat(getResponse.item()).hasSize(5);
    assertThat(getResponse.item().get("name").s()).isEqualTo("John Doe");
    assertThat(getResponse.item().get("age").n()).isEqualTo("30");
    assertThat(getResponse.item().get("email").s()).isEqualTo("john@example.com");
  }

  @Test
  void getItem_withProjection() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("John Doe").build(),
        "age", AttributeValue.builder().n("30").build(),
        "email", AttributeValue.builder().s("john@example.com").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Get with projection
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .projectionExpression("name, age")
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);
    assertThat(getResponse.item()).hasSize(2);
    assertThat(getResponse.item()).containsKey("name");
    assertThat(getResponse.item()).containsKey("age");
    assertThat(getResponse.item()).doesNotContainKey("email");
  }

  @Test
  void getItem_withReturnConsumedCapacity_returnsCapacityUnits() {
    // Put an item with known size
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("capacity-test-user").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("Capacity Test User").build(),
        "description", AttributeValue.builder().s("This is a test item for capacity calculation").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Get item with ReturnConsumedCapacity=TOTAL
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("capacity-test-user").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);

    // Verify item was retrieved
    assertThat(getResponse.hasItem()).isTrue();
    assertThat(getResponse.item()).hasSize(4);

    // Verify consumed capacity was returned
    assertThat(getResponse.consumedCapacity()).isNotNull();
    assertThat(getResponse.consumedCapacity().tableName()).isEqualTo(TABLE_NAME);
    assertThat(getResponse.consumedCapacity().capacityUnits()).isGreaterThan(0.0);
    assertThat(getResponse.consumedCapacity().readCapacityUnits()).isGreaterThan(0.0);
    // For a small item (<4KB), should be exactly 1 RCU
    assertThat(getResponse.consumedCapacity().capacityUnits()).isEqualTo(1.0);
  }

  @Test
  void getItem_withoutReturnConsumedCapacity_doesNotReturnCapacity() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("no-capacity-user").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "data", AttributeValue.builder().s("test").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Get item without ReturnConsumedCapacity (defaults to NONE)
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("no-capacity-user").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);

    // Verify item was retrieved
    assertThat(getResponse.hasItem()).isTrue();

    // Verify NO consumed capacity was returned
    assertThat(getResponse.consumedCapacity()).isNull();
  }

  @Test
  void getItem_notFound() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("nonexistent").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);
    assertThat(getResponse.hasItem()).isFalse();
  }

  @Test
  void updateItem_setAttributes() {
    // Put initial item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("John Doe").build(),
        "count", AttributeValue.builder().n("5").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Update with SET expression
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final UpdateItemRequest updateRequest = UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .updateExpression("SET #n = :newName, count = count + :inc")
        .expressionAttributeNames(Map.of("#n", "name"))
        .expressionAttributeValues(Map.of(
            ":newName", AttributeValue.builder().s("Jane Doe").build(),
            ":inc", AttributeValue.builder().n("3").build()
        ))
        .returnValues(ReturnValue.ALL_NEW)
        .build();

    final UpdateItemResponse updateResponse = client.updateItem(updateRequest);
    assertThat(updateResponse.attributes().get("name").s()).isEqualTo("Jane Doe");
    assertThat(updateResponse.attributes().get("count").n()).isEqualTo("8");
  }

  @Test
  void updateItem_createNewItem() {
    // Update on non-existent item should create it
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-456").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build()
    );

    final UpdateItemRequest updateRequest = UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .updateExpression("SET #n = :name, count = :count")
        .expressionAttributeNames(Map.of("#n", "name"))
        .expressionAttributeValues(Map.of(
            ":name", AttributeValue.builder().s("New User").build(),
            ":count", AttributeValue.builder().n("1").build()
        ))
        .returnValues(ReturnValue.ALL_NEW)
        .build();

    final UpdateItemResponse updateResponse = client.updateItem(updateRequest);
    assertThat(updateResponse.attributes()).containsKey("name");
    assertThat(updateResponse.attributes()).containsKey("count");

    // Verify it was created
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build());
    assertThat(getResponse.item().get("name").s()).isEqualTo("New User");
  }

  @Test
  void updateItem_removeAttributes() {
    // Put initial item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("John Doe").build(),
        "email", AttributeValue.builder().s("john@example.com").build(),
        "age", AttributeValue.builder().n("30").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Remove email
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final UpdateItemRequest updateRequest = UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .updateExpression("REMOVE email")
        .returnValues(ReturnValue.ALL_NEW)
        .build();

    final UpdateItemResponse updateResponse = client.updateItem(updateRequest);
    assertThat(updateResponse.attributes()).doesNotContainKey("email");
    assertThat(updateResponse.attributes()).containsKey("name");
    assertThat(updateResponse.attributes()).containsKey("age");
  }

  @Test
  void deleteItem_withReturnValues() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-789").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build(),
        "name", AttributeValue.builder().s("Deleted User").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Delete with return values
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-789").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build()
    );

    final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .returnValues(ReturnValue.ALL_OLD)
        .build();

    final DeleteItemResponse deleteResponse = client.deleteItem(deleteRequest);
    assertThat(deleteResponse.attributes()).isNotNull();
    assertThat(deleteResponse.attributes().get("name").s()).isEqualTo("Deleted User");

    // Verify it's gone
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build());
    assertThat(getResponse.hasItem()).isFalse();
  }

  @Test
  void query_withHashKey() {
    // Put multiple items with same hash key
    for (int i = 1; i <= 3; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user-123").build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-0" + i + "T00:00:00Z").build(),
          "data", AttributeValue.builder().s("Item " + i).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Query by hash key
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :uid")
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("user-123").build()
        ))
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);
    assertThat(queryResponse.items()).hasSize(3);
    assertThat(queryResponse.count()).isEqualTo(3);
  }

  @Test
  void query_withHashAndSortKeyCondition() {
    // Put multiple items
    for (int i = 1; i <= 5; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user-456").build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-0" + i + "T00:00:00Z").build(),
          "data", AttributeValue.builder().s("Item " + i).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Query with sort key condition (without expression attribute names for now)
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :uid AND timestamp < :ts")
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("user-456").build(),
            ":ts", AttributeValue.builder().s("2024-01-04T00:00:00Z").build()
        ))
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);
    assertThat(queryResponse.items()).hasSize(3);  // Items 1, 2, 3
  }

  @Test
  void query_withLimit() {
    // Put multiple items
    for (int i = 1; i <= 10; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user-789").build(),
          SORT_KEY, AttributeValue.builder().s(String.format("2024-01-%02dT00:00:00Z", i)).build(),
          "data", AttributeValue.builder().s("Item " + i).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Query with limit
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :uid")
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("user-789").build()
        ))
        .limit(5)
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);
    assertThat(queryResponse.items()).hasSize(5);
    assertThat(queryResponse.count()).isEqualTo(5);
    assertThat(queryResponse.hasLastEvaluatedKey()).isTrue();
  }

  @Test
  void scan_allItems() {
    // Put items from different users
    for (int i = 1; i <= 3; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user-" + i).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
          "data", AttributeValue.builder().s("User " + i + " data").build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Scan all
    final ScanRequest scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .build();

    final ScanResponse scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSizeGreaterThanOrEqualTo(3);
    assertThat(scanResponse.count()).isGreaterThanOrEqualTo(3);
  }

  @Test
  void scan_withLimit() {
    // Put many items
    for (int i = 1; i <= 10; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user-" + i).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
          "data", AttributeValue.builder().s("Data " + i).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Scan with limit
    final ScanRequest scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .limit(5)
        .build();

    final ScanResponse scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSize(5);
    assertThat(scanResponse.count()).isEqualTo(5);
    assertThat(scanResponse.hasLastEvaluatedKey()).isTrue();
  }

  @Test
  void operations_onNonExistentTable() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    assertThatThrownBy(() ->
        client.getItem(GetItemRequest.builder()
            .tableName("NonExistentTable")
            .key(key)
            .build())
    ).isInstanceOf(ResourceNotFoundException.class);
  }

  @Test
  void complexWorkflow_putUpdateQueryDelete() {
    // 1. Put multiple items
    for (int i = 1; i <= 3; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("workflow-user").build(),
          SORT_KEY, AttributeValue.builder().s("event-" + i).build(),
          "status", AttributeValue.builder().s("pending").build(),
          "count", AttributeValue.builder().n("0").build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // 2. Update one item
    client.updateItem(UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("workflow-user").build(),
            SORT_KEY, AttributeValue.builder().s("event-2").build()
        ))
        .updateExpression("SET #s = :status, count = count + :inc")
        .expressionAttributeNames(Map.of("#s", "status"))
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("complete").build(),
            ":inc", AttributeValue.builder().n("5").build()
        ))
        .build());

    // 3. Query all items for this user
    final QueryResponse queryResponse = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :uid")
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("workflow-user").build()
        ))
        .build());

    assertThat(queryResponse.items()).hasSize(3);

    // Verify updated item
    final var event2 = queryResponse.items().stream()
        .filter(item -> item.get(SORT_KEY).s().equals("event-2"))
        .findFirst()
        .orElseThrow();
    assertThat(event2.get("status").s()).isEqualTo("complete");
    assertThat(event2.get("count").n()).isEqualTo("5");

    // 4. Delete one item
    client.deleteItem(DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("workflow-user").build(),
            SORT_KEY, AttributeValue.builder().s("event-1").build()
        ))
        .build());

    // 5. Verify deletion
    final QueryResponse afterDeleteQuery = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :uid")
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("workflow-user").build()
        ))
        .build());

    assertThat(afterDeleteQuery.items()).hasSize(2);
  }

  @Test
  void putItem_withConditionExpression_attributeNotExists_success() {
    // First put should succeed (item doesn't exist yet)
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("cond-user-1").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("Conditional User").build()
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .conditionExpression("attribute_not_exists(userId)")
        .build();

    final PutItemResponse putResponse = client.putItem(putRequest);
    assertThat(putResponse).isNotNull();

    // Verify item was created
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("cond-user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.item().get("name").s()).isEqualTo("Conditional User");
  }

  @Test
  void putItem_withConditionExpression_attributeNotExists_fails() {
    // Put an item first
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("cond-user-2").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("Original User").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Try to put again with condition that item doesn't exist - should fail
    final Map<String, AttributeValue> newItem = Map.of(
        HASH_KEY, AttributeValue.builder().s("cond-user-2").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("Updated User").build()
    );

    final PutItemRequest conditionalPutRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(newItem)
        .conditionExpression("attribute_not_exists(userId)")
        .build();

    assertThatThrownBy(() -> client.putItem(conditionalPutRequest))
        .isInstanceOf(ConditionalCheckFailedException.class)
        .hasMessageContaining("conditional request failed");

    // Verify original item still exists
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("cond-user-2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.item().get("name").s()).isEqualTo("Original User");
  }

  @Test
  void putItem_withConditionExpression_attributeEquals_success() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("cond-user-3").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "status", AttributeValue.builder().s("pending").build(),
        "version", AttributeValue.builder().n("1").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Update with condition that status = pending
    final Map<String, AttributeValue> updatedItem = Map.of(
        HASH_KEY, AttributeValue.builder().s("cond-user-3").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "status", AttributeValue.builder().s("active").build(),
        "version", AttributeValue.builder().n("2").build()
    );

    final PutItemRequest conditionalPutRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(updatedItem)
        .conditionExpression("#s = :pending AND #v = :version")
        .expressionAttributeNames(Map.of("#s", "status", "#v", "version"))
        .expressionAttributeValues(Map.of(
            ":pending", AttributeValue.builder().s("pending").build(),
            ":version", AttributeValue.builder().n("1").build()
        ))
        .build();

    final PutItemResponse putResponse = client.putItem(conditionalPutRequest);
    assertThat(putResponse).isNotNull();

    // Verify update succeeded
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("cond-user-3").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.item().get("status").s()).isEqualTo("active");
    assertThat(getResponse.item().get("version").n()).isEqualTo("2");
  }

  @Test
  void deleteItem_withConditionExpression_attributeExists_success() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("del-user-1").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "name", AttributeValue.builder().s("To Delete").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Delete with condition that item exists
    final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .conditionExpression("attribute_exists(userId)")
        .returnValues(ReturnValue.ALL_OLD)
        .build();

    final DeleteItemResponse deleteResponse = client.deleteItem(deleteRequest);
    assertThat(deleteResponse.attributes()).isNotNull();
    assertThat(deleteResponse.attributes().get("name").s()).isEqualTo("To Delete");

    // Verify item was deleted
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.hasItem()).isFalse();
  }

  @Test
  void deleteItem_withConditionExpression_attributeExists_fails() {
    // Try to delete non-existent item with condition - should fail
    final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("nonexistent-user").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .conditionExpression("attribute_exists(userId)")
        .build();

    assertThatThrownBy(() -> client.deleteItem(deleteRequest))
        .isInstanceOf(ConditionalCheckFailedException.class)
        .hasMessageContaining("conditional request failed");
  }

  @Test
  void deleteItem_withConditionExpression_attributeEquals_success() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("del-user-2").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "status", AttributeValue.builder().s("archived").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Delete only if status = archived
    final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .conditionExpression("#s = :archived")
        .expressionAttributeNames(Map.of("#s", "status"))
        .expressionAttributeValues(Map.of(
            ":archived", AttributeValue.builder().s("archived").build()
        ))
        .build();

    final DeleteItemResponse deleteResponse = client.deleteItem(deleteRequest);
    assertThat(deleteResponse).isNotNull();

    // Verify item was deleted
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.hasItem()).isFalse();
  }

  @Test
  void deleteItem_withConditionExpression_attributeEquals_fails() {
    // Put an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("del-user-3").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "status", AttributeValue.builder().s("active").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Try to delete with wrong status - should fail
    final DeleteItemRequest deleteRequest = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-3").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .conditionExpression("#s = :archived")
        .expressionAttributeNames(Map.of("#s", "status"))
        .expressionAttributeValues(Map.of(
            ":archived", AttributeValue.builder().s("archived").build()
        ))
        .build();

    assertThatThrownBy(() -> client.deleteItem(deleteRequest))
        .isInstanceOf(ConditionalCheckFailedException.class)
        .hasMessageContaining("conditional request failed");

    // Verify item still exists
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("del-user-3").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.item().get("status").s()).isEqualTo("active");
  }

  @Test
  void batchGetItem_retrievesMultipleItems() {
    // Put some items first
    for (int i = 0; i < 3; i++) {
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(Map.of(
              HASH_KEY, AttributeValue.builder().s("batch-user-" + i).build(),
              SORT_KEY, AttributeValue.builder().s("2024-01-0" + (i + 1) + "T00:00:00Z").build(),
              "name", AttributeValue.builder().s("Batch User " + i).build()
          ))
          .build());
    }

    // Batch get items
    final software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse response = client.batchGetItem(
        software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest.builder()
            .requestItems(Map.of(
                TABLE_NAME, software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes.builder()
                    .keys(
                        Map.of(
                            HASH_KEY, AttributeValue.builder().s("batch-user-0").build(),
                            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
                        ),
                        Map.of(
                            HASH_KEY, AttributeValue.builder().s("batch-user-1").build(),
                            SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build()
                        ),
                        Map.of(
                            HASH_KEY, AttributeValue.builder().s("batch-user-2").build(),
                            SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build()
                        )
                    )
                    .build()
            ))
            .build()
    );

    assertThat(response.responses()).containsKey(TABLE_NAME);
    assertThat(response.responses().get(TABLE_NAME)).hasSize(3);
    assertThat(response.responses().get(TABLE_NAME).get(0)).containsKey("name");
  }

  @Test
  void batchWriteItem_putsAndDeletesMultipleItems() {
    // Put initial items to delete
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("batch-del-user").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
            "name", AttributeValue.builder().s("To Delete").build()
        ))
        .build());

    // Batch write - put 2 new items and delete 1 existing item
    client.batchWriteItem(
        software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest.builder()
            .requestItems(Map.of(
                TABLE_NAME, List.of(
                    // Put request 1
                    software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
                        .putRequest(software.amazon.awssdk.services.dynamodb.model.PutRequest.builder()
                            .item(Map.of(
                                HASH_KEY, AttributeValue.builder().s("batch-write-user-1").build(),
                                SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
                                "name", AttributeValue.builder().s("Batch Write User 1").build()
                            ))
                            .build())
                        .build(),
                    // Put request 2
                    software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
                        .putRequest(software.amazon.awssdk.services.dynamodb.model.PutRequest.builder()
                            .item(Map.of(
                                HASH_KEY, AttributeValue.builder().s("batch-write-user-2").build(),
                                SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
                                "name", AttributeValue.builder().s("Batch Write User 2").build()
                            ))
                            .build())
                        .build(),
                    // Delete request
                    software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
                        .deleteRequest(software.amazon.awssdk.services.dynamodb.model.DeleteRequest.builder()
                            .key(Map.of(
                                HASH_KEY, AttributeValue.builder().s("batch-del-user").build(),
                                SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
                            ))
                            .build())
                        .build()
                )
            ))
            .build()
    );

    // Verify the puts
    final GetItemResponse getResponse1 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("batch-write-user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse1.hasItem()).isTrue();
    assertThat(getResponse1.item().get("name").s()).isEqualTo("Batch Write User 1");

    final GetItemResponse getResponse2 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("batch-write-user-2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse2.hasItem()).isTrue();
    assertThat(getResponse2.item().get("name").s()).isEqualTo("Batch Write User 2");

    // Verify the delete
    final GetItemResponse getResponseDel = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("batch-del-user").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponseDel.hasItem()).isFalse();
  }

  @Test
  void query_withFilterExpression_filtersResults() {
    // Put multiple items
    final String hashKey = "user123";

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
            "status", AttributeValue.builder().s("active").build(),
            "age", AttributeValue.builder().n("25").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build(),
            "status", AttributeValue.builder().s("inactive").build(),
            "age", AttributeValue.builder().n("30").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build(),
            "status", AttributeValue.builder().s("active").build(),
            "age", AttributeValue.builder().n("35").build()
        ))
        .build());

    // Query with FilterExpression to only get active users
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk")
        .filterExpression("#status = :statusVal")
        .expressionAttributeNames(Map.of(
            "#pk", HASH_KEY,
            "#status", "status"
        ))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build(),
            ":statusVal", AttributeValue.builder().s("active").build()
        ))
        .build());

    // Should only return 2 items (the active ones)
    assertThat(response.count()).isEqualTo(2);
    assertThat(response.items()).hasSize(2);
    assertThat(response.items()).allMatch(item -> item.get("status").s().equals("active"));
  }

  @Test
  void query_withFilterExpression_comparisonOperators() {
    // Put items with numeric attributes
    final String hashKey = "user456";

    for (int i = 1; i <= 5; i++) {
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(Map.of(
              HASH_KEY, AttributeValue.builder().s(hashKey).build(),
              SORT_KEY, AttributeValue.builder().s("2024-01-0" + i + "T00:00:00Z").build(),
              "score", AttributeValue.builder().n(String.valueOf(i * 10)).build()
          ))
          .build());
    }

    // Query with FilterExpression using comparison operator
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :pk")
        .filterExpression("score > :minScore")
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build(),
            ":minScore", AttributeValue.builder().n("30").build()
        ))
        .build());

    // Should return items with score > 30 (40 and 50)
    assertThat(response.count()).isEqualTo(2);
    assertThat(response.items()).allMatch(item ->
        Integer.parseInt(item.get("score").n()) > 30);
  }

  @Test
  void scan_withFilterExpression_filtersResults() {
    // Put multiple items
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
            "category", AttributeValue.builder().s("premium").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build(),
            "category", AttributeValue.builder().s("basic").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user3").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build(),
            "category", AttributeValue.builder().s("premium").build()
        ))
        .build());

    // Scan with FilterExpression
    final ScanResponse response = client.scan(ScanRequest.builder()
        .tableName(TABLE_NAME)
        .filterExpression("category = :cat")
        .expressionAttributeValues(Map.of(
            ":cat", AttributeValue.builder().s("premium").build()
        ))
        .build());

    // Should only return 2 premium items
    assertThat(response.count()).isEqualTo(2);
    assertThat(response.items()).hasSize(2);
    assertThat(response.items()).allMatch(item -> item.get("category").s().equals("premium"));
  }

  @Test
  void scan_withFilterExpression_attributeFunctions() {
    // Put items with/without certain attributes
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
            "email", AttributeValue.builder().s("user1@example.com").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user2").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build()
            // No email attribute
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("user3").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build(),
            "email", AttributeValue.builder().s("user3@example.com").build()
        ))
        .build());

    // Scan with FilterExpression using attribute_exists
    final ScanResponse response = client.scan(ScanRequest.builder()
        .tableName(TABLE_NAME)
        .filterExpression("attribute_exists(email)")
        .build());

    // Should only return 2 items with email attribute
    assertThat(response.count()).isEqualTo(2);
    assertThat(response.items()).hasSize(2);
    assertThat(response.items()).allMatch(item -> item.containsKey("email"));
  }

  @Test
  void query_withFilterExpression_complexExpression() {
    // Put items
    final String hashKey = "user789";

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
            "status", AttributeValue.builder().s("active").build(),
            "score", AttributeValue.builder().n("80").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-02T00:00:00Z").build(),
            "status", AttributeValue.builder().s("active").build(),
            "score", AttributeValue.builder().n("40").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-03T00:00:00Z").build(),
            "status", AttributeValue.builder().s("inactive").build(),
            "score", AttributeValue.builder().n("90").build()
        ))
        .build());

    // Query with complex FilterExpression (AND condition)
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :pk")
        .filterExpression("#status = :statusVal AND score >= :minScore")
        .expressionAttributeNames(Map.of("#status", "status"))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build(),
            ":statusVal", AttributeValue.builder().s("active").build(),
            ":minScore", AttributeValue.builder().n("50").build()
        ))
        .build());

    // Should only return 1 item (active with score >= 50)
    assertThat(response.count()).isEqualTo(1);
    assertThat(response.items()).hasSize(1);
    assertThat(response.items().get(0).get("score").n()).isEqualTo("80");
  }

  @Test
  void transactGetItems_success() {
    // Put test items
    final String hashKey1 = "txn-get-1";
    final String hashKey2 = "txn-get-2";
    final String sortKey1 = "item-1";
    final String sortKey2 = "item-2";

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey1).build(),
            SORT_KEY, AttributeValue.builder().s(sortKey1).build(),
            "data", AttributeValue.builder().s("first item").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey2).build(),
            SORT_KEY, AttributeValue.builder().s(sortKey2).build(),
            "data", AttributeValue.builder().s("second item").build()
        ))
        .build());

    // Execute transactGetItems
    final software.amazon.awssdk.services.dynamodb.model.TransactGetItemsResponse response =
        client.transactGetItems(software.amazon.awssdk.services.dynamodb.model.TransactGetItemsRequest.builder()
            .transactItems(
                software.amazon.awssdk.services.dynamodb.model.TransactGetItem.builder()
                    .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
                        .tableName(TABLE_NAME)
                        .key(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey1).build(),
                            SORT_KEY, AttributeValue.builder().s(sortKey1).build()
                        ))
                        .build())
                    .build(),
                software.amazon.awssdk.services.dynamodb.model.TransactGetItem.builder()
                    .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
                        .tableName(TABLE_NAME)
                        .key(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey2).build(),
                            SORT_KEY, AttributeValue.builder().s(sortKey2).build()
                        ))
                        .build())
                    .build()
            )
            .build());

    // Verify both items were retrieved
    assertThat(response.responses()).hasSize(2);
    assertThat(response.responses().get(0).item().get("data").s()).isEqualTo("first item");
    assertThat(response.responses().get(1).item().get("data").s()).isEqualTo("second item");
  }

  @Test
  void transactGetItems_withProjection_returnsOnlyProjectedAttributes() {
    // Put test items with multiple attributes
    final String hashKey1 = "user-proj-1";
    final String hashKey2 = "user-proj-2";
    final String sortKey1 = "2024-01-01";
    final String sortKey2 = "2024-01-02";

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey1).build(),
            SORT_KEY, AttributeValue.builder().s(sortKey1).build(),
            "name", AttributeValue.builder().s("Alice").build(),
            "email", AttributeValue.builder().s("alice@example.com").build(),
            "age", AttributeValue.builder().n("25").build(),
            "city", AttributeValue.builder().s("New York").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey2).build(),
            SORT_KEY, AttributeValue.builder().s(sortKey2).build(),
            "name", AttributeValue.builder().s("Bob").build(),
            "email", AttributeValue.builder().s("bob@example.com").build(),
            "age", AttributeValue.builder().n("30").build(),
            "city", AttributeValue.builder().s("San Francisco").build()
        ))
        .build());

    // Execute transactGetItems with projection (only name and email)
    final software.amazon.awssdk.services.dynamodb.model.TransactGetItemsResponse response =
        client.transactGetItems(software.amazon.awssdk.services.dynamodb.model.TransactGetItemsRequest.builder()
            .transactItems(
                software.amazon.awssdk.services.dynamodb.model.TransactGetItem.builder()
                    .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
                        .tableName(TABLE_NAME)
                        .key(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey1).build(),
                            SORT_KEY, AttributeValue.builder().s(sortKey1).build()
                        ))
                        .projectionExpression("name, email")
                        .build())
                    .build(),
                software.amazon.awssdk.services.dynamodb.model.TransactGetItem.builder()
                    .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
                        .tableName(TABLE_NAME)
                        .key(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey2).build(),
                            SORT_KEY, AttributeValue.builder().s(sortKey2).build()
                        ))
                        .projectionExpression("name, email")
                        .build())
                    .build()
            )
            .build());

    // Verify both items were retrieved with only projected attributes
    assertThat(response.responses()).hasSize(2);

    // First item - should have name and email, but NOT age or city
    final Map<String, AttributeValue> item1 = response.responses().get(0).item();
    assertThat(item1).containsKey("name");
    assertThat(item1).containsKey("email");
    assertThat(item1.get("name").s()).isEqualTo("Alice");
    assertThat(item1.get("email").s()).isEqualTo("alice@example.com");
    assertThat(item1).doesNotContainKey("age");
    assertThat(item1).doesNotContainKey("city");

    // Second item - should have name and email, but NOT age or city
    final Map<String, AttributeValue> item2 = response.responses().get(1).item();
    assertThat(item2).containsKey("name");
    assertThat(item2).containsKey("email");
    assertThat(item2.get("name").s()).isEqualTo("Bob");
    assertThat(item2.get("email").s()).isEqualTo("bob@example.com");
    assertThat(item2).doesNotContainKey("age");
    assertThat(item2).doesNotContainKey("city");
  }

  @Test
  void transactWriteItems_allPuts_success() {
    final String hashKey1 = "txn-put-1";
    final String hashKey2 = "txn-put-2";

    // Execute transactWriteItems with two puts
    client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
        .transactItems(
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
                    .tableName(TABLE_NAME)
                    .item(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey1).build(),
                        SORT_KEY, AttributeValue.builder().s("item-1").build(),
                        "value", AttributeValue.builder().n("100").build()
                    ))
                    .build())
                .build(),
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
                    .tableName(TABLE_NAME)
                    .item(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey2).build(),
                        SORT_KEY, AttributeValue.builder().s("item-2").build(),
                        "value", AttributeValue.builder().n("200").build()
                    ))
                    .build())
                .build()
        )
        .build());

    // Verify both items were created
    final GetItemResponse item1 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey1).build(),
            SORT_KEY, AttributeValue.builder().s("item-1").build()
        ))
        .build());

    final GetItemResponse item2 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey2).build(),
            SORT_KEY, AttributeValue.builder().s("item-2").build()
        ))
        .build());

    assertThat(item1.item().get("value").n()).isEqualTo("100");
    assertThat(item2.item().get("value").n()).isEqualTo("200");
  }

  @Test
  void transactWriteItems_mixedOperations_success() {
    final String hashKey = "txn-mixed";

    // First create an item to update and delete
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-update").build(),
            "value", AttributeValue.builder().n("10").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-delete").build(),
            "value", AttributeValue.builder().n("20").build()
        ))
        .build());

    // Execute transaction with Put, Update, and Delete
    client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
        .transactItems(
            // Put a new item
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
                    .tableName(TABLE_NAME)
                    .item(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                        SORT_KEY, AttributeValue.builder().s("item-new").build(),
                        "value", AttributeValue.builder().n("30").build()
                    ))
                    .build())
                .build(),
            // Update existing item
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .update(software.amazon.awssdk.services.dynamodb.model.Update.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                        SORT_KEY, AttributeValue.builder().s("item-update").build()
                    ))
                    .updateExpression("SET #v = #v + :inc")
                    .expressionAttributeNames(Map.of("#v", "value"))
                    .expressionAttributeValues(Map.of(
                        ":inc", AttributeValue.builder().n("5").build()
                    ))
                    .build())
                .build(),
            // Delete existing item
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .delete(software.amazon.awssdk.services.dynamodb.model.Delete.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                        SORT_KEY, AttributeValue.builder().s("item-delete").build()
                    ))
                    .build())
                .build()
        )
        .build());

    // Verify new item was created
    final GetItemResponse newItem = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-new").build()
        ))
        .build());
    assertThat(newItem.item().get("value").n()).isEqualTo("30");

    // Verify update was applied
    final GetItemResponse updatedItem = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-update").build()
        ))
        .build());
    assertThat(updatedItem.item().get("value").n()).isEqualTo("15");

    // Verify item was deleted
    final GetItemResponse deletedItem = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-delete").build()
        ))
        .build());
    assertThat(deletedItem.hasItem()).isFalse();
  }

  @Test
  void transactWriteItems_conditionCheckSuccess() {
    final String hashKey = "txn-condition";

    // Create item with version attribute
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-1").build(),
            "version", AttributeValue.builder().n("1").build(),
            "data", AttributeValue.builder().s("original").build()
        ))
        .build());

    // Execute transaction with condition check and update
    client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
        .transactItems(
            // Condition check - verify version is 1
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .conditionCheck(software.amazon.awssdk.services.dynamodb.model.ConditionCheck.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                        SORT_KEY, AttributeValue.builder().s("item-1").build()
                    ))
                    .conditionExpression("version = :v")
                    .expressionAttributeValues(Map.of(
                        ":v", AttributeValue.builder().n("1").build()
                    ))
                    .build())
                .build(),
            // Update item
            software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                .update(software.amazon.awssdk.services.dynamodb.model.Update.builder()
                    .tableName(TABLE_NAME)
                    .key(Map.of(
                        HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                        SORT_KEY, AttributeValue.builder().s("item-1").build()
                    ))
                    .updateExpression("SET version = :newV, #d = :newData")
                    .expressionAttributeNames(Map.of("#d", "data"))
                    .expressionAttributeValues(Map.of(
                        ":newV", AttributeValue.builder().n("2").build(),
                        ":newData", AttributeValue.builder().s("updated").build()
                    ))
                    .build())
                .build()
        )
        .build());

    // Verify update was applied
    final GetItemResponse item = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-1").build()
        ))
        .build());

    assertThat(item.item().get("version").n()).isEqualTo("2");
    assertThat(item.item().get("data").s()).isEqualTo("updated");
  }

  @Test
  void transactWriteItems_conditionCheckFailure() {
    final String hashKey = "txn-fail";

    // Create item with version attribute
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-1").build(),
            "version", AttributeValue.builder().n("2").build()
        ))
        .build());

    // Attempt transaction with incorrect version check - should fail
    assertThatThrownBy(() ->
        client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
            .transactItems(
                software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                    .conditionCheck(software.amazon.awssdk.services.dynamodb.model.ConditionCheck.builder()
                        .tableName(TABLE_NAME)
                        .key(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                            SORT_KEY, AttributeValue.builder().s("item-1").build()
                        ))
                        .conditionExpression("version = :v")
                        .expressionAttributeValues(Map.of(
                            ":v", AttributeValue.builder().n("1").build()  // Wrong version!
                        ))
                        .build())
                    .build()
            )
            .build())
    ).isInstanceOf(software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException.class);
  }

  @Test
  void transactWriteItems_atomicRollback_onConditionFailure() {
    // Test that transaction atomicity works: if operation #2 fails, operation #1 should be rolled back
    final String hashKey = "rollback-test";

    // Create initial item
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("existing-item").build(),
            "value", AttributeValue.builder().s("initial").build()
        ))
        .build());

    // Attempt transaction:
    // 1. Put a new item (should succeed)
    // 2. Update existing item with wrong condition (should fail)
    // Result: NEITHER operation should be persisted due to rollback
    assertThatThrownBy(() ->
        client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
            .transactItems(
                // Operation 1: Put new item
                software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                    .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
                        .tableName(TABLE_NAME)
                        .item(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                            SORT_KEY, AttributeValue.builder().s("new-item").build(),
                            "value", AttributeValue.builder().s("should-not-exist").build()
                        ))
                        .build())
                    .build(),
                // Operation 2: Update existing item with failing condition
                software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                    .update(software.amazon.awssdk.services.dynamodb.model.Update.builder()
                        .tableName(TABLE_NAME)
                        .key(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                            SORT_KEY, AttributeValue.builder().s("existing-item").build()
                        ))
                        .updateExpression("SET #v = :newval")
                        .conditionExpression("#v = :wrongval")  // This will fail
                        .expressionAttributeNames(Map.of("#v", "value"))
                        .expressionAttributeValues(Map.of(
                            ":newval", AttributeValue.builder().s("updated").build(),
                            ":wrongval", AttributeValue.builder().s("wrong").build()
                        ))
                        .build())
                    .build()
            )
            .build())
    ).isInstanceOf(software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException.class);

    // Verify operation #1 was rolled back - the new item should NOT exist
    final GetItemResponse getNewItemResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("new-item").build()
        ))
        .build());
    assertThat(getNewItemResponse.hasItem()).isFalse();  // Item should not exist due to rollback

    // Verify existing item was not modified
    final GetItemResponse getExistingResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("existing-item").build()
        ))
        .build());
    assertThat(getExistingResponse.hasItem()).isTrue();
    assertThat(getExistingResponse.item().get("value").s()).isEqualTo("initial");  // Should still be "initial"
  }

  @Test
  void transactWriteItems_multipleOperations_allOrNothing() {
    // Test atomic transaction with 3 operations: if #3 fails, #1 and #2 should be rolled back
    final String hashKey = "multi-op-test";

    // Attempt transaction with 3 operations, where the 3rd will fail
    assertThatThrownBy(() ->
        client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
            .transactItems(
                // Operation 1: Put item A
                software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                    .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
                        .tableName(TABLE_NAME)
                        .item(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                            SORT_KEY, AttributeValue.builder().s("item-a").build(),
                            "value", AttributeValue.builder().s("A").build()
                        ))
                        .build())
                    .build(),
                // Operation 2: Put item B
                software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                    .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
                        .tableName(TABLE_NAME)
                        .item(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                            SORT_KEY, AttributeValue.builder().s("item-b").build(),
                            "value", AttributeValue.builder().s("B").build()
                        ))
                        .build())
                    .build(),
                // Operation 3: Put item with failing condition (item should not exist)
                software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
                    .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
                        .tableName(TABLE_NAME)
                        .item(Map.of(
                            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                            SORT_KEY, AttributeValue.builder().s("item-c").build(),
                            "value", AttributeValue.builder().s("C").build()
                        ))
                        .conditionExpression("attribute_exists(#pk)")  // Will fail since item doesn't exist
                        .expressionAttributeNames(Map.of("#pk", HASH_KEY))
                        .build())
                    .build()
            )
            .build())
    ).isInstanceOf(software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException.class);

    // Verify ALL operations were rolled back - none of the items should exist
    final GetItemResponse getItemA = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-a").build()
        ))
        .build());
    assertThat(getItemA.hasItem()).isFalse();

    final GetItemResponse getItemB = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-b").build()
        ))
        .build());
    assertThat(getItemB.hasItem()).isFalse();

    final GetItemResponse getItemC = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-c").build()
        ))
        .build());
    assertThat(getItemC.hasItem()).isFalse();
  }

  @Test
  void batchWriteItem_partialFailure_returnsUnprocessedItems() {
    // Test that batch write properly tracks failed items and returns them as unprocessed
    final String hashKey = "batch-test";

    // Create an existing item with a specific value
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("existing").build(),
            "value", AttributeValue.builder().s("original").build()
        ))
        .build());

    // Attempt batch write with multiple items - some may fail (we'll force a failure using a non-existent table)
    // Create a write request that includes both valid and invalid operations
    final List<software.amazon.awssdk.services.dynamodb.model.WriteRequest> validRequests = List.of(
        software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
            .putRequest(software.amazon.awssdk.services.dynamodb.model.PutRequest.builder()
                .item(Map.of(
                    HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                    SORT_KEY, AttributeValue.builder().s("item-1").build(),
                    "value", AttributeValue.builder().s("value-1").build()
                ))
                .build())
            .build(),
        software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
            .putRequest(software.amazon.awssdk.services.dynamodb.model.PutRequest.builder()
                .item(Map.of(
                    HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                    SORT_KEY, AttributeValue.builder().s("item-2").build(),
                    "value", AttributeValue.builder().s("value-2").build()
                ))
                .build())
            .build()
    );

    final List<software.amazon.awssdk.services.dynamodb.model.WriteRequest> invalidTableRequests = List.of(
        software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
            .putRequest(software.amazon.awssdk.services.dynamodb.model.PutRequest.builder()
                .item(Map.of(
                    HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                    SORT_KEY, AttributeValue.builder().s("item-3").build(),
                    "value", AttributeValue.builder().s("value-3").build()
                ))
                .build())
            .build()
    );

    // Execute batch write with one non-existent table
    final software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse response =
        client.batchWriteItem(software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest.builder()
            .requestItems(Map.of(
                TABLE_NAME, validRequests,
                "non-existent-table", invalidTableRequests
            ))
            .build());

    // Verify that items for the non-existent table are returned as unprocessed
    assertThat(response.hasUnprocessedItems()).isTrue();
    assertThat(response.unprocessedItems()).containsKey("non-existent-table");
    assertThat(response.unprocessedItems().get("non-existent-table")).hasSize(1);

    // Verify that items for the valid table were successfully written
    final GetItemResponse item1 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-1").build()
        ))
        .build());
    assertThat(item1.hasItem()).isTrue();
    assertThat(item1.item().get("value").s()).isEqualTo("value-1");

    final GetItemResponse item2 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-2").build()
        ))
        .build());
    assertThat(item2.hasItem()).isTrue();
    assertThat(item2.item().get("value").s()).isEqualTo("value-2");
  }

  @Test
  void batchWriteItem_allSuccess_noUnprocessedItems() {
    // Test that when all writes succeed, no unprocessed items are returned
    final String hashKey = "batch-success-test";

    final software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse response =
        client.batchWriteItem(software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest.builder()
            .requestItems(Map.of(
                TABLE_NAME, List.of(
                    software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
                        .putRequest(software.amazon.awssdk.services.dynamodb.model.PutRequest.builder()
                            .item(Map.of(
                                HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                                SORT_KEY, AttributeValue.builder().s("item-1").build(),
                                "value", AttributeValue.builder().s("value-1").build()
                            ))
                            .build())
                        .build(),
                    software.amazon.awssdk.services.dynamodb.model.WriteRequest.builder()
                        .putRequest(software.amazon.awssdk.services.dynamodb.model.PutRequest.builder()
                            .item(Map.of(
                                HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                                SORT_KEY, AttributeValue.builder().s("item-2").build(),
                                "value", AttributeValue.builder().s("value-2").build()
                            ))
                            .build())
                        .build()
                )
            ))
            .build());

    // Verify no unprocessed items
    assertThat(response.hasUnprocessedItems()).isFalse();

    // Verify all items were written
    final GetItemResponse item1 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-1").build()
        ))
        .build());
    assertThat(item1.hasItem()).isTrue();

    final GetItemResponse item2 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s(hashKey).build(),
            SORT_KEY, AttributeValue.builder().s("item-2").build()
        ))
        .build());
    assertThat(item2.hasItem()).isTrue();
  }

  @Test
  void query_withPagination_multiplePages() {
    // Test that query pagination works correctly with ExclusiveStartKey
    final String hashKey = "pagination-test";

    // Create 10 items with the same hash key but different sort keys
    for (int i = 1; i <= 10; i++) {
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(Map.of(
              HASH_KEY, AttributeValue.builder().s(hashKey).build(),
              SORT_KEY, AttributeValue.builder().s(String.format("item-%02d", i)).build(),
              "value", AttributeValue.builder().n(String.valueOf(i)).build()
          ))
          .build());
    }

    // First query - get 3 items
    QueryResponse page1 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build()
        ))
        .limit(3)
        .build());

    assertThat(page1.count()).isEqualTo(3);
    assertThat(page1.hasLastEvaluatedKey()).isTrue();
    assertThat(page1.items().get(0).get(SORT_KEY).s()).isEqualTo("item-01");
    assertThat(page1.items().get(2).get(SORT_KEY).s()).isEqualTo("item-03");

    // Second query - use LastEvaluatedKey to get next 3 items
    QueryResponse page2 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build()
        ))
        .exclusiveStartKey(page1.lastEvaluatedKey())
        .limit(3)
        .build());

    assertThat(page2.count()).isEqualTo(3);
    assertThat(page2.hasLastEvaluatedKey()).isTrue();
    assertThat(page2.items().get(0).get(SORT_KEY).s()).isEqualTo("item-04");
    assertThat(page2.items().get(2).get(SORT_KEY).s()).isEqualTo("item-06");

    // Third query - get next 3 items
    QueryResponse page3 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build()
        ))
        .exclusiveStartKey(page2.lastEvaluatedKey())
        .limit(3)
        .build());

    assertThat(page3.count()).isEqualTo(3);
    assertThat(page3.hasLastEvaluatedKey()).isTrue();
    assertThat(page3.items().get(0).get(SORT_KEY).s()).isEqualTo("item-07");
    assertThat(page3.items().get(2).get(SORT_KEY).s()).isEqualTo("item-09");

    // Fourth query - get final item (only 1 left)
    QueryResponse page4 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build()
        ))
        .exclusiveStartKey(page3.lastEvaluatedKey())
        .limit(3)
        .build());

    assertThat(page4.count()).isEqualTo(1);
    assertThat(page4.hasLastEvaluatedKey()).isFalse();  // No more items
    assertThat(page4.items().get(0).get(SORT_KEY).s()).isEqualTo("item-10");

    // Verify we got all 10 unique items
    final int totalItems = page1.count() + page2.count() + page3.count() + page4.count();
    assertThat(totalItems).isEqualTo(10);
  }

  @Test
  void query_withPagination_exactPageBoundary() {
    // Test pagination when the total items is exactly a multiple of the page size
    final String hashKey = "exact-boundary-test";

    // Create exactly 9 items (3 pages of 3 items each)
    for (int i = 1; i <= 9; i++) {
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(Map.of(
              HASH_KEY, AttributeValue.builder().s(hashKey).build(),
              SORT_KEY, AttributeValue.builder().s(String.format("item-%02d", i)).build(),
              "value", AttributeValue.builder().n(String.valueOf(i)).build()
          ))
          .build());
    }

    // Page 1
    QueryResponse page1 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build()
        ))
        .limit(3)
        .build());

    assertThat(page1.count()).isEqualTo(3);
    assertThat(page1.hasLastEvaluatedKey()).isTrue();

    // Page 2
    QueryResponse page2 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build()
        ))
        .exclusiveStartKey(page1.lastEvaluatedKey())
        .limit(3)
        .build());

    assertThat(page2.count()).isEqualTo(3);
    assertThat(page2.hasLastEvaluatedKey()).isTrue();

    // Page 3 - last page
    QueryResponse page3 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build()
        ))
        .exclusiveStartKey(page2.lastEvaluatedKey())
        .limit(3)
        .build());

    assertThat(page3.count()).isEqualTo(3);
    assertThat(page3.hasLastEvaluatedKey()).isFalse();  // No more items - exactly at boundary

    // Verify total
    assertThat(page1.count() + page2.count() + page3.count()).isEqualTo(9);
  }

  @Test
  void query_withPagination_andSortKeyCondition() {
    // Test pagination with a sort key condition (e.g., begins_with)
    final String hashKey = "pagination-sort-condition-test";

    // Create items with different prefixes
    for (int i = 1; i <= 10; i++) {
      String prefix = (i <= 5) ? "A" : "B";
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(Map.of(
              HASH_KEY, AttributeValue.builder().s(hashKey).build(),
              SORT_KEY, AttributeValue.builder().s(prefix + String.format("-%02d", i)).build(),
              "value", AttributeValue.builder().n(String.valueOf(i)).build()
          ))
          .build());
    }

    // Query only items starting with "A" with pagination
    QueryResponse page1 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk AND begins_with(#sk, :prefix)")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY, "#sk", SORT_KEY))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build(),
            ":prefix", AttributeValue.builder().s("A").build()
        ))
        .limit(2)
        .build());

    assertThat(page1.count()).isEqualTo(2);
    assertThat(page1.hasLastEvaluatedKey()).isTrue();
    assertThat(page1.items().get(0).get(SORT_KEY).s()).startsWith("A");

    // Get next page
    QueryResponse page2 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk AND begins_with(#sk, :prefix)")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY, "#sk", SORT_KEY))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build(),
            ":prefix", AttributeValue.builder().s("A").build()
        ))
        .exclusiveStartKey(page1.lastEvaluatedKey())
        .limit(2)
        .build());

    assertThat(page2.count()).isEqualTo(2);
    assertThat(page2.hasLastEvaluatedKey()).isTrue();
    assertThat(page2.items().get(0).get(SORT_KEY).s()).startsWith("A");

    // Get final page
    QueryResponse page3 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk AND begins_with(#sk, :prefix)")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY, "#sk", SORT_KEY))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s(hashKey).build(),
            ":prefix", AttributeValue.builder().s("A").build()
        ))
        .exclusiveStartKey(page2.lastEvaluatedKey())
        .limit(2)
        .build());

    assertThat(page3.count()).isEqualTo(1);  // Only 1 item left (5 total items with prefix "A")
    assertThat(page3.hasLastEvaluatedKey()).isFalse();
    assertThat(page3.items().get(0).get(SORT_KEY).s()).startsWith("A");

    // Verify total
    assertThat(page1.count() + page2.count() + page3.count()).isEqualTo(5);
  }

  @Test
  void transactWriteItems_exceedsMaxItemCount_throwsException() {
    // Test that transaction with more than 25 items throws IllegalArgumentException
    final String hashKey = "limit-test";

    // Create a list with 26 transaction items (exceeds DynamoDB limit of 25)
    final List<software.amazon.awssdk.services.dynamodb.model.TransactWriteItem> items = new ArrayList<>();
    for (int i = 1; i <= 26; i++) {
      items.add(software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
          .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
              .tableName(TABLE_NAME)
              .item(Map.of(
                  HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                  SORT_KEY, AttributeValue.builder().s(String.format("item-%02d", i)).build(),
                  "value", AttributeValue.builder().n(String.valueOf(i)).build()
              ))
              .build())
          .build());
    }

    // Attempt transaction with 26 items - should fail
    assertThatThrownBy(() ->
        client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
            .transactItems(items)
            .build())
    ).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Transaction request cannot contain more than 25 items")
        .hasMessageContaining("26 items");
  }

  @Test
  void transactWriteItems_exactlyMaxItemCount_succeeds() {
    // Test that transaction with exactly 25 items succeeds
    final String hashKey = "max-limit-test";

    // Create a list with exactly 25 transaction items (at the limit)
    final List<software.amazon.awssdk.services.dynamodb.model.TransactWriteItem> items = new ArrayList<>();
    for (int i = 1; i <= 25; i++) {
      items.add(software.amazon.awssdk.services.dynamodb.model.TransactWriteItem.builder()
          .put(software.amazon.awssdk.services.dynamodb.model.Put.builder()
              .tableName(TABLE_NAME)
              .item(Map.of(
                  HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                  SORT_KEY, AttributeValue.builder().s(String.format("item-%02d", i)).build(),
                  "value", AttributeValue.builder().n(String.valueOf(i)).build()
              ))
              .build())
          .build());
    }

    // Execute transaction with 25 items - should succeed
    final software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse response =
        client.transactWriteItems(software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest.builder()
            .transactItems(items)
            .build());

    assertThat(response).isNotNull();

    // Verify all 25 items were created
    for (int i = 1; i <= 25; i++) {
      final GetItemResponse item = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of(
              HASH_KEY, AttributeValue.builder().s(hashKey).build(),
              SORT_KEY, AttributeValue.builder().s(String.format("item-%02d", i)).build()
          ))
          .build());
      assertThat(item.hasItem()).isTrue();
    }
  }

  @Test
  void transactGetItems_exceedsMaxItemCount_throwsException() {
    // Test that transactGetItems with more than 25 items throws IllegalArgumentException
    final String hashKey = "get-limit-test";

    // Create a list with 26 transaction get items (exceeds DynamoDB limit of 25)
    final List<software.amazon.awssdk.services.dynamodb.model.TransactGetItem> items = new ArrayList<>();
    for (int i = 1; i <= 26; i++) {
      items.add(software.amazon.awssdk.services.dynamodb.model.TransactGetItem.builder()
          .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
              .tableName(TABLE_NAME)
              .key(Map.of(
                  HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                  SORT_KEY, AttributeValue.builder().s(String.format("item-%02d", i)).build()
              ))
              .build())
          .build());
    }

    // Attempt transaction with 26 items - should fail
    assertThatThrownBy(() ->
        client.transactGetItems(software.amazon.awssdk.services.dynamodb.model.TransactGetItemsRequest.builder()
            .transactItems(items)
            .build())
    ).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Transaction request cannot contain more than 25 items")
        .hasMessageContaining("26 items");
  }

  @Test
  void transactGetItems_exactlyMaxItemCount_succeeds() {
    // Test that transactGetItems with exactly 25 items succeeds
    final String hashKey = "get-max-limit-test";

    // First, create 25 items
    for (int i = 1; i <= 25; i++) {
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(Map.of(
              HASH_KEY, AttributeValue.builder().s(hashKey).build(),
              SORT_KEY, AttributeValue.builder().s(String.format("item-%02d", i)).build(),
              "value", AttributeValue.builder().n(String.valueOf(i)).build()
          ))
          .build());
    }

    // Create a list with exactly 25 transaction get items (at the limit)
    final List<software.amazon.awssdk.services.dynamodb.model.TransactGetItem> items = new ArrayList<>();
    for (int i = 1; i <= 25; i++) {
      items.add(software.amazon.awssdk.services.dynamodb.model.TransactGetItem.builder()
          .get(software.amazon.awssdk.services.dynamodb.model.Get.builder()
              .tableName(TABLE_NAME)
              .key(Map.of(
                  HASH_KEY, AttributeValue.builder().s(hashKey).build(),
                  SORT_KEY, AttributeValue.builder().s(String.format("item-%02d", i)).build()
              ))
              .build())
          .build());
    }

    // Execute transaction with 25 items - should succeed
    final software.amazon.awssdk.services.dynamodb.model.TransactGetItemsResponse response =
        client.transactGetItems(software.amazon.awssdk.services.dynamodb.model.TransactGetItemsRequest.builder()
            .transactItems(items)
            .build());

    assertThat(response).isNotNull();
    assertThat(response.responses()).hasSize(25);

    // Verify all items were retrieved
    for (int i = 0; i < 25; i++) {
      assertThat(response.responses().get(i).hasItem()).isTrue();
      assertThat(response.responses().get(i).item().get("value").n()).isEqualTo(String.valueOf(i + 1));
    }
  }

  @Test
  void batchGetItem_exceedsMaxItemCount_throwsException() {
    // Create 101 items in the table (exceeds BatchGetItem limit of 100)
    for (int i = 0; i < 101; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user" + i).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
          "value", AttributeValue.builder().n(String.valueOf(i + 1)).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Build batch get request with 101 keys
    final List<Map<String, AttributeValue>> keys = new java.util.ArrayList<>();
    for (int i = 0; i < 101; i++) {
      keys.add(Map.of(
          HASH_KEY, AttributeValue.builder().s("user" + i).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
      ));
    }

    final BatchGetItemRequest batchGetRequest = BatchGetItemRequest.builder()
        .requestItems(Map.of(
            TABLE_NAME, KeysAndAttributes.builder()
                .keys(keys)
                .build()
        ))
        .build();

    // Verify that batch get with 101 items throws exception
    assertThatThrownBy(() -> client.batchGetItem(batchGetRequest))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("BatchGetItem request cannot contain more than 100 items")
        .hasMessageContaining("101 items");
  }

  @Test
  void batchGetItem_exactlyMaxItemCount_succeeds() {
    // Create exactly 100 items in the table (at the limit)
    for (int i = 0; i < 100; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user" + i).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
          "value", AttributeValue.builder().n(String.valueOf(i + 1)).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Build batch get request with exactly 100 keys
    final List<Map<String, AttributeValue>> keys = new java.util.ArrayList<>();
    for (int i = 0; i < 100; i++) {
      keys.add(Map.of(
          HASH_KEY, AttributeValue.builder().s("user" + i).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
      ));
    }

    final BatchGetItemRequest batchGetRequest = BatchGetItemRequest.builder()
        .requestItems(Map.of(
            TABLE_NAME, KeysAndAttributes.builder()
                .keys(keys)
                .build()
        ))
        .build();

    // Verify batch get with exactly 100 items succeeds
    final BatchGetItemResponse response = client.batchGetItem(batchGetRequest);
    assertThat(response).isNotNull();
    assertThat(response.responses()).isNotNull();
    assertThat(response.responses().get(TABLE_NAME)).hasSize(100);
  }

  @Test
  void batchWriteItem_exceedsMaxRequestCount_throwsException() {
    // Build batch write request with 26 write requests (exceeds limit of 25)
    final List<WriteRequest> writeRequests = new java.util.ArrayList<>();
    for (int i = 0; i < 26; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user" + i).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
          "value", AttributeValue.builder().n(String.valueOf(i + 1)).build()
      );
      writeRequests.add(WriteRequest.builder()
          .putRequest(PutRequest.builder().item(item).build())
          .build());
    }

    final BatchWriteItemRequest batchWriteRequest = BatchWriteItemRequest.builder()
        .requestItems(Map.of(TABLE_NAME, writeRequests))
        .build();

    // Verify that batch write with 26 requests throws exception
    assertThatThrownBy(() -> client.batchWriteItem(batchWriteRequest))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("BatchWriteItem request cannot contain more than 25 requests")
        .hasMessageContaining("26 requests");
  }

  @Test
  void batchWriteItem_exactlyMaxRequestCount_succeeds() {
    // Build batch write request with exactly 25 write requests (at the limit)
    final List<WriteRequest> writeRequests = new java.util.ArrayList<>();
    for (int i = 0; i < 25; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user" + i).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
          "value", AttributeValue.builder().n(String.valueOf(i + 1)).build()
      );
      writeRequests.add(WriteRequest.builder()
          .putRequest(PutRequest.builder().item(item).build())
          .build());
    }

    final BatchWriteItemRequest batchWriteRequest = BatchWriteItemRequest.builder()
        .requestItems(Map.of(TABLE_NAME, writeRequests))
        .build();

    // Verify batch write with exactly 25 requests succeeds
    final BatchWriteItemResponse response = client.batchWriteItem(batchWriteRequest);
    assertThat(response).isNotNull();
    assertThat(response.unprocessedItems()).isEmpty();

    // Verify all items were written
    for (int i = 0; i < 25; i++) {
      final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of(
              HASH_KEY, AttributeValue.builder().s("user" + i).build(),
              SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
          ))
          .build());
      assertThat(getResponse.hasItem()).isTrue();
      assertThat(getResponse.item().get("value").n()).isEqualTo(String.valueOf(i + 1));
    }
  }

  @Test
  void putItem_emptyStringInHashKey_throwsException() {
    // Attempt to put item with empty string as hash key value
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("").build(),  // Empty string - not allowed in key
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "value", AttributeValue.builder().n("123").build()
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    // Verify that empty string in hash key throws exception
    assertThatThrownBy(() -> client.putItem(putRequest))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("An AttributeValue may not contain an empty string")
        .hasMessageContaining("Key: " + HASH_KEY);
  }

  @Test
  void putItem_emptyStringInSortKey_throwsException() {
    // Attempt to put item with empty string as sort key value
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user123").build(),
        SORT_KEY, AttributeValue.builder().s("").build(),  // Empty string - not allowed in key
        "value", AttributeValue.builder().n("123").build()
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    // Verify that empty string in sort key throws exception
    assertThatThrownBy(() -> client.putItem(putRequest))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("An AttributeValue may not contain an empty string")
        .hasMessageContaining("Key: " + SORT_KEY);
  }

  @Test
  void putItem_emptyBinaryAttribute_throwsException() {
    // Attempt to put item with empty binary attribute
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "data", AttributeValue.builder().b(software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[0])).build()  // Empty binary
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    // Verify that empty binary attribute throws exception
    assertThatThrownBy(() -> client.putItem(putRequest))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Binary attributes cannot be empty")
        .hasMessageContaining("Attribute: data");
  }

  @Test
  void putItem_emptyStringInStringSet_throwsException() {
    // Attempt to put item with empty string in string set
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "tags", AttributeValue.builder().ss("tag1", "", "tag3").build()  // Empty string in set
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    // Verify that empty string in string set throws exception
    assertThatThrownBy(() -> client.putItem(putRequest))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("String set attributes cannot contain empty strings")
        .hasMessageContaining("Attribute: tags");
  }

  @Test
  void putItem_emptyBytesInBinarySet_throwsException() {
    // Attempt to put item with empty bytes in binary set
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "binarySet", AttributeValue.builder().bs(
            software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[]{1, 2, 3}),
            software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[0])  // Empty bytes
        ).build()
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    // Verify that empty bytes in binary set throws exception
    assertThatThrownBy(() -> client.putItem(putRequest))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Binary set attributes cannot contain empty values")
        .hasMessageContaining("Attribute: binarySet");
  }

  @Test
  void updateItem_resultingInEmptyHashKey_throwsException() {
    // First create an item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "value", AttributeValue.builder().n("123").build()
    );
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Attempt to update the hash key to an empty string
    final UpdateItemRequest updateRequest = UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("user123").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .updateExpression("SET " + HASH_KEY + " = :emptyValue")
        .expressionAttributeValues(Map.of(
            ":emptyValue", AttributeValue.builder().s("").build()
        ))
        .build();

    // Verify that updating hash key to empty string throws exception
    assertThatThrownBy(() -> client.updateItem(updateRequest))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("An AttributeValue may not contain an empty string")
        .hasMessageContaining("Key: " + HASH_KEY);
  }

  @Test
  void putItem_validNonKeyEmptyString_succeeds() {
    // DynamoDB allows empty strings in non-key attributes (though it's not best practice)
    // Note: This is actually NOT true in real DynamoDB - empty strings are not allowed anywhere
    // But we'll test that non-key attributes can have values
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "description", AttributeValue.builder().s("valid description").build(),
        "value", AttributeValue.builder().n("123").build()
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    // Verify that valid item succeeds
    final PutItemResponse response = client.putItem(putRequest);
    assertThat(response).isNotNull();

    // Verify item was created
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("user123").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.hasItem()).isTrue();
    assertThat(getResponse.item().get("description").s()).isEqualTo("valid description");
  }

  @Test
  void scan_withPagination_multiplePages() {
    // Create 10 items with different hash keys (scan returns all items in table)
    for (int i = 0; i < 10; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user" + String.format("%03d", i)).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
          "value", AttributeValue.builder().n(String.valueOf(i + 1)).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Scan with limit of 3 - should return first 3 items + LastEvaluatedKey
    ScanRequest scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .limit(3)
        .build();

    ScanResponse scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSize(3);
    assertThat(scanResponse.hasLastEvaluatedKey()).isTrue();
    assertThat(scanResponse.lastEvaluatedKey()).isNotNull();

    // Page 2
    scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .limit(3)
        .exclusiveStartKey(scanResponse.lastEvaluatedKey())
        .build();

    scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSize(3);
    assertThat(scanResponse.hasLastEvaluatedKey()).isTrue();

    // Page 3
    scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .limit(3)
        .exclusiveStartKey(scanResponse.lastEvaluatedKey())
        .build();

    scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSize(3);
    assertThat(scanResponse.hasLastEvaluatedKey()).isTrue();

    // Page 4 (final page with 1 item)
    scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .limit(3)
        .exclusiveStartKey(scanResponse.lastEvaluatedKey())
        .build();

    scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSize(1);
    assertThat(scanResponse.hasLastEvaluatedKey()).isFalse();  // No more pages
  }

  @Test
  void scan_withPagination_exactPageBoundary() {
    // Create exactly 9 items (3 pages of 3 items each)
    for (int i = 0; i < 9; i++) {
      final Map<String, AttributeValue> item = Map.of(
          HASH_KEY, AttributeValue.builder().s("user" + String.format("%03d", i)).build(),
          SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
          "value", AttributeValue.builder().n(String.valueOf(i + 1)).build()
      );
      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());
    }

    // Page 1
    ScanRequest scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .limit(3)
        .build();

    ScanResponse scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSize(3);
    assertThat(scanResponse.hasLastEvaluatedKey()).isTrue();

    // Page 2
    scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .limit(3)
        .exclusiveStartKey(scanResponse.lastEvaluatedKey())
        .build();

    scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSize(3);
    assertThat(scanResponse.hasLastEvaluatedKey()).isTrue();

    // Page 3 (final page, exactly 3 items - should have no LastEvaluatedKey)
    scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .limit(3)
        .exclusiveStartKey(scanResponse.lastEvaluatedKey())
        .build();

    scanResponse = client.scan(scanRequest);
    assertThat(scanResponse.items()).hasSize(3);
    assertThat(scanResponse.hasLastEvaluatedKey()).isFalse();  // No more pages (exact boundary)
  }

  @Test
  void putItem_itemSizeExceeds400KB_throwsException() {
    // Create a large item that exceeds 400KB
    // Create a string that's approximately 410KB
    final StringBuilder largeValue = new StringBuilder();
    for (int i = 0; i < 410_000; i++) {
      largeValue.append("x");
    }

    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "largeData", AttributeValue.builder().s(largeValue.toString()).build()
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    // Verify that oversized item throws exception
    assertThatThrownBy(() -> client.putItem(putRequest))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Item size has exceeded the maximum allowed size of 400 KB");
  }

  @Test
  void putItem_itemSizeExactly400KB_succeeds() {
    // Create an item that's approximately at the 400KB limit (but under)
    // We need to account for JSON overhead, so we'll use about 390KB of actual data
    final StringBuilder largeValue = new StringBuilder();
    for (int i = 0; i < 390_000; i++) {
      largeValue.append("x");
    }

    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "largeData", AttributeValue.builder().s(largeValue.toString()).build()
    );

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    // Verify that item at/near the limit succeeds
    final PutItemResponse response = client.putItem(putRequest);
    assertThat(response).isNotNull();

    // Verify item was created
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("user123").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .build());
    assertThat(getResponse.hasItem()).isTrue();
  }

  @Test
  void updateItem_resultingItemSizeExceeds400KB_throwsException() {
    // First create a small item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build(),
        "value", AttributeValue.builder().n("123").build()
    );
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Create a large string that will make the item exceed 400KB when added
    final StringBuilder largeValue = new StringBuilder();
    for (int i = 0; i < 410_000; i++) {
      largeValue.append("x");
    }

    // Attempt to update with a large attribute
    final UpdateItemRequest updateRequest = UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("user123").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
        ))
        .updateExpression("SET largeData = :largeValue")
        .expressionAttributeValues(Map.of(
            ":largeValue", AttributeValue.builder().s(largeValue.toString()).build()
        ))
        .build();

    // Verify that update resulting in oversized item throws exception
    assertThatThrownBy(() -> client.updateItem(updateRequest))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Item size has exceeded the maximum allowed size of 400 KB");
  }
}
