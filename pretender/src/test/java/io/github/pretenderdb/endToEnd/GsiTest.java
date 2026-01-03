package io.github.pretenderdb.endToEnd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * End-to-end tests for Global Secondary Index (GSI) functionality.
 */
class GsiTest extends BaseEndToEndTest {

  private static final String TABLE_NAME = "GsiTestTable";
  private static final String GSI_NAME = "StatusIndex";
  private static final String GSI_NAME_2 = "EmailIndex";

  private software.amazon.awssdk.services.dynamodb.DynamoDbClient client;

  @BeforeEach
  void setup() {
    client = component.dynamoDbPretenderClient();

    // Clean up if exists
    try {
      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  void createTableWithGsi_success() {
    // Create table with one GSI
    final CreateTableRequest request = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName("userId").keyType(KeyType.HASH).build()
        )
        .globalSecondaryIndexes(
            GlobalSecondaryIndex.builder()
                .indexName(GSI_NAME)
                .keySchema(
                    KeySchemaElement.builder().attributeName("status").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("timestamp").keyType(KeyType.RANGE).build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build()
        )
        .build();

    client.createTable(request);

    // Verify table exists
    assertThat(client.listTables().tableNames()).contains(TABLE_NAME);
  }

  @Test
  void putItemAndQueryGsi_withAllProjection() {
    // Create table with GSI (ALL projection)
    createTableWithGsi(ProjectionType.ALL, null);

    // Put items with different statuses
    putItem("user-1", "active", "2024-01-01T00:00:00Z", "alice@example.com", "Alice");
    putItem("user-2", "active", "2024-01-02T00:00:00Z", "bob@example.com", "Bob");
    putItem("user-3", "inactive", "2024-01-03T00:00:00Z", "charlie@example.com", "Charlie");
    putItem("user-4", "active", "2024-01-04T00:00:00Z", "david@example.com", "David");

    // Query GSI for active users
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("active").build()
        ))
        .build());

    // Verify results
    assertThat(response.items()).hasSize(3);
    assertThat(response.count()).isEqualTo(3);

    // Verify all attributes present (ALL projection)
    for (Map<String, AttributeValue> item : response.items()) {
      assertThat(item).containsKeys("userId", "status", "timestamp", "email", "name");
    }
  }

  @Test
  void queryGsi_withSortKeyCondition() {
    createTableWithGsi(ProjectionType.ALL, null);

    // Put items
    putItem("user-1", "active", "2024-01-01T00:00:00Z", "alice@example.com", "Alice");
    putItem("user-2", "active", "2024-01-02T00:00:00Z", "bob@example.com", "Bob");
    putItem("user-3", "active", "2024-01-03T00:00:00Z", "charlie@example.com", "Charlie");
    putItem("user-4", "active", "2024-01-04T00:00:00Z", "david@example.com", "David");

    // Query with sort key condition
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status AND timestamp < :ts")
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("active").build(),
            ":ts", AttributeValue.builder().s("2024-01-03T00:00:00Z").build()
        ))
        .build());

    // Should return users 1 and 2
    assertThat(response.items()).hasSize(2);
    assertThat(response.items()).extracting(item -> item.get("userId").s())
        .containsExactlyInAnyOrder("user-1", "user-2");
  }

  @Test
  void queryGsi_withKeysOnlyProjection() {
    // Create table with KEYS_ONLY projection
    createTableWithGsi(ProjectionType.KEYS_ONLY, null);

    // Put items
    putItem("user-1", "active", "2024-01-01T00:00:00Z", "alice@example.com", "Alice");
    putItem("user-2", "active", "2024-01-02T00:00:00Z", "bob@example.com", "Bob");

    // Query GSI
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("active").build()
        ))
        .build());

    // Verify only keys present
    assertThat(response.items()).hasSize(2);
    for (Map<String, AttributeValue> item : response.items()) {
      // Base table key + GSI keys only
      assertThat(item).containsKeys("userId", "status", "timestamp");
      assertThat(item).doesNotContainKeys("email", "name");
    }
  }

  @Test
  void queryGsi_withIncludeProjection() {
    // Create table with INCLUDE projection (include email)
    createTableWithGsi(ProjectionType.INCLUDE, List.of("email"));

    // Put items
    putItem("user-1", "active", "2024-01-01T00:00:00Z", "alice@example.com", "Alice");
    putItem("user-2", "active", "2024-01-02T00:00:00Z", "bob@example.com", "Bob");

    // Query GSI
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("active").build()
        ))
        .build());

    // Verify keys + included attributes present
    assertThat(response.items()).hasSize(2);
    for (Map<String, AttributeValue> item : response.items()) {
      assertThat(item).containsKeys("userId", "status", "timestamp", "email");
      assertThat(item).doesNotContainKey("name");  // Not included
    }
  }

  @Test
  void updateItem_updatesGsi() {
    createTableWithGsi(ProjectionType.ALL, null);

    // Put item with status=inactive
    putItem("user-1", "inactive", "2024-01-01T00:00:00Z", "alice@example.com", "Alice");

    // Verify not in active query
    QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("active").build()))
        .build());
    assertThat(response.items()).isEmpty();

    // Update status to active
    client.updateItem(software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("userId", AttributeValue.builder().s("user-1").build()))
        .updateExpression("SET status = :status")
        .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("active").build()))
        .build());

    // Verify now in active query
    response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("active").build()))
        .build());
    assertThat(response.items()).hasSize(1);
    assertThat(response.items().get(0).get("userId").s()).isEqualTo("user-1");
  }

  @Test
  void deleteItem_removesFromGsi() {
    createTableWithGsi(ProjectionType.ALL, null);

    // Put items
    putItem("user-1", "active", "2024-01-01T00:00:00Z", "alice@example.com", "Alice");
    putItem("user-2", "active", "2024-01-02T00:00:00Z", "bob@example.com", "Bob");

    // Verify both in GSI
    QueryResponse response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("active").build()))
        .build());
    assertThat(response.items()).hasSize(2);

    // Delete user-1
    client.deleteItem(software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("userId", AttributeValue.builder().s("user-1").build()))
        .build());

    // Verify only user-2 remains in GSI
    response = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("active").build()))
        .build());
    assertThat(response.items()).hasSize(1);
    assertThat(response.items().get(0).get("userId").s()).isEqualTo("user-2");
  }

  @Test
  void queryNonExistentIndex_throwsError() {
    createTableWithGsi(ProjectionType.ALL, null);

    // Query non-existent index
    assertThatThrownBy(() -> client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName("NonExistentIndex")
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("active").build()))
        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Index not found");
  }

  @Test
  void multipleGsi_bothWork() {
    // Create table with two GSIs
    final CreateTableRequest request = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName("userId").keyType(KeyType.HASH).build()
        )
        .globalSecondaryIndexes(
            GlobalSecondaryIndex.builder()
                .indexName(GSI_NAME)
                .keySchema(
                    KeySchemaElement.builder().attributeName("status").keyType(KeyType.HASH).build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build(),
            GlobalSecondaryIndex.builder()
                .indexName(GSI_NAME_2)
                .keySchema(
                    KeySchemaElement.builder().attributeName("email").keyType(KeyType.HASH).build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build()
        )
        .build();

    client.createTable(request);

    // Put item
    putItem("user-1", "active", "2024-01-01T00:00:00Z", "alice@example.com", "Alice");

    // Query first GSI
    QueryResponse response1 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("active").build()))
        .build());
    assertThat(response1.items()).hasSize(1);

    // Query second GSI
    QueryResponse response2 = client.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME_2)
        .keyConditionExpression("email = :email")
        .expressionAttributeValues(Map.of(":email", AttributeValue.builder().s("alice@example.com").build()))
        .build());
    assertThat(response2.items()).hasSize(1);
  }

  private void createTableWithGsi(final ProjectionType projectionType, final List<String> nonKeyAttributes) {
    final Projection.Builder projectionBuilder = Projection.builder().projectionType(projectionType);
    if (nonKeyAttributes != null && !nonKeyAttributes.isEmpty()) {
      projectionBuilder.nonKeyAttributes(nonKeyAttributes);
    }

    final CreateTableRequest request = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName("userId").keyType(KeyType.HASH).build()
        )
        .globalSecondaryIndexes(
            GlobalSecondaryIndex.builder()
                .indexName(GSI_NAME)
                .keySchema(
                    KeySchemaElement.builder().attributeName("status").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("timestamp").keyType(KeyType.RANGE).build()
                )
                .projection(projectionBuilder.build())
                .build()
        )
        .build();

    client.createTable(request);
  }

  private void putItem(final String userId, final String status, final String timestamp,
                       final String email, final String name) {
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            "userId", AttributeValue.builder().s(userId).build(),
            "status", AttributeValue.builder().s(status).build(),
            "timestamp", AttributeValue.builder().s(timestamp).build(),
            "email", AttributeValue.builder().s(email).build(),
            "name", AttributeValue.builder().s(name).build()
        ))
        .build());
  }
}
