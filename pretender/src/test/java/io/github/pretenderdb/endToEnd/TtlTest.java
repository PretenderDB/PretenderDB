package io.github.pretenderdb.endToEnd;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Integration tests for Time-To-Live (TTL) functionality.
 */
public class TtlTest extends BaseEndToEndTest {

  private static final String TABLE_NAME = "TtlTestTable";
  private static final String TTL_ATTRIBUTE_NAME = "expirationTime";

  private software.amazon.awssdk.services.dynamodb.DynamoDbClient client;

  @BeforeEach
  void setup() {
    client = component.dynamoDbPretenderClient();
  }

  @Test
  void getItem_expiredItem_returnsEmpty() {
    // Create table with TTL enabled
    createTableWithTtl();

    // Put item with TTL in the past (expired)
    final long expiredTimestamp = Instant.now().getEpochSecond() - 3600; // 1 hour ago
    putItemWithTtl("expired-item", expiredTimestamp);

    // Get item - should return empty
    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("id", AttributeValue.builder().s("expired-item").build()))
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);

    assertThat(getResponse.hasItem()).isFalse();
  }

  @Test
  void getItem_nonExpiredItem_returnsItem() {
    // Create table with TTL enabled
    createTableWithTtl();

    // Put item with TTL in the future (not expired)
    final long futureTimestamp = Instant.now().getEpochSecond() + 3600; // 1 hour from now
    putItemWithTtl("active-item", futureTimestamp);

    // Get item - should return the item
    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("id", AttributeValue.builder().s("active-item").build()))
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);

    assertThat(getResponse.hasItem()).isTrue();
    assertThat(getResponse.item()).containsKey("id");
    assertThat(getResponse.item().get("id").s()).isEqualTo("active-item");
  }

  @Test
  void query_filtersExpiredItems() {
    // Create table with hash and sort key, TTL enabled
    createTableWithSortKeyAndTtl();

    final long expiredTimestamp = Instant.now().getEpochSecond() - 3600;
    final long futureTimestamp = Instant.now().getEpochSecond() + 3600;

    // Put multiple items with same hash key
    putItemWithTtlAndSortKey("user1", "item1", expiredTimestamp);
    putItemWithTtlAndSortKey("user1", "item2", futureTimestamp);
    putItemWithTtlAndSortKey("user1", "item3", expiredTimestamp);
    putItemWithTtlAndSortKey("user1", "item4", futureTimestamp);

    // Query - should only return non-expired items
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("userId = :userId")
        .expressionAttributeValues(Map.of(
            ":userId", AttributeValue.builder().s("user1").build()
        ))
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);

    assertThat(queryResponse.items()).hasSize(2);
    assertThat(queryResponse.items())
        .extracting(item -> item.get("itemId").s())
        .containsExactlyInAnyOrder("item2", "item4");
  }

  @Test
  void scan_filtersExpiredItems() {
    // Create table with TTL enabled
    createTableWithTtl();

    final long expiredTimestamp = Instant.now().getEpochSecond() - 3600;
    final long futureTimestamp = Instant.now().getEpochSecond() + 3600;

    // Put items
    putItemWithTtl("expired1", expiredTimestamp);
    putItemWithTtl("active1", futureTimestamp);
    putItemWithTtl("expired2", expiredTimestamp);
    putItemWithTtl("active2", futureTimestamp);

    // Scan - should only return non-expired items
    final ScanRequest scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .build();

    final ScanResponse scanResponse = client.scan(scanRequest);

    assertThat(scanResponse.items()).hasSize(2);
    assertThat(scanResponse.items())
        .extracting(item -> item.get("id").s())
        .containsExactlyInAnyOrder("active1", "active2");
  }

  @Test
  void expiredItem_deletedOnRead() {
    // Create table with TTL enabled
    createTableWithTtl();

    // Put item with TTL in the past
    final long expiredTimestamp = Instant.now().getEpochSecond() - 3600;
    putItemWithTtl("to-delete", expiredTimestamp);

    // First get - should return empty and delete the item
    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("id", AttributeValue.builder().s("to-delete").build()))
        .build();

    final GetItemResponse firstGet = client.getItem(getRequest);
    assertThat(firstGet.hasItem()).isFalse();

    // Second get - should still return empty (item was deleted)
    final GetItemResponse secondGet = client.getItem(getRequest);
    assertThat(secondGet.hasItem()).isFalse();
  }

  @Test
  void itemWithoutTtlAttribute_doesNotExpire() {
    // Create table with TTL enabled
    createTableWithTtl();

    // Put item without TTL attribute
    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            "id", AttributeValue.builder().s("no-ttl-item").build(),
            "data", AttributeValue.builder().s("some data").build()
        ))
        .build();

    client.putItem(putRequest);

    // Get item - should return the item
    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("id", AttributeValue.builder().s("no-ttl-item").build()))
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);

    assertThat(getResponse.hasItem()).isTrue();
    assertThat(getResponse.item().get("id").s()).isEqualTo("no-ttl-item");
  }

  @Test
  void ttlDisabled_itemsDoNotExpire() {
    // Create table without TTL enabled
    final CreateTableRequest createRequest = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(KeySchemaElement.builder()
            .attributeName("id")
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build())
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(5L)
            .writeCapacityUnits(5L)
            .build())
        .build();

    client.createTable(createRequest);

    // Put item with TTL in the past (but TTL is disabled on table)
    final long expiredTimestamp = Instant.now().getEpochSecond() - 3600;
    putItemWithTtl("should-not-expire", expiredTimestamp);

    // Get item - should still return the item because TTL is disabled
    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("id", AttributeValue.builder().s("should-not-expire").build()))
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);

    assertThat(getResponse.hasItem()).isTrue();
    assertThat(getResponse.item().get("id").s()).isEqualTo("should-not-expire");
  }

  @Test
  void invalidTtlValue_handledGracefully() {
    // Create table with TTL enabled
    createTableWithTtl();

    // Put item with invalid TTL (not a number)
    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            "id", AttributeValue.builder().s("invalid-ttl").build(),
            TTL_ATTRIBUTE_NAME, AttributeValue.builder().s("not-a-number").build()
        ))
        .build();

    client.putItem(putRequest);

    // Get item - should return the item (invalid TTL is ignored)
    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("id", AttributeValue.builder().s("invalid-ttl").build()))
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);

    assertThat(getResponse.hasItem()).isTrue();
  }

  // Helper methods

  private void createTableWithTtl() {
    final CreateTableRequest createRequest = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(KeySchemaElement.builder()
            .attributeName("id")
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build())
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(5L)
            .writeCapacityUnits(5L)
            .build())
        .build();

    client.createTable(createRequest);

    // Enable TTL (in a real implementation, this would be UpdateTimeToLive API)
    // For now, we'll manually update the metadata
    enableTtlOnTable(TABLE_NAME, TTL_ATTRIBUTE_NAME);
  }

  private void createTableWithSortKeyAndTtl() {
    final CreateTableRequest createRequest = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder()
                .attributeName("userId")
                .keyType(KeyType.HASH)
                .build(),
            KeySchemaElement.builder()
                .attributeName("itemId")
                .keyType(KeyType.RANGE)
                .build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder()
                .attributeName("userId")
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName("itemId")
                .attributeType(ScalarAttributeType.S)
                .build()
        )
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(5L)
            .writeCapacityUnits(5L)
            .build())
        .build();

    client.createTable(createRequest);
    enableTtlOnTable(TABLE_NAME, TTL_ATTRIBUTE_NAME);
  }

  private void putItemWithTtl(final String id, final long ttlTimestamp) {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s(id).build());
    item.put(TTL_ATTRIBUTE_NAME, AttributeValue.builder().n(String.valueOf(ttlTimestamp)).build());
    item.put("data", AttributeValue.builder().s("test data").build());

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    client.putItem(putRequest);
  }

  private void putItemWithTtlAndSortKey(final String userId, final String itemId, final long ttlTimestamp) {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("userId", AttributeValue.builder().s(userId).build());
    item.put("itemId", AttributeValue.builder().s(itemId).build());
    item.put(TTL_ATTRIBUTE_NAME, AttributeValue.builder().n(String.valueOf(ttlTimestamp)).build());
    item.put("data", AttributeValue.builder().s("test data").build());

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    client.putItem(putRequest);
  }

  private void enableTtlOnTable(final String tableName, final String ttlAttributeName) {
    // Access the internal PdbTableManager to update TTL settings
    // This simulates the UpdateTimeToLive API call
    component.pdbTableManager().enableTtl(tableName, ttlAttributeName);
  }
}
