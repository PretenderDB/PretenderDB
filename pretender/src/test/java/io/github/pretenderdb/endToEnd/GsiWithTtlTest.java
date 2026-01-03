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
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * End-to-end tests for combined GSI and TTL functionality.
 */
public class GsiWithTtlTest extends BaseEndToEndTest {

  private static final String TABLE_NAME = "GsiTtlTestTable";
  private static final String GSI_NAME = "StatusIndex";
  private static final String TTL_ATTRIBUTE_NAME = "expirationTime";

  private software.amazon.awssdk.services.dynamodb.DynamoDbClient client;

  @BeforeEach
  void setup() {
    client = component.dynamoDbPretenderClient();
  }

  @Test
  void queryGsi_filtersExpiredItems() {
    // Create table with GSI and enable TTL
    createTableWithGsiAndTtl();

    final long expiredTimestamp = Instant.now().getEpochSecond() - 3600;
    final long futureTimestamp = Instant.now().getEpochSecond() + 3600;

    // Put items with same status (GSI hash key) but different TTL
    putItemWithStatusAndTtl("user1", "ACTIVE", expiredTimestamp, "expired user 1");
    putItemWithStatusAndTtl("user2", "ACTIVE", futureTimestamp, "active user 2");
    putItemWithStatusAndTtl("user3", "ACTIVE", expiredTimestamp, "expired user 3");
    putItemWithStatusAndTtl("user4", "ACTIVE", futureTimestamp, "active user 4");

    // Query GSI by status - should only return non-expired items
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("ACTIVE").build()
        ))
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);

    assertThat(queryResponse.items()).hasSize(2);
    assertThat(queryResponse.items())
        .extracting(item -> item.get("userId").s())
        .containsExactlyInAnyOrder("user2", "user4");
  }

  @Test
  void expiredItemDeletedFromBothMainAndGsiTables() {
    // Create table with GSI and enable TTL
    createTableWithGsiAndTtl();

    final long expiredTimestamp = Instant.now().getEpochSecond() - 3600;

    // Put expired item
    putItemWithStatusAndTtl("expired-user", "ACTIVE", expiredTimestamp, "will be deleted");

    // Get item from main table - should trigger deletion
    final GetItemRequest getRequest = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("userId", AttributeValue.builder().s("expired-user").build()))
        .build();

    final GetItemResponse getResponse = client.getItem(getRequest);
    assertThat(getResponse.hasItem()).isFalse();

    // Query GSI - should also not find the item
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("ACTIVE").build()
        ))
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);
    assertThat(queryResponse.items()).isEmpty();
  }

  @Test
  void gsiWithKeysOnlyProjection_onlyIncludesKeys() {
    // Create table with KEYS_ONLY GSI projection
    createTableWithKeysOnlyGsi();

    final long futureTimestamp = Instant.now().getEpochSecond() + 3600;

    // Put item
    putItemWithStatusAndTtl("user1", "ACTIVE", futureTimestamp, "test data");

    // Query GSI - should only return key attributes (userId, status)
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("ACTIVE").build()
        ))
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);

    assertThat(queryResponse.items()).hasSize(1);
    final Map<String, AttributeValue> item = queryResponse.items().get(0);

    // Should have keys only
    assertThat(item).containsKey("userId");
    assertThat(item).containsKey("status");

    // Should NOT have non-key attributes (including TTL)
    assertThat(item).doesNotContainKey("data");
    // Note: TTL attribute is not a key, so it's not included in KEYS_ONLY projection
    // This matches DynamoDB behavior where KEYS_ONLY strictly means only key attributes
  }

  @Test
  void gsiWithAllProjection_includesAllAttributesIncludingTtl() {
    // Create table with ALL projection GSI
    createTableWithGsiAndTtl();

    final long futureTimestamp = Instant.now().getEpochSecond() + 3600;

    // Put item
    putItemWithStatusAndTtl("user1", "ACTIVE", futureTimestamp, "test data");

    // Query GSI - should return all attributes
    final QueryRequest queryRequest = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName(GSI_NAME)
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("ACTIVE").build()
        ))
        .build();

    final QueryResponse queryResponse = client.query(queryRequest);

    assertThat(queryResponse.items()).hasSize(1);
    final Map<String, AttributeValue> item = queryResponse.items().get(0);

    assertThat(item).containsKey("userId");
    assertThat(item).containsKey("status");
    assertThat(item).containsKey(TTL_ATTRIBUTE_NAME);
    assertThat(item).containsKey("data");
  }

  @Test
  void multipleGsis_eachFiltersExpiredItems() {
    // Create table with two GSIs
    createTableWithMultipleGsisAndTtl();

    final long expiredTimestamp = Instant.now().getEpochSecond() - 3600;
    final long futureTimestamp = Instant.now().getEpochSecond() + 3600;

    // Put items with different status and category
    putItemWithStatusCategoryAndTtl("user1", "ACTIVE", "PREMIUM", futureTimestamp);
    putItemWithStatusCategoryAndTtl("user2", "ACTIVE", "BASIC", expiredTimestamp);
    putItemWithStatusCategoryAndTtl("user3", "INACTIVE", "PREMIUM", futureTimestamp);
    putItemWithStatusCategoryAndTtl("user4", "INACTIVE", "BASIC", expiredTimestamp);

    // Query first GSI (status) - should only return non-expired items
    final QueryRequest statusQuery = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName("StatusIndex")
        .keyConditionExpression("status = :status")
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("ACTIVE").build()
        ))
        .build();

    final QueryResponse statusResponse = client.query(statusQuery);
    assertThat(statusResponse.items()).hasSize(1);
    assertThat(statusResponse.items().get(0).get("userId").s()).isEqualTo("user1");

    // Query second GSI (category) - should only return non-expired items
    final QueryRequest categoryQuery = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName("CategoryIndex")
        .keyConditionExpression("category = :category")
        .expressionAttributeValues(Map.of(
            ":category", AttributeValue.builder().s("PREMIUM").build()
        ))
        .build();

    final QueryResponse categoryResponse = client.query(categoryQuery);
    assertThat(categoryResponse.items()).hasSize(2);
    assertThat(categoryResponse.items())
        .extracting(item -> item.get("userId").s())
        .containsExactlyInAnyOrder("user1", "user3");
  }

  // Helper methods

  private void createTableWithGsiAndTtl() {
    final CreateTableRequest createRequest = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(KeySchemaElement.builder()
            .attributeName("userId")
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(
            AttributeDefinition.builder()
                .attributeName("userId")
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName("status")
                .attributeType(ScalarAttributeType.S)
                .build()
        )
        .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
            .indexName(GSI_NAME)
            .keySchema(KeySchemaElement.builder()
                .attributeName("status")
                .keyType(KeyType.HASH)
                .build())
            .projection(Projection.builder()
                .projectionType(ProjectionType.ALL)
                .build())
            .provisionedThroughput(ProvisionedThroughput.builder()
                .readCapacityUnits(5L)
                .writeCapacityUnits(5L)
                .build())
            .build())
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(5L)
            .writeCapacityUnits(5L)
            .build())
        .build();

    client.createTable(createRequest);
    component.pdbTableManager().enableTtl(TABLE_NAME, TTL_ATTRIBUTE_NAME);
  }

  private void createTableWithKeysOnlyGsi() {
    final CreateTableRequest createRequest = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(KeySchemaElement.builder()
            .attributeName("userId")
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(
            AttributeDefinition.builder()
                .attributeName("userId")
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName("status")
                .attributeType(ScalarAttributeType.S)
                .build()
        )
        .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
            .indexName(GSI_NAME)
            .keySchema(KeySchemaElement.builder()
                .attributeName("status")
                .keyType(KeyType.HASH)
                .build())
            .projection(Projection.builder()
                .projectionType(ProjectionType.KEYS_ONLY)
                .build())
            .provisionedThroughput(ProvisionedThroughput.builder()
                .readCapacityUnits(5L)
                .writeCapacityUnits(5L)
                .build())
            .build())
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(5L)
            .writeCapacityUnits(5L)
            .build())
        .build();

    client.createTable(createRequest);
    component.pdbTableManager().enableTtl(TABLE_NAME, TTL_ATTRIBUTE_NAME);
  }

  private void createTableWithMultipleGsisAndTtl() {
    final CreateTableRequest createRequest = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(KeySchemaElement.builder()
            .attributeName("userId")
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(
            AttributeDefinition.builder()
                .attributeName("userId")
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName("status")
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName("category")
                .attributeType(ScalarAttributeType.S)
                .build()
        )
        .globalSecondaryIndexes(
            GlobalSecondaryIndex.builder()
                .indexName("StatusIndex")
                .keySchema(KeySchemaElement.builder()
                    .attributeName("status")
                    .keyType(KeyType.HASH)
                    .build())
                .projection(Projection.builder()
                    .projectionType(ProjectionType.ALL)
                    .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                    .readCapacityUnits(5L)
                    .writeCapacityUnits(5L)
                    .build())
                .build(),
            GlobalSecondaryIndex.builder()
                .indexName("CategoryIndex")
                .keySchema(KeySchemaElement.builder()
                    .attributeName("category")
                    .keyType(KeyType.HASH)
                    .build())
                .projection(Projection.builder()
                    .projectionType(ProjectionType.ALL)
                    .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                    .readCapacityUnits(5L)
                    .writeCapacityUnits(5L)
                    .build())
                .build()
        )
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(5L)
            .writeCapacityUnits(5L)
            .build())
        .build();

    client.createTable(createRequest);
    component.pdbTableManager().enableTtl(TABLE_NAME, TTL_ATTRIBUTE_NAME);
  }

  private void putItemWithStatusAndTtl(final String userId, final String status,
                                       final long ttlTimestamp, final String data) {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("userId", AttributeValue.builder().s(userId).build());
    item.put("status", AttributeValue.builder().s(status).build());
    item.put(TTL_ATTRIBUTE_NAME, AttributeValue.builder().n(String.valueOf(ttlTimestamp)).build());
    item.put("data", AttributeValue.builder().s(data).build());

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    client.putItem(putRequest);
  }

  private void putItemWithStatusCategoryAndTtl(final String userId, final String status,
                                               final String category, final long ttlTimestamp) {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("userId", AttributeValue.builder().s(userId).build());
    item.put("status", AttributeValue.builder().s(status).build());
    item.put("category", AttributeValue.builder().s(category).build());
    item.put(TTL_ATTRIBUTE_NAME, AttributeValue.builder().n(String.valueOf(ttlTimestamp)).build());

    final PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    client.putItem(putRequest);
  }
}
