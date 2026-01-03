package io.github.pretenderdb.integ;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
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
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * Integration test that validates Global Secondary Index operations work identically
 * across DynamoDB Local and Pretender implementations.
 */
class GsiIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(GsiIntegrationTest.class);
  private static final String TABLE_NAME = "GsiTestTable";
  private static final String GSI_NAME = "StatusIndex";

  static Stream<DynamoDbProvider> dynamoDbProviders() {
    return Stream.of(
        new DynamoDbLocalProvider(),
        new PretenderPostgresProvider(),
        new PretenderHsqldbProvider()
    );
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testCreateTableWithGsi(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing CREATE TABLE with GSI: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table with GSI
      CreateTableRequest request = CreateTableRequest.builder()
          .tableName(TABLE_NAME)
          .keySchema(
              KeySchemaElement.builder()
                  .attributeName("id")
                  .keyType(KeyType.HASH)
                  .build()
          )
          .attributeDefinitions(
              AttributeDefinition.builder()
                  .attributeName("id")
                  .attributeType(ScalarAttributeType.S)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("status")
                  .attributeType(ScalarAttributeType.S)
                  .build()
          )
          .globalSecondaryIndexes(
              GlobalSecondaryIndex.builder()
                  .indexName(GSI_NAME)
                  .keySchema(
                      KeySchemaElement.builder()
                          .attributeName("status")
                          .keyType(KeyType.HASH)
                          .build()
                  )
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

      client.createTable(request);

      // Write an item and query via GSI
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("test-id").build());
      item.put("status", AttributeValue.builder().s("active").build());
      item.put("name", AttributeValue.builder().s("Test Item").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Query via GSI
      Map<String, AttributeValue> expressionValues = new HashMap<>();
      expressionValues.put(":status", AttributeValue.builder().s("active").build());

      QueryResponse queryResponse = client.query(QueryRequest.builder()
          .tableName(TABLE_NAME)
          .indexName(GSI_NAME)
          .keyConditionExpression("#status = :status")
          .expressionAttributeNames(Map.of("#status", "status"))
          .expressionAttributeValues(expressionValues)
          .build());

      assertThat(queryResponse.count()).isEqualTo(1);
      assertThat(queryResponse.items().get(0))
          .containsEntry("id", AttributeValue.builder().s("test-id").build());

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ CREATE TABLE with GSI test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testQueryGsiWithMultipleItems(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing QUERY GSI with multiple items: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createTableWithGsi(client);

      // Write items with different statuses
      String[] statuses = {"active", "active", "pending", "active", "inactive", "pending"};
      for (int i = 0; i < statuses.length; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("item-" + i).build());
        item.put("status", AttributeValue.builder().s(statuses[i]).build());
        item.put("value", AttributeValue.builder().n(String.valueOf(i * 10)).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Query for active items via GSI
      Map<String, AttributeValue> expressionValues = new HashMap<>();
      expressionValues.put(":status", AttributeValue.builder().s("active").build());

      QueryResponse queryResponse = client.query(QueryRequest.builder()
          .tableName(TABLE_NAME)
          .indexName(GSI_NAME)
          .keyConditionExpression("#status = :status")
          .expressionAttributeNames(Map.of("#status", "status"))
          .expressionAttributeValues(expressionValues)
          .build());

      // Should get 3 active items
      assertThat(queryResponse.count()).isEqualTo(3);
      assertThat(queryResponse.items()).hasSize(3);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ QUERY GSI with multiple items test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testGsiWithHashAndRangeKey(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing GSI with hash and range key: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table with GSI that has both hash and range key
      CreateTableRequest request = CreateTableRequest.builder()
          .tableName(TABLE_NAME)
          .keySchema(
              KeySchemaElement.builder()
                  .attributeName("id")
                  .keyType(KeyType.HASH)
                  .build()
          )
          .attributeDefinitions(
              AttributeDefinition.builder()
                  .attributeName("id")
                  .attributeType(ScalarAttributeType.S)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("category")
                  .attributeType(ScalarAttributeType.S)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("timestamp")
                  .attributeType(ScalarAttributeType.S)
                  .build()
          )
          .globalSecondaryIndexes(
              GlobalSecondaryIndex.builder()
                  .indexName("CategoryTimestampIndex")
                  .keySchema(
                      KeySchemaElement.builder()
                          .attributeName("category")
                          .keyType(KeyType.HASH)
                          .build(),
                      KeySchemaElement.builder()
                          .attributeName("timestamp")
                          .keyType(KeyType.RANGE)
                          .build()
                  )
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

      client.createTable(request);

      // Write items with same category but different timestamps
      for (int i = 1; i <= 5; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("event-" + i).build());
        item.put("category", AttributeValue.builder().s("sports").build());
        item.put("timestamp", AttributeValue.builder().s("2024-01-0" + i).build());
        item.put("description", AttributeValue.builder().s("Event " + i).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Query GSI with hash key only
      Map<String, AttributeValue> hashOnlyValues = new HashMap<>();
      hashOnlyValues.put(":category", AttributeValue.builder().s("sports").build());

      QueryResponse allSportsResponse = client.query(QueryRequest.builder()
          .tableName(TABLE_NAME)
          .indexName("CategoryTimestampIndex")
          .keyConditionExpression("category = :category")
          .expressionAttributeValues(hashOnlyValues)
          .build());

      assertThat(allSportsResponse.count()).isEqualTo(5);

      // Query GSI with hash and range key condition
      Map<String, AttributeValue> rangeValues = new HashMap<>();
      rangeValues.put(":category", AttributeValue.builder().s("sports").build());
      rangeValues.put(":startDate", AttributeValue.builder().s("2024-01-03").build());

      QueryResponse filteredResponse = client.query(QueryRequest.builder()
          .tableName(TABLE_NAME)
          .indexName("CategoryTimestampIndex")
          .keyConditionExpression("category = :category AND #timestamp >= :startDate")
          .expressionAttributeNames(Map.of("#timestamp", "timestamp"))
          .expressionAttributeValues(rangeValues)
          .build());

      assertThat(filteredResponse.count()).isEqualTo(3);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ GSI with hash and range key test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testGsiProjectionKeysOnly(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing GSI with KEYS_ONLY projection: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table with KEYS_ONLY projection GSI
      CreateTableRequest request = CreateTableRequest.builder()
          .tableName(TABLE_NAME)
          .keySchema(
              KeySchemaElement.builder()
                  .attributeName("id")
                  .keyType(KeyType.HASH)
                  .build()
          )
          .attributeDefinitions(
              AttributeDefinition.builder()
                  .attributeName("id")
                  .attributeType(ScalarAttributeType.S)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("status")
                  .attributeType(ScalarAttributeType.S)
                  .build()
          )
          .globalSecondaryIndexes(
              GlobalSecondaryIndex.builder()
                  .indexName(GSI_NAME)
                  .keySchema(
                      KeySchemaElement.builder()
                          .attributeName("status")
                          .keyType(KeyType.HASH)
                          .build()
                  )
                  .projection(Projection.builder()
                      .projectionType(ProjectionType.KEYS_ONLY)
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

      client.createTable(request);

      // Write item with multiple attributes
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("keys-test").build());
      item.put("status", AttributeValue.builder().s("active").build());
      item.put("name", AttributeValue.builder().s("Should not be in GSI").build());
      item.put("description", AttributeValue.builder().s("Extra data").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Query GSI
      Map<String, AttributeValue> expressionValues = new HashMap<>();
      expressionValues.put(":status", AttributeValue.builder().s("active").build());

      QueryResponse queryResponse = client.query(QueryRequest.builder()
          .tableName(TABLE_NAME)
          .indexName(GSI_NAME)
          .keyConditionExpression("#status = :status")
          .expressionAttributeNames(Map.of("#status", "status"))
          .expressionAttributeValues(expressionValues)
          .build());

      assertThat(queryResponse.count()).isEqualTo(1);

      // With KEYS_ONLY projection, only table keys and GSI keys are returned
      Map<String, AttributeValue> resultItem = queryResponse.items().get(0);
      assertThat(resultItem).containsKey("id");  // table key
      assertThat(resultItem).containsKey("status");  // GSI key
      assertThat(resultItem).doesNotContainKey("name");  // not projected
      assertThat(resultItem).doesNotContainKey("description");  // not projected

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ GSI with KEYS_ONLY projection test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testGsiUpdateOnItemModification(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing GSI update on item modification: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createTableWithGsi(client);

      // Write initial item
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("update-test").build());
      item.put("status", AttributeValue.builder().s("pending").build());
      item.put("value", AttributeValue.builder().n("100").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Verify item appears in GSI under "pending" status
      QueryResponse pendingQuery = client.query(QueryRequest.builder()
          .tableName(TABLE_NAME)
          .indexName(GSI_NAME)
          .keyConditionExpression("#status = :status")
          .expressionAttributeNames(Map.of("#status", "status"))
          .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("pending").build()))
          .build());

      assertThat(pendingQuery.count()).isEqualTo(1);

      // Update item status (GSI key change)
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("update-test").build());

      Map<String, AttributeValue> updateValues = new HashMap<>();
      updateValues.put(":newStatus", AttributeValue.builder().s("active").build());

      client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("SET #status = :newStatus")
          .expressionAttributeNames(Map.of("#status", "status"))
          .expressionAttributeValues(updateValues)
          .build());

      // Verify item no longer appears under "pending"
      QueryResponse pendingAfterUpdate = client.query(QueryRequest.builder()
          .tableName(TABLE_NAME)
          .indexName(GSI_NAME)
          .keyConditionExpression("#status = :status")
          .expressionAttributeNames(Map.of("#status", "status"))
          .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("pending").build()))
          .build());

      assertThat(pendingAfterUpdate.count()).isEqualTo(0);

      // Verify item now appears under "active"
      QueryResponse activeQuery = client.query(QueryRequest.builder()
          .tableName(TABLE_NAME)
          .indexName(GSI_NAME)
          .keyConditionExpression("#status = :status")
          .expressionAttributeNames(Map.of("#status", "status"))
          .expressionAttributeValues(Map.of(":status", AttributeValue.builder().s("active").build()))
          .build());

      assertThat(activeQuery.count()).isEqualTo(1);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ GSI update on item modification test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  private void createTableWithGsi(DynamoDbClient client) {
    CreateTableRequest request = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder()
                .attributeName("id")
                .keyType(KeyType.HASH)
                .build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder()
                .attributeName("id")
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName("status")
                .attributeType(ScalarAttributeType.S)
                .build()
        )
        .globalSecondaryIndexes(
            GlobalSecondaryIndex.builder()
                .indexName(GSI_NAME)
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName("status")
                        .keyType(KeyType.HASH)
                        .build()
                )
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

    client.createTable(request);
  }
}
