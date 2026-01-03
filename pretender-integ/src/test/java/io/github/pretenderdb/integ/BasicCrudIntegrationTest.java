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
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
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
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/**
 * Integration test that validates basic CRUD operations work identically
 * across DynamoDB Local and Pretender with PostgreSQL.
 * <p>
 * Each test runs against both implementations to ensure compatibility.
 */
class BasicCrudIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(BasicCrudIntegrationTest.class);
  private static final String TABLE_NAME = "IntegrationTestTable";

  /**
   * Provides all DynamoDB implementations to test against.
   */
  static Stream<DynamoDbProvider> dynamoDbProviders() {
    return Stream.of(
        new DynamoDbLocalProvider(),
        new PretenderPostgresProvider(),
        new PretenderHsqldbProvider()
    );
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testCreateTable(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing CREATE TABLE with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table with hash key only
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
                  .build()
          )
          .provisionedThroughput(ProvisionedThroughput.builder()
              .readCapacityUnits(5L)
              .writeCapacityUnits(5L)
              .build())
          .build();

      client.createTable(request);

      // Verify table exists by attempting to describe it (we'll use putItem to verify)
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("test-id").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());

      log.info("✓ CREATE TABLE test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testPutItemAndGetItem(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing PUT ITEM and GET ITEM with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table
      createSimpleTable(client);

      // Put item with multiple attributes
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("user-123").build());
      item.put("name", AttributeValue.builder().s("John Doe").build());
      item.put("age", AttributeValue.builder().n("30").build());
      item.put("active", AttributeValue.builder().bool(true).build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Get item
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("user-123").build());

      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      // Verify all attributes
      assertThat(response.hasItem()).isTrue();
      assertThat(response.item()).containsEntry("id", AttributeValue.builder().s("user-123").build());
      assertThat(response.item()).containsEntry("name", AttributeValue.builder().s("John Doe").build());
      assertThat(response.item()).containsEntry("age", AttributeValue.builder().n("30").build());
      assertThat(response.item()).containsEntry("active", AttributeValue.builder().bool(true).build());

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());

      log.info("✓ PUT ITEM and GET ITEM test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testUpdateItem(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing UPDATE ITEM with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table and insert initial item
      createSimpleTable(client);

      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("user-456").build());
      item.put("name", AttributeValue.builder().s("Jane Doe").build());
      item.put("score", AttributeValue.builder().n("100").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Update item using UpdateExpression
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("user-456").build());

      Map<String, AttributeValue> expressionValues = new HashMap<>();
      expressionValues.put(":newScore", AttributeValue.builder().n("150").build());
      expressionValues.put(":bonus", AttributeValue.builder().n("25").build());

      UpdateItemResponse updateResponse = client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("SET score = :newScore, bonus = :bonus")
          .expressionAttributeValues(expressionValues)
          .build());

      // Get updated item
      GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(getResponse.item()).containsEntry("score", AttributeValue.builder().n("150").build());
      assertThat(getResponse.item()).containsEntry("bonus", AttributeValue.builder().n("25").build());

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());

      log.info("✓ UPDATE ITEM test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testDeleteItem(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing DELETE ITEM with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table and insert item
      createSimpleTable(client);

      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("user-789").build());
      item.put("name", AttributeValue.builder().s("Bob Smith").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Verify item exists
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("user-789").build());

      GetItemResponse beforeDelete = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(beforeDelete.hasItem()).isTrue();

      // Delete item
      client.deleteItem(DeleteItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      // Verify item is gone
      GetItemResponse afterDelete = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(afterDelete.hasItem()).isFalse();

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());

      log.info("✓ DELETE ITEM test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testQueryWithHashAndRangeKey(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing QUERY with hash and range key: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table with hash and range key
      CreateTableRequest request = CreateTableRequest.builder()
          .tableName(TABLE_NAME)
          .keySchema(
              KeySchemaElement.builder()
                  .attributeName("userId")
                  .keyType(KeyType.HASH)
                  .build(),
              KeySchemaElement.builder()
                  .attributeName("timestamp")
                  .keyType(KeyType.RANGE)
                  .build()
          )
          .attributeDefinitions(
              AttributeDefinition.builder()
                  .attributeName("userId")
                  .attributeType(ScalarAttributeType.S)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("timestamp")
                  .attributeType(ScalarAttributeType.S)
                  .build()
          )
          .provisionedThroughput(ProvisionedThroughput.builder()
              .readCapacityUnits(5L)
              .writeCapacityUnits(5L)
              .build())
          .build();

      client.createTable(request);

      // Insert multiple items
      for (int i = 1; i <= 5; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("userId", AttributeValue.builder().s("user-123").build());
        item.put("timestamp", AttributeValue.builder().s("2024-01-0" + i).build());
        item.put("event", AttributeValue.builder().s("Event " + i).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Query items for specific user
      Map<String, AttributeValue> expressionValues = new HashMap<>();
      expressionValues.put(":userId", AttributeValue.builder().s("user-123").build());

      QueryResponse queryResponse = client.query(QueryRequest.builder()
          .tableName(TABLE_NAME)
          .keyConditionExpression("userId = :userId")
          .expressionAttributeValues(expressionValues)
          .build());

      assertThat(queryResponse.count()).isEqualTo(5);
      assertThat(queryResponse.items()).hasSize(5);

      // Verify items are returned in sort key order
      for (int i = 0; i < 5; i++) {
        assertThat(queryResponse.items().get(i))
            .containsEntry("timestamp", AttributeValue.builder().s("2024-01-0" + (i + 1)).build());
      }

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());

      log.info("✓ QUERY test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testScan(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing SCAN with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table
      createSimpleTable(client);

      // Insert multiple items
      for (int i = 1; i <= 10; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("item-" + i).build());
        item.put("value", AttributeValue.builder().n(String.valueOf(i * 10)).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Scan all items
      ScanResponse scanResponse = client.scan(ScanRequest.builder()
          .tableName(TABLE_NAME)
          .build());

      assertThat(scanResponse.count()).isEqualTo(10);
      assertThat(scanResponse.items()).hasSize(10);

      // Clean up
      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());

      log.info("✓ SCAN test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  /**
   * Helper method to create a simple table with just a hash key.
   */
  private void createSimpleTable(DynamoDbClient client) {
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
