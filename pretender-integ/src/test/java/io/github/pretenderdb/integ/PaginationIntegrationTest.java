package io.github.pretenderdb.integ;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
 * Integration test that validates pagination works identically
 * across DynamoDB Local and Pretender implementations.
 */
class PaginationIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(PaginationIntegrationTest.class);
  private static final String TABLE_NAME = "PaginationTestTable";

  static Stream<DynamoDbProvider> dynamoDbProviders() {
    return Stream.of(
        new DynamoDbLocalProvider(),
        new PretenderPostgresProvider(),
        new PretenderHsqldbProvider()
    );
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testScanWithPagination(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing SCAN with pagination: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write 20 items
      for (int i = 1; i <= 20; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s(String.format("item-%02d", i)).build());
        item.put("value", AttributeValue.builder().n(String.valueOf(i)).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Scan with limit (page size of 5)
      List<Map<String, AttributeValue>> allItems = new ArrayList<>();
      Map<String, AttributeValue> lastEvaluatedKey = null;
      int pageCount = 0;

      do {
        ScanRequest.Builder scanBuilder = ScanRequest.builder()
            .tableName(TABLE_NAME)
            .limit(5);

        if (lastEvaluatedKey != null) {
          scanBuilder.exclusiveStartKey(lastEvaluatedKey);
        }

        ScanResponse response = client.scan(scanBuilder.build());
        allItems.addAll(response.items());
        lastEvaluatedKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
        pageCount++;

        // Each page should have at most 5 items
        assertThat(response.items()).hasSizeLessThanOrEqualTo(5);
      } while (lastEvaluatedKey != null);

      // Should have gotten all 20 items
      // Note: Page count may vary based on implementation internals
      assertThat(allItems).hasSize(20);
      assertThat(pageCount).isGreaterThanOrEqualTo(4);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ SCAN with pagination test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testQueryWithPagination(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing QUERY with pagination: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createTableWithRangeKey(client);

      // Write 15 items with same hash key
      for (int i = 1; i <= 15; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("userId", AttributeValue.builder().s("user-123").build());
        item.put("timestamp", AttributeValue.builder().n(String.valueOf(1000 + i)).build());
        item.put("message", AttributeValue.builder().s("Message " + i).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Query with pagination (page size of 4)
      List<Map<String, AttributeValue>> allItems = new ArrayList<>();
      Map<String, AttributeValue> lastEvaluatedKey = null;
      int pageCount = 0;

      Map<String, AttributeValue> keyConditionValues = new HashMap<>();
      keyConditionValues.put(":userId", AttributeValue.builder().s("user-123").build());

      do {
        QueryRequest.Builder queryBuilder = QueryRequest.builder()
            .tableName(TABLE_NAME)
            .keyConditionExpression("userId = :userId")
            .expressionAttributeValues(keyConditionValues)
            .limit(4);

        if (lastEvaluatedKey != null) {
          queryBuilder.exclusiveStartKey(lastEvaluatedKey);
        }

        QueryResponse response = client.query(queryBuilder.build());
        allItems.addAll(response.items());
        lastEvaluatedKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
        pageCount++;

        assertThat(response.items()).hasSizeLessThanOrEqualTo(4);
      } while (lastEvaluatedKey != null);

      // Should have gotten all 15 items across 4 pages (4+4+4+3)
      assertThat(allItems).hasSize(15);
      assertThat(pageCount).isEqualTo(4);

      // Verify items are in sort key order
      for (int i = 0; i < allItems.size() - 1; i++) {
        int currentTimestamp = Integer.parseInt(allItems.get(i).get("timestamp").n());
        int nextTimestamp = Integer.parseInt(allItems.get(i + 1).get("timestamp").n());
        assertThat(currentTimestamp).isLessThan(nextTimestamp);
      }

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ QUERY with pagination test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testScanWithFilterAndPagination(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing SCAN with filter and pagination: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write 30 items (15 even, 15 odd)
      for (int i = 1; i <= 30; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s(String.format("item-%02d", i)).build());
        item.put("value", AttributeValue.builder().n(String.valueOf(i)).build());
        item.put("category", AttributeValue.builder().s(i % 2 == 0 ? "even" : "odd").build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Scan with filter for "even" category and pagination
      List<Map<String, AttributeValue>> allEvenItems = new ArrayList<>();
      Map<String, AttributeValue> lastEvaluatedKey = null;
      int totalScanned = 0;

      Map<String, AttributeValue> filterValues = new HashMap<>();
      filterValues.put(":category", AttributeValue.builder().s("even").build());

      do {
        ScanRequest.Builder scanBuilder = ScanRequest.builder()
            .tableName(TABLE_NAME)
            .filterExpression("category = :category")
            .expressionAttributeValues(filterValues)
            .limit(10);  // Scan limit (not result limit)

        if (lastEvaluatedKey != null) {
          scanBuilder.exclusiveStartKey(lastEvaluatedKey);
        }

        ScanResponse response = client.scan(scanBuilder.build());
        allEvenItems.addAll(response.items());
        lastEvaluatedKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
        totalScanned += response.scannedCount();
      } while (lastEvaluatedKey != null);

      // Should get 15 even items (filtered from 30 total)
      assertThat(allEvenItems).hasSize(15);
      assertThat(totalScanned).isEqualTo(30);  // All items were scanned

      // Verify all returned items are "even"
      for (Map<String, AttributeValue> item : allEvenItems) {
        assertThat(item.get("category").s()).isEqualTo("even");
      }

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ SCAN with filter and pagination test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testQueryWithRangeKeyConditionAndPagination(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing QUERY with range key condition and pagination: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createTableWithRangeKey(client);

      // Write 20 items
      for (int i = 1; i <= 20; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("userId", AttributeValue.builder().s("user-456").build());
        item.put("timestamp", AttributeValue.builder().n(String.valueOf(2000 + i)).build());
        item.put("data", AttributeValue.builder().s("Data " + i).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Query with range key condition (timestamp >= 2010) and pagination
      List<Map<String, AttributeValue>> allItems = new ArrayList<>();
      Map<String, AttributeValue> lastEvaluatedKey = null;

      Map<String, AttributeValue> keyConditionValues = new HashMap<>();
      keyConditionValues.put(":userId", AttributeValue.builder().s("user-456").build());
      keyConditionValues.put(":minTimestamp", AttributeValue.builder().n("2010").build());

      do {
        QueryRequest.Builder queryBuilder = QueryRequest.builder()
            .tableName(TABLE_NAME)
            .keyConditionExpression("userId = :userId AND #timestamp >= :minTimestamp")
            .expressionAttributeNames(Map.of("#timestamp", "timestamp"))
            .expressionAttributeValues(keyConditionValues)
            .limit(3);

        if (lastEvaluatedKey != null) {
          queryBuilder.exclusiveStartKey(lastEvaluatedKey);
        }

        QueryResponse response = client.query(queryBuilder.build());
        allItems.addAll(response.items());
        lastEvaluatedKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
      } while (lastEvaluatedKey != null);

      // Should get items with timestamp 2010-2020 (11 items)
      assertThat(allItems).hasSize(11);

      // Verify all items meet the range key condition
      for (Map<String, AttributeValue> item : allItems) {
        int timestamp = Integer.parseInt(item.get("timestamp").n());
        assertThat(timestamp).isGreaterThanOrEqualTo(2010);
      }

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ QUERY with range key condition and pagination test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testScanWithProjectionExpressionAndPagination(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing SCAN with projection expression and pagination: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write items with multiple attributes
      for (int i = 1; i <= 12; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("proj-" + i).build());
        item.put("name", AttributeValue.builder().s("Name " + i).build());
        item.put("email", AttributeValue.builder().s("user" + i + "@example.com").build());
        item.put("age", AttributeValue.builder().n(String.valueOf(20 + i)).build());
        item.put("address", AttributeValue.builder().s("Address " + i).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Scan with projection (only id and name) and pagination
      List<Map<String, AttributeValue>> allItems = new ArrayList<>();
      Map<String, AttributeValue> lastEvaluatedKey = null;

      do {
        ScanRequest.Builder scanBuilder = ScanRequest.builder()
            .tableName(TABLE_NAME)
            .projectionExpression("id, #name")
            .expressionAttributeNames(Map.of("#name", "name"))
            .limit(5);

        if (lastEvaluatedKey != null) {
          scanBuilder.exclusiveStartKey(lastEvaluatedKey);
        }

        ScanResponse response = client.scan(scanBuilder.build());
        allItems.addAll(response.items());
        lastEvaluatedKey = response.hasLastEvaluatedKey() ? response.lastEvaluatedKey() : null;
      } while (lastEvaluatedKey != null);

      assertThat(allItems).hasSize(12);

      // Verify projection - each item should only have id and name
      for (Map<String, AttributeValue> item : allItems) {
        assertThat(item).containsKeys("id", "name");
        assertThat(item).doesNotContainKeys("email", "age", "address");
      }

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ SCAN with projection expression and pagination test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

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

  private void createTableWithRangeKey(DynamoDbClient client) {
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
                .attributeType(ScalarAttributeType.N)
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
