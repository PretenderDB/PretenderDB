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
 * Integration test that validates filter expressions work identically
 * across DynamoDB Local and Pretender implementations.
 */
class FilterExpressionsIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(FilterExpressionsIntegrationTest.class);
  private static final String TABLE_NAME = "FilterTestTable";

  static Stream<DynamoDbProvider> dynamoDbProviders() {
    return Stream.of(
        new DynamoDbLocalProvider(),
        new PretenderPostgresProvider(),
        new PretenderHsqldbProvider()
    );
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testScanWithSimpleFilterExpression(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing SCAN with simple filter expression: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write items with different ages
      for (int i = 1; i <= 10; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("user-" + i).build());
        item.put("age", AttributeValue.builder().n(String.valueOf(20 + i)).build());
        item.put("name", AttributeValue.builder().s("User " + i).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Scan with filter: age >= 25
      Map<String, AttributeValue> filterValues = new HashMap<>();
      filterValues.put(":minAge", AttributeValue.builder().n("25").build());

      ScanResponse scanResponse = client.scan(ScanRequest.builder()
          .tableName(TABLE_NAME)
          .filterExpression("age >= :minAge")
          .expressionAttributeValues(filterValues)
          .build());

      // Should get users with age 25-30 (users 5-10 = 6 items)
      assertThat(scanResponse.count()).isEqualTo(6);
      assertThat(scanResponse.scannedCount()).isEqualTo(10);  // All items scanned

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ SCAN with simple filter test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testQueryWithFilterExpression(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing QUERY with filter expression: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createTableWithRangeKey(client);

      // Write items
      for (int i = 1; i <= 8; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("userId", AttributeValue.builder().s("user-123").build());
        item.put("timestamp", AttributeValue.builder().n(String.valueOf(1000 + i)).build());
        item.put("amount", AttributeValue.builder().n(String.valueOf(i * 50)).build());
        item.put("type", AttributeValue.builder().s(i % 2 == 0 ? "credit" : "debit").build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Query with key condition and filter expression
      Map<String, AttributeValue> expressionValues = new HashMap<>();
      expressionValues.put(":userId", AttributeValue.builder().s("user-123").build());
      expressionValues.put(":minAmount", AttributeValue.builder().n("200").build());
      expressionValues.put(":type", AttributeValue.builder().s("credit").build());

      QueryResponse queryResponse = client.query(QueryRequest.builder()
          .tableName(TABLE_NAME)
          .keyConditionExpression("userId = :userId")
          .filterExpression("amount >= :minAmount AND #type = :type")
          .expressionAttributeNames(Map.of("#type", "type"))
          .expressionAttributeValues(expressionValues)
          .build());

      // Credits with amount >= 200: items 4, 6, 8 (amounts 200, 300, 400)
      assertThat(queryResponse.count()).isEqualTo(3);
      assertThat(queryResponse.scannedCount()).isEqualTo(8);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ QUERY with filter test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testFilterWithAttributeExists(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing filter with attribute_exists: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write items, some with email and some without
      for (int i = 1; i <= 6; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("user-" + i).build());
        item.put("name", AttributeValue.builder().s("User " + i).build());

        // Only odd numbered users have email
        if (i % 2 == 1) {
          item.put("email", AttributeValue.builder().s("user" + i + "@example.com").build());
        }

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Scan for items with email
      ScanResponse scanResponse = client.scan(ScanRequest.builder()
          .tableName(TABLE_NAME)
          .filterExpression("attribute_exists(email)")
          .build());

      // Should get users 1, 3, 5 (3 items)
      assertThat(scanResponse.count()).isEqualTo(3);
      assertThat(scanResponse.scannedCount()).isEqualTo(6);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Filter with attribute_exists test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testFilterWithBeginsWithFunction(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing filter with begins_with function: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write items with different prefixes
      String[] names = {"admin-user1", "admin-user2", "user3", "admin-user4", "guest5", "user6"};
      for (int i = 0; i < names.length; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("id-" + i).build());
        item.put("username", AttributeValue.builder().s(names[i]).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Scan for usernames beginning with "admin-"
      Map<String, AttributeValue> filterValues = new HashMap<>();
      filterValues.put(":prefix", AttributeValue.builder().s("admin-").build());

      ScanResponse scanResponse = client.scan(ScanRequest.builder()
          .tableName(TABLE_NAME)
          .filterExpression("begins_with(username, :prefix)")
          .expressionAttributeValues(filterValues)
          .build());

      // Should get admin-user1, admin-user2, admin-user4 (3 items)
      assertThat(scanResponse.count()).isEqualTo(3);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Filter with begins_with test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testFilterWithContainsFunction(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing filter with contains function: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write items with tags
      Map<String, AttributeValue> item1 = new HashMap<>();
      item1.put("id", AttributeValue.builder().s("item-1").build());
      item1.put("tags", AttributeValue.builder().ss("important", "urgent", "review").build());

      Map<String, AttributeValue> item2 = new HashMap<>();
      item2.put("id", AttributeValue.builder().s("item-2").build());
      item2.put("tags", AttributeValue.builder().ss("normal", "pending").build());

      Map<String, AttributeValue> item3 = new HashMap<>();
      item3.put("id", AttributeValue.builder().s("item-3").build());
      item3.put("tags", AttributeValue.builder().ss("important", "archived").build());

      client.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item1).build());
      client.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item2).build());
      client.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item3).build());

      // Scan for items containing "important" tag
      Map<String, AttributeValue> filterValues = new HashMap<>();
      filterValues.put(":tag", AttributeValue.builder().s("important").build());

      ScanResponse scanResponse = client.scan(ScanRequest.builder()
          .tableName(TABLE_NAME)
          .filterExpression("contains(tags, :tag)")
          .expressionAttributeValues(filterValues)
          .build());

      // Should get item-1 and item-3 (2 items)
      assertThat(scanResponse.count()).isEqualTo(2);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Filter with contains test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testFilterWithComplexLogicalOperators(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing filter with complex logical operators: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write items
      for (int i = 1; i <= 10; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("product-" + i).build());
        item.put("price", AttributeValue.builder().n(String.valueOf(i * 10)).build());
        item.put("inStock", AttributeValue.builder().bool(i % 3 != 0).build());
        item.put("featured", AttributeValue.builder().bool(i % 2 == 0).build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Filter: (price >= 30 AND price <= 70) AND (inStock = true OR featured = true)
      Map<String, AttributeValue> filterValues = new HashMap<>();
      filterValues.put(":minPrice", AttributeValue.builder().n("30").build());
      filterValues.put(":maxPrice", AttributeValue.builder().n("70").build());
      filterValues.put(":true", AttributeValue.builder().bool(true).build());

      ScanResponse scanResponse = client.scan(ScanRequest.builder()
          .tableName(TABLE_NAME)
          .filterExpression("(price >= :minPrice AND price <= :maxPrice) AND (inStock = :true OR featured = :true)")
          .expressionAttributeValues(filterValues)
          .build());

      // Products 3-7 with price 30-70
      // Product 3: inStock=false, featured=false -> excluded
      // Product 4: inStock=true, featured=true -> included
      // Product 5: inStock=true, featured=false -> included
      // Product 6: inStock=false, featured=true -> included
      // Product 7: inStock=true, featured=false -> included
      assertThat(scanResponse.count()).isEqualTo(4);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Filter with complex logical operators test passed for: {}", provider.getProviderName());
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
