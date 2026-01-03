package io.github.pretenderdb.integ;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Integration test that validates all DynamoDB data types work identically
 * across DynamoDB Local and Pretender implementations.
 */
class DataTypesIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(DataTypesIntegrationTest.class);
  private static final String TABLE_NAME = "DataTypesTestTable";

  static Stream<DynamoDbProvider> dynamoDbProviders() {
    return Stream.of(
        new DynamoDbLocalProvider(),
        new PretenderPostgresProvider(),
        new PretenderHsqldbProvider()
    );
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testScalarDataTypes(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing scalar data types: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create item with all scalar types
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("scalar-test").build());
      item.put("stringAttr", AttributeValue.builder().s("Hello World").build());
      item.put("numberAttr", AttributeValue.builder().n("12345.67").build());
      item.put("binaryAttr", AttributeValue.builder()
          .b(SdkBytes.fromString("Binary data", StandardCharsets.UTF_8))
          .build());
      item.put("boolAttr", AttributeValue.builder().bool(true).build());
      item.put("nullAttr", AttributeValue.builder().nul(true).build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Retrieve and verify
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("scalar-test").build()))
          .build());

      assertThat(response.hasItem()).isTrue();
      assertThat(response.item().get("stringAttr").s()).isEqualTo("Hello World");
      assertThat(response.item().get("numberAttr").n()).isEqualTo("12345.67");
      assertThat(response.item().get("binaryAttr").b().asString(StandardCharsets.UTF_8))
          .isEqualTo("Binary data");
      assertThat(response.item().get("boolAttr").bool()).isTrue();
      assertThat(response.item().get("nullAttr").nul()).isTrue();

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Scalar data types test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testSetDataTypes(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing set data types (SS, NS, BS): {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create item with all set types
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("set-test").build());
      item.put("stringSet", AttributeValue.builder()
          .ss("apple", "banana", "cherry")
          .build());
      item.put("numberSet", AttributeValue.builder()
          .ns("100", "200", "300")
          .build());
      item.put("binarySet", AttributeValue.builder()
          .bs(
              SdkBytes.fromString("data1", StandardCharsets.UTF_8),
              SdkBytes.fromString("data2", StandardCharsets.UTF_8),
              SdkBytes.fromString("data3", StandardCharsets.UTF_8)
          )
          .build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Retrieve and verify
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("set-test").build()))
          .build());

      assertThat(response.hasItem()).isTrue();
      assertThat(response.item().get("stringSet").ss())
          .containsExactlyInAnyOrder("apple", "banana", "cherry");
      assertThat(response.item().get("numberSet").ns())
          .containsExactlyInAnyOrder("100", "200", "300");
      assertThat(response.item().get("binarySet").bs())
          .hasSize(3);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Set data types test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testListDataType(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing list data type: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create item with list containing mixed types
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("list-test").build());
      item.put("mixedList", AttributeValue.builder()
          .l(
              AttributeValue.builder().s("string element").build(),
              AttributeValue.builder().n("42").build(),
              AttributeValue.builder().bool(true).build(),
              AttributeValue.builder().nul(true).build()
          )
          .build());
      item.put("numberList", AttributeValue.builder()
          .l(
              AttributeValue.builder().n("1").build(),
              AttributeValue.builder().n("2").build(),
              AttributeValue.builder().n("3").build()
          )
          .build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Retrieve and verify
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("list-test").build()))
          .build());

      assertThat(response.hasItem()).isTrue();

      List<AttributeValue> mixedList = response.item().get("mixedList").l();
      assertThat(mixedList).hasSize(4);
      assertThat(mixedList.get(0).s()).isEqualTo("string element");
      assertThat(mixedList.get(1).n()).isEqualTo("42");
      assertThat(mixedList.get(2).bool()).isTrue();
      assertThat(mixedList.get(3).nul()).isTrue();

      List<AttributeValue> numberList = response.item().get("numberList").l();
      assertThat(numberList).hasSize(3);
      assertThat(numberList.get(0).n()).isEqualTo("1");
      assertThat(numberList.get(1).n()).isEqualTo("2");
      assertThat(numberList.get(2).n()).isEqualTo("3");

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ List data type test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testMapDataType(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing map data type: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create item with nested map
      Map<String, AttributeValue> addressMap = new HashMap<>();
      addressMap.put("street", AttributeValue.builder().s("123 Main St").build());
      addressMap.put("city", AttributeValue.builder().s("Springfield").build());
      addressMap.put("zipCode", AttributeValue.builder().s("12345").build());
      addressMap.put("verified", AttributeValue.builder().bool(true).build());

      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("map-test").build());
      item.put("name", AttributeValue.builder().s("John Doe").build());
      item.put("address", AttributeValue.builder().m(addressMap).build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Retrieve and verify
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("map-test").build()))
          .build());

      assertThat(response.hasItem()).isTrue();

      Map<String, AttributeValue> retrievedAddress = response.item().get("address").m();
      assertThat(retrievedAddress.get("street").s()).isEqualTo("123 Main St");
      assertThat(retrievedAddress.get("city").s()).isEqualTo("Springfield");
      assertThat(retrievedAddress.get("zipCode").s()).isEqualTo("12345");
      assertThat(retrievedAddress.get("verified").bool()).isTrue();

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Map data type test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testNestedDocumentTypes(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing deeply nested document types: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create complex nested structure
      Map<String, AttributeValue> orderItem1 = new HashMap<>();
      orderItem1.put("productId", AttributeValue.builder().s("prod-123").build());
      orderItem1.put("quantity", AttributeValue.builder().n("2").build());
      orderItem1.put("price", AttributeValue.builder().n("29.99").build());

      Map<String, AttributeValue> orderItem2 = new HashMap<>();
      orderItem2.put("productId", AttributeValue.builder().s("prod-456").build());
      orderItem2.put("quantity", AttributeValue.builder().n("1").build());
      orderItem2.put("price", AttributeValue.builder().n("49.99").build());

      Map<String, AttributeValue> shippingAddress = new HashMap<>();
      shippingAddress.put("street", AttributeValue.builder().s("456 Oak Ave").build());
      shippingAddress.put("city", AttributeValue.builder().s("Portland").build());

      Map<String, AttributeValue> customerInfo = new HashMap<>();
      customerInfo.put("customerId", AttributeValue.builder().s("cust-789").build());
      customerInfo.put("email", AttributeValue.builder().s("customer@example.com").build());
      customerInfo.put("shippingAddress", AttributeValue.builder().m(shippingAddress).build());

      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("nested-test").build());
      item.put("orderDate", AttributeValue.builder().s("2024-01-15").build());
      item.put("customer", AttributeValue.builder().m(customerInfo).build());
      item.put("items", AttributeValue.builder()
          .l(
              AttributeValue.builder().m(orderItem1).build(),
              AttributeValue.builder().m(orderItem2).build()
          )
          .build());
      item.put("tags", AttributeValue.builder().ss("express", "gift", "priority").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Retrieve and verify
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("nested-test").build()))
          .build());

      assertThat(response.hasItem()).isTrue();

      // Verify nested customer data
      Map<String, AttributeValue> customer = response.item().get("customer").m();
      assertThat(customer.get("customerId").s()).isEqualTo("cust-789");

      Map<String, AttributeValue> address = customer.get("shippingAddress").m();
      assertThat(address.get("city").s()).isEqualTo("Portland");

      // Verify list of maps
      List<AttributeValue> orderItems = response.item().get("items").l();
      assertThat(orderItems).hasSize(2);
      assertThat(orderItems.get(0).m().get("productId").s()).isEqualTo("prod-123");
      assertThat(orderItems.get(1).m().get("quantity").n()).isEqualTo("1");

      // Verify string set
      assertThat(response.item().get("tags").ss())
          .containsExactlyInAnyOrder("express", "gift", "priority");

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Nested document types test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testEmptyAndNullValues(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing empty and null values: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create item with NULL and empty containers
      Map<String, AttributeValue> emptyMap = new HashMap<>();
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("empty-test").build());
      item.put("nullValue", AttributeValue.builder().nul(true).build());
      item.put("emptyList", AttributeValue.builder().l(List.of()).build());
      item.put("emptyMap", AttributeValue.builder().m(emptyMap).build());
      item.put("presentValue", AttributeValue.builder().s("I exist").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Retrieve and verify
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("empty-test").build()))
          .build());

      assertThat(response.hasItem()).isTrue();
      assertThat(response.item().get("nullValue").nul()).isTrue();
      assertThat(response.item().get("emptyList").l()).isEmpty();
      assertThat(response.item().get("emptyMap").m()).isEmpty();
      assertThat(response.item().get("presentValue").s()).isEqualTo("I exist");

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Empty and null values test passed for: {}", provider.getProviderName());
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
}
