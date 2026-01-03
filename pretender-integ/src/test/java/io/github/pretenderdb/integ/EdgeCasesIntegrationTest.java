package io.github.pretenderdb.integ;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
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
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * Integration test that validates edge cases and validation behavior
 * work identically across DynamoDB Local and Pretender implementations.
 */
class EdgeCasesIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(EdgeCasesIntegrationTest.class);
  private static final String TABLE_NAME = "EdgeCasesTestTable";

  static Stream<DynamoDbProvider> dynamoDbProviders() {
    return Stream.of(
        new DynamoDbLocalProvider(),
        new PretenderPostgresProvider(),
        new PretenderHsqldbProvider()
    );
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testGetItemNonExistentKey(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing GET ITEM with non-existent key: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Try to get item that doesn't exist
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("does-not-exist").build());

      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      // Should return empty response (not throw exception)
      assertThat(response.hasItem()).isFalse();
      assertThat(response.item()).isEmpty();

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ GET ITEM with non-existent key test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testDeleteItemNonExistent(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing DELETE ITEM on non-existent item: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Delete item that doesn't exist - should succeed silently
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("never-existed").build());

      client.deleteItem(DeleteItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      // No exception should be thrown
      assertThat(true).isTrue();

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ DELETE ITEM on non-existent item test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testPutItemOverwrite(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing PUT ITEM overwrite behavior: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Put initial item
      Map<String, AttributeValue> item1 = new HashMap<>();
      item1.put("id", AttributeValue.builder().s("overwrite-test").build());
      item1.put("value", AttributeValue.builder().n("100").build());
      item1.put("name", AttributeValue.builder().s("Original").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item1)
          .build());

      // Overwrite with new item (different attributes)
      Map<String, AttributeValue> item2 = new HashMap<>();
      item2.put("id", AttributeValue.builder().s("overwrite-test").build());
      item2.put("value", AttributeValue.builder().n("200").build());
      item2.put("description", AttributeValue.builder().s("New attribute").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item2)
          .build());

      // Get item and verify complete replacement
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("overwrite-test").build()))
          .build());

      assertThat(response.item()).containsEntry("value", AttributeValue.builder().n("200").build());
      assertThat(response.item()).containsEntry("description", AttributeValue.builder().s("New attribute").build());
      assertThat(response.item()).doesNotContainKey("name");  // Old attribute removed

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ PUT ITEM overwrite test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testUpdateItemNonExistent(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing UPDATE ITEM on non-existent item: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Update item that doesn't exist - should create it
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("new-item").build());

      Map<String, AttributeValue> updateValues = new HashMap<>();
      updateValues.put(":value", AttributeValue.builder().n("999").build());

      client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("SET #value = :value")
          .expressionAttributeNames(Map.of("#value", "value"))
          .expressionAttributeValues(updateValues)
          .build());

      // Verify item was created
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(response.hasItem()).isTrue();
      assertThat(response.item()).containsEntry("value", AttributeValue.builder().n("999").build());

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ UPDATE ITEM on non-existent item test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testReturnValuesAllOld(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing ReturnValues ALL_OLD: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Put initial item
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("return-test").build());
      item.put("version", AttributeValue.builder().n("1").build());
      item.put("data", AttributeValue.builder().s("original data").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Update with ALL_OLD
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("return-test").build());

      Map<String, AttributeValue> updateValues = new HashMap<>();
      updateValues.put(":newVersion", AttributeValue.builder().n("2").build());

      var updateResponse = client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("SET version = :newVersion")
          .expressionAttributeValues(updateValues)
          .returnValues(ReturnValue.ALL_OLD)
          .build());

      // Should return old values
      assertThat(updateResponse.hasAttributes()).isTrue();
      assertThat(updateResponse.attributes()).containsEntry("version", AttributeValue.builder().n("1").build());
      assertThat(updateResponse.attributes()).containsEntry("data", AttributeValue.builder().s("original data").build());

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ ReturnValues ALL_OLD test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testUpdateExpressionRemove(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing UPDATE EXPRESSION REMOVE operation: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Put item with multiple attributes
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("remove-test").build());
      item.put("attr1", AttributeValue.builder().s("keep me").build());
      item.put("attr2", AttributeValue.builder().s("remove me").build());
      item.put("attr3", AttributeValue.builder().n("123").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Remove attr2
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("remove-test").build());

      client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("REMOVE attr2")
          .build());

      // Verify attr2 is removed
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(response.item()).containsKeys("id", "attr1", "attr3");
      assertThat(response.item()).doesNotContainKey("attr2");

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ UPDATE EXPRESSION REMOVE test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testUpdateExpressionAddToNumber(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing UPDATE EXPRESSION ADD to number: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Put initial item
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("add-test").build());
      item.put("counter", AttributeValue.builder().n("10").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Add to counter (use expression attribute name since "counter" is a reserved keyword)
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("add-test").build());

      Map<String, AttributeValue> addValues = new HashMap<>();
      addValues.put(":increment", AttributeValue.builder().n("5").build());

      client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("ADD #counter :increment")
          .expressionAttributeNames(Map.of("#counter", "counter"))
          .expressionAttributeValues(addValues)
          .build());

      // Verify counter increased
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(response.item()).containsEntry("counter", AttributeValue.builder().n("15").build());

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ UPDATE EXPRESSION ADD to number test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testUpdateExpressionAddToSet(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing UPDATE EXPRESSION ADD to string set: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Put item with string set
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("set-test").build());
      item.put("tags", AttributeValue.builder().ss("tag1", "tag2").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Add to set
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("set-test").build());

      Map<String, AttributeValue> addValues = new HashMap<>();
      addValues.put(":newTags", AttributeValue.builder().ss("tag3", "tag4").build());

      client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("ADD tags :newTags")
          .expressionAttributeValues(addValues)
          .build());

      // Verify set has all tags
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(response.item().get("tags").ss())
          .containsExactlyInAnyOrder("tag1", "tag2", "tag3", "tag4");

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ UPDATE EXPRESSION ADD to set test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testUpdateExpressionDeleteFromSet(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing UPDATE EXPRESSION DELETE from set: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Put item with string set
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("delete-set-test").build());
      item.put("categories", AttributeValue.builder()
          .ss("electronics", "gadgets", "sale", "new")
          .build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Delete from set
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("delete-set-test").build());

      Map<String, AttributeValue> deleteValues = new HashMap<>();
      deleteValues.put(":removeCategories", AttributeValue.builder().ss("sale", "new").build());

      client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("DELETE categories :removeCategories")
          .expressionAttributeValues(deleteValues)
          .build());

      // Verify set has remaining items
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(response.item().get("categories").ss())
          .containsExactlyInAnyOrder("electronics", "gadgets");

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ UPDATE EXPRESSION DELETE from set test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testNumberKeyDataTypes(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing number key data types: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table with number hash key
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
                  .attributeType(ScalarAttributeType.N)
                  .build()
          )
          .provisionedThroughput(ProvisionedThroughput.builder()
              .readCapacityUnits(5L)
              .writeCapacityUnits(5L)
              .build())
          .build();

      client.createTable(request);

      // Put item with number key
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().n("12345").build());
      item.put("data", AttributeValue.builder().s("Number key test").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Get item
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().n("12345").build()))
          .build());

      assertThat(response.hasItem()).isTrue();
      assertThat(response.item()).containsEntry("data", AttributeValue.builder().s("Number key test").build());

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Number key data types test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testBinaryKeyDataTypes(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing binary key data types: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();

      // Create table with binary hash key
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
                  .attributeType(ScalarAttributeType.B)
                  .build()
          )
          .provisionedThroughput(ProvisionedThroughput.builder()
              .readCapacityUnits(5L)
              .writeCapacityUnits(5L)
              .build())
          .build();

      client.createTable(request);

      // Put item with binary key
      SdkBytes binaryKey = SdkBytes.fromString("binary-key-123", StandardCharsets.UTF_8);
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().b(binaryKey).build());
      item.put("data", AttributeValue.builder().s("Binary key test").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Get item
      GetItemResponse response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().b(binaryKey).build()))
          .build());

      assertThat(response.hasItem()).isTrue();
      assertThat(response.item()).containsEntry("data", AttributeValue.builder().s("Binary key test").build());
      assertThat(response.item().get("id").b().asString(StandardCharsets.UTF_8))
          .isEqualTo("binary-key-123");

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Binary key data types test passed for: {}", provider.getProviderName());
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
