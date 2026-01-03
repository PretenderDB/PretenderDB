package io.github.pretenderdb.integ;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
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
 * Integration test that validates conditional operations work identically
 * across DynamoDB Local and Pretender implementations.
 */
class ConditionalOperationsIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(ConditionalOperationsIntegrationTest.class);
  private static final String TABLE_NAME = "ConditionalTestTable";

  static Stream<DynamoDbProvider> dynamoDbProviders() {
    return Stream.of(
        new DynamoDbLocalProvider(),
        new PretenderPostgresProvider(),
        new PretenderHsqldbProvider()
    );
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testPutItemWithAttributeNotExists(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing PUT ITEM with attribute_not_exists condition: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // First put should succeed (item doesn't exist)
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("conditional-1").build());
      item.put("value", AttributeValue.builder().n("100").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .conditionExpression("attribute_not_exists(id)")
          .build());

      // Verify item was created
      GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("conditional-1").build()))
          .build());

      assertThat(getResponse.hasItem()).isTrue();

      // Second put with same condition should fail (item now exists)
      Map<String, AttributeValue> item2 = new HashMap<>();
      item2.put("id", AttributeValue.builder().s("conditional-1").build());
      item2.put("value", AttributeValue.builder().n("200").build());

      assertThatThrownBy(() -> client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item2)
          .conditionExpression("attribute_not_exists(id)")
          .build()))
          .isInstanceOf(ConditionalCheckFailedException.class);

      // Verify original item unchanged
      GetItemResponse finalCheck = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("conditional-1").build()))
          .build());

      assertThat(finalCheck.item()).containsEntry("value", AttributeValue.builder().n("100").build());

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ PUT ITEM with attribute_not_exists test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testUpdateItemWithConditionExpression(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing UPDATE ITEM with condition expression: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create initial item
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("update-cond-1").build());
      item.put("version", AttributeValue.builder().n("1").build());
      item.put("balance", AttributeValue.builder().n("1000").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Update with correct version should succeed
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("update-cond-1").build());

      Map<String, AttributeValue> updateValues = new HashMap<>();
      updateValues.put(":newBalance", AttributeValue.builder().n("900").build());
      updateValues.put(":expectedVersion", AttributeValue.builder().n("1").build());
      updateValues.put(":newVersion", AttributeValue.builder().n("2").build());

      client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("SET balance = :newBalance, version = :newVersion")
          .conditionExpression("version = :expectedVersion")
          .expressionAttributeValues(updateValues)
          .build());

      // Verify update succeeded
      GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(getResponse.item()).containsEntry("version", AttributeValue.builder().n("2").build());
      assertThat(getResponse.item()).containsEntry("balance", AttributeValue.builder().n("900").build());

      // Update with wrong version should fail
      Map<String, AttributeValue> wrongVersionValues = new HashMap<>();
      wrongVersionValues.put(":newBalance", AttributeValue.builder().n("800").build());
      wrongVersionValues.put(":expectedVersion", AttributeValue.builder().n("1").build());  // wrong version
      wrongVersionValues.put(":newVersion", AttributeValue.builder().n("3").build());

      assertThatThrownBy(() -> client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("SET balance = :newBalance, version = :newVersion")
          .conditionExpression("version = :expectedVersion")
          .expressionAttributeValues(wrongVersionValues)
          .build()))
          .isInstanceOf(ConditionalCheckFailedException.class);

      // Verify item unchanged
      GetItemResponse finalCheck = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(finalCheck.item()).containsEntry("version", AttributeValue.builder().n("2").build());
      assertThat(finalCheck.item()).containsEntry("balance", AttributeValue.builder().n("900").build());

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ UPDATE ITEM with condition expression test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testDeleteItemWithCondition(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing DELETE ITEM with condition: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create items
      Map<String, AttributeValue> item1 = new HashMap<>();
      item1.put("id", AttributeValue.builder().s("delete-cond-1").build());
      item1.put("status", AttributeValue.builder().s("active").build());

      Map<String, AttributeValue> item2 = new HashMap<>();
      item2.put("id", AttributeValue.builder().s("delete-cond-2").build());
      item2.put("status", AttributeValue.builder().s("inactive").build());

      client.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item1).build());
      client.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item2).build());

      // Try to delete item with wrong condition (should fail)
      Map<String, AttributeValue> key1 = new HashMap<>();
      key1.put("id", AttributeValue.builder().s("delete-cond-1").build());

      Map<String, AttributeValue> wrongConditionValues = new HashMap<>();
      wrongConditionValues.put(":expectedStatus", AttributeValue.builder().s("inactive").build());

      assertThatThrownBy(() -> client.deleteItem(DeleteItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key1)
          .conditionExpression("#status = :expectedStatus")
          .expressionAttributeNames(Map.of("#status", "status"))
          .expressionAttributeValues(wrongConditionValues)
          .build()))
          .isInstanceOf(ConditionalCheckFailedException.class);

      // Verify item still exists
      GetItemResponse stillExists = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key1)
          .build());

      assertThat(stillExists.hasItem()).isTrue();

      // Delete with correct condition (should succeed)
      Map<String, AttributeValue> correctConditionValues = new HashMap<>();
      correctConditionValues.put(":expectedStatus", AttributeValue.builder().s("active").build());

      client.deleteItem(DeleteItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key1)
          .conditionExpression("#status = :expectedStatus")
          .expressionAttributeNames(Map.of("#status", "status"))
          .expressionAttributeValues(correctConditionValues)
          .returnValues(ReturnValue.ALL_OLD)
          .build());

      // Verify item deleted
      GetItemResponse afterDelete = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key1)
          .build());

      assertThat(afterDelete.hasItem()).isFalse();

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ DELETE ITEM with condition test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testConditionWithComparisonOperators(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing conditions with comparison operators: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create item
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("comparison-test").build());
      item.put("stock", AttributeValue.builder().n("50").build());
      item.put("minStock", AttributeValue.builder().n("10").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Update with >= condition (should succeed)
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("comparison-test").build());

      Map<String, AttributeValue> updateValues = new HashMap<>();
      updateValues.put(":minRequired", AttributeValue.builder().n("30").build());
      updateValues.put(":decrease", AttributeValue.builder().n("20").build());

      client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("SET stock = stock - :decrease")
          .conditionExpression("stock >= :minRequired")
          .expressionAttributeValues(updateValues)
          .build());

      // Verify update
      GetItemResponse afterUpdate = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(afterUpdate.item()).containsEntry("stock", AttributeValue.builder().n("30").build());

      // Try update with > condition that should fail
      Map<String, AttributeValue> failValues = new HashMap<>();
      failValues.put(":minRequired", AttributeValue.builder().n("30").build());  // stock is exactly 30
      failValues.put(":decrease", AttributeValue.builder().n("10").build());

      assertThatThrownBy(() -> client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("SET stock = stock - :decrease")
          .conditionExpression("stock > :minRequired")  // > instead of >=
          .expressionAttributeValues(failValues)
          .build()))
          .isInstanceOf(ConditionalCheckFailedException.class);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Conditions with comparison operators test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testConditionWithAttributeExists(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing condition with attribute_exists: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create item with optional attribute
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("id", AttributeValue.builder().s("exists-test").build());
      item.put("email", AttributeValue.builder().s("test@example.com").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item)
          .build());

      // Update only if email exists (should succeed)
      Map<String, AttributeValue> key = new HashMap<>();
      key.put("id", AttributeValue.builder().s("exists-test").build());

      Map<String, AttributeValue> updateValues = new HashMap<>();
      updateValues.put(":verified", AttributeValue.builder().bool(true).build());

      client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .updateExpression("SET verified = :verified")
          .conditionExpression("attribute_exists(email)")
          .expressionAttributeValues(updateValues)
          .build());

      // Verify update
      GetItemResponse result = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key)
          .build());

      assertThat(result.item()).containsEntry("verified", AttributeValue.builder().bool(true).build());

      // Create item without email
      Map<String, AttributeValue> item2 = new HashMap<>();
      item2.put("id", AttributeValue.builder().s("exists-test-2").build());
      item2.put("name", AttributeValue.builder().s("No Email User").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(item2)
          .build());

      // Try to update with email exists condition (should fail)
      Map<String, AttributeValue> key2 = new HashMap<>();
      key2.put("id", AttributeValue.builder().s("exists-test-2").build());

      assertThatThrownBy(() -> client.updateItem(UpdateItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(key2)
          .updateExpression("SET verified = :verified")
          .conditionExpression("attribute_exists(email)")
          .expressionAttributeValues(updateValues)
          .build()))
          .isInstanceOf(ConditionalCheckFailedException.class);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ Condition with attribute_exists test passed for: {}", provider.getProviderName());
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
