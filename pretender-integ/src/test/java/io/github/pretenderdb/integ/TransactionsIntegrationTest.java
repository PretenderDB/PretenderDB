package io.github.pretenderdb.integ;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.Get;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TransactGetItem;
import software.amazon.awssdk.services.dynamodb.model.TransactGetItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactGetItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;

/**
 * Integration test that validates transactional operations work identically
 * across DynamoDB Local and Pretender implementations.
 */
class TransactionsIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(TransactionsIntegrationTest.class);
  private static final String TABLE_NAME = "TransactionTestTable";

  static Stream<DynamoDbProvider> dynamoDbProviders() {
    return Stream.of(
        new DynamoDbLocalProvider(),
        new PretenderPostgresProvider(),
        new PretenderHsqldbProvider()
    );
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testTransactGetItemsWithMultipleItems(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing TRANSACT GET ITEMS with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write test items
      for (int i = 1; i <= 5; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("txn-item-" + i).build());
        item.put("value", AttributeValue.builder().n(String.valueOf(i * 10)).build());
        item.put("status", AttributeValue.builder().s("active").build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Transact get multiple items
      List<TransactGetItem> transactItems = new ArrayList<>();
      for (int i : new int[]{1, 3, 5}) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("txn-item-" + i).build());

        transactItems.add(TransactGetItem.builder()
            .get(Get.builder()
                .tableName(TABLE_NAME)
                .key(key)
                .build())
            .build());
      }

      TransactGetItemsResponse response = client.transactGetItems(TransactGetItemsRequest.builder()
          .transactItems(transactItems)
          .build());

      // Verify we got 3 items
      assertThat(response.responses()).hasSize(3);

      // Verify item content
      for (int i = 0; i < 3; i++) {
        ItemResponse itemResponse = response.responses().get(i);
        assertThat(itemResponse.hasItem()).isTrue();
        assertThat(itemResponse.item()).containsKey("id");
        assertThat(itemResponse.item()).containsKey("value");
        assertThat(itemResponse.item()).containsKey("status");
      }

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ TRANSACT GET ITEMS test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testTransactWriteItemsWithMultiplePuts(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing TRANSACT WRITE ITEMS (puts) with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Transactional write with multiple puts
      List<TransactWriteItem> transactItems = new ArrayList<>();
      for (int i = 1; i <= 5; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("txn-put-" + i).build());
        item.put("amount", AttributeValue.builder().n(String.valueOf(i * 100)).build());
        item.put("type", AttributeValue.builder().s("credit").build());

        transactItems.add(TransactWriteItem.builder()
            .put(Put.builder()
                .tableName(TABLE_NAME)
                .item(item)
                .build())
            .build());
      }

      client.transactWriteItems(TransactWriteItemsRequest.builder()
          .transactItems(transactItems)
          .build());

      // Verify all items were written
      for (int i = 1; i <= 5; i++) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("txn-put-" + i).build());

        GetItemResponse response = client.getItem(GetItemRequest.builder()
            .tableName(TABLE_NAME)
            .key(key)
            .build());

        assertThat(response.hasItem()).isTrue();
        assertThat(response.item()).containsEntry("amount",
            AttributeValue.builder().n(String.valueOf(i * 100)).build());
      }

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ TRANSACT WRITE ITEMS (puts) test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testTransactWriteItemsWithMixedOperations(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing TRANSACT WRITE ITEMS (mixed) with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Setup: Write initial items
      for (int i = 1; i <= 3; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("mixed-" + i).build());
        item.put("balance", AttributeValue.builder().n("100").build());
        item.put("status", AttributeValue.builder().s("pending").build());

        client.putItem(PutItemRequest.builder()
            .tableName(TABLE_NAME)
            .item(item)
            .build());
      }

      // Transaction with mixed operations
      List<TransactWriteItem> transactItems = new ArrayList<>();

      // 1. Put a new item
      Map<String, AttributeValue> newItem = new HashMap<>();
      newItem.put("id", AttributeValue.builder().s("mixed-4").build());
      newItem.put("balance", AttributeValue.builder().n("200").build());
      newItem.put("status", AttributeValue.builder().s("active").build());

      transactItems.add(TransactWriteItem.builder()
          .put(Put.builder()
              .tableName(TABLE_NAME)
              .item(newItem)
              .build())
          .build());

      // 2. Update an existing item
      Map<String, AttributeValue> updateKey = new HashMap<>();
      updateKey.put("id", AttributeValue.builder().s("mixed-1").build());

      Map<String, AttributeValue> updateValues = new HashMap<>();
      updateValues.put(":newStatus", AttributeValue.builder().s("active").build());

      transactItems.add(TransactWriteItem.builder()
          .update(Update.builder()
              .tableName(TABLE_NAME)
              .key(updateKey)
              .updateExpression("SET #status = :newStatus")
              .expressionAttributeNames(Map.of("#status", "status"))
              .expressionAttributeValues(updateValues)
              .build())
          .build());

      // 3. Delete an item
      Map<String, AttributeValue> deleteKey = new HashMap<>();
      deleteKey.put("id", AttributeValue.builder().s("mixed-2").build());

      transactItems.add(TransactWriteItem.builder()
          .delete(Delete.builder()
              .tableName(TABLE_NAME)
              .key(deleteKey)
              .build())
          .build());

      client.transactWriteItems(TransactWriteItemsRequest.builder()
          .transactItems(transactItems)
          .build());

      // Verify results
      // Check new item exists
      GetItemResponse newItemResponse = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("mixed-4").build()))
          .build());
      assertThat(newItemResponse.hasItem()).isTrue();
      assertThat(newItemResponse.item()).containsEntry("balance", AttributeValue.builder().n("200").build());

      // Check updated item
      GetItemResponse updatedItemResponse = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("mixed-1").build()))
          .build());
      assertThat(updatedItemResponse.hasItem()).isTrue();
      assertThat(updatedItemResponse.item()).containsEntry("status", AttributeValue.builder().s("active").build());

      // Check deleted item doesn't exist
      GetItemResponse deletedItemResponse = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("mixed-2").build()))
          .build());
      assertThat(deletedItemResponse.hasItem()).isFalse();

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ TRANSACT WRITE ITEMS (mixed) test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testTransactWriteItemsRollbackOnConditionFailure(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing TRANSACT WRITE ITEMS rollback with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Setup: Write initial item
      Map<String, AttributeValue> initialItem = new HashMap<>();
      initialItem.put("id", AttributeValue.builder().s("rollback-test").build());
      initialItem.put("version", AttributeValue.builder().n("1").build());
      initialItem.put("data", AttributeValue.builder().s("original").build());

      client.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(initialItem)
          .build());

      // Transaction that should fail due to condition
      List<TransactWriteItem> transactItems = new ArrayList<>();

      // Item 1: Try to put a new item
      Map<String, AttributeValue> newItem1 = new HashMap<>();
      newItem1.put("id", AttributeValue.builder().s("new-item-1").build());
      newItem1.put("data", AttributeValue.builder().s("should not be created").build());

      transactItems.add(TransactWriteItem.builder()
          .put(Put.builder()
              .tableName(TABLE_NAME)
              .item(newItem1)
              .build())
          .build());

      // Item 2: Update with a failing condition (version must be 2, but it's 1)
      Map<String, AttributeValue> updateKey = new HashMap<>();
      updateKey.put("id", AttributeValue.builder().s("rollback-test").build());

      Map<String, AttributeValue> updateValues = new HashMap<>();
      updateValues.put(":newData", AttributeValue.builder().s("modified").build());
      updateValues.put(":expectedVersion", AttributeValue.builder().n("2").build());

      transactItems.add(TransactWriteItem.builder()
          .update(Update.builder()
              .tableName(TABLE_NAME)
              .key(updateKey)
              .updateExpression("SET #data = :newData")
              .conditionExpression("#version = :expectedVersion")
              .expressionAttributeNames(Map.of("#data", "data", "#version", "version"))
              .expressionAttributeValues(updateValues)
              .build())
          .build());

      // Execute transaction - should fail
      assertThatThrownBy(() -> client.transactWriteItems(TransactWriteItemsRequest.builder()
          .transactItems(transactItems)
          .build()))
          .isInstanceOf(TransactionCanceledException.class);

      // Verify rollback: original item unchanged
      GetItemResponse originalResponse = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("rollback-test").build()))
          .build());

      assertThat(originalResponse.hasItem()).isTrue();
      assertThat(originalResponse.item()).containsEntry("version", AttributeValue.builder().n("1").build());
      assertThat(originalResponse.item()).containsEntry("data", AttributeValue.builder().s("original").build());

      // Verify new item was not created
      GetItemResponse newItemResponse = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("new-item-1").build()))
          .build());

      assertThat(newItemResponse.hasItem()).isFalse();

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ TRANSACT WRITE ITEMS rollback test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testTransactWriteItemsWithConditionCheck(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing TRANSACT WRITE ITEMS with condition check: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Setup: Create account items
      Map<String, AttributeValue> account1 = new HashMap<>();
      account1.put("id", AttributeValue.builder().s("account-1").build());
      account1.put("balance", AttributeValue.builder().n("500").build());

      Map<String, AttributeValue> account2 = new HashMap<>();
      account2.put("id", AttributeValue.builder().s("account-2").build());
      account2.put("balance", AttributeValue.builder().n("200").build());

      client.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(account1).build());
      client.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(account2).build());

      // Transaction: Transfer money with condition check
      List<TransactWriteItem> transactItems = new ArrayList<>();

      // 1. Deduct from account-1 (with condition to ensure sufficient balance)
      Map<String, AttributeValue> deductKey = new HashMap<>();
      deductKey.put("id", AttributeValue.builder().s("account-1").build());

      Map<String, AttributeValue> deductValues = new HashMap<>();
      deductValues.put(":amount", AttributeValue.builder().n("100").build());
      deductValues.put(":minBalance", AttributeValue.builder().n("100").build());

      transactItems.add(TransactWriteItem.builder()
          .update(Update.builder()
              .tableName(TABLE_NAME)
              .key(deductKey)
              .updateExpression("SET balance = balance - :amount")
              .conditionExpression("balance >= :minBalance")
              .expressionAttributeValues(deductValues)
              .build())
          .build());

      // 2. Add to account-2
      Map<String, AttributeValue> addKey = new HashMap<>();
      addKey.put("id", AttributeValue.builder().s("account-2").build());

      Map<String, AttributeValue> addValues = new HashMap<>();
      addValues.put(":amount", AttributeValue.builder().n("100").build());

      transactItems.add(TransactWriteItem.builder()
          .update(Update.builder()
              .tableName(TABLE_NAME)
              .key(addKey)
              .updateExpression("SET balance = balance + :amount")
              .expressionAttributeValues(addValues)
              .build())
          .build());

      // Execute transaction
      client.transactWriteItems(TransactWriteItemsRequest.builder()
          .transactItems(transactItems)
          .build());

      // Verify final balances
      GetItemResponse acc1Response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("account-1").build()))
          .build());
      assertThat(acc1Response.item()).containsEntry("balance", AttributeValue.builder().n("400").build());

      GetItemResponse acc2Response = client.getItem(GetItemRequest.builder()
          .tableName(TABLE_NAME)
          .key(Map.of("id", AttributeValue.builder().s("account-2").build()))
          .build());
      assertThat(acc2Response.item()).containsEntry("balance", AttributeValue.builder().n("300").build());

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ TRANSACT WRITE ITEMS with condition check test passed for: {}", provider.getProviderName());
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
