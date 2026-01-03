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
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Integration test that validates batch operations work identically
 * across DynamoDB Local and Pretender implementations.
 */
class BatchOperationsIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(BatchOperationsIntegrationTest.class);
  private static final String TABLE_NAME = "BatchTestTable";

  static Stream<DynamoDbProvider> dynamoDbProviders() {
    return Stream.of(
        new DynamoDbLocalProvider(),
        new PretenderPostgresProvider(),
        new PretenderHsqldbProvider()
    );
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testBatchWriteItemWithMultiplePuts(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing BATCH WRITE ITEM (puts) with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Create batch write request with 10 items
      List<WriteRequest> writeRequests = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("batch-item-" + i).build());
        item.put("value", AttributeValue.builder().n(String.valueOf(i * 100)).build());
        item.put("description", AttributeValue.builder().s("Batch item " + i).build());

        writeRequests.add(WriteRequest.builder()
            .putRequest(PutRequest.builder().item(item).build())
            .build());
      }

      Map<String, List<WriteRequest>> requestItems = new HashMap<>();
      requestItems.put(TABLE_NAME, writeRequests);

      BatchWriteItemResponse response = client.batchWriteItem(BatchWriteItemRequest.builder()
          .requestItems(requestItems)
          .build());

      // Verify no unprocessed items
      assertThat(response.unprocessedItems()).isEmpty();

      // Verify items were written by batch getting them
      List<Map<String, AttributeValue>> keys = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("batch-item-" + i).build());
        keys.add(key);
      }

      Map<String, KeysAndAttributes> getRequestItems = new HashMap<>();
      getRequestItems.put(TABLE_NAME, KeysAndAttributes.builder().keys(keys).build());

      BatchGetItemResponse getResponse = client.batchGetItem(BatchGetItemRequest.builder()
          .requestItems(getRequestItems)
          .build());

      assertThat(getResponse.responses()).containsKey(TABLE_NAME);
      assertThat(getResponse.responses().get(TABLE_NAME)).hasSize(10);

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ BATCH WRITE ITEM (puts) test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testBatchGetItemWithMultipleKeys(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing BATCH GET ITEM with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // First, write some items
      List<WriteRequest> writeRequests = new ArrayList<>();
      for (int i = 1; i <= 15; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("item-" + i).build());
        item.put("data", AttributeValue.builder().s("Data for item " + i).build());

        writeRequests.add(WriteRequest.builder()
            .putRequest(PutRequest.builder().item(item).build())
            .build());
      }

      Map<String, List<WriteRequest>> writeRequestItems = new HashMap<>();
      writeRequestItems.put(TABLE_NAME, writeRequests);

      client.batchWriteItem(BatchWriteItemRequest.builder()
          .requestItems(writeRequestItems)
          .build());

      // Now batch get specific items
      List<Map<String, AttributeValue>> keys = new ArrayList<>();
      for (int i : new int[]{2, 5, 7, 11, 14}) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("item-" + i).build());
        keys.add(key);
      }

      Map<String, KeysAndAttributes> requestItems = new HashMap<>();
      requestItems.put(TABLE_NAME, KeysAndAttributes.builder().keys(keys).build());

      BatchGetItemResponse response = client.batchGetItem(BatchGetItemRequest.builder()
          .requestItems(requestItems)
          .build());

      // Verify we got exactly 5 items
      assertThat(response.responses()).containsKey(TABLE_NAME);
      assertThat(response.responses().get(TABLE_NAME)).hasSize(5);
      assertThat(response.unprocessedKeys()).isEmpty();

      // Verify the correct items were returned
      List<String> returnedIds = response.responses().get(TABLE_NAME).stream()
          .map(item -> item.get("id").s())
          .sorted()
          .toList();
      assertThat(returnedIds).containsExactly("item-11", "item-14", "item-2", "item-5", "item-7");

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ BATCH GET ITEM test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testBatchWriteItemWithDeletes(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing BATCH WRITE ITEM (deletes) with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // First, write some items
      List<WriteRequest> writeRequests = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("delete-item-" + i).build());
        item.put("status", AttributeValue.builder().s("active").build());

        writeRequests.add(WriteRequest.builder()
            .putRequest(PutRequest.builder().item(item).build())
            .build());
      }

      Map<String, List<WriteRequest>> putRequestItems = new HashMap<>();
      putRequestItems.put(TABLE_NAME, writeRequests);

      client.batchWriteItem(BatchWriteItemRequest.builder()
          .requestItems(putRequestItems)
          .build());

      // Now batch delete some items
      List<WriteRequest> deleteRequests = new ArrayList<>();
      for (int i : new int[]{2, 4, 6, 8}) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("delete-item-" + i).build());

        deleteRequests.add(WriteRequest.builder()
            .deleteRequest(DeleteRequest.builder().key(key).build())
            .build());
      }

      Map<String, List<WriteRequest>> deleteRequestItems = new HashMap<>();
      deleteRequestItems.put(TABLE_NAME, deleteRequests);

      BatchWriteItemResponse deleteResponse = client.batchWriteItem(BatchWriteItemRequest.builder()
          .requestItems(deleteRequestItems)
          .build());

      assertThat(deleteResponse.unprocessedItems()).isEmpty();

      // Verify remaining items (should have 6 items left: 1, 3, 5, 7, 9, 10)
      List<Map<String, AttributeValue>> allKeys = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("delete-item-" + i).build());
        allKeys.add(key);
      }

      Map<String, KeysAndAttributes> getRequestItems = new HashMap<>();
      getRequestItems.put(TABLE_NAME, KeysAndAttributes.builder().keys(allKeys).build());

      BatchGetItemResponse getResponse = client.batchGetItem(BatchGetItemRequest.builder()
          .requestItems(getRequestItems)
          .build());

      // Should only get 6 items back (the ones not deleted)
      assertThat(getResponse.responses().get(TABLE_NAME)).hasSize(6);

      List<String> remainingIds = getResponse.responses().get(TABLE_NAME).stream()
          .map(item -> item.get("id").s())
          .sorted()
          .toList();
      assertThat(remainingIds).containsExactly(
          "delete-item-1", "delete-item-10", "delete-item-3",
          "delete-item-5", "delete-item-7", "delete-item-9"
      );

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ BATCH WRITE ITEM (deletes) test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testBatchWriteItemWithMixedPutsAndDeletes(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing BATCH WRITE ITEM (mixed puts/deletes) with: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write initial items
      List<WriteRequest> initialWrites = new ArrayList<>();
      for (int i = 1; i <= 5; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("mixed-item-" + i).build());
        item.put("version", AttributeValue.builder().n("1").build());

        initialWrites.add(WriteRequest.builder()
            .putRequest(PutRequest.builder().item(item).build())
            .build());
      }

      Map<String, List<WriteRequest>> initialRequestItems = new HashMap<>();
      initialRequestItems.put(TABLE_NAME, initialWrites);

      client.batchWriteItem(BatchWriteItemRequest.builder()
          .requestItems(initialRequestItems)
          .build());

      // Mix of puts and deletes in one batch
      List<WriteRequest> mixedRequests = new ArrayList<>();

      // Add new items (6-8)
      for (int i = 6; i <= 8; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("mixed-item-" + i).build());
        item.put("version", AttributeValue.builder().n("1").build());

        mixedRequests.add(WriteRequest.builder()
            .putRequest(PutRequest.builder().item(item).build())
            .build());
      }

      // Delete items (2, 4)
      for (int i : new int[]{2, 4}) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("mixed-item-" + i).build());

        mixedRequests.add(WriteRequest.builder()
            .deleteRequest(DeleteRequest.builder().key(key).build())
            .build());
      }

      Map<String, List<WriteRequest>> mixedRequestItems = new HashMap<>();
      mixedRequestItems.put(TABLE_NAME, mixedRequests);

      BatchWriteItemResponse response = client.batchWriteItem(BatchWriteItemRequest.builder()
          .requestItems(mixedRequestItems)
          .build());

      assertThat(response.unprocessedItems()).isEmpty();

      // Verify final state: should have items 1, 3, 5, 6, 7, 8 (6 total)
      List<Map<String, AttributeValue>> allKeys = new ArrayList<>();
      for (int i = 1; i <= 8; i++) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("mixed-item-" + i).build());
        allKeys.add(key);
      }

      Map<String, KeysAndAttributes> getRequestItems = new HashMap<>();
      getRequestItems.put(TABLE_NAME, KeysAndAttributes.builder().keys(allKeys).build());

      BatchGetItemResponse getResponse = client.batchGetItem(BatchGetItemRequest.builder()
          .requestItems(getRequestItems)
          .build());

      assertThat(getResponse.responses().get(TABLE_NAME)).hasSize(6);

      List<String> finalIds = getResponse.responses().get(TABLE_NAME).stream()
          .map(item -> item.get("id").s())
          .sorted()
          .toList();
      assertThat(finalIds).containsExactly(
          "mixed-item-1", "mixed-item-3", "mixed-item-5",
          "mixed-item-6", "mixed-item-7", "mixed-item-8"
      );

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ BATCH WRITE ITEM (mixed) test passed for: {}", provider.getProviderName());
    } finally {
      provider.close();
    }
  }

  @ParameterizedTest
  @MethodSource("dynamoDbProviders")
  void testBatchGetItemWithNonExistentKeys(DynamoDbProvider provider) throws Exception {
    log.info("=".repeat(80));
    log.info("Testing BATCH GET ITEM with non-existent keys: {}", provider.getProviderName());
    log.info("=".repeat(80));

    try {
      provider.start();
      DynamoDbClient client = provider.getDynamoDbClient();
      createSimpleTable(client);

      // Write only 3 items
      List<WriteRequest> writeRequests = new ArrayList<>();
      for (int i : new int[]{1, 3, 5}) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("sparse-item-" + i).build());
        item.put("data", AttributeValue.builder().s("Data " + i).build());

        writeRequests.add(WriteRequest.builder()
            .putRequest(PutRequest.builder().item(item).build())
            .build());
      }

      Map<String, List<WriteRequest>> writeRequestItems = new HashMap<>();
      writeRequestItems.put(TABLE_NAME, writeRequests);

      client.batchWriteItem(BatchWriteItemRequest.builder()
          .requestItems(writeRequestItems)
          .build());

      // Try to batch get items including non-existent ones
      List<Map<String, AttributeValue>> keys = new ArrayList<>();
      for (int i = 1; i <= 6; i++) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("sparse-item-" + i).build());
        keys.add(key);
      }

      Map<String, KeysAndAttributes> requestItems = new HashMap<>();
      requestItems.put(TABLE_NAME, KeysAndAttributes.builder().keys(keys).build());

      BatchGetItemResponse response = client.batchGetItem(BatchGetItemRequest.builder()
          .requestItems(requestItems)
          .build());

      // Should only get the 3 items that exist
      assertThat(response.responses().get(TABLE_NAME)).hasSize(3);

      List<String> returnedIds = response.responses().get(TABLE_NAME).stream()
          .map(item -> item.get("id").s())
          .sorted()
          .toList();
      assertThat(returnedIds).containsExactly("sparse-item-1", "sparse-item-3", "sparse-item-5");

      client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
      log.info("✓ BATCH GET ITEM with non-existent keys test passed for: {}", provider.getProviderName());
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
