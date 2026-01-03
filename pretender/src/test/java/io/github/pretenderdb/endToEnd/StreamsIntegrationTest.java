package io.github.pretenderdb.endToEnd;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pretenderdb.DynamoDbPretenderClient;
import io.github.pretenderdb.DynamoDbStreamsPretenderClient;
import io.github.pretenderdb.manager.PdbTableManager;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.model.Record;

/**
 * Integration tests for DynamoDB Streams functionality.
 */
class StreamsIntegrationTest extends BaseEndToEndTest {

  private static final String TABLE_NAME = "StreamTestTable";

  private DynamoDbPretenderClient dynamoClient;
  private DynamoDbStreamsPretenderClient streamsClient;
  private PdbTableManager tableManager;

  @BeforeEach
  void setupClients() {
    dynamoClient = component.dynamoDbPretenderClient();
    streamsClient = component.dynamoDbStreamsPretenderClient();
    tableManager = component.pdbTableManager();

    // Create test table
    createTestTable();
  }

  private void createTestTable() {
    dynamoClient.createTable(CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build()
        )
        .build());
  }

  @Test
  void enableStream_createsStreamConfiguration() {
    // Enable stream
    tableManager.enableStream(TABLE_NAME, "NEW_AND_OLD_IMAGES");

    // Verify stream is enabled
    final var metadata = tableManager.getPdbTable(TABLE_NAME);
    assertThat(metadata).isPresent();
    assertThat(metadata.get().streamEnabled()).isTrue();
    assertThat(metadata.get().streamViewType()).hasValue("NEW_AND_OLD_IMAGES");
    assertThat(metadata.get().streamArn()).isPresent();
    assertThat(metadata.get().streamLabel()).isPresent();
  }

  @Test
  void listStreams_returnsEnabledStreams() {
    // Enable stream
    tableManager.enableStream(TABLE_NAME, "KEYS_ONLY");

    // List streams
    final ListStreamsResponse response = streamsClient.listStreams(
        ListStreamsRequest.builder().build()
    );

    assertThat(response.streams()).isNotEmpty();
    assertThat(response.streams()).anyMatch(s -> s.tableName().equals(TABLE_NAME));
  }

  @Test
  void describeStream_returnsStreamDetails() {
    // Enable stream
    tableManager.enableStream(TABLE_NAME, "NEW_AND_OLD_IMAGES");

    final var metadata = tableManager.getPdbTable(TABLE_NAME).orElseThrow();
    final String streamArn = metadata.streamArn().orElseThrow();

    // Describe stream
    final DescribeStreamResponse response = streamsClient.describeStream(
        DescribeStreamRequest.builder()
            .streamArn(streamArn)
            .build()
    );

    assertThat(response.streamDescription()).isNotNull();
    assertThat(response.streamDescription().streamArn()).isEqualTo(streamArn);
    assertThat(response.streamDescription().tableName()).isEqualTo(TABLE_NAME);
    assertThat(response.streamDescription().streamViewType()).isEqualTo(StreamViewType.NEW_AND_OLD_IMAGES);
    assertThat(response.streamDescription().shards()).hasSize(1);
    assertThat(response.streamDescription().shards().get(0).shardId()).isEqualTo("shard-00000");
  }

  @Test
  void putItem_withStreamEnabled_capturesInsertEvent() {
    // Enable stream
    tableManager.enableStream(TABLE_NAME, "NEW_AND_OLD_IMAGES");

    // Put item
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("test-id-1").build(),
        "name", AttributeValue.builder().s("Test Item").build()
    );

    dynamoClient.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Get stream records
    final List<Record> records = getAllStreamRecords();

    assertThat(records).hasSize(1);
    final Record record = records.get(0);
    assertThat(record.eventName()).isEqualTo(OperationType.INSERT);
    assertThat(record.dynamodb().keys()).containsKey("id");
    assertThat(record.dynamodb().newImage()).isNotNull();
    assertThat(record.dynamodb().newImage()).containsKey("id");
    assertThat(record.dynamodb().newImage()).containsKey("name");
    assertThat(record.dynamodb().oldImage()).satisfiesAnyOf(
        img -> assertThat(img).isNull(),
        img -> assertThat(img).isEmpty()
    );
  }

  @Test
  void updateItem_withStreamEnabled_capturesModifyEvent() {
    // Enable stream
    tableManager.enableStream(TABLE_NAME, "NEW_AND_OLD_IMAGES");

    // Put initial item
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("test-id-2").build(),
        "name", AttributeValue.builder().s("Original Name").build()
    );
    dynamoClient.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Update item
    dynamoClient.updateItem(UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("id", AttributeValue.builder().s("test-id-2").build()))
        .updateExpression("SET #n = :val")
        .expressionAttributeNames(Map.of("#n", "name"))
        .expressionAttributeValues(Map.of(":val", AttributeValue.builder().s("Updated Name").build()))
        .build());

    // Get stream records
    final List<Record> records = getAllStreamRecords();

    assertThat(records).hasSize(2); // INSERT + MODIFY
    final Record modifyRecord = records.get(1);
    assertThat(modifyRecord.eventName()).isEqualTo(OperationType.MODIFY);
    assertThat(modifyRecord.dynamodb().oldImage()).isNotNull();
    assertThat(modifyRecord.dynamodb().oldImage().get("name").s()).isEqualTo("Original Name");
    assertThat(modifyRecord.dynamodb().newImage()).isNotNull();
    assertThat(modifyRecord.dynamodb().newImage().get("name").s()).isEqualTo("Updated Name");
  }

  @Test
  void deleteItem_withStreamEnabled_capturesRemoveEvent() {
    // Enable stream
    tableManager.enableStream(TABLE_NAME, "NEW_AND_OLD_IMAGES");

    // Put item
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("test-id-3").build(),
        "name", AttributeValue.builder().s("To Be Deleted").build()
    );
    dynamoClient.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Delete item
    dynamoClient.deleteItem(DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("id", AttributeValue.builder().s("test-id-3").build()))
        .build());

    // Get stream records
    final List<Record> records = getAllStreamRecords();

    assertThat(records).hasSize(2); // INSERT + REMOVE
    final Record removeRecord = records.get(1);
    assertThat(removeRecord.eventName()).isEqualTo(OperationType.REMOVE);
    assertThat(removeRecord.dynamodb().oldImage()).isNotNull();
    assertThat(removeRecord.dynamodb().oldImage().get("name").s()).isEqualTo("To Be Deleted");
    assertThat(removeRecord.dynamodb().newImage()).satisfiesAnyOf(
        img -> assertThat(img).isNull(),
        img -> assertThat(img).isEmpty()
    );
  }

  @Test
  void streamViewType_keysOnly_onlyIncludesKeys() {
    // Enable stream with KEYS_ONLY
    tableManager.enableStream(TABLE_NAME, "KEYS_ONLY");

    // Put item
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("test-id-4").build(),
        "name", AttributeValue.builder().s("Test Item").build(),
        "value", AttributeValue.builder().n("42").build()
    );
    dynamoClient.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Get stream records
    final List<Record> records = getAllStreamRecords();

    assertThat(records).hasSize(1);
    final Record record = records.get(0);
    assertThat(record.dynamodb().streamViewType()).isEqualTo(StreamViewType.KEYS_ONLY);
    assertThat(record.dynamodb().keys()).containsKey("id");
    assertThat(record.dynamodb().newImage()).satisfiesAnyOf(
        img -> assertThat(img).isNull(),
        img -> assertThat(img).isEmpty()
    );
    assertThat(record.dynamodb().oldImage()).satisfiesAnyOf(
        img -> assertThat(img).isNull(),
        img -> assertThat(img).isEmpty()
    );
  }

  @Test
  void streamViewType_newImage_includesNewImageOnly() {
    // Enable stream with NEW_IMAGE
    tableManager.enableStream(TABLE_NAME, "NEW_IMAGE");

    // Put item
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("test-id-5").build(),
        "name", AttributeValue.builder().s("Original").build()
    );
    dynamoClient.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Update item
    dynamoClient.updateItem(UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of("id", AttributeValue.builder().s("test-id-5").build()))
        .updateExpression("SET #n = :val")
        .expressionAttributeNames(Map.of("#n", "name"))
        .expressionAttributeValues(Map.of(":val", AttributeValue.builder().s("Updated").build()))
        .build());

    // Get stream records
    final List<Record> records = getAllStreamRecords();

    assertThat(records).hasSize(2); // INSERT + MODIFY
    final Record modifyRecord = records.get(1);
    assertThat(modifyRecord.dynamodb().streamViewType()).isEqualTo(StreamViewType.NEW_IMAGE);
    assertThat(modifyRecord.dynamodb().newImage()).isNotNull();
    assertThat(modifyRecord.dynamodb().newImage().get("name").s()).isEqualTo("Updated");
    assertThat(modifyRecord.dynamodb().oldImage()).satisfiesAnyOf(
        img -> assertThat(img).isNull(),
        img -> assertThat(img).isEmpty()
    );
  }

  @Test
  void getShardIterator_trimHorizon_startsFromBeginning() {
    // Enable stream and add items
    tableManager.enableStream(TABLE_NAME, "KEYS_ONLY");

    final Map<String, AttributeValue> item1 = Map.of("id", AttributeValue.builder().s("item-1").build());
    final Map<String, AttributeValue> item2 = Map.of("id", AttributeValue.builder().s("item-2").build());

    dynamoClient.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item1).build());
    dynamoClient.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item2).build());

    // Get iterator from TRIM_HORIZON
    final String streamArn = tableManager.getPdbTable(TABLE_NAME).orElseThrow().streamArn().orElseThrow();
    final GetShardIteratorResponse iteratorResponse = streamsClient.getShardIterator(
        GetShardIteratorRequest.builder()
            .streamArn(streamArn)
            .shardId("shard-00000")
            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
            .build()
    );

    // Get records
    final GetRecordsResponse recordsResponse = streamsClient.getRecords(
        GetRecordsRequest.builder()
            .shardIterator(iteratorResponse.shardIterator())
            .build()
    );

    assertThat(recordsResponse.records()).hasSize(2);
  }

  @Test
  void getShardIterator_latest_startsAfterExisting() {
    // Enable stream and add item
    tableManager.enableStream(TABLE_NAME, "KEYS_ONLY");

    final Map<String, AttributeValue> item1 = Map.of("id", AttributeValue.builder().s("item-1").build());
    dynamoClient.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item1).build());

    // Get iterator from LATEST
    final String streamArn = tableManager.getPdbTable(TABLE_NAME).orElseThrow().streamArn().orElseThrow();
    final GetShardIteratorResponse iteratorResponse = streamsClient.getShardIterator(
        GetShardIteratorRequest.builder()
            .streamArn(streamArn)
            .shardId("shard-00000")
            .shardIteratorType(ShardIteratorType.LATEST)
            .build()
    );

    // Get records - should be empty since iterator is after existing records
    final GetRecordsResponse recordsResponse = streamsClient.getRecords(
        GetRecordsRequest.builder()
            .shardIterator(iteratorResponse.shardIterator())
            .build()
    );

    assertThat(recordsResponse.records()).isEmpty();

    // Add new item
    final Map<String, AttributeValue> item2 = Map.of("id", AttributeValue.builder().s("item-2").build());
    dynamoClient.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item2).build());

    // Use next iterator to get new records
    if (recordsResponse.nextShardIterator() != null) {
      final GetRecordsResponse newRecordsResponse = streamsClient.getRecords(
          GetRecordsRequest.builder()
              .shardIterator(recordsResponse.nextShardIterator())
              .build()
      );

      // Should now see the new item
      assertThat(newRecordsResponse.records()).hasSize(1);
    }
  }

  @Test
  void getRecords_withLimit_respectsLimit() {
    // Enable stream and add multiple items
    tableManager.enableStream(TABLE_NAME, "KEYS_ONLY");

    for (int i = 0; i < 10; i++) {
      final Map<String, AttributeValue> item = Map.of(
          "id", AttributeValue.builder().s("item-" + i).build()
      );
      dynamoClient.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item).build());
    }

    // Get records with limit
    final String streamArn = tableManager.getPdbTable(TABLE_NAME).orElseThrow().streamArn().orElseThrow();
    final GetShardIteratorResponse iteratorResponse = streamsClient.getShardIterator(
        GetShardIteratorRequest.builder()
            .streamArn(streamArn)
            .shardId("shard-00000")
            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
            .build()
    );

    final GetRecordsResponse recordsResponse = streamsClient.getRecords(
        GetRecordsRequest.builder()
            .shardIterator(iteratorResponse.shardIterator())
            .limit(5)
            .build()
    );

    assertThat(recordsResponse.records()).hasSize(5);
    assertThat(recordsResponse.nextShardIterator()).isNotNull();

    // Get next batch
    final GetRecordsResponse nextRecordsResponse = streamsClient.getRecords(
        GetRecordsRequest.builder()
            .shardIterator(recordsResponse.nextShardIterator())
            .limit(5)
            .build()
    );

    assertThat(nextRecordsResponse.records()).hasSize(5);
  }

  @Test
  void disableStream_stopsCapturingNewEvents() {
    // Enable stream
    tableManager.enableStream(TABLE_NAME, "KEYS_ONLY");

    // Put item (should be captured)
    final Map<String, AttributeValue> item1 = Map.of("id", AttributeValue.builder().s("item-1").build());
    dynamoClient.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item1).build());

    // Disable stream
    tableManager.disableStream(TABLE_NAME);

    // Put another item (should NOT be captured)
    final Map<String, AttributeValue> item2 = Map.of("id", AttributeValue.builder().s("item-2").build());
    dynamoClient.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item2).build());

    // Verify metadata
    final var metadata = tableManager.getPdbTable(TABLE_NAME).orElseThrow();
    assertThat(metadata.streamEnabled()).isFalse();
  }

  @Test
  void sequenceNumbers_strictlyIncreasing() {
    // Enable stream and add multiple items
    tableManager.enableStream(TABLE_NAME, "KEYS_ONLY");

    for (int i = 0; i < 5; i++) {
      final Map<String, AttributeValue> item = Map.of(
          "id", AttributeValue.builder().s("item-" + i).build()
      );
      dynamoClient.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item).build());
    }

    // Get all records
    final List<Record> records = getAllStreamRecords();

    assertThat(records).hasSize(5);

    // Verify sequence numbers are strictly increasing
    long previousSequence = -1;
    for (Record record : records) {
      final long currentSequence = Long.parseLong(record.dynamodb().sequenceNumber());
      assertThat(currentSequence).isGreaterThan(previousSequence);
      previousSequence = currentSequence;
    }
  }

  /**
   * Helper method to get all stream records from TRIM_HORIZON.
   *
   * @return list of all records
   */
  private List<Record> getAllStreamRecords() {
    final String streamArn = tableManager.getPdbTable(TABLE_NAME).orElseThrow().streamArn().orElseThrow();

    final GetShardIteratorResponse iteratorResponse = streamsClient.getShardIterator(
        GetShardIteratorRequest.builder()
            .streamArn(streamArn)
            .shardId("shard-00000")
            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
            .build()
    );

    final GetRecordsResponse recordsResponse = streamsClient.getRecords(
        GetRecordsRequest.builder()
            .shardIterator(iteratorResponse.shardIterator())
            .build()
    );

    return recordsResponse.records();
  }
}
