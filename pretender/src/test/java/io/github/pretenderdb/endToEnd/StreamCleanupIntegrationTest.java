package io.github.pretenderdb.endToEnd;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pretenderdb.DynamoDbPretenderClient;
import io.github.pretenderdb.DynamoDbStreamsPretenderClient;
import io.github.pretenderdb.manager.PdbTableManager;
import io.github.pretenderdb.service.StreamCleanupService;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;

/**
 * Integration tests for StreamCleanupService.
 */
class StreamCleanupIntegrationTest extends BaseEndToEndTest {

  private static final String TABLE_NAME = "CleanupTestTable";

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
  void cleanupService_startsAndStops() {
    final StreamCleanupService cleanupService = component.streamCleanupService();

    assertThat(cleanupService.isRunning()).isFalse();

    cleanupService.start();
    assertThat(cleanupService.isRunning()).isTrue();

    cleanupService.stop();
    assertThat(cleanupService.isRunning()).isFalse();
  }

  @Test
  void cleanupService_preservesRecentRecords() {
    // Enable stream
    tableManager.enableStream(TABLE_NAME, "KEYS_ONLY");

    // Add recent items (within 24 hours)
    for (int i = 0; i < 5; i++) {
      final Map<String, AttributeValue> item = Map.of(
          "id", AttributeValue.builder().s("recent-item-" + i).build()
      );
      dynamoClient.putItem(PutItemRequest.builder().tableName(TABLE_NAME).item(item).build());
    }

    // Verify 5 records exist via stream API
    int recordCount = getStreamRecordCount();
    assertThat(recordCount).isEqualTo(5);

    // Run cleanup service
    final StreamCleanupService cleanupService = component.streamCleanupService();
    cleanupService.runCleanup();

    // Verify all records still exist (nothing older than 24 hours)
    recordCount = getStreamRecordCount();
    assertThat(recordCount).isEqualTo(5);
  }

  /**
   * Helper to count stream records via API.
   */
  private int getStreamRecordCount() {
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

    return recordsResponse.records().size();
  }
}
