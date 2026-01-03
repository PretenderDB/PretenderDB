/*
 * Copyright (c) 2023. Ned Wolpert
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pretenderdb;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pretenderdb.dagger.PretenderComponent;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.model.Record;

/**
 * Comprehensive integration test using PostgreSQL via Testcontainers.
 * Validates all features work correctly with PostgreSQL:
 * - Item operations (CRUD, query, scan)
 * - Global Secondary Indexes (GSI)
 * - Expression support (KeyCondition, Update, Condition, Filter)
 * - DynamoDB Streams
 * - Batch operations
 */
class PostgreSQLIntegrationTest extends BasePostgreSQLTest {

  private static final String TABLE_NAME = "PostgresTestTable";
  private static final String HASH_KEY = "pk";
  private static final String SORT_KEY = "sk";

  private PretenderComponent component;
  private DynamoDbClient dynamoDbClient;
  private DynamoDbStreamsPretenderClient streamsClient;

  @BeforeEach
  void setupComponents() {
    component = PretenderComponent.instance(configuration);
    dynamoDbClient = component.dynamoDbPretenderClient();
    streamsClient = component.dynamoDbStreamsPretenderClient();
  }

  @AfterEach
  void cleanupTables() {
    try {
      dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
    } catch (ResourceNotFoundException e) {
      // Table may not exist
    }
  }

  @Test
  void fullCrudLifecycle_withPostgreSQL() {
    // Create table
    dynamoDbClient.createTable(CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName(HASH_KEY).attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName(SORT_KEY).attributeType(ScalarAttributeType.S).build()
        )
        .build());

    // Put item
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user-1").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01").build(),
        "name", AttributeValue.builder().s("John Doe").build(),
        "age", AttributeValue.builder().n("30").build()
    );

    dynamoDbClient.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // Get item
    final GetItemResponse getResponse = dynamoDbClient.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01").build()
        ))
        .build());

    assertThat(getResponse.item()).isNotNull();
    assertThat(getResponse.item().get("name").s()).isEqualTo("John Doe");
    assertThat(getResponse.item().get("age").n()).isEqualTo("30");

    // Update item
    final UpdateItemResponse updateResponse = dynamoDbClient.updateItem(UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01").build()
        ))
        .updateExpression("SET age = :newAge, #status = :statusVal")
        .expressionAttributeNames(Map.of("#status", "status"))
        .expressionAttributeValues(Map.of(
            ":newAge", AttributeValue.builder().n("31").build(),
            ":statusVal", AttributeValue.builder().s("active").build()
        ))
        .returnValues(ReturnValue.ALL_NEW)
        .build());

    assertThat(updateResponse.attributes().get("age").n()).isEqualTo("31");
    assertThat(updateResponse.attributes().get("status").s()).isEqualTo("active");

    // Delete item
    dynamoDbClient.deleteItem(DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01").build()
        ))
        .build());

    // Verify deletion
    final GetItemResponse deletedResponse = dynamoDbClient.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("user-1").build(),
            SORT_KEY, AttributeValue.builder().s("2024-01-01").build()
        ))
        .build());

    assertThat(deletedResponse.hasItem()).isFalse();
  }

  @Test
  void queryAndScan_withFilterExpression_withPostgreSQL() {
    // Create table and put test data
    dynamoDbClient.createTable(CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName(HASH_KEY).attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName(SORT_KEY).attributeType(ScalarAttributeType.S).build()
        )
        .build());

    // Put multiple items
    for (int i = 1; i <= 5; i++) {
      dynamoDbClient.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(Map.of(
              HASH_KEY, AttributeValue.builder().s("partition-1").build(),
              SORT_KEY, AttributeValue.builder().s("item-" + i).build(),
              "score", AttributeValue.builder().n(String.valueOf(i * 10)).build(),
              "status", AttributeValue.builder().s(i % 2 == 0 ? "active" : "inactive").build()
          ))
          .build());
    }

    // Query with FilterExpression
    final QueryResponse queryResponse = dynamoDbClient.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("#pk = :pk")
        .filterExpression("score > :minScore AND #status = :statusVal")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY, "#status", "status"))
        .expressionAttributeValues(Map.of(
            ":pk", AttributeValue.builder().s("partition-1").build(),
            ":minScore", AttributeValue.builder().n("20").build(),
            ":statusVal", AttributeValue.builder().s("active").build()
        ))
        .build());

    // Should return item-4 (score=40, active)
    assertThat(queryResponse.count()).isEqualTo(1);
    assertThat(queryResponse.items().get(0).get("score").n()).isEqualTo("40");

    // Scan with FilterExpression
    final ScanResponse scanResponse = dynamoDbClient.scan(ScanRequest.builder()
        .tableName(TABLE_NAME)
        .filterExpression("attribute_exists(score) AND score >= :threshold")
        .expressionAttributeValues(Map.of(
            ":threshold", AttributeValue.builder().n("30").build()
        ))
        .build());

    // Should return items with score >= 30 (item-3, item-4, item-5)
    assertThat(scanResponse.count()).isEqualTo(3);
  }

  @Test
  void globalSecondaryIndex_withPostgreSQL() {
    // Create table with GSI
    dynamoDbClient.createTable(CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName(HASH_KEY).attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName(SORT_KEY).attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName("gsi_pk").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName("gsi_sk").attributeType(ScalarAttributeType.S).build()
        )
        .globalSecondaryIndexes(
            GlobalSecondaryIndex.builder()
                .indexName("test-gsi")
                .keySchema(
                    KeySchemaElement.builder().attributeName("gsi_pk").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("gsi_sk").keyType(KeyType.RANGE).build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build()
        )
        .build());

    // Put items
    dynamoDbClient.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("main-1").build(),
            SORT_KEY, AttributeValue.builder().s("sort-1").build(),
            "gsi_pk", AttributeValue.builder().s("category-A").build(),
            "gsi_sk", AttributeValue.builder().s("2024-01-01").build(),
            "data", AttributeValue.builder().s("value1").build()
        ))
        .build());

    dynamoDbClient.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("main-2").build(),
            SORT_KEY, AttributeValue.builder().s("sort-2").build(),
            "gsi_pk", AttributeValue.builder().s("category-A").build(),
            "gsi_sk", AttributeValue.builder().s("2024-01-02").build(),
            "data", AttributeValue.builder().s("value2").build()
        ))
        .build());

    // Query GSI
    final QueryResponse gsiQueryResponse = dynamoDbClient.query(QueryRequest.builder()
        .tableName(TABLE_NAME)
        .indexName("test-gsi")
        .keyConditionExpression("gsi_pk = :gsi_pk")
        .expressionAttributeValues(Map.of(
            ":gsi_pk", AttributeValue.builder().s("category-A").build()
        ))
        .build());

    assertThat(gsiQueryResponse.count()).isEqualTo(2);
    assertThat(gsiQueryResponse.items()).allMatch(item -> item.get("gsi_pk").s().equals("category-A"));
  }

  @Test
  void dynamoDbStreams_captureAllEvents_withPostgreSQL() {
    // Create table
    dynamoDbClient.createTable(CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName(HASH_KEY).attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName(SORT_KEY).attributeType(ScalarAttributeType.S).build()
        )
        .build());

    // Enable streams
    final var tableManager = component.pdbTableManager();
    tableManager.enableStream(TABLE_NAME, "NEW_AND_OLD_IMAGES");

    // Get stream ARN from metadata
    final var metadata = tableManager.getPdbTable(TABLE_NAME).orElseThrow();
    final String streamArn = metadata.streamArn().orElseThrow();

    // INSERT event
    dynamoDbClient.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("stream-test").build(),
            SORT_KEY, AttributeValue.builder().s("v1").build(),
            "value", AttributeValue.builder().n("100").build()
        ))
        .build());

    // MODIFY event
    dynamoDbClient.updateItem(UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("stream-test").build(),
            SORT_KEY, AttributeValue.builder().s("v1").build()
        ))
        .updateExpression("SET #val = :newVal")
        .expressionAttributeNames(Map.of("#val", "value"))
        .expressionAttributeValues(Map.of(
            ":newVal", AttributeValue.builder().n("200").build()
        ))
        .build());

    // REMOVE event
    dynamoDbClient.deleteItem(DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("stream-test").build(),
            SORT_KEY, AttributeValue.builder().s("v1").build()
        ))
        .build());

    // Read stream records
    final DescribeStreamResponse describeResponse = streamsClient.describeStream(
        DescribeStreamRequest.builder()
            .streamArn(streamArn)
            .build());

    assertThat(describeResponse.streamDescription().shards()).isNotEmpty();

    // Get shard iterator
    final String shardId = describeResponse.streamDescription().shards().get(0).shardId();
    final GetShardIteratorResponse iteratorResponse = streamsClient.getShardIterator(
        GetShardIteratorRequest.builder()
            .streamArn(streamArn)
            .shardId(shardId)
            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
            .build());

    // Get records
    final GetRecordsResponse recordsResponse = streamsClient.getRecords(
        GetRecordsRequest.builder()
            .shardIterator(iteratorResponse.shardIterator())
            .build());

    final List<Record> records = recordsResponse.records();
    assertThat(records).hasSize(3);

    // Verify INSERT event
    final Record insertRecord = records.get(0);
    assertThat(insertRecord.eventName().toString()).isEqualTo("INSERT");
    assertThat(insertRecord.dynamodb().newImage()).isNotNull();
    assertThat(insertRecord.dynamodb().newImage().get("value").n()).isEqualTo("100");
    // AWS SDK returns empty maps for unset fields instead of null
    assertThat(insertRecord.dynamodb().oldImage()).satisfiesAnyOf(
        image -> assertThat(image).isNull(),
        image -> assertThat(image).isEmpty()
    );

    // Verify MODIFY event
    final Record modifyRecord = records.get(1);
    assertThat(modifyRecord.eventName().toString()).isEqualTo("MODIFY");
    assertThat(modifyRecord.dynamodb().newImage()).isNotNull();
    assertThat(modifyRecord.dynamodb().newImage().get("value").n()).isEqualTo("200");
    assertThat(modifyRecord.dynamodb().oldImage()).isNotNull();
    assertThat(modifyRecord.dynamodb().oldImage().get("value").n()).isEqualTo("100");

    // Verify REMOVE event
    final Record removeRecord = records.get(2);
    assertThat(removeRecord.eventName().toString()).isEqualTo("REMOVE");
    assertThat(removeRecord.dynamodb().oldImage()).isNotNull();
    assertThat(removeRecord.dynamodb().oldImage().get("value").n()).isEqualTo("200");
    // AWS SDK returns empty maps for unset fields instead of null
    assertThat(removeRecord.dynamodb().newImage()).satisfiesAnyOf(
        image -> assertThat(image).isNull(),
        image -> assertThat(image).isEmpty()
    );
  }

  @Test
  void conditionalWrites_withPostgreSQL() {
    // Create table
    dynamoDbClient.createTable(CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName(HASH_KEY).attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName(SORT_KEY).attributeType(ScalarAttributeType.S).build()
        )
        .build());

    // Conditional put - should succeed (item doesn't exist)
    dynamoDbClient.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of(
            HASH_KEY, AttributeValue.builder().s("cond-1").build(),
            SORT_KEY, AttributeValue.builder().s("v1").build(),
            "version", AttributeValue.builder().n("1").build()
        ))
        .conditionExpression("attribute_not_exists(#pk)")
        .expressionAttributeNames(Map.of("#pk", HASH_KEY))
        .build());

    // Conditional update - should succeed (version matches)
    final UpdateItemResponse updateResponse = dynamoDbClient.updateItem(UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(
            HASH_KEY, AttributeValue.builder().s("cond-1").build(),
            SORT_KEY, AttributeValue.builder().s("v1").build()
        ))
        .updateExpression("SET version = version + :inc")
        .conditionExpression("version = :expectedVersion")
        .expressionAttributeValues(Map.of(
            ":inc", AttributeValue.builder().n("1").build(),
            ":expectedVersion", AttributeValue.builder().n("1").build()
        ))
        .returnValues(ReturnValue.ALL_NEW)
        .build());

    assertThat(updateResponse.attributes().get("version").n()).isEqualTo("2");
  }

  @Test
  void batchOperations_withPostgreSQL() {
    // Create table
    dynamoDbClient.createTable(CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName(HASH_KEY).attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName(SORT_KEY).attributeType(ScalarAttributeType.S).build()
        )
        .build());

    // Batch put items
    for (int i = 1; i <= 3; i++) {
      dynamoDbClient.putItem(PutItemRequest.builder()
          .tableName(TABLE_NAME)
          .item(Map.of(
              HASH_KEY, AttributeValue.builder().s("batch-" + i).build(),
              SORT_KEY, AttributeValue.builder().s("v1").build(),
              "data", AttributeValue.builder().s("batch-data-" + i).build()
          ))
          .build());
    }

    // Batch get items
    final var batchGetResponse = dynamoDbClient.batchGetItem(
        software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest.builder()
            .requestItems(Map.of(
                TABLE_NAME, software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes.builder()
                    .keys(
                        Map.of(
                            HASH_KEY, AttributeValue.builder().s("batch-1").build(),
                            SORT_KEY, AttributeValue.builder().s("v1").build()
                        ),
                        Map.of(
                            HASH_KEY, AttributeValue.builder().s("batch-2").build(),
                            SORT_KEY, AttributeValue.builder().s("v1").build()
                        ),
                        Map.of(
                            HASH_KEY, AttributeValue.builder().s("batch-3").build(),
                            SORT_KEY, AttributeValue.builder().s("v1").build()
                        )
                    )
                    .build()
            ))
            .build());

    assertThat(batchGetResponse.responses().get(TABLE_NAME)).hasSize(3);
    assertThat(batchGetResponse.responses().get(TABLE_NAME))
        .allMatch(item -> item.containsKey("data"));
  }
}
