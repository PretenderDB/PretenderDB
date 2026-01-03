package io.github.pretenderdb.endToEnd;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

/**
 * End-to-end tests for Expression Attribute Names (#placeholder) support.
 * Tests the complete flow through all layers: Client -> Manager -> Parser -> DAO.
 */
public class ExpressionAttributeNamesTest extends BaseEndToEndTest {

  @Test
  void query_withExpressionAttributeNamesInKeyCondition() {
    final DynamoDbClient client = component.dynamoDbPretenderClient();

    // Create table with hash and sort key
    client.createTable(CreateTableRequest.builder()
        .tableName("Users")
        .keySchema(
            KeySchemaElement.builder().attributeName("userId").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("timestamp").keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("userId").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName("timestamp").attributeType(ScalarAttributeType.S).build()
        )
        .build());

    // Put multiple items
    client.putItem(PutItemRequest.builder()
        .tableName("Users")
        .item(Map.of(
            "userId", AttributeValue.builder().s("user-123").build(),
            "timestamp", AttributeValue.builder().s("2024-01-01T10:00:00Z").build(),
            "data", AttributeValue.builder().s("record-1").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName("Users")
        .item(Map.of(
            "userId", AttributeValue.builder().s("user-123").build(),
            "timestamp", AttributeValue.builder().s("2024-01-02T10:00:00Z").build(),
            "data", AttributeValue.builder().s("record-2").build()
        ))
        .build());

    // Query using expression attribute names for both hash and sort keys
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName("Users")
        .keyConditionExpression("#pk = :uid AND #sk > :ts")
        .expressionAttributeNames(Map.of(
            "#pk", "userId",
            "#sk", "timestamp"
        ))
        .expressionAttributeValues(Map.of(
            ":uid", AttributeValue.builder().s("user-123").build(),
            ":ts", AttributeValue.builder().s("2024-01-01T12:00:00Z").build()
        ))
        .build());

    assertThat(response.count()).isEqualTo(1);
    assertThat(response.items().get(0).get("data").s()).isEqualTo("record-2");
  }

  @Test
  void updateItem_withExpressionAttributeNamesInUpdateExpression() {
    final DynamoDbClient client = component.dynamoDbPretenderClient();

    // Create table
    client.createTable(CreateTableRequest.builder()
        .tableName("Products")
        .keySchema(
            KeySchemaElement.builder().attributeName("productId").keyType(KeyType.HASH).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("productId").attributeType(ScalarAttributeType.S).build()
        )
        .build());

    // Put item
    client.putItem(PutItemRequest.builder()
        .tableName("Products")
        .item(Map.of(
            "productId", AttributeValue.builder().s("prod-001").build(),
            "name", AttributeValue.builder().s("Widget").build(),
            "status", AttributeValue.builder().s("INACTIVE").build(),
            "count", AttributeValue.builder().n("10").build()
        ))
        .build());

    // Update using expression attribute names (useful for reserved words like 'status' and 'name')
    final UpdateItemResponse updateResponse = client.updateItem(UpdateItemRequest.builder()
        .tableName("Products")
        .key(Map.of(
            "productId", AttributeValue.builder().s("prod-001").build()
        ))
        .updateExpression("SET #s = :newStatus, #n = :newName, #c = #c + :inc")
        .expressionAttributeNames(Map.of(
            "#s", "status",
            "#n", "name",
            "#c", "count"
        ))
        .expressionAttributeValues(Map.of(
            ":newStatus", AttributeValue.builder().s("ACTIVE").build(),
            ":newName", AttributeValue.builder().s("Super Widget").build(),
            ":inc", AttributeValue.builder().n("5").build()
        ))
        .returnValues(ReturnValue.ALL_NEW)
        .build());

    assertThat(updateResponse.attributes().get("status").s()).isEqualTo("ACTIVE");
    assertThat(updateResponse.attributes().get("name").s()).isEqualTo("Super Widget");
    assertThat(updateResponse.attributes().get("count").n()).isEqualTo("15");
  }

  @Test
  void query_withExpressionAttributeNamesOnGSI() {
    final DynamoDbClient client = component.dynamoDbPretenderClient();

    // Create table with GSI
    client.createTable(CreateTableRequest.builder()
        .tableName("Orders")
        .keySchema(
            KeySchemaElement.builder().attributeName("orderId").keyType(KeyType.HASH).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("orderId").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName("status").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName("date").attributeType(ScalarAttributeType.S).build()
        )
        .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
            .indexName("StatusDateIndex")
            .keySchema(
                KeySchemaElement.builder().attributeName("status").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("date").keyType(KeyType.RANGE).build()
            )
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .build())
        .build());

    // Put items
    client.putItem(PutItemRequest.builder()
        .tableName("Orders")
        .item(Map.of(
            "orderId", AttributeValue.builder().s("order-001").build(),
            "status", AttributeValue.builder().s("PENDING").build(),
            "date", AttributeValue.builder().s("2024-01-01").build(),
            "amount", AttributeValue.builder().n("100").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName("Orders")
        .item(Map.of(
            "orderId", AttributeValue.builder().s("order-002").build(),
            "status", AttributeValue.builder().s("PENDING").build(),
            "date", AttributeValue.builder().s("2024-01-15").build(),
            "amount", AttributeValue.builder().n("200").build()
        ))
        .build());

    // Query GSI using expression attribute names (status and date are reserved words in some contexts)
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName("Orders")
        .indexName("StatusDateIndex")
        .keyConditionExpression("#s = :status AND #d > :date")
        .expressionAttributeNames(Map.of(
            "#s", "status",
            "#d", "date"
        ))
        .expressionAttributeValues(Map.of(
            ":status", AttributeValue.builder().s("PENDING").build(),
            ":date", AttributeValue.builder().s("2024-01-10").build()
        ))
        .build());

    assertThat(response.count()).isEqualTo(1);
    assertThat(response.items().get(0).get("orderId").s()).isEqualTo("order-002");
    assertThat(response.items().get(0).get("amount").n()).isEqualTo("200");
  }

  @Test
  void query_withBeginsWithAndExpressionAttributeName() {
    final DynamoDbClient client = component.dynamoDbPretenderClient();

    // Create table
    client.createTable(CreateTableRequest.builder()
        .tableName("Documents")
        .keySchema(
            KeySchemaElement.builder().attributeName("docId").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("version").keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("docId").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName("version").attributeType(ScalarAttributeType.S).build()
        )
        .build());

    // Put items with version prefixes
    client.putItem(PutItemRequest.builder()
        .tableName("Documents")
        .item(Map.of(
            "docId", AttributeValue.builder().s("doc-123").build(),
            "version", AttributeValue.builder().s("v1.0.0").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName("Documents")
        .item(Map.of(
            "docId", AttributeValue.builder().s("doc-123").build(),
            "version", AttributeValue.builder().s("v1.1.0").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName("Documents")
        .item(Map.of(
            "docId", AttributeValue.builder().s("doc-123").build(),
            "version", AttributeValue.builder().s("v2.0.0").build()
        ))
        .build());

    // Query using begins_with with expression attribute names
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName("Documents")
        .keyConditionExpression("#id = :docId AND begins_with(#ver, :prefix)")
        .expressionAttributeNames(Map.of(
            "#id", "docId",
            "#ver", "version"
        ))
        .expressionAttributeValues(Map.of(
            ":docId", AttributeValue.builder().s("doc-123").build(),
            ":prefix", AttributeValue.builder().s("v1").build()
        ))
        .build());

    assertThat(response.count()).isEqualTo(2);
    assertThat(response.items()).extracting(item -> item.get("version").s())
        .containsExactlyInAnyOrder("v1.0.0", "v1.1.0");
  }

  @Test
  void updateItem_removeAttributeWithExpressionAttributeName() {
    final DynamoDbClient client = component.dynamoDbPretenderClient();

    // Create table
    client.createTable(CreateTableRequest.builder()
        .tableName("Sessions")
        .keySchema(
            KeySchemaElement.builder().attributeName("sessionId").keyType(KeyType.HASH).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("sessionId").attributeType(ScalarAttributeType.S).build()
        )
        .build());

    // Put item
    client.putItem(PutItemRequest.builder()
        .tableName("Sessions")
        .item(Map.of(
            "sessionId", AttributeValue.builder().s("sess-001").build(),
            "token", AttributeValue.builder().s("secret-token").build(),
            "data", AttributeValue.builder().s("session-data").build()
        ))
        .build());

    // Remove attribute using expression attribute name
    client.updateItem(UpdateItemRequest.builder()
        .tableName("Sessions")
        .key(Map.of(
            "sessionId", AttributeValue.builder().s("sess-001").build()
        ))
        .updateExpression("REMOVE #t")
        .expressionAttributeNames(Map.of(
            "#t", "token"
        ))
        .build());

    // Verify token was removed
    final GetItemResponse getResponse = client.getItem(GetItemRequest.builder()
        .tableName("Sessions")
        .key(Map.of(
            "sessionId", AttributeValue.builder().s("sess-001").build()
        ))
        .build());

    assertThat(getResponse.item()).containsKeys("sessionId", "data");
    assertThat(getResponse.item()).doesNotContainKey("token");
  }

  @Test
  void query_withBetweenAndExpressionAttributeNames() {
    final DynamoDbClient client = component.dynamoDbPretenderClient();

    // Create table
    client.createTable(CreateTableRequest.builder()
        .tableName("Events")
        .keySchema(
            KeySchemaElement.builder().attributeName("eventType").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("timestamp").keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("eventType").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName("timestamp").attributeType(ScalarAttributeType.N).build()
        )
        .build());

    // Put items
    client.putItem(PutItemRequest.builder()
        .tableName("Events")
        .item(Map.of(
            "eventType", AttributeValue.builder().s("LOGIN").build(),
            "timestamp", AttributeValue.builder().n("1000").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName("Events")
        .item(Map.of(
            "eventType", AttributeValue.builder().s("LOGIN").build(),
            "timestamp", AttributeValue.builder().n("2000").build()
        ))
        .build());

    client.putItem(PutItemRequest.builder()
        .tableName("Events")
        .item(Map.of(
            "eventType", AttributeValue.builder().s("LOGIN").build(),
            "timestamp", AttributeValue.builder().n("3000").build()
        ))
        .build());

    // Query using BETWEEN with expression attribute names
    final QueryResponse response = client.query(QueryRequest.builder()
        .tableName("Events")
        .keyConditionExpression("#type = :eventType AND #ts BETWEEN :start AND :end")
        .expressionAttributeNames(Map.of(
            "#type", "eventType",
            "#ts", "timestamp"
        ))
        .expressionAttributeValues(Map.of(
            ":eventType", AttributeValue.builder().s("LOGIN").build(),
            ":start", AttributeValue.builder().n("1500").build(),
            ":end", AttributeValue.builder().n("2500").build()
        ))
        .build());

    assertThat(response.count()).isEqualTo(1);
    assertThat(response.items().get(0).get("timestamp").n()).isEqualTo("2000");
  }
}
