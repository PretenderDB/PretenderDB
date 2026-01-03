package io.github.pretenderdb.endToEnd;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pretenderdb.DynamoDbPretenderClient;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

public class CreateTableTest extends BaseEndToEndTest {

  private static final String TABLE_NAME = "testTable";
  private static final String HASH_KEY = "theHashKey";
  private static final String SORT_KEY = "theSortKey";

  @Test
  void testCreateTable() {
    final DynamoDbPretenderClient client = component.dynamoDbPretenderClient();
    final List<KeySchemaElement> keySchemaElements = List.of(
        KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build(),
        KeySchemaElement.builder().attributeName(SORT_KEY).keyType(KeyType.RANGE).build()
    );
    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(keySchemaElements)
        .build();
    final CreateTableResponse createTableResponse = client.createTable(createTableRequest);
    assertThat(createTableResponse.tableDescription())
        .hasFieldOrPropertyWithValue("tableName", TABLE_NAME)
        .hasFieldOrPropertyWithValue("keySchema", keySchemaElements);
    assertThat(client.listTables(ListTablesRequest.builder().build())).hasFieldOrPropertyWithValue("tableNames", List.of(TABLE_NAME));

    final DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder().tableName(TABLE_NAME).build();
    assertThat(client.deleteTable(deleteTableRequest).tableDescription()).hasFieldOrPropertyWithValue("tableName", TABLE_NAME);

    assertThat(client.listTables(ListTablesRequest.builder().build())).hasFieldOrPropertyWithValue("tableNames", List.of());
  }

}
