package io.github.pretenderdb.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.converter.ItemConverter;
import io.github.pretenderdb.dao.PdbItemDao;
import io.github.pretenderdb.expression.ConditionExpressionParser;
import io.github.pretenderdb.expression.KeyConditionExpressionParser;
import io.github.pretenderdb.expression.UpdateExpressionParser;
import io.github.pretenderdb.model.ImmutablePdbItem;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbItem;
import io.github.pretenderdb.model.PdbMetadata;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

@ExtendWith(MockitoExtension.class)
class PdbItemManagerTest {

  private static final String TABLE_NAME = "TestTable";
  private static final String ITEM_TABLE_NAME = "pdb_item_testtable";
  private static final String HASH_KEY = "id";
  private static final String SORT_KEY = "timestamp";
  private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");

  @Mock private PdbTableManager tableManager;
  @Mock private PdbItemTableManager itemTableManager;
  @Mock private PdbItemDao itemDao;
  @Mock private ItemConverter itemConverter;
  @Mock private AttributeValueConverter attributeValueConverter;
  @Mock private ConditionExpressionParser conditionExpressionParser;
  @Mock private KeyConditionExpressionParser keyConditionExpressionParser;
  @Mock private UpdateExpressionParser updateExpressionParser;
  @Mock private GsiProjectionHelper gsiProjectionHelper;
  @Mock private io.github.pretenderdb.helper.StreamCaptureHelper streamCaptureHelper;
  @Mock private io.github.pretenderdb.helper.AttributeEncryptionHelper encryptionHelper;
  @Mock private io.github.pretenderdb.util.CapacityCalculator capacityCalculator;
  @Mock private Clock clock;
  @Mock private org.jdbi.v3.core.Jdbi jdbi;

  private PdbItemManager manager;
  private PdbMetadata metadata;

  @BeforeEach
  void setup() {
    manager = new PdbItemManager(tableManager, itemTableManager, itemDao, itemConverter,
        attributeValueConverter, conditionExpressionParser, keyConditionExpressionParser, updateExpressionParser,
        gsiProjectionHelper, streamCaptureHelper, encryptionHelper, capacityCalculator, clock, jdbi);

    metadata = ImmutablePdbMetadata.builder()
        .name(TABLE_NAME)
        .hashKey(HASH_KEY)
        .sortKey(SORT_KEY)
        .createDate(NOW)
        .build();

    // Mock for item size validation (lenient because not all tests call putItem/updateItem)
    org.mockito.Mockito.lenient().when(attributeValueConverter.toJson(any())).thenReturn("{}");

    // Mock for encryption helper - pass through by default (no encryption)
    org.mockito.Mockito.lenient().when(encryptionHelper.encryptAttributes(any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
    org.mockito.Mockito.lenient().when(encryptionHelper.decryptAttributes(any(), any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
  }

  @Test
  void putItem_success() {
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01").build(),
        "name", AttributeValue.builder().s("Test").build()
    );

    final PdbItem pdbItem = ImmutablePdbItem.builder()
        .tableName(TABLE_NAME)
        .hashKeyValue("123")
        .sortKeyValue("2024-01-01")
        .attributesJson("{}")
        .createDate(NOW)
        .updateDate(NOW)
        .build();

    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(itemConverter.toPdbItem(TABLE_NAME, item, metadata)).thenReturn(pdbItem);
    when(itemDao.insert(ITEM_TABLE_NAME, pdbItem)).thenReturn(true);

    final PutItemRequest request = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build();

    manager.putItem(request);

    verify(itemDao).insert(ITEM_TABLE_NAME, pdbItem);
  }

  @Test
  void putItem_tableNotFound() {
    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.empty());

    final PutItemRequest request = PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(Map.of())
        .build();

    assertThatThrownBy(() -> manager.putItem(request))
        .isInstanceOf(ResourceNotFoundException.class)
        .hasMessageContaining("Table not found");
  }

  @Test
  void getItem_found() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("123").build(),
        SORT_KEY, AttributeValue.builder().s("2024-01-01").build()
    );

    final PdbItem pdbItem = ImmutablePdbItem.builder()
        .tableName(TABLE_NAME)
        .hashKeyValue("123")
        .sortKeyValue("2024-01-01")
        .attributesJson("{\"id\": {\"S\": \"123\"}, \"name\": {\"S\": \"Test\"}}")
        .createDate(NOW)
        .updateDate(NOW)
        .build();

    final Map<String, AttributeValue> expectedItem = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "name", AttributeValue.builder().s("Test").build()
    );

    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(attributeValueConverter.extractKeyValue(key, HASH_KEY)).thenReturn("123");
    when(attributeValueConverter.extractKeyValue(key, SORT_KEY)).thenReturn("2024-01-01");
    when(itemDao.get(ITEM_TABLE_NAME, "123", Optional.of("2024-01-01")))
        .thenReturn(Optional.of(pdbItem));
    when(attributeValueConverter.fromJson(pdbItem.attributesJson())).thenReturn(expectedItem);

    final GetItemRequest request = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build();

    final GetItemResponse response = manager.getItem(request);

    assertThat(response.item()).isEqualTo(expectedItem);
  }

  @Test
  void getItem_notFound() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("123").build()
    );

    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(attributeValueConverter.extractKeyValue(key, HASH_KEY)).thenReturn("123");
    when(itemDao.get(eq(ITEM_TABLE_NAME), eq("123"), any()))
        .thenReturn(Optional.empty());

    final GetItemRequest request = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build();

    final GetItemResponse response = manager.getItem(request);

    assertThat(response.hasItem()).isFalse();
  }

  @Test
  void getItem_withProjection() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("123").build()
    );

    final PdbItem pdbItem = ImmutablePdbItem.builder()
        .tableName(TABLE_NAME)
        .hashKeyValue("123")
        .attributesJson("{}")
        .createDate(NOW)
        .updateDate(NOW)
        .build();

    final Map<String, AttributeValue> fullItem = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "name", AttributeValue.builder().s("Test").build(),
        "age", AttributeValue.builder().n("30").build()
    );

    final Map<String, AttributeValue> projectedItem = Map.of(
        "name", AttributeValue.builder().s("Test").build()
    );

    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(attributeValueConverter.extractKeyValue(key, HASH_KEY)).thenReturn("123");
    when(itemDao.get(eq(ITEM_TABLE_NAME), eq("123"), any()))
        .thenReturn(Optional.of(pdbItem));
    when(attributeValueConverter.fromJson(pdbItem.attributesJson())).thenReturn(fullItem);
    when(itemConverter.applyProjection(any(), eq("name"), any())).thenReturn(projectedItem);

    final GetItemRequest request = GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .projectionExpression("name")
        .build();

    final GetItemResponse response = manager.getItem(request);

    assertThat(response.item()).isEqualTo(projectedItem);
  }

  @Test
  void updateItem_existingItem() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("123").build()
    );

    final PdbItem existingPdbItem = ImmutablePdbItem.builder()
        .tableName(TABLE_NAME)
        .hashKeyValue("123")
        .attributesJson("{}")
        .createDate(NOW)
        .updateDate(NOW)
        .build();

    final Map<String, AttributeValue> existingAttributes = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "count", AttributeValue.builder().n("5").build()
    );

    final Map<String, AttributeValue> updatedAttributes = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "count", AttributeValue.builder().n("8").build()
    );

    final PdbItem updatedPdbItem = ImmutablePdbItem.builder()
        .tableName(TABLE_NAME)
        .hashKeyValue("123")
        .attributesJson("{}")
        .createDate(NOW)
        .updateDate(NOW)
        .build();

    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(attributeValueConverter.extractKeyValue(key, HASH_KEY)).thenReturn("123");
    when(itemDao.get(eq(ITEM_TABLE_NAME), eq("123"), any()))
        .thenReturn(Optional.of(existingPdbItem));
    when(attributeValueConverter.fromJson(existingPdbItem.attributesJson()))
        .thenReturn(existingAttributes);
    when(updateExpressionParser.applyUpdate(any(), anyString(), any(), any()))
        .thenReturn(updatedAttributes);
    when(itemConverter.updatePdbItem(existingPdbItem, updatedAttributes, metadata))
        .thenReturn(updatedPdbItem);
    when(itemDao.update(ITEM_TABLE_NAME, updatedPdbItem)).thenReturn(true);

    final UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .updateExpression("SET count = count + :inc")
        .expressionAttributeValues(Map.of(":inc", AttributeValue.builder().n("3").build()))
        .returnValues(ReturnValue.ALL_NEW)
        .build();

    final UpdateItemResponse response = manager.updateItem(request);

    assertThat(response.attributes()).isEqualTo(updatedAttributes);
    verify(itemDao).update(ITEM_TABLE_NAME, updatedPdbItem);
  }

  @Test
  void updateItem_newItem() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("123").build()
    );

    final Map<String, AttributeValue> updatedAttributes = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "count", AttributeValue.builder().n("1").build()
    );

    final PdbItem updatedPdbItem = ImmutablePdbItem.builder()
        .tableName(TABLE_NAME)
        .hashKeyValue("123")
        .attributesJson("{}")
        .createDate(NOW)
        .updateDate(NOW)
        .build();

    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(attributeValueConverter.extractKeyValue(key, HASH_KEY)).thenReturn("123");
    when(itemDao.get(eq(ITEM_TABLE_NAME), eq("123"), any()))
        .thenReturn(Optional.empty());
    when(updateExpressionParser.applyUpdate(any(), anyString(), any(), any()))
        .thenReturn(updatedAttributes);
    when(itemConverter.toPdbItem(TABLE_NAME, updatedAttributes, metadata))
        .thenReturn(updatedPdbItem);
    when(itemDao.insert(ITEM_TABLE_NAME, updatedPdbItem)).thenReturn(true);

    final UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .updateExpression("SET count = :val")
        .expressionAttributeValues(Map.of(":val", AttributeValue.builder().n("1").build()))
        .build();

    manager.updateItem(request);

    verify(itemDao).insert(ITEM_TABLE_NAME, updatedPdbItem);
  }

  @Test
  void deleteItem_withReturnValues() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("123").build()
    );

    final PdbItem pdbItem = ImmutablePdbItem.builder()
        .tableName(TABLE_NAME)
        .hashKeyValue("123")
        .attributesJson("{}")
        .createDate(NOW)
        .updateDate(NOW)
        .build();

    final Map<String, AttributeValue> oldItem = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "name", AttributeValue.builder().s("Test").build()
    );

    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(attributeValueConverter.extractKeyValue(key, HASH_KEY)).thenReturn("123");
    when(itemDao.get(eq(ITEM_TABLE_NAME), eq("123"), any()))
        .thenReturn(Optional.of(pdbItem));
    when(attributeValueConverter.fromJson(pdbItem.attributesJson())).thenReturn(oldItem);
    when(itemDao.delete(eq(ITEM_TABLE_NAME), eq("123"), any())).thenReturn(true);

    final DeleteItemRequest request = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .returnValues(ReturnValue.ALL_OLD)
        .build();

    final DeleteItemResponse response = manager.deleteItem(request);

    assertThat(response.attributes()).isEqualTo(oldItem);
    verify(itemDao).delete(eq(ITEM_TABLE_NAME), eq("123"), any());
  }

  @Test
  void deleteItem_noReturnValues() {
    final Map<String, AttributeValue> key = Map.of(
        HASH_KEY, AttributeValue.builder().s("123").build()
    );

    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(attributeValueConverter.extractKeyValue(key, HASH_KEY)).thenReturn("123");
    when(itemDao.delete(eq(ITEM_TABLE_NAME), eq("123"), any())).thenReturn(true);

    final DeleteItemRequest request = DeleteItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(key)
        .build();

    final DeleteItemResponse response = manager.deleteItem(request);

    assertThat(response.hasAttributes()).isFalse();
  }

  @Test
  void query_success() {
    final Map<String, AttributeValue> values = Map.of(
        ":id", AttributeValue.builder().s("123").build()
    );

    final KeyConditionExpressionParser.ParsedKeyCondition condition =
        new KeyConditionExpressionParser.ParsedKeyCondition(
            "123", null, Optional.empty());

    final PdbItem item1 = ImmutablePdbItem.builder()
        .tableName(TABLE_NAME)
        .hashKeyValue("123")
        .sortKeyValue("2024-01-01")
        .attributesJson("{}")
        .createDate(NOW)
        .updateDate(NOW)
        .build();

    final Map<String, AttributeValue> attr1 = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "timestamp", AttributeValue.builder().s("2024-01-01").build()
    );

    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(keyConditionExpressionParser.parse(eq("id = :id"), eq(values), any())).thenReturn(condition);
    when(itemDao.query(eq(ITEM_TABLE_NAME), eq("123"), eq(null), eq(Optional.empty()), eq(101),
        eq(Optional.empty()), eq(Optional.empty())))
        .thenReturn(List.of(item1));
    when(attributeValueConverter.fromJson(item1.attributesJson())).thenReturn(attr1);

    final QueryRequest request = QueryRequest.builder()
        .tableName(TABLE_NAME)
        .keyConditionExpression("id = :id")
        .expressionAttributeValues(values)
        .build();

    final QueryResponse response = manager.query(request);

    assertThat(response.items()).hasSize(1);
    assertThat(response.items().get(0)).isEqualTo(attr1);
    assertThat(response.count()).isEqualTo(1);
  }

  @Test
  void scan_withPagination() {
    final PdbItem item1 = ImmutablePdbItem.builder()
        .tableName(TABLE_NAME)
        .hashKeyValue("123")
        .attributesJson("{}")
        .createDate(NOW)
        .updateDate(NOW)
        .build();

    final Map<String, AttributeValue> attr1 = Map.of(
        "id", AttributeValue.builder().s("123").build()
    );

    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(itemDao.scan(eq(ITEM_TABLE_NAME), eq(101), eq(Optional.empty()), eq(Optional.empty())))
        .thenReturn(List.of(item1));
    when(attributeValueConverter.fromJson(item1.attributesJson())).thenReturn(attr1);

    final ScanRequest request = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .build();

    final ScanResponse response = manager.scan(request);

    assertThat(response.items()).hasSize(1);
    assertThat(response.count()).isEqualTo(1);
  }
}
