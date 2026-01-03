package io.github.pretenderdb.helper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.dao.PdbMetadataDao;
import io.github.pretenderdb.dao.PdbStreamDao;
import io.github.pretenderdb.manager.PdbStreamTableManager;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbMetadata;
import io.github.pretenderdb.model.PdbStreamRecord;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@ExtendWith(MockitoExtension.class)
class StreamCaptureHelperTest {

  @Mock
  private PdbMetadataDao metadataDao;

  @Mock
  private PdbStreamDao streamDao;

  @Mock
  private PdbStreamTableManager streamTableManager;

  @Mock
  private AttributeValueConverter attributeValueConverter;

  @Captor
  private ArgumentCaptor<PdbStreamRecord> recordCaptor;

  private StreamCaptureHelper helper;

  @BeforeEach
  void setup() {
    helper = new StreamCaptureHelper(metadataDao, streamDao, streamTableManager, attributeValueConverter);
  }

  @Test
  void captureInsert_streamsNotEnabled_doesNothing() {
    final String tableName = "test-table";
    final Map<String, AttributeValue> item = createTestItem();

    // Table exists but streams not enabled
    when(metadataDao.getTable(tableName)).thenReturn(Optional.of(createMetadata(false, null)));

    helper.captureInsert(tableName, item);

    verify(streamDao, never()).insert(anyString(), any(PdbStreamRecord.class));
  }

  @Test
  void captureInsert_tableNotFound_doesNothing() {
    final String tableName = "nonexistent-table";
    final Map<String, AttributeValue> item = createTestItem();

    when(metadataDao.getTable(tableName)).thenReturn(Optional.empty());

    helper.captureInsert(tableName, item);

    verify(streamDao, never()).insert(anyString(), any(PdbStreamRecord.class));
  }

  @Test
  void captureInsert_keysOnly_createsCorrectRecord() {
    final String tableName = "test-table";
    final String streamTableName = "pdb_stream_test-table";
    final Map<String, AttributeValue> item = createTestItem();

    setupMocksForCapture(tableName, streamTableName, true, "KEYS_ONLY");

    helper.captureInsert(tableName, item);

    verify(streamDao).insert(eq(streamTableName), recordCaptor.capture());
    final PdbStreamRecord record = recordCaptor.getValue();

    assert record.eventType().equals("INSERT");
    assert record.eventId() != null;
    assert record.hashKeyValue().equals("test-hash");
    assert record.keysJson().equals("{\"keys\":\"json\"}");
    assert record.oldImageJson().isEmpty();
    assert record.newImageJson().isEmpty();
  }

  @Test
  void captureInsert_newImage_includesNewImage() {
    final String tableName = "test-table";
    final String streamTableName = "pdb_stream_test-table";
    final Map<String, AttributeValue> item = createTestItem();

    setupMocksForCapture(tableName, streamTableName, true, "NEW_IMAGE");

    helper.captureInsert(tableName, item);

    verify(streamDao).insert(eq(streamTableName), recordCaptor.capture());
    final PdbStreamRecord record = recordCaptor.getValue();

    assert record.eventType().equals("INSERT");
    assert record.newImageJson().isPresent();
    assert record.newImageJson().get().equals("{\"full\":\"item\"}");
    assert record.oldImageJson().isEmpty();
  }

  @Test
  void captureInsert_newAndOldImages_includesNewImageOnly() {
    final String tableName = "test-table";
    final String streamTableName = "pdb_stream_test-table";
    final Map<String, AttributeValue> item = createTestItem();

    setupMocksForCapture(tableName, streamTableName, true, "NEW_AND_OLD_IMAGES");

    helper.captureInsert(tableName, item);

    verify(streamDao).insert(eq(streamTableName), recordCaptor.capture());
    final PdbStreamRecord record = recordCaptor.getValue();

    assert record.eventType().equals("INSERT");
    assert record.newImageJson().isPresent();
    assert record.oldImageJson().isEmpty(); // INSERT has no old image
  }

  @Test
  void captureModify_keysOnly_createsCorrectRecord() {
    final String tableName = "test-table";
    final String streamTableName = "pdb_stream_test-table";
    final Map<String, AttributeValue> oldItem = createTestItem();
    final Map<String, AttributeValue> newItem = createTestItem();

    setupMocksForCapture(tableName, streamTableName, true, "KEYS_ONLY");

    helper.captureModify(tableName, oldItem, newItem);

    verify(streamDao).insert(eq(streamTableName), recordCaptor.capture());
    final PdbStreamRecord record = recordCaptor.getValue();

    assert record.eventType().equals("MODIFY");
    assert record.oldImageJson().isEmpty();
    assert record.newImageJson().isEmpty();
  }

  @Test
  void captureModify_oldImage_includesOldImageOnly() {
    final String tableName = "test-table";
    final String streamTableName = "pdb_stream_test-table";
    final Map<String, AttributeValue> oldItem = createTestItem();
    final Map<String, AttributeValue> newItem = createTestItem();

    setupMocksForCapture(tableName, streamTableName, true, "OLD_IMAGE");

    helper.captureModify(tableName, oldItem, newItem);

    verify(streamDao).insert(eq(streamTableName), recordCaptor.capture());
    final PdbStreamRecord record = recordCaptor.getValue();

    assert record.eventType().equals("MODIFY");
    assert record.oldImageJson().isPresent();
    assert record.newImageJson().isEmpty();
  }

  @Test
  void captureModify_newImage_includesNewImageOnly() {
    final String tableName = "test-table";
    final String streamTableName = "pdb_stream_test-table";
    final Map<String, AttributeValue> oldItem = createTestItem();
    final Map<String, AttributeValue> newItem = createTestItem();

    setupMocksForCapture(tableName, streamTableName, true, "NEW_IMAGE");

    helper.captureModify(tableName, oldItem, newItem);

    verify(streamDao).insert(eq(streamTableName), recordCaptor.capture());
    final PdbStreamRecord record = recordCaptor.getValue();

    assert record.eventType().equals("MODIFY");
    assert record.oldImageJson().isEmpty();
    assert record.newImageJson().isPresent();
  }

  @Test
  void captureModify_newAndOldImages_includesBothImages() {
    final String tableName = "test-table";
    final String streamTableName = "pdb_stream_test-table";
    final Map<String, AttributeValue> oldItem = createTestItem();
    final Map<String, AttributeValue> newItem = createTestItem();

    setupMocksForCapture(tableName, streamTableName, true, "NEW_AND_OLD_IMAGES");

    helper.captureModify(tableName, oldItem, newItem);

    verify(streamDao).insert(eq(streamTableName), recordCaptor.capture());
    final PdbStreamRecord record = recordCaptor.getValue();

    assert record.eventType().equals("MODIFY");
    assert record.oldImageJson().isPresent();
    assert record.newImageJson().isPresent();
  }

  @Test
  void captureRemove_keysOnly_createsCorrectRecord() {
    final String tableName = "test-table";
    final String streamTableName = "pdb_stream_test-table";
    final Map<String, AttributeValue> oldItem = createTestItem();

    setupMocksForCapture(tableName, streamTableName, true, "KEYS_ONLY");

    helper.captureRemove(tableName, oldItem);

    verify(streamDao).insert(eq(streamTableName), recordCaptor.capture());
    final PdbStreamRecord record = recordCaptor.getValue();

    assert record.eventType().equals("REMOVE");
    assert record.oldImageJson().isEmpty();
    assert record.newImageJson().isEmpty();
  }

  @Test
  void captureRemove_oldImage_includesOldImage() {
    final String tableName = "test-table";
    final String streamTableName = "pdb_stream_test-table";
    final Map<String, AttributeValue> oldItem = createTestItem();

    setupMocksForCapture(tableName, streamTableName, true, "OLD_IMAGE");

    helper.captureRemove(tableName, oldItem);

    verify(streamDao).insert(eq(streamTableName), recordCaptor.capture());
    final PdbStreamRecord record = recordCaptor.getValue();

    assert record.eventType().equals("REMOVE");
    assert record.oldImageJson().isPresent();
    assert record.newImageJson().isEmpty();
  }

  @Test
  void captureRemove_newAndOldImages_includesOldImageOnly() {
    final String tableName = "test-table";
    final String streamTableName = "pdb_stream_test-table";
    final Map<String, AttributeValue> oldItem = createTestItem();

    setupMocksForCapture(tableName, streamTableName, true, "NEW_AND_OLD_IMAGES");

    helper.captureRemove(tableName, oldItem);

    verify(streamDao).insert(eq(streamTableName), recordCaptor.capture());
    final PdbStreamRecord record = recordCaptor.getValue();

    assert record.eventType().equals("REMOVE");
    assert record.oldImageJson().isPresent();
    assert record.newImageJson().isEmpty(); // REMOVE has no new image
  }

  private Map<String, AttributeValue> createTestItem() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("test-id").build());
    item.put("name", AttributeValue.builder().s("test-name").build());
    return item;
  }

  private PdbMetadata createMetadata(boolean streamEnabled, String streamViewType) {
    final ImmutablePdbMetadata.Builder builder = ImmutablePdbMetadata.builder()
        .name("test-table")
        .hashKey("id")
        .createDate(Instant.now())
        .streamEnabled(streamEnabled);

    if (streamViewType != null) {
      builder.streamViewType(streamViewType);
    }

    return builder.build();
  }

  private void setupMocksForCapture(String tableName, String streamTableName,
                                    boolean streamEnabled, String viewType) {
    final PdbMetadata metadata = createMetadata(streamEnabled, viewType);

    when(metadataDao.getTable(tableName)).thenReturn(Optional.of(metadata));
    when(streamTableManager.getStreamTableName(tableName)).thenReturn(streamTableName);
    when(attributeValueConverter.extractKeyValue(any(), eq("id"))).thenReturn("test-hash");
    when(attributeValueConverter.toJson(any())).thenAnswer(invocation -> {
      Map<String, AttributeValue> arg = invocation.getArgument(0);
      if (arg.size() == 1) {
        return "{\"keys\":\"json\"}"; // Keys only
      }
      return "{\"full\":\"item\"}"; // Full item
    });
  }
}
