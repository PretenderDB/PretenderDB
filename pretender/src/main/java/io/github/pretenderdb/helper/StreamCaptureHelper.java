package io.github.pretenderdb.helper;

import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.dao.PdbMetadataDao;
import io.github.pretenderdb.dao.PdbStreamDao;
import io.github.pretenderdb.manager.PdbStreamTableManager;
import io.github.pretenderdb.model.ImmutablePdbStreamRecord;
import io.github.pretenderdb.model.PdbMetadata;
import io.github.pretenderdb.model.PdbStreamRecord;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Helper class for capturing item changes to DynamoDB Streams.
 * Encapsulates logic for creating stream records based on event type and StreamViewType.
 */
@Singleton
public class StreamCaptureHelper {

  private static final Logger log = LoggerFactory.getLogger(StreamCaptureHelper.class);

  private final PdbMetadataDao metadataDao;
  private final PdbStreamDao streamDao;
  private final PdbStreamTableManager streamTableManager;
  private final AttributeValueConverter attributeValueConverter;

  /**
   * Instantiates a new Stream capture helper.
   *
   * @param metadataDao             the metadata dao
   * @param streamDao               the stream dao
   * @param streamTableManager      the stream table manager
   * @param attributeValueConverter the attribute value converter
   */
  @Inject
  public StreamCaptureHelper(final PdbMetadataDao metadataDao,
                             final PdbStreamDao streamDao,
                             final PdbStreamTableManager streamTableManager,
                             final AttributeValueConverter attributeValueConverter) {
    log.info("StreamCaptureHelper({}, {}, {}, {})", metadataDao, streamDao, streamTableManager, attributeValueConverter);
    this.metadataDao = metadataDao;
    this.streamDao = streamDao;
    this.streamTableManager = streamTableManager;
    this.attributeValueConverter = attributeValueConverter;
  }

  /**
   * Captures an INSERT event (new item created).
   *
   * @param tableName the table name
   * @param newItem   the new item
   */
  public void captureInsert(final String tableName, final Map<String, AttributeValue> newItem) {
    log.trace("captureInsert({}, {})", tableName, newItem);

    final Optional<PdbMetadata> metadata = metadataDao.getTable(tableName);
    if (metadata.isEmpty() || !metadata.get().streamEnabled()) {
      log.trace("Streams not enabled for table {}", tableName);
      return;
    }

    final PdbMetadata table = metadata.get();
    final String streamTableName = streamTableManager.getStreamTableName(tableName);

    // Extract keys
    final String hashKeyValue = attributeValueConverter.extractKeyValue(newItem, table.hashKey());
    final Optional<String> sortKeyValue = table.sortKey()
        .map(sk -> attributeValueConverter.extractKeyValue(newItem, sk));

    // Build stream record based on StreamViewType
    final ImmutablePdbStreamRecord.Builder recordBuilder = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(0L) // Will be auto-generated
        .eventId(UUID.randomUUID().toString())
        .eventType("INSERT")
        .eventTimestamp(Instant.now())
        .hashKeyValue(hashKeyValue)
        .keysJson(buildKeysJson(newItem, table))
        .approximateCreationTime(System.currentTimeMillis())
        .sizeBytes(calculateSize(newItem))
        .createDate(Instant.now());

    // Add sort key if present
    sortKeyValue.ifPresent(recordBuilder::sortKeyValue);

    // Add image data based on StreamViewType
    final String viewType = table.streamViewType().orElse("KEYS_ONLY");
    switch (viewType) {
      case "NEW_IMAGE":
      case "NEW_AND_OLD_IMAGES":
        recordBuilder.newImageJson(attributeValueConverter.toJson(newItem));
        break;
      case "KEYS_ONLY":
      default:
        // No image data
        break;
    }

    final PdbStreamRecord record = recordBuilder.build();
    streamDao.insert(streamTableName, record);
    log.debug("Captured INSERT event for table {} with eventId {}", tableName, record.eventId());
  }

  /**
   * Captures a MODIFY event (existing item updated).
   *
   * @param tableName the table name
   * @param oldItem   the old item
   * @param newItem   the new item
   */
  public void captureModify(final String tableName,
                            final Map<String, AttributeValue> oldItem,
                            final Map<String, AttributeValue> newItem) {
    log.trace("captureModify({}, {}, {})", tableName, oldItem, newItem);

    final Optional<PdbMetadata> metadata = metadataDao.getTable(tableName);
    if (metadata.isEmpty() || !metadata.get().streamEnabled()) {
      log.trace("Streams not enabled for table {}", tableName);
      return;
    }

    final PdbMetadata table = metadata.get();
    final String streamTableName = streamTableManager.getStreamTableName(tableName);

    // Extract keys from new item
    final String hashKeyValue = attributeValueConverter.extractKeyValue(newItem, table.hashKey());
    final Optional<String> sortKeyValue = table.sortKey()
        .map(sk -> attributeValueConverter.extractKeyValue(newItem, sk));

    // Build stream record
    final ImmutablePdbStreamRecord.Builder recordBuilder = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(0L)
        .eventId(UUID.randomUUID().toString())
        .eventType("MODIFY")
        .eventTimestamp(Instant.now())
        .hashKeyValue(hashKeyValue)
        .keysJson(buildKeysJson(newItem, table))
        .approximateCreationTime(System.currentTimeMillis())
        .sizeBytes(calculateSize(newItem))
        .createDate(Instant.now());

    sortKeyValue.ifPresent(recordBuilder::sortKeyValue);

    // Add image data based on StreamViewType
    final String viewType = table.streamViewType().orElse("KEYS_ONLY");
    switch (viewType) {
      case "OLD_IMAGE":
        recordBuilder.oldImageJson(attributeValueConverter.toJson(oldItem));
        break;
      case "NEW_IMAGE":
        recordBuilder.newImageJson(attributeValueConverter.toJson(newItem));
        break;
      case "NEW_AND_OLD_IMAGES":
        recordBuilder.oldImageJson(attributeValueConverter.toJson(oldItem));
        recordBuilder.newImageJson(attributeValueConverter.toJson(newItem));
        break;
      case "KEYS_ONLY":
      default:
        // No image data
        break;
    }

    final PdbStreamRecord record = recordBuilder.build();
    streamDao.insert(streamTableName, record);
    log.debug("Captured MODIFY event for table {} with eventId {}", tableName, record.eventId());
  }

  /**
   * Captures a REMOVE event (item deleted).
   *
   * @param tableName the table name
   * @param oldItem   the old item
   */
  public void captureRemove(final String tableName, final Map<String, AttributeValue> oldItem) {
    log.trace("captureRemove({}, {})", tableName, oldItem);

    final Optional<PdbMetadata> metadata = metadataDao.getTable(tableName);
    if (metadata.isEmpty() || !metadata.get().streamEnabled()) {
      log.trace("Streams not enabled for table {}", tableName);
      return;
    }

    final PdbMetadata table = metadata.get();
    final String streamTableName = streamTableManager.getStreamTableName(tableName);

    // Extract keys
    final String hashKeyValue = attributeValueConverter.extractKeyValue(oldItem, table.hashKey());
    final Optional<String> sortKeyValue = table.sortKey()
        .map(sk -> attributeValueConverter.extractKeyValue(oldItem, sk));

    // Build stream record
    final ImmutablePdbStreamRecord.Builder recordBuilder = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(0L)
        .eventId(UUID.randomUUID().toString())
        .eventType("REMOVE")
        .eventTimestamp(Instant.now())
        .hashKeyValue(hashKeyValue)
        .keysJson(buildKeysJson(oldItem, table))
        .approximateCreationTime(System.currentTimeMillis())
        .sizeBytes(calculateSize(oldItem))
        .createDate(Instant.now());

    sortKeyValue.ifPresent(recordBuilder::sortKeyValue);

    // Add image data based on StreamViewType
    final String viewType = table.streamViewType().orElse("KEYS_ONLY");
    switch (viewType) {
      case "OLD_IMAGE":
      case "NEW_AND_OLD_IMAGES":
        recordBuilder.oldImageJson(attributeValueConverter.toJson(oldItem));
        break;
      case "KEYS_ONLY":
      case "NEW_IMAGE":
      default:
        // No old image for REMOVE with KEYS_ONLY or NEW_IMAGE
        break;
    }

    final PdbStreamRecord record = recordBuilder.build();
    streamDao.insert(streamTableName, record);
    log.debug("Captured REMOVE event for table {} with eventId {}", tableName, record.eventId());
  }

  /**
   * Builds the keys-only JSON representation (just the key attributes).
   *
   * @param item  the item
   * @param table the table metadata
   * @return the keys json
   */
  private String buildKeysJson(final Map<String, AttributeValue> item, final PdbMetadata table) {
    final Map<String, AttributeValue> keys = new java.util.HashMap<>();
    keys.put(table.hashKey(), item.get(table.hashKey()));
    table.sortKey().ifPresent(sk -> keys.put(sk, item.get(sk)));
    return attributeValueConverter.toJson(keys);
  }

  /**
   * Calculates approximate size of an item in bytes.
   * This is a simplified calculation - DynamoDB has complex size rules.
   *
   * @param item the item
   * @return the size in bytes
   */
  private int calculateSize(final Map<String, AttributeValue> item) {
    // Simple approximation: length of JSON representation
    final String json = attributeValueConverter.toJson(item);
    return json.length();
  }
}
