package io.github.pretenderdb.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Immutable model representing a DynamoDB Stream record stored in the pretender database.
 * Each record captures a change event (INSERT, MODIFY, or REMOVE) for an item.
 */
@Value.Immutable
@JsonSerialize(as = ImmutablePdbStreamRecord.class)
@JsonDeserialize(as = ImmutablePdbStreamRecord.class)
public interface PdbStreamRecord {

  /**
   * Auto-increment sequence number providing strict ordering of events.
   *
   * @return the sequence number
   */
  long sequenceNumber();

  /**
   * Unique event identifier (UUID) for idempotency.
   *
   * @return the event id
   */
  String eventId();

  /**
   * Type of event: INSERT, MODIFY, or REMOVE.
   *
   * @return the event type
   */
  String eventType();

  /**
   * Timestamp when the event occurred.
   *
   * @return the event timestamp
   */
  Instant eventTimestamp();

  /**
   * Hash key value of the affected item.
   *
   * @return the hash key value
   */
  String hashKeyValue();

  /**
   * Sort key value of the affected item (if table has sort key).
   *
   * @return the sort key value
   */
  Optional<String> sortKeyValue();

  /**
   * JSON representation of the item's keys (always present regardless of view type).
   *
   * @return the keys json
   */
  String keysJson();

  /**
   * JSON representation of the item before the change (for MODIFY and REMOVE events).
   * Only populated if StreamViewType includes old image.
   *
   * @return the old image json
   */
  Optional<String> oldImageJson();

  /**
   * JSON representation of the item after the change (for INSERT and MODIFY events).
   * Only populated if StreamViewType includes new image.
   *
   * @return the new image json
   */
  Optional<String> newImageJson();

  /**
   * Approximate creation time in epoch milliseconds (matches DynamoDB Streams API).
   *
   * @return the approximate creation time
   */
  long approximateCreationTime();

  /**
   * Approximate size of the stream record in bytes.
   *
   * @return the size bytes
   */
  int sizeBytes();

  /**
   * Timestamp when this record was created in the database.
   *
   * @return the create date
   */
  Instant createDate();
}
