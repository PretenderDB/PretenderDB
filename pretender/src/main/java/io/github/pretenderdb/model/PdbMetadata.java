package io.github.pretenderdb.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * The interface PdbMetadata.
 */
@Value.Immutable
@JsonSerialize(as = ImmutablePdbMetadata.class)
@JsonDeserialize(as = ImmutablePdbMetadata.class)
public interface PdbMetadata {

  /**
   * Name string.
   *
   * @return the string
   */
  String name();

  /**
   * Hash key string.
   *
   * @return the string
   */
  String hashKey();

  /**
   * Sort key optional.
   *
   * @return the optional
   */
  Optional<String> sortKey();

  /**
   * Global Secondary Indexes for this table.
   *
   * @return the list of GSIs
   */
  @Value.Default
  default List<PdbGlobalSecondaryIndex> globalSecondaryIndexes() {
    return List.of();
  }

  /**
   * The attribute name used for Time-To-Live (TTL).
   * When present, items will be automatically deleted when the TTL timestamp expires.
   *
   * @return the TTL attribute name
   */
  Optional<String> ttlAttributeName();

  /**
   * Whether TTL is enabled for this table.
   *
   * @return true if TTL is enabled
   */
  @Value.Default
  default boolean ttlEnabled() {
    return false;
  }

  /**
   * Whether DynamoDB Streams is enabled for this table.
   *
   * @return true if streams is enabled
   */
  @Value.Default
  default boolean streamEnabled() {
    return false;
  }

  /**
   * The StreamViewType configuration for this table's stream.
   * Possible values: KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES.
   *
   * @return the stream view type
   */
  Optional<String> streamViewType();

  /**
   * The Amazon Resource Name (ARN) for the stream.
   *
   * @return the stream ARN
   */
  Optional<String> streamArn();

  /**
   * A timestamp-based label that uniquely identifies the stream.
   * Used to distinguish different stream enablement periods.
   *
   * @return the stream label
   */
  Optional<String> streamLabel();

  /**
   * Create date instant.
   *
   * @return the instant
   */
  Instant createDate();

}
