package io.github.pretenderdb.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Represents a DynamoDB item stored in the pretender database.
 * Uses a hybrid storage approach: hash/sort key values are extracted
 * for indexing, while all attributes are stored as JSONB.
 */
@Value.Immutable
@JsonSerialize(as = ImmutablePdbItem.class)
@JsonDeserialize(as = ImmutablePdbItem.class)
public interface PdbItem {

  /**
   * The DynamoDB table name this item belongs to.
   *
   * @return the table name
   */
  String tableName();

  /**
   * The hash (partition) key value extracted for indexing.
   *
   * @return the hash key value
   */
  String hashKeyValue();

  /**
   * The sort (range) key value extracted for indexing.
   * Empty if the table doesn't have a sort key.
   *
   * @return the sort key value
   */
  Optional<String> sortKeyValue();

  /**
   * All item attributes serialized as JSON.
   * Contains the complete AttributeValue map including keys.
   *
   * @return the attributes JSON
   */
  String attributesJson();

  /**
   * When the item was created.
   *
   * @return the create date
   */
  Instant createDate();

  /**
   * When the item was last updated.
   *
   * @return the update date
   */
  Instant updateDate();
}
