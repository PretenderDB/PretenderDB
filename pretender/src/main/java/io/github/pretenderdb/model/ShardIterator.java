package io.github.pretenderdb.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.github.pretenderdb.manager.PdbStreamManager;
import org.immutables.value.Value;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;

/**
 * Internal model for shard iterator information.
 * Used for encoding/decoding shard iterator strings.
 * <p>
 * Shard iterators are encoded as Base64 JSON strings and passed between
 * {@code getShardIterator()} and {@code getRecords()} calls to maintain
 * read position within a stream shard.
 * </p>
 * <p>
 * <b>Note on Shard ID:</b> Pretender always uses a single shard ("shard-00000") per stream.
 * Real AWS DynamoDB Streams may have multiple shards (shard-00000, shard-00001, etc.)
 * and the shardId field would vary. See {@link PdbStreamManager} for details.
 * </p>
 */
@Value.Immutable
@JsonSerialize(as = ImmutableShardIterator.class)
@JsonDeserialize(as = ImmutableShardIterator.class)
public interface ShardIterator {

  /**
   * The table name.
   *
   * @return the table name
   */
  String tableName();

  /**
   * The shard ID.
   * <p>
   * In Pretender, this is always "shard-00000" because Pretender uses a single-shard model.
   * Real AWS DynamoDB Streams use multiple shards (shard-00000, shard-00001, etc.) and
   * dynamically create new shards through splitting/merging.
   * </p>
   *
   * @return the shard id (always "shard-00000" in Pretender)
   */
  String shardId();

  /**
   * The sequence number position for this iterator.
   *
   * @return the sequence number
   */
  long sequenceNumber();

  /**
   * The shard iterator type.
   *
   * @return the type
   */
  ShardIteratorType type();
}
