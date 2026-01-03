package io.github.pretenderdb.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Represents a Global Secondary Index (GSI) in Pretender's DynamoDB-compatible storage.
 * Each GSI has its own hash key, optional sort key, and projection configuration.
 */
@Value.Immutable
@JsonSerialize(as = ImmutablePdbGlobalSecondaryIndex.class)
@JsonDeserialize(as = ImmutablePdbGlobalSecondaryIndex.class)
public interface PdbGlobalSecondaryIndex {

  /**
   * The name of the GSI.
   *
   * @return the index name
   */
  String indexName();

  /**
   * The hash key attribute name for this GSI.
   *
   * @return the hash key name
   */
  String hashKey();

  /**
   * The optional sort key attribute name for this GSI.
   *
   * @return the sort key name
   */
  Optional<String> sortKey();

  /**
   * The projection type for this GSI.
   * Valid values: ALL, KEYS_ONLY, INCLUDE
   *
   * @return the projection type
   */
  String projectionType();

  /**
   * The non-key attributes to include in the index projection (only used when projectionType = INCLUDE).
   *
   * @return the non-key attributes
   */
  Optional<String> nonKeyAttributes();
}
