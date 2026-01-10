package io.github.pretenderdb.cli.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Manifest summary for DynamoDB JSON and Ion exports (AWS compatible).
 */
@Value.Immutable
@JsonSerialize(as = ImmutableExportManifest.class)
@JsonDeserialize(as = ImmutableExportManifest.class)
public interface ExportManifest {

  /**
   * Format version.
   */
  String version();

  /**
   * Export ARN (local for pretenderdb).
   */
  String exportArn();

  /**
   * Start time of export.
   */
  Instant startTime();

  /**
   * End time of export.
   */
  Instant endTime();

  /**
   * Table ARN.
   */
  String tableArn();

  /**
   * Table name.
   */
  String tableName();

  /**
   * S3 bucket (or "local" for pretenderdb).
   */
  String s3Bucket();

  /**
   * S3 prefix.
   */
  String s3Prefix();

  /**
   * Total size in bytes.
   */
  long billedSizeBytes();

  /**
   * Total item count.
   */
  long itemCount();

  /**
   * Output format (DYNAMODB_JSON or ION).
   */
  String outputFormat();

  /**
   * Export type (FULL_EXPORT).
   */
  String exportType();

  /**
   * Table schema information.
   */
  TableSchema tableSchema();

  /**
   * Table schema details.
   */
  @Value.Immutable
  @JsonSerialize(as = ImmutableTableSchema.class)
  @JsonDeserialize(as = ImmutableTableSchema.class)
  interface TableSchema {

    /**
     * Hash key attribute name.
     */
    String hashKey();

    /**
     * Sort key attribute name (optional).
     */
    Optional<String> sortKey();

    /**
     * Global secondary indexes.
     */
    List<GlobalSecondaryIndex> globalSecondaryIndexes();

    /**
     * TTL attribute name (optional).
     */
    Optional<String> ttlAttributeName();

    /**
     * TTL enabled flag.
     */
    @Value.Default
    default boolean ttlEnabled() {
      return false;
    }

    /**
     * Streams enabled flag.
     */
    @Value.Default
    default boolean streamEnabled() {
      return false;
    }

    /**
     * Stream view type (optional).
     */
    Optional<String> streamViewType();
  }

  /**
   * Global secondary index definition.
   */
  @Value.Immutable
  @JsonSerialize(as = ImmutableGlobalSecondaryIndex.class)
  @JsonDeserialize(as = ImmutableGlobalSecondaryIndex.class)
  interface GlobalSecondaryIndex {

    /**
     * Index name.
     */
    String indexName();

    /**
     * Hash key attribute name.
     */
    String hashKey();

    /**
     * Sort key attribute name (optional).
     */
    Optional<String> sortKey();

    /**
     * Projection type (ALL, KEYS_ONLY, INCLUDE).
     */
    String projectionType();

    /**
     * Non-key attributes for INCLUDE projection.
     */
    List<String> nonKeyAttributes();
  }
}
