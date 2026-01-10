package io.github.pretenderdb.cli.model;

import org.immutables.value.Value;

/**
 * Options for import operations.
 */
@Value.Immutable
public interface ImportOptions {

  /**
   * Create table from schema if it doesn't exist.
   */
  @Value.Default
  default boolean createTable() {
    return false;
  }

  /**
   * Delete all existing items before import.
   */
  @Value.Default
  default boolean clearExisting() {
    return false;
  }

  /**
   * Fail if table already exists (when createTable is true).
   */
  @Value.Default
  default boolean failIfExists() {
    return false;
  }

  /**
   * Batch size for writes (default 25, max per DynamoDB limits).
   */
  @Value.Default
  default int batchSize() {
    return 25;
  }
}
