package io.github.pretenderdb.cli.format;

/**
 * Export/Import format options.
 */
public enum ExportFormat {
  /**
   * CSV format with JSON-serialized complex types.
   */
  CSV,

  /**
   * DynamoDB JSON format (AWS S3 Export compatible).
   */
  DYNAMODB_JSON,

  /**
   * Amazon Ion text format.
   */
  ION
}
