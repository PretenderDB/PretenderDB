package io.github.pretenderdb.cli.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.cli.format.ExportFormat;
import io.github.pretenderdb.cli.model.ImportOptions;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import java.nio.file.Path;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

/**
 * Main orchestrator for table imports.
 */
public class TableImporter {

  private static final Logger log = LoggerFactory.getLogger(TableImporter.class);

  private final PdbTableManager pdbTableManager;
  private final PdbItemManager pdbItemManager;
  private final PdbTableConverter pdbTableConverter;
  private final CsvImporter csvImporter;
  private final DynamoDbJsonImporter dynamoDbJsonImporter;
  private final IonImporter ionImporter;
  private final ObjectMapper objectMapper;

  /**
   * Constructor.
   */
  @Inject
  public TableImporter(
      final PdbTableManager pdbTableManager,
      final PdbItemManager pdbItemManager,
      final PdbTableConverter pdbTableConverter,
      final CsvImporter csvImporter,
      final DynamoDbJsonImporter dynamoDbJsonImporter,
      final IonImporter ionImporter,
      final ObjectMapper objectMapper) {
    this.pdbTableManager = pdbTableManager;
    this.pdbItemManager = pdbItemManager;
    this.pdbTableConverter = pdbTableConverter;
    this.csvImporter = csvImporter;
    this.dynamoDbJsonImporter = dynamoDbJsonImporter;
    this.ionImporter = ionImporter;
    this.objectMapper = objectMapper;
  }

  /**
   * Import a table.
   *
   * @param tableName the table name
   * @param format the import format
   * @param inputPath the input path
   * @param options the import options
   * @throws Exception if import fails
   */
  public void importTable(
      final String tableName,
      final ExportFormat format,
      final Path inputPath,
      final ImportOptions options)
      throws Exception {
    log.info("Starting import of table '{}' from {} format", tableName, format);

    // Check if table exists
    final var existingMetadata = pdbTableManager.getPdbTable(tableName);

    if (existingMetadata.isPresent()) {
      log.info("Table '{}' already exists", tableName);

      if (options.failIfExists() && options.createTable()) {
        throw new IllegalStateException(
            "Table '" + tableName + "' already exists and --fail-if-exists was specified");
      }

      // Clear existing data if requested
      if (options.clearExisting()) {
        log.info("Clearing existing data from table '{}'", tableName);
        clearTableData(tableName);
      }
    } else {
      log.info("Table '{}' does not exist", tableName);

      if (!options.createTable()) {
        throw new IllegalArgumentException(
            "Table '"
                + tableName
                + "' does not exist. Use --create-table to create it from schema.");
      }

      log.info("Table will be created from schema during import");
    }

    // Delegate to format-specific importer
    switch (format) {
      case CSV:
        csvImporter.importData(tableName, inputPath, options, pdbItemManager, pdbTableManager,
            pdbTableConverter);
        break;
      case DYNAMODB_JSON:
        dynamoDbJsonImporter.importData(
            tableName, inputPath, options, pdbItemManager, pdbTableManager, pdbTableConverter);
        break;
      case ION:
        ionImporter.importData(tableName, inputPath, options, pdbItemManager, pdbTableManager,
            pdbTableConverter);
        break;
      default:
        throw new IllegalArgumentException("Unsupported format: " + format);
    }

    log.info("Import completed successfully");
  }

  /**
   * Clear all data from a table.
   */
  private void clearTableData(final String tableName) throws Exception {
    log.info("Deleting all items from table '{}'", tableName);

    // For simplicity, just delete and recreate the table
    // This is faster than deleting items one by one
    try {
      final var metadata = pdbTableManager.getPdbTable(tableName).orElseThrow();

      // Delete table
      pdbTableManager.deletePdbTable(tableName);

      // Recreate table
      pdbTableManager.insertPdbTable(metadata);

      log.info("Table '{}' cleared successfully", tableName);
    } catch (ResourceNotFoundException e) {
      log.warn("Table '{}' not found during clear operation", tableName);
    }
  }
}
