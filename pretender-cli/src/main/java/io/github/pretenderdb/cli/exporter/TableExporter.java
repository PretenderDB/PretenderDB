package io.github.pretenderdb.cli.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.cli.format.ExportFormat;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import java.nio.file.Path;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main orchestrator for table exports.
 */
public class TableExporter {

  private static final Logger log = LoggerFactory.getLogger(TableExporter.class);

  private final PdbTableManager pdbTableManager;
  private final PdbItemManager pdbItemManager;
  private final PdbTableConverter pdbTableConverter;
  private final CsvExporter csvExporter;
  private final DynamoDbJsonExporter dynamoDbJsonExporter;
  private final IonExporter ionExporter;
  private final ObjectMapper objectMapper;

  /**
   * Constructor.
   */
  @Inject
  public TableExporter(
      final PdbTableManager pdbTableManager,
      final PdbItemManager pdbItemManager,
      final PdbTableConverter pdbTableConverter,
      final CsvExporter csvExporter,
      final DynamoDbJsonExporter dynamoDbJsonExporter,
      final IonExporter ionExporter,
      final ObjectMapper objectMapper) {
    this.pdbTableManager = pdbTableManager;
    this.pdbItemManager = pdbItemManager;
    this.pdbTableConverter = pdbTableConverter;
    this.csvExporter = csvExporter;
    this.dynamoDbJsonExporter = dynamoDbJsonExporter;
    this.ionExporter = ionExporter;
    this.objectMapper = objectMapper;
  }

  /**
   * Export a table.
   *
   * @param tableName the table name
   * @param format the export format
   * @param outputPath the output path
   * @throws Exception if export fails
   */
  public void export(final String tableName, final ExportFormat format, final Path outputPath)
      throws Exception {
    log.info("Starting export of table '{}' in {} format", tableName, format);

    // Verify table exists
    final var metadata =
        pdbTableManager
            .getPdbTable(tableName)
            .orElseThrow(
                () -> new IllegalArgumentException("Table '" + tableName + "' does not exist"));

    log.info("Found table '{}' with hash key '{}'", tableName, metadata.hashKey());

    // Delegate to format-specific exporter
    switch (format) {
      case CSV:
        csvExporter.export(tableName, metadata, outputPath, pdbItemManager, pdbTableConverter);
        break;
      case DYNAMODB_JSON:
        dynamoDbJsonExporter.export(
            tableName, metadata, outputPath, pdbItemManager, pdbTableConverter);
        break;
      case ION:
        ionExporter.export(tableName, metadata, outputPath, pdbItemManager, pdbTableConverter);
        break;
      default:
        throw new IllegalArgumentException("Unsupported format: " + format);
    }

    log.info("Export completed successfully");
  }
}
