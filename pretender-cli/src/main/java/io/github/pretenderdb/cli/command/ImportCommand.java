package io.github.pretenderdb.cli.command;

import io.github.pretenderdb.cli.dagger.CliComponent;
import io.github.pretenderdb.cli.format.ExportFormat;
import io.github.pretenderdb.cli.importer.TableImporter;
import io.github.pretenderdb.cli.model.ImmutableImportOptions;
import io.github.pretenderdb.cli.model.ImportOptions;
import io.github.pretenderdb.dbu.model.Database;
import io.github.pretenderdb.dbu.model.ImmutableDatabase;
import io.github.pretenderdb.model.Configuration;
import io.github.pretenderdb.model.ImmutableConfiguration;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Import command for importing DynamoDB tables.
 */
@Command(
    name = "import",
    description = "Import a DynamoDB table from CSV, DynamoDB JSON, or Ion format")
public class ImportCommand implements Callable<Integer> {

  private static final Logger log = LoggerFactory.getLogger(ImportCommand.class);

  @Option(
      names = {"--table", "-t"},
      description = "Table name to import into",
      required = true)
  private String tableName;

  @Option(
      names = {"--format", "-f"},
      description = "Import format: ${COMPLETION-CANDIDATES}",
      required = true)
  private ExportFormat format;

  @Option(
      names = {"--input", "-i"},
      description = "Input directory or file path",
      required = true)
  private Path input;

  @Option(
      names = {"--db-url"},
      description = "Database JDBC URL (default: env PRETENDER_DB_URL)",
      defaultValue = "${PRETENDER_DB_URL}")
  private String dbUrl;

  @Option(
      names = {"--db-user"},
      description = "Database username (default: env PRETENDER_DB_USER)",
      defaultValue = "${PRETENDER_DB_USER}")
  private String dbUser;

  @Option(
      names = {"--db-password"},
      description = "Database password (default: env PRETENDER_DB_PASSWORD)",
      defaultValue = "${PRETENDER_DB_PASSWORD}")
  private String dbPassword;

  @Option(
      names = {"--create-table"},
      description = "Create table from schema if it doesn't exist")
  private boolean createTable;

  @Option(
      names = {"--clear-existing"},
      description = "Delete all existing items before import")
  private boolean clearExisting;

  @Option(
      names = {"--fail-if-exists"},
      description = "Fail if table already exists (when --create-table is used)")
  private boolean failIfExists;

  @Option(
      names = {"--batch-size"},
      description = "Batch size for writes (default: 25)",
      defaultValue = "25")
  private int batchSize;

  @Override
  public Integer call() throws Exception {
    log.info("Importing table '{}' from {} format from {}", tableName, format, input);

    // Validate database connection parameters
    if (dbUrl == null || dbUrl.isEmpty()) {
      log.error("Database URL is required. Set --db-url or PRETENDER_DB_URL environment variable.");
      return 1;
    }

    // Validate batch size
    if (batchSize < 1 || batchSize > 25) {
      log.error("Batch size must be between 1 and 25 (DynamoDB limit)");
      return 1;
    }

    // Create configuration
    final Database database =
        ImmutableDatabase.builder()
            .url(dbUrl)
            .username(dbUser != null ? dbUser : "")
            .password(dbPassword != null ? dbPassword : "")
            .build();

    final Configuration configuration =
        ImmutableConfiguration.builder().database(database).build();

    // Build import options
    final ImportOptions options =
        ImmutableImportOptions.builder()
            .createTable(createTable)
            .clearExisting(clearExisting)
            .failIfExists(failIfExists)
            .batchSize(batchSize)
            .build();

    // Initialize Dagger component
    final CliComponent component = CliComponent.create(configuration);

    // Get importer and execute
    final TableImporter importer = component.tableImporter();
    importer.importTable(tableName, format, input, options);

    log.info("Import completed successfully");
    return 0;
  }
}
