package io.github.pretenderdb.cli.command;

import io.github.pretenderdb.cli.dagger.CliComponent;
import io.github.pretenderdb.cli.exporter.TableExporter;
import io.github.pretenderdb.cli.format.ExportFormat;
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
 * Export command for exporting DynamoDB tables.
 */
@Command(
    name = "export",
    description = "Export a DynamoDB table to CSV, DynamoDB JSON, or Ion format")
public class ExportCommand implements Callable<Integer> {

  private static final Logger log = LoggerFactory.getLogger(ExportCommand.class);

  @Option(
      names = {"--table", "-t"},
      description = "Table name to export",
      required = true)
  private String tableName;

  @Option(
      names = {"--format", "-f"},
      description = "Export format: ${COMPLETION-CANDIDATES}",
      required = true)
  private ExportFormat format;

  @Option(
      names = {"--output", "-o"},
      description = "Output directory or file path",
      required = true)
  private Path output;

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

  @Override
  public Integer call() throws Exception {
    log.info("Exporting table '{}' in {} format to {}", tableName, format, output);

    // Validate database connection parameters
    if (dbUrl == null || dbUrl.isEmpty()) {
      log.error("Database URL is required. Set --db-url or PRETENDER_DB_URL environment variable.");
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

    // Initialize Dagger component
    final CliComponent component = CliComponent.create(configuration);

    // Get exporter and execute
    final TableExporter exporter = component.tableExporter();
    exporter.export(tableName, format, output);

    log.info("Export completed successfully");
    return 0;
  }
}
