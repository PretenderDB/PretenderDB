package io.github.pretenderdb.dbu.liquibase;

import static org.slf4j.LoggerFactory.getLogger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import liquibase.Scope;
import liquibase.changelog.ChangeLogParameters;
import liquibase.command.CommandScope;
import liquibase.command.core.UpdateCommandStep;
import liquibase.command.core.helpers.DatabaseChangelogCommandStep;
import liquibase.command.core.helpers.DbUrlConnectionCommandStep;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;

/**
 * The type Liquibase helper.
 */
@Singleton
public class LiquibaseHelper {

  private static final Logger log = getLogger(LiquibaseHelper.class);

  /**
   * Instantiates a new Liquibase helper.
   */
  @Inject
  public LiquibaseHelper() {
  }

  /**
   * Run liquibase.
   *
   * @param dataSource    the data source
   * @param changeLogFile the change log file
   */
  public void runLiquibase(final DataSource dataSource,
                           final String changeLogFile) {
    try {
      runLiquibase(dataSource.getConnection(), changeLogFile);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Run liquibase.
   *
   * @param jdbi          the jdbi
   * @param changeLogFile the change log file
   */
  public void runLiquibase(final Jdbi jdbi,
                           final String changeLogFile) {
    jdbi.useHandle(handle -> {
      try (final Connection connection = handle.getConnection()) {
        new LiquibaseHelper().runLiquibase(connection, changeLogFile);
        log.info("runLiquibase(): complete");
      } catch (RuntimeException | SQLException e) {
        throw new IllegalStateException("Database update failure", e);
      }
    });
  }

  /**
   * Run liquibase.
   *
   * @param connection    the connection
   * @param changeLogFile the change log file
   */
  public void runLiquibase(final Connection connection,
                           final String changeLogFile) {
    try {
      final Database database = DatabaseFactory.getInstance()
          .findCorrectDatabaseImplementation(new JdbcConnection(connection));
      final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

      Map<String, Object> scopeObjects = new HashMap<>();
      scopeObjects.put(Scope.Attr.database.name(), database);
      scopeObjects.put(Scope.Attr.resourceAccessor.name(), resourceAccessor);

      Scope.child(scopeObjects, () -> {
        CommandScope commandScope = new CommandScope(UpdateCommandStep.COMMAND_NAME);
        commandScope.addArgumentValue(DbUrlConnectionCommandStep.DATABASE_ARG, database);
        commandScope.addArgumentValue(UpdateCommandStep.CHANGELOG_FILE_ARG, changeLogFile);
        //commandScope.addArgumentValue(UpdateCommandStep.CONTEXTS_ARG, new Contexts().toString());
        //commandScope.addArgumentValue(UpdateCommandStep.LABEL_FILTER_ARG, new LabelExpression().getOriginalString());
        //commandScope.addArgumentValue(ChangeExecListenerCommandStep.CHANGE_EXEC_LISTENER_ARG, null);
        commandScope.addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_PARAMETERS, new ChangeLogParameters(database));

        //commandScope.setOutput(new WriterOutputStream(new PrintWriter(System.out), GlobalConfiguration.OUTPUT_FILE_ENCODING.getCurrentValue()));
        commandScope.execute();
        return null;
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
