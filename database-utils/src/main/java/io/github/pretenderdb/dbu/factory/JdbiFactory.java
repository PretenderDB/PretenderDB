package io.github.pretenderdb.dbu.factory;

import static org.slf4j.LoggerFactory.getLogger;

import io.github.pretenderdb.dbu.model.Database;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.jdbi.v3.cache.caffeine.CaffeineCachePlugin;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.core.statement.Slf4JSqlLogger;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.slf4j.Logger;

/**
 * The type Jdbi factory.
 */
@Singleton
public class JdbiFactory {

  public static final String IMMUTABLES = "JdbiImmutableClasses";
  private static final Logger log = getLogger(JdbiFactory.class);

  private final Database database;
  private final Set<Class<?>> immutableClasses;

  /**
   * Instantiates a new Jdbi factory.
   *
   * @param database         the configuration
   * @param immutableClasses the immutable classes
   */
  @Inject
  public JdbiFactory(final Database database,
                     @Named(IMMUTABLES) final Set<Class<?>> immutableClasses) {
    this.database = database;
    this.immutableClasses = immutableClasses;
    log.info("JdbiFactory({})", database);
  }

  /**
   * Create jdbi jdbi.
   *
   * @return the jdbi
   */
  public Jdbi createJdbi() {
    log.trace("createJdbi()");
    final Jdbi jdbi = Jdbi.create(database.url(), database.username(), database.password());
    setup(jdbi);
    return jdbi;
  }

  /**
   * Setup so it can be used even if we do not create the JDBI resource.
   *
   * @param jdbi the jdbi
   */
  public void setup(final Jdbi jdbi) {
    log.info("setup({})", jdbi);
    final JdbiImmutables immutablesConfig = jdbi.getConfig(JdbiImmutables.class);
    immutableClasses.forEach(immutablesConfig::registerImmutable);
    jdbi.installPlugin(new SqlObjectPlugin())
        .installPlugin(new CaffeineCachePlugin());
    if (database.usePostgresql()) {
      jdbi.installPlugin(new PostgresPlugin());
    }
    jdbi.setSqlLogger(new Slf4JSqlLogger());
  }
}
