package io.github.pretenderdb.dagger;

import dagger.Module;
import dagger.Provides;
import io.github.pretenderdb.dbu.model.Database;
import io.github.pretenderdb.model.Configuration;
import javax.inject.Named;
import javax.inject.Singleton;

/**
 * The type Configuration module.
 */
@Module
public record ConfigurationModule(Configuration configuration, boolean runLiquibase) {

  /**
   * The constant RUN_LIQUIBASE.
   */
  public static final String RUN_LIQUIBASE = "runLiquibase_config";


  /**
   * Instantiates a new Configuration module.
   *
   * @param configuration the configuration
   */
  public ConfigurationModule(final Configuration configuration) {
    this(configuration, true);
  }

  /**
   * Instantiates a new Configuration module.
   *
   * @param configuration the configuration
   * @param runLiquibase  the run liquibase
   */
  public ConfigurationModule {
  }

  /**
   * Configuration configuration.
   *
   * @return the configuration
   */
  @Override
  @Provides
  @Singleton
  public Configuration configuration() {
    return configuration;
  }

  /**
   * Database database.
   *
   * @param configuration the configuration
   * @return the database
   */
  @Provides
  @Singleton
  public Database database(final Configuration configuration) {
    return configuration.database();
  }

  /**
   * Run liquibase boolean.
   *
   * @return the boolean
   */
  @Override
  @Provides
  @Singleton
  @Named(RUN_LIQUIBASE)
  public boolean runLiquibase() {
    return runLiquibase;
  }
}
