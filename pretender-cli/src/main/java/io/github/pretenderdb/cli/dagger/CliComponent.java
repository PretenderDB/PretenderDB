package io.github.pretenderdb.cli.dagger;

import dagger.Component;
import io.github.pretenderdb.cli.exporter.TableExporter;
import io.github.pretenderdb.cli.importer.TableImporter;
import io.github.pretenderdb.dagger.CommonModule;
import io.github.pretenderdb.dagger.ConfigurationModule;
import io.github.pretenderdb.dagger.PretenderModule;
import io.github.pretenderdb.model.Configuration;
import javax.inject.Singleton;

/**
 * Dagger component for CLI tool.
 */
@Singleton
@Component(
    modules = {CliModule.class, PretenderModule.class, ConfigurationModule.class, CommonModule.class})
public interface CliComponent {

  /**
   * Create CLI component with configuration.
   *
   * @param configuration the configuration
   * @return the CLI component
   */
  static CliComponent create(final Configuration configuration) {
    return DaggerCliComponent.builder()
        .configurationModule(new ConfigurationModule(configuration))
        .build();
  }

  /**
   * Table exporter.
   *
   * @return the table exporter
   */
  TableExporter tableExporter();

  /**
   * Table importer.
   *
   * @return the table importer
   */
  TableImporter tableImporter();
}
