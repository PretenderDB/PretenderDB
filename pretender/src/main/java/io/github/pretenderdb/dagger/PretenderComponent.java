package io.github.pretenderdb.dagger;

import dagger.Component;
import io.github.pretenderdb.DynamoDbPretenderClient;
import io.github.pretenderdb.DynamoDbStreamsPretenderClient;
import io.github.pretenderdb.manager.PdbTableManager;
import io.github.pretenderdb.model.Configuration;
import io.github.pretenderdb.service.StreamCleanupService;
import io.github.pretenderdb.service.TtlCleanupService;
import javax.inject.Singleton;

/**
 * The interface Pretender component.
 */
@Singleton
@Component(modules = {PretenderModule.class, ConfigurationModule.class, CommonModule.class})
public interface PretenderComponent {

  /**
   * Instance pretender component.
   *
   * @param configuration the configuration
   * @return the pretender component
   */
  static PretenderComponent instance(final Configuration configuration) {
    return DaggerPretenderComponent.builder().configurationModule(new ConfigurationModule(configuration)).build();
  }

  /**
   * Pretender database manager pretender database manager.
   *
   * @return the pretender database manager
   */
  DynamoDbPretenderClient dynamoDbPretenderClient();

  /**
   * PDB table manager.
   *
   * @return the pdb table manager
   */
  PdbTableManager pdbTableManager();

  /**
   * TTL cleanup service.
   *
   * @return the ttl cleanup service
   */
  TtlCleanupService ttlCleanupService();

  /**
   * DynamoDB Streams pretender client.
   *
   * @return the dynamodb streams pretender client
   */
  DynamoDbStreamsPretenderClient dynamoDbStreamsPretenderClient();

  /**
   * Stream cleanup service.
   *
   * @return the stream cleanup service
   */
  StreamCleanupService streamCleanupService();

  /**
   * PDB stream DAO (for testing).
   *
   * @return the pdb stream dao
   */
  io.github.pretenderdb.dao.PdbStreamDao pdbStreamDao();

  /**
   * PDB stream table manager (for testing).
   *
   * @return the pdb stream table manager
   */
  io.github.pretenderdb.manager.PdbStreamTableManager pdbStreamTableManager();

  /**
   * Attribute encryption helper (for testing and configuration).
   *
   * @return the attribute encryption helper
   */
  io.github.pretenderdb.helper.AttributeEncryptionHelper encryptionHelper();
}
