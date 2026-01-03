package io.github.pretenderdb.integ;

import io.github.pretenderdb.dbu.model.Database;
import io.github.pretenderdb.dbu.model.ImmutableDatabase;
import io.github.pretenderdb.dagger.DaggerPretenderComponent;
import io.github.pretenderdb.dagger.PretenderComponent;
import io.github.pretenderdb.model.Configuration;
import io.github.pretenderdb.model.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * DynamoDB provider that uses Pretender backed by HSQLDB in-memory database.
 * This tests Pretender with an embedded database without external dependencies.
 */
public class PretenderHsqldbProvider implements DynamoDbProvider {

  private static final Logger log = LoggerFactory.getLogger(PretenderHsqldbProvider.class);

  private PretenderComponent component;
  private DynamoDbClient client;

  @Override
  public void start() throws Exception {
    log.info("Starting Pretender with HSQLDB in-memory database");

    // Create HSQLDB in-memory database configuration
    // Use timestamp to ensure unique database per test instance
    Database database = ImmutableDatabase.builder()
        .url("jdbc:hsqldb:mem:pretender_integ_test_" + System.currentTimeMillis())
        .username("SA")
        .password("")
        .build();

    Configuration configuration = ImmutableConfiguration.builder()
        .database(database)
        .build();

    // Create Pretender component
    component = DaggerPretenderComponent.builder()
        .configurationModule(new io.github.pretenderdb.dagger.ConfigurationModule(configuration))
        .build();

    client = component.dynamoDbPretenderClient();

    log.info("Pretender with HSQLDB initialized successfully");
  }

  @Override
  public DynamoDbClient getDynamoDbClient() {
    return client;
  }

  @Override
  public String getProviderName() {
    return "Pretender with HSQLDB (In-Memory)";
  }

  @Override
  public void close() throws Exception {
    log.info("Stopping Pretender with HSQLDB");
    // No explicit cleanup needed for in-memory HSQLDB - it will be garbage collected
    log.info("Pretender with HSQLDB stopped");
  }
}
