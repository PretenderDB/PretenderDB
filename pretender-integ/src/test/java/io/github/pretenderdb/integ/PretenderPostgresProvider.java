package io.github.pretenderdb.integ;

import io.github.pretenderdb.dbu.model.Database;
import io.github.pretenderdb.dbu.model.ImmutableDatabase;
import io.github.pretenderdb.dagger.DaggerPretenderComponent;
import io.github.pretenderdb.dagger.PretenderComponent;
import io.github.pretenderdb.model.Configuration;
import io.github.pretenderdb.model.ImmutableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * DynamoDB provider that uses Pretender backed by PostgreSQL via Testcontainers.
 * This is the implementation being tested for compatibility.
 */
public class PretenderPostgresProvider implements DynamoDbProvider {

  private static final Logger log = LoggerFactory.getLogger(PretenderPostgresProvider.class);

  private PostgreSQLContainer<?> postgresContainer;
  private PretenderComponent component;
  private DynamoDbClient client;

  @Override
  public void start() throws Exception {
    log.info("Starting PostgreSQL container for Pretender");

    // Start PostgreSQL container
    postgresContainer = new PostgreSQLContainer<>("postgres:17-alpine")
        .withDatabaseName("pretender_integ_test")
        .withUsername("pretender")
        .withPassword("pretender");

    postgresContainer.start();

    log.info("PostgreSQL container started: {}", postgresContainer.getJdbcUrl());

    // Create Pretender configuration
    Database database = ImmutableDatabase.builder()
        .url(postgresContainer.getJdbcUrl())
        .username(postgresContainer.getUsername())
        .password(postgresContainer.getPassword())
        .build();

    Configuration configuration = ImmutableConfiguration.builder()
        .database(database)
        .build();

    // Create Pretender component
    component = DaggerPretenderComponent.builder()
        .configurationModule(new io.github.pretenderdb.dagger.ConfigurationModule(configuration))
        .build();

    client = component.dynamoDbPretenderClient();

    log.info("Pretender with PostgreSQL initialized successfully");
  }

  @Override
  public DynamoDbClient getDynamoDbClient() {
    return client;
  }

  @Override
  public String getProviderName() {
    return "Pretender with PostgreSQL (Testcontainers)";
  }

  @Override
  public void close() throws Exception {
    log.info("Stopping Pretender and PostgreSQL container");
    if (postgresContainer != null) {
      postgresContainer.stop();
    }
    log.info("Pretender and PostgreSQL container stopped");
  }
}
