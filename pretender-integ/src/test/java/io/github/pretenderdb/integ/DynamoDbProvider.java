package io.github.pretenderdb.integ;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Interface for providing DynamoDB client implementations.
 * This allows tests to run against different DynamoDB implementations
 * (DynamoDB Local, Pretender with PostgreSQL, etc.)
 */
public interface DynamoDbProvider extends AutoCloseable {

  /**
   * Gets the DynamoDB client implementation.
   *
   * @return the DynamoDB client
   */
  DynamoDbClient getDynamoDbClient();

  /**
   * Gets the name of this provider for display in test reports.
   *
   * @return the provider name
   */
  String getProviderName();

  /**
   * Starts/initializes the DynamoDB provider.
   * This may start containers, local processes, etc.
   *
   * @throws Exception if startup fails
   */
  void start() throws Exception;

  /**
   * Stops/cleans up the DynamoDB provider.
   *
   * @throws Exception if shutdown fails
   */
  @Override
  void close() throws Exception;
}
