package io.github.pretenderdb.integ;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * DynamoDB provider that uses AWS DynamoDB Local (embedded local DynamoDB server).
 * This provides a reference implementation to test against.
 */
public class DynamoDbLocalProvider implements DynamoDbProvider {

  private static final Logger log = LoggerFactory.getLogger(DynamoDbLocalProvider.class);
  private static final int PORT = 8000;
  private static final String ENDPOINT = "http://localhost:" + PORT;

  private DynamoDBProxyServer server;
  private DynamoDbClient client;

  @Override
  public void start() throws Exception {
    log.info("Starting DynamoDB Local on port {}", PORT);

    // Start DynamoDB Local server
    String[] localArgs = {"-inMemory", "-port", String.valueOf(PORT)};
    server = ServerRunner.createServerFromCommandLineArgs(localArgs);
    server.start();

    log.info("DynamoDB Local started successfully");

    // Create DynamoDB client pointing to local instance
    client = DynamoDbClient.builder()
        .endpointOverride(URI.create(ENDPOINT))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("fakeAccessKey", "fakeSecretKey")))
        .build();
  }

  @Override
  public DynamoDbClient getDynamoDbClient() {
    return client;
  }

  @Override
  public String getProviderName() {
    return "DynamoDB Local (AWS Official)";
  }

  @Override
  public void close() throws Exception {
    log.info("Stopping DynamoDB Local");
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.stop();
    }
    log.info("DynamoDB Local stopped");
  }
}
