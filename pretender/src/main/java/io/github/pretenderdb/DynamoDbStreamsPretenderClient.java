package io.github.pretenderdb;

import io.github.pretenderdb.manager.PdbStreamManager;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;

/**
 * Pretender implementation for DynamoDB Streams.
 * Provides DynamoDB Streams API compatibility using SQL database backend.
 * Note: Does not implement DynamoDbStreamsClient interface directly to avoid dependency.
 */
@Singleton
public class DynamoDbStreamsPretenderClient {

  private static final Logger log = LoggerFactory.getLogger(DynamoDbStreamsPretenderClient.class);

  private final PdbStreamManager streamManager;

  /**
   * Instantiates a new DynamoDB Streams pretender client.
   *
   * @param streamManager the stream manager
   */
  @Inject
  public DynamoDbStreamsPretenderClient(final PdbStreamManager streamManager) {
    log.info("DynamoDbStreamsPretenderClient({})", streamManager);
    this.streamManager = streamManager;
  }

  /**
   * Service name.
   *
   * @return the service name
   */
  public String serviceName() {
    return "dynamodb-streams";
  }

  /**
   * Close (no-op for pretender).
   */
  public void close() {
    log.debug("close() called - no-op for pretender client");
  }

  /**
   * Describes a stream.
   *
   * @param request the describe stream request
   * @return the describe stream response
   */
  public DescribeStreamResponse describeStream(final DescribeStreamRequest request) {
    log.trace("describeStream({})", request);
    return streamManager.describeStream(request);
  }

  /**
   * Gets a shard iterator.
   *
   * @param request the get shard iterator request
   * @return the get shard iterator response
   */
  public GetShardIteratorResponse getShardIterator(final GetShardIteratorRequest request) {
    log.trace("getShardIterator({})", request);
    return streamManager.getShardIterator(request);
  }

  /**
   * Gets stream records.
   *
   * @param request the get records request
   * @return the get records response
   */
  public GetRecordsResponse getRecords(final GetRecordsRequest request) {
    log.trace("getRecords({})", request);
    return streamManager.getRecords(request);
  }

  /**
   * Lists streams.
   *
   * @param request the list streams request
   * @return the list streams response
   */
  public ListStreamsResponse listStreams(final ListStreamsRequest request) {
    log.trace("listStreams({})", request);
    return streamManager.listStreams(request);
  }
}
