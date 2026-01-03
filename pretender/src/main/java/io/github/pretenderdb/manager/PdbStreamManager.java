package io.github.pretenderdb.manager;

import io.github.pretenderdb.converter.ShardIteratorCodec;
import io.github.pretenderdb.converter.StreamRecordConverter;
import io.github.pretenderdb.dao.PdbMetadataDao;
import io.github.pretenderdb.dao.PdbStreamDao;
import io.github.pretenderdb.model.ImmutableShardIterator;
import io.github.pretenderdb.model.PdbMetadata;
import io.github.pretenderdb.model.PdbStreamRecord;
import io.github.pretenderdb.model.ShardIterator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

/**
 * Manager for DynamoDB Streams operations.
 * Provides business logic for stream consumption API.
 *
 * <h2>Shard Management Implementation</h2>
 * <p>
 * Pretender uses a <b>single-shard model</b> for all DynamoDB Streams, unlike real AWS DynamoDB
 * which dynamically creates and splits shards based on traffic patterns.
 * </p>
 *
 * <h3>Design Rationale</h3>
 * <ul>
 *   <li><b>Simplicity</b>: No complex shard splitting/merging logic required</li>
 *   <li><b>Sequential Guarantees</b>: All records globally ordered by sequence number</li>
 *   <li><b>SQL-Friendly</b>: Leverages database auto-increment for sequence numbers</li>
 *   <li><b>Sufficient for Testing</b>: Local development doesn't need AWS-level scale</li>
 * </ul>
 *
 * <h3>Limitations</h3>
 * <ul>
 *   <li><b>No Parallel Processing</b>: Single shard = no parallel consumer support</li>
 *   <li><b>Fixed Throughput</b>: Limited by single database query performance (~5K-10K records/sec)</li>
 *   <li><b>No Hash Partitioning</b>: All records in one shard regardless of hash key</li>
 * </ul>
 *
 * <h3>When This Matters</h3>
 * <p><b>Works fine for:</b></p>
 * <ul>
 *   <li>Local development (&lt;100 writes/second)</li>
 *   <li>Integration testing (&lt;1,000 events)</li>
 *   <li>Small-scale production (&lt;1,000 writes/second)</li>
 * </ul>
 *
 * <p><b>Not suitable for:</b></p>
 * <ul>
 *   <li>High-throughput load testing (&gt;5,000 writes/second)</li>
 *   <li>Testing multi-shard consumer logic</li>
 *   <li>Production traffic simulation requiring shard splits</li>
 * </ul>
 *
 * <p>See <code>STREAMS_ARCHITECTURE.md</code> for detailed documentation on shard management,
 * limitations, and future enhancement options.</p>
 *
 * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html">AWS DynamoDB Streams</a>
 */
@Singleton
public class PdbStreamManager {

  private static final Logger log = LoggerFactory.getLogger(PdbStreamManager.class);

  /**
   * Single static shard ID used for all streams.
   * <p>
   * Real AWS DynamoDB Streams dynamically create shards (shard-00000, shard-00001, etc.)
   * and split them based on traffic. Pretender uses a single shard for simplicity.
   * This means all stream records for a table are stored sequentially in one shard,
   * providing global ordering but no parallel processing capability.
   * </p>
   * <p>
   * <b>Trade-off</b>: Simpler implementation vs. limited throughput and no parallel consumption.
   * </p>
   */
  private static final String SHARD_ID = "shard-00000";

  private static final int DEFAULT_LIMIT = 1000;

  private final PdbMetadataDao metadataDao;
  private final PdbStreamDao streamDao;
  private final PdbStreamTableManager streamTableManager;
  private final StreamRecordConverter streamRecordConverter;
  private final ShardIteratorCodec shardIteratorCodec;

  /**
   * Instantiates a new Pdb stream manager.
   *
   * @param metadataDao           the metadata dao
   * @param streamDao             the stream dao
   * @param streamTableManager    the stream table manager
   * @param streamRecordConverter the stream record converter
   * @param shardIteratorCodec    the shard iterator codec
   */
  @Inject
  public PdbStreamManager(final PdbMetadataDao metadataDao,
                          final PdbStreamDao streamDao,
                          final PdbStreamTableManager streamTableManager,
                          final StreamRecordConverter streamRecordConverter,
                          final ShardIteratorCodec shardIteratorCodec) {
    log.info("PdbStreamManager({}, {}, {}, {}, {})",
        metadataDao, streamDao, streamTableManager, streamRecordConverter, shardIteratorCodec);
    this.metadataDao = metadataDao;
    this.streamDao = streamDao;
    this.streamTableManager = streamTableManager;
    this.streamRecordConverter = streamRecordConverter;
    this.shardIteratorCodec = shardIteratorCodec;
  }

  /**
   * Describes a stream for a table.
   *
   * @param request the describe stream request
   * @return the describe stream response
   */
  public DescribeStreamResponse describeStream(final DescribeStreamRequest request) {
    log.trace("describeStream({})", request);

    final String streamArn = request.streamArn();
    final String tableName = extractTableNameFromArn(streamArn);

    // Verify table exists and streams enabled
    final PdbMetadata metadata = metadataDao.getTable(tableName)
        .orElseThrow(() -> ResourceNotFoundException.builder()
            .message("Stream not found: " + streamArn)
            .build());

    if (!metadata.streamEnabled()) {
      throw ResourceNotFoundException.builder()
          .message("Stream not enabled for table: " + tableName)
          .build();
    }

    final String streamTableName = streamTableManager.getStreamTableName(tableName);

    // Get sequence number range
    final long trimHorizon = streamDao.getTrimHorizon(streamTableName);
    final long latestSequence = streamDao.getLatestSequenceNumber(streamTableName);

    // Build shard with sequence number range
    // Note: In real DynamoDB, endingSequenceNumber is set when a shard closes (during split/merge).
    // Pretender never closes shards, so endingSequenceNumber is always null.
    final SequenceNumberRange sequenceNumberRange = SequenceNumberRange.builder()
        .startingSequenceNumber(String.valueOf(trimHorizon))
        .endingSequenceNumber(latestSequence > 0 ? String.valueOf(latestSequence) : null)
        .build();

    // Always returns single shard with ID "shard-00000"
    // Real AWS DynamoDB would return multiple shards: [shard-00000, shard-00001, ...]
    final Shard shard = Shard.builder()
        .shardId(SHARD_ID)
        .sequenceNumberRange(sequenceNumberRange)
        .build();

    // Build stream description
    final StreamDescription description = StreamDescription.builder()
        .streamArn(metadata.streamArn().orElse(streamArn))
        .streamLabel(metadata.streamLabel().orElse(""))
        .streamStatus(StreamStatus.ENABLED)
        .streamViewType(StreamViewType.fromValue(metadata.streamViewType().orElse("KEYS_ONLY")))
        .tableName(tableName)
        .shards(shard)
        .creationRequestDateTime(Instant.now()) // Pretender doesn't track stream creation time separately
        .build();

    return DescribeStreamResponse.builder()
        .streamDescription(description)
        .build();
  }

  /**
   * Gets a shard iterator for reading stream records.
   *
   * @param request the get shard iterator request
   * @return the get shard iterator response
   */
  public GetShardIteratorResponse getShardIterator(final GetShardIteratorRequest request) {
    log.trace("getShardIterator({})", request);

    final String streamArn = request.streamArn();
    final String tableName = extractTableNameFromArn(streamArn);
    final ShardIteratorType type = request.shardIteratorType();

    // Verify table exists and streams enabled
    final PdbMetadata metadata = metadataDao.getTable(tableName)
        .orElseThrow(() -> ResourceNotFoundException.builder()
            .message("Stream not found: " + streamArn)
            .build());

    if (!metadata.streamEnabled()) {
      throw ResourceNotFoundException.builder()
          .message("Stream not enabled for table: " + tableName)
          .build();
    }

    final String streamTableName = streamTableManager.getStreamTableName(tableName);

    // Determine starting sequence number based on iterator type
    final long sequenceNumber = switch (type) {
      case TRIM_HORIZON -> 0; // Start from beginning (before first record)
      case LATEST -> streamDao.getLatestSequenceNumber(streamTableName) + 1;
      case AT_SEQUENCE_NUMBER -> {
        if (request.sequenceNumber() == null) {
          throw new IllegalArgumentException("sequenceNumber required for AT_SEQUENCE_NUMBER");
        }
        yield Long.parseLong(request.sequenceNumber()) - 1; // getRecords uses >, so subtract 1 to include this record
      }
      case AFTER_SEQUENCE_NUMBER -> {
        if (request.sequenceNumber() == null) {
          throw new IllegalArgumentException("sequenceNumber required for AFTER_SEQUENCE_NUMBER");
        }
        yield Long.parseLong(request.sequenceNumber()); // getRecords uses >, so this is correct
      }
      default -> throw new IllegalArgumentException("Unsupported iterator type: " + type);
    };

    // Build and encode shard iterator
    final ShardIterator iterator = ImmutableShardIterator.builder()
        .tableName(tableName)
        .shardId(SHARD_ID)
        .sequenceNumber(sequenceNumber)
        .type(type)
        .build();

    final String encodedIterator = shardIteratorCodec.encode(iterator);

    return GetShardIteratorResponse.builder()
        .shardIterator(encodedIterator)
        .build();
  }

  /**
   * Gets stream records from a shard iterator.
   *
   * @param request the get records request
   * @return the get records response
   */
  public GetRecordsResponse getRecords(final GetRecordsRequest request) {
    log.trace("getRecords({})", request);

    final String encodedIterator = request.shardIterator();
    final ShardIterator iterator = shardIteratorCodec.decode(encodedIterator);

    final String tableName = iterator.tableName();
    final long startSequence = iterator.sequenceNumber();
    final int limit = request.limit() != null ? request.limit() : DEFAULT_LIMIT;

    // Verify table exists and streams enabled
    final PdbMetadata metadata = metadataDao.getTable(tableName)
        .orElseThrow(() -> ResourceNotFoundException.builder()
            .message("Table not found: " + tableName)
            .build());

    if (!metadata.streamEnabled()) {
      log.warn("Streams not enabled for table {}, returning empty records", tableName);
      return GetRecordsResponse.builder()
          .records(List.of())
          .build();
    }

    final String streamTableName = streamTableManager.getStreamTableName(tableName);
    final String streamArn = metadata.streamArn().orElse(generateStreamArn(tableName));

    // Fetch records from DAO
    final List<PdbStreamRecord> pdbRecords = streamDao.getRecords(streamTableName, startSequence, limit);

    // Convert to AWS SDK Record format
    final List<Record> records = new ArrayList<>();
    for (PdbStreamRecord pdbRecord : pdbRecords) {
      records.add(streamRecordConverter.toRecord(pdbRecord, streamArn));
    }

    // Build next iterator if there are more records
    String nextIterator = null;
    if (!pdbRecords.isEmpty()) {
      final long lastSequence = pdbRecords.get(pdbRecords.size() - 1).sequenceNumber();
      final ShardIterator nextIter = ImmutableShardIterator.builder()
          .tableName(tableName)
          .shardId(SHARD_ID)
          .sequenceNumber(lastSequence) // Don't add 1 - AFTER_SEQUENCE_NUMBER means "after this"
          .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
          .build();
      nextIterator = shardIteratorCodec.encode(nextIter);
    }

    return GetRecordsResponse.builder()
        .records(records)
        .nextShardIterator(nextIterator)
        .build();
  }

  /**
   * Lists all streams (tables with streams enabled).
   *
   * @param request the list streams request
   * @return the list streams response
   */
  public ListStreamsResponse listStreams(final ListStreamsRequest request) {
    log.trace("listStreams({})", request);

    // Get all tables with streams enabled
    final List<String> tableNames = metadataDao.getTablesWithStreamsEnabled();

    // Build stream list
    final List<software.amazon.awssdk.services.dynamodb.model.Stream> streams = new ArrayList<>();
    for (String tableName : tableNames) {
      final Optional<PdbMetadata> metadata = metadataDao.getTable(tableName);
      if (metadata.isPresent() && metadata.get().streamEnabled()) {
        final software.amazon.awssdk.services.dynamodb.model.Stream stream =
            software.amazon.awssdk.services.dynamodb.model.Stream.builder()
                .streamArn(metadata.get().streamArn().orElse(generateStreamArn(tableName)))
                .streamLabel(metadata.get().streamLabel().orElse(""))
                .tableName(tableName)
                .build();
        streams.add(stream);
      }
    }

    return ListStreamsResponse.builder()
        .streams(streams)
        .build();
  }

  /**
   * Extracts table name from stream ARN.
   * Format: arn:aws:dynamodb:us-east-1:000000000000:table/{tableName}/stream/{timestamp}
   *
   * @param streamArn the stream ARN
   * @return the table name
   */
  private String extractTableNameFromArn(final String streamArn) {
    try {
      // Split by "/" and get the table name part
      final String[] parts = streamArn.split("/");
      if (parts.length >= 2) {
        return parts[parts.length - 3]; // table name is 3rd from end
      }
      throw new IllegalArgumentException("Invalid stream ARN format: " + streamArn);
    } catch (Exception e) {
      log.error("Failed to extract table name from ARN: {}", streamArn, e);
      throw new IllegalArgumentException("Invalid stream ARN: " + streamArn, e);
    }
  }

  /**
   * Generates a stream ARN for a table.
   *
   * @param tableName the table name
   * @return the stream ARN
   */
  private String generateStreamArn(final String tableName) {
    final long timestamp = System.currentTimeMillis();
    return String.format("arn:aws:dynamodb:us-east-1:000000000000:table/%s/stream/%d",
        tableName, timestamp);
  }
}
