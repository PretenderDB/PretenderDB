package io.github.pretenderdb.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.github.pretenderdb.converter.ShardIteratorCodec;
import io.github.pretenderdb.converter.StreamRecordConverter;
import io.github.pretenderdb.dao.PdbMetadataDao;
import io.github.pretenderdb.dao.PdbStreamDao;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.ImmutablePdbStreamRecord;
import io.github.pretenderdb.model.ImmutableShardIterator;
import io.github.pretenderdb.model.PdbMetadata;
import io.github.pretenderdb.model.PdbStreamRecord;
import io.github.pretenderdb.model.ShardIterator;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

@ExtendWith(MockitoExtension.class)
class PdbStreamManagerTest {

  private static final String TABLE_NAME = "test-table";
  private static final String STREAM_ARN = "arn:aws:dynamodb:us-east-1:000000000000:table/test-table/stream/123";
  private static final String STREAM_TABLE_NAME = "pdb_stream_test-table";
  private static final String SHARD_ID = "shard-00000";

  @Mock
  private PdbMetadataDao metadataDao;

  @Mock
  private PdbStreamDao streamDao;

  @Mock
  private PdbStreamTableManager streamTableManager;

  @Mock
  private StreamRecordConverter streamRecordConverter;

  @Mock
  private ShardIteratorCodec shardIteratorCodec;

  private PdbStreamManager manager;

  @BeforeEach
  void setup() {
    manager = new PdbStreamManager(metadataDao, streamDao, streamTableManager,
        streamRecordConverter, shardIteratorCodec);
  }

  @Test
  void describeStream_success() {
    final PdbMetadata metadata = createMetadata(true);

    when(metadataDao.getTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenReturn(STREAM_TABLE_NAME);
    when(streamDao.getTrimHorizon(STREAM_TABLE_NAME)).thenReturn(1L);
    when(streamDao.getLatestSequenceNumber(STREAM_TABLE_NAME)).thenReturn(10L);

    final DescribeStreamResponse response = manager.describeStream(
        DescribeStreamRequest.builder().streamArn(STREAM_ARN).build()
    );

    assertThat(response.streamDescription()).isNotNull();
    assertThat(response.streamDescription().streamArn()).isEqualTo(STREAM_ARN);
    assertThat(response.streamDescription().tableName()).isEqualTo(TABLE_NAME);
    assertThat(response.streamDescription().streamStatus()).isEqualTo(StreamStatus.ENABLED);
    assertThat(response.streamDescription().streamViewType()).isEqualTo(StreamViewType.NEW_AND_OLD_IMAGES);
    assertThat(response.streamDescription().shards()).hasSize(1);
    assertThat(response.streamDescription().shards().get(0).shardId()).isEqualTo(SHARD_ID);
  }

  @Test
  void describeStream_tableNotFound() {
    when(metadataDao.getTable(TABLE_NAME)).thenReturn(Optional.empty());

    assertThatExceptionOfType(ResourceNotFoundException.class)
        .isThrownBy(() -> manager.describeStream(
            DescribeStreamRequest.builder().streamArn(STREAM_ARN).build()
        ))
        .withMessageContaining("Stream not found");
  }

  @Test
  void describeStream_streamNotEnabled() {
    final PdbMetadata metadata = createMetadata(false);

    when(metadataDao.getTable(TABLE_NAME)).thenReturn(Optional.of(metadata));

    assertThatExceptionOfType(ResourceNotFoundException.class)
        .isThrownBy(() -> manager.describeStream(
            DescribeStreamRequest.builder().streamArn(STREAM_ARN).build()
        ))
        .withMessageContaining("Stream not enabled");
  }

  @Test
  void getShardIterator_trimHorizon() {
    final PdbMetadata metadata = createMetadata(true);

    when(metadataDao.getTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenReturn(STREAM_TABLE_NAME);
    when(shardIteratorCodec.encode(any())).thenReturn("encoded-iterator");

    final GetShardIteratorResponse response = manager.getShardIterator(
        GetShardIteratorRequest.builder()
            .streamArn(STREAM_ARN)
            .shardId(SHARD_ID)
            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
            .build()
    );

    assertThat(response.shardIterator()).isEqualTo("encoded-iterator");
  }

  @Test
  void getShardIterator_latest() {
    final PdbMetadata metadata = createMetadata(true);

    when(metadataDao.getTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenReturn(STREAM_TABLE_NAME);
    when(streamDao.getLatestSequenceNumber(STREAM_TABLE_NAME)).thenReturn(10L);
    when(shardIteratorCodec.encode(any())).thenReturn("encoded-iterator");

    final GetShardIteratorResponse response = manager.getShardIterator(
        GetShardIteratorRequest.builder()
            .streamArn(STREAM_ARN)
            .shardId(SHARD_ID)
            .shardIteratorType(ShardIteratorType.LATEST)
            .build()
    );

    assertThat(response.shardIterator()).isEqualTo("encoded-iterator");
  }

  @Test
  void getShardIterator_atSequenceNumber() {
    final PdbMetadata metadata = createMetadata(true);

    when(metadataDao.getTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenReturn(STREAM_TABLE_NAME);
    when(shardIteratorCodec.encode(any())).thenReturn("encoded-iterator");

    final GetShardIteratorResponse response = manager.getShardIterator(
        GetShardIteratorRequest.builder()
            .streamArn(STREAM_ARN)
            .shardId(SHARD_ID)
            .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
            .sequenceNumber("5")
            .build()
    );

    assertThat(response.shardIterator()).isEqualTo("encoded-iterator");
  }

  @Test
  void getShardIterator_afterSequenceNumber() {
    final PdbMetadata metadata = createMetadata(true);

    when(metadataDao.getTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenReturn(STREAM_TABLE_NAME);
    when(shardIteratorCodec.encode(any())).thenReturn("encoded-iterator");

    final GetShardIteratorResponse response = manager.getShardIterator(
        GetShardIteratorRequest.builder()
            .streamArn(STREAM_ARN)
            .shardId(SHARD_ID)
            .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
            .sequenceNumber("5")
            .build()
    );

    assertThat(response.shardIterator()).isEqualTo("encoded-iterator");
  }

  @Test
  void getRecords_success() {
    final PdbMetadata metadata = createMetadata(true);
    final ShardIterator iterator = ImmutableShardIterator.builder()
        .tableName(TABLE_NAME)
        .shardId(SHARD_ID)
        .sequenceNumber(1L)
        .type(ShardIteratorType.TRIM_HORIZON)
        .build();

    final List<PdbStreamRecord> pdbRecords = List.of(
        createPdbStreamRecord(1L),
        createPdbStreamRecord(2L)
    );

    final Record sdkRecord = Record.builder()
        .eventID("event-1")
        .dynamodb(StreamRecord.builder().sequenceNumber("1").build())
        .build();

    when(shardIteratorCodec.decode("encoded-iterator")).thenReturn(iterator);
    when(metadataDao.getTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenReturn(STREAM_TABLE_NAME);
    when(streamDao.getRecords(eq(STREAM_TABLE_NAME), eq(1L), anyInt())).thenReturn(pdbRecords);
    when(streamRecordConverter.toRecord(any(PdbStreamRecord.class), anyString())).thenReturn(sdkRecord);
    when(shardIteratorCodec.encode(any())).thenReturn("next-iterator");

    final GetRecordsResponse response = manager.getRecords(
        GetRecordsRequest.builder()
            .shardIterator("encoded-iterator")
            .build()
    );

    assertThat(response.records()).hasSize(2);
    assertThat(response.nextShardIterator()).isEqualTo("next-iterator");
  }

  @Test
  void getRecords_emptyResults() {
    final PdbMetadata metadata = createMetadata(true);
    final ShardIterator iterator = ImmutableShardIterator.builder()
        .tableName(TABLE_NAME)
        .shardId(SHARD_ID)
        .sequenceNumber(1L)
        .type(ShardIteratorType.TRIM_HORIZON)
        .build();

    when(shardIteratorCodec.decode("encoded-iterator")).thenReturn(iterator);
    when(metadataDao.getTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenReturn(STREAM_TABLE_NAME);
    when(streamDao.getRecords(eq(STREAM_TABLE_NAME), anyLong(), anyInt())).thenReturn(List.of());

    final GetRecordsResponse response = manager.getRecords(
        GetRecordsRequest.builder()
            .shardIterator("encoded-iterator")
            .build()
    );

    assertThat(response.records()).isEmpty();
    assertThat(response.nextShardIterator()).isNull();
  }

  @Test
  void listStreams_success() {
    final PdbMetadata metadata = createMetadata(true);

    when(metadataDao.getTablesWithStreamsEnabled()).thenReturn(List.of(TABLE_NAME));
    when(metadataDao.getTable(TABLE_NAME)).thenReturn(Optional.of(metadata));

    final ListStreamsResponse response = manager.listStreams(ListStreamsRequest.builder().build());

    assertThat(response.streams()).hasSize(1);
    assertThat(response.streams().get(0).tableName()).isEqualTo(TABLE_NAME);
    assertThat(response.streams().get(0).streamArn()).isEqualTo(STREAM_ARN);
  }

  @Test
  void listStreams_noStreams() {
    when(metadataDao.getTablesWithStreamsEnabled()).thenReturn(List.of());

    final ListStreamsResponse response = manager.listStreams(ListStreamsRequest.builder().build());

    assertThat(response.streams()).isEmpty();
  }

  private PdbMetadata createMetadata(boolean streamEnabled) {
    final ImmutablePdbMetadata.Builder builder = ImmutablePdbMetadata.builder()
        .name(TABLE_NAME)
        .hashKey("id")
        .createDate(Instant.now())
        .streamEnabled(streamEnabled);

    if (streamEnabled) {
      builder.streamViewType("NEW_AND_OLD_IMAGES");
      builder.streamArn(STREAM_ARN);
      builder.streamLabel("label-123");
    }

    return builder.build();
  }

  private PdbStreamRecord createPdbStreamRecord(long sequenceNumber) {
    return ImmutablePdbStreamRecord.builder()
        .sequenceNumber(sequenceNumber)
        .eventId("event-" + sequenceNumber)
        .eventType("INSERT")
        .eventTimestamp(Instant.now())
        .hashKeyValue("test-hash")
        .keysJson("{\"id\":{\"S\":\"test-id\"}}")
        .approximateCreationTime(System.currentTimeMillis())
        .sizeBytes(100)
        .createDate(Instant.now())
        .build();
  }
}
