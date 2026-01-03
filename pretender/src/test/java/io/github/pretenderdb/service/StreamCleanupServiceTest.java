package io.github.pretenderdb.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.pretenderdb.dao.PdbMetadataDao;
import io.github.pretenderdb.dao.PdbStreamDao;
import io.github.pretenderdb.manager.PdbStreamTableManager;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for StreamCleanupService.
 */
@ExtendWith(MockitoExtension.class)
public class StreamCleanupServiceTest {

  private static final String TABLE_NAME = "TestTable";
  private static final String STREAM_TABLE_NAME = "pdb_stream_testtable";
  private static final Instant NOW = Instant.parse("2025-12-30T12:00:00Z");

  @Mock
  private PdbMetadataDao metadataDao;

  @Mock
  private PdbStreamDao streamDao;

  @Mock
  private PdbStreamTableManager streamTableManager;

  @Captor
  private ArgumentCaptor<Instant> cutoffTimeCaptor;

  private Clock clock;
  private StreamCleanupService service;

  @BeforeEach
  void setup() {
    clock = Clock.fixed(NOW, ZoneId.of("UTC"));
    // Use 60 second interval for testing
    service = new StreamCleanupService(metadataDao, streamDao, streamTableManager, clock, 60);
  }

  @Test
  void start_startsScheduledCleanup() {
    assertThat(service.isRunning()).isFalse();

    service.start();

    assertThat(service.isRunning()).isTrue();

    service.stop();
  }

  @Test
  void stop_stopsScheduledCleanup() {
    service.start();
    assertThat(service.isRunning()).isTrue();

    service.stop();

    assertThat(service.isRunning()).isFalse();
  }

  @Test
  void start_whenAlreadyRunning_doesNotStartAgain() {
    service.start();
    assertThat(service.isRunning()).isTrue();

    service.start(); // Second call

    assertThat(service.isRunning()).isTrue();

    service.stop();
  }

  @Test
  void stop_whenNotRunning_doesNothing() {
    assertThat(service.isRunning()).isFalse();

    service.stop(); // Should not throw

    assertThat(service.isRunning()).isFalse();
  }

  @Test
  void runCleanup_noTablesWithStreamsEnabled() {
    when(metadataDao.getTablesWithStreamsEnabled()).thenReturn(List.of());

    service.runCleanup();

    // Verify no deletions attempted
    verify(streamDao, never()).deleteOlderThan(anyString(), eq(NOW));
  }

  @Test
  void runCleanup_deletesOldStreamRecords() {
    // Setup: table with streams enabled
    when(metadataDao.getTablesWithStreamsEnabled()).thenReturn(List.of(TABLE_NAME));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenReturn(STREAM_TABLE_NAME);
    when(streamDao.deleteOlderThan(eq(STREAM_TABLE_NAME), cutoffTimeCaptor.capture())).thenReturn(5);

    // Execute
    service.runCleanup();

    // Verify deletion called with 24-hour cutoff
    verify(streamDao).deleteOlderThan(eq(STREAM_TABLE_NAME), cutoffTimeCaptor.capture());

    final Instant cutoffTime = cutoffTimeCaptor.getValue();
    final Instant expected24HoursAgo = NOW.minusSeconds(24 * 3600);

    assertThat(cutoffTime).isEqualTo(expected24HoursAgo);
  }

  @Test
  void runCleanup_multipleTablesWithStreams() {
    final String table2 = "Table2";
    final String streamTable2 = "pdb_stream_table2";

    // Setup: multiple tables with streams
    when(metadataDao.getTablesWithStreamsEnabled()).thenReturn(List.of(TABLE_NAME, table2));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenReturn(STREAM_TABLE_NAME);
    when(streamTableManager.getStreamTableName(table2)).thenReturn(streamTable2);
    when(streamDao.deleteOlderThan(eq(STREAM_TABLE_NAME), cutoffTimeCaptor.capture())).thenReturn(3);
    when(streamDao.deleteOlderThan(eq(streamTable2), cutoffTimeCaptor.capture())).thenReturn(7);

    // Execute
    service.runCleanup();

    // Verify both tables cleaned
    verify(streamDao).deleteOlderThan(eq(STREAM_TABLE_NAME), cutoffTimeCaptor.capture());
    verify(streamDao).deleteOlderThan(eq(streamTable2), cutoffTimeCaptor.capture());
  }

  @Test
  void runCleanup_continuesOnError() {
    final String table2 = "Table2";
    final String streamTable2 = "pdb_stream_table2";

    // Setup: first table throws exception, second should still be processed
    when(metadataDao.getTablesWithStreamsEnabled()).thenReturn(List.of(TABLE_NAME, table2));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenThrow(new RuntimeException("Test error"));
    when(streamTableManager.getStreamTableName(table2)).thenReturn(streamTable2);
    when(streamDao.deleteOlderThan(eq(streamTable2), cutoffTimeCaptor.capture())).thenReturn(2);

    // Execute - should not throw
    service.runCleanup();

    // Verify second table still cleaned despite first error
    verify(streamDao).deleteOlderThan(eq(streamTable2), cutoffTimeCaptor.capture());
  }

  @Test
  void runCleanup_noRecordsDeleted() {
    when(metadataDao.getTablesWithStreamsEnabled()).thenReturn(List.of(TABLE_NAME));
    when(streamTableManager.getStreamTableName(TABLE_NAME)).thenReturn(STREAM_TABLE_NAME);
    when(streamDao.deleteOlderThan(eq(STREAM_TABLE_NAME), cutoffTimeCaptor.capture())).thenReturn(0);

    service.runCleanup();

    verify(streamDao).deleteOlderThan(eq(STREAM_TABLE_NAME), cutoffTimeCaptor.capture());
  }
}
