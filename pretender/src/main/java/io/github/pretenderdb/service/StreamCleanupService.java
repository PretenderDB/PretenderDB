package io.github.pretenderdb.service;

import io.github.pretenderdb.dao.PdbMetadataDao;
import io.github.pretenderdb.dao.PdbStreamDao;
import io.github.pretenderdb.manager.PdbStreamTableManager;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background service for cleaning up expired stream records.
 * Runs periodically to delete stream records older than 24 hours (matching AWS behavior).
 */
@Singleton
public class StreamCleanupService {

  private static final Logger log = LoggerFactory.getLogger(StreamCleanupService.class);
  private static final long DEFAULT_CLEANUP_INTERVAL_SECONDS = 3600; // 60 minutes
  private static final long STREAM_RETENTION_HOURS = 24;

  private final PdbMetadataDao metadataDao;
  private final PdbStreamDao streamDao;
  private final PdbStreamTableManager streamTableManager;
  private final Clock clock;

  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean running;
  private final long cleanupIntervalSeconds;

  /**
   * Instantiates a new Stream cleanup service.
   *
   * @param metadataDao        the metadata dao
   * @param streamDao          the stream dao
   * @param streamTableManager the stream table manager
   * @param clock              the clock
   */
  @Inject
  public StreamCleanupService(final PdbMetadataDao metadataDao,
                              final PdbStreamDao streamDao,
                              final PdbStreamTableManager streamTableManager,
                              final Clock clock) {
    this(metadataDao, streamDao, streamTableManager, clock, DEFAULT_CLEANUP_INTERVAL_SECONDS);
  }

  /**
   * Instantiates a new Stream cleanup service with custom interval.
   *
   * @param metadataDao            the metadata dao
   * @param streamDao              the stream dao
   * @param streamTableManager     the stream table manager
   * @param clock                  the clock
   * @param cleanupIntervalSeconds the cleanup interval in seconds
   */
  public StreamCleanupService(final PdbMetadataDao metadataDao,
                              final PdbStreamDao streamDao,
                              final PdbStreamTableManager streamTableManager,
                              final Clock clock,
                              final long cleanupIntervalSeconds) {
    log.info("StreamCleanupService({}, {}, {}, {}, interval={}s)",
        metadataDao, streamDao, streamTableManager, clock, cleanupIntervalSeconds);
    this.metadataDao = metadataDao;
    this.streamDao = streamDao;
    this.streamTableManager = streamTableManager;
    this.clock = clock;
    this.cleanupIntervalSeconds = cleanupIntervalSeconds;
    this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread thread = new Thread(r, "stream-cleanup");
      thread.setDaemon(true);
      return thread;
    });
    this.running = new AtomicBoolean(false);
  }

  /**
   * Starts the background cleanup service.
   */
  public void start() {
    if (running.compareAndSet(false, true)) {
      log.info("Starting stream cleanup service with interval {}s", cleanupIntervalSeconds);
      scheduler.scheduleAtFixedRate(
          this::runCleanup,
          cleanupIntervalSeconds,  // Initial delay
          cleanupIntervalSeconds,  // Period
          TimeUnit.SECONDS
      );
    } else {
      log.warn("Stream cleanup service already running");
    }
  }

  /**
   * Stops the background cleanup service.
   */
  public void stop() {
    if (running.compareAndSet(true, false)) {
      log.info("Stopping stream cleanup service");
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    } else {
      log.warn("Stream cleanup service not running");
    }
  }

  /**
   * Checks if the cleanup service is running.
   *
   * @return true if running
   */
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Runs a single cleanup cycle.
   * Public for testing.
   */
  public void runCleanup() {
    try {
      log.debug("Starting stream cleanup cycle");
      final long startTime = System.currentTimeMillis();
      int totalDeleted = 0;

      // Calculate cutoff time (24 hours ago)
      final Instant cutoffTime = clock.instant().minus(STREAM_RETENTION_HOURS, ChronoUnit.HOURS);

      // Get all tables with streams enabled
      final List<String> tableNames = metadataDao.getTablesWithStreamsEnabled();

      for (String tableName : tableNames) {
        try {
          // Get stream table name
          final String streamTableName = streamTableManager.getStreamTableName(tableName);

          // Delete old records
          final int deleted = streamDao.deleteOlderThan(streamTableName, cutoffTime);
          totalDeleted += deleted;

          if (deleted > 0) {
            log.info("Deleted {} expired stream records from table {}", deleted, tableName);
          }
        } catch (Exception e) {
          log.error("Error cleaning up stream records for table {}", tableName, e);
        }
      }

      final long duration = System.currentTimeMillis() - startTime;
      log.debug("Stream cleanup cycle completed in {}ms, deleted {} records", duration, totalDeleted);
    } catch (Exception e) {
      log.error("Error during stream cleanup", e);
    }
  }
}
