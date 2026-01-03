package io.github.pretenderdb.service;

import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.dao.PdbItemDao;
import io.github.pretenderdb.manager.PdbItemTableManager;
import io.github.pretenderdb.manager.PdbTableManager;
import io.github.pretenderdb.model.PdbGlobalSecondaryIndex;
import io.github.pretenderdb.model.PdbItem;
import io.github.pretenderdb.model.PdbMetadata;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Background service for cleaning up expired TTL items.
 * Runs periodically to scan tables with TTL enabled and delete expired items.
 */
@Singleton
public class TtlCleanupService {

  private static final Logger log = LoggerFactory.getLogger(TtlCleanupService.class);
  private static final long DEFAULT_CLEANUP_INTERVAL_SECONDS = 300; // 5 minutes
  private static final int DEFAULT_BATCH_SIZE = 100;

  private final PdbTableManager tableManager;
  private final PdbItemTableManager itemTableManager;
  private final PdbItemDao itemDao;
  private final AttributeValueConverter attributeValueConverter;
  private final Clock clock;

  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean running;
  private final long cleanupIntervalSeconds;
  private final int batchSize;

  /**
   * Instantiates a new TTL cleanup service.
   *
   * @param tableManager            the table manager
   * @param itemTableManager        the item table manager
   * @param itemDao                 the item DAO
   * @param attributeValueConverter the attribute value converter
   * @param clock                   the clock
   */
  @Inject
  public TtlCleanupService(final PdbTableManager tableManager,
                           final PdbItemTableManager itemTableManager,
                           final PdbItemDao itemDao,
                           final AttributeValueConverter attributeValueConverter,
                           final Clock clock) {
    this(tableManager, itemTableManager, itemDao, attributeValueConverter, clock,
        DEFAULT_CLEANUP_INTERVAL_SECONDS, DEFAULT_BATCH_SIZE);
  }

  /**
   * Instantiates a new TTL cleanup service with custom settings.
   *
   * @param tableManager            the table manager
   * @param itemTableManager        the item table manager
   * @param itemDao                 the item DAO
   * @param attributeValueConverter the attribute value converter
   * @param clock                   the clock
   * @param cleanupIntervalSeconds  the cleanup interval in seconds
   * @param batchSize               the batch size for deletes
   */
  public TtlCleanupService(final PdbTableManager tableManager,
                           final PdbItemTableManager itemTableManager,
                           final PdbItemDao itemDao,
                           final AttributeValueConverter attributeValueConverter,
                           final Clock clock,
                           final long cleanupIntervalSeconds,
                           final int batchSize) {
    log.info("TtlCleanupService({}, {}, {}, {}, {}, interval={}s, batch={})",
        tableManager, itemTableManager, itemDao, attributeValueConverter, clock,
        cleanupIntervalSeconds, batchSize);
    this.tableManager = tableManager;
    this.itemTableManager = itemTableManager;
    this.itemDao = itemDao;
    this.attributeValueConverter = attributeValueConverter;
    this.clock = clock;
    this.cleanupIntervalSeconds = cleanupIntervalSeconds;
    this.batchSize = batchSize;
    this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread thread = new Thread(r, "ttl-cleanup");
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
      log.info("Starting TTL cleanup service with interval {}s", cleanupIntervalSeconds);
      scheduler.scheduleAtFixedRate(
          this::runCleanup,
          cleanupIntervalSeconds,  // Initial delay
          cleanupIntervalSeconds,  // Period
          TimeUnit.SECONDS
      );
    } else {
      log.warn("TTL cleanup service already running");
    }
  }

  /**
   * Stops the background cleanup service.
   */
  public void stop() {
    if (running.compareAndSet(true, false)) {
      log.info("Stopping TTL cleanup service");
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
      log.warn("TTL cleanup service not running");
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
   * Package-private for testing.
   */
  void runCleanup() {
    try {
      log.debug("Starting TTL cleanup cycle");
      final long startTime = System.currentTimeMillis();
      int totalDeleted = 0;

      // Get all tables
      final List<String> tableNames = tableManager.listPdbTables();

      for (String tableName : tableNames) {
        final Optional<PdbMetadata> metadataOpt = tableManager.getPdbTable(tableName);
        if (metadataOpt.isEmpty()) {
          continue;
        }

        final PdbMetadata metadata = metadataOpt.get();

        // Skip tables without TTL enabled
        if (!metadata.ttlEnabled() || metadata.ttlAttributeName().isEmpty()) {
          continue;
        }

        // Clean up expired items in this table
        final int deleted = cleanupTable(metadata);
        totalDeleted += deleted;

        if (deleted > 0) {
          log.info("Deleted {} expired items from table {}", deleted, tableName);
        }
      }

      final long duration = System.currentTimeMillis() - startTime;
      log.debug("TTL cleanup cycle completed in {}ms, deleted {} items", duration, totalDeleted);
    } catch (Exception e) {
      log.error("Error during TTL cleanup", e);
    }
  }

  /**
   * Cleans up expired items from a single table.
   *
   * @param metadata the table metadata
   * @return the number of items deleted
   */
  private int cleanupTable(final PdbMetadata metadata) {
    final String tableName = metadata.name();
    final String itemTableName = itemTableManager.getItemTableName(tableName);
    int deletedCount = 0;

    try {
      // Scan for all items in the table
      final List<PdbItem> allItems = itemDao.scan(itemTableName, Integer.MAX_VALUE);
      final List<PdbItem> expiredItems = new ArrayList<>();

      // Filter for expired items
      for (PdbItem item : allItems) {
        final Map<String, AttributeValue> attributes = attributeValueConverter.fromJson(item.attributesJson());

        if (isExpired(metadata, attributes)) {
          expiredItems.add(item);
        }

        // Process in batches
        if (expiredItems.size() >= batchSize) {
          deleteBatch(metadata, expiredItems);
          deletedCount += expiredItems.size();
          expiredItems.clear();
        }
      }

      // Delete remaining items
      if (!expiredItems.isEmpty()) {
        deleteBatch(metadata, expiredItems);
        deletedCount += expiredItems.size();
      }

    } catch (Exception e) {
      log.error("Error cleaning up table {}", tableName, e);
    }

    return deletedCount;
  }

  /**
   * Deletes a batch of expired items from main and GSI tables.
   *
   * @param metadata     the table metadata
   * @param expiredItems the expired items to delete
   */
  private void deleteBatch(final PdbMetadata metadata, final List<PdbItem> expiredItems) {
    final String itemTableName = itemTableManager.getItemTableName(metadata.name());

    for (PdbItem item : expiredItems) {
      try {
        // Delete from main table
        itemDao.delete(itemTableName, item.hashKeyValue(), item.sortKeyValue());

        // Delete from GSI tables
        final Map<String, AttributeValue> attributes = attributeValueConverter.fromJson(item.attributesJson());
        deleteFromGsiTables(metadata, attributes, item.hashKeyValue(), item.sortKeyValue());

        log.trace("Deleted expired item from table {}: {}", metadata.name(), item.hashKeyValue());
      } catch (Exception e) {
        log.error("Error deleting expired item from table {}: {}", metadata.name(), item.hashKeyValue(), e);
      }
    }
  }

  /**
   * Deletes an item from all GSI tables.
   *
   * @param metadata         the table metadata
   * @param itemAttrs        the item attributes
   * @param mainHashKeyValue the main table hash key value
   * @param mainSortKeyValue the main table sort key value
   */
  private void deleteFromGsiTables(final PdbMetadata metadata,
                                   final Map<String, AttributeValue> itemAttrs,
                                   final String mainHashKeyValue,
                                   final Optional<String> mainSortKeyValue) {
    if (metadata.globalSecondaryIndexes().isEmpty()) {
      return;
    }

    for (PdbGlobalSecondaryIndex gsi : metadata.globalSecondaryIndexes()) {
      try {
        // Check if item has GSI keys
        if (!hasGsiKeys(itemAttrs, gsi)) {
          continue;
        }

        // Extract GSI key values
        final String gsiHashKeyValue = attributeValueConverter.extractKeyValue(itemAttrs, gsi.hashKey());
        final Optional<String> gsiSortKeyValue = gsi.sortKey().map(sk ->
            attributeValueConverter.extractKeyValue(itemAttrs, sk));

        // Build composite sort key
        final String compositeSortKey = buildCompositeSortKey(
            gsiSortKeyValue,
            mainHashKeyValue,
            mainSortKeyValue
        );

        // Delete from GSI table
        final String gsiTableName = itemTableManager.getGsiTableName(metadata.name(), gsi.indexName());
        itemDao.delete(gsiTableName, gsiHashKeyValue, Optional.of(compositeSortKey));

        log.trace("Deleted expired item from GSI table {}", gsiTableName);
      } catch (Exception e) {
        log.error("Error deleting from GSI table {}: {}", gsi.indexName(), e.getMessage());
      }
    }
  }

  /**
   * Checks if an item has expired based on TTL.
   *
   * @param metadata  the table metadata
   * @param itemAttrs the item attributes
   * @return true if the item is expired
   */
  private boolean isExpired(final PdbMetadata metadata, final Map<String, AttributeValue> itemAttrs) {
    if (!metadata.ttlEnabled() || metadata.ttlAttributeName().isEmpty()) {
      return false;
    }

    final String ttlAttrName = metadata.ttlAttributeName().get();
    if (!itemAttrs.containsKey(ttlAttrName)) {
      return false;
    }

    final AttributeValue ttlValue = itemAttrs.get(ttlAttrName);
    if (ttlValue.n() == null) {
      return false;
    }

    try {
      final long ttlEpochSeconds = Long.parseLong(ttlValue.n());
      final long currentEpochSeconds = clock.instant().getEpochSecond();
      return currentEpochSeconds > ttlEpochSeconds;
    } catch (NumberFormatException e) {
      log.warn("Invalid TTL value for attribute {}: {}", ttlAttrName, ttlValue.n());
      return false;
    }
  }

  /**
   * Checks if item has all required GSI keys.
   *
   * @param itemAttrs the item attributes
   * @param gsi       the GSI metadata
   * @return true if item has all required GSI keys
   */
  private boolean hasGsiKeys(final Map<String, AttributeValue> itemAttrs, final PdbGlobalSecondaryIndex gsi) {
    if (!itemAttrs.containsKey(gsi.hashKey())) {
      return false;
    }

    return gsi.sortKey().map(itemAttrs::containsKey).orElse(true);
  }

  /**
   * Builds a composite sort key for GSI tables to ensure uniqueness.
   *
   * @param gsiSortKeyValue  the GSI sort key value (if present)
   * @param mainHashKeyValue the main table hash key value
   * @param mainSortKeyValue the main table sort key value (if present)
   * @return the composite sort key
   */
  private String buildCompositeSortKey(final Optional<String> gsiSortKeyValue,
                                       final String mainHashKeyValue,
                                       final Optional<String> mainSortKeyValue) {
    final StringBuilder sb = new StringBuilder();

    gsiSortKeyValue.ifPresent(gsk -> sb.append(gsk).append("#"));
    sb.append(mainHashKeyValue);
    mainSortKeyValue.ifPresent(msk -> sb.append("#").append(msk));

    return sb.toString();
  }
}
