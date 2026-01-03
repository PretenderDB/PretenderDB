package io.github.pretenderdb.manager;

import io.github.pretenderdb.dao.PdbMetadataDao;
import io.github.pretenderdb.model.PdbMetadata;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type PdbMetadata manager.
 */
@Singleton
public class PdbTableManager {

  private static final Logger log = LoggerFactory.getLogger(PdbTableManager.class);

  private final PdbMetadataDao pdbMetadataDao;
  private final PdbItemTableManager pdbItemTableManager;
  private final PdbStreamTableManager pdbStreamTableManager;

  /**
   * Instantiates a new PdbMetadata manager.
   *
   * @param pdbMetadataDao        the dao
   * @param pdbItemTableManager   the item table manager
   * @param pdbStreamTableManager the stream table manager
   */
  @Inject
  public PdbTableManager(final PdbMetadataDao pdbMetadataDao,
                         final PdbItemTableManager pdbItemTableManager,
                         final PdbStreamTableManager pdbStreamTableManager) {
    log.info("PdbTableManager({}, {}, {})", pdbMetadataDao, pdbItemTableManager, pdbStreamTableManager);
    this.pdbMetadataDao = pdbMetadataDao;
    this.pdbItemTableManager = pdbItemTableManager;
    this.pdbStreamTableManager = pdbStreamTableManager;
  }

  /**
   * Insert pdb table boolean.
   *
   * @param pdbMetadata the pdb table
   * @return the boolean
   */
  public boolean insertPdbTable(final PdbMetadata pdbMetadata) {
    log.trace("insertPdbTable({})", pdbMetadata);
    if (getPdbTable(pdbMetadata.name()).isPresent()) {
      log.warn("Table already exists: {}", pdbMetadata);
      return false;
    }
    try {
      final boolean inserted = pdbMetadataDao.insert(pdbMetadata);
      if (inserted) {
        // Create the corresponding item storage table
        pdbItemTableManager.createItemTable(pdbMetadata);
        log.info("Created DynamoDB table and item storage table: {}", pdbMetadata.name());
      }
      return inserted;
    } catch (UnableToExecuteStatementException e) {
      if (e.getCause() instanceof SQLIntegrityConstraintViolationException) {
        log.warn("Table already exists: {}", pdbMetadata);
        return false;
      } else {
        log.error("Unable to insert table: {}", pdbMetadata, e);
        throw e;
      }
    }
  }

  /**
   * Gets pdb table.
   *
   * @param name the name
   * @return the pdb table
   */
  public Optional<PdbMetadata> getPdbTable(final String name) {
    log.trace("getPdbTable({})", name);
    return pdbMetadataDao.getTable(name);
  }

  /**
   * Delete pdb table boolean.
   *
   * @param name the name
   * @return the boolean
   */
  public boolean deletePdbTable(final String name) {
    log.trace("deletePdbTable({})", name);
    final boolean deleted = pdbMetadataDao.delete(name);
    if (deleted) {
      // Drop the corresponding item storage table
      pdbItemTableManager.dropItemTable(name);
      log.info("Deleted DynamoDB table and item storage table: {}", name);
    }
    return deleted;
  }

  /**
   * List pdb tables list.
   *
   * @return the list
   */
  public List<String> listPdbTables() {
    log.trace("listPdbTables()");
    return pdbMetadataDao.listTableNames();
  }

  /**
   * Enable TTL on a table.
   *
   * @param tableName        the table name
   * @param ttlAttributeName the TTL attribute name
   */
  public void enableTtl(final String tableName, final String ttlAttributeName) {
    log.trace("enableTtl({}, {})", tableName, ttlAttributeName);
    pdbMetadataDao.updateTtl(tableName, ttlAttributeName, true);
    log.info("Enabled TTL on table {} with attribute {}", tableName, ttlAttributeName);
  }

  /**
   * Disable TTL on a table.
   *
   * @param tableName the table name
   */
  public void disableTtl(final String tableName) {
    log.trace("disableTtl({})", tableName);
    pdbMetadataDao.updateTtl(tableName, null, false);
    log.info("Disabled TTL on table {}", tableName);
  }

  /**
   * Enable DynamoDB Streams on a table.
   *
   * @param tableName      the table name
   * @param streamViewType the stream view type (KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES)
   */
  public void enableStream(final String tableName, final String streamViewType) {
    log.trace("enableStream({}, {})", tableName, streamViewType);

    // Validate table exists
    final Optional<PdbMetadata> metadata = getPdbTable(tableName);
    if (metadata.isEmpty()) {
      throw new IllegalArgumentException("Table not found: " + tableName);
    }

    // Create stream table if doesn't exist
    pdbStreamTableManager.createStreamTable(tableName);

    // Generate stream ARN and label
    final String streamArn = generateStreamArn(tableName);
    final String streamLabel = generateStreamLabel();

    // Update metadata
    pdbMetadataDao.updateStreamConfig(tableName, true, streamViewType, streamArn, streamLabel);

    log.info("Enabled DynamoDB Streams on table {} with viewType {} (ARN: {})",
        tableName, streamViewType, streamArn);
  }

  /**
   * Disable DynamoDB Streams on a table.
   * Note: Stream table is not dropped - records persist for potential 24-hour retention.
   *
   * @param tableName the table name
   */
  public void disableStream(final String tableName) {
    log.trace("disableStream({})", tableName);
    pdbMetadataDao.updateStreamConfig(tableName, false, null, null, null);
    log.info("Disabled DynamoDB Streams on table {}", tableName);
    // Note: Not dropping stream table - stream records should persist
  }

  /**
   * Generates a stream ARN for a table.
   * Format: arn:aws:dynamodb:us-east-1:123456789012:table/{tableName}/stream/{timestamp}
   *
   * @param tableName the table name
   * @return the stream ARN
   */
  private String generateStreamArn(final String tableName) {
    final long timestamp = System.currentTimeMillis();
    return String.format("arn:aws:dynamodb:us-east-1:000000000000:table/%s/stream/%d",
        tableName, timestamp);
  }

  /**
   * Generates a stream label (timestamp-based identifier).
   *
   * @return the stream label
   */
  private String generateStreamLabel() {
    return String.valueOf(System.currentTimeMillis());
  }
}
