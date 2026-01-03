package io.github.pretenderdb.manager;

import io.github.pretenderdb.dbu.model.Database;
import io.github.pretenderdb.model.PdbGlobalSecondaryIndex;
import io.github.pretenderdb.model.PdbMetadata;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages dynamic creation and deletion of item storage tables.
 * Each DynamoDB table gets a corresponding SQL table for storing items.
 */
@Singleton
public class PdbItemTableManager {

  private static final Logger log = LoggerFactory.getLogger(PdbItemTableManager.class);
  private static final String TABLE_PREFIX = "pdb_item_";
  private static final String INDEX_PREFIX = "idx_";

  private final Jdbi jdbi;
  private final Database database;

  /**
   * Instantiates a new Pdb item table manager.
   *
   * @param jdbi     the jdbi
   * @param database the database configuration
   */
  @Inject
  public PdbItemTableManager(final Jdbi jdbi, final Database database) {
    log.info("PdbItemTableManager({}, {})", jdbi, database);
    this.jdbi = jdbi;
    this.database = database;
  }

  /**
   * Creates an item storage table for a DynamoDB table.
   *
   * @param metadata the table metadata
   */
  public void createItemTable(final PdbMetadata metadata) {
    final String itemTableName = getItemTableName(metadata.name());
    log.info("createItemTable: {}", itemTableName);

    jdbi.useHandle(handle -> {
      // Create the main item table with hybrid storage approach
      final String createTableSql = buildCreateTableSql(itemTableName, metadata.hashKey(), metadata.sortKey().orElse(null));
      handle.execute(createTableSql);

      // Create index on hash key for efficient lookups (best effort)
      try {
        final String indexName = INDEX_PREFIX + sanitizeTableName(metadata.name()) + "_hash";
        final String createIndexSql = String.format(
            "CREATE INDEX \"%s\" ON \"%s\" (hash_key_value)",
            indexName,
            itemTableName
        );
        handle.execute(createIndexSql);
      } catch (Exception e) {
        // Index creation is best effort - log and continue
        log.warn("Failed to create index for table {}: {}", metadata.name(), e.getMessage());
      }

      // Create GSI tables
      for (PdbGlobalSecondaryIndex gsi : metadata.globalSecondaryIndexes()) {
        createGsiTable(handle, metadata.name(), gsi, metadata);
      }

      log.debug("Created item table and {} GSI tables for: {}", metadata.globalSecondaryIndexes().size(), metadata.name());
    });
  }

  /**
   * Drops an item storage table and all associated GSI tables.
   *
   * @param tableName the DynamoDB table name
   */
  public void dropItemTable(final String tableName) {
    final String itemTableName = getItemTableName(tableName);
    log.info("dropItemTable: {}", itemTableName);

    jdbi.useHandle(handle -> {
      // Drop main item table (CASCADE will drop any dependent objects)
      final String dropTableSql = String.format(
          "DROP TABLE IF EXISTS \"%s\" CASCADE",
          itemTableName
      );
      handle.execute(dropTableSql);

      // Drop GSI tables (pattern: pdb_item_<tablename>_gsi_*)
      // Note: We don't have the metadata here, so we drop by pattern
      // This is safe because table names are sanitized
      final String gsiPattern = TABLE_PREFIX + sanitizeTableName(tableName) + "_gsi_%";

      // For HSQLDB/PostgreSQL, we query information_schema to find GSI tables
      try {
        final String findGsiTablesSql = database.usePostgresql()
            ? "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '" + gsiPattern + "' AND table_schema = 'public'"
            : "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '" + gsiPattern + "' AND table_schema = 'PUBLIC'";

        handle.createQuery(findGsiTablesSql)
            .mapTo(String.class)
            .forEach(gsiTableName -> {
              final String dropGsiSql = String.format("DROP TABLE IF EXISTS \"%s\" CASCADE", gsiTableName);
              handle.execute(dropGsiSql);
              log.debug("Dropped GSI table: {}", gsiTableName);
            });
      } catch (Exception e) {
        log.warn("Error dropping GSI tables for {}: {}", tableName, e.getMessage());
      }

      log.debug("Dropped item table: {}", itemTableName);
    });
  }

  /**
   * Gets the PostgreSQL table name for storing items.
   *
   * @param dynamoTableName the dynamo table name
   * @return the item table name
   */
  public String getItemTableName(final String dynamoTableName) {
    return TABLE_PREFIX + sanitizeTableName(dynamoTableName);
  }

  /**
   * Gets the PostgreSQL table name for a GSI.
   *
   * @param dynamoTableName the dynamo table name
   * @param indexName       the GSI name
   * @return the GSI table name
   */
  public String getGsiTableName(final String dynamoTableName, final String indexName) {
    return TABLE_PREFIX + sanitizeTableName(dynamoTableName) + "_gsi_" + sanitizeTableName(indexName);
  }

  /**
   * Creates a GSI table.
   * GSI tables need to store the main table's keys as well to uniquely identify items.
   *
   * @param handle    the JDBI handle
   * @param tableName the DynamoDB table name
   * @param gsi       the GSI metadata
   */
  private void createGsiTable(final org.jdbi.v3.core.Handle handle, final String tableName, final PdbGlobalSecondaryIndex gsi,
                              final PdbMetadata metadata) {
    final String gsiTableName = getGsiTableName(tableName, gsi.indexName());
    log.info("createGsiTable: {}", gsiTableName);

    try {
      // GSI tables use the same structure as main tables:
      // - hash_key_value stores the GSI hash key value
      // - sort_key_value stores a composite of GSI sort key (if present) + main table keys for uniqueness
      // This ensures each item in the GSI table has a unique primary key
      final String createTableSql = buildGsiTableSql(gsiTableName, gsi, metadata);
      handle.execute(createTableSql);

      // Create index on GSI hash key
      try {
        final String indexName = INDEX_PREFIX + sanitizeTableName(tableName) + "_gsi_" + sanitizeTableName(gsi.indexName()) + "_hash";
        final String createIndexSql = String.format(
            "CREATE INDEX \"%s\" ON \"%s\" (hash_key_value)",
            indexName,
            gsiTableName
        );
        handle.execute(createIndexSql);
      } catch (Exception e) {
        log.warn("Failed to create index for GSI table {}: {}", gsiTableName, e.getMessage());
      }

      log.debug("Created GSI table: {}", gsiTableName);
    } catch (Exception e) {
      log.error("Failed to create GSI table {}: {}", gsiTableName, e.getMessage(), e);
      throw new RuntimeException("Failed to create GSI table: " + gsiTableName, e);
    }
  }

  /**
   * Builds the CREATE TABLE SQL for an item storage table.
   *
   * @param tableName the table name
   * @param hashKey   the hash key attribute name (not used for column, just for documentation)
   * @param sortKey   the sort key attribute name (null if no sort key)
   * @return the CREATE TABLE SQL
   */
  private String buildCreateTableSql(final String tableName, final String hashKey, final String sortKey) {
    // Determine if table has a sort key
    final boolean hasSortKey = sortKey != null;

    // Build primary key constraint
    final String primaryKeyConstraint = hasSortKey
        ? "PRIMARY KEY (hash_key_value, sort_key_value)"
        : "PRIMARY KEY (hash_key_value)";

    // Choose JSON column type based on database
    final String jsonColumnType = database.usePostgresql() ? "JSONB" : "CLOB";

    // Quote table name for HSQLDB/PostgreSQL compatibility
    final String quotedTableName = "\"" + tableName + "\"";

    // Build the SQL
    return String.format("""
            CREATE TABLE IF NOT EXISTS %s (
              hash_key_value VARCHAR(2048) NOT NULL,
              sort_key_value VARCHAR(2048) %s,
              attributes_json %s NOT NULL,
              create_date TIMESTAMP NOT NULL,
              update_date TIMESTAMP NOT NULL,
              %s
            )
            """,
        quotedTableName,
        hasSortKey ? "NOT NULL" : "NULL",
        jsonColumnType,
        primaryKeyConstraint);
  }

  /**
   * Builds the CREATE TABLE SQL for a GSI table.
   * GSI tables always have a composite sort key to ensure uniqueness.
   *
   * @param tableName the table name
   * @param gsi       the GSI metadata
   * @param metadata  the main table metadata
   * @return the CREATE TABLE SQL
   */
  private String buildGsiTableSql(final String tableName, final PdbGlobalSecondaryIndex gsi, final PdbMetadata metadata) {
    // GSI tables ALWAYS need a sort key for uniqueness
    // The sort key is a composite of: GSI sort key (if present) + main table keys
    final String primaryKeyConstraint = "PRIMARY KEY (hash_key_value, sort_key_value)";

    // Choose JSON column type based on database
    final String jsonColumnType = database.usePostgresql() ? "JSONB" : "CLOB";

    // Quote table name for HSQLDB/PostgreSQL compatibility
    final String quotedTableName = "\"" + tableName + "\"";

    // Build the SQL - sort_key_value is always NOT NULL for GSI tables
    return String.format(
        """
            CREATE TABLE IF NOT EXISTS %s (
                hash_key_value VARCHAR(2048) NOT NULL,
                sort_key_value VARCHAR(2048) NOT NULL,
                attributes_json %s NOT NULL,
                create_date TIMESTAMP NOT NULL,
                update_date TIMESTAMP NOT NULL,
                %s)
            """,
        quotedTableName,
        jsonColumnType,
        primaryKeyConstraint
    );
  }

  /**
   * Sanitizes a table name to prevent SQL injection and ensure valid identifier.
   * Only allows alphanumeric characters, underscores, and hyphens.
   */
  private String sanitizeTableName(final String tableName) {
    return tableName.replaceAll("[^a-zA-Z0-9_-]", "_").toLowerCase();
  }
}
