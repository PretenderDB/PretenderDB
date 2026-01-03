package io.github.pretenderdb.dao;

import io.github.pretenderdb.model.PdbMetadata;
import java.util.List;
import java.util.Optional;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

/**
 * The interface PdbMetadata dao.
 */
public interface PdbMetadataDao {

  /**
   * List table names list.
   *
   * @return the list
   */
  @SqlQuery("select NAME from PDB_TABLE order by NAME asc")
  List<String> listTableNames();

  /**
   * Gets table.
   *
   * @param name the name
   * @return the table
   */
  @SqlQuery("select * from PDB_TABLE where NAME = :name")
  Optional<PdbMetadata> getTable(@Bind("name") String name);

  /**
   * Insert boolean.
   *
   * @param pdbMetadata the pdb table
   * @return the boolean
   */
  @SqlUpdate("insert into PDB_TABLE (NAME, HASH_KEY, SORT_KEY, GLOBAL_SECONDARY_INDEXES, TTL_ATTRIBUTE_NAME, TTL_ENABLED, " +
      "STREAM_ENABLED, STREAM_VIEW_TYPE, STREAM_ARN, STREAM_LABEL, CREATE_DATE) "
      + "values (:name, :hashKey, :sortKey, :globalSecondaryIndexes, :ttlAttributeName, :ttlEnabled, " +
      ":streamEnabled, :streamViewType, :streamArn, :streamLabel, :createDate)")
  boolean insert(@BindPojo PdbMetadata pdbMetadata);

  /**
   * Delete boolean.
   *
   * @param name the name
   * @return the boolean
   */
  @SqlUpdate("delete from PDB_TABLE where NAME = :name")
  boolean delete(@Bind("name") String name);

  /**
   * Update TTL settings.
   *
   * @param name             the table name
   * @param ttlAttributeName the TTL attribute name
   * @param ttlEnabled       whether TTL is enabled
   * @return the boolean
   */
  @SqlUpdate("update PDB_TABLE set TTL_ATTRIBUTE_NAME = :ttlAttributeName, TTL_ENABLED = :ttlEnabled where NAME = :name")
  boolean updateTtl(@Bind("name") String name,
                    @Bind("ttlAttributeName") String ttlAttributeName,
                    @Bind("ttlEnabled") boolean ttlEnabled);

  /**
   * Update stream configuration.
   *
   * @param name           the table name
   * @param streamEnabled  whether streams are enabled
   * @param streamViewType the stream view type (KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES)
   * @param streamArn      the stream ARN
   * @param streamLabel    the stream label
   * @return the boolean
   */
  @SqlUpdate("update PDB_TABLE set STREAM_ENABLED = :streamEnabled, STREAM_VIEW_TYPE = :streamViewType, " +
      "STREAM_ARN = :streamArn, STREAM_LABEL = :streamLabel where NAME = :name")
  boolean updateStreamConfig(@Bind("name") String name,
                             @Bind("streamEnabled") boolean streamEnabled,
                             @Bind("streamViewType") String streamViewType,
                             @Bind("streamArn") String streamArn,
                             @Bind("streamLabel") String streamLabel);

  /**
   * Gets list of table names with streams enabled.
   *
   * @return the list of table names
   */
  @SqlQuery("select NAME from PDB_TABLE where STREAM_ENABLED = true order by NAME asc")
  List<String> getTablesWithStreamsEnabled();

}
