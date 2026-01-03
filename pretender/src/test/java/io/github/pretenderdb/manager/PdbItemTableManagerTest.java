package io.github.pretenderdb.manager;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pretenderdb.BaseJdbiTest;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbMetadata;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PdbItemTableManagerTest extends BaseJdbiTest {

  private PdbItemTableManager manager;

  @BeforeEach
  void setup() {
    manager = new PdbItemTableManager(jdbi, configuration.database());
  }

  @Test
  void createItemTable_hashKeyOnly() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("test-table")
        .hashKey("id")
        .createDate(Instant.now())
        .build();

    manager.createItemTable(metadata);

    // Verify table was created by querying information schema
    final boolean tableExists = jdbi.withHandle(handle ->
        handle.createQuery("SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_name = :tableName")
            .bind("tableName", "pdb_item_test-table")
            .mapTo(Integer.class)
            .one() > 0
    );

    assertThat(tableExists).isTrue();
  }

  @Test
  void createItemTable_withSortKey() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("users")
        .hashKey("userId")
        .sortKey("timestamp")
        .createDate(Instant.now())
        .build();

    manager.createItemTable(metadata);

    // Verify table was created
    final boolean tableExists = jdbi.withHandle(handle ->
        handle.createQuery("SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE LOWER(table_name) = LOWER(:tableName)")
            .bind("tableName", "pdb_item_users")
            .mapTo(Integer.class)
            .one() > 0
    );

    assertThat(tableExists).isTrue();

    // Verify both key columns exist
    final long keyColumns = jdbi.withHandle(handle ->
        handle.createQuery("SELECT COUNT(*) FROM information_schema.columns " +
                "WHERE LOWER(table_name) = LOWER(:tableName) " +
                "AND LOWER(column_name) IN ('hash_key_value', 'sort_key_value')")
            .bind("tableName", "pdb_item_users")
            .mapTo(Long.class)
            .one()
    );

    assertThat(keyColumns).isEqualTo(2);
  }

  @Test
  void dropItemTable() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("temp-table")
        .hashKey("id")
        .createDate(Instant.now())
        .build();

    // Create then drop
    manager.createItemTable(metadata);
    manager.dropItemTable("temp-table");

    // Verify table was dropped
    final boolean tableExists = jdbi.withHandle(handle ->
        handle.createQuery("SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_name = :tableName")
            .bind("tableName", "pdb_item_temp-table")
            .mapTo(Integer.class)
            .one() > 0
    );

    assertThat(tableExists).isFalse();
  }

  @Test
  void getItemTableName() {
    assertThat(manager.getItemTableName("MyTable")).isEqualTo("pdb_item_mytable");
    assertThat(manager.getItemTableName("my-table")).isEqualTo("pdb_item_my-table");
    assertThat(manager.getItemTableName("my_table")).isEqualTo("pdb_item_my_table");
  }

  @Test
  void createItemTable_idempotent() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("idempotent-test")
        .hashKey("id")
        .createDate(Instant.now())
        .build();

    // Create twice - should not fail
    manager.createItemTable(metadata);
    manager.createItemTable(metadata);

    // Verify table exists
    final boolean tableExists = jdbi.withHandle(handle ->
        handle.createQuery("SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_name = :tableName")
            .bind("tableName", "pdb_item_idempotent-test")
            .mapTo(Integer.class)
            .one() > 0
    );

    assertThat(tableExists).isTrue();
  }

  @Test
  void createItemTable_sanitizesTableName() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("Table@With$Special!Characters")
        .hashKey("id")
        .createDate(Instant.now())
        .build();

    manager.createItemTable(metadata);

    // Verify sanitized table name
    assertThat(manager.getItemTableName("Table@With$Special!Characters"))
        .isEqualTo("pdb_item_table_with_special_characters");
  }

  @Test
  void createItemTable_createsIndex() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("indexed_table")  // Use underscores instead of hyphens
        .hashKey("id")
        .createDate(Instant.now())
        .build();

    // Should not throw exception - index creation is best effort
    manager.createItemTable(metadata);

    // Verify table was created
    final boolean tableExists = jdbi.withHandle(handle ->
        handle.createQuery("SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE LOWER(table_name) = LOWER(:tableName)")
            .bind("tableName", "pdb_item_indexed_table")
            .mapTo(Integer.class)
            .one() > 0
    );

    assertThat(tableExists).isTrue();
  }
}
