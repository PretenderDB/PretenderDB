package io.github.pretenderdb.dao;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pretenderdb.BaseJdbiTest;
import io.github.pretenderdb.manager.PdbItemTableManager;
import io.github.pretenderdb.model.ImmutablePdbItem;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbItem;
import io.github.pretenderdb.model.PdbMetadata;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PdbItemDaoTest extends BaseJdbiTest {

  private PdbItemDao dao;
  private PdbItemTableManager tableManager;
  private String testTableName;

  @BeforeEach
  void setup() {
    dao = new PdbItemDao(jdbi, configuration.database());
    tableManager = new PdbItemTableManager(jdbi, configuration.database());

    // Create a test table
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("test_items")
        .hashKey("id")
        .createDate(Instant.now())
        .build();

    tableManager.createItemTable(metadata);
    testTableName = tableManager.getItemTableName("test_items");
  }

  @Test
  void insert_get_roundTrip() {
    final PdbItem item = ImmutablePdbItem.builder()
        .tableName(testTableName)
        .hashKeyValue("item-123")
        .attributesJson("{\"id\":{\"S\":\"item-123\"},\"name\":{\"S\":\"Test Item\"}}")
        .createDate(Instant.now())
        .updateDate(Instant.now())
        .build();

    final boolean inserted = dao.insert(testTableName, item);
    assertThat(inserted).isTrue();

    final Optional<PdbItem> retrieved = dao.get(testTableName, "item-123", Optional.empty());
    assertThat(retrieved).isPresent();
    assertThat(retrieved.get().hashKeyValue()).isEqualTo("item-123");
    assertThat(retrieved.get().attributesJson()).contains("Test Item");
  }

  @Test
  void insert_withSortKey() {
    // Create table with sort key
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("sorted_items")
        .hashKey("userId")
        .sortKey("timestamp")
        .createDate(Instant.now())
        .build();

    tableManager.createItemTable(metadata);
    final String sortedTableName = tableManager.getItemTableName("sorted_items");

    final PdbItem item = ImmutablePdbItem.builder()
        .tableName(sortedTableName)
        .hashKeyValue("user-456")
        .sortKeyValue("2024-01-01T00:00:00Z")
        .attributesJson("{\"userId\":{\"S\":\"user-456\"},\"timestamp\":{\"S\":\"2024-01-01T00:00:00Z\"}}")
        .createDate(Instant.now())
        .updateDate(Instant.now())
        .build();

    final boolean inserted = dao.insert(sortedTableName, item);
    assertThat(inserted).isTrue();

    final Optional<PdbItem> retrieved = dao.get(sortedTableName, "user-456",
        Optional.of("2024-01-01T00:00:00Z"));
    assertThat(retrieved).isPresent();
    assertThat(retrieved.get().sortKeyValue()).contains("2024-01-01T00:00:00Z");
  }

  @Test
  void get_nonExistent() {
    final Optional<PdbItem> retrieved = dao.get(testTableName, "not-found", Optional.empty());
    assertThat(retrieved).isEmpty();
  }

  @Test
  void update() {
    // Insert initial item
    final PdbItem initial = ImmutablePdbItem.builder()
        .tableName(testTableName)
        .hashKeyValue("item-789")
        .attributesJson("{\"id\":{\"S\":\"item-789\"},\"count\":{\"N\":\"10\"}}")
        .createDate(Instant.now())
        .updateDate(Instant.now())
        .build();

    dao.insert(testTableName, initial);

    // Update the item
    final PdbItem updated = ImmutablePdbItem.builder()
        .from(initial)
        .attributesJson("{\"id\":{\"S\":\"item-789\"},\"count\":{\"N\":\"20\"}}")
        .updateDate(Instant.now())
        .build();

    final boolean result = dao.update(testTableName, updated);
    assertThat(result).isTrue();

    // Verify update
    final Optional<PdbItem> retrieved = dao.get(testTableName, "item-789", Optional.empty());
    assertThat(retrieved).isPresent();
    assertThat(retrieved.get().attributesJson()).contains("\"N\":\"20\"");
  }

  @Test
  void delete() {
    // Insert item
    final PdbItem item = ImmutablePdbItem.builder()
        .tableName(testTableName)
        .hashKeyValue("item-delete")
        .attributesJson("{\"id\":{\"S\":\"item-delete\"}}")
        .createDate(Instant.now())
        .updateDate(Instant.now())
        .build();

    dao.insert(testTableName, item);

    // Delete it
    final boolean deleted = dao.delete(testTableName, "item-delete", Optional.empty());
    assertThat(deleted).isTrue();

    // Verify deletion
    final Optional<PdbItem> retrieved = dao.get(testTableName, "item-delete", Optional.empty());
    assertThat(retrieved).isEmpty();
  }

  @Test
  void delete_nonExistent() {
    final boolean deleted = dao.delete(testTableName, "not-there", Optional.empty());
    assertThat(deleted).isFalse();
  }

  @Test
  void query_byHashKey() {
    // Create table with sort key
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("query_test")
        .hashKey("userId")
        .sortKey("timestamp")
        .createDate(Instant.now())
        .build();

    tableManager.createItemTable(metadata);
    final String queryTableName = tableManager.getItemTableName("query_test");

    // Insert multiple items with same hash key
    for (int i = 1; i <= 5; i++) {
      final PdbItem item = ImmutablePdbItem.builder()
          .tableName(queryTableName)
          .hashKeyValue("user-query")
          .sortKeyValue("2024-01-0" + i)
          .attributesJson("{\"userId\":{\"S\":\"user-query\"},\"timestamp\":{\"S\":\"2024-01-0" + i + "\"}}")
          .createDate(Instant.now())
          .updateDate(Instant.now())
          .build();
      dao.insert(queryTableName, item);
    }

    // Query by hash key only
    final List<PdbItem> results = dao.query(queryTableName, "user-query", null, Optional.empty(), 10);
    assertThat(results).hasSize(5);
  }

  @Test
  void query_withLimit() {
    // Insert multiple items
    for (int i = 1; i <= 10; i++) {
      final PdbItem item = ImmutablePdbItem.builder()
          .tableName(testTableName)
          .hashKeyValue("item-" + i)
          .attributesJson("{\"id\":{\"S\":\"item-" + i + "\"}}")
          .createDate(Instant.now())
          .updateDate(Instant.now())
          .build();
      dao.insert(testTableName, item);
    }

    // Scan with limit
    final List<PdbItem> results = dao.scan(testTableName, 5);
    assertThat(results).hasSize(5);
  }

  @Test
  void scan() {
    // Insert items
    for (int i = 1; i <= 3; i++) {
      final PdbItem item = ImmutablePdbItem.builder()
          .tableName(testTableName)
          .hashKeyValue("scan-item-" + i)
          .attributesJson("{\"id\":{\"S\":\"scan-item-" + i + "\"}}")
          .createDate(Instant.now())
          .updateDate(Instant.now())
          .build();
      dao.insert(testTableName, item);
    }

    final List<PdbItem> results = dao.scan(testTableName, 100);
    assertThat(results).hasSizeGreaterThanOrEqualTo(3);
  }

  @Test
  void scan_emptyTable() {
    final List<PdbItem> results = dao.scan(testTableName, 100);
    assertThat(results).isEmpty();
  }
}
