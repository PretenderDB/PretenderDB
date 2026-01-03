package io.github.pretenderdb.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.dao.PdbItemDao;
import io.github.pretenderdb.manager.PdbItemTableManager;
import io.github.pretenderdb.manager.PdbTableManager;
import io.github.pretenderdb.model.ImmutablePdbGlobalSecondaryIndex;
import io.github.pretenderdb.model.ImmutablePdbItem;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbGlobalSecondaryIndex;
import io.github.pretenderdb.model.PdbItem;
import io.github.pretenderdb.model.PdbMetadata;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Unit tests for TtlCleanupService.
 */
@ExtendWith(MockitoExtension.class)
public class TtlCleanupServiceTest {

  private static final String TABLE_NAME = "TestTable";
  private static final String ITEM_TABLE_NAME = "pdb_item_testtable";
  private static final String TTL_ATTRIBUTE_NAME = "expirationTime";
  private static final Instant NOW = Instant.parse("2025-12-30T12:00:00Z");

  @Mock
  private PdbTableManager tableManager;

  @Mock
  private PdbItemTableManager itemTableManager;

  @Mock
  private PdbItemDao itemDao;

  @Mock
  private AttributeValueConverter attributeValueConverter;

  private Clock clock;
  private TtlCleanupService service;

  @BeforeEach
  void setup() {
    clock = Clock.fixed(NOW, ZoneId.of("UTC"));
    service = new TtlCleanupService(tableManager, itemTableManager, itemDao, attributeValueConverter, clock, 60, 10);
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
  void runCleanup_skipsTablesWithoutTtl() {
    // Setup: table without TTL
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name(TABLE_NAME)
        .hashKey("id")
        .ttlEnabled(false)
        .createDate(NOW)
        .build();

    when(tableManager.listPdbTables()).thenReturn(List.of(TABLE_NAME));
    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));

    // Execute
    service.runCleanup();

    // Verify: no scans performed
    verify(itemDao, org.mockito.Mockito.never()).scan(anyString(), org.mockito.ArgumentMatchers.anyInt());
  }

  @Test
  void runCleanup_deletesExpiredItems() {
    // Setup: table with TTL enabled
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name(TABLE_NAME)
        .hashKey("id")
        .ttlEnabled(true)
        .ttlAttributeName(TTL_ATTRIBUTE_NAME)
        .createDate(NOW)
        .build();

    // Create expired item (1 hour ago)
    final long expiredTimestamp = NOW.getEpochSecond() - 3600;
    final Map<String, AttributeValue> expiredAttributes = Map.of(
        "id", AttributeValue.builder().s("expired-item").build(),
        TTL_ATTRIBUTE_NAME, AttributeValue.builder().n(String.valueOf(expiredTimestamp)).build()
    );
    final String expiredJson = "{\"id\":{\"S\":\"expired-item\"},\"" + TTL_ATTRIBUTE_NAME + "\":{\"N\":\"" + expiredTimestamp + "\"}}";

    final PdbItem expiredItem = ImmutablePdbItem.builder()
        .tableName(ITEM_TABLE_NAME)
        .hashKeyValue("expired-item")
        .sortKeyValue(Optional.empty())
        .attributesJson(expiredJson)
        .createDate(NOW.minusSeconds(7200))
        .updateDate(NOW.minusSeconds(7200))
        .build();

    // Create non-expired item (1 hour in future)
    final long futureTimestamp = NOW.getEpochSecond() + 3600;
    final Map<String, AttributeValue> activeAttributes = Map.of(
        "id", AttributeValue.builder().s("active-item").build(),
        TTL_ATTRIBUTE_NAME, AttributeValue.builder().n(String.valueOf(futureTimestamp)).build()
    );
    final String activeJson = "{\"id\":{\"S\":\"active-item\"},\"" + TTL_ATTRIBUTE_NAME + "\":{\"N\":\"" + futureTimestamp + "\"}}";

    final PdbItem activeItem = ImmutablePdbItem.builder()
        .tableName(ITEM_TABLE_NAME)
        .hashKeyValue("active-item")
        .sortKeyValue(Optional.empty())
        .attributesJson(activeJson)
        .createDate(NOW)
        .updateDate(NOW)
        .build();

    when(tableManager.listPdbTables()).thenReturn(List.of(TABLE_NAME));
    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(itemTableManager.getItemTableName(TABLE_NAME)).thenReturn(ITEM_TABLE_NAME);
    when(itemDao.scan(eq(ITEM_TABLE_NAME), org.mockito.ArgumentMatchers.anyInt()))
        .thenReturn(List.of(expiredItem, activeItem));
    when(attributeValueConverter.fromJson(expiredJson)).thenReturn(expiredAttributes);
    when(attributeValueConverter.fromJson(activeJson)).thenReturn(activeAttributes);

    // Execute
    service.runCleanup();

    // Verify: expired item deleted, active item not deleted
    verify(itemDao).delete(ITEM_TABLE_NAME, "expired-item", Optional.empty());
    verify(itemDao, org.mockito.Mockito.never()).delete(ITEM_TABLE_NAME, "active-item", Optional.empty());
  }

  @Test
  void runCleanup_deletesExpiredItemsFromGsiTables() {
    // Setup: table with TTL and GSI
    final PdbGlobalSecondaryIndex gsi = ImmutablePdbGlobalSecondaryIndex.builder()
        .indexName("StatusIndex")
        .hashKey("status")
        .projectionType("ALL")
        .build();

    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name(TABLE_NAME)
        .hashKey("id")
        .ttlEnabled(true)
        .ttlAttributeName(TTL_ATTRIBUTE_NAME)
        .globalSecondaryIndexes(List.of(gsi))
        .createDate(NOW)
        .build();

    // Create expired item with GSI keys
    final long expiredTimestamp = NOW.getEpochSecond() - 3600;
    final Map<String, AttributeValue> expiredAttributes = new HashMap<>();
    expiredAttributes.put("id", AttributeValue.builder().s("expired-item").build());
    expiredAttributes.put("status", AttributeValue.builder().s("ACTIVE").build());
    expiredAttributes.put(TTL_ATTRIBUTE_NAME, AttributeValue.builder().n(String.valueOf(expiredTimestamp)).build());

    final String expiredJson = "{\"id\":{\"S\":\"expired-item\"},\"status\":{\"S\":\"ACTIVE\"},\"" + TTL_ATTRIBUTE_NAME + "\":{\"N\":\"" + expiredTimestamp + "\"}}";

    final PdbItem expiredItem = ImmutablePdbItem.builder()
        .tableName(ITEM_TABLE_NAME)
        .hashKeyValue("expired-item")
        .sortKeyValue(Optional.empty())
        .attributesJson(expiredJson)
        .createDate(NOW.minusSeconds(7200))
        .updateDate(NOW.minusSeconds(7200))
        .build();

    when(tableManager.listPdbTables()).thenReturn(List.of(TABLE_NAME));
    when(tableManager.getPdbTable(TABLE_NAME)).thenReturn(Optional.of(metadata));
    when(itemTableManager.getItemTableName(TABLE_NAME)).thenReturn(ITEM_TABLE_NAME);
    when(itemTableManager.getGsiTableName(TABLE_NAME, "StatusIndex")).thenReturn("pdb_item_testtable_gsi_statusindex");
    when(itemDao.scan(eq(ITEM_TABLE_NAME), org.mockito.ArgumentMatchers.anyInt()))
        .thenReturn(List.of(expiredItem));
    when(attributeValueConverter.fromJson(expiredJson)).thenReturn(expiredAttributes);
    when(attributeValueConverter.extractKeyValue(expiredAttributes, "status")).thenReturn("ACTIVE");

    // Execute
    service.runCleanup();

    // Verify: deleted from both main and GSI tables
    verify(itemDao).delete(ITEM_TABLE_NAME, "expired-item", Optional.empty());
    verify(itemDao).delete(eq("pdb_item_testtable_gsi_statusindex"), eq("ACTIVE"), any());
  }
}
