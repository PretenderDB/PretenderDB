package io.github.pretenderdb.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.pretenderdb.model.ImmutablePdbItem;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbItem;
import io.github.pretenderdb.model.PdbMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class ItemConverterTest {

  private ItemConverter itemConverter;
  private AttributeValueConverter attributeValueConverter;
  private Clock clock;
  private Instant fixedInstant;

  @BeforeEach
  void setup() {
    fixedInstant = Instant.parse("2024-01-01T00:00:00Z");
    clock = Clock.fixed(fixedInstant, ZoneOffset.UTC);
    attributeValueConverter = new AttributeValueConverter(new ObjectMapper());
    itemConverter = new ItemConverter(attributeValueConverter, clock);
  }

  @Test
  void toPdbItem_hashKeyOnly() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("test-table")
        .hashKey("id")
        .createDate(fixedInstant)
        .build();

    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("item123").build(),
        "name", AttributeValue.builder().s("Test Item").build(),
        "count", AttributeValue.builder().n("42").build()
    );

    final PdbItem pdbItem = itemConverter.toPdbItem("test-table", item, metadata);

    assertThat(pdbItem.tableName()).isEqualTo("test-table");
    assertThat(pdbItem.hashKeyValue()).isEqualTo("item123");
    assertThat(pdbItem.sortKeyValue()).isEmpty();
    assertThat(pdbItem.attributesJson()).isNotBlank();
    assertThat(pdbItem.createDate()).isEqualTo(fixedInstant);
    assertThat(pdbItem.updateDate()).isEqualTo(fixedInstant);

    // Verify we can round-trip
    final Map<String, AttributeValue> roundTrip = itemConverter.toItemAttributes(pdbItem);
    assertThat(roundTrip).containsEntry("id", AttributeValue.builder().s("item123").build());
    assertThat(roundTrip).containsEntry("name", AttributeValue.builder().s("Test Item").build());
    assertThat(roundTrip).containsEntry("count", AttributeValue.builder().n("42").build());
  }

  @Test
  void toPdbItem_withSortKey() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("test-table")
        .hashKey("userId")
        .sortKey("timestamp")
        .createDate(fixedInstant)
        .build();

    final Map<String, AttributeValue> item = Map.of(
        "userId", AttributeValue.builder().s("user123").build(),
        "timestamp", AttributeValue.builder().n("1234567890").build(),
        "action", AttributeValue.builder().s("login").build()
    );

    final PdbItem pdbItem = itemConverter.toPdbItem("test-table", item, metadata);

    assertThat(pdbItem.tableName()).isEqualTo("test-table");
    assertThat(pdbItem.hashKeyValue()).isEqualTo("user123");
    assertThat(pdbItem.sortKeyValue()).contains("1234567890");
    assertThat(pdbItem.attributesJson()).isNotBlank();

    // Verify round-trip
    final Map<String, AttributeValue> roundTrip = itemConverter.toItemAttributes(pdbItem);
    assertThat(roundTrip).containsEntry("userId", AttributeValue.builder().s("user123").build());
    assertThat(roundTrip).containsEntry("timestamp", AttributeValue.builder().n("1234567890").build());
    assertThat(roundTrip).containsEntry("action", AttributeValue.builder().s("login").build());
  }

  @Test
  void toPdbItem_missingHashKey() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("test-table")
        .hashKey("id")
        .createDate(fixedInstant)
        .build();

    final Map<String, AttributeValue> item = Map.of(
        "name", AttributeValue.builder().s("Test Item").build()
    );

    assertThatThrownBy(() -> itemConverter.toPdbItem("test-table", item, metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("id");
  }

  @Test
  void toPdbItem_missingSortKey() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("test-table")
        .hashKey("userId")
        .sortKey("timestamp")
        .createDate(fixedInstant)
        .build();

    final Map<String, AttributeValue> item = Map.of(
        "userId", AttributeValue.builder().s("user123").build(),
        "action", AttributeValue.builder().s("login").build()
    );

    assertThatThrownBy(() -> itemConverter.toPdbItem("test-table", item, metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("timestamp");
  }

  @Test
  void updatePdbItem_success() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("test-table")
        .hashKey("id")
        .createDate(fixedInstant)
        .build();

    final PdbItem existingItem = ImmutablePdbItem.builder()
        .tableName("test-table")
        .hashKeyValue("item123")
        .attributesJson("{\"id\":{\"S\":\"item123\"},\"count\":{\"N\":\"10\"}}")
        .createDate(fixedInstant.minusSeconds(3600))
        .updateDate(fixedInstant.minusSeconds(3600))
        .build();

    final Map<String, AttributeValue> newItem = Map.of(
        "id", AttributeValue.builder().s("item123").build(),
        "count", AttributeValue.builder().n("20").build()
    );

    final PdbItem updated = itemConverter.updatePdbItem(existingItem, newItem, metadata);

    assertThat(updated.tableName()).isEqualTo("test-table");
    assertThat(updated.hashKeyValue()).isEqualTo("item123");
    assertThat(updated.createDate()).isEqualTo(fixedInstant.minusSeconds(3600));
    assertThat(updated.updateDate()).isEqualTo(fixedInstant);

    // Verify new attributes are stored
    final Map<String, AttributeValue> attributes = itemConverter.toItemAttributes(updated);
    assertThat(attributes.get("count").n()).isEqualTo("20");
  }

  @Test
  void updatePdbItem_cannotChangeHashKey() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("test-table")
        .hashKey("id")
        .createDate(fixedInstant)
        .build();

    final PdbItem existingItem = ImmutablePdbItem.builder()
        .tableName("test-table")
        .hashKeyValue("item123")
        .attributesJson("{\"id\":{\"S\":\"item123\"}}")
        .createDate(fixedInstant)
        .updateDate(fixedInstant)
        .build();

    final Map<String, AttributeValue> newItem = Map.of(
        "id", AttributeValue.builder().s("different-id").build()
    );

    assertThatThrownBy(() -> itemConverter.updatePdbItem(existingItem, newItem, metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("hash key");
  }

  @Test
  void updatePdbItem_cannotChangeSortKey() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("test-table")
        .hashKey("userId")
        .sortKey("timestamp")
        .createDate(fixedInstant)
        .build();

    final PdbItem existingItem = ImmutablePdbItem.builder()
        .tableName("test-table")
        .hashKeyValue("user123")
        .sortKeyValue("1000")
        .attributesJson("{\"userId\":{\"S\":\"user123\"},\"timestamp\":{\"N\":\"1000\"}}")
        .createDate(fixedInstant)
        .updateDate(fixedInstant)
        .build();

    final Map<String, AttributeValue> newItem = Map.of(
        "userId", AttributeValue.builder().s("user123").build(),
        "timestamp", AttributeValue.builder().n("2000").build()
    );

    assertThatThrownBy(() -> itemConverter.updatePdbItem(existingItem, newItem, metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sort key");
  }

  @Test
  void applyProjection_simple() {
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("item123").build(),
        "name", AttributeValue.builder().s("Test").build(),
        "count", AttributeValue.builder().n("42").build(),
        "active", AttributeValue.builder().bool(true).build()
    );

    final Map<String, AttributeValue> projected = itemConverter.applyProjection(item, "id,name");

    assertThat(projected).hasSize(2);
    assertThat(projected).containsKey("id");
    assertThat(projected).containsKey("name");
    assertThat(projected).doesNotContainKey("count");
    assertThat(projected).doesNotContainKey("active");
  }

  @Test
  void applyProjection_withSpaces() {
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("item123").build(),
        "name", AttributeValue.builder().s("Test").build(),
        "count", AttributeValue.builder().n("42").build()
    );

    final Map<String, AttributeValue> projected = itemConverter.applyProjection(item, " id , count ");

    assertThat(projected).hasSize(2);
    assertThat(projected).containsKey("id");
    assertThat(projected).containsKey("count");
    assertThat(projected).doesNotContainKey("name");
  }

  @Test
  void applyProjection_nonExistentAttribute() {
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("item123").build(),
        "name", AttributeValue.builder().s("Test").build()
    );

    final Map<String, AttributeValue> projected = itemConverter.applyProjection(item, "id,notFound");

    assertThat(projected).hasSize(1);
    assertThat(projected).containsKey("id");
  }

  @Test
  void applyProjection_null() {
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("item123").build()
    );

    final Map<String, AttributeValue> projected = itemConverter.applyProjection(item, null);

    assertThat(projected).isEqualTo(item);
  }

  @Test
  void applyProjection_empty() {
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("item123").build()
    );

    final Map<String, AttributeValue> projected = itemConverter.applyProjection(item, "");

    assertThat(projected).isEqualTo(item);
  }

  @Test
  void toItemAttributes_roundTrip() {
    final PdbMetadata metadata = ImmutablePdbMetadata.builder()
        .name("test-table")
        .hashKey("id")
        .createDate(fixedInstant)
        .build();

    final Map<String, AttributeValue> original = Map.of(
        "id", AttributeValue.builder().s("item123").build(),
        "data", AttributeValue.builder().m(Map.of(
            "nested", AttributeValue.builder().s("value").build()
        )).build(),
        "tags", AttributeValue.builder().ss("tag1", "tag2").build()
    );

    final PdbItem pdbItem = itemConverter.toPdbItem("test-table", original, metadata);
    final Map<String, AttributeValue> roundTrip = itemConverter.toItemAttributes(pdbItem);

    assertThat(roundTrip).containsEntry("id", original.get("id"));
    assertThat(roundTrip.get("data").m().get("nested").s()).isEqualTo("value");
    assertThat(roundTrip.get("tags").ss()).containsExactlyInAnyOrder("tag1", "tag2");
  }
}
