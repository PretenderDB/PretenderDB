package io.github.pretenderdb.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class AttributeValueConverterTest {

  private AttributeValueConverter converter;

  @BeforeEach
  void setup() {
    converter = new AttributeValueConverter(new ObjectMapper());
  }

  @Test
  void toJson_fromJson_string() {
    final Map<String, AttributeValue> attributes = Map.of(
        "name", AttributeValue.builder().s("John Doe").build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result).containsEntry("name", AttributeValue.builder().s("John Doe").build());
  }

  @Test
  void toJson_fromJson_number() {
    final Map<String, AttributeValue> attributes = Map.of(
        "age", AttributeValue.builder().n("42").build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result).containsEntry("age", AttributeValue.builder().n("42").build());
  }

  @Test
  void toJson_fromJson_binary() {
    final byte[] bytes = new byte[]{1, 2, 3, 4, 5};
    final Map<String, AttributeValue> attributes = Map.of(
        "data", AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result.get("data").b().asByteArray()).isEqualTo(bytes);
  }

  @Test
  void toJson_fromJson_boolean() {
    final Map<String, AttributeValue> attributes = Map.of(
        "active", AttributeValue.builder().bool(true).build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result).containsEntry("active", AttributeValue.builder().bool(true).build());
  }

  @Test
  void toJson_fromJson_null() {
    final Map<String, AttributeValue> attributes = Map.of(
        "deleted", AttributeValue.builder().nul(true).build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result).containsEntry("deleted", AttributeValue.builder().nul(true).build());
  }

  @Test
  void toJson_fromJson_stringSet() {
    final Map<String, AttributeValue> attributes = Map.of(
        "tags", AttributeValue.builder().ss("tag1", "tag2", "tag3").build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result.get("tags").ss()).containsExactlyInAnyOrder("tag1", "tag2", "tag3");
  }

  @Test
  void toJson_fromJson_numberSet() {
    final Map<String, AttributeValue> attributes = Map.of(
        "scores", AttributeValue.builder().ns("100", "200", "300").build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result.get("scores").ns()).containsExactlyInAnyOrder("100", "200", "300");
  }

  @Test
  void toJson_fromJson_binarySet() {
    final byte[] bytes1 = new byte[]{1, 2, 3};
    final byte[] bytes2 = new byte[]{4, 5, 6};
    final Map<String, AttributeValue> attributes = Map.of(
        "binaries", AttributeValue.builder().bs(
            SdkBytes.fromByteArray(bytes1),
            SdkBytes.fromByteArray(bytes2)
        ).build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result.get("binaries").bs()).hasSize(2);
    assertThat(result.get("binaries").bs().get(0).asByteArray()).isEqualTo(bytes1);
    assertThat(result.get("binaries").bs().get(1).asByteArray()).isEqualTo(bytes2);
  }

  @Test
  void toJson_fromJson_list() {
    final Map<String, AttributeValue> attributes = Map.of(
        "items", AttributeValue.builder().l(
            AttributeValue.builder().s("first").build(),
            AttributeValue.builder().n("123").build(),
            AttributeValue.builder().bool(true).build()
        ).build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result.get("items").l()).containsExactly(
        AttributeValue.builder().s("first").build(),
        AttributeValue.builder().n("123").build(),
        AttributeValue.builder().bool(true).build()
    );
  }

  @Test
  void toJson_fromJson_map() {
    final Map<String, AttributeValue> attributes = Map.of(
        "address", AttributeValue.builder().m(Map.of(
            "street", AttributeValue.builder().s("123 Main St").build(),
            "city", AttributeValue.builder().s("Anytown").build(),
            "zip", AttributeValue.builder().n("12345").build()
        )).build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result.get("address").m()).containsEntry("street", AttributeValue.builder().s("123 Main St").build());
    assertThat(result.get("address").m()).containsEntry("city", AttributeValue.builder().s("Anytown").build());
    assertThat(result.get("address").m()).containsEntry("zip", AttributeValue.builder().n("12345").build());
  }

  @Test
  void toJson_fromJson_nestedStructures() {
    final Map<String, AttributeValue> attributes = Map.of(
        "user", AttributeValue.builder().m(Map.of(
            "id", AttributeValue.builder().s("user123").build(),
            "metadata", AttributeValue.builder().m(Map.of(
                "created", AttributeValue.builder().n("1234567890").build(),
                "tags", AttributeValue.builder().ss("premium", "verified").build()
            )).build(),
            "scores", AttributeValue.builder().l(
                AttributeValue.builder().n("100").build(),
                AttributeValue.builder().n("200").build()
            ).build()
        )).build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    // Verify nested map
    assertThat(result.get("user").m().get("id").s()).isEqualTo("user123");
    assertThat(result.get("user").m().get("metadata").m().get("created").n()).isEqualTo("1234567890");
    assertThat(result.get("user").m().get("metadata").m().get("tags").ss())
        .containsExactlyInAnyOrder("premium", "verified");
    assertThat(result.get("user").m().get("scores").l()).hasSize(2);
  }

  @Test
  void toJson_fromJson_multipleAttributes() {
    final Map<String, AttributeValue> attributes = Map.of(
        "id", AttributeValue.builder().s("item123").build(),
        "name", AttributeValue.builder().s("Test Item").build(),
        "count", AttributeValue.builder().n("42").build(),
        "active", AttributeValue.builder().bool(true).build()
    );

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result).hasSize(4);
    assertThat(result.get("id").s()).isEqualTo("item123");
    assertThat(result.get("name").s()).isEqualTo("Test Item");
    assertThat(result.get("count").n()).isEqualTo("42");
    assertThat(result.get("active").bool()).isTrue();
  }

  @Test
  void extractKeyValue_string() {
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("test-id").build()
    );

    final String value = converter.extractKeyValue(item, "id");

    assertThat(value).isEqualTo("test-id");
  }

  @Test
  void extractKeyValue_number() {
    final Map<String, AttributeValue> item = Map.of(
        "timestamp", AttributeValue.builder().n("1234567890").build()
    );

    final String value = converter.extractKeyValue(item, "timestamp");

    assertThat(value).isEqualTo("1234567890");
  }

  @Test
  void extractKeyValue_binary() {
    final byte[] bytes = new byte[]{1, 2, 3, 4, 5};
    final Map<String, AttributeValue> item = Map.of(
        "binaryKey", AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build()
    );

    final String value = converter.extractKeyValue(item, "binaryKey");

    assertThat(value).isNotNull();
  }

  @Test
  void extractKeyValue_missingKey() {
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("test-id").build()
    );

    assertThatThrownBy(() -> converter.extractKeyValue(item, "notFound"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not found");
  }

  @Test
  void extractKeyValue_nonScalarType() {
    final Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().m(Map.of("nested", AttributeValue.builder().s("value").build())).build()
    );

    assertThatThrownBy(() -> converter.extractKeyValue(item, "id"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a scalar type");
  }

  @Test
  void toJson_fromJson_emptyMap() {
    final Map<String, AttributeValue> attributes = Map.of();

    final String json = converter.toJson(attributes);
    final Map<String, AttributeValue> result = converter.fromJson(json);

    assertThat(result).isEmpty();
  }
}
