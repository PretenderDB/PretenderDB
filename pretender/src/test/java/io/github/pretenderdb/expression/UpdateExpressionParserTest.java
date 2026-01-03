package io.github.pretenderdb.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class UpdateExpressionParserTest {

  private UpdateExpressionParser parser;

  @BeforeEach
  void setup() {
    parser = new UpdateExpressionParser();
  }

  @Test
  void applyUpdate_setSingleAttribute() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());

    final String expression = "SET #n = :name";
    final Map<String, AttributeValue> values = Map.of(
        ":name", AttributeValue.builder().s("John Doe").build()
    );
    final Map<String, String> names = Map.of("#n", "name");

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, names);

    assertThat(result).hasSize(2);
    assertThat(result.get("id").s()).isEqualTo("123");
    assertThat(result.get("name").s()).isEqualTo("John Doe");
  }

  @Test
  void applyUpdate_setMultipleAttributes() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());

    final String expression = "SET name = :name, age = :age";
    final Map<String, AttributeValue> values = Map.of(
        ":name", AttributeValue.builder().s("John").build(),
        ":age", AttributeValue.builder().n("30").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result).hasSize(3);
    assertThat(result.get("name").s()).isEqualTo("John");
    assertThat(result.get("age").n()).isEqualTo("30");
  }

  @Test
  void applyUpdate_setNumericAddition() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("count", AttributeValue.builder().n("5").build());

    final String expression = "SET count = count + :inc";
    final Map<String, AttributeValue> values = Map.of(
        ":inc", AttributeValue.builder().n("3").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("count").n()).isEqualTo("8");
  }

  @Test
  void applyUpdate_listAppend() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("tags", AttributeValue.builder().l(
        AttributeValue.builder().s("tag1").build(),
        AttributeValue.builder().s("tag2").build()
    ).build());

    final String expression = "SET tags = list_append(tags, :newTags)";
    final Map<String, AttributeValue> values = Map.of(
        ":newTags", AttributeValue.builder().l(
            AttributeValue.builder().s("tag3").build()
        ).build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("tags").l()).hasSize(3);
    assertThat(result.get("tags").l().get(0).s()).isEqualTo("tag1");
    assertThat(result.get("tags").l().get(1).s()).isEqualTo("tag2");
    assertThat(result.get("tags").l().get(2).s()).isEqualTo("tag3");
  }

  @Test
  void applyUpdate_listAppendToNonExistent() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());

    final String expression = "SET tags = list_append(tags, :newTags)";
    final Map<String, AttributeValue> values = Map.of(
        ":newTags", AttributeValue.builder().l(
            AttributeValue.builder().s("tag1").build()
        ).build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("tags").l()).hasSize(1);
    assertThat(result.get("tags").l().get(0).s()).isEqualTo("tag1");
  }

  @Test
  void applyUpdate_ifNotExists_attributeDoesNotExist() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());

    final String expression = "SET count = if_not_exists(count, :default)";
    final Map<String, AttributeValue> values = Map.of(
        ":default", AttributeValue.builder().n("0").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("count").n()).isEqualTo("0");
  }

  @Test
  void applyUpdate_ifNotExists_attributeExists() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("count", AttributeValue.builder().n("5").build());

    final String expression = "SET count = if_not_exists(count, :default)";
    final Map<String, AttributeValue> values = Map.of(
        ":default", AttributeValue.builder().n("0").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("count").n()).isEqualTo("5");  // Should keep existing value
  }

  @Test
  void applyUpdate_removeSingleAttribute() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("name", AttributeValue.builder().s("John").build());
    item.put("age", AttributeValue.builder().n("30").build());

    final String expression = "REMOVE name";

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, Map.of(), Map.of());

    assertThat(result).hasSize(2);
    assertThat(result).containsKey("id");
    assertThat(result).containsKey("age");
    assertThat(result).doesNotContainKey("name");
  }

  @Test
  void applyUpdate_removeMultipleAttributes() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("name", AttributeValue.builder().s("John").build());
    item.put("age", AttributeValue.builder().n("30").build());
    item.put("email", AttributeValue.builder().s("john@example.com").build());

    final String expression = "REMOVE name, age";

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, Map.of(), Map.of());

    assertThat(result).hasSize(2);
    assertThat(result).containsKey("id");
    assertThat(result).containsKey("email");
  }

  @Test
  void applyUpdate_removeWithExpressionAttributeName() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("status", AttributeValue.builder().s("active").build());

    final String expression = "REMOVE #s";
    final Map<String, String> names = Map.of("#s", "status");

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, Map.of(), names);

    assertThat(result).hasSize(1);
    assertThat(result).doesNotContainKey("status");
  }

  @Test
  void applyUpdate_addToNumber() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("count", AttributeValue.builder().n("5").build());

    final String expression = "ADD count :inc";
    final Map<String, AttributeValue> values = Map.of(
        ":inc", AttributeValue.builder().n("3").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("count").n()).isEqualTo("8");
  }

  @Test
  void applyUpdate_addToNonExistentNumber() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());

    final String expression = "ADD count :inc";
    final Map<String, AttributeValue> values = Map.of(
        ":inc", AttributeValue.builder().n("5").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("count").n()).isEqualTo("5");
  }

  @Test
  void applyUpdate_addToStringSet() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("tags", AttributeValue.builder().ss("tag1", "tag2").build());

    final String expression = "ADD tags :newTags";
    final Map<String, AttributeValue> values = Map.of(
        ":newTags", AttributeValue.builder().ss("tag3", "tag4").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("tags").ss()).containsExactlyInAnyOrder("tag1", "tag2", "tag3", "tag4");
  }

  @Test
  void applyUpdate_addToNonExistentSet() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());

    final String expression = "ADD tags :newTags";
    final Map<String, AttributeValue> values = Map.of(
        ":newTags", AttributeValue.builder().ss("tag1", "tag2").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("tags").ss()).containsExactlyInAnyOrder("tag1", "tag2");
  }

  @Test
  void applyUpdate_deleteFromStringSet() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("tags", AttributeValue.builder().ss("tag1", "tag2", "tag3").build());

    final String expression = "DELETE tags :removeTags";
    final Map<String, AttributeValue> values = Map.of(
        ":removeTags", AttributeValue.builder().ss("tag2").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("tags").ss()).containsExactlyInAnyOrder("tag1", "tag3");
  }

  @Test
  void applyUpdate_deleteAllFromSet() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("tags", AttributeValue.builder().ss("tag1", "tag2").build());

    final String expression = "DELETE tags :removeTags";
    final Map<String, AttributeValue> values = Map.of(
        ":removeTags", AttributeValue.builder().ss("tag1", "tag2").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result).doesNotContainKey("tags");  // Set removed when empty
  }

  @Test
  void applyUpdate_combinedOperations() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());
    item.put("name", AttributeValue.builder().s("John").build());
    item.put("count", AttributeValue.builder().n("5").build());
    item.put("tags", AttributeValue.builder().ss("tag1").build());

    final String expression = "SET #n = :newName, count = count + :inc ADD tags :newTags REMOVE name";
    final Map<String, AttributeValue> values = Map.of(
        ":newName", AttributeValue.builder().s("Jane").build(),
        ":inc", AttributeValue.builder().n("3").build(),
        ":newTags", AttributeValue.builder().ss("tag2").build()
    );
    final Map<String, String> names = Map.of("#n", "fullName");

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, names);

    assertThat(result.get("fullName").s()).isEqualTo("Jane");
    assertThat(result.get("count").n()).isEqualTo("8");
    assertThat(result.get("tags").ss()).containsExactlyInAnyOrder("tag1", "tag2");
    assertThat(result).doesNotContainKey("name");
  }

  @Test
  void applyUpdate_nullExpression() {
    final Map<String, AttributeValue> item = new HashMap<>();

    assertThatThrownBy(() -> parser.applyUpdate(item, null, Map.of(), Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }

  @Test
  void applyUpdate_emptyExpression() {
    final Map<String, AttributeValue> item = new HashMap<>();

    assertThatThrownBy(() -> parser.applyUpdate(item, "", Map.of(), Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }

  @Test
  void applyUpdate_missingExpressionAttributeValue() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());

    final String expression = "SET name = :name";

    assertThatThrownBy(() -> parser.applyUpdate(item, expression, Map.of(), Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not found: :name");
  }

  @Test
  void applyUpdate_missingExpressionAttributeName() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());

    final String expression = "SET #n = :name";
    final Map<String, AttributeValue> values = Map.of(
        ":name", AttributeValue.builder().s("John").build()
    );

    assertThatThrownBy(() -> parser.applyUpdate(item, expression, values, Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not found: #n");
  }

  @Test
  void applyUpdate_caseInsensitive() {
    final Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", AttributeValue.builder().s("123").build());

    final String expression = "set name = :name remove age add count :inc";
    final Map<String, AttributeValue> values = Map.of(
        ":name", AttributeValue.builder().s("John").build(),
        ":inc", AttributeValue.builder().n("1").build()
    );

    final Map<String, AttributeValue> result = parser.applyUpdate(item, expression, values, Map.of());

    assertThat(result.get("name").s()).isEqualTo("John");
    assertThat(result.get("count").n()).isEqualTo("1");
  }
}
