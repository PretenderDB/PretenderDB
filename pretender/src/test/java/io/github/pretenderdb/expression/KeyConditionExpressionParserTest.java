package io.github.pretenderdb.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.pretenderdb.expression.KeyConditionExpressionParser.ParsedKeyCondition;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class KeyConditionExpressionParserTest {

  private KeyConditionExpressionParser parser;

  @BeforeEach
  void setup() {
    parser = new KeyConditionExpressionParser();
  }

  @Test
  void parse_hashKeyOnly() {
    final String expression = "userId = :uid";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isNull();
    assertThat(result.sortKeyValue()).isEmpty();
  }

  @Test
  void parse_hashKeyWithNumberValue() {
    final String expression = "id = :id";
    final Map<String, AttributeValue> values = Map.of(
        ":id", AttributeValue.builder().n("12345").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.hashKeyValue()).isEqualTo("12345");
  }

  @Test
  void parse_hashAndSortKeyEquals() {
    final String expression = "userId = :uid AND timestamp = :ts";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":ts", AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value = :sortKey");
    assertThat(result.sortKeyValue()).contains("2024-01-01T00:00:00Z");
  }

  @Test
  void parse_sortKeyLessThan() {
    final String expression = "userId = :uid AND timestamp < :ts";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":ts", AttributeValue.builder().s("2024-01-01").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value < :sortKey");
    assertThat(result.sortKeyValue()).contains("2024-01-01");
  }

  @Test
  void parse_sortKeyGreaterThan() {
    final String expression = "userId = :uid AND timestamp > :ts";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":ts", AttributeValue.builder().s("2024-01-01").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value > :sortKey");
    assertThat(result.sortKeyValue()).contains("2024-01-01");
  }

  @Test
  void parse_sortKeyLessThanOrEqual() {
    final String expression = "userId = :uid AND timestamp <= :ts";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":ts", AttributeValue.builder().n("1000").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value <= :sortKey");
    assertThat(result.sortKeyValue()).contains("1000");
  }

  @Test
  void parse_sortKeyGreaterThanOrEqual() {
    final String expression = "userId = :uid AND timestamp >= :ts";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":ts", AttributeValue.builder().n("500").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value >= :sortKey");
    assertThat(result.sortKeyValue()).contains("500");
  }

  @Test
  void parse_sortKeyBetween() {
    final String expression = "userId = :uid AND timestamp BETWEEN :start AND :end";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":start", AttributeValue.builder().s("2024-01-01").build(),
        ":end", AttributeValue.builder().s("2024-12-31").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value BETWEEN '2024-01-01' AND '2024-12-31'");
    assertThat(result.sortKeyValue()).isEmpty();
  }

  @Test
  void parse_sortKeyBeginsWith() {
    final String expression = "userId = :uid AND begins_with(timestamp, :prefix)";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":prefix", AttributeValue.builder().s("2024-01").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value LIKE :sortKey");
    assertThat(result.sortKeyValue()).contains("2024-01%");
  }

  @Test
  void parse_caseInsensitive() {
    final String expression = "userId = :uid and timestamp = :ts";  // lowercase 'and'
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":ts", AttributeValue.builder().s("2024-01-01").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value = :sortKey");
  }

  @Test
  void parse_nullExpression() {
    assertThatThrownBy(() -> parser.parse(null, Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null");
  }

  @Test
  void parse_emptyExpression() {
    assertThatThrownBy(() -> parser.parse("", Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null or empty");
  }

  @Test
  void parse_missingHashKey() {
    final String expression = "AND timestamp = :ts";  // AND clause without hash key condition
    final Map<String, AttributeValue> values = Map.of(
        ":ts", AttributeValue.builder().s("2024-01-01").build()
    );

    assertThatThrownBy(() -> parser.parse(expression, values))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing hash key");
  }

  @Test
  void parse_missingPlaceholderValue() {
    final String expression = "userId = :uid";
    final Map<String, AttributeValue> values = Map.of();  // No value for :uid

    assertThatThrownBy(() -> parser.parse(expression, values))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing value for placeholder");
  }

  @Test
  void parse_nonScalarKeyType() {
    final String expression = "userId = :uid";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().m(Map.of("nested", AttributeValue.builder().s("value").build())).build()
    );

    assertThatThrownBy(() -> parser.parse(expression, values))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a scalar type");
  }

  @Test
  void parse_extraWhitespace() {
    final String expression = "  userId   =   :uid   AND   timestamp   =   :ts  ";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":ts", AttributeValue.builder().s("2024-01-01").build()
    );

    final ParsedKeyCondition result = parser.parse(expression, values);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isNotNull();
  }

  // Tests for Expression Attribute Names (#placeholder)

  @Test
  void parse_hashKeyWithExpressionAttributeName() {
    final String expression = "#pk = :uid";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build()
    );
    final Map<String, String> names = Map.of(
        "#pk", "userId"
    );

    final ParsedKeyCondition result = parser.parse(expression, values, names);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isNull();
    assertThat(result.sortKeyValue()).isEmpty();
  }

  @Test
  void parse_hashAndSortKeyWithExpressionAttributeNames() {
    final String expression = "#pk = :uid AND #sk = :ts";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":ts", AttributeValue.builder().s("2024-01-01T00:00:00Z").build()
    );
    final Map<String, String> names = Map.of(
        "#pk", "userId",
        "#sk", "timestamp"
    );

    final ParsedKeyCondition result = parser.parse(expression, values, names);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value = :sortKey");
    assertThat(result.sortKeyValue()).contains("2024-01-01T00:00:00Z");
  }

  @Test
  void parse_sortKeyLessThanWithExpressionAttributeName() {
    final String expression = "userId = :uid AND #ts < :tsval";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":tsval", AttributeValue.builder().s("2024-01-01").build()
    );
    final Map<String, String> names = Map.of(
        "#ts", "timestamp"
    );

    final ParsedKeyCondition result = parser.parse(expression, values, names);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value < :sortKey");
    assertThat(result.sortKeyValue()).contains("2024-01-01");
  }

  @Test
  void parse_sortKeyBetweenWithExpressionAttributeName() {
    final String expression = "#pk = :uid AND #sk BETWEEN :start AND :end";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":start", AttributeValue.builder().s("2024-01-01").build(),
        ":end", AttributeValue.builder().s("2024-12-31").build()
    );
    final Map<String, String> names = Map.of(
        "#pk", "userId",
        "#sk", "timestamp"
    );

    final ParsedKeyCondition result = parser.parse(expression, values, names);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value BETWEEN '2024-01-01' AND '2024-12-31'");
    assertThat(result.sortKeyValue()).isEmpty();
  }

  @Test
  void parse_sortKeyBeginsWithExpressionAttributeName() {
    final String expression = "#pk = :uid AND begins_with(#sk, :prefix)";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":prefix", AttributeValue.builder().s("2024-01").build()
    );
    final Map<String, String> names = Map.of(
        "#pk", "userId",
        "#sk", "timestamp"
    );

    final ParsedKeyCondition result = parser.parse(expression, values, names);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value LIKE :sortKey");
    assertThat(result.sortKeyValue()).contains("2024-01%");
  }

  @Test
  void parse_mixedAttributeNamesAndPlaceholders() {
    // Direct attribute name for hash key, expression attribute name for sort key
    final String expression = "userId = :uid AND #sk >= :ts";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build(),
        ":ts", AttributeValue.builder().n("1000").build()
    );
    final Map<String, String> names = Map.of(
        "#sk", "timestamp"
    );

    final ParsedKeyCondition result = parser.parse(expression, values, names);

    assertThat(result.hashKeyValue()).isEqualTo("user-123");
    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value >= :sortKey");
    assertThat(result.sortKeyValue()).contains("1000");
  }

  @Test
  void parse_expressionAttributeNameNotProvided() {
    final String expression = "#pk = :uid";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build()
    );

    // No expressionAttributeNames provided (null)
    assertThatThrownBy(() -> parser.parse(expression, values, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expressionAttributeNames not provided");
  }

  @Test
  void parse_expressionAttributeNameNotFound() {
    final String expression = "#pk = :uid";
    final Map<String, AttributeValue> values = Map.of(
        ":uid", AttributeValue.builder().s("user-123").build()
    );
    final Map<String, String> names = Map.of(
        "#other", "someOtherAttribute"
    );

    // #pk not in expressionAttributeNames
    assertThatThrownBy(() -> parser.parse(expression, values, names))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expression attribute name not found: #pk");
  }

  @Test
  void parse_reservedWordWithExpressionAttributeName() {
    // Test using a DynamoDB reserved word like 'status' via expression attribute name
    final String expression = "#status = :statusVal AND #date > :dateVal";
    final Map<String, AttributeValue> values = Map.of(
        ":statusVal", AttributeValue.builder().s("ACTIVE").build(),
        ":dateVal", AttributeValue.builder().s("2024-01-01").build()
    );
    final Map<String, String> names = Map.of(
        "#status", "status",
        "#date", "date"
    );

    final ParsedKeyCondition result = parser.parse(expression, values, names);

    assertThat(result.hashKeyValue()).isEqualTo("ACTIVE");
    assertThat(result.sortKeyCondition()).isEqualTo("sort_key_value > :sortKey");
    assertThat(result.sortKeyValue()).contains("2024-01-01");
  }
}
