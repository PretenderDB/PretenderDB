package io.github.pretenderdb.expression;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class ConditionExpressionParserTest {

  private ConditionExpressionParser parser;

  @BeforeEach
  void setup() {
    parser = new ConditionExpressionParser();
  }

  // Tests for attribute_exists function

  @Test
  void evaluate_attributeExists_returnsTrueWhenAttributeExists() {
    final Map<String, AttributeValue> item = Map.of(
        "name", AttributeValue.builder().s("John").build()
    );

    final boolean result = parser.evaluate(item, "attribute_exists(name)", Map.of(), null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_attributeExists_returnsFalseWhenAttributeDoesNotExist() {
    final Map<String, AttributeValue> item = Map.of(
        "name", AttributeValue.builder().s("John").build()
    );

    final boolean result = parser.evaluate(item, "attribute_exists(email)", Map.of(), null);

    assertThat(result).isFalse();
  }

  @Test
  void evaluate_attributeExists_returnsFalseWhenItemIsNull() {
    final boolean result = parser.evaluate(null, "attribute_exists(name)", Map.of(), null);

    assertThat(result).isFalse();
  }

  // Tests for attribute_not_exists function

  @Test
  void evaluate_attributeNotExists_returnsTrueWhenAttributeDoesNotExist() {
    final Map<String, AttributeValue> item = Map.of(
        "name", AttributeValue.builder().s("John").build()
    );

    final boolean result = parser.evaluate(item, "attribute_not_exists(email)", Map.of(), null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_attributeNotExists_returnsFalseWhenAttributeExists() {
    final Map<String, AttributeValue> item = Map.of(
        "name", AttributeValue.builder().s("John").build()
    );

    final boolean result = parser.evaluate(item, "attribute_not_exists(name)", Map.of(), null);

    assertThat(result).isFalse();
  }

  @Test
  void evaluate_attributeNotExists_returnsTrueWhenItemIsNull() {
    final boolean result = parser.evaluate(null, "attribute_not_exists(name)", Map.of(), null);

    assertThat(result).isTrue();
  }

  // Tests for comparison operators

  @Test
  void evaluate_equals_returnsTrueWhenValuesMatch() {
    final Map<String, AttributeValue> item = Map.of(
        "status", AttributeValue.builder().s("ACTIVE").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":status", AttributeValue.builder().s("ACTIVE").build()
    );

    final boolean result = parser.evaluate(item, "status = :status", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_equals_returnsFalseWhenValuesDontMatch() {
    final Map<String, AttributeValue> item = Map.of(
        "status", AttributeValue.builder().s("ACTIVE").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":status", AttributeValue.builder().s("INACTIVE").build()
    );

    final boolean result = parser.evaluate(item, "status = :status", values, null);

    assertThat(result).isFalse();
  }

  @Test
  void evaluate_notEquals_returnsTrueWhenValuesDiffer() {
    final Map<String, AttributeValue> item = Map.of(
        "status", AttributeValue.builder().s("ACTIVE").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":status", AttributeValue.builder().s("INACTIVE").build()
    );

    final boolean result = parser.evaluate(item, "status <> :status", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_lessThan_withNumbers() {
    final Map<String, AttributeValue> item = Map.of(
        "age", AttributeValue.builder().n("25").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":maxAge", AttributeValue.builder().n("30").build()
    );

    final boolean result = parser.evaluate(item, "age < :maxAge", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_greaterThan_withNumbers() {
    final Map<String, AttributeValue> item = Map.of(
        "count", AttributeValue.builder().n("50").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":threshold", AttributeValue.builder().n("40").build()
    );

    final boolean result = parser.evaluate(item, "count > :threshold", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_lessThanOrEqual_withNumbers() {
    final Map<String, AttributeValue> item = Map.of(
        "score", AttributeValue.builder().n("100").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":maxScore", AttributeValue.builder().n("100").build()
    );

    final boolean result = parser.evaluate(item, "score <= :maxScore", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_greaterThanOrEqual_withNumbers() {
    final Map<String, AttributeValue> item = Map.of(
        "points", AttributeValue.builder().n("75").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":minPoints", AttributeValue.builder().n("50").build()
    );

    final boolean result = parser.evaluate(item, "points >= :minPoints", values, null);

    assertThat(result).isTrue();
  }

  // Tests for BETWEEN operator

  @Test
  void evaluate_between_returnsTrueWhenValueInRange() {
    final Map<String, AttributeValue> item = Map.of(
        "age", AttributeValue.builder().n("30").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":min", AttributeValue.builder().n("18").build(),
        ":max", AttributeValue.builder().n("65").build()
    );

    final boolean result = parser.evaluate(item, "age BETWEEN :min AND :max", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_between_returnsFalseWhenValueOutOfRange() {
    final Map<String, AttributeValue> item = Map.of(
        "age", AttributeValue.builder().n("70").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":min", AttributeValue.builder().n("18").build(),
        ":max", AttributeValue.builder().n("65").build()
    );

    final boolean result = parser.evaluate(item, "age BETWEEN :min AND :max", values, null);

    assertThat(result).isFalse();
  }

  // Tests for begins_with function

  @Test
  void evaluate_beginsWith_returnsTrueWhenStringStartsWithPrefix() {
    final Map<String, AttributeValue> item = Map.of(
        "email", AttributeValue.builder().s("john@example.com").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":prefix", AttributeValue.builder().s("john").build()
    );

    final boolean result = parser.evaluate(item, "begins_with(email, :prefix)", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_beginsWith_returnsFalseWhenStringDoesNotStartWithPrefix() {
    final Map<String, AttributeValue> item = Map.of(
        "email", AttributeValue.builder().s("jane@example.com").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":prefix", AttributeValue.builder().s("john").build()
    );

    final boolean result = parser.evaluate(item, "begins_with(email, :prefix)", values, null);

    assertThat(result).isFalse();
  }

  // Tests for contains function

  @Test
  void evaluate_contains_returnsTrueWhenStringContainsSubstring() {
    final Map<String, AttributeValue> item = Map.of(
        "description", AttributeValue.builder().s("This is a test description").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":word", AttributeValue.builder().s("test").build()
    );

    final boolean result = parser.evaluate(item, "contains(description, :word)", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_contains_returnsTrueWhenListContainsValue() {
    final Map<String, AttributeValue> item = Map.of(
        "tags", AttributeValue.builder().l(
            AttributeValue.builder().s("tag1").build(),
            AttributeValue.builder().s("tag2").build(),
            AttributeValue.builder().s("tag3").build()
        ).build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":tag", AttributeValue.builder().s("tag2").build()
    );

    final boolean result = parser.evaluate(item, "contains(tags, :tag)", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_contains_returnsTrueWhenSetContainsValue() {
    final Map<String, AttributeValue> item = Map.of(
        "categories", AttributeValue.builder().ss("A", "B", "C").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":category", AttributeValue.builder().s("B").build()
    );

    final boolean result = parser.evaluate(item, "contains(categories, :category)", values, null);

    assertThat(result).isTrue();
  }

  // Tests for logical operators (AND, OR, NOT)

  @Test
  void evaluate_andOperator_returnsTrueWhenBothConditionsTrue() {
    final Map<String, AttributeValue> item = Map.of(
        "status", AttributeValue.builder().s("ACTIVE").build(),
        "age", AttributeValue.builder().n("30").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":status", AttributeValue.builder().s("ACTIVE").build(),
        ":minAge", AttributeValue.builder().n("18").build()
    );

    final boolean result = parser.evaluate(item, "status = :status AND age > :minAge", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_andOperator_returnsFalseWhenOneConditionFalse() {
    final Map<String, AttributeValue> item = Map.of(
        "status", AttributeValue.builder().s("ACTIVE").build(),
        "age", AttributeValue.builder().n("15").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":status", AttributeValue.builder().s("ACTIVE").build(),
        ":minAge", AttributeValue.builder().n("18").build()
    );

    final boolean result = parser.evaluate(item, "status = :status AND age > :minAge", values, null);

    assertThat(result).isFalse();
  }

  @Test
  void evaluate_orOperator_returnsTrueWhenOneConditionTrue() {
    final Map<String, AttributeValue> item = Map.of(
        "status", AttributeValue.builder().s("INACTIVE").build(),
        "age", AttributeValue.builder().n("30").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":status", AttributeValue.builder().s("ACTIVE").build(),
        ":minAge", AttributeValue.builder().n("18").build()
    );

    final boolean result = parser.evaluate(item, "status = :status OR age > :minAge", values, null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_orOperator_returnsFalseWhenBothConditionsFalse() {
    final Map<String, AttributeValue> item = Map.of(
        "status", AttributeValue.builder().s("INACTIVE").build(),
        "age", AttributeValue.builder().n("15").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":status", AttributeValue.builder().s("ACTIVE").build(),
        ":minAge", AttributeValue.builder().n("18").build()
    );

    final boolean result = parser.evaluate(item, "status = :status OR age > :minAge", values, null);

    assertThat(result).isFalse();
  }

  @Test
  void evaluate_notOperator_invertsCondition() {
    final Map<String, AttributeValue> item = Map.of(
        "name", AttributeValue.builder().s("John").build()
    );

    final boolean result = parser.evaluate(item, "NOT attribute_exists(email)", Map.of(), null);

    assertThat(result).isTrue();
  }

  // Tests for expression attribute names

  @Test
  void evaluate_withExpressionAttributeName() {
    final Map<String, AttributeValue> item = Map.of(
        "status", AttributeValue.builder().s("ACTIVE").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":status", AttributeValue.builder().s("ACTIVE").build()
    );
    final Map<String, String> names = Map.of(
        "#s", "status"
    );

    final boolean result = parser.evaluate(item, "#s = :status", values, names);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_complexConditionWithParentheses() {
    final Map<String, AttributeValue> item = Map.of(
        "status", AttributeValue.builder().s("ACTIVE").build(),
        "age", AttributeValue.builder().n("30").build(),
        "premium", AttributeValue.builder().bool(true).build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":status", AttributeValue.builder().s("ACTIVE").build(),
        ":minAge", AttributeValue.builder().n("18").build()
    );

    final boolean result = parser.evaluate(item,
        "(status = :status AND age > :minAge) OR attribute_exists(premium)", values, null);

    assertThat(result).isTrue();
  }

  // Test for null/empty condition

  @Test
  void evaluate_nullCondition_returnsTrue() {
    final Map<String, AttributeValue> item = Map.of(
        "name", AttributeValue.builder().s("John").build()
    );

    final boolean result = parser.evaluate(item, null, Map.of(), null);

    assertThat(result).isTrue();
  }

  @Test
  void evaluate_emptyCondition_returnsTrue() {
    final Map<String, AttributeValue> item = Map.of(
        "name", AttributeValue.builder().s("John").build()
    );

    final boolean result = parser.evaluate(item, "", Map.of(), null);

    assertThat(result).isTrue();
  }

  // Edge cases

  @Test
  void evaluate_missingAttribute_comparisonReturnsFalse() {
    final Map<String, AttributeValue> item = Map.of(
        "name", AttributeValue.builder().s("John").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":age", AttributeValue.builder().n("30").build()
    );

    final boolean result = parser.evaluate(item, "age = :age", values, null);

    assertThat(result).isFalse();
  }

  @Test
  void evaluate_caseInsensitiveOperators() {
    final Map<String, AttributeValue> item = Map.of(
        "status", AttributeValue.builder().s("ACTIVE").build(),
        "age", AttributeValue.builder().n("30").build()
    );
    final Map<String, AttributeValue> values = Map.of(
        ":status", AttributeValue.builder().s("ACTIVE").build(),
        ":minAge", AttributeValue.builder().n("18").build()
    );

    // Test lowercase 'and', 'or', 'between'
    assertThat(parser.evaluate(item, "status = :status and age > :minAge", values, null)).isTrue();
    assertThat(parser.evaluate(item, "status = :status or age > :minAge", values, null)).isTrue();
    assertThat(parser.evaluate(item, "age between :minAge AND :status", Map.of(
        ":minAge", AttributeValue.builder().n("18").build(),
        ":status", AttributeValue.builder().n("40").build()
    ), null)).isTrue();
  }
}
