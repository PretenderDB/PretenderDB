package io.github.pretenderdb.expression;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Parses and evaluates DynamoDB ConditionExpression for putItem and deleteItem operations.
 * Supports: comparison operators, logical operators, and DynamoDB functions.
 */
@Singleton
public class ConditionExpressionParser {

  private static final Logger log = LoggerFactory.getLogger(ConditionExpressionParser.class);

  // Patterns for condition functions
  private static final Pattern ATTRIBUTE_EXISTS_PATTERN = Pattern.compile(
      "attribute_exists\\s*\\(\\s*(#?\\w+)\\s*\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern ATTRIBUTE_NOT_EXISTS_PATTERN = Pattern.compile(
      "attribute_not_exists\\s*\\(\\s*(#?\\w+)\\s*\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern BEGINS_WITH_PATTERN = Pattern.compile(
      "begins_with\\s*\\(\\s*(#?\\w+)\\s*,\\s*:(\\w+)\\s*\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern CONTAINS_PATTERN = Pattern.compile(
      "contains\\s*\\(\\s*(#?\\w+)\\s*,\\s*:(\\w+)\\s*\\)", Pattern.CASE_INSENSITIVE);

  // Patterns for comparison operators
  private static final Pattern EQUALS_PATTERN = Pattern.compile(
      "(#?\\w+)\\s*=\\s*:(\\w+)");
  private static final Pattern NOT_EQUALS_PATTERN = Pattern.compile(
      "(#?\\w+)\\s*<>\\s*:(\\w+)");
  private static final Pattern LESS_THAN_PATTERN = Pattern.compile(
      "(#?\\w+)\\s*<\\s*:(\\w+)");
  private static final Pattern GREATER_THAN_PATTERN = Pattern.compile(
      "(#?\\w+)\\s*>\\s*:(\\w+)");
  private static final Pattern LESS_THAN_EQUALS_PATTERN = Pattern.compile(
      "(#?\\w+)\\s*<=\\s*:(\\w+)");
  private static final Pattern GREATER_THAN_EQUALS_PATTERN = Pattern.compile(
      "(#?\\w+)\\s*>=\\s*:(\\w+)");
  private static final Pattern BETWEEN_PATTERN = Pattern.compile(
      "(#?\\w+)\\s+BETWEEN\\s+:(\\w+)\\s+AND\\s+:(\\w+)", Pattern.CASE_INSENSITIVE);

  /**
   * Instantiates a new Condition expression parser.
   */
  @Inject
  public ConditionExpressionParser() {
    log.info("ConditionExpressionParser()");
  }

  /**
   * Evaluates a ConditionExpression against an item.
   *
   * @param item                      the item to evaluate against (can be null for deleteItem on non-existent item)
   * @param conditionExpression       the condition expression
   * @param expressionAttributeValues the expression attribute values
   * @param expressionAttributeNames  the expression attribute names (optional)
   * @return true if condition is satisfied, false otherwise
   */
  public boolean evaluate(final Map<String, AttributeValue> item,
                          final String conditionExpression,
                          final Map<String, AttributeValue> expressionAttributeValues,
                          final Map<String, String> expressionAttributeNames) {
    log.trace("evaluate({}, {}, {}, {})", item, conditionExpression, expressionAttributeValues, expressionAttributeNames);

    if (conditionExpression == null || conditionExpression.isBlank()) {
      // No condition means always pass
      return true;
    }

    // Handle logical operators (simplified: split by AND/OR)
    // For initial implementation, support simple conditions and AND/OR combinations
    return evaluateCondition(item, conditionExpression.trim(), expressionAttributeValues, expressionAttributeNames);
  }

  /**
   * Evaluates a single condition or compound condition with AND/OR.
   * Simplified approach: check for atomic conditions first, then split on OR, then AND.
   */
  private boolean evaluateCondition(final Map<String, AttributeValue> item,
                                    final String condition,
                                    final Map<String, AttributeValue> values,
                                    final Map<String, String> names) {
    final String trimmed = condition.trim();

    // Handle parentheses for grouping - only strip if they wrap the entire expression
    if (trimmed.startsWith("(") && trimmed.endsWith(")") && isWrappedInParentheses(trimmed)) {
      return evaluateCondition(item, trimmed.substring(1, trimmed.length() - 1).trim(), values, names);
    }

    // Handle NOT operator
    if (trimmed.startsWith("NOT ") || trimmed.startsWith("not ")) {
      final String innerCondition = trimmed.substring(4).trim();
      return !evaluateCondition(item, innerCondition, values, names);
    }

    // Try to split on OR (lowest precedence) - but not within BETWEEN or parentheses
    final Pattern orPattern = Pattern.compile("\\s+(?i)or\\s+");
    Matcher orMatcher = orPattern.matcher(trimmed);
    while (orMatcher.find()) {
      // Check if this OR is not within a BETWEEN expression or parentheses
      final String beforeOr = trimmed.substring(0, orMatcher.start());
      if (!isWithinBetween(beforeOr, trimmed) && !isWithinParentheses(trimmed, orMatcher.start())) {
        final String left = trimmed.substring(0, orMatcher.start()).trim();
        final String right = trimmed.substring(orMatcher.end()).trim();
        return evaluateCondition(item, left, values, names) || evaluateCondition(item, right, values, names);
      }
    }

    // Try to split on AND (higher precedence than OR) - but not within BETWEEN or parentheses
    final Pattern andPattern = Pattern.compile("\\s+(?i)and\\s+");
    Matcher andMatcher = andPattern.matcher(trimmed);
    while (andMatcher.find()) {
      // Check if this AND is not within a BETWEEN expression or parentheses
      final String beforeAnd = trimmed.substring(0, andMatcher.start());
      if (!isWithinBetween(beforeAnd, trimmed) && !isWithinParentheses(trimmed, andMatcher.start())) {
        final String left = trimmed.substring(0, andMatcher.start()).trim();
        final String right = trimmed.substring(andMatcher.end()).trim();
        return evaluateCondition(item, left, values, names) && evaluateCondition(item, right, values, names);
      }
    }

    // No logical operators found, evaluate as atomic condition
    return evaluateAtomicCondition(item, trimmed, values, names);
  }

  /**
   * Checks if the opening "(" and closing ")" wrap the entire expression.
   * Returns true if the first "(" matches with the last ")", false otherwise.
   */
  private boolean isWrappedInParentheses(final String expression) {
    if (!expression.startsWith("(") || !expression.endsWith(")")) {
      return false;
    }
    // Track parenthesis depth
    int depth = 0;
    for (int i = 0; i < expression.length(); i++) {
      if (expression.charAt(i) == '(') {
        depth++;
      } else if (expression.charAt(i) == ')') {
        depth--;
        // If we reach depth 0 before the end, the outer parentheses don't wrap everything
        if (depth == 0 && i < expression.length() - 1) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks if a position in the expression is within parentheses.
   * Returns true if the position is inside any set of parentheses.
   */
  private boolean isWithinParentheses(final String expression, final int position) {
    int depth = 0;
    for (int i = 0; i < position && i < expression.length(); i++) {
      if (expression.charAt(i) == '(') {
        depth++;
      } else if (expression.charAt(i) == ')') {
        depth--;
      }
    }
    return depth > 0;
  }

  /**
   * Checks if the current position is within a BETWEEN expression.
   * A simple heuristic: if there's an unmatched BETWEEN keyword before this point.
   */
  private boolean isWithinBetween(final String beforeOperator, final String fullCondition) {
    final String upper = beforeOperator.toUpperCase();
    final int betweenIdx = upper.lastIndexOf(" BETWEEN ");
    if (betweenIdx < 0) {
      return false;  // No BETWEEN before this operator
    }

    // Check if there's a closing AND for this BETWEEN before our position
    final String afterBetween = beforeOperator.substring(betweenIdx + 9);  // 9 = length of " BETWEEN "
    final int andAfterBetween = afterBetween.toUpperCase().indexOf(" AND ");
    // If there's no AND after BETWEEN yet, we're within the BETWEEN
    return andAfterBetween < 0;
  }

  /**
   * Evaluates a single atomic condition (no logical operators).
   */
  private boolean evaluateAtomicCondition(final Map<String, AttributeValue> item,
                                          final String condition,
                                          final Map<String, AttributeValue> values,
                                          final Map<String, String> names) {
    // Check for attribute_exists
    Matcher matcher = ATTRIBUTE_EXISTS_PATTERN.matcher(condition);
    if (matcher.matches()) {
      final String attrName = resolveAttributeName(matcher.group(1), names);
      return item != null && item.containsKey(attrName);
    }

    // Check for attribute_not_exists
    matcher = ATTRIBUTE_NOT_EXISTS_PATTERN.matcher(condition);
    if (matcher.matches()) {
      final String attrName = resolveAttributeName(matcher.group(1), names);
      return item == null || !item.containsKey(attrName);
    }

    // Check for begins_with
    matcher = BEGINS_WITH_PATTERN.matcher(condition);
    if (matcher.matches()) {
      final String attrName = resolveAttributeName(matcher.group(1), names);
      final String valuePlaceholder = ":" + matcher.group(2);
      if (item == null || !item.containsKey(attrName)) {
        return false;
      }
      final AttributeValue itemValue = item.get(attrName);
      final AttributeValue prefixValue = values.get(valuePlaceholder);
      if (itemValue.s() == null || prefixValue.s() == null) {
        return false;
      }
      return itemValue.s().startsWith(prefixValue.s());
    }

    // Check for contains
    matcher = CONTAINS_PATTERN.matcher(condition);
    if (matcher.matches()) {
      final String attrName = resolveAttributeName(matcher.group(1), names);
      final String valuePlaceholder = ":" + matcher.group(2);
      if (item == null || !item.containsKey(attrName)) {
        return false;
      }
      final AttributeValue itemValue = item.get(attrName);
      final AttributeValue searchValue = values.get(valuePlaceholder);
      return containsValue(itemValue, searchValue);
    }

    // Check for BETWEEN
    matcher = BETWEEN_PATTERN.matcher(condition);
    if (matcher.matches()) {
      final String attrName = resolveAttributeName(matcher.group(1), names);
      final String value1Placeholder = ":" + matcher.group(2);
      final String value2Placeholder = ":" + matcher.group(3);
      if (item == null || !item.containsKey(attrName)) {
        return false;
      }
      final AttributeValue itemValue = item.get(attrName);
      final AttributeValue lowerBound = values.get(value1Placeholder);
      final AttributeValue upperBound = values.get(value2Placeholder);
      return compareValues(itemValue, lowerBound) >= 0 && compareValues(itemValue, upperBound) <= 0;
    }

    // Check for comparison operators (order matters: check <= before <, >= before >)
    matcher = LESS_THAN_EQUALS_PATTERN.matcher(condition);
    if (matcher.matches()) {
      return evaluateComparison(item, matcher.group(1), matcher.group(2), values, names, "<=");
    }

    matcher = GREATER_THAN_EQUALS_PATTERN.matcher(condition);
    if (matcher.matches()) {
      return evaluateComparison(item, matcher.group(1), matcher.group(2), values, names, ">=");
    }

    matcher = NOT_EQUALS_PATTERN.matcher(condition);
    if (matcher.matches()) {
      return evaluateComparison(item, matcher.group(1), matcher.group(2), values, names, "<>");
    }

    matcher = LESS_THAN_PATTERN.matcher(condition);
    if (matcher.matches()) {
      return evaluateComparison(item, matcher.group(1), matcher.group(2), values, names, "<");
    }

    matcher = GREATER_THAN_PATTERN.matcher(condition);
    if (matcher.matches()) {
      return evaluateComparison(item, matcher.group(1), matcher.group(2), values, names, ">");
    }

    matcher = EQUALS_PATTERN.matcher(condition);
    if (matcher.matches()) {
      return evaluateComparison(item, matcher.group(1), matcher.group(2), values, names, "=");
    }

    // If we get here, we couldn't parse the condition
    log.warn("Unable to parse condition: {}", condition);
    return false;
  }

  /**
   * Evaluates a comparison operation.
   */
  private boolean evaluateComparison(final Map<String, AttributeValue> item,
                                     final String attrNameToken,
                                     final String valuePlaceholder,
                                     final Map<String, AttributeValue> values,
                                     final Map<String, String> names,
                                     final String operator) {
    final String attrName = resolveAttributeName(attrNameToken, names);
    if (item == null || !item.containsKey(attrName)) {
      return false;
    }

    final AttributeValue itemValue = item.get(attrName);
    final AttributeValue compareValue = values.get(":" + valuePlaceholder);
    if (compareValue == null) {
      throw new IllegalArgumentException("Missing value for placeholder: :" + valuePlaceholder);
    }

    final int cmp = compareValues(itemValue, compareValue);
    switch (operator) {
      case "=":
        return cmp == 0;
      case "<>":
        return cmp != 0;
      case "<":
        return cmp < 0;
      case ">":
        return cmp > 0;
      case "<=":
        return cmp <= 0;
      case ">=":
        return cmp >= 0;
      default:
        return false;
    }
  }

  /**
   * Compares two AttributeValues.
   * Returns negative if v1 < v2, zero if equal, positive if v1 > v2.
   */
  private int compareValues(final AttributeValue v1, final AttributeValue v2) {
    // String comparison
    if (v1.s() != null && v2.s() != null) {
      return v1.s().compareTo(v2.s());
    }

    // Number comparison
    if (v1.n() != null && v2.n() != null) {
      final double n1 = Double.parseDouble(v1.n());
      final double n2 = Double.parseDouble(v2.n());
      return Double.compare(n1, n2);
    }

    // Boolean comparison
    if (v1.bool() != null && v2.bool() != null) {
      return Boolean.compare(v1.bool(), v2.bool());
    }

    // Binary comparison
    if (v1.b() != null && v2.b() != null) {
      return v1.b().asByteBuffer().compareTo(v2.b().asByteBuffer());
    }

    // If types don't match or are unsupported, return not equal
    return -1;
  }

  /**
   * Checks if a value contains another value (for strings, lists, and sets).
   */
  private boolean containsValue(final AttributeValue itemValue, final AttributeValue searchValue) {
    // String contains
    if (itemValue.s() != null && searchValue.s() != null) {
      return itemValue.s().contains(searchValue.s());
    }

    // List contains
    if (itemValue.hasL() && searchValue != null) {
      for (AttributeValue listItem : itemValue.l()) {
        if (compareValues(listItem, searchValue) == 0) {
          return true;
        }
      }
      return false;
    }

    // String set contains
    if (itemValue.hasSs() && searchValue.s() != null) {
      return itemValue.ss().contains(searchValue.s());
    }

    // Number set contains
    if (itemValue.hasNs() && searchValue.n() != null) {
      return itemValue.ns().contains(searchValue.n());
    }

    // Binary set contains
    if (itemValue.hasBs() && searchValue.b() != null) {
      return itemValue.bs().contains(searchValue.b());
    }

    return false;
  }

  /**
   * Resolve attribute name (handle expression attribute names).
   */
  private String resolveAttributeName(final String name, final Map<String, String> names) {
    if (name.startsWith("#")) {
      if (names == null) {
        throw new IllegalArgumentException("Expression attribute name used but expressionAttributeNames not provided: " + name);
      }
      final String resolved = names.get(name);
      if (resolved == null) {
        throw new IllegalArgumentException("Expression attribute name not found: " + name);
      }
      return resolved;
    }
    return name;
  }
}
