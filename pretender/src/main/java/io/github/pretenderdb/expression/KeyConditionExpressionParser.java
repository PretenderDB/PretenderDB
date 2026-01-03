package io.github.pretenderdb.expression;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Parses DynamoDB KeyConditionExpression for query operations.
 * Supports: =, &lt;, &gt;, &lt;=, &gt;=, BETWEEN, begins_with()
 */
@Singleton
public class KeyConditionExpressionParser {

  private static final Logger log = LoggerFactory.getLogger(KeyConditionExpressionParser.class);

  // Patterns for parsing key condition expressions
  // Support both direct attribute names (userId) and expression attribute names (#user)
  private static final Pattern HASH_KEY_PATTERN = Pattern.compile("^\\s*(#?\\w+)\\s*=\\s*:(\\w+)");
  private static final Pattern SORT_KEY_EQ_PATTERN = Pattern.compile("AND\\s+(#?\\w+)\\s*=\\s*:(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern SORT_KEY_LT_PATTERN = Pattern.compile("AND\\s+(#?\\w+)\\s*<\\s*:(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern SORT_KEY_GT_PATTERN = Pattern.compile("AND\\s+(#?\\w+)\\s*>\\s*:(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern SORT_KEY_LE_PATTERN = Pattern.compile("AND\\s+(#?\\w+)\\s*<=\\s*:(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern SORT_KEY_GE_PATTERN = Pattern.compile("AND\\s+(#?\\w+)\\s*>=\\s*:(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern SORT_KEY_BETWEEN_PATTERN = Pattern.compile("AND\\s+(#?\\w+)\\s+BETWEEN\\s+:(\\w+)\\s+AND\\s+:(\\w+)", Pattern.CASE_INSENSITIVE);
  private static final Pattern SORT_KEY_BEGINS_WITH_PATTERN = Pattern.compile("AND\\s+begins_with\\s*\\(\\s*(#?\\w+)\\s*,\\s*:(\\w+)\\s*\\)", Pattern.CASE_INSENSITIVE);

  /**
   * Instantiates a new Key condition expression parser.
   */
  @Inject
  public KeyConditionExpressionParser() {
    log.info("KeyConditionExpressionParser()");
  }

  /**
   * Parses a KeyConditionExpression and returns the SQL WHERE clause and sort key value.
   * Backward-compatible overload without expression attribute names.
   *
   * @param keyConditionExpression    the key condition expression
   * @param expressionAttributeValues the expression attribute values
   * @return the parsed result
   */
  public ParsedKeyCondition parse(final String keyConditionExpression,
                                  final Map<String, AttributeValue> expressionAttributeValues) {
    return parse(keyConditionExpression, expressionAttributeValues, null);
  }

  /**
   * Parses a KeyConditionExpression and returns the SQL WHERE clause and sort key value.
   *
   * @param keyConditionExpression    the key condition expression
   * @param expressionAttributeValues the expression attribute values
   * @param expressionAttributeNames  the expression attribute names (optional, can be null)
   * @return the parsed result
   */
  public ParsedKeyCondition parse(final String keyConditionExpression,
                                  final Map<String, AttributeValue> expressionAttributeValues,
                                  final Map<String, String> expressionAttributeNames) {
    log.trace("parse({}, {}, {})", keyConditionExpression, expressionAttributeValues, expressionAttributeNames);

    if (keyConditionExpression == null || keyConditionExpression.isBlank()) {
      throw new IllegalArgumentException("KeyConditionExpression cannot be null or empty");
    }

    // Parse hash key (required)
    final Matcher hashMatcher = HASH_KEY_PATTERN.matcher(keyConditionExpression);
    if (!hashMatcher.find()) {
      throw new IllegalArgumentException("Invalid KeyConditionExpression: missing hash key");
    }

    // Resolve hash key attribute name (supports #placeholder)
    final String hashKeyAttrName = resolveAttributeName(hashMatcher.group(1), expressionAttributeNames);
    log.trace("Resolved hash key attribute: {}", hashKeyAttrName);

    final String hashKeyPlaceholder = hashMatcher.group(2);
    final AttributeValue hashKeyValue = expressionAttributeValues.get(":" + hashKeyPlaceholder);
    if (hashKeyValue == null) {
      throw new IllegalArgumentException("Missing value for placeholder: :" + hashKeyPlaceholder);
    }

    // Parse sort key condition (optional)
    String sortKeyCondition = null;
    Optional<String> sortKeyValue = Optional.empty();

    // Check for BETWEEN
    Matcher sortMatcher = SORT_KEY_BETWEEN_PATTERN.matcher(keyConditionExpression);
    if (sortMatcher.find()) {
      // Resolve sort key attribute name (supports #placeholder)
      final String sortKeyAttrName = resolveAttributeName(sortMatcher.group(1), expressionAttributeNames);
      log.trace("Resolved sort key attribute: {}", sortKeyAttrName);

      final String placeholder1 = sortMatcher.group(2);
      final String placeholder2 = sortMatcher.group(3);
      final AttributeValue value1 = expressionAttributeValues.get(":" + placeholder1);
      final AttributeValue value2 = expressionAttributeValues.get(":" + placeholder2);
      if (value1 == null || value2 == null) {
        throw new IllegalArgumentException("Missing value for BETWEEN placeholders");
      }
      sortKeyCondition = String.format("sort_key_value BETWEEN '%s' AND '%s'",
          extractScalarValue(value1), extractScalarValue(value2));
      return new ParsedKeyCondition(extractScalarValue(hashKeyValue), sortKeyCondition, Optional.empty());
    }

    // Check for begins_with
    sortMatcher = SORT_KEY_BEGINS_WITH_PATTERN.matcher(keyConditionExpression);
    if (sortMatcher.find()) {
      // Resolve sort key attribute name (supports #placeholder)
      final String sortKeyAttrName = resolveAttributeName(sortMatcher.group(1), expressionAttributeNames);
      log.trace("Resolved sort key attribute: {}", sortKeyAttrName);

      final String placeholder = sortMatcher.group(2);
      final AttributeValue value = expressionAttributeValues.get(":" + placeholder);
      if (value == null) {
        throw new IllegalArgumentException("Missing value for begins_with placeholder: :" + placeholder);
      }
      sortKeyCondition = "sort_key_value LIKE :sortKey";
      sortKeyValue = Optional.of(extractScalarValue(value) + "%");
      return new ParsedKeyCondition(extractScalarValue(hashKeyValue), sortKeyCondition, sortKeyValue);
    }

    // Check for comparison operators
    sortMatcher = SORT_KEY_EQ_PATTERN.matcher(keyConditionExpression);
    if (sortMatcher.find()) {
      // Resolve sort key attribute name (supports #placeholder)
      final String sortKeyAttrName = resolveAttributeName(sortMatcher.group(1), expressionAttributeNames);
      log.trace("Resolved sort key attribute: {}", sortKeyAttrName);

      final String placeholder = sortMatcher.group(2);
      final AttributeValue value = expressionAttributeValues.get(":" + placeholder);
      if (value == null) {
        throw new IllegalArgumentException("Missing value for placeholder: :" + placeholder);
      }
      sortKeyCondition = "sort_key_value = :sortKey";
      sortKeyValue = Optional.of(extractScalarValue(value));
      return new ParsedKeyCondition(extractScalarValue(hashKeyValue), sortKeyCondition, sortKeyValue);
    }

    sortMatcher = SORT_KEY_LT_PATTERN.matcher(keyConditionExpression);
    if (sortMatcher.find()) {
      // Resolve sort key attribute name (supports #placeholder)
      final String sortKeyAttrName = resolveAttributeName(sortMatcher.group(1), expressionAttributeNames);
      log.trace("Resolved sort key attribute: {}", sortKeyAttrName);

      final String placeholder = sortMatcher.group(2);
      final AttributeValue value = expressionAttributeValues.get(":" + placeholder);
      if (value == null) {
        throw new IllegalArgumentException("Missing value for placeholder: :" + placeholder);
      }
      sortKeyCondition = "sort_key_value < :sortKey";
      sortKeyValue = Optional.of(extractScalarValue(value));
      return new ParsedKeyCondition(extractScalarValue(hashKeyValue), sortKeyCondition, sortKeyValue);
    }

    sortMatcher = SORT_KEY_GT_PATTERN.matcher(keyConditionExpression);
    if (sortMatcher.find()) {
      // Resolve sort key attribute name (supports #placeholder)
      final String sortKeyAttrName = resolveAttributeName(sortMatcher.group(1), expressionAttributeNames);
      log.trace("Resolved sort key attribute: {}", sortKeyAttrName);

      final String placeholder = sortMatcher.group(2);
      final AttributeValue value = expressionAttributeValues.get(":" + placeholder);
      if (value == null) {
        throw new IllegalArgumentException("Missing value for placeholder: :" + placeholder);
      }
      sortKeyCondition = "sort_key_value > :sortKey";
      sortKeyValue = Optional.of(extractScalarValue(value));
      return new ParsedKeyCondition(extractScalarValue(hashKeyValue), sortKeyCondition, sortKeyValue);
    }

    sortMatcher = SORT_KEY_LE_PATTERN.matcher(keyConditionExpression);
    if (sortMatcher.find()) {
      // Resolve sort key attribute name (supports #placeholder)
      final String sortKeyAttrName = resolveAttributeName(sortMatcher.group(1), expressionAttributeNames);
      log.trace("Resolved sort key attribute: {}", sortKeyAttrName);

      final String placeholder = sortMatcher.group(2);
      final AttributeValue value = expressionAttributeValues.get(":" + placeholder);
      if (value == null) {
        throw new IllegalArgumentException("Missing value for placeholder: :" + placeholder);
      }
      sortKeyCondition = "sort_key_value <= :sortKey";
      sortKeyValue = Optional.of(extractScalarValue(value));
      return new ParsedKeyCondition(extractScalarValue(hashKeyValue), sortKeyCondition, sortKeyValue);
    }

    sortMatcher = SORT_KEY_GE_PATTERN.matcher(keyConditionExpression);
    if (sortMatcher.find()) {
      // Resolve sort key attribute name (supports #placeholder)
      final String sortKeyAttrName = resolveAttributeName(sortMatcher.group(1), expressionAttributeNames);
      log.trace("Resolved sort key attribute: {}", sortKeyAttrName);

      final String placeholder = sortMatcher.group(2);
      final AttributeValue value = expressionAttributeValues.get(":" + placeholder);
      if (value == null) {
        throw new IllegalArgumentException("Missing value for placeholder: :" + placeholder);
      }
      sortKeyCondition = "sort_key_value >= :sortKey";
      sortKeyValue = Optional.of(extractScalarValue(value));
      return new ParsedKeyCondition(extractScalarValue(hashKeyValue), sortKeyCondition, sortKeyValue);
    }

    // Hash key only
    return new ParsedKeyCondition(extractScalarValue(hashKeyValue), null, Optional.empty());
  }

  /**
   * Resolve attribute name (handle expression attribute names).
   *
   * @param name  the attribute name or placeholder
   * @param names the expression attribute names map
   * @return the resolved attribute name
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

  /**
   * Extracts the scalar value from an AttributeValue.
   */
  private String extractScalarValue(final AttributeValue value) {
    if (value.s() != null) {
      return value.s();
    } else if (value.n() != null) {
      return value.n();
    } else if (value.b() != null) {
      return value.b().asUtf8String();
    } else {
      throw new IllegalArgumentException("Key attribute must be a scalar type (S, N, or B)");
    }
  }

  /**
   * Result of parsing a KeyConditionExpression.
   *
   * @param sortKeyCondition SQL WHERE clause fragment for sort key
   * @param sortKeyValue     Value to bind to :sortKey
   */
  public record ParsedKeyCondition(String hashKeyValue, String sortKeyCondition, Optional<String> sortKeyValue) {
    /**
     * Instantiates a new Parsed key condition.
     *
     * @param hashKeyValue     the hash key value
     * @param sortKeyCondition the sort key condition
     * @param sortKeyValue     the sort key value
     */
    public ParsedKeyCondition {
    }

    /**
     * Gets hash key value.
     *
     * @return the hash key value
     */
    @Override
      public String hashKeyValue() {
        return hashKeyValue;
      }

    /**
     * Gets sort key condition (SQL WHERE clause fragment).
     *
     * @return the sort key condition
     */
    @Override
      public String sortKeyCondition() {
        return sortKeyCondition;
      }

    /**
     * Gets sort key value to bind to the SQL parameter.
     *
     * @return the sort key value
     */
    @Override
      public Optional<String> sortKeyValue() {
        return sortKeyValue;
      }
    }
}
