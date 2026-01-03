package io.github.pretenderdb.expression;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Parses and applies DynamoDB UpdateExpression to AttributeValue map.
 * Supports: SET, REMOVE, ADD, DELETE actions.
 */
@Singleton
public class UpdateExpressionParser {

  private static final Logger log = LoggerFactory.getLogger(UpdateExpressionParser.class);

  // Patterns for parsing update expressions
  private static final Pattern SET_PATTERN = Pattern.compile(
      "SET\\s+(.+?)(?=\\s+(?:REMOVE|ADD|DELETE)|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern REMOVE_PATTERN = Pattern.compile(
      "REMOVE\\s+(.+?)(?=\\s+(?:SET|ADD|DELETE)|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern ADD_PATTERN = Pattern.compile(
      "ADD\\s+(.+?)(?=\\s+(?:SET|REMOVE|DELETE)|$)", Pattern.CASE_INSENSITIVE);
  private static final Pattern DELETE_PATTERN = Pattern.compile(
      "DELETE\\s+(.+?)(?=\\s+(?:SET|REMOVE|ADD)|$)", Pattern.CASE_INSENSITIVE);

  // SET action patterns
  // Matches "attr = value" where value can contain commas inside parentheses
  private static final Pattern SET_ASSIGN_PATTERN = Pattern.compile(
      "(#?\\w+)\\s*=\\s*(.+?)(?=\\s*,\\s*#?\\w+\\s*=|$)");
  private static final Pattern LIST_APPEND_PATTERN = Pattern.compile(
      "list_append\\s*\\(\\s*(.+?)\\s*,\\s*(.+?)\\s*\\)");
  private static final Pattern IF_NOT_EXISTS_PATTERN = Pattern.compile(
      "if_not_exists\\s*\\(\\s*(.+?)\\s*,\\s*(.+?)\\s*\\)");
  private static final Pattern NUMERIC_ADD_PATTERN = Pattern.compile(
      "(#?\\w+)\\s*\\+\\s*(.+)");

  /**
   * Instantiates a new Update expression parser.
   */
  @Inject
  public UpdateExpressionParser() {
    log.info("UpdateExpressionParser()");
  }

  /**
   * Applies an UpdateExpression to an AttributeValue map.
   *
   * @param item                      the current item attributes
   * @param updateExpression          the update expression
   * @param expressionAttributeValues the expression attribute values
   * @param expressionAttributeNames  the expression attribute names (optional)
   * @return the updated attributes map
   */
  public Map<String, AttributeValue> applyUpdate(
      final Map<String, AttributeValue> item,
      final String updateExpression,
      final Map<String, AttributeValue> expressionAttributeValues,
      final Map<String, String> expressionAttributeNames) {

    log.trace("applyUpdate({}, {}, {})", item, updateExpression, expressionAttributeValues);

    if (updateExpression == null || updateExpression.isBlank()) {
      throw new IllegalArgumentException("UpdateExpression cannot be null or empty");
    }

    // Work with a mutable copy
    final Map<String, AttributeValue> result = new HashMap<>(item);

    // Process each action type
    processSET(result, updateExpression, expressionAttributeValues, expressionAttributeNames);
    processREMOVE(result, updateExpression, expressionAttributeNames);
    processADD(result, updateExpression, expressionAttributeValues, expressionAttributeNames);
    processDELETE(result, updateExpression, expressionAttributeValues, expressionAttributeNames);

    return result;
  }

  /**
   * Process SET actions.
   */
  private void processSET(final Map<String, AttributeValue> item,
                          final String updateExpression,
                          final Map<String, AttributeValue> values,
                          final Map<String, String> names) {
    final Matcher setMatcher = SET_PATTERN.matcher(updateExpression);
    if (!setMatcher.find()) {
      return;
    }

    final String setClause = setMatcher.group(1).trim();
    final Matcher assignMatcher = SET_ASSIGN_PATTERN.matcher(setClause);

    while (assignMatcher.find()) {
      final String attrName = resolveAttributeName(assignMatcher.group(1).trim(), names);
      final String valueExpr = assignMatcher.group(2).trim();

      // Check for list_append
      final Matcher listAppendMatcher = LIST_APPEND_PATTERN.matcher(valueExpr);
      if (listAppendMatcher.matches()) {
        applyListAppend(item, attrName, listAppendMatcher.group(1).trim(),
            listAppendMatcher.group(2).trim(), values, names);
        continue;
      }

      // Check for if_not_exists
      final Matcher ifNotExistsMatcher = IF_NOT_EXISTS_PATTERN.matcher(valueExpr);
      if (ifNotExistsMatcher.matches()) {
        applyIfNotExists(item, attrName, ifNotExistsMatcher.group(1).trim(),
            ifNotExistsMatcher.group(2).trim(), values, names);
        continue;
      }

      // Check for numeric addition (attr + value)
      final Matcher numericAddMatcher = NUMERIC_ADD_PATTERN.matcher(valueExpr);
      if (numericAddMatcher.matches()) {
        applyNumericAdd(item, attrName, numericAddMatcher.group(1).trim(),
            numericAddMatcher.group(2).trim(), values, names);
        continue;
      }

      // Simple assignment
      final AttributeValue value = resolveValue(valueExpr, values);
      item.put(attrName, value);
    }
  }

  /**
   * Process REMOVE actions.
   */
  private void processREMOVE(final Map<String, AttributeValue> item,
                             final String updateExpression,
                             final Map<String, String> names) {
    final Matcher removeMatcher = REMOVE_PATTERN.matcher(updateExpression);
    if (!removeMatcher.find()) {
      return;
    }

    final String removeClause = removeMatcher.group(1).trim();
    final String[] attrs = removeClause.split(",");

    for (String attr : attrs) {
      final String attrName = resolveAttributeName(attr.trim(), names);
      item.remove(attrName);
    }
  }

  /**
   * Process ADD actions.
   */
  private void processADD(final Map<String, AttributeValue> item,
                          final String updateExpression,
                          final Map<String, AttributeValue> values,
                          final Map<String, String> names) {
    final Matcher addMatcher = ADD_PATTERN.matcher(updateExpression);
    if (!addMatcher.find()) {
      return;
    }

    final String addClause = addMatcher.group(1).trim();
    final String[] parts = addClause.split(",");

    for (String part : parts) {
      final String[] attrValue = part.trim().split("\\s+", 2);
      if (attrValue.length != 2) {
        throw new IllegalArgumentException("Invalid ADD syntax: " + part);
      }

      final String attrName = resolveAttributeName(attrValue[0].trim(), names);
      final AttributeValue addValue = resolveValue(attrValue[1].trim(), values);

      // Numeric addition
      if (addValue.n() != null) {
        final AttributeValue current = item.get(attrName);
        if (current == null) {
          item.put(attrName, addValue);
        } else if (current.n() != null) {
          final BigDecimal sum = new BigDecimal(current.n()).add(new BigDecimal(addValue.n()));
          item.put(attrName, AttributeValue.builder().n(sum.toString()).build());
        } else {
          throw new IllegalArgumentException("Cannot ADD number to non-numeric attribute: " + attrName);
        }
      }
      // Set addition
      else if (addValue.ss() != null || addValue.ns() != null || addValue.bs() != null) {
        addToSet(item, attrName, addValue);
      } else {
        throw new IllegalArgumentException("ADD requires number or set type");
      }
    }
  }

  /**
   * Process DELETE actions.
   */
  private void processDELETE(final Map<String, AttributeValue> item,
                             final String updateExpression,
                             final Map<String, AttributeValue> values,
                             final Map<String, String> names) {
    final Matcher deleteMatcher = DELETE_PATTERN.matcher(updateExpression);
    if (!deleteMatcher.find()) {
      return;
    }

    final String deleteClause = deleteMatcher.group(1).trim();
    final String[] parts = deleteClause.split(",");

    for (String part : parts) {
      final String[] attrValue = part.trim().split("\\s+", 2);
      if (attrValue.length != 2) {
        throw new IllegalArgumentException("Invalid DELETE syntax: " + part);
      }

      final String attrName = resolveAttributeName(attrValue[0].trim(), names);
      final AttributeValue deleteValue = resolveValue(attrValue[1].trim(), values);

      deleteFromSet(item, attrName, deleteValue);
    }
  }

  /**
   * Apply list_append function.
   */
  private void applyListAppend(final Map<String, AttributeValue> item,
                               final String attrName,
                               final String list1Expr,
                               final String list2Expr,
                               final Map<String, AttributeValue> values,
                               final Map<String, String> names) {
    final AttributeValue list1 = resolveValueOrAttribute(list1Expr, item, values, names);
    final AttributeValue list2 = resolveValueOrAttribute(list2Expr, item, values, names);

    if (list1 == null || list1.l() == null) {
      item.put(attrName, list2);
      return;
    }
    if (list2 == null || list2.l() == null) {
      item.put(attrName, list1);
      return;
    }

    final List<AttributeValue> result = new ArrayList<>(list1.l());
    result.addAll(list2.l());
    item.put(attrName, AttributeValue.builder().l(result).build());
  }

  /**
   * Apply if_not_exists function.
   */
  private void applyIfNotExists(final Map<String, AttributeValue> item,
                                final String attrName,
                                final String checkAttrExpr,
                                final String defaultValueExpr,
                                final Map<String, AttributeValue> values,
                                final Map<String, String> names) {
    final String checkAttr = resolveAttributeName(checkAttrExpr, names);
    if (!item.containsKey(checkAttr)) {
      final AttributeValue defaultValue = resolveValue(defaultValueExpr, values);
      item.put(attrName, defaultValue);
    }
  }

  /**
   * Apply numeric addition.
   */
  private void applyNumericAdd(final Map<String, AttributeValue> item,
                               final String attrName,
                               final String operandExpr,
                               final String addValueExpr,
                               final Map<String, AttributeValue> values,
                               final Map<String, String> names) {
    final String operandAttr = resolveAttributeName(operandExpr, names);
    final AttributeValue current = item.get(operandAttr);
    final AttributeValue addValue = resolveValue(addValueExpr, values);

    if (current == null || current.n() == null) {
      throw new IllegalArgumentException("Cannot add to non-numeric attribute: " + operandAttr);
    }
    if (addValue.n() == null) {
      throw new IllegalArgumentException("ADD value must be numeric");
    }

    final BigDecimal sum = new BigDecimal(current.n()).add(new BigDecimal(addValue.n()));
    item.put(attrName, AttributeValue.builder().n(sum.toString()).build());
  }

  /**
   * Add values to a set.
   */
  private void addToSet(final Map<String, AttributeValue> item,
                        final String attrName,
                        final AttributeValue addValue) {
    final AttributeValue current = item.get(attrName);

    if (addValue.ss() != null) {
      final Set<String> result = current != null && current.ss() != null
          ? new HashSet<>(current.ss()) : new HashSet<>();
      result.addAll(addValue.ss());
      item.put(attrName, AttributeValue.builder().ss(result).build());
    } else if (addValue.ns() != null) {
      final Set<String> result = current != null && current.ns() != null
          ? new HashSet<>(current.ns()) : new HashSet<>();
      result.addAll(addValue.ns());
      item.put(attrName, AttributeValue.builder().ns(result).build());
    } else if (addValue.bs() != null) {
      final Set<software.amazon.awssdk.core.SdkBytes> result = current != null && current.bs() != null
          ? new HashSet<>(current.bs()) : new HashSet<>();
      result.addAll(addValue.bs());
      item.put(attrName, AttributeValue.builder().bs(result).build());
    }
  }

  /**
   * Delete values from a set.
   */
  private void deleteFromSet(final Map<String, AttributeValue> item,
                             final String attrName,
                             final AttributeValue deleteValue) {
    final AttributeValue current = item.get(attrName);
    if (current == null) {
      return;
    }

    if (deleteValue.ss() != null && current.ss() != null) {
      final Set<String> result = new HashSet<>(current.ss());
      result.removeAll(deleteValue.ss());
      if (result.isEmpty()) {
        item.remove(attrName);
      } else {
        item.put(attrName, AttributeValue.builder().ss(result).build());
      }
    } else if (deleteValue.ns() != null && current.ns() != null) {
      final Set<String> result = new HashSet<>(current.ns());
      result.removeAll(deleteValue.ns());
      if (result.isEmpty()) {
        item.remove(attrName);
      } else {
        item.put(attrName, AttributeValue.builder().ns(result).build());
      }
    } else if (deleteValue.bs() != null && current.bs() != null) {
      final Set<software.amazon.awssdk.core.SdkBytes> result = new HashSet<>(current.bs());
      result.removeAll(deleteValue.bs());
      if (result.isEmpty()) {
        item.remove(attrName);
      } else {
        item.put(attrName, AttributeValue.builder().bs(result).build());
      }
    }
  }

  /**
   * Resolve attribute name (handle expression attribute names).
   */
  private String resolveAttributeName(final String name, final Map<String, String> names) {
    if (name.startsWith("#") && names != null) {
      final String resolved = names.get(name);
      if (resolved == null) {
        throw new IllegalArgumentException("Expression attribute name not found: " + name);
      }
      return resolved;
    }
    return name;
  }

  /**
   * Resolve value from expression attribute values.
   */
  private AttributeValue resolveValue(final String valueExpr, final Map<String, AttributeValue> values) {
    if (valueExpr.startsWith(":")) {
      final AttributeValue value = values.get(valueExpr);
      if (value == null) {
        throw new IllegalArgumentException("Expression attribute value not found: " + valueExpr);
      }
      return value;
    }
    throw new IllegalArgumentException("Invalid value expression (must start with :): " + valueExpr);
  }

  /**
   * Resolve value from either attribute or expression values.
   */
  private AttributeValue resolveValueOrAttribute(final String expr,
                                                 final Map<String, AttributeValue> item,
                                                 final Map<String, AttributeValue> values,
                                                 final Map<String, String> names) {
    if (expr.startsWith(":")) {
      return resolveValue(expr, values);
    } else {
      final String attrName = resolveAttributeName(expr, names);
      return item.get(attrName);
    }
  }
}
