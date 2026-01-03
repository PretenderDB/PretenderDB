package io.github.pretenderdb.converter;

import io.github.pretenderdb.model.ImmutablePdbItem;
import io.github.pretenderdb.model.PdbItem;
import io.github.pretenderdb.model.PdbMetadata;
import java.time.Clock;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Converts between DynamoDB request/response items and PdbItem.
 */
@Singleton
public class ItemConverter {

  private static final Logger log = LoggerFactory.getLogger(ItemConverter.class);

  private final AttributeValueConverter attributeValueConverter;
  private final Clock clock;

  /**
   * Instantiates a new Item converter.
   *
   * @param attributeValueConverter the attribute value converter
   * @param clock                   the clock
   */
  @Inject
  public ItemConverter(final AttributeValueConverter attributeValueConverter,
                       final Clock clock) {
    log.info("ItemConverter({}, {})", attributeValueConverter, clock);
    this.attributeValueConverter = attributeValueConverter;
    this.clock = clock;
  }

  /**
   * Converts a DynamoDB item map to PdbItem.
   *
   * @param tableName the table name
   * @param item      the item attributes
   * @param metadata  the table metadata
   * @return the pdb item
   */
  public PdbItem toPdbItem(final String tableName,
                           final Map<String, AttributeValue> item,
                           final PdbMetadata metadata) {
    log.trace("toPdbItem({}, {}, {})", tableName, item, metadata);

    // Validate hash key exists
    final String hashKeyValue = attributeValueConverter.extractKeyValue(item, metadata.hashKey());

    // Extract sort key if table has one
    final ImmutablePdbItem.Builder builder = ImmutablePdbItem.builder()
        .tableName(tableName)
        .hashKeyValue(hashKeyValue)
        .attributesJson(attributeValueConverter.toJson(item))
        .createDate(clock.instant())
        .updateDate(clock.instant());

    // Only set sort key if table has one
    metadata.sortKey().ifPresent(sortKey -> {
      final String sortKeyValue = attributeValueConverter.extractKeyValue(item, sortKey);
      builder.sortKeyValue(sortKeyValue);
    });

    return builder.build();
  }

  /**
   * Updates an existing PdbItem with new attributes.
   *
   * @param existingItem the existing item
   * @param newItem      the new item attributes
   * @param metadata     the table metadata
   * @return the updated pdb item
   */
  public PdbItem updatePdbItem(final PdbItem existingItem,
                               final Map<String, AttributeValue> newItem,
                               final PdbMetadata metadata) {
    log.trace("updatePdbItem({}, {}, {})", existingItem, newItem, metadata);

    // Validate hash key exists and matches
    final String hashKeyValue = attributeValueConverter.extractKeyValue(newItem, metadata.hashKey());
    if (!hashKeyValue.equals(existingItem.hashKeyValue())) {
      throw new IllegalArgumentException("Cannot update hash key value");
    }

    // Validate sort key if table has one
    metadata.sortKey().ifPresent(sortKey -> {
      final String sortKeyValue = attributeValueConverter.extractKeyValue(newItem, sortKey);
      if (!sortKeyValue.equals(existingItem.sortKeyValue().orElse(null))) {
        throw new IllegalArgumentException("Cannot update sort key value");
      }
    });

    // Serialize new attributes
    final String attributesJson = attributeValueConverter.toJson(newItem);

    return ImmutablePdbItem.builder()
        .from(existingItem)
        .attributesJson(attributesJson)
        .updateDate(clock.instant())
        .build();
  }

  /**
   * Converts a PdbItem back to DynamoDB AttributeValue map.
   *
   * @param pdbItem the pdb item
   * @return the attribute value map
   */
  public Map<String, AttributeValue> toItemAttributes(final PdbItem pdbItem) {
    log.trace("toItemAttributes({})", pdbItem);
    return attributeValueConverter.fromJson(pdbItem.attributesJson());
  }

  /**
   * Applies a projection expression to filter attributes.
   * Supports simple comma-separated attribute names.
   *
   * @param item                 the item
   * @param projectionExpression the projection expression (e.g., "id,name,age")
   * @return the filtered item
   */
  public Map<String, AttributeValue> applyProjection(final Map<String, AttributeValue> item,
                                                     final String projectionExpression) {
    log.trace("applyProjection({}, {})", item, projectionExpression);

    if (projectionExpression == null || projectionExpression.isBlank()) {
      return item;
    }

    // Parse projection expression (simple comma-separated for now)
    final String[] projectedAttributes = Arrays.stream(projectionExpression.split(","))
        .map(String::trim)
        .toArray(String[]::new);

    // Filter item to only include projected attributes
    return Arrays.stream(projectedAttributes)
        .filter(item::containsKey)
        .collect(Collectors.toMap(
            attr -> attr,
            item::get
        ));
  }
}
