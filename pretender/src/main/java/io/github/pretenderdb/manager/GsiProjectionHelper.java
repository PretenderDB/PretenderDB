package io.github.pretenderdb.manager;

import io.github.pretenderdb.model.PdbGlobalSecondaryIndex;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Helper for applying GSI projection to item attributes.
 */
@Singleton
public class GsiProjectionHelper {

  private static final Logger log = LoggerFactory.getLogger(GsiProjectionHelper.class);

  /**
   * Instantiates a new Gsi projection helper.
   */
  @Inject
  public GsiProjectionHelper() {
    log.info("GsiProjectionHelper()");
  }

  /**
   * Applies GSI projection to an item's attributes.
   *
   * @param item        the complete item attributes
   * @param gsi         the GSI metadata
   * @param baseHashKey the base table hash key name
   * @param baseSortKey the base table sort key name (optional)
   * @return the projected attributes
   */
  public Map<String, AttributeValue> applyProjection(
      final Map<String, AttributeValue> item,
      final PdbGlobalSecondaryIndex gsi,
      final String baseHashKey,
      final String baseSortKey) {

    final String projectionType = gsi.projectionType();

    if ("ALL".equals(projectionType)) {
      // Include all attributes
      return new HashMap<>(item);
    }

    // Start with key attributes
    final Set<String> includedAttributes = new HashSet<>();

    // Base table keys
    includedAttributes.add(baseHashKey);
    if (baseSortKey != null) {
      includedAttributes.add(baseSortKey);
    }

    // GSI keys
    includedAttributes.add(gsi.hashKey());
    gsi.sortKey().ifPresent(includedAttributes::add);

    if ("INCLUDE".equals(projectionType)) {
      // Add non-key attributes from projection
      gsi.nonKeyAttributes().ifPresent(attrs -> {
        for (String attr : attrs.split(",")) {
          includedAttributes.add(attr.trim());
        }
      });
    }

    // KEYS_ONLY or INCLUDE: filter attributes
    final Map<String, AttributeValue> projected = new HashMap<>();
    for (String attrName : includedAttributes) {
      if (item.containsKey(attrName)) {
        projected.put(attrName, item.get(attrName));
      }
    }

    return projected;
  }

  /**
   * Checks if an item has all required GSI key attributes.
   *
   * @param item the item attributes
   * @param gsi  the GSI metadata
   * @return true if the item has all GSI keys
   */
  public boolean hasGsiKeys(final Map<String, AttributeValue> item, final PdbGlobalSecondaryIndex gsi) {
    if (!item.containsKey(gsi.hashKey())) {
      return false;
    }

    return gsi.sortKey().isEmpty() || item.containsKey(gsi.sortKey().get());
  }
}
