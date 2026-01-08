package io.github.pretenderdb.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.pretenderdb.model.PdbGlobalSecondaryIndex;
import java.sql.Types;
import java.util.List;
import org.jdbi.v3.core.argument.AbstractArgumentFactory;
import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.config.ConfigRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBI ArgumentFactory for serializing List of PdbGlobalSecondaryIndex to JSON.
 */
public class GsiListArgumentFactory extends AbstractArgumentFactory<List<PdbGlobalSecondaryIndex>> {

  private static final Logger log = LoggerFactory.getLogger(GsiListArgumentFactory.class);
  private final ObjectMapper objectMapper;

  /**
   * Instantiates a new Gsi list argument factory.
   *
   * @param objectMapper the object mapper
   */
  public GsiListArgumentFactory(final ObjectMapper objectMapper) {
    super(Types.CLOB);
    this.objectMapper = objectMapper;
  }

  @Override
  protected Argument build(final List<PdbGlobalSecondaryIndex> value, final ConfigRegistry config) {
    return (position, statement, ctx) -> {
      if (value == null || value.isEmpty()) {
        statement.setNull(position, Types.CLOB);
      } else {
        try {
          // Manually serialize to avoid Immutables serialization issues
          final ArrayNode arrayNode = objectMapper.createArrayNode();
          for (PdbGlobalSecondaryIndex gsi : value) {
            final ObjectNode gsiNode = objectMapper.createObjectNode();
            gsiNode.put("indexName", gsi.indexName());
            gsiNode.put("hashKey", gsi.hashKey());
            gsi.sortKey().ifPresent(sk -> gsiNode.put("sortKey", sk));
            gsiNode.put("projectionType", gsi.projectionType());
            gsi.nonKeyAttributes().ifPresent(nka -> gsiNode.put("nonKeyAttributes", nka));
            arrayNode.add(gsiNode);
          }
          final String json = objectMapper.writeValueAsString(arrayNode);
          statement.setString(position, json);
        } catch (Exception e) {
          log.error("Failed to serialize GSI list to JSON", e);
          throw new RuntimeException("Failed to serialize GSI list", e);
        }
      }
    };
  }
}
