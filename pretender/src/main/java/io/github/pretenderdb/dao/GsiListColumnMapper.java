package io.github.pretenderdb.dao;

import io.github.pretenderdb.model.ImmutablePdbGlobalSecondaryIndex;
import io.github.pretenderdb.model.PdbGlobalSecondaryIndex;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBI ColumnMapper for deserializing List of PdbGlobalSecondaryIndex from JSON.
 */
public class GsiListColumnMapper implements ColumnMapper<List<PdbGlobalSecondaryIndex>> {

  private static final Logger log = LoggerFactory.getLogger(GsiListColumnMapper.class);

  private final ObjectMapper objectMapper;

  /**
   * Instantiates a new Gsi list column mapper.
   *
   * @param objectMapper the object mapper
   */
  public GsiListColumnMapper(final ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public List<PdbGlobalSecondaryIndex> map(final ResultSet rs, final int columnNumber, final StatementContext ctx)
      throws SQLException {
    final String json = rs.getString(columnNumber);
    if (json == null || json.trim().isEmpty()) {
      return List.of();
    }
    try {
      // Manually deserialize to avoid Immutables deserialization issues
      final JsonNode arrayNode = objectMapper.readTree(json);
      final List<PdbGlobalSecondaryIndex> result = new ArrayList<>();

      if (arrayNode.isArray()) {
        for (JsonNode gsiNode : arrayNode) {
          final ImmutablePdbGlobalSecondaryIndex.Builder builder = ImmutablePdbGlobalSecondaryIndex.builder()
              .indexName(gsiNode.get("indexName").asText())
              .hashKey(gsiNode.get("hashKey").asText())
              .projectionType(gsiNode.get("projectionType").asText());

          if (gsiNode.has("sortKey")) {
            builder.sortKey(gsiNode.get("sortKey").asText());
          }

          if (gsiNode.has("nonKeyAttributes")) {
            builder.nonKeyAttributes(gsiNode.get("nonKeyAttributes").asText());
          }

          result.add(builder.build());
        }
      }

      return result;
    } catch (Exception e) {
      log.error("Failed to deserialize GSI list from JSON: {}", json, e);
      return List.of();
    }
  }
}
