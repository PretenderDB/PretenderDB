package io.github.pretenderdb.cli.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.model.PdbMetadata;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Exporter for CSV format.
 */
public class CsvExporter {

  private static final Logger log = LoggerFactory.getLogger(CsvExporter.class);
  private static final int SCAN_PAGE_SIZE = 1000;

  private final ObjectMapper objectMapper;
  private final AttributeValueConverter attributeValueConverter;

  /**
   * Constructor.
   */
  public CsvExporter(
      final ObjectMapper objectMapper, final AttributeValueConverter attributeValueConverter) {
    this.objectMapper = objectMapper;
    this.attributeValueConverter = attributeValueConverter;
  }

  /**
   * Export table to CSV format.
   */
  public void export(
      final String tableName,
      final PdbMetadata metadata,
      final Path outputPath,
      final PdbItemManager pdbItemManager,
      final PdbTableConverter pdbTableConverter)
      throws Exception {
    log.info("Exporting table '{}' to CSV format", tableName);

    // Determine output paths
    final Path csvFilePath;
    final Path schemaFilePath;

    if (Files.isDirectory(outputPath)) {
      // Output path is a directory
      csvFilePath = outputPath.resolve(tableName + ".csv");
      schemaFilePath = outputPath.resolve(tableName + "-schema.json");
    } else {
      // Output path is a file
      csvFilePath = outputPath;
      schemaFilePath = outputPath.getParent().resolve(tableName + "-schema.json");
    }

    // First pass: collect all unique attribute names
    log.info("Scanning table to collect attribute names...");
    final Set<String> allAttributeNames = new TreeSet<>(); // Sorted set
    final List<Map<String, AttributeValue>> allItems = new ArrayList<>();

    Map<String, AttributeValue> lastEvaluatedKey = null;

    do {
      final ScanRequest.Builder scanBuilder =
          ScanRequest.builder().tableName(tableName).limit(SCAN_PAGE_SIZE);

      if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
        scanBuilder.exclusiveStartKey(lastEvaluatedKey);
      }

      final ScanResponse response = pdbItemManager.scan(scanBuilder.build());

      for (final Map<String, AttributeValue> item : response.items()) {
        allAttributeNames.addAll(item.keySet());
        allItems.add(item);
      }

      lastEvaluatedKey = response.lastEvaluatedKey();
    } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

    log.info("Found {} unique attributes across {} items", allAttributeNames.size(),
        allItems.size());

    // Write CSV file
    writeCsvFile(csvFilePath, allAttributeNames, allItems);

    // Write schema file
    writeSchemaFile(schemaFilePath, metadata);

    log.info("CSV export completed: {} items, {} attributes", allItems.size(),
        allAttributeNames.size());
  }

  /**
   * Write CSV file with all items.
   */
  private void writeCsvFile(
      final Path csvFilePath,
      final Set<String> attributeNames,
      final List<Map<String, AttributeValue>> items)
      throws IOException {
    try (final BufferedWriter writer = Files.newBufferedWriter(csvFilePath);
        final CSVPrinter csvPrinter =
            new CSVPrinter(
                writer,
                CSVFormat.DEFAULT.builder()
                    .setHeader(attributeNames.toArray(new String[0]))
                    .build())) {

      for (final Map<String, AttributeValue> item : items) {
        final List<String> row = new ArrayList<>();

        for (final String attrName : attributeNames) {
          final AttributeValue attrValue = item.get(attrName);

          if (attrValue == null) {
            row.add(""); // Empty cell for missing attribute
          } else {
            row.add(attributeValueToCsvString(attrValue));
          }
        }

        csvPrinter.printRecord(row);
      }
    }

    log.info("Wrote CSV file: {}", csvFilePath);
  }

  /**
   * Convert AttributeValue to CSV cell string.
   */
  private String attributeValueToCsvString(final AttributeValue value) {
    try {
      // String
      if (value.s() != null) {
        return value.s();
      }

      // Number
      if (value.n() != null) {
        return value.n();
      }

      // Boolean
      if (value.bool() != null) {
        return value.bool().toString();
      }

      // Binary (Base64 encoded)
      if (value.b() != null) {
        return Base64.getEncoder().encodeToString(value.b().asByteArray());
      }

      // Null
      if (value.nul() != null && value.nul()) {
        return "";
      }

      // String Set, Number Set, Binary Set, List, Map -> JSON serialize
      if (value.hasSs()) {
        return objectMapper.writeValueAsString(value.ss());
      }

      if (value.hasNs()) {
        return objectMapper.writeValueAsString(value.ns());
      }

      if (value.hasBs()) {
        // Convert binary set to Base64 encoded array
        final List<String> base64List = new ArrayList<>();
        for (final var bytes : value.bs()) {
          base64List.add(Base64.getEncoder().encodeToString(bytes.asByteArray()));
        }
        return objectMapper.writeValueAsString(base64List);
      }

      if (value.hasL()) {
        // Serialize list as JSON
        return objectMapper.writeValueAsString(convertListToPlainObject(value.l()));
      }

      if (value.hasM()) {
        // Serialize map as JSON
        return objectMapper.writeValueAsString(convertMapToPlainObject(value.m()));
      }

      return "";
    } catch (Exception e) {
      log.warn("Error converting attribute value to CSV string: {}", e.getMessage());
      return "";
    }
  }

  /**
   * Convert DynamoDB list to plain Java list for JSON serialization.
   */
  private List<Object> convertListToPlainObject(final List<AttributeValue> list) {
    final List<Object> result = new ArrayList<>();
    for (final AttributeValue item : list) {
      result.add(convertAttributeValueToPlainObject(item));
    }
    return result;
  }

  /**
   * Convert DynamoDB map to plain Java map for JSON serialization.
   */
  private Map<String, Object> convertMapToPlainObject(final Map<String, AttributeValue> map) {
    final Map<String, Object> result = new java.util.HashMap<>();
    for (final Map.Entry<String, AttributeValue> entry : map.entrySet()) {
      result.put(entry.getKey(), convertAttributeValueToPlainObject(entry.getValue()));
    }
    return result;
  }

  /**
   * Convert AttributeValue to plain Java object for JSON serialization.
   */
  private Object convertAttributeValueToPlainObject(final AttributeValue value) {
    if (value.s() != null) {
      return value.s();
    }
    if (value.n() != null) {
      return value.n();
    }
    if (value.bool() != null) {
      return value.bool();
    }
    if (value.b() != null) {
      return Base64.getEncoder().encodeToString(value.b().asByteArray());
    }
    if (value.nul() != null && value.nul()) {
      return null;
    }
    if (value.hasSs()) {
      return value.ss();
    }
    if (value.hasNs()) {
      return value.ns();
    }
    if (value.hasBs()) {
      final List<String> base64List = new ArrayList<>();
      for (final var bytes : value.bs()) {
        base64List.add(Base64.getEncoder().encodeToString(bytes.asByteArray()));
      }
      return base64List;
    }
    if (value.hasL()) {
      return convertListToPlainObject(value.l());
    }
    if (value.hasM()) {
      return convertMapToPlainObject(value.m());
    }
    return null;
  }

  /**
   * Write schema file.
   */
  private void writeSchemaFile(final Path schemaFilePath, final PdbMetadata metadata)
      throws IOException {
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(schemaFilePath.toFile(), metadata);
    log.info("Wrote schema file: {}", schemaFilePath);
  }
}
