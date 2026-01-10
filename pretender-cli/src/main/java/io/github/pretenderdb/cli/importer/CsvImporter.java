package io.github.pretenderdb.cli.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.cli.model.ImportOptions;
import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import io.github.pretenderdb.model.PdbMetadata;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Importer for CSV format.
 */
public class CsvImporter {

  private static final Logger log = LoggerFactory.getLogger(CsvImporter.class);
  private static final int MAX_RETRIES = 3;

  private final ObjectMapper objectMapper;
  private final AttributeValueConverter attributeValueConverter;

  /**
   * Constructor.
   */
  public CsvImporter(
      final ObjectMapper objectMapper, final AttributeValueConverter attributeValueConverter) {
    this.objectMapper = objectMapper;
    this.attributeValueConverter = attributeValueConverter;
  }

  /**
   * Import data from CSV format.
   */
  public void importData(
      final String tableName,
      final Path inputPath,
      final ImportOptions options,
      final PdbItemManager pdbItemManager,
      final PdbTableManager pdbTableManager,
      final PdbTableConverter pdbTableConverter)
      throws Exception {
    log.info("Importing table '{}' from CSV format", tableName);

    // Determine CSV and schema file paths
    final Path csvFilePath;
    final Path schemaFilePath;

    if (Files.isDirectory(inputPath)) {
      csvFilePath = inputPath.resolve(tableName + ".csv");
      schemaFilePath = inputPath.resolve(tableName + "-schema.json");
    } else {
      csvFilePath = inputPath;
      schemaFilePath = inputPath.getParent().resolve(tableName + "-schema.json");
    }

    if (!Files.exists(csvFilePath)) {
      throw new IllegalArgumentException("CSV file not found: " + csvFilePath);
    }

    // Read schema if creating table
    if (options.createTable()) {
      if (!Files.exists(schemaFilePath)) {
        throw new IllegalArgumentException("Schema file not found: " + schemaFilePath);
      }

      final PdbMetadata metadata =
          objectMapper.readValue(schemaFilePath.toFile(), PdbMetadata.class);
      log.info("Creating table '{}' from schema", tableName);
      pdbTableManager.insertPdbTable(metadata);
    }

    // Read and import CSV
    long itemCount = 0;
    List<WriteRequest> batch = new ArrayList<>();

    try (final Reader reader = Files.newBufferedReader(csvFilePath);
        final CSVParser csvParser =
            new CSVParser(
                reader,
                CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setIgnoreHeaderCase(true)
                    .setTrim(true)
                    .build())) {

      for (final CSVRecord record : csvParser) {
        final Map<String, AttributeValue> item = new HashMap<>();

        for (final String columnName : csvParser.getHeaderNames()) {
          final String cellValue = record.get(columnName);

          if (cellValue == null || cellValue.isEmpty()) {
            continue; // Skip null/empty attributes
          }

          final AttributeValue attributeValue = parseAttributeValue(cellValue);
          item.put(columnName, attributeValue);
        }

        // Add to batch
        batch.add(
            WriteRequest.builder()
                .putRequest(PutRequest.builder().item(item).build())
                .build());

        // Write batch when full
        if (batch.size() >= options.batchSize()) {
          batchWriteWithRetry(tableName, batch, pdbItemManager);
          itemCount += batch.size();
          batch.clear();
        }
      }

      // Write remaining items
      if (!batch.isEmpty()) {
        batchWriteWithRetry(tableName, batch, pdbItemManager);
        itemCount += batch.size();
      }
    }

    log.info("CSV import completed: {} items imported", itemCount);
  }

  /**
   * Parse CSV cell value to AttributeValue (heuristic-based type detection).
   */
  private AttributeValue parseAttributeValue(final String cellValue) {
    // 1. Check if it's a JSON object/array (Map/List)
    if (cellValue.startsWith("{") || cellValue.startsWith("[")) {
      try {
        final Object parsed = objectMapper.readValue(cellValue, Object.class);

        if (parsed instanceof Map) {
          @SuppressWarnings("unchecked")
          final Map<String, Object> map = (Map<String, Object>) parsed;
          return convertMapToAttributeValue(map);
        } else if (parsed instanceof List) {
          @SuppressWarnings("unchecked")
          final List<Object> list = (List<Object>) parsed;
          return convertListToAttributeValue(list);
        }
      } catch (Exception e) {
        // Not JSON, treat as string
      }
    }

    // 2. Try parsing as number
    try {
      new BigDecimal(cellValue);
      return AttributeValue.builder().n(cellValue).build();
    } catch (NumberFormatException e) {
      // Not a number
    }

    // 3. Try parsing as boolean
    if ("true".equalsIgnoreCase(cellValue) || "false".equalsIgnoreCase(cellValue)) {
      return AttributeValue.builder().bool(Boolean.parseBoolean(cellValue)).build();
    }

    // 4. Default to string
    return AttributeValue.builder().s(cellValue).build();
  }

  /**
   * Convert plain map to DynamoDB Map AttributeValue.
   */
  @SuppressWarnings("unchecked")
  private AttributeValue convertMapToAttributeValue(final Map<String, Object> map) {
    final Map<String, AttributeValue> attributeMap = new HashMap<>();

    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      attributeMap.put(entry.getKey(), convertObjectToAttributeValue(entry.getValue()));
    }

    return AttributeValue.builder().m(attributeMap).build();
  }

  /**
   * Convert plain list to DynamoDB List AttributeValue.
   */
  @SuppressWarnings("unchecked")
  private AttributeValue convertListToAttributeValue(final List<Object> list) {
    final List<AttributeValue> attributeList = new ArrayList<>();

    for (final Object item : list) {
      attributeList.add(convertObjectToAttributeValue(item));
    }

    return AttributeValue.builder().l(attributeList).build();
  }

  /**
   * Convert plain Java object to AttributeValue.
   */
  @SuppressWarnings("unchecked")
  private AttributeValue convertObjectToAttributeValue(final Object obj) {
    if (obj == null) {
      return AttributeValue.builder().nul(true).build();
    }

    if (obj instanceof String) {
      final String str = (String) obj;
      // Try to detect Base64-encoded binary
      try {
        if (str.length() > 0 && str.matches("^[A-Za-z0-9+/]+={0,2}$") && str.length() % 4 == 0) {
          // Might be Base64, but we'll treat as string for safety
        }
      } catch (Exception e) {
        // Ignore
      }
      return AttributeValue.builder().s(str).build();
    }

    if (obj instanceof Number) {
      return AttributeValue.builder().n(obj.toString()).build();
    }

    if (obj instanceof Boolean) {
      return AttributeValue.builder().bool((Boolean) obj).build();
    }

    if (obj instanceof Map) {
      return convertMapToAttributeValue((Map<String, Object>) obj);
    }

    if (obj instanceof List) {
      return convertListToAttributeValue((List<Object>) obj);
    }

    // Default: convert to string
    return AttributeValue.builder().s(obj.toString()).build();
  }

  /**
   * Batch write items with retry logic.
   */
  private void batchWriteWithRetry(
      final String tableName,
      final List<WriteRequest> requests,
      final PdbItemManager pdbItemManager)
      throws Exception {
    List<WriteRequest> unprocessedItems = new ArrayList<>(requests);
    int retryCount = 0;

    while (!unprocessedItems.isEmpty() && retryCount < MAX_RETRIES) {
      final BatchWriteItemRequest request =
          BatchWriteItemRequest.builder()
              .requestItems(Map.of(tableName, unprocessedItems))
              .build();

      final BatchWriteItemResponse response = pdbItemManager.batchWriteItem(request);

      // Check for unprocessed items
      if (response.hasUnprocessedItems()
          && response.unprocessedItems().containsKey(tableName)) {
        unprocessedItems = response.unprocessedItems().get(tableName);
        retryCount++;

        if (!unprocessedItems.isEmpty()) {
          log.warn(
              "Batch write has {} unprocessed items, retrying ({}/{})",
              unprocessedItems.size(),
              retryCount,
              MAX_RETRIES);
          Thread.sleep((long) Math.pow(2, retryCount) * 100);
        }
      } else {
        unprocessedItems = List.of();
      }
    }

    if (!unprocessedItems.isEmpty()) {
      log.error("Failed to write {} items after {} retries", unprocessedItems.size(), MAX_RETRIES);
      throw new RuntimeException(
          "Failed to write " + unprocessedItems.size() + " items after retries");
    }
  }
}
