package io.github.pretenderdb.cli.importer;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import io.github.pretenderdb.cli.model.ImportOptions;
import java.io.IOException;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Importer for Amazon Ion format.
 */
public class IonImporter {

  private static final Logger log = LoggerFactory.getLogger(IonImporter.class);
  private static final int MAX_RETRIES = 3;

  private final IonSystem ionSystem;

  /**
   * Constructor.
   */
  public IonImporter(final IonSystem ionSystem) {
    this.ionSystem = ionSystem;
  }

  /**
   * Import data from Ion format.
   */
  public void importData(
      final String tableName,
      final Path inputPath,
      final ImportOptions options,
      final PdbItemManager pdbItemManager,
      final PdbTableManager pdbTableManager,
      final PdbTableConverter pdbTableConverter)
      throws Exception {
    log.info("Importing table '{}' from Ion format", tableName);

    // For Ion import, we look for *.ion.gz files in data/ directory
    final Path dataDir = inputPath.resolve("data");
    if (!Files.exists(dataDir) || !Files.isDirectory(dataDir)) {
      throw new IllegalArgumentException("data directory not found in " + inputPath);
    }

    // Find all .ion.gz files
    final List<Path> dataFiles = new ArrayList<>();
    try (final DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.ion.gz")) {
      for (final Path file : stream) {
        dataFiles.add(file);
      }
    }

    if (dataFiles.isEmpty()) {
      throw new IllegalArgumentException("No .ion.gz files found in " + dataDir);
    }

    log.info("Found {} Ion data files to import", dataFiles.size());

    // Process each file
    long totalItemsImported = 0;
    for (final Path dataFile : dataFiles) {
      final long itemsImported = processIonFile(dataFile, tableName, options, pdbItemManager);
      totalItemsImported += itemsImported;
      log.info("Imported {} items from {}", itemsImported, dataFile.getFileName());
    }

    log.info("Import completed: {} total items imported", totalItemsImported);
  }

  /**
   * Process a single Ion file.
   */
  private long processIonFile(
      final Path filePath,
      final String tableName,
      final ImportOptions options,
      final PdbItemManager pdbItemManager)
      throws Exception {
    log.info("Processing Ion file: {}", filePath.getFileName());

    long itemCount = 0;
    List<WriteRequest> batch = new ArrayList<>();

    try (final InputStream is = new GZIPInputStream(Files.newInputStream(filePath));
        final IonReader reader = ionSystem.newReader(is)) {

      while (reader.next() != null) {
        // Expect top-level struct
        if (reader.getType() != IonType.STRUCT) {
          log.warn("Expected STRUCT at top level, got {}", reader.getType());
          continue;
        }

        reader.stepIn();

        // Look for "Item" field
        while (reader.next() != null) {
          final String fieldName = reader.getFieldName();
          if ("Item".equals(fieldName)) {
            final Map<String, AttributeValue> item = readIonStruct(reader);
            batch.add(
                WriteRequest.builder()
                    .putRequest(PutRequest.builder().item(item).build())
                    .build());

            if (batch.size() >= options.batchSize()) {
              batchWriteWithRetry(tableName, batch, pdbItemManager);
              itemCount += batch.size();
              batch.clear();
            }
          }
        }

        reader.stepOut();
      }

      // Write remaining items
      if (!batch.isEmpty()) {
        batchWriteWithRetry(tableName, batch, pdbItemManager);
        itemCount += batch.size();
      }
    }

    return itemCount;
  }

  /**
   * Read Ion struct as DynamoDB item.
   */
  private Map<String, AttributeValue> readIonStruct(final IonReader reader) throws IOException {
    final Map<String, AttributeValue> result = new HashMap<>();

    if (reader.getType() != IonType.STRUCT) {
      return result;
    }

    reader.stepIn();

    while (reader.next() != null) {
      final String fieldName = reader.getFieldName();
      final AttributeValue value = readIonValue(reader);
      result.put(fieldName, value);
    }

    reader.stepOut();

    return result;
  }

  /**
   * Read Ion value and convert to AttributeValue.
   */
  private AttributeValue readIonValue(final IonReader reader) throws IOException {
    final IonType type = reader.getType();

    if (reader.isNullValue()) {
      return AttributeValue.builder().nul(true).build();
    }

    // Check for type annotations (for sets)
    final String[] annotations = reader.getTypeAnnotations();
    if (annotations.length > 0) {
      return readAnnotatedValue(reader, annotations[0]);
    }

    // Read based on Ion type
    return switch (type) {
      case STRING -> AttributeValue.builder().s(reader.stringValue()).build();
      case DECIMAL, INT, FLOAT -> AttributeValue.builder().n(reader.bigDecimalValue().toString()).build();
      case BOOL -> AttributeValue.builder().bool(reader.booleanValue()).build();
      case BLOB -> AttributeValue.builder().b(SdkBytes.fromByteArray(reader.newBytes())).build();
      case NULL -> AttributeValue.builder().nul(true).build();
      case LIST -> readIonList(reader);
      case STRUCT -> readIonMap(reader);
      default -> {
        log.warn("Unsupported Ion type: {}", type);
        yield AttributeValue.builder().nul(true).build();
      }
    };
  }

  /**
   * Read annotated value (DynamoDB sets).
   */
  private AttributeValue readAnnotatedValue(final IonReader reader, final String annotation)
      throws IOException {
    return switch (annotation) {
      case "$dynamodb_SS" -> {
        final List<String> ss = new ArrayList<>();
        reader.stepIn();
        while (reader.next() != null) {
          ss.add(reader.stringValue());
        }
        reader.stepOut();
        yield AttributeValue.builder().ss(ss).build();
      }
      case "$dynamodb_NS" -> {
        final List<String> ns = new ArrayList<>();
        reader.stepIn();
        while (reader.next() != null) {
          ns.add(reader.bigDecimalValue().toString());
        }
        reader.stepOut();
        yield AttributeValue.builder().ns(ns).build();
      }
      case "$dynamodb_BS" -> {
        final List<SdkBytes> bs = new ArrayList<>();
        reader.stepIn();
        while (reader.next() != null) {
          bs.add(SdkBytes.fromByteArray(reader.newBytes()));
        }
        reader.stepOut();
        yield AttributeValue.builder().bs(bs).build();
      }
      default -> {
        log.warn("Unknown annotation: {}", annotation);
        yield readIonValue(reader);
      }
    };
  }

  /**
   * Read Ion list as DynamoDB List.
   */
  private AttributeValue readIonList(final IonReader reader) throws IOException {
    final List<AttributeValue> list = new ArrayList<>();

    reader.stepIn();

    while (reader.next() != null) {
      list.add(readIonValue(reader));
    }

    reader.stepOut();

    return AttributeValue.builder().l(list).build();
  }

  /**
   * Read Ion struct as DynamoDB Map.
   */
  private AttributeValue readIonMap(final IonReader reader) throws IOException {
    final Map<String, AttributeValue> map = new HashMap<>();

    reader.stepIn();

    while (reader.next() != null) {
      final String fieldName = reader.getFieldName();
      map.put(fieldName, readIonValue(reader));
    }

    reader.stepOut();

    return AttributeValue.builder().m(map).build();
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
