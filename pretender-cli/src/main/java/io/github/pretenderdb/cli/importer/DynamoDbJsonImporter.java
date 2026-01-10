package io.github.pretenderdb.cli.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.cli.model.ExportManifest;
import io.github.pretenderdb.cli.model.ImportOptions;
import io.github.pretenderdb.cli.model.ManifestFile;
import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import io.github.pretenderdb.model.PdbMetadata;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Importer for DynamoDB JSON format (AWS S3 Export compatible).
 */
public class DynamoDbJsonImporter {

  private static final Logger log = LoggerFactory.getLogger(DynamoDbJsonImporter.class);
  private static final int MAX_RETRIES = 3;

  private final ObjectMapper objectMapper;
  private final AttributeValueConverter attributeValueConverter;

  /**
   * Constructor.
   */
  public DynamoDbJsonImporter(
      final ObjectMapper objectMapper, final AttributeValueConverter attributeValueConverter) {
    this.objectMapper = objectMapper;
    this.attributeValueConverter = attributeValueConverter;
  }

  /**
   * Import data from DynamoDB JSON format.
   */
  public void importData(
      final String tableName,
      final Path inputPath,
      final ImportOptions options,
      final PdbItemManager pdbItemManager,
      final PdbTableManager pdbTableManager,
      final PdbTableConverter pdbTableConverter)
      throws Exception {
    log.info("Importing table '{}' from DynamoDB JSON format", tableName);

    // Read manifest-summary.json
    final Path manifestPath = inputPath.resolve("manifest-summary.json");
    if (!Files.exists(manifestPath)) {
      throw new IllegalArgumentException("manifest-summary.json not found in " + inputPath);
    }

    final ExportManifest manifest =
        objectMapper.readValue(manifestPath.toFile(), ExportManifest.class);
    log.info("Found export with {} items", manifest.itemCount());

    // Create table if requested
    if (options.createTable()) {
      createTableFromSchema(tableName, manifest, pdbTableManager, pdbTableConverter);
    }

    // Read manifest-files.json
    final Path manifestFilesPath = inputPath.resolve("manifest-files.json");
    if (!Files.exists(manifestFilesPath)) {
      throw new IllegalArgumentException("manifest-files.json not found in " + inputPath);
    }

    final List<ManifestFile> manifestFiles = readManifestFiles(manifestFilesPath);
    log.info("Found {} data files to import", manifestFiles.size());

    // Process each data file
    long totalItemsImported = 0;
    for (final ManifestFile manifestFile : manifestFiles) {
      final Path dataFilePath = inputPath.resolve(manifestFile.dataFileS3Key());
      final long itemsImported = processDataFile(dataFilePath, tableName, options, pdbItemManager);
      totalItemsImported += itemsImported;
      log.info("Imported {} items from {}", itemsImported, dataFilePath.getFileName());
    }

    log.info("Import completed: {} total items imported", totalItemsImported);
  }

  /**
   * Create table from manifest schema.
   */
  private void createTableFromSchema(
      final String tableName,
      final ExportManifest manifest,
      final PdbTableManager pdbTableManager,
      final PdbTableConverter pdbTableConverter)
      throws Exception {
    log.info("Creating table '{}' from schema", tableName);

    final ExportManifest.TableSchema schema = manifest.tableSchema();

    // Build key schema
    final List<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(
        KeySchemaElement.builder()
            .attributeName(schema.hashKey())
            .keyType(KeyType.HASH)
            .build());

    if (schema.sortKey().isPresent()) {
      keySchema.add(
          KeySchemaElement.builder()
              .attributeName(schema.sortKey().get())
              .keyType(KeyType.RANGE)
              .build());
    }

    // Build attribute definitions (key attributes only for now)
    final List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(
        AttributeDefinition.builder()
            .attributeName(schema.hashKey())
            .attributeType(ScalarAttributeType.S)
            .build());

    if (schema.sortKey().isPresent()) {
      attributeDefinitions.add(
          AttributeDefinition.builder()
              .attributeName(schema.sortKey().get())
              .attributeType(ScalarAttributeType.S)
              .build());
    }

    // Build GSIs
    final List<GlobalSecondaryIndex> gsis = new ArrayList<>();
    for (final ExportManifest.GlobalSecondaryIndex gsiDef : schema.globalSecondaryIndexes()) {
      final List<KeySchemaElement> gsiKeySchema = new ArrayList<>();
      gsiKeySchema.add(
          KeySchemaElement.builder()
              .attributeName(gsiDef.hashKey())
              .keyType(KeyType.HASH)
              .build());

      if (gsiDef.sortKey().isPresent()) {
        gsiKeySchema.add(
            KeySchemaElement.builder()
                .attributeName(gsiDef.sortKey().get())
                .keyType(KeyType.RANGE)
                .build());
      }

      // Add GSI key attributes to attribute definitions if not already present
      if (attributeDefinitions.stream()
          .noneMatch(ad -> ad.attributeName().equals(gsiDef.hashKey()))) {
        attributeDefinitions.add(
            AttributeDefinition.builder()
                .attributeName(gsiDef.hashKey())
                .attributeType(ScalarAttributeType.S)
                .build());
      }

      if (gsiDef.sortKey().isPresent()
          && attributeDefinitions.stream()
              .noneMatch(ad -> ad.attributeName().equals(gsiDef.sortKey().get()))) {
        attributeDefinitions.add(
            AttributeDefinition.builder()
                .attributeName(gsiDef.sortKey().get())
                .attributeType(ScalarAttributeType.S)
                .build());
      }

      final Projection projection =
          Projection.builder()
              .projectionType(ProjectionType.fromValue(gsiDef.projectionType()))
              .nonKeyAttributes(
                  gsiDef.nonKeyAttributes().isEmpty() ? null : gsiDef.nonKeyAttributes())
              .build();

      gsis.add(
          GlobalSecondaryIndex.builder()
              .indexName(gsiDef.indexName())
              .keySchema(gsiKeySchema)
              .projection(projection)
              .build());
    }

    // Build create table request
    final CreateTableRequest.Builder createBuilder =
        CreateTableRequest.builder()
            .tableName(tableName)
            .keySchema(keySchema)
            .attributeDefinitions(attributeDefinitions);

    if (!gsis.isEmpty()) {
      createBuilder.globalSecondaryIndexes(gsis);
    }

    final CreateTableRequest createRequest = createBuilder.build();

    // Convert to PdbMetadata and create table
    final PdbMetadata metadata = pdbTableConverter.convert(createRequest);
    pdbTableManager.insertPdbTable(metadata);

    log.info("Table '{}' created successfully", tableName);
  }

  /**
   * Read manifest-files.json (JSON Lines format).
   */
  private List<ManifestFile> readManifestFiles(final Path path) throws IOException {
    final List<ManifestFile> manifestFiles = new ArrayList<>();

    try (final BufferedReader reader = Files.newBufferedReader(path)) {
      String line;
      while ((line = reader.readLine()) != null) {
        final ManifestFile mf = objectMapper.readValue(line, ManifestFile.class);
        manifestFiles.add(mf);
      }
    }

    return manifestFiles;
  }

  /**
   * Process a single data file.
   */
  private long processDataFile(
      final Path filePath,
      final String tableName,
      final ImportOptions options,
      final PdbItemManager pdbItemManager)
      throws Exception {
    log.info("Processing data file: {}", filePath.getFileName());

    long itemCount = 0;
    List<WriteRequest> batch = new ArrayList<>();

    try (final BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(new GZIPInputStream(Files.newInputStream(filePath))))) {

      String line;
      while ((line = reader.readLine()) != null) {
        // Parse {"Item": {...}}
        @SuppressWarnings("unchecked")
        final Map<String, Object> wrapper = objectMapper.readValue(line, Map.class);
        final Object itemObj = wrapper.get("Item");

        if (itemObj == null) {
          log.warn("Skipping line without 'Item' key: {}", line);
          continue;
        }

        // Convert item JSON to AttributeValue map
        final String itemJson = objectMapper.writeValueAsString(itemObj);
        final Map<String, AttributeValue> item = attributeValueConverter.fromJson(itemJson);

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

    return itemCount;
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
          // Exponential backoff
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
