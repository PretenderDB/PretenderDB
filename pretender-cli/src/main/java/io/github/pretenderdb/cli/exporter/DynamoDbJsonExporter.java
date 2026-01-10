package io.github.pretenderdb.cli.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.cli.model.ExportManifest;
import io.github.pretenderdb.cli.model.ExportManifest.GlobalSecondaryIndex;
import io.github.pretenderdb.cli.model.ImmutableExportManifest;
import io.github.pretenderdb.cli.model.ImmutableGlobalSecondaryIndex;
import io.github.pretenderdb.cli.model.ImmutableManifestFile;
import io.github.pretenderdb.cli.model.ImmutableTableSchema;
import io.github.pretenderdb.cli.model.ManifestFile;
import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.model.PdbMetadata;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Exporter for DynamoDB JSON format (AWS S3 Export compatible).
 */
public class DynamoDbJsonExporter {

  private static final Logger log = LoggerFactory.getLogger(DynamoDbJsonExporter.class);
  private static final int ITEMS_PER_FILE = 10000;
  private static final int SCAN_PAGE_SIZE = 1000;

  private final ObjectMapper objectMapper;
  private final AttributeValueConverter attributeValueConverter;

  /**
   * Constructor.
   */
  public DynamoDbJsonExporter(
      final ObjectMapper objectMapper, final AttributeValueConverter attributeValueConverter) {
    this.objectMapper = objectMapper;
    this.attributeValueConverter = attributeValueConverter;
  }

  /**
   * Export table to DynamoDB JSON format.
   */
  public void export(
      final String tableName,
      final PdbMetadata metadata,
      final Path outputPath,
      final PdbItemManager pdbItemManager,
      final PdbTableConverter pdbTableConverter)
      throws Exception {
    log.info("Exporting table '{}' to DynamoDB JSON format", tableName);

    final Instant startTime = Instant.now();

    // Create output directory structure
    Files.createDirectories(outputPath);
    final Path dataDir = outputPath.resolve("data");
    Files.createDirectories(dataDir);

    // Scan and write items
    final List<ManifestFile> manifestFiles = new ArrayList<>();
    long totalItemCount = 0;
    long totalSizeBytes = 0;

    Map<String, AttributeValue> lastEvaluatedKey = null;
    int fileCounter = 1;
    int currentFileItemCount = 0;
    BufferedWriter currentWriter = null;
    Path currentFilePath = null;
    MessageDigest currentMd5 = null;

    try {
      do {
        // Build scan request
        final ScanRequest.Builder scanBuilder =
            ScanRequest.builder().tableName(tableName).limit(SCAN_PAGE_SIZE);

        if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
          scanBuilder.exclusiveStartKey(lastEvaluatedKey);
        }

        final ScanResponse response = pdbItemManager.scan(scanBuilder.build());

        for (final Map<String, AttributeValue> item : response.items()) {
          // Open new file if needed
          if (currentWriter == null || currentFileItemCount >= ITEMS_PER_FILE) {
            if (currentWriter != null) {
              // Close previous file and add to manifest
              currentWriter.close();
              final ManifestFile manifestFile = createManifestFile(
                  currentFilePath, currentFileItemCount, currentMd5);
              manifestFiles.add(manifestFile);
              log.info("Completed data file {}: {} items", currentFilePath.getFileName(),
                  currentFileItemCount);
            }

            // Open new file
            currentFilePath = dataDir.resolve(String.format("item-%04d.json.gz", fileCounter++));
            currentWriter = openGzipWriter(currentFilePath);
            currentMd5 = MessageDigest.getInstance("MD5");
            currentFileItemCount = 0;
          }

          // Write item in DynamoDB JSON format
          writeItem(currentWriter, item, currentMd5);
          currentFileItemCount++;
          totalItemCount++;
        }

        lastEvaluatedKey = response.lastEvaluatedKey();
      } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

      // Close final file
      if (currentWriter != null) {
        currentWriter.close();
        final ManifestFile manifestFile = createManifestFile(
            currentFilePath, currentFileItemCount, currentMd5);
        manifestFiles.add(manifestFile);
        log.info("Completed data file {}: {} items", currentFilePath.getFileName(),
            currentFileItemCount);
      }

    } finally {
      if (currentWriter != null) {
        try {
          currentWriter.close();
        } catch (IOException e) {
          log.warn("Error closing writer", e);
        }
      }
    }

    final Instant endTime = Instant.now();

    // Calculate total size
    for (final ManifestFile mf : manifestFiles) {
      final Path filePath = outputPath.resolve(mf.dataFileS3Key());
      totalSizeBytes += Files.size(filePath);
    }

    log.info("Exported {} items in {} files", totalItemCount, manifestFiles.size());

    // Write manifest-files.json
    writeManifestFiles(outputPath.resolve("manifest-files.json"), manifestFiles);

    // Write manifest-summary.json
    writeManifestSummary(outputPath.resolve("manifest-summary.json"), tableName, metadata,
        startTime, endTime, totalItemCount, totalSizeBytes);

    log.info("Export completed: {} items, {} bytes", totalItemCount, totalSizeBytes);
  }

  /**
   * Open a gzip writer.
   */
  private BufferedWriter openGzipWriter(final Path path) throws IOException {
    return new BufferedWriter(
        new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(path))));
  }

  /**
   * Write an item in DynamoDB JSON format.
   */
  private void writeItem(
      final BufferedWriter writer,
      final Map<String, AttributeValue> item,
      final MessageDigest md5)
      throws IOException {
    // Convert item to JSON using AttributeValueConverter
    final String itemJson = attributeValueConverter.toJson(item);

    // Wrap in {"Item": {...}} format
    final String wrappedJson = "{\"Item\":" + itemJson + "}";

    // Write to file
    writer.write(wrappedJson);
    writer.newLine();

    // Update MD5
    md5.update(wrappedJson.getBytes());
    md5.update((byte) '\n');
  }

  /**
   * Create manifest file entry with MD5 checksum.
   */
  private ManifestFile createManifestFile(
      final Path filePath, final int itemCount, final MessageDigest md5) {
    final String md5Hex = bytesToHex(md5.digest());
    final String relativePath = "data/" + filePath.getFileName().toString();

    return ImmutableManifestFile.builder()
        .itemCount(itemCount)
        .md5Checksum(md5Hex)
        .dataFileS3Key(relativePath)
        .build();
  }

  /**
   * Convert bytes to hex string.
   */
  private String bytesToHex(final byte[] bytes) {
    final StringBuilder sb = new StringBuilder();
    for (final byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  /**
   * Write manifest-files.json (JSON Lines format).
   */
  private void writeManifestFiles(final Path path, final List<ManifestFile> manifestFiles)
      throws IOException {
    try (final BufferedWriter writer = Files.newBufferedWriter(path)) {
      for (final ManifestFile mf : manifestFiles) {
        writer.write(objectMapper.writeValueAsString(mf));
        writer.newLine();
      }
    }
    log.info("Wrote manifest-files.json with {} entries", manifestFiles.size());
  }

  /**
   * Write manifest-summary.json.
   */
  private void writeManifestSummary(
      final Path path,
      final String tableName,
      final PdbMetadata metadata,
      final Instant startTime,
      final Instant endTime,
      final long itemCount,
      final long sizeBytes)
      throws IOException {
    // Build table schema
    final List<GlobalSecondaryIndex> gsis = new ArrayList<>();
    for (final var gsi : metadata.globalSecondaryIndexes()) {
      // nonKeyAttributes stored as Optional<String> in PdbMetadata, parse if present
      final List<String> nonKeyAttrs = gsi.nonKeyAttributes()
          .map(s -> List.of(s.split(",")))
          .orElse(List.of());

      gsis.add(
          ImmutableGlobalSecondaryIndex.builder()
              .indexName(gsi.indexName())
              .hashKey(gsi.hashKey())
              .sortKey(gsi.sortKey())
              .projectionType(gsi.projectionType().toString())
              .addAllNonKeyAttributes(nonKeyAttrs)
              .build());
    }

    final ExportManifest.TableSchema tableSchema =
        ImmutableTableSchema.builder()
            .hashKey(metadata.hashKey())
            .sortKey(metadata.sortKey())
            .addAllGlobalSecondaryIndexes(gsis)
            .ttlAttributeName(metadata.ttlAttributeName())
            .ttlEnabled(metadata.ttlEnabled())
            .streamEnabled(metadata.streamEnabled())
            .streamViewType(metadata.streamViewType())
            .build();

    // Build export manifest
    final ExportManifest manifest =
        ImmutableExportManifest.builder()
            .version("2023-08-01")
            .exportArn("arn:pretenderdb:export:local:table/" + tableName + "/export/" + startTime.getEpochSecond())
            .startTime(startTime)
            .endTime(endTime)
            .tableArn("arn:pretenderdb:table:local:" + tableName)
            .tableName(tableName)
            .s3Bucket("local")
            .s3Prefix("pretenderdb-export")
            .billedSizeBytes(sizeBytes)
            .itemCount(itemCount)
            .outputFormat("DYNAMODB_JSON")
            .exportType("FULL_EXPORT")
            .tableSchema(tableSchema)
            .build();

    // Write manifest
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), manifest);
    log.info("Wrote manifest-summary.json");
  }
}
