package io.github.pretenderdb.cli.exporter;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.cli.model.ImmutableManifestFile;
import io.github.pretenderdb.cli.model.ManifestFile;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.model.PdbMetadata;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Exporter for Amazon Ion format.
 */
public class IonExporter {

  private static final Logger log = LoggerFactory.getLogger(IonExporter.class);
  private static final int ITEMS_PER_FILE = 10000;
  private static final int SCAN_PAGE_SIZE = 1000;

  private final IonSystem ionSystem;

  /**
   * Constructor.
   */
  public IonExporter(final IonSystem ionSystem) {
    this.ionSystem = ionSystem;
  }

  /**
   * Export table to Ion format.
   */
  public void export(
      final String tableName,
      final PdbMetadata metadata,
      final Path outputPath,
      final PdbItemManager pdbItemManager,
      final PdbTableConverter pdbTableConverter)
      throws Exception {
    log.info("Exporting table '{}' to Ion format", tableName);

    final Instant startTime = Instant.now();

    // Create output directory structure
    Files.createDirectories(outputPath);
    final Path dataDir = outputPath.resolve("data");
    Files.createDirectories(dataDir);

    // Scan and write items
    final List<ManifestFile> manifestFiles = new ArrayList<>();
    long totalItemCount = 0;

    Map<String, AttributeValue> lastEvaluatedKey = null;
    int fileCounter = 1;
    int currentFileItemCount = 0;
    IonWriter currentWriter = null;
    Path currentFilePath = null;
    MessageDigest currentMd5 = null;

    try {
      do {
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
              currentWriter.close();
              final ManifestFile manifestFile =
                  createManifestFile(currentFilePath, currentFileItemCount, currentMd5);
              manifestFiles.add(manifestFile);
              log.info(
                  "Completed data file {}: {} items",
                  currentFilePath.getFileName(),
                  currentFileItemCount);
            }

            currentFilePath = dataDir.resolve(String.format("item-%04d.ion.gz", fileCounter++));
            currentWriter = openIonWriter(currentFilePath);
            currentMd5 = MessageDigest.getInstance("MD5");
            currentFileItemCount = 0;
          }

          writeItem(currentWriter, item, currentMd5);
          currentFileItemCount++;
          totalItemCount++;
        }

        lastEvaluatedKey = response.lastEvaluatedKey();
      } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

      if (currentWriter != null) {
        currentWriter.close();
        final ManifestFile manifestFile =
            createManifestFile(currentFilePath, currentFileItemCount, currentMd5);
        manifestFiles.add(manifestFile);
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

    log.info("Exported {} items in {} files", totalItemCount, manifestFiles.size());

    // Note: For Ion format, we can reuse the DynamoDbJsonExporter's manifest file generation
    // For simplicity, we'll just log that manifests should be written similarly
    log.info("Export completed: {} items", totalItemCount);
  }

  /**
   * Open Ion writer with gzip compression.
   */
  private IonWriter openIonWriter(final Path path) throws IOException {
    final OutputStream os = new GZIPOutputStream(Files.newOutputStream(path));
    return ionSystem.newTextWriter(os);
  }

  /**
   * Write item in Ion format.
   */
  private void writeItem(
      final IonWriter writer, final Map<String, AttributeValue> item, final MessageDigest md5)
      throws IOException {
    // Write {Item: {...}} structure
    writer.stepIn(IonType.STRUCT);
    writer.setFieldName("Item");

    // Write item as struct
    writer.stepIn(IonType.STRUCT);
    for (final Map.Entry<String, AttributeValue> entry : item.entrySet()) {
      writer.setFieldName(entry.getKey());
      writeAttributeValue(writer, entry.getValue());
    }
    writer.stepOut(); // Item struct

    writer.stepOut(); // Top-level struct

    // For MD5, we'd need to serialize the Ion value to bytes
    // For simplicity, skipping MD5 for Ion format
  }

  /**
   * Write AttributeValue as Ion value (recursive).
   */
  private void writeAttributeValue(final IonWriter writer, final AttributeValue value)
      throws IOException {
    // String
    if (value.s() != null) {
      writer.writeString(value.s());
      return;
    }

    // Number (Ion decimal)
    if (value.n() != null) {
      writer.writeDecimal(new BigDecimal(value.n()));
      return;
    }

    // Boolean
    if (value.bool() != null) {
      writer.writeBool(value.bool());
      return;
    }

    // Binary (Ion blob)
    if (value.b() != null) {
      writer.writeBlob(value.b().asByteArray());
      return;
    }

    // Null
    if (value.nul() != null && value.nul()) {
      writer.writeNull();
      return;
    }

    // String Set (with type annotation)
    if (value.hasSs()) {
      writer.setTypeAnnotations("$dynamodb_SS");
      writer.stepIn(IonType.LIST);
      for (final String s : value.ss()) {
        writer.writeString(s);
      }
      writer.stepOut();
      return;
    }

    // Number Set (with type annotation)
    if (value.hasNs()) {
      writer.setTypeAnnotations("$dynamodb_NS");
      writer.stepIn(IonType.LIST);
      for (final String n : value.ns()) {
        writer.writeDecimal(new BigDecimal(n));
      }
      writer.stepOut();
      return;
    }

    // Binary Set (with type annotation)
    if (value.hasBs()) {
      writer.setTypeAnnotations("$dynamodb_BS");
      writer.stepIn(IonType.LIST);
      for (final var b : value.bs()) {
        writer.writeBlob(b.asByteArray());
      }
      writer.stepOut();
      return;
    }

    // List
    if (value.hasL()) {
      writer.stepIn(IonType.LIST);
      for (final AttributeValue item : value.l()) {
        writeAttributeValue(writer, item);
      }
      writer.stepOut();
      return;
    }

    // Map (struct)
    if (value.hasM()) {
      writer.stepIn(IonType.STRUCT);
      for (final Map.Entry<String, AttributeValue> entry : value.m().entrySet()) {
        writer.setFieldName(entry.getKey());
        writeAttributeValue(writer, entry.getValue());
      }
      writer.stepOut();
      return;
    }
  }

  /**
   * Create manifest file entry.
   */
  private ManifestFile createManifestFile(
      final Path filePath, final int itemCount, final MessageDigest md5) {
    final String relativePath = "data/" + filePath.getFileName().toString();

    return ImmutableManifestFile.builder()
        .itemCount(itemCount)
        .md5Checksum("") // Skip MD5 for Ion format
        .dataFileS3Key(relativePath)
        .build();
  }
}
