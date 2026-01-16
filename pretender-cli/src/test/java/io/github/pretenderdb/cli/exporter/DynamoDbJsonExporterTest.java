package io.github.pretenderdb.cli.exporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.cli.model.ExportManifest;
import io.github.pretenderdb.cli.model.ManifestFile;
import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbMetadata;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Unit tests for DynamoDbJsonExporter.
 */
@ExtendWith(MockitoExtension.class)
class DynamoDbJsonExporterTest {

  @Mock private PdbItemManager pdbItemManager;

  @Mock private PdbTableConverter pdbTableConverter;

  private DynamoDbJsonExporter exporter;
  private ObjectMapper objectMapper;
  private AttributeValueConverter attributeValueConverter;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    objectMapper = new ObjectMapper();
    objectMapper.findAndRegisterModules();
    attributeValueConverter = new AttributeValueConverter(objectMapper);
    exporter = new DynamoDbJsonExporter(objectMapper, attributeValueConverter);
  }

  @Test
  void export_createsManifestFiles() throws Exception {
    // Given
    final String tableName = "TestTable";
    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder()
            .name(tableName)
            .hashKey("id")
            .sortKey(Optional.empty())
            .createDate(Instant.now())
            .build();

    final Map<String, AttributeValue> item =
        Map.of(
            "id", AttributeValue.builder().s("user1").build(),
            "name", AttributeValue.builder().s("Alice").build());

    when(pdbItemManager.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().items(item).lastEvaluatedKey(Map.of()).build());

    // When
    exporter.export(tableName, metadata, tempDir, pdbItemManager, pdbTableConverter);

    // Then
    assertThat(tempDir.resolve("manifest-summary.json")).exists();
    assertThat(tempDir.resolve("manifest-files.json")).exists();
    assertThat(tempDir.resolve("data")).exists().isDirectory();
  }

  @Test
  void export_manifestSummary_containsCorrectMetadata() throws Exception {
    // Given
    final String tableName = "TestTable";
    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder()
            .name(tableName)
            .hashKey("id")
            .sortKey(Optional.of("timestamp"))
            .ttlEnabled(true)
            .ttlAttributeName(Optional.of("expiry"))
            .createDate(Instant.now())
            .build();

    when(pdbItemManager.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().lastEvaluatedKey(Map.of()).build());

    // When
    exporter.export(tableName, metadata, tempDir, pdbItemManager, pdbTableConverter);

    // Then
    final Path manifestPath = tempDir.resolve("manifest-summary.json");
    final ExportManifest manifest =
        objectMapper.readValue(manifestPath.toFile(), ExportManifest.class);

    assertThat(manifest.tableName()).isEqualTo(tableName);
    assertThat(manifest.outputFormat()).isEqualTo("DYNAMODB_JSON");
    assertThat(manifest.exportType()).isEqualTo("FULL_EXPORT");
    assertThat(manifest.itemCount()).isEqualTo(0);

    final ExportManifest.TableSchema schema = manifest.tableSchema();
    assertThat(schema.hashKey()).isEqualTo("id");
    assertThat(schema.sortKey()).isPresent().contains("timestamp");
    assertThat(schema.ttlEnabled()).isTrue();
    assertThat(schema.ttlAttributeName()).isPresent().contains("expiry");
  }

  @Test
  void export_dataFiles_containItemsInDynamoDbJsonFormat() throws Exception {
    // Given
    final String tableName = "TestTable";
    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder().name(tableName).hashKey("id").createDate(Instant.now()).build();

    final Map<String, AttributeValue> item =
        Map.of(
            "id", AttributeValue.builder().s("user1").build(),
            "name", AttributeValue.builder().s("Alice").build(),
            "age", AttributeValue.builder().n("30").build());

    when(pdbItemManager.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().items(item).lastEvaluatedKey(Map.of()).build());

    // When
    exporter.export(tableName, metadata, tempDir, pdbItemManager, pdbTableConverter);

    // Then
    final Path dataDir = tempDir.resolve("data");
    final Path dataFile = dataDir.resolve("item-0001.json.gz");

    assertThat(dataFile).exists();

    // Read and verify gzipped JSON content
    try (final BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(new GZIPInputStream(Files.newInputStream(dataFile))))) {
      final String line = reader.readLine();
      assertThat(line).isNotNull();

      // Parse the line
      @SuppressWarnings("unchecked")
      final Map<String, Object> wrapper = objectMapper.readValue(line, Map.class);

      assertThat(wrapper).containsKey("Item");

      @SuppressWarnings("unchecked")
      final Map<String, Object> itemMap = (Map<String, Object>) wrapper.get("Item");

      // Verify DynamoDB JSON format
      assertThat(itemMap).containsKey("id");
      @SuppressWarnings("unchecked")
      final Map<String, String> idAttr = (Map<String, String>) itemMap.get("id");
      assertThat(idAttr).containsEntry("S", "user1");

      assertThat(itemMap).containsKey("age");
      @SuppressWarnings("unchecked")
      final Map<String, String> ageAttr = (Map<String, String>) itemMap.get("age");
      assertThat(ageAttr).containsEntry("N", "30");
    }
  }

  @Test
  void export_manifestFiles_listsDataFiles() throws Exception {
    // Given
    final String tableName = "TestTable";
    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder().name(tableName).hashKey("id").createDate(Instant.now()).build();

    final Map<String, AttributeValue> item =
        Map.of("id", AttributeValue.builder().s("user1").build());

    when(pdbItemManager.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().items(item).lastEvaluatedKey(Map.of()).build());

    // When
    exporter.export(tableName, metadata, tempDir, pdbItemManager, pdbTableConverter);

    // Then
    final Path manifestFilesPath = tempDir.resolve("manifest-files.json");

    try (final BufferedReader reader = Files.newBufferedReader(manifestFilesPath)) {
      final String line = reader.readLine();
      assertThat(line).isNotNull();

      final ManifestFile manifestFile = objectMapper.readValue(line, ManifestFile.class);
      assertThat(manifestFile.itemCount()).isEqualTo(1);
      assertThat(manifestFile.dataFileS3Key()).startsWith("data/item-");
      assertThat(manifestFile.md5Checksum()).isNotEmpty();
    }
  }
}
