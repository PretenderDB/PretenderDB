package io.github.pretenderdb.cli.exporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbMetadata;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 * Unit tests for CsvExporter.
 */
@ExtendWith(MockitoExtension.class)
class CsvExporterTest {

  @Mock private PdbItemManager pdbItemManager;

  @Mock private PdbTableConverter pdbTableConverter;

  private CsvExporter csvExporter;
  private ObjectMapper objectMapper;
  private AttributeValueConverter attributeValueConverter;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    objectMapper = new ObjectMapper();
    objectMapper.findAndRegisterModules();
    attributeValueConverter = new AttributeValueConverter(objectMapper);
    csvExporter = new CsvExporter(objectMapper, attributeValueConverter);
  }

  @Test
  void export_withSimpleItems_createsValidCsv() throws Exception {
    // Given
    final String tableName = "TestTable";
    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder()
            .name(tableName)
            .hashKey("id")
            .sortKey(Optional.empty())
            .createDate(Instant.now())
            .build();

    final Map<String, AttributeValue> item1 =
        Map.of(
            "id", AttributeValue.builder().s("user1").build(),
            "name", AttributeValue.builder().s("Alice").build(),
            "age", AttributeValue.builder().n("30").build());

    final Map<String, AttributeValue> item2 =
        Map.of(
            "id", AttributeValue.builder().s("user2").build(),
            "name", AttributeValue.builder().s("Bob").build(),
            "age", AttributeValue.builder().n("25").build());

    when(pdbItemManager.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(item1, item2)
                .lastEvaluatedKey(Map.of())
                .build());

    // When
    csvExporter.export(tableName, metadata, tempDir, pdbItemManager, pdbTableConverter);

    // Then
    final Path csvFile = tempDir.resolve(tableName + ".csv");
    final Path schemaFile = tempDir.resolve(tableName + "-schema.json");

    assertThat(csvFile).exists();
    assertThat(schemaFile).exists();

    final String csvContent = Files.readString(csvFile);
    assertThat(csvContent).contains("age,id,name"); // Header row (sorted)
    assertThat(csvContent).contains("30,user1,Alice");
    assertThat(csvContent).contains("25,user2,Bob");

    final PdbMetadata readSchema = objectMapper.readValue(schemaFile.toFile(), PdbMetadata.class);
    assertThat(readSchema.name()).isEqualTo(tableName);
    assertThat(readSchema.hashKey()).isEqualTo("id");
  }

  @Test
  void export_withComplexTypes_serializesAsJson() throws Exception {
    // Given
    final String tableName = "TestTable";
    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder().name(tableName).hashKey("id").createDate(Instant.now()).build();

    final Map<String, AttributeValue> item =
        Map.of(
            "id",
            AttributeValue.builder().s("user1").build(),
            "address",
            AttributeValue.builder()
                .m(
                    Map.of(
                        "city", AttributeValue.builder().s("NYC").build(),
                        "zip", AttributeValue.builder().s("10001").build()))
                .build(),
            "tags",
            AttributeValue.builder()
                .l(
                    AttributeValue.builder().s("admin").build(),
                    AttributeValue.builder().s("user").build())
                .build());

    when(pdbItemManager.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().items(item).lastEvaluatedKey(Map.of()).build());

    // When
    csvExporter.export(tableName, metadata, tempDir, pdbItemManager, pdbTableConverter);

    // Then
    final Path csvFile = tempDir.resolve(tableName + ".csv");
    final String csvContent = Files.readString(csvFile);

    assertThat(csvContent).contains("address,id,tags");
    assertThat(csvContent).contains("user1");
    assertThat(csvContent).contains("\"city\""); // Map serialized as JSON
    assertThat(csvContent).contains("\"admin\""); // List serialized as JSON
  }

  @Test
  void export_withMissingAttributes_handlesGracefully() throws Exception {
    // Given
    final String tableName = "TestTable";
    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder().name(tableName).hashKey("id").createDate(Instant.now()).build();

    final Map<String, AttributeValue> item1 =
        Map.of(
            "id", AttributeValue.builder().s("user1").build(),
            "name", AttributeValue.builder().s("Alice").build(),
            "age", AttributeValue.builder().n("30").build());

    // item2 is missing 'age' attribute
    final Map<String, AttributeValue> item2 =
        Map.of(
            "id", AttributeValue.builder().s("user2").build(),
            "name", AttributeValue.builder().s("Bob").build());

    when(pdbItemManager.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder()
                .items(item1, item2)
                .lastEvaluatedKey(Map.of())
                .build());

    // When
    csvExporter.export(tableName, metadata, tempDir, pdbItemManager, pdbTableConverter);

    // Then
    final Path csvFile = tempDir.resolve(tableName + ".csv");
    final String csvContent = Files.readString(csvFile);

    // Both rows should be present
    assertThat(csvContent).contains("user1");
    assertThat(csvContent).contains("user2");

    // Header should include all attributes
    assertThat(csvContent).contains("age,id,name");

    // Count lines: header + 2 data rows
    final long lineCount = csvContent.lines().count();
    assertThat(lineCount).isEqualTo(3);
  }

  @Test
  void export_withStringSet_serializesAsJsonArray() throws Exception {
    // Given
    final String tableName = "TestTable";
    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder().name(tableName).hashKey("id").createDate(Instant.now()).build();

    final Map<String, AttributeValue> item =
        Map.of(
            "id",
            AttributeValue.builder().s("user1").build(),
            "roles",
            AttributeValue.builder().ss("admin", "editor", "viewer").build());

    when(pdbItemManager.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().items(item).lastEvaluatedKey(Map.of()).build());

    // When
    csvExporter.export(tableName, metadata, tempDir, pdbItemManager, pdbTableConverter);

    // Then
    final Path csvFile = tempDir.resolve(tableName + ".csv");
    final String csvContent = Files.readString(csvFile);

    assertThat(csvContent).contains("id,roles");
    assertThat(csvContent).contains("\"admin\"");
    assertThat(csvContent).contains("\"editor\"");
    assertThat(csvContent).contains("\"viewer\"");
  }
}
