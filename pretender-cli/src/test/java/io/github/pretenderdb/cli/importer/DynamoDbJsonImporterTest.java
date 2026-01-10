package io.github.pretenderdb.cli.importer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.cli.model.ExportManifest;
import io.github.pretenderdb.cli.model.ImmutableExportManifest;
import io.github.pretenderdb.cli.model.ImmutableImportOptions;
import io.github.pretenderdb.cli.model.ImmutableTableSchema;
import io.github.pretenderdb.cli.model.ImportOptions;
import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;

/**
 * Unit tests for DynamoDbJsonImporter.
 */
@ExtendWith(MockitoExtension.class)
class DynamoDbJsonImporterTest {

  @Mock private PdbItemManager pdbItemManager;

  @Mock private PdbTableManager pdbTableManager;

  @Mock private PdbTableConverter pdbTableConverter;

  @Captor private ArgumentCaptor<BatchWriteItemRequest> batchWriteCaptor;

  private DynamoDbJsonImporter importer;
  private ObjectMapper objectMapper;
  private AttributeValueConverter attributeValueConverter;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    objectMapper = new ObjectMapper();
    objectMapper.findAndRegisterModules();
    attributeValueConverter = new AttributeValueConverter(objectMapper);
    importer = new DynamoDbJsonImporter(objectMapper, attributeValueConverter);
  }

  @Test
  void importData_missingManifest_throwsException() {
    // Given
    final String tableName = "TestTable";
    final ImportOptions options = ImmutableImportOptions.builder().build();

    // When/Then
    assertThatThrownBy(
            () ->
                importer.importData(
                    tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("manifest-summary.json not found");
  }

  @Test
  void importData_withValidManifestAndData_importsSuccessfully() throws Exception {
    // Given
    final String tableName = "TestTable";

    // Create manifest-summary.json
    final ExportManifest manifest =
        ImmutableExportManifest.builder()
            .version("2023-08-01")
            .exportArn("arn:test")
            .startTime(Instant.now())
            .endTime(Instant.now())
            .tableArn("arn:table")
            .tableName(tableName)
            .s3Bucket("local")
            .s3Prefix("test")
            .billedSizeBytes(1000)
            .itemCount(1)
            .outputFormat("DYNAMODB_JSON")
            .exportType("FULL_EXPORT")
            .tableSchema(
                ImmutableTableSchema.builder()
                    .hashKey("id")
                    .sortKey(Optional.empty())
                    .build())
            .build();

    objectMapper.writeValue(tempDir.resolve("manifest-summary.json").toFile(), manifest);

    // Create manifest-files.json
    final String manifestFilesContent =
        "{\"itemCount\":1,\"md5Checksum\":\"abc123\",\"dataFileS3Key\":\"data/item-0001.json.gz\"}";
    Files.writeString(tempDir.resolve("manifest-files.json"), manifestFilesContent);

    // Create data file
    final Path dataDir = tempDir.resolve("data");
    Files.createDirectories(dataDir);
    final Path dataFile = dataDir.resolve("item-0001.json.gz");

    try (final BufferedWriter writer =
        new BufferedWriter(
            new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(dataFile))))) {
      writer.write("{\"Item\":{\"id\":{\"S\":\"user1\"},\"name\":{\"S\":\"Alice\"}}}");
      writer.newLine();
    }

    final ImportOptions options = ImmutableImportOptions.builder().createTable(false).build();

    when(pdbItemManager.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenReturn(BatchWriteItemResponse.builder().build());

    // When
    importer.importData(
        tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter);

    // Then
    verify(pdbItemManager, times(1)).batchWriteItem(batchWriteCaptor.capture());

    final BatchWriteItemRequest request = batchWriteCaptor.getValue();
    assertThat(request.requestItems()).containsKey(tableName);
    assertThat(request.requestItems().get(tableName)).hasSize(1);

    final var item = request.requestItems().get(tableName).get(0).putRequest().item();
    assertThat(item).containsKey("id");
    assertThat(item.get("id").s()).isEqualTo("user1");
    assertThat(item).containsKey("name");
    assertThat(item.get("name").s()).isEqualTo("Alice");
  }

  @Test
  void importData_withCreateTable_createsTableFromSchema() throws Exception {
    // Given
    final String tableName = "TestTable";

    final ExportManifest manifest =
        ImmutableExportManifest.builder()
            .version("2023-08-01")
            .exportArn("arn:test")
            .startTime(Instant.now())
            .endTime(Instant.now())
            .tableArn("arn:table")
            .tableName(tableName)
            .s3Bucket("local")
            .s3Prefix("test")
            .billedSizeBytes(0)
            .itemCount(0)
            .outputFormat("DYNAMODB_JSON")
            .exportType("FULL_EXPORT")
            .tableSchema(
                ImmutableTableSchema.builder()
                    .hashKey("id")
                    .sortKey(Optional.of("timestamp"))
                    .build())
            .build();

    objectMapper.writeValue(tempDir.resolve("manifest-summary.json").toFile(), manifest);
    Files.writeString(tempDir.resolve("manifest-files.json"), "");

    final ImportOptions options = ImmutableImportOptions.builder().createTable(true).build();

    when(pdbTableConverter.convert(any()))
        .thenReturn(null); // Not testing conversion logic here

    // When
    importer.importData(
        tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter);

    // Then
    verify(pdbTableManager, times(1)).insertPdbTable(any());
    verify(pdbTableConverter, times(1)).convert(any());
  }
}
