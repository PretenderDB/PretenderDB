package io.github.pretenderdb.cli.importer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.pretenderdb.cli.model.ImmutableImportOptions;
import io.github.pretenderdb.cli.model.ImportOptions;
import io.github.pretenderdb.converter.AttributeValueConverter;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbMetadata;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;

/**
 * Unit tests for CsvImporter.
 */
@ExtendWith(MockitoExtension.class)
class CsvImporterTest {

  @Mock private PdbItemManager pdbItemManager;

  @Mock private PdbTableManager pdbTableManager;

  @Mock private PdbTableConverter pdbTableConverter;

  @Captor private ArgumentCaptor<BatchWriteItemRequest> batchWriteCaptor;

  private CsvImporter csvImporter;
  private ObjectMapper objectMapper;
  private AttributeValueConverter attributeValueConverter;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    objectMapper = new ObjectMapper();
    objectMapper.findAndRegisterModules();
    attributeValueConverter = new AttributeValueConverter(objectMapper);
    csvImporter = new CsvImporter(objectMapper, attributeValueConverter);
  }

  @Test
  void importData_withSimpleCsv_importsSuccessfully() throws Exception {
    // Given
    final String tableName = "TestTable";
    final Path csvFile = tempDir.resolve(tableName + ".csv");
    final Path schemaFile = tempDir.resolve(tableName + "-schema.json");

    final String csvContent =
        """
        id,name,age
        user1,Alice,30
        user2,Bob,25
        """;
    Files.writeString(csvFile, csvContent);

    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder().name(tableName).hashKey("id").createDate(Instant.now()).build();
    objectMapper.writeValue(schemaFile.toFile(), metadata);

    final ImportOptions options = ImmutableImportOptions.builder().createTable(true).build();

    when(pdbItemManager.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenReturn(BatchWriteItemResponse.builder().build());

    // When
    csvImporter.importData(
        tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter);

    // Then
    verify(pdbTableManager, times(1)).insertPdbTable(metadata);
    verify(pdbItemManager, times(1)).batchWriteItem(batchWriteCaptor.capture());

    final BatchWriteItemRequest request = batchWriteCaptor.getValue();
    assertThat(request.requestItems()).containsKey(tableName);
    assertThat(request.requestItems().get(tableName)).hasSize(2);

    // Verify items
    final var writeRequests = request.requestItems().get(tableName);
    final Map<String, AttributeValue> item1 = writeRequests.get(0).putRequest().item();
    assertThat(item1).containsKey("id");
    assertThat(item1.get("id").s()).isEqualTo("user1");
    assertThat(item1).containsKey("name");
    assertThat(item1.get("name").s()).isEqualTo("Alice");
    assertThat(item1).containsKey("age");
    assertThat(item1.get("age").n()).isEqualTo("30");
  }

  @Test
  void importData_withoutCreateTable_doesNotCreateTable() throws Exception {
    // Given
    final String tableName = "TestTable";
    final Path csvFile = tempDir.resolve(tableName + ".csv");

    final String csvContent =
        """
        id,name
        user1,Alice
        """;
    Files.writeString(csvFile, csvContent);

    final ImportOptions options = ImmutableImportOptions.builder().createTable(false).build();

    when(pdbItemManager.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenReturn(BatchWriteItemResponse.builder().build());

    // When
    csvImporter.importData(
        tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter);

    // Then
    verify(pdbTableManager, times(0)).insertPdbTable(any());
    verify(pdbItemManager, times(1)).batchWriteItem(any());
  }

  @Test
  void importData_missingCsvFile_throwsException() {
    // Given
    final String tableName = "NonExistent";
    final ImportOptions options = ImmutableImportOptions.builder().build();

    // When/Then
    assertThatThrownBy(
            () ->
                csvImporter.importData(
                    tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("CSV file not found");
  }

  @Test
  void importData_missingSchemaFileWithCreateTable_throwsException() throws Exception {
    // Given
    final String tableName = "TestTable";
    final Path csvFile = tempDir.resolve(tableName + ".csv");

    Files.writeString(csvFile, "id,name\nuser1,Alice");

    final ImportOptions options = ImmutableImportOptions.builder().createTable(true).build();

    // When/Then
    assertThatThrownBy(
            () ->
                csvImporter.importData(
                    tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Schema file not found");
  }

  @Test
  void importData_withComplexTypes_parsesJsonCorrectly() throws Exception {
    // Given
    final String tableName = "TestTable";
    final Path csvFile = tempDir.resolve(tableName + ".csv");

    final String csvContent =
        """
        id,address,tags
        user1,"{""city"":""NYC"",""zip"":""10001""}","[""admin"",""user""]"
        """;
    Files.writeString(csvFile, csvContent);

    final ImportOptions options = ImmutableImportOptions.builder().createTable(false).build();

    when(pdbItemManager.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenReturn(BatchWriteItemResponse.builder().build());

    // When
    csvImporter.importData(
        tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter);

    // Then
    verify(pdbItemManager, times(1)).batchWriteItem(batchWriteCaptor.capture());

    final var writeRequests = batchWriteCaptor.getValue().requestItems().get(tableName);
    final Map<String, AttributeValue> item = writeRequests.get(0).putRequest().item();

    assertThat(item).containsKey("address");
    assertThat(item.get("address").m()).isNotEmpty();
    assertThat(item.get("address").m()).containsKey("city");

    assertThat(item).containsKey("tags");
    assertThat(item.get("tags").l()).hasSize(2);
  }

  @Test
  void importData_withEmptyCells_skipsMissingAttributes() throws Exception {
    // Given
    final String tableName = "TestTable";
    final Path csvFile = tempDir.resolve(tableName + ".csv");

    final String csvContent =
        """
        id,name,age
        user1,Alice,30
        user2,Bob,
        """;
    Files.writeString(csvFile, csvContent);

    final ImportOptions options = ImmutableImportOptions.builder().createTable(false).build();

    when(pdbItemManager.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenReturn(BatchWriteItemResponse.builder().build());

    // When
    csvImporter.importData(
        tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter);

    // Then
    verify(pdbItemManager, times(1)).batchWriteItem(batchWriteCaptor.capture());

    final var writeRequests = batchWriteCaptor.getValue().requestItems().get(tableName);

    // First item should have age
    final Map<String, AttributeValue> item1 = writeRequests.get(0).putRequest().item();
    assertThat(item1).containsKey("age");

    // Second item should not have age
    final Map<String, AttributeValue> item2 = writeRequests.get(1).putRequest().item();
    assertThat(item2).doesNotContainKey("age");
  }
}
