package io.github.pretenderdb.cli.importer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonSystemBuilder;
import io.github.pretenderdb.cli.model.ImmutableImportOptions;
import io.github.pretenderdb.cli.model.ImportOptions;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * Unit tests for IonImporter.
 */
@ExtendWith(MockitoExtension.class)
class IonImporterTest {

  @Mock private PdbItemManager pdbItemManager;

  @Mock private PdbTableManager pdbTableManager;

  @Mock private PdbTableConverter pdbTableConverter;

  @Captor private ArgumentCaptor<BatchWriteItemRequest> batchWriteCaptor;

  private IonImporter importer;
  private IonSystem ionSystem;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    ionSystem = IonSystemBuilder.standard().build();
    importer = new IonImporter(ionSystem);
  }

  @Test
  void importData_missingDataDirectory_throwsException() {
    // Given
    final String tableName = "TestTable";
    final ImportOptions options = ImmutableImportOptions.builder().build();

    // When/Then
    assertThatThrownBy(
            () ->
                importer.importData(
                    tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("data directory not found");
  }

  @Test
  void importData_noIonFiles_throwsException() throws Exception {
    // Given
    final String tableName = "TestTable";
    Files.createDirectories(tempDir.resolve("data"));

    final ImportOptions options = ImmutableImportOptions.builder().build();

    // When/Then
    assertThatThrownBy(
            () ->
                importer.importData(
                    tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No .ion.gz files found");
  }

  @Test
  void importData_withValidIonFile_importsSuccessfully() throws Exception {
    // Given
    final String tableName = "TestTable";
    final Path dataDir = tempDir.resolve("data");
    Files.createDirectories(dataDir);

    final Path ionFile = dataDir.resolve("item-0001.ion.gz");

    // Write Ion data
    try (final OutputStream os = new GZIPOutputStream(Files.newOutputStream(ionFile));
        final IonWriter writer = ionSystem.newTextWriter(os)) {

      // Write {Item: {id: "user1", name: "Alice"}}
      writer.stepIn(com.amazon.ion.IonType.STRUCT);
      writer.setFieldName("Item");

      writer.stepIn(com.amazon.ion.IonType.STRUCT);
      writer.setFieldName("id");
      writer.writeString("user1");
      writer.setFieldName("name");
      writer.writeString("Alice");
      writer.stepOut(); // Item struct

      writer.stepOut(); // Top-level struct
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
  void importData_withAnnotatedStringSet_parsesCorrectly() throws Exception {
    // Given
    final String tableName = "TestTable";
    final Path dataDir = tempDir.resolve("data");
    Files.createDirectories(dataDir);

    final Path ionFile = dataDir.resolve("item-0001.ion.gz");

    // Write Ion data with string set annotation
    try (final OutputStream os = new GZIPOutputStream(Files.newOutputStream(ionFile));
        final IonWriter writer = ionSystem.newTextWriter(os)) {

      writer.stepIn(com.amazon.ion.IonType.STRUCT);
      writer.setFieldName("Item");

      writer.stepIn(com.amazon.ion.IonType.STRUCT);
      writer.setFieldName("id");
      writer.writeString("user1");

      writer.setFieldName("roles");
      writer.setTypeAnnotations("$dynamodb_SS");
      writer.stepIn(com.amazon.ion.IonType.LIST);
      writer.writeString("admin");
      writer.writeString("editor");
      writer.stepOut();

      writer.stepOut(); // Item struct
      writer.stepOut(); // Top-level struct
    }

    final ImportOptions options = ImmutableImportOptions.builder().build();

    when(pdbItemManager.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenReturn(BatchWriteItemResponse.builder().build());

    // When
    importer.importData(
        tableName, tempDir, options, pdbItemManager, pdbTableManager, pdbTableConverter);

    // Then
    verify(pdbItemManager, times(1)).batchWriteItem(batchWriteCaptor.capture());

    final var item = batchWriteCaptor.getValue().requestItems().get(tableName).get(0).putRequest().item();
    assertThat(item).containsKey("roles");
    assertThat(item.get("roles").ss()).containsExactly("admin", "editor");
  }
}
