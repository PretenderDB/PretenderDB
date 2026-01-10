package io.github.pretenderdb.cli.exporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonSystemBuilder;
import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbMetadata;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
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
 * Unit tests for IonExporter.
 */
@ExtendWith(MockitoExtension.class)
class IonExporterTest {

  @Mock private PdbItemManager pdbItemManager;

  @Mock private PdbTableConverter pdbTableConverter;

  private IonExporter exporter;
  private IonSystem ionSystem;

  @TempDir Path tempDir;

  @BeforeEach
  void setUp() {
    ionSystem = IonSystemBuilder.standard().build();
    exporter = new IonExporter(ionSystem);
  }

  @Test
  void export_createsDataDirectory() throws Exception {
    // Given
    final String tableName = "TestTable";
    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder().name(tableName).hashKey("id").createDate(Instant.now()).build();

    when(pdbItemManager.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().lastEvaluatedKey(Map.of()).build());

    // When
    exporter.export(tableName, metadata, tempDir, pdbItemManager, pdbTableConverter);

    // Then
    assertThat(tempDir.resolve("data")).exists().isDirectory();
  }

  @Test
  void export_dataFiles_containItemsInIonFormat() throws Exception {
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
    final Path dataFile = dataDir.resolve("item-0001.ion.gz");

    assertThat(dataFile).exists();

    // Read and verify Ion content
    try (final InputStream is = new GZIPInputStream(Files.newInputStream(dataFile));
        final IonReader reader = ionSystem.newReader(is)) {

      // Should have at least one top-level value
      assertThat(reader.next()).isNotNull();

      // Should be a struct
      assertThat(reader.getType()).isEqualTo(IonType.STRUCT);

      reader.stepIn();

      // Should have "Item" field
      IonType fieldType = reader.next();
      assertThat(fieldType).isNotNull();
      assertThat(reader.getFieldName()).isEqualTo("Item");

      // Item should be a struct
      assertThat(reader.getType()).isEqualTo(IonType.STRUCT);
    }
  }

  @Test
  void export_withStringSet_usesTypeAnnotation() throws Exception {
    // Given
    final String tableName = "TestTable";
    final PdbMetadata metadata =
        ImmutablePdbMetadata.builder().name(tableName).hashKey("id").createDate(Instant.now()).build();

    final Map<String, AttributeValue> item =
        Map.of(
            "id",
            AttributeValue.builder().s("user1").build(),
            "roles",
            AttributeValue.builder().ss("admin", "editor").build());

    when(pdbItemManager.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().items(item).lastEvaluatedKey(Map.of()).build());

    // When
    exporter.export(tableName, metadata, tempDir, pdbItemManager, pdbTableConverter);

    // Then
    final Path dataFile = tempDir.resolve("data/item-0001.ion.gz");

    try (final InputStream is = new GZIPInputStream(Files.newInputStream(dataFile));
        final IonReader reader = ionSystem.newReader(is)) {

      reader.next();
      reader.stepIn(); // Enter top-level struct

      // Find "Item" field
      while (reader.next() != null) {
        if ("Item".equals(reader.getFieldName())) {
          reader.stepIn(); // Enter Item struct

          // Find "roles" field
          while (reader.next() != null) {
            if ("roles".equals(reader.getFieldName())) {
              // Should have annotation for string set
              final String[] annotations = reader.getTypeAnnotations();
              assertThat(annotations).contains("$dynamodb_SS");

              // Should be a list
              assertThat(reader.getType()).isEqualTo(IonType.LIST);
              break;
            }
          }
          break;
        }
      }
    }
  }
}
