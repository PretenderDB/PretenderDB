package io.github.pretenderdb.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbMetadata;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

@ExtendWith(MockitoExtension.class)
class PdbMetadataConverterTest {

  private static final Instant NOW = Instant.now();

  @Mock private Clock clock;

  @InjectMocks private PdbTableConverter converter;

  @Test
  void testConvert() {
    final List<KeySchemaElement> keySchemaElements = List.of(
        KeySchemaElement.builder().attributeName("p_hashKey").keyType(KeyType.HASH).build(),
        KeySchemaElement.builder().attributeName("p_sortKey").keyType(KeyType.RANGE).build()
    );
    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName("tableName")
        .keySchema(keySchemaElements)
        .build();
    when(clock.instant()).thenReturn(NOW);

    assertThat(converter.convert(createTableRequest))
        .hasFieldOrPropertyWithValue("name", "tableName")
        .hasFieldOrPropertyWithValue("hashKey", "p_hashKey")
        .hasFieldOrPropertyWithValue("sortKey", "p_sortKey")
        .hasFieldOrPropertyWithValue("createDate", NOW);
  }

  @Test
  void testConvert_noSort() {
    final List<KeySchemaElement> keySchemaElements = List.of(
        KeySchemaElement.builder().attributeName("p_hashKey").keyType(KeyType.HASH).build()
    );
    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName("tableName")
        .keySchema(keySchemaElements)
        .build();
    when(clock.instant()).thenReturn(NOW);

    assertThat(converter.convert(createTableRequest))
        .hasFieldOrPropertyWithValue("name", "tableName")
        .hasFieldOrPropertyWithValue("hashKey", "p_hashKey")
        .hasFieldOrPropertyWithValue("sortKey", null)
        .hasFieldOrPropertyWithValue("createDate", NOW);
  }

  @Test
  void testConvert_noHash() {
    final List<KeySchemaElement> keySchemaElements = List.of(
        KeySchemaElement.builder().attributeName("p_sortKey").keyType(KeyType.RANGE).build()
    );
    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName("tableName")
        .keySchema(keySchemaElements)
        .build();

    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> converter.convert(createTableRequest));
  }

  @Test
  public void testFromPdbTable() {
    final PdbMetadata pdbMetadata = ImmutablePdbMetadata.builder()
        .name("tableName")
        .hashKey("p_hashKey")
        .sortKey("p_sortKey")
        .createDate(NOW)
        .build();
    final TableDescription tableDescription = converter.fromPdbTable(pdbMetadata);
    assertThat(tableDescription)
        .hasFieldOrPropertyWithValue("tableName", "tableName")
        .hasFieldOrPropertyWithValue("keySchema", List.of(
            KeySchemaElement.builder().attributeName("p_hashKey").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("p_sortKey").keyType(KeyType.RANGE).build()
        ));
  }

  @Test
  public void testFromPdbTable_noSort() {
    final PdbMetadata pdbMetadata = ImmutablePdbMetadata.builder()
        .name("tableName")
        .hashKey("p_hashKey")
        .createDate(NOW)
        .build();
    final TableDescription tableDescription = converter.fromPdbTable(pdbMetadata);
    assertThat(tableDescription)
        .hasFieldOrPropertyWithValue("tableName", "tableName")
        .hasFieldOrPropertyWithValue("keySchema", List.of(
            KeySchemaElement.builder().attributeName("p_hashKey").keyType(KeyType.HASH).build()
        ));
  }

}