package io.github.pretenderdb.converter;

import io.github.pretenderdb.model.ImmutablePdbGlobalSecondaryIndex;
import io.github.pretenderdb.model.ImmutablePdbMetadata;
import io.github.pretenderdb.model.PdbGlobalSecondaryIndex;
import io.github.pretenderdb.model.PdbMetadata;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * The type Meta data converter.
 */
@Singleton
public class PdbTableConverter {

  private final Clock clock;

  /**
   * Instantiates a new Meta data converter.
   *
   * @param clock the clock
   */
  @Inject
  public PdbTableConverter(final Clock clock) {
    this.clock = clock;
  }

  /**
   * Convert pdb table.
   *
   * @param createTableRequest the create table request
   * @return the pdb table
   */
  public PdbMetadata convert(final CreateTableRequest createTableRequest) {
    final String hashKey = createTableRequest.keySchema().stream()
        .filter(ks -> ks.keyType().equals(KeyType.HASH))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No hash key found"))
        .attributeName();
    final Optional<String> sortKey = createTableRequest.keySchema().stream()
        .filter(ks -> ks.keyType().equals(KeyType.RANGE))
        .findFirst()
        .map(KeySchemaElement::attributeName);

    // Extract Global Secondary Indexes
    final List<PdbGlobalSecondaryIndex> gsiList = createTableRequest.hasGlobalSecondaryIndexes()
        ? createTableRequest.globalSecondaryIndexes().stream()
        .map(this::convertGlobalSecondaryIndex)
        .collect(Collectors.toList())
        : List.of();

    return ImmutablePdbMetadata.builder()
        .name(createTableRequest.tableName())
        .hashKey(hashKey)
        .sortKey(sortKey)
        .globalSecondaryIndexes(gsiList)
        .ttlAttributeName(Optional.empty())  // TTL is set via UpdateTimeToLive API
        .ttlEnabled(false)
        .createDate(clock.instant())
        .build();
  }

  /**
   * Convert a DynamoDB GlobalSecondaryIndex to PdbGlobalSecondaryIndex.
   *
   * @param gsi the GlobalSecondaryIndex from AWS SDK
   * @return the PdbGlobalSecondaryIndex
   */
  private PdbGlobalSecondaryIndex convertGlobalSecondaryIndex(final GlobalSecondaryIndex gsi) {
    final String hashKey = gsi.keySchema().stream()
        .filter(ks -> ks.keyType().equals(KeyType.HASH))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("GSI missing hash key"))
        .attributeName();

    final Optional<String> sortKey = gsi.keySchema().stream()
        .filter(ks -> ks.keyType().equals(KeyType.RANGE))
        .findFirst()
        .map(KeySchemaElement::attributeName);

    final String projectionType = gsi.projection() != null && gsi.projection().projectionType() != null
        ? gsi.projection().projectionType().toString()
        : ProjectionType.ALL.toString();

    final Optional<String> nonKeyAttributes = gsi.projection() != null
        && gsi.projection().hasNonKeyAttributes()
        ? Optional.of(String.join(",", gsi.projection().nonKeyAttributes()))
        : Optional.empty();

    return ImmutablePdbGlobalSecondaryIndex.builder()
        .indexName(gsi.indexName())
        .hashKey(hashKey)
        .sortKey(sortKey)
        .projectionType(projectionType)
        .nonKeyAttributes(nonKeyAttributes)
        .build();
  }

  /**
   * From pdb table table description.
   *
   * @param pdbMetadata the pdb table
   * @return the table description
   */
  public TableDescription fromPdbTable(PdbMetadata pdbMetadata) {
    final KeySchemaElement hashKey = KeySchemaElement.builder().attributeName(pdbMetadata.hashKey()).keyType(KeyType.HASH).build();
    final Optional<KeySchemaElement> sortKey = pdbMetadata.sortKey().map(sk -> KeySchemaElement.builder().attributeName(sk).keyType(KeyType.RANGE).build());
    final List<KeySchemaElement> keySchema = Stream.of(hashKey, sortKey.orElse(null))
        .filter(Objects::nonNull)
        .toList();
    return TableDescription.builder()
        .tableName(pdbMetadata.name())
        .keySchema(keySchema)
        .build();
  }
}
