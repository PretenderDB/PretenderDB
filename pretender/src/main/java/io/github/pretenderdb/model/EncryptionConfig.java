package io.github.pretenderdb.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Set;
import org.immutables.value.Value;

/**
 * Configuration for attribute-level encryption for a DynamoDB table.
 * Specifies which attributes should be encrypted at rest.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableEncryptionConfig.class)
@JsonDeserialize(as = ImmutableEncryptionConfig.class)
public interface EncryptionConfig {

  /**
   * The table name this configuration applies to.
   *
   * @return the table name
   */
  String tableName();

  /**
   * Set of attribute names that should be encrypted.
   * Key attributes (hash key, sort key) cannot be encrypted.
   *
   * @return the set of encrypted attribute names
   */
  Set<String> encryptedAttributes();

  /**
   * Whether encryption is enabled for this table.
   *
   * @return true if encryption is enabled
   */
  @Value.Default
  default boolean enabled() {
    return true;
  }
}
