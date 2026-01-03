package io.github.pretenderdb.encryption;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Service interface for encrypting and decrypting DynamoDB attribute values.
 * Implementations should use authenticated encryption (e.g., AES-GCM) to ensure
 * both confidentiality and integrity of encrypted data.
 */
public interface EncryptionService {

  /**
   * Encrypts an AttributeValue for storage.
   *
   * @param plaintext     the plaintext attribute value
   * @param attributeName the name of the attribute being encrypted
   * @param tableName     the DynamoDB table name
   * @return encrypted attribute value (as Binary type)
   */
  AttributeValue encrypt(AttributeValue plaintext, String attributeName, String tableName);

  /**
   * Decrypts an AttributeValue retrieved from storage.
   *
   * @param encrypted     the encrypted attribute value (Binary type)
   * @param attributeName the name of the attribute being decrypted
   * @param tableName     the DynamoDB table name
   * @return decrypted attribute value (original type)
   */
  AttributeValue decrypt(AttributeValue encrypted, String attributeName, String tableName);

  /**
   * Checks if encryption is enabled.
   *
   * @return true if encryption is enabled, false otherwise
   */
  boolean isEnabled();
}
