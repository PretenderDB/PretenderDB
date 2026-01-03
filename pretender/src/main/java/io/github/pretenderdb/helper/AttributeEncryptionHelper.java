package io.github.pretenderdb.helper;

import io.github.pretenderdb.encryption.EncryptionService;
import io.github.pretenderdb.model.EncryptionConfig;
import io.github.pretenderdb.model.PdbMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Helper class for managing attribute-level encryption and decryption.
 * Coordinates with EncryptionService and EncryptionConfig to selectively
 * encrypt/decrypt attributes based on table configuration.
 */
@Singleton
public class AttributeEncryptionHelper {

  private static final Logger log = LoggerFactory.getLogger(AttributeEncryptionHelper.class);

  private final EncryptionService encryptionService;
  private final Map<String, EncryptionConfig> configCache;

  /**
   * Instantiates a new Attribute encryption helper.
   *
   * @param encryptionService the encryption service
   */
  @Inject
  public AttributeEncryptionHelper(final EncryptionService encryptionService) {
    log.info("AttributeEncryptionHelper({})", encryptionService);
    this.encryptionService = encryptionService;
    this.configCache = new HashMap<>();
  }

  /**
   * Sets the encryption configuration for a table.
   *
   * @param config the encryption config
   */
  public void setEncryptionConfig(final EncryptionConfig config) {
    log.info("Setting encryption config for table: {}", config.tableName());
    configCache.put(config.tableName(), config);
  }

  /**
   * Gets the encryption configuration for a table.
   *
   * @param tableName the table name
   * @return the encryption config, if present
   */
  public Optional<EncryptionConfig> getEncryptionConfig(final String tableName) {
    return Optional.ofNullable(configCache.get(tableName));
  }

  /**
   * Removes the encryption configuration for a table.
   *
   * @param tableName the table name
   */
  public void removeEncryptionConfig(final String tableName) {
    log.info("Removing encryption config for table: {}", tableName);
    configCache.remove(tableName);
  }

  /**
   * Encrypts specified attributes in an item before storage.
   * Key attributes (hash key, sort key) are never encrypted.
   *
   * @param item     the item to encrypt
   * @param metadata the table metadata
   * @return item with encrypted attributes
   */
  public Map<String, AttributeValue> encryptAttributes(
      final Map<String, AttributeValue> item,
      final PdbMetadata metadata) {
    log.trace("encryptAttributes(item, {})", metadata.name());

    if (!encryptionService.isEnabled()) {
      return item;
    }

    final Optional<EncryptionConfig> configOpt = getEncryptionConfig(metadata.name());
    if (configOpt.isEmpty() || !configOpt.get().enabled()) {
      return item;
    }

    final EncryptionConfig config = configOpt.get();
    final Map<String, AttributeValue> encryptedItem = new HashMap<>(item);

    for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
      final String attributeName = entry.getKey();

      // Skip key attributes - they cannot be encrypted
      if (attributeName.equals(metadata.hashKey()) ||
          (metadata.sortKey().isPresent() && attributeName.equals(metadata.sortKey().get()))) {
        continue;
      }

      // Encrypt if configured
      if (config.encryptedAttributes().contains(attributeName)) {
        final AttributeValue encrypted = encryptionService.encrypt(
            entry.getValue(),
            attributeName,
            metadata.name()
        );
        encryptedItem.put(attributeName, encrypted);
        log.trace("Encrypted attribute: {}", attributeName);
      }
    }

    return encryptedItem;
  }

  /**
   * Decrypts specified attributes in an item after retrieval.
   *
   * @param item     the item with encrypted attributes
   * @param metadata the table metadata
   * @return item with decrypted attributes
   */
  public Map<String, AttributeValue> decryptAttributes(
      final Map<String, AttributeValue> item,
      final PdbMetadata metadata) {
    log.trace("decryptAttributes(item, {})", metadata.name());

    if (!encryptionService.isEnabled()) {
      return item;
    }

    final Optional<EncryptionConfig> configOpt = getEncryptionConfig(metadata.name());
    if (configOpt.isEmpty() || !configOpt.get().enabled()) {
      return item;
    }

    final EncryptionConfig config = configOpt.get();
    final Map<String, AttributeValue> decryptedItem = new HashMap<>(item);

    for (String attributeName : config.encryptedAttributes()) {
      if (item.containsKey(attributeName)) {
        final AttributeValue encrypted = item.get(attributeName);
        // Only decrypt if it's Binary type (encrypted format)
        if (encrypted.b() != null) {
          final AttributeValue decrypted = encryptionService.decrypt(
              encrypted,
              attributeName,
              metadata.name()
          );
          decryptedItem.put(attributeName, decrypted);
          log.trace("Decrypted attribute: {}", attributeName);
        }
      }
    }

    return decryptedItem;
  }

  /**
   * Checks if encryption is enabled for a table.
   *
   * @param tableName the table name
   * @return true if encryption is enabled
   */
  public boolean isEncryptionEnabled(final String tableName) {
    return encryptionService.isEnabled() &&
        getEncryptionConfig(tableName).map(EncryptionConfig::enabled).orElse(false);
  }
}
