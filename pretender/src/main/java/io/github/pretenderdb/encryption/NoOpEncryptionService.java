package io.github.pretenderdb.encryption;

import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * No-op implementation of EncryptionService that performs no encryption.
 * This is the default implementation when encryption is disabled.
 */
@Singleton
public class NoOpEncryptionService implements EncryptionService {

  private static final Logger log = LoggerFactory.getLogger(NoOpEncryptionService.class);

  /**
   * Instantiates a new No-op encryption service.
   */
  public NoOpEncryptionService() {
    log.info("NoOpEncryptionService()");
  }

  @Override
  public AttributeValue encrypt(final AttributeValue plaintext, final String attributeName, final String tableName) {
    return plaintext;
  }

  @Override
  public AttributeValue decrypt(final AttributeValue encrypted, final String attributeName, final String tableName) {
    return encrypted;
  }

  @Override
  public boolean isEnabled() {
    return false;
  }
}
