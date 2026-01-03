package io.github.pretenderdb.encryption;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * AES-GCM implementation of EncryptionService.
 * Uses AES-256-GCM for authenticated encryption of attribute values.
 *
 * <p>Encrypted format: [12-byte IV][ciphertext][16-byte authentication tag]</p>
 *
 * <p>The plaintext being encrypted is the JSON representation of the AttributeValue,
 * allowing us to preserve the original type information.</p>
 */
@Singleton
public class AesGcmEncryptionService implements EncryptionService {

  private static final Logger log = LoggerFactory.getLogger(AesGcmEncryptionService.class);

  private static final String ALGORITHM = "AES/GCM/NoPadding";
  private static final int GCM_IV_LENGTH = 12; // 96 bits
  private static final int GCM_TAG_LENGTH = 128; // 128 bits
  private static final int AES_KEY_SIZE = 256; // 256 bits

  private final SecretKey masterKey;
  private final SecureRandom secureRandom;
  private final ObjectMapper objectMapper;

  /**
   * Instantiates a new AES-GCM encryption service with a randomly generated master key.
   *
   * <p>WARNING: This generates a new key each time. In production, you should
   * inject a persistent master key from a key management system.</p>
   *
   * @param objectMapper the object mapper for serializing AttributeValues
   */
  @Inject
  public AesGcmEncryptionService(final ObjectMapper objectMapper) {
    log.info("AesGcmEncryptionService(objectMapper)");
    this.objectMapper = objectMapper;
    this.secureRandom = new SecureRandom();
    this.masterKey = generateMasterKey();
    log.warn("Using randomly generated master key - encrypted data will not be recoverable after restart!");
  }

  /**
   * Instantiates a new AES-GCM encryption service with a provided master key.
   *
   * @param masterKey    the master key (must be 32 bytes for AES-256)
   * @param objectMapper the object mapper for serializing AttributeValues
   */
  public AesGcmEncryptionService(final byte[] masterKey, final ObjectMapper objectMapper) {
    log.info("AesGcmEncryptionService(masterKey, objectMapper)");
    if (masterKey.length != 32) {
      throw new IllegalArgumentException("Master key must be 32 bytes (256 bits) for AES-256");
    }
    this.objectMapper = objectMapper;
    this.secureRandom = new SecureRandom();
    this.masterKey = new SecretKeySpec(masterKey, "AES");
  }

  @Override
  public AttributeValue encrypt(final AttributeValue plaintext, final String attributeName, final String tableName) {
    log.trace("encrypt({}, {}, {})", plaintext, attributeName, tableName);

    try {
      // Serialize the AttributeValue to JSON to preserve type information
      final String json = serializeAttributeValue(plaintext);
      final byte[] plaintextBytes = json.getBytes(StandardCharsets.UTF_8);

      // Generate random IV
      final byte[] iv = new byte[GCM_IV_LENGTH];
      secureRandom.nextBytes(iv);

      // Derive attribute-specific key
      final SecretKey attributeKey = deriveAttributeKey(attributeName, tableName);

      // Encrypt
      final Cipher cipher = Cipher.getInstance(ALGORITHM);
      final GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
      cipher.init(Cipher.ENCRYPT_MODE, attributeKey, gcmSpec);

      // Add additional authenticated data (AAD) for context binding
      final String aad = tableName + ":" + attributeName;
      cipher.updateAAD(aad.getBytes(StandardCharsets.UTF_8));

      final byte[] ciphertext = cipher.doFinal(plaintextBytes);

      // Combine IV + ciphertext (ciphertext already includes the GCM auth tag)
      final byte[] encrypted = ByteBuffer.allocate(GCM_IV_LENGTH + ciphertext.length)
          .put(iv)
          .put(ciphertext)
          .array();

      // Return as Binary AttributeValue
      return AttributeValue.builder()
          .b(SdkBytes.fromByteArray(encrypted))
          .build();

    } catch (GeneralSecurityException | JsonProcessingException e) {
      log.error("Encryption failed for attribute {} in table {}", attributeName, tableName, e);
      throw new EncryptionException("Failed to encrypt attribute: " + attributeName, e);
    }
  }

  @Override
  public AttributeValue decrypt(final AttributeValue encrypted, final String attributeName, final String tableName) {
    log.trace("decrypt({}, {}, {})", encrypted, attributeName, tableName);

    try {
      // Extract encrypted bytes
      if (encrypted.b() == null) {
        throw new IllegalArgumentException("Encrypted attribute must be Binary type");
      }
      final byte[] encryptedBytes = encrypted.b().asByteArray();

      // Extract IV and ciphertext
      if (encryptedBytes.length < GCM_IV_LENGTH) {
        throw new IllegalArgumentException("Encrypted data is too short");
      }
      final byte[] iv = Arrays.copyOfRange(encryptedBytes, 0, GCM_IV_LENGTH);
      final byte[] ciphertext = Arrays.copyOfRange(encryptedBytes, GCM_IV_LENGTH, encryptedBytes.length);

      // Derive attribute-specific key
      final SecretKey attributeKey = deriveAttributeKey(attributeName, tableName);

      // Decrypt
      final Cipher cipher = Cipher.getInstance(ALGORITHM);
      final GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
      cipher.init(Cipher.DECRYPT_MODE, attributeKey, gcmSpec);

      // Add additional authenticated data (AAD) - must match encryption
      final String aad = tableName + ":" + attributeName;
      cipher.updateAAD(aad.getBytes(StandardCharsets.UTF_8));

      final byte[] plaintextBytes = cipher.doFinal(ciphertext);
      final String json = new String(plaintextBytes, StandardCharsets.UTF_8);

      // Deserialize JSON back to AttributeValue
      return deserializeAttributeValue(json);

    } catch (GeneralSecurityException | JsonProcessingException e) {
      log.error("Decryption failed for attribute {} in table {}", attributeName, tableName, e);
      throw new EncryptionException("Failed to decrypt attribute: " + attributeName, e);
    }
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  /**
   * Generates a random AES-256 master key.
   */
  private SecretKey generateMasterKey() {
    try {
      final KeyGenerator keyGen = KeyGenerator.getInstance("AES");
      keyGen.init(AES_KEY_SIZE, secureRandom);
      return keyGen.generateKey();
    } catch (GeneralSecurityException e) {
      throw new EncryptionException("Failed to generate master key", e);
    }
  }

  /**
   * Derives an attribute-specific key from the master key.
   * Uses HKDF-like approach: HMAC-SHA256(masterKey, tableName:attributeName)
   */
  private SecretKey deriveAttributeKey(final String attributeName, final String tableName) {
    try {
      final javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
      mac.init(masterKey);
      final String context = tableName + ":" + attributeName;
      final byte[] derivedKey = mac.doFinal(context.getBytes(StandardCharsets.UTF_8));
      return new SecretKeySpec(derivedKey, "AES");
    } catch (GeneralSecurityException e) {
      throw new EncryptionException("Failed to derive attribute key", e);
    }
  }

  /**
   * Serializes an AttributeValue to JSON format.
   */
  private String serializeAttributeValue(final AttributeValue value) throws JsonProcessingException {
    // Convert to a simple type-tagged format
    if (value.s() != null) {
      return objectMapper.writeValueAsString(new TypedValue("S", value.s()));
    } else if (value.n() != null) {
      return objectMapper.writeValueAsString(new TypedValue("N", value.n()));
    } else if (value.b() != null) {
      return objectMapper.writeValueAsString(new TypedValue("B", value.b().asByteArray()));
    } else if (value.bool() != null) {
      return objectMapper.writeValueAsString(new TypedValue("BOOL", value.bool()));
    } else if (value.nul() != null) {
      return objectMapper.writeValueAsString(new TypedValue("NULL", value.nul()));
    } else if (value.hasSs()) {
      return objectMapper.writeValueAsString(new TypedValue("SS", value.ss()));
    } else if (value.hasNs()) {
      return objectMapper.writeValueAsString(new TypedValue("NS", value.ns()));
    } else if (value.hasBs()) {
      return objectMapper.writeValueAsString(new TypedValue("BS",
          value.bs().stream().map(SdkBytes::asByteArray).toList()));
    } else if (value.hasL()) {
      // For List, we need to recursively serialize each element
      throw new UnsupportedOperationException("List encryption not yet supported");
    } else if (value.hasM()) {
      // For Map, we need to recursively serialize each value
      throw new UnsupportedOperationException("Map encryption not yet supported");
    } else {
      throw new IllegalArgumentException("Unsupported AttributeValue type");
    }
  }

  /**
   * Deserializes JSON back to an AttributeValue.
   */
  @SuppressWarnings("unchecked")
  private AttributeValue deserializeAttributeValue(final String json) throws JsonProcessingException {
    final java.util.Map<String, Object> map = objectMapper.readValue(json, java.util.Map.class);
    final String type = (String) map.get("type");
    final Object value = map.get("value");

    return switch (type) {
      case "S" -> AttributeValue.builder().s((String) value).build();
      case "N" -> AttributeValue.builder().n((String) value).build();
      case "B" -> {
        final byte[] bytes = value instanceof byte[] ? (byte[]) value :
            objectMapper.convertValue(value, byte[].class);
        yield AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
      }
      case "BOOL" -> AttributeValue.builder().bool((Boolean) value).build();
      case "NULL" -> AttributeValue.builder().nul((Boolean) value).build();
      case "SS" -> AttributeValue.builder().ss((java.util.List<String>) value).build();
      case "NS" -> AttributeValue.builder().ns((java.util.List<String>) value).build();
      case "BS" -> {
        final java.util.List<byte[]> bytesList = ((java.util.List<?>) value).stream()
            .map(item -> item instanceof byte[] ? (byte[]) item :
                objectMapper.convertValue(item, byte[].class))
            .toList();
        yield AttributeValue.builder().bs(bytesList.stream()
            .map(SdkBytes::fromByteArray)
            .toList()).build();
      }
      default -> throw new IllegalArgumentException("Unknown AttributeValue type: " + type);
    };
  }

  /**
   * Simple record for type-tagged values during serialization.
   */
  private record TypedValue(String type, Object value) {}
}
