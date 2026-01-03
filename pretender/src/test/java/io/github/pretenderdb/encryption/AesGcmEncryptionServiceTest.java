package io.github.pretenderdb.encryption;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class AesGcmEncryptionServiceTest {

  private AesGcmEncryptionService service;
  private ObjectMapper objectMapper;

  @BeforeEach
  void setup() {
    objectMapper = new ObjectMapper();
    service = new AesGcmEncryptionService(objectMapper);
  }

  @Test
  void isEnabled_returnsTrue() {
    assertThat(service.isEnabled()).isTrue();
  }

  @Test
  void encrypt_decrypt_stringAttribute_roundTrip() {
    final AttributeValue plaintext = AttributeValue.builder()
        .s("sensitive data")
        .build();

    final AttributeValue encrypted = service.encrypt(plaintext, "ssn", "Users");
    assertThat(encrypted.b()).isNotNull();
    assertThat(encrypted.b().asByteArray().length).isGreaterThan(0);

    final AttributeValue decrypted = service.decrypt(encrypted, "ssn", "Users");
    assertThat(decrypted.s()).isEqualTo("sensitive data");
  }

  @Test
  void encrypt_decrypt_numberAttribute_roundTrip() {
    final AttributeValue plaintext = AttributeValue.builder()
        .n("12345")
        .build();

    final AttributeValue encrypted = service.encrypt(plaintext, "accountNumber", "Accounts");
    final AttributeValue decrypted = service.decrypt(encrypted, "accountNumber", "Accounts");

    assertThat(decrypted.n()).isEqualTo("12345");
  }

  @Test
  void encrypt_decrypt_boolAttribute_roundTrip() {
    final AttributeValue plaintext = AttributeValue.builder()
        .bool(true)
        .build();

    final AttributeValue encrypted = service.encrypt(plaintext, "isActive", "Users");
    final AttributeValue decrypted = service.decrypt(encrypted, "isActive", "Users");

    assertThat(decrypted.bool()).isTrue();
  }

  @Test
  void encrypt_decrypt_binaryAttribute_roundTrip() {
    final byte[] data = new byte[]{1, 2, 3, 4, 5};
    final AttributeValue plaintext = AttributeValue.builder()
        .b(SdkBytes.fromByteArray(data))
        .build();

    final AttributeValue encrypted = service.encrypt(plaintext, "photo", "Users");
    final AttributeValue decrypted = service.decrypt(encrypted, "photo", "Users");

    assertThat(decrypted.b().asByteArray()).isEqualTo(data);
  }

  @Test
  void encrypt_decrypt_stringSetAttribute_roundTrip() {
    final AttributeValue plaintext = AttributeValue.builder()
        .ss("tag1", "tag2", "tag3")
        .build();

    final AttributeValue encrypted = service.encrypt(plaintext, "tags", "Items");
    final AttributeValue decrypted = service.decrypt(encrypted, "tags", "Items");

    assertThat(decrypted.ss()).containsExactlyInAnyOrder("tag1", "tag2", "tag3");
  }

  @Test
  void encrypt_decrypt_numberSetAttribute_roundTrip() {
    final AttributeValue plaintext = AttributeValue.builder()
        .ns("1", "2", "3")
        .build();

    final AttributeValue encrypted = service.encrypt(plaintext, "scores", "Games");
    final AttributeValue decrypted = service.decrypt(encrypted, "scores", "Games");

    assertThat(decrypted.ns()).containsExactlyInAnyOrder("1", "2", "3");
  }

  @Test
  void encrypt_decrypt_binarySetAttribute_roundTrip() {
    final AttributeValue plaintext = AttributeValue.builder()
        .bs(SdkBytes.fromByteArray(new byte[]{1, 2}),
            SdkBytes.fromByteArray(new byte[]{3, 4}))
        .build();

    final AttributeValue encrypted = service.encrypt(plaintext, "hashes", "Files");
    final AttributeValue decrypted = service.decrypt(encrypted, "hashes", "Files");

    assertThat(decrypted.bs()).hasSize(2);
    assertThat(decrypted.bs().get(0).asByteArray()).isEqualTo(new byte[]{1, 2});
    assertThat(decrypted.bs().get(1).asByteArray()).isEqualTo(new byte[]{3, 4});
  }

  @Test
  void encrypt_decrypt_nullAttribute_roundTrip() {
    final AttributeValue plaintext = AttributeValue.builder()
        .nul(true)
        .build();

    final AttributeValue encrypted = service.encrypt(plaintext, "optional", "Items");
    final AttributeValue decrypted = service.decrypt(encrypted, "optional", "Items");

    assertThat(decrypted.nul()).isTrue();
  }

  @Test
  void encrypt_differentAttributeNames_producesDifferentCiphertext() {
    final AttributeValue plaintext = AttributeValue.builder()
        .s("same data")
        .build();

    final AttributeValue encrypted1 = service.encrypt(plaintext, "attr1", "Table");
    final AttributeValue encrypted2 = service.encrypt(plaintext, "attr2", "Table");

    // Different attribute names should produce different ciphertext due to AAD
    assertThat(encrypted1.b().asByteArray()).isNotEqualTo(encrypted2.b().asByteArray());
  }

  @Test
  void encrypt_differentTables_producesDifferentCiphertext() {
    final AttributeValue plaintext = AttributeValue.builder()
        .s("same data")
        .build();

    final AttributeValue encrypted1 = service.encrypt(plaintext, "attr", "Table1");
    final AttributeValue encrypted2 = service.encrypt(plaintext, "attr", "Table2");

    // Different table names should produce different ciphertext due to AAD
    assertThat(encrypted1.b().asByteArray()).isNotEqualTo(encrypted2.b().asByteArray());
  }

  @Test
  void decrypt_withWrongAttributeName_fails() {
    final AttributeValue plaintext = AttributeValue.builder()
        .s("sensitive")
        .build();

    final AttributeValue encrypted = service.encrypt(plaintext, "attr1", "Table");

    // Decrypting with wrong attribute name should fail (AAD mismatch)
    assertThatThrownBy(() -> service.decrypt(encrypted, "attr2", "Table"))
        .isInstanceOf(EncryptionException.class)
        .hasMessageContaining("Failed to decrypt attribute");
  }

  @Test
  void decrypt_withWrongTableName_fails() {
    final AttributeValue plaintext = AttributeValue.builder()
        .s("sensitive")
        .build();

    final AttributeValue encrypted = service.encrypt(plaintext, "attr", "Table1");

    // Decrypting with wrong table name should fail (AAD mismatch)
    assertThatThrownBy(() -> service.decrypt(encrypted, "attr", "Table2"))
        .isInstanceOf(EncryptionException.class)
        .hasMessageContaining("Failed to decrypt attribute");
  }

  @Test
  void decrypt_nonBinaryAttribute_throwsException() {
    final AttributeValue nonBinary = AttributeValue.builder()
        .s("not encrypted")
        .build();

    assertThatThrownBy(() -> service.decrypt(nonBinary, "attr", "Table"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be Binary type");
  }

  @Test
  void constructor_withInvalidKeySize_throwsException() {
    final byte[] invalidKey = new byte[16]; // Only 16 bytes, need 32 for AES-256

    assertThatThrownBy(() -> new AesGcmEncryptionService(invalidKey, objectMapper))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Master key must be 32 bytes");
  }

  @Test
  void constructor_withProvidedKey_usesKey() {
    final byte[] masterKey = new byte[32]; // 32 bytes for AES-256
    java.util.Arrays.fill(masterKey, (byte) 42);

    final AesGcmEncryptionService serviceWithKey = new AesGcmEncryptionService(masterKey, objectMapper);

    final AttributeValue plaintext = AttributeValue.builder()
        .s("test data")
        .build();

    final AttributeValue encrypted = serviceWithKey.encrypt(plaintext, "attr", "Table");
    final AttributeValue decrypted = serviceWithKey.decrypt(encrypted, "attr", "Table");

    assertThat(decrypted.s()).isEqualTo("test data");
  }
}
