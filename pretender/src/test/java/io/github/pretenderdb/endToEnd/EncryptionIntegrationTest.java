package io.github.pretenderdb.endToEnd;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pretenderdb.BasePostgreSQLTest;
import io.github.pretenderdb.DynamoDbPretenderClient;
import io.github.pretenderdb.encryption.AesGcmEncryptionService;
import io.github.pretenderdb.helper.AttributeEncryptionHelper;
import io.github.pretenderdb.model.ImmutableEncryptionConfig;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * End-to-end integration test for attribute-level encryption.
 * Tests that sensitive attributes can be encrypted at rest and decrypted on retrieval.
 */
class EncryptionIntegrationTest extends BasePostgreSQLTest {

  private static final String TABLE_NAME = "EncryptedUsers";
  private static final String HASH_KEY = "userId";

  private DynamoDbPretenderClient client;
  private AttributeEncryptionHelper encryptionHelper;

  @BeforeEach
  void setupEncryption() {
    // Get the client component
    client = component.dynamoDbPretenderClient();

    // Get the encryption helper from the component
    encryptionHelper = component.encryptionHelper();

    // Configure encryption for the table
    encryptionHelper.setEncryptionConfig(
        ImmutableEncryptionConfig.builder()
            .tableName(TABLE_NAME)
            .enabled(true)
            .addEncryptedAttributes("ssn", "creditCard", "medicalRecord")
            .build()
    );

    // Create table
    client.createTable(CreateTableRequest.builder()
        .tableName(TABLE_NAME)
        .keySchema(
            KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName(HASH_KEY).attributeType(ScalarAttributeType.S).build()
        )
        .build());
  }

  @Test
  void putItem_getItem_encryptsAndDecryptsSensitiveAttributes() {
    // Given an item with sensitive attributes
    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user123").build(),
        "name", AttributeValue.builder().s("John Doe").build(),
        "ssn", AttributeValue.builder().s("123-45-6789").build(),
        "creditCard", AttributeValue.builder().s("4111-1111-1111-1111").build(),
        "email", AttributeValue.builder().s("john@example.com").build()
    );

    // When we put the item
    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item)
        .build());

    // And retrieve it
    final GetItemResponse response = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(HASH_KEY, AttributeValue.builder().s("user123").build()))
        .build());

    // Then the item should be returned with decrypted values
    assertThat(response.hasItem()).isTrue();
    final Map<String, AttributeValue> retrievedItem = response.item();

    // Non-encrypted attributes should match
    assertThat(retrievedItem.get("name").s()).isEqualTo("John Doe");
    assertThat(retrievedItem.get("email").s()).isEqualTo("john@example.com");

    // Encrypted attributes should also match (decrypted on retrieval)
    assertThat(retrievedItem.get("ssn").s()).isEqualTo("123-45-6789");
    assertThat(retrievedItem.get("creditCard").s()).isEqualTo("4111-1111-1111-1111");
  }

  @Test
  void putItem_withoutEncryption_storesPlaintext() {
    // Given a table without encryption configured
    final String unencryptedTable = "UnencryptedTable";

    client.createTable(CreateTableRequest.builder()
        .tableName(unencryptedTable)
        .keySchema(
            KeySchemaElement.builder().attributeName(HASH_KEY).keyType(KeyType.HASH).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName(HASH_KEY).attributeType(ScalarAttributeType.S).build()
        )
        .build());

    final Map<String, AttributeValue> item = Map.of(
        HASH_KEY, AttributeValue.builder().s("user456").build(),
        "data", AttributeValue.builder().s("plaintext data").build()
    );

    // When we put the item without encryption
    client.putItem(PutItemRequest.builder()
        .tableName(unencryptedTable)
        .item(item)
        .build());

    // Then we can retrieve it normally
    final GetItemResponse response = client.getItem(GetItemRequest.builder()
        .tableName(unencryptedTable)
        .key(Map.of(HASH_KEY, AttributeValue.builder().s("user456").build()))
        .build());

    assertThat(response.hasItem()).isTrue();
    assertThat(response.item().get("data").s()).isEqualTo("plaintext data");
  }

  @Test
  void encryptionConfig_canBeUpdated() {
    // Given a table with encryption for specific attributes
    encryptionHelper.setEncryptionConfig(
        ImmutableEncryptionConfig.builder()
            .tableName(TABLE_NAME)
            .enabled(true)
            .encryptedAttributes(Set.of("ssn"))
            .build()
    );

    final Map<String, AttributeValue> item1 = Map.of(
        HASH_KEY, AttributeValue.builder().s("user789").build(),
        "ssn", AttributeValue.builder().s("987-65-4321").build(),
        "email", AttributeValue.builder().s("test@example.com").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item1)
        .build());

    // When we update the encryption config to include email
    encryptionHelper.setEncryptionConfig(
        ImmutableEncryptionConfig.builder()
            .tableName(TABLE_NAME)
            .enabled(true)
            .encryptedAttributes(Set.of("ssn", "email"))
            .build()
    );

    final Map<String, AttributeValue> item2 = Map.of(
        HASH_KEY, AttributeValue.builder().s("user790").build(),
        "ssn", AttributeValue.builder().s("111-22-3333").build(),
        "email", AttributeValue.builder().s("newuser@example.com").build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName(TABLE_NAME)
        .item(item2)
        .build());

    // Then both items should be retrievable
    final GetItemResponse response1 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(HASH_KEY, AttributeValue.builder().s("user789").build()))
        .build());

    final GetItemResponse response2 = client.getItem(GetItemRequest.builder()
        .tableName(TABLE_NAME)
        .key(Map.of(HASH_KEY, AttributeValue.builder().s("user790").build()))
        .build());

    assertThat(response1.item().get("ssn").s()).isEqualTo("987-65-4321");
    assertThat(response1.item().get("email").s()).isEqualTo("test@example.com");

    assertThat(response2.item().get("ssn").s()).isEqualTo("111-22-3333");
    assertThat(response2.item().get("email").s()).isEqualTo("newuser@example.com");
  }
}
