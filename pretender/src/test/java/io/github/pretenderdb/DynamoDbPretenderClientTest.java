package io.github.pretenderdb;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pretenderdb.converter.PdbTableConverter;
import io.github.pretenderdb.manager.PdbItemManager;
import io.github.pretenderdb.manager.PdbTableManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DynamoDbPretenderClientTest {

  @Mock private PdbTableManager pdbTableManager;
  @Mock private PdbTableConverter pdbTableConverter;
  @Mock private PdbItemManager pdbItemManager;

  private DynamoDbPretenderClient client;

  @BeforeEach
  void setup() {
    client = new DynamoDbPretenderClient(pdbTableManager, pdbTableConverter, pdbItemManager);
  }

  @AfterEach
  void tearDown() {
    client.close();
  }

  @Test
  void serviceName() {
    assertThat(client.serviceName()).isEqualTo("dynamodb");
  }

}