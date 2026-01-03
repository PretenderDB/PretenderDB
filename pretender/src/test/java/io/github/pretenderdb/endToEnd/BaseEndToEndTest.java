package io.github.pretenderdb.endToEnd;

import io.github.pretenderdb.dbu.model.ImmutableDatabase;
import io.github.pretenderdb.dagger.PretenderComponent;
import io.github.pretenderdb.model.Configuration;
import io.github.pretenderdb.model.ImmutableConfiguration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseEndToEndTest {

  protected PretenderComponent component;

  private Configuration configuration() {
    return ImmutableConfiguration.builder()
        .database(
            ImmutableDatabase.builder()
                .url("jdbc:hsqldb:mem:PretenderComponentTest" + UUID.randomUUID())
                .username("SA")
                .password("")
                .build())
        .build();
  }

  @BeforeEach
  void setupComponent() {
    component = PretenderComponent.instance(configuration());
  }

}
