package io.github.pretenderdb.dbu.liquibase;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pretenderdb.dbu.factory.JdbiFactory;
import io.github.pretenderdb.dbu.model.Database;
import io.github.pretenderdb.dbu.model.ImmutableDatabase;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Test;

class LiquibaseHelperTest  {

  @Test
  void runLiquibase() {
    Database database = ImmutableDatabase.builder()
        .url("jdbc:hsqldb:mem:" + getClass().getSimpleName() + ":" + UUID.randomUUID())
        .username("SA")
        .password("")
        .build();
    Jdbi jdbi = new JdbiFactory(database, Set.of()).createJdbi();
    new LiquibaseHelper().runLiquibase(jdbi, "liquibase/liquibase-setup.xml");
    final List<Map<String, Object>> list = jdbi.withHandle(handle -> handle.createQuery("select * from PDB_TABLE").mapToMap().list());
    assertThat(list).isEmpty();
  }

}