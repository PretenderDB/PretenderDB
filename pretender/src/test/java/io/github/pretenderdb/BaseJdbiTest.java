/*
 * Copyright (c) 2023. Ned Wolpert
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pretenderdb;

import static io.github.pretenderdb.dagger.PretenderModule.LIQUIBASE_SETUP_XML;

import io.github.pretenderdb.dbu.factory.JdbiFactory;
import io.github.pretenderdb.dbu.liquibase.LiquibaseHelper;
import io.github.pretenderdb.dbu.model.ImmutableDatabase;
import io.github.pretenderdb.dagger.PretenderModule;
import io.github.pretenderdb.dao.GsiListArgumentFactory;
import io.github.pretenderdb.dao.GsiListColumnMapper;
import io.github.pretenderdb.model.Configuration;
import io.github.pretenderdb.model.ImmutableConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseJdbiTest {

  protected Jdbi jdbi;
  protected Configuration configuration;

  @BeforeEach
  void setupJdbi() {
    configuration = ImmutableConfiguration.builder()
        .database(
            ImmutableDatabase.builder()
                .url("jdbc:hsqldb:mem:" + getClass().getSimpleName() + ":" + UUID.randomUUID())
                .username("SA")
                .password("")
                .build()
        ).build();
    jdbi = new JdbiFactory(configuration.database(), new PretenderModule().immutableClasses()).createJdbi();
    new LiquibaseHelper().runLiquibase(jdbi, LIQUIBASE_SETUP_XML);

    // Register custom mappers for GSI list serialization
    final ObjectMapper objectMapper = new ObjectMapper();
    jdbi.registerArgument(new GsiListArgumentFactory(objectMapper));
    jdbi.registerColumnMapper(new GsiListColumnMapper(objectMapper));
  }

  @AfterEach
  void shutdownJdbi() {
    if (jdbi != null) {
      try {
        // Shutdown HSQLDB to release resources
        jdbi.withHandle(handle -> handle.execute("SHUTDOWN"));
      } catch (Exception e) {
        // Ignore shutdown errors
      }
    }
  }

}
