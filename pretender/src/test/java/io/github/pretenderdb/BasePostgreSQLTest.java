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
import io.github.pretenderdb.dagger.PretenderComponent;
import io.github.pretenderdb.dao.GsiListArgumentFactory;
import io.github.pretenderdb.dao.GsiListColumnMapper;
import io.github.pretenderdb.model.Configuration;
import io.github.pretenderdb.model.ImmutableConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base test class using Testcontainers PostgreSQL for integration tests.
 * This validates that all features work correctly with PostgreSQL (the production database).
 */
@Testcontainers
public abstract class BasePostgreSQLTest {

  @Container
  protected static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
      new PostgreSQLContainer<>("postgres:17-alpine")
          .withDatabaseName("pretender_test")
          .withUsername("test")
          .withPassword("test")
          .withReuse(true);

  protected Jdbi jdbi;
  protected Configuration configuration;
  protected PretenderComponent component;

  @BeforeEach
  void setupPostgreSQL() {
    configuration = ImmutableConfiguration.builder()
        .database(
            ImmutableDatabase.builder()
                .url(POSTGRES_CONTAINER.getJdbcUrl())
                .username(POSTGRES_CONTAINER.getUsername())
                .password(POSTGRES_CONTAINER.getPassword())
                .build()
        ).build();
    jdbi = new JdbiFactory(configuration.database(), new PretenderModule().immutableClasses()).createJdbi();
    new LiquibaseHelper().runLiquibase(jdbi, LIQUIBASE_SETUP_XML);

    // Register custom mappers for GSI list serialization
    final ObjectMapper objectMapper = new ObjectMapper();
    jdbi.registerArgument(new GsiListArgumentFactory(objectMapper));
    jdbi.registerColumnMapper(new GsiListColumnMapper(objectMapper));

    // Initialize component for tests that need it
    component = PretenderComponent.instance(configuration);
  }

  @AfterEach
  void cleanupPostgreSQL() {
    if (jdbi != null) {
      try {
        // Drop all tables to clean up for next test
        jdbi.withHandle(handle -> {
          // Drop all tables in the public schema
          handle.execute("DROP SCHEMA public CASCADE");
          handle.execute("CREATE SCHEMA public");
          return null;
        });
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }
  }
}
