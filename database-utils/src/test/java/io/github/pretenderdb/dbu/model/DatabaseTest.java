package io.github.pretenderdb.dbu.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DatabaseTest {

  @Test
  void testPasswordHidden() {
    Database database = ImmutableDatabase.builder()
        .url("jdbc:hsqldb:mem:DatabaseTest")
        .username("SA")
        .password("password")
        .build();
    assertThat(database.password()).isEqualTo("password");
    assertThat(database.toString()).doesNotContain("password");
  }

}