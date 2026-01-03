package io.github.pretenderdb.dbu.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * The interface Pretender configuration.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableDatabase.class)
@JsonDeserialize(builder = ImmutableDatabase.Builder.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface Database {

  /**
   * Database url string.
   *
   * @return the string
   */
  String url();

  /**
   * Database username string.
   *
   * @return the string
   */
  String username();

  /**
   * Database password string.
   *
   * @return the string
   */
  @Value.Redacted
  String password();

  /**
   * Use postgresql boolean.
   *
   * @return the boolean
   */
  @Value.Derived
  default boolean usePostgresql() {
    return url().startsWith("jdbc:postgresql");
  }

}
