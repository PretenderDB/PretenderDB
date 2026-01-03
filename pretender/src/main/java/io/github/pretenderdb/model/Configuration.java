package io.github.pretenderdb.model;

import io.github.pretenderdb.dbu.model.Database;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * The interface Pretender configuration.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableConfiguration.class)
@JsonDeserialize(builder = ImmutableConfiguration.Builder.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface Configuration {

  /**
   * Database database.
   *
   * @return the database
   */
  Database database();

}
