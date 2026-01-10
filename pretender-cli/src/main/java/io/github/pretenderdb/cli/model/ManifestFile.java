package io.github.pretenderdb.cli.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * Represents a single data file entry in manifest-files.json.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableManifestFile.class)
@JsonDeserialize(as = ImmutableManifestFile.class)
public interface ManifestFile {

  /**
   * Number of items in this file.
   */
  long itemCount();

  /**
   * MD5 checksum of the file.
   */
  String md5Checksum();

  /**
   * Relative path to the data file (e.g., "data/item-0001.json.gz").
   */
  String dataFileS3Key();
}
