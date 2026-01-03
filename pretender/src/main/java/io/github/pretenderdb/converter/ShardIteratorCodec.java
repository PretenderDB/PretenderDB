package io.github.pretenderdb.converter;

import io.github.pretenderdb.model.ImmutableShardIterator;
import io.github.pretenderdb.model.ShardIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Codec for encoding/decoding shard iterators to/from Base64 strings.
 * Shard iterators are JSON objects encoded as Base64 for use in the DynamoDB Streams API.
 */
@Singleton
public class ShardIteratorCodec {

  private static final Logger log = LoggerFactory.getLogger(ShardIteratorCodec.class);

  private final ObjectMapper objectMapper;

  /**
   * Instantiates a new Shard iterator codec.
   *
   * @param objectMapper the object mapper
   */
  @Inject
  public ShardIteratorCodec(final ObjectMapper objectMapper) {
    log.info("ShardIteratorCodec({})", objectMapper);
    this.objectMapper = objectMapper;
  }

  /**
   * Encodes a shard iterator to a Base64 string.
   *
   * @param iterator the iterator
   * @return the encoded string
   */
  public String encode(final ShardIterator iterator) {
    log.trace("encode({})", iterator);
    try {
      final String json = objectMapper.writeValueAsString(iterator);
      final String encoded = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
      log.debug("Encoded shard iterator: {} -> {}", iterator, encoded);
      return encoded;
    } catch (Exception e) {
      log.error("Failed to encode shard iterator: {}", iterator, e);
      throw new IllegalArgumentException("Failed to encode shard iterator", e);
    }
  }

  /**
   * Decodes a Base64 string to a shard iterator.
   *
   * @param encoded the encoded string
   * @return the shard iterator
   */
  public ShardIterator decode(final String encoded) {
    log.trace("decode({})", encoded);
    try {
      final byte[] decoded = Base64.getDecoder().decode(encoded);
      final String json = new String(decoded, StandardCharsets.UTF_8);
      final ShardIterator iterator = objectMapper.readValue(json, ImmutableShardIterator.class);
      log.debug("Decoded shard iterator: {} -> {}", encoded, iterator);
      return iterator;
    } catch (Exception e) {
      log.error("Failed to decode shard iterator: {}", encoded, e);
      throw new IllegalArgumentException("Invalid shard iterator", e);
    }
  }
}
