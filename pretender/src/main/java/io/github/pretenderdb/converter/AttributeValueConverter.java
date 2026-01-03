package io.github.pretenderdb.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;

/**
 * Converts between DynamoDB AttributeValue maps and JSON strings.
 * Handles all AttributeValue types: S, N, B, SS, NS, BS, M, L, BOOL, NULL.
 */
@Singleton
public class AttributeValueConverter {

  private static final Logger log = LoggerFactory.getLogger(AttributeValueConverter.class);

  private final ObjectMapper objectMapper;

  /**
   * Instantiates a new Attribute value converter.
   *
   * @param objectMapper the object mapper
   */
  @Inject
  public AttributeValueConverter(final ObjectMapper objectMapper) {
    log.info("AttributeValueConverter({})", objectMapper);
    this.objectMapper = objectMapper;
  }

  /**
   * Converts a map of AttributeValues to a JSON string.
   *
   * @param attributes the attributes
   * @return the json string
   */
  public String toJson(final Map<String, AttributeValue> attributes) {
    log.trace("toJson({})", attributes);
    try {
      // Convert AttributeValue map to a serializable format
      final Map<String, Object> serializableMap = attributes.entrySet().stream()
          .collect(java.util.stream.Collectors.toMap(
              Map.Entry::getKey,
              entry -> convertAttributeValueToObject(entry.getValue())
          ));
      return objectMapper.writeValueAsString(serializableMap);
    } catch (JsonProcessingException e) {
      log.error("Failed to serialize attributes to JSON", e);
      throw InternalServerErrorException.builder()
          .message("Failed to serialize attributes")
          .cause(e)
          .build();
    }
  }

  /**
   * Converts a JSON string to a map of AttributeValues.
   *
   * @param json the json
   * @return the attribute value map
   */
  @SuppressWarnings("unchecked")
  public Map<String, AttributeValue> fromJson(final String json) {
    log.trace("fromJson({})", json);
    try {
      final Map<String, Object> deserializedMap = objectMapper.readValue(json, Map.class);
      return deserializedMap.entrySet().stream()
          .collect(java.util.stream.Collectors.toMap(
              Map.Entry::getKey,
              entry -> convertObjectToAttributeValue(entry.getValue())
          ));
    } catch (JsonProcessingException e) {
      log.error("Failed to deserialize JSON to attributes", e);
      throw InternalServerErrorException.builder()
          .message("Failed to deserialize attributes")
          .cause(e)
          .build();
    }
  }

  /**
   * Extracts the value of a key attribute as a string.
   *
   * @param item    the item
   * @param keyName the key name
   * @return the key value as string
   */
  public String extractKeyValue(final Map<String, AttributeValue> item, final String keyName) {
    log.trace("extractKeyValue({}, {})", item, keyName);
    final AttributeValue value = item.get(keyName);
    if (value == null) {
      throw new IllegalArgumentException("Key attribute '" + keyName + "' not found in item");
    }

    // Extract the value based on type
    if (value.s() != null) {
      return value.s();
    } else if (value.n() != null) {
      return value.n();
    } else if (value.b() != null) {
      return value.b().asUtf8String();
    } else {
      throw new IllegalArgumentException("Key attribute '" + keyName + "' must be a scalar type (S, N, or B)");
    }
  }

  /**
   * Converts an AttributeValue to a JSON-serializable object.
   */
  @SuppressWarnings("unchecked")
  private Object convertAttributeValueToObject(final AttributeValue value) {
    // String
    if (value.s() != null) {
      return Map.of("S", value.s());
    }
    // Number
    if (value.n() != null) {
      return Map.of("N", value.n());
    }
    // Binary
    if (value.b() != null) {
      return Map.of("B", value.b().asByteArray());
    }
    // Boolean
    if (value.bool() != null) {
      return Map.of("BOOL", value.bool());
    }
    // Null
    if (value.nul() != null && value.nul()) {
      return Map.of("NULL", true);
    }
    // String Set
    if (value.hasSs()) {
      return Map.of("SS", value.ss());
    }
    // Number Set
    if (value.hasNs()) {
      return Map.of("NS", value.ns());
    }
    // Binary Set
    if (value.hasBs()) {
      return Map.of("BS", value.bs().stream().map(SdkBytes::asByteArray).toList());
    }
    // List
    if (value.hasL()) {
      return Map.of("L", value.l().stream()
          .map(this::convertAttributeValueToObject)
          .toList());
    }
    // Map
    if (value.hasM()) {
      return Map.of("M", value.m().entrySet().stream()
          .collect(java.util.stream.Collectors.toMap(
              Map.Entry::getKey,
              entry -> convertAttributeValueToObject(entry.getValue())
          )));
    }

    throw new IllegalArgumentException("Unsupported AttributeValue type: " + value);
  }

  /**
   * Converts a JSON-deserialized object back to an AttributeValue.
   */
  @SuppressWarnings("unchecked")
  private AttributeValue convertObjectToAttributeValue(final Object obj) {
    if (!(obj instanceof Map)) {
      throw new IllegalArgumentException("Expected Map for AttributeValue, got: " + obj.getClass());
    }

    final Map<String, Object> map = (Map<String, Object>) obj;
    if (map.size() != 1) {
      throw new IllegalArgumentException("AttributeValue map must have exactly one entry");
    }

    final Map.Entry<String, Object> entry = map.entrySet().iterator().next();
    final String type = entry.getKey();
    final Object value = entry.getValue();

    return switch (type) {
      case "S" -> AttributeValue.builder().s((String) value).build();
      case "N" -> AttributeValue.builder().n((String) value).build();
      case "B" -> {
        final byte[] bytes = value instanceof byte[] ? (byte[]) value :
            objectMapper.convertValue(value, byte[].class);
        yield AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
      }
      case "BOOL" -> AttributeValue.builder().bool((Boolean) value).build();
      case "NULL" -> AttributeValue.builder().nul((Boolean) value).build();
      case "SS" -> AttributeValue.builder().ss((java.util.List<String>) value).build();
      case "NS" -> AttributeValue.builder().ns((java.util.List<String>) value).build();
      case "BS" -> {
        final java.util.List<byte[]> bytesList = ((java.util.List<?>) value).stream()
            .map(item -> item instanceof byte[] ? (byte[]) item :
                objectMapper.convertValue(item, byte[].class))
            .toList();
        yield AttributeValue.builder().bs(bytesList.stream()
            .map(SdkBytes::fromByteArray)
            .toList()).build();
      }
      case "L" -> AttributeValue.builder().l(
          ((java.util.List<?>) value).stream()
              .map(this::convertObjectToAttributeValue)
              .toList()
      ).build();
      case "M" -> AttributeValue.builder().m(
          ((Map<String, Object>) value).entrySet().stream()
              .collect(java.util.stream.Collectors.toMap(
                  Map.Entry::getKey,
                  e -> convertObjectToAttributeValue(e.getValue())
              ))
      ).build();
      default -> throw new IllegalArgumentException("Unknown AttributeValue type: " + type);
    };
  }
}
