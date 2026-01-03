package io.github.pretenderdb.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PdbStreamRecordTest {

  private ObjectMapper objectMapper;

  @BeforeEach
  void setup() {
    objectMapper = new ObjectMapper();
    objectMapper.findAndRegisterModules(); // Register JavaTimeModule for Instant
  }

  @Test
  void builder_withAllFields_buildsSuccessfully() {
    final Instant now = Instant.now();
    final long epochMillis = now.toEpochMilli();

    final PdbStreamRecord record = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(12345L)
        .eventId("event-uuid-123")
        .eventType("INSERT")
        .eventTimestamp(now)
        .hashKeyValue("user-123")
        .sortKeyValue("2024-01-01")
        .keysJson("{\"userId\":{\"S\":\"user-123\"}}")
        .newImageJson("{\"userId\":{\"S\":\"user-123\"},\"name\":{\"S\":\"John\"}}")
        .approximateCreationTime(epochMillis)
        .sizeBytes(256)
        .createDate(now)
        .build();

    assertThat(record.sequenceNumber()).isEqualTo(12345L);
    assertThat(record.eventId()).isEqualTo("event-uuid-123");
    assertThat(record.eventType()).isEqualTo("INSERT");
    assertThat(record.eventTimestamp()).isEqualTo(now);
    assertThat(record.hashKeyValue()).isEqualTo("user-123");
    assertThat(record.sortKeyValue()).isPresent().contains("2024-01-01");
    assertThat(record.keysJson()).isEqualTo("{\"userId\":{\"S\":\"user-123\"}}");
    assertThat(record.oldImageJson()).isEmpty();
    assertThat(record.newImageJson()).isPresent().contains("{\"userId\":{\"S\":\"user-123\"},\"name\":{\"S\":\"John\"}}");
    assertThat(record.approximateCreationTime()).isEqualTo(epochMillis);
    assertThat(record.sizeBytes()).isEqualTo(256);
    assertThat(record.createDate()).isEqualTo(now);
  }

  @Test
  void builder_withoutOptionalFields_buildsSuccessfully() {
    final Instant now = Instant.now();

    final PdbStreamRecord record = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(1L)
        .eventId("event-uuid-456")
        .eventType("REMOVE")
        .eventTimestamp(now)
        .hashKeyValue("user-456")
        .keysJson("{\"userId\":{\"S\":\"user-456\"}}")
        .oldImageJson("{\"userId\":{\"S\":\"user-456\"}}")
        .approximateCreationTime(now.toEpochMilli())
        .sizeBytes(128)
        .createDate(now)
        .build();

    assertThat(record.sortKeyValue()).isEmpty();
    assertThat(record.newImageJson()).isEmpty();
    assertThat(record.oldImageJson()).isPresent();
  }

  @Test
  void builder_modifyEvent_hasBothImages() {
    final Instant now = Instant.now();

    final PdbStreamRecord record = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(2L)
        .eventId("event-uuid-789")
        .eventType("MODIFY")
        .eventTimestamp(now)
        .hashKeyValue("user-789")
        .keysJson("{\"userId\":{\"S\":\"user-789\"}}")
        .oldImageJson("{\"userId\":{\"S\":\"user-789\"},\"count\":{\"N\":\"5\"}}")
        .newImageJson("{\"userId\":{\"S\":\"user-789\"},\"count\":{\"N\":\"10\"}}")
        .approximateCreationTime(now.toEpochMilli())
        .sizeBytes(512)
        .createDate(now)
        .build();

    assertThat(record.eventType()).isEqualTo("MODIFY");
    assertThat(record.oldImageJson()).isPresent();
    assertThat(record.newImageJson()).isPresent();
  }

  @Test
  void jsonSerialization_roundTrip() throws Exception {
    final Instant now = Instant.now();

    final PdbStreamRecord original = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(100L)
        .eventId("test-event")
        .eventType("INSERT")
        .eventTimestamp(now)
        .hashKeyValue("hash-key")
        .sortKeyValue("sort-key")
        .keysJson("{\"key\":\"value\"}")
        .newImageJson("{\"data\":\"test\"}")
        .approximateCreationTime(now.toEpochMilli())
        .sizeBytes(200)
        .createDate(now)
        .build();

    // Serialize to JSON
    final String json = objectMapper.writeValueAsString(original);
    assertThat(json).isNotNull();

    // Deserialize from JSON
    final PdbStreamRecord deserialized = objectMapper.readValue(json, PdbStreamRecord.class);

    // Verify all fields match
    assertThat(deserialized.sequenceNumber()).isEqualTo(original.sequenceNumber());
    assertThat(deserialized.eventId()).isEqualTo(original.eventId());
    assertThat(deserialized.eventType()).isEqualTo(original.eventType());
    assertThat(deserialized.eventTimestamp()).isEqualTo(original.eventTimestamp());
    assertThat(deserialized.hashKeyValue()).isEqualTo(original.hashKeyValue());
    assertThat(deserialized.sortKeyValue()).isEqualTo(original.sortKeyValue());
    assertThat(deserialized.keysJson()).isEqualTo(original.keysJson());
    assertThat(deserialized.newImageJson()).isEqualTo(original.newImageJson());
    assertThat(deserialized.approximateCreationTime()).isEqualTo(original.approximateCreationTime());
    assertThat(deserialized.sizeBytes()).isEqualTo(original.sizeBytes());
    assertThat(deserialized.createDate()).isEqualTo(original.createDate());
  }

  @Test
  void immutability_cannotModifyAfterCreation() {
    final PdbStreamRecord record = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(1L)
        .eventId("test")
        .eventType("INSERT")
        .eventTimestamp(Instant.now())
        .hashKeyValue("key")
        .keysJson("{}")
        .approximateCreationTime(System.currentTimeMillis())
        .sizeBytes(100)
        .createDate(Instant.now())
        .build();

    // Immutables should provide immutable instance
    assertThat(record).isInstanceOf(ImmutablePdbStreamRecord.class);
  }
}
