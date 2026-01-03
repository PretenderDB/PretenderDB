package io.github.pretenderdb.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.github.pretenderdb.model.ImmutablePdbStreamRecord;
import io.github.pretenderdb.model.PdbStreamRecord;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

@ExtendWith(MockitoExtension.class)
class StreamRecordConverterTest {

  @Mock
  private AttributeValueConverter attributeValueConverter;

  private StreamRecordConverter converter;

  @BeforeEach
  void setup() {
    converter = new StreamRecordConverter(attributeValueConverter);
  }

  @Test
  void toRecord_insert_createsCorrectRecord() {
    final PdbStreamRecord pdbRecord = createPdbRecord("INSERT", true, false);
    final String streamArn = "arn:aws:dynamodb:us-east-1:000000000000:table/test-table/stream/123";

    setupMocks();

    final Record record = converter.toRecord(pdbRecord, streamArn);

    assertThat(record.eventID()).isEqualTo("test-event-id");
    assertThat(record.eventName()).isEqualTo(OperationType.INSERT);
    assertThat(record.eventVersion()).isEqualTo("1.1");
    assertThat(record.eventSource()).isEqualTo("aws:dynamodb");
    assertThat(record.awsRegion()).isEqualTo("us-east-1");
    assertThat(record.dynamodb()).isNotNull();
  }

  @Test
  void toRecord_modify_createsCorrectRecord() {
    final PdbStreamRecord pdbRecord = createPdbRecord("MODIFY", true, true);
    final String streamArn = "arn:aws:dynamodb:us-east-1:000000000000:table/test-table/stream/123";

    setupMocks();

    final Record record = converter.toRecord(pdbRecord, streamArn);

    assertThat(record.eventName()).isEqualTo(OperationType.MODIFY);
  }

  @Test
  void toRecord_remove_createsCorrectRecord() {
    final PdbStreamRecord pdbRecord = createPdbRecord("REMOVE", false, true);
    final String streamArn = "arn:aws:dynamodb:us-east-1:000000000000:table/test-table/stream/123";

    setupMocks();

    final Record record = converter.toRecord(pdbRecord, streamArn);

    assertThat(record.eventName()).isEqualTo(OperationType.REMOVE);
  }

  @Test
  void toStreamRecord_keysOnly_onlyIncludesKeys() {
    final PdbStreamRecord pdbRecord = createPdbRecord("INSERT", false, false);

    setupMocks();

    final StreamRecord streamRecord = converter.toStreamRecord(pdbRecord);

    assertThat(streamRecord.sequenceNumber()).isEqualTo("123");
    assertThat(streamRecord.sizeBytes()).isEqualTo(100L);
    assertThat(streamRecord.streamViewType()).isEqualTo(StreamViewType.KEYS_ONLY);
    assertThat(streamRecord.keys()).isNotNull();
    // AWS SDK returns empty maps for unset fields instead of null
    assertThat(streamRecord.oldImage()).satisfiesAnyOf(
        image -> assertThat(image).isNull(),
        image -> assertThat(image).isEmpty()
    );
    assertThat(streamRecord.newImage()).satisfiesAnyOf(
        image -> assertThat(image).isNull(),
        image -> assertThat(image).isEmpty()
    );
  }

  @Test
  void toStreamRecord_newImage_includesNewImage() {
    final PdbStreamRecord pdbRecord = createPdbRecord("INSERT", true, false);

    setupMocks();

    final StreamRecord streamRecord = converter.toStreamRecord(pdbRecord);

    assertThat(streamRecord.streamViewType()).isEqualTo(StreamViewType.NEW_IMAGE);
    assertThat(streamRecord.newImage()).isNotNull();
    assertThat(streamRecord.oldImage()).satisfiesAnyOf(
        image -> assertThat(image).isNull(),
        image -> assertThat(image).isEmpty()
    );
  }

  @Test
  void toStreamRecord_oldImage_includesOldImage() {
    final PdbStreamRecord pdbRecord = createPdbRecord("REMOVE", false, true);

    setupMocks();

    final StreamRecord streamRecord = converter.toStreamRecord(pdbRecord);

    assertThat(streamRecord.streamViewType()).isEqualTo(StreamViewType.OLD_IMAGE);
    assertThat(streamRecord.oldImage()).isNotNull();
    assertThat(streamRecord.newImage()).satisfiesAnyOf(
        image -> assertThat(image).isNull(),
        image -> assertThat(image).isEmpty()
    );
  }

  @Test
  void toStreamRecord_newAndOldImages_includesBothImages() {
    final PdbStreamRecord pdbRecord = createPdbRecord("MODIFY", true, true);

    setupMocks();

    final StreamRecord streamRecord = converter.toStreamRecord(pdbRecord);

    assertThat(streamRecord.streamViewType()).isEqualTo(StreamViewType.NEW_AND_OLD_IMAGES);
    assertThat(streamRecord.newImage()).isNotNull();
    assertThat(streamRecord.oldImage()).isNotNull();
  }

  private PdbStreamRecord createPdbRecord(String eventType, boolean includeNewImage, boolean includeOldImage) {
    final ImmutablePdbStreamRecord.Builder builder = ImmutablePdbStreamRecord.builder()
        .sequenceNumber(123L)
        .eventId("test-event-id")
        .eventType(eventType)
        .eventTimestamp(Instant.now())
        .hashKeyValue("test-hash")
        .keysJson("{\"id\":{\"S\":\"test-id\"}}")
        .approximateCreationTime(System.currentTimeMillis())
        .sizeBytes(100)
        .createDate(Instant.now());

    if (includeNewImage) {
      builder.newImageJson("{\"id\":{\"S\":\"test-id\"},\"name\":{\"S\":\"new-value\"}}");
    }

    if (includeOldImage) {
      builder.oldImageJson("{\"id\":{\"S\":\"test-id\"},\"name\":{\"S\":\"old-value\"}}");
    }

    return builder.build();
  }

  private void setupMocks() {
    when(attributeValueConverter.fromJson(anyString())).thenAnswer(invocation -> {
      final String json = invocation.getArgument(0);
      final Map<String, AttributeValue> result = new HashMap<>();

      // Simple mock - return different maps based on JSON content
      if (json.contains("new-value")) {
        result.put("id", AttributeValue.builder().s("test-id").build());
        result.put("name", AttributeValue.builder().s("new-value").build());
      } else if (json.contains("old-value")) {
        result.put("id", AttributeValue.builder().s("test-id").build());
        result.put("name", AttributeValue.builder().s("old-value").build());
      } else {
        result.put("id", AttributeValue.builder().s("test-id").build());
      }

      return result;
    });
  }
}
