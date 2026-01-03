package io.github.pretenderdb.converter;

import io.github.pretenderdb.model.PdbStreamRecord;
import java.time.Instant;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;

/**
 * Converter for transforming PdbStreamRecord to AWS SDK Record and StreamRecord objects.
 */
@Singleton
public class StreamRecordConverter {

  private static final Logger log = LoggerFactory.getLogger(StreamRecordConverter.class);

  private final AttributeValueConverter attributeValueConverter;

  /**
   * Instantiates a new Stream record converter.
   *
   * @param attributeValueConverter the attribute value converter
   */
  @Inject
  public StreamRecordConverter(final AttributeValueConverter attributeValueConverter) {
    log.info("StreamRecordConverter({})", attributeValueConverter);
    this.attributeValueConverter = attributeValueConverter;
  }

  /**
   * Converts a PdbStreamRecord to an AWS SDK Record.
   *
   * @param pdbRecord the pdb stream record
   * @param streamArn the stream ARN
   * @return the AWS SDK Record
   */
  public Record toRecord(final PdbStreamRecord pdbRecord, final String streamArn) {
    log.trace("toRecord({}, {})", pdbRecord, streamArn);

    final StreamRecord streamRecord = toStreamRecord(pdbRecord);

    final Record.Builder recordBuilder = Record.builder()
        .eventID(pdbRecord.eventId())
        .eventName(toOperationType(pdbRecord.eventType()))
        .eventVersion("1.1")
        .eventSource("aws:dynamodb")
        .awsRegion("us-east-1")
        .dynamodb(streamRecord);

    return recordBuilder.build();
  }

  /**
   * Converts a PdbStreamRecord to an AWS SDK StreamRecord.
   *
   * @param pdbRecord the pdb stream record
   * @return the AWS SDK StreamRecord
   */
  public StreamRecord toStreamRecord(final PdbStreamRecord pdbRecord) {
    log.trace("toStreamRecord({})", pdbRecord);

    // Parse keys from JSON
    final Map<String, AttributeValue> keys = attributeValueConverter.fromJson(pdbRecord.keysJson());

    final StreamRecord.Builder streamBuilder = StreamRecord.builder()
        .sequenceNumber(String.valueOf(pdbRecord.sequenceNumber()))
        .sizeBytes((long) pdbRecord.sizeBytes())
        .streamViewType(deriveStreamViewType(pdbRecord))
        .keys(keys)
        .approximateCreationDateTime(Instant.ofEpochMilli(pdbRecord.approximateCreationTime()));

    // Add old image if present
    pdbRecord.oldImageJson().ifPresent(json -> {
      final Map<String, AttributeValue> oldImage = attributeValueConverter.fromJson(json);
      streamBuilder.oldImage(oldImage);
    });

    // Add new image if present
    pdbRecord.newImageJson().ifPresent(json -> {
      final Map<String, AttributeValue> newImage = attributeValueConverter.fromJson(json);
      streamBuilder.newImage(newImage);
    });

    return streamBuilder.build();
  }

  /**
   * Converts event type string to OperationType enum.
   *
   * @param eventType the event type (INSERT, MODIFY, REMOVE)
   * @return the operation type
   */
  private OperationType toOperationType(final String eventType) {
    return switch (eventType) {
      case "INSERT" -> OperationType.INSERT;
      case "MODIFY" -> OperationType.MODIFY;
      case "REMOVE" -> OperationType.REMOVE;
      default -> throw new IllegalArgumentException("Unknown event type: " + eventType);
    };
  }

  /**
   * Derives the StreamViewType from the presence of old/new images.
   *
   * @param pdbRecord the pdb stream record
   * @return the stream view type enum
   */
  private software.amazon.awssdk.services.dynamodb.model.StreamViewType deriveStreamViewType(final PdbStreamRecord pdbRecord) {
    final boolean hasOldImage = pdbRecord.oldImageJson().isPresent();
    final boolean hasNewImage = pdbRecord.newImageJson().isPresent();

    if (hasOldImage && hasNewImage) {
      return software.amazon.awssdk.services.dynamodb.model.StreamViewType.NEW_AND_OLD_IMAGES;
    } else if (hasNewImage) {
      return software.amazon.awssdk.services.dynamodb.model.StreamViewType.NEW_IMAGE;
    } else if (hasOldImage) {
      return software.amazon.awssdk.services.dynamodb.model.StreamViewType.OLD_IMAGE;
    } else {
      return software.amazon.awssdk.services.dynamodb.model.StreamViewType.KEYS_ONLY;
    }
  }
}
