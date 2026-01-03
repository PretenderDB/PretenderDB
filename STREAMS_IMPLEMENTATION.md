# DynamoDB Streams Implementation for Pretender

## Overview

This document describes the DynamoDB Streams implementation in the Pretender module, which provides a local, SQL-backed implementation of AWS DynamoDB Streams for development and testing purposes.

## Architecture

### Storage Model

Stream records are stored in dynamically-created PostgreSQL/HSQLDB tables:

```sql
CREATE TABLE pdb_stream_<tablename> (
  sequence_number BIGSERIAL PRIMARY KEY,
  event_id VARCHAR(128) NOT NULL UNIQUE,
  event_type VARCHAR(16) NOT NULL,           -- INSERT, MODIFY, REMOVE
  event_timestamp TIMESTAMP NOT NULL,
  hash_key_value VARCHAR(2048) NOT NULL,
  sort_key_value VARCHAR(2048),
  keys_json JSONB/CLOB NOT NULL,            -- Key attributes only
  old_image_json JSONB/CLOB,                -- Old item (for MODIFY, REMOVE)
  new_image_json JSONB/CLOB,                -- New item (for INSERT, MODIFY)
  approximate_creation_time BIGINT NOT NULL,
  size_bytes INT NOT NULL,
  create_date TIMESTAMP NOT NULL
)
```

### Component Structure

#### Phase 1: Foundation (Models)
- **PdbStreamRecord** (`model/PdbStreamRecord.java`) - Immutable model for stream records
- **ShardIterator** (`model/ShardIterator.java`) - Internal model for shard iterator state

#### Phase 2: Change Capture
- **StreamCaptureHelper** (`helper/StreamCaptureHelper.java`) - Captures INSERT/MODIFY/REMOVE events
- **PdbStreamTableManager** (`manager/PdbStreamTableManager.java`) - Creates/manages stream tables
- **PdbStreamDao** (`dao/PdbStreamDao.java`) - JDBI-based data access for stream records
- **PdbMetadataDao** (enhanced) - Added stream configuration fields and queries

#### Phase 3: Consumption API
- **ShardIteratorCodec** (`converter/ShardIteratorCodec.java`) - Base64 encoding/decoding of shard iterators
- **StreamRecordConverter** (`converter/StreamRecordConverter.java`) - Converts internal records to AWS SDK format
- **PdbStreamManager** (`manager/PdbStreamManager.java`) - Business logic for all stream operations
- **DynamoDbStreamsPretenderClient** (`DynamoDbStreamsPretenderClient.java`) - Client implementation

#### Phase 4: Cleanup & Maintenance
- **StreamCleanupService** (`service/StreamCleanupService.java`) - Background service for 24-hour retention
- Scheduled cleanup runs every 60 minutes (configurable)
- Deletes records older than 24 hours (matching AWS behavior)

## Features

### Stream Configuration
- Enable/disable streams per table
- Four StreamViewType options:
  - `KEYS_ONLY` - Only key attributes
  - `NEW_IMAGE` - New item state
  - `OLD_IMAGE` - Old item state (for updates/deletes)
  - `NEW_AND_OLD_IMAGES` - Both old and new states

### Event Capture
- **INSERT** - Captured when new items are created via putItem or updateItem
- **MODIFY** - Captured when existing items are updated
- **REMOVE** - Captured when items are deleted

### Stream Consumption
- `describeStream()` - Get stream metadata and shard information
- `listStreams()` - List all tables with streams enabled
- `getShardIterator()` - Initialize iteration with:
  - `TRIM_HORIZON` - Start from oldest available record
  - `LATEST` - Start from most recent record
  - `AT_SEQUENCE_NUMBER` - Start at specific sequence
  - `AFTER_SEQUENCE_NUMBER` - Start after specific sequence
- `getRecords()` - Retrieve stream records with pagination

### Retention and Cleanup
- Automatic 24-hour retention (AWS behavior)
- Background cleanup service (start/stop lifecycle)
- Manual cleanup via `runCleanup()`

## Simplified Design Decisions

### Single Shard Model
- Implementation uses a single shard (`shard-00000`) for simplicity
- Sufficient for local development/testing scenarios
- Avoids complexity of shard splitting/merging

### Sequence Number Implementation
- Auto-incrementing BIGSERIAL/IDENTITY column
- Guarantees strictly increasing sequence numbers
- Simplifies iteration logic

### Stream ARN Format
```
arn:aws:dynamodb:us-east-1:000000000000:table/{tableName}/stream/{timestamp}
```

## Integration with Item Operations

Stream capture is automatically integrated into:
- `PdbItemManager.putItem()` - Lines 147-156
- `PdbItemManager.updateItem()` - Lines 262-269
- `PdbItemManager.deleteItem()` - Lines 344-347

## Usage Example

```java
// Enable streams on a table
tableManager.enableStream("MyTable", "NEW_AND_OLD_IMAGES");

// Perform operations (automatically captured)
dynamoClient.putItem(PutItemRequest.builder()
    .tableName("MyTable")
    .item(Map.of("id", AttributeValue.builder().s("123").build()))
    .build());

// List streams
ListStreamsResponse streams = streamsClient.listStreams(
    ListStreamsRequest.builder().build()
);

// Get shard iterator
GetShardIteratorResponse iteratorResponse = streamsClient.getShardIterator(
    GetShardIteratorRequest.builder()
        .streamArn(streamArn)
        .shardId("shard-00000")
        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
        .build()
);

// Read records
GetRecordsResponse recordsResponse = streamsClient.getRecords(
    GetRecordsRequest.builder()
        .shardIterator(iteratorResponse.shardIterator())
        .limit(100)
        .build()
);

// Process records
for (Record record : recordsResponse.records()) {
    System.out.println("Event: " + record.eventName());
    System.out.println("Keys: " + record.dynamodb().keys());
    if (record.dynamodb().newImage() != null) {
        System.out.println("New Image: " + record.dynamodb().newImage());
    }
}
```

## Testing

### Unit Tests
- `StreamCaptureHelperTest` - Event capture logic
- `ShardIteratorCodecTest` - Encoding/decoding
- `StreamRecordConverterTest` - Record conversion
- `PdbStreamManagerTest` - Business logic
- `StreamCleanupServiceTest` - Cleanup service

### Integration Tests
- `StreamsIntegrationTest` - End-to-end streams functionality (15 tests)
- `StreamCleanupIntegrationTest` - Cleanup service integration (2 tests)

### Test Coverage
- All stream-related functionality covered
- Event capture for all operation types
- All StreamViewType variations
- Pagination and shard iterator types
- Cleanup and retention

## Configuration

### Cleanup Service
The cleanup service can be configured with a custom interval:

```java
new StreamCleanupService(metadataDao, streamDao, streamTableManager, clock,
    customIntervalSeconds);
```

Default interval: 3600 seconds (60 minutes)
Retention period: 24 hours (fixed, matching AWS)

## Limitations and Future Enhancements

### Current Limitations
- Single shard only (no shard splitting)
- No support for shard merging
- No support for parallel scan across multiple shards
- Stream records not included in item table backups

### Potential Enhancements
- Multi-shard support for high-throughput tables
- Shard splitting based on throughput
- Integration with external stream processors
- Metrics and monitoring for stream lag

## Database Support

### PostgreSQL
- Uses JSONB for efficient JSON storage
- Uses BIGSERIAL for auto-increment sequence numbers
- Supports GIN indexes on JSONB columns

### HSQLDB
- Uses CLOB for JSON storage
- Uses BIGINT IDENTITY for sequence numbers
- Suitable for in-memory testing

## Performance Considerations

### Indexes
- Primary key on sequence_number for fast sequential access
- Index on event_timestamp for efficient cleanup queries
- Index on sequence_number for getRecords() performance

### Cleanup
- Cleanup runs every 60 minutes by default
- Deletes in batches using timestamp-based WHERE clause
- Minimal impact on stream read operations

## Error Handling

- `ResourceNotFoundException` - Table/stream not found
- Graceful handling of disabled streams (returns empty records)
- Transaction isolation prevents record skipping
- Automatic retry for transient database errors (via JDBI)

## Compatibility

The implementation aims for compatibility with AWS SDK v2 DynamoDB Streams API:
- `software.amazon.awssdk.services.dynamodb.model.*`
- Same request/response objects
- Same exception types
- Same semantics for iterator types and pagination

## Migration Notes

When migrating from this implementation to actual DynamoDB Streams:
1. Replace `DynamoDbStreamsPretenderClient` with AWS SDK client
2. Update stream ARN format (if hardcoded)
3. Consider shard count for production workloads
4. Review retention period requirements (24h is AWS default)
