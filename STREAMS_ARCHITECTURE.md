# Pretender Streams Architecture

## Overview

This document explains how Pretender implements DynamoDB Streams, focusing on shard management, architectural decisions, and limitations compared to AWS DynamoDB Streams.

## Table of Contents

1. [DynamoDB Streams Background](#dynamodb-streams-background)
2. [Pretender's Implementation](#pretenders-implementation)
3. [Shard Management](#shard-management)
4. [Limitations and Trade-offs](#limitations-and-trade-offs)
5. [When This Matters](#when-this-matters)
6. [Future Enhancements](#future-enhancements)

---

## DynamoDB Streams Background

### What are DynamoDB Streams?

DynamoDB Streams is a change data capture (CDC) feature that provides a time-ordered sequence of item-level modifications in a DynamoDB table. Applications can read from streams to:

- Replicate data across regions
- Trigger Lambda functions on data changes
- Maintain materialized views
- Implement event sourcing patterns
- Audit data changes

### How Real DynamoDB Streams Work

**Shards** are the fundamental unit of parallelism in DynamoDB Streams:

```
Table: Users
├─ Stream: arn:aws:dynamodb:us-east-1:123456789:table/Users/stream/2024-01-01
    ├─ Shard: shardId-00000 (hash key range: 0000-3FFF)
    │   ├─ SequenceNumberRange: 100-500
    │   └─ ParentShardId: null
    ├─ Shard: shardId-00001 (hash key range: 4000-7FFF)
    │   ├─ SequenceNumberRange: 101-498
    │   └─ ParentShardId: null
    ├─ Shard: shardId-00002 (hash key range: 8000-BFFF)
    │   └─ ParentShardId: shardId-00000 (split from shard 00000)
    └─ Shard: shardId-00003 (hash key range: C000-FFFF)
        └─ ParentShardId: shardId-00001 (split from shard 00001)
```

**Key Characteristics**:

1. **Dynamic Scaling**: Shards automatically split when throughput increases
2. **Hash Key Partitioning**: Each shard handles a range of hash key values
3. **Parent-Child Relationships**: New shards reference their parent shards
4. **Parallel Processing**: Multiple consumers can read different shards concurrently
5. **Ordered Per Shard**: Records within a shard are strictly ordered by sequence number
6. **Eventually Ordered Globally**: Cross-shard ordering is not guaranteed

**Shard Lifecycle**:

```
Initial State:
  Shard-A (OPEN) ──> High traffic detected

Split Event:
  Shard-A (CLOSED) ──┬──> Shard-B (OPEN) handles hash range 0000-7FFF
                     └──> Shard-C (OPEN) handles hash range 8000-FFFF

Merge Event (less common):
  Shard-D (CLOSED) ──┐
                     ├──> Shard-F (OPEN) handles combined range
  Shard-E (CLOSED) ──┘
```

**Shard Iterator Types**:
- `TRIM_HORIZON`: Start from oldest available record (24hr retention)
- `LATEST`: Start after most recent record
- `AT_SEQUENCE_NUMBER`: Start at specific sequence number (inclusive)
- `AFTER_SEQUENCE_NUMBER`: Start after specific sequence number (exclusive)

### Real-World DynamoDB Streams Behavior

**Example: High-Traffic Application**

A production DynamoDB table with 10,000 writes/second might have:
- 50-100 active shards at any given time
- Shards splitting every few hours during peak traffic
- Consumers tracking parent-child relationships to avoid missing records
- Complex shard discovery logic (polling DescribeStream for new shards)

**Consumer Pattern**:
```java
// Real AWS DynamoDB Streams consumer (complex!)
while (true) {
  // 1. Discover all shards
  List<Shard> shards = describeStream().shards();

  // 2. Process each shard in parallel
  for (Shard shard : shards) {
    if (shard.sequenceNumberRange().endingSequenceNumber() == null) {
      // Shard is still OPEN, read from it
      processRecords(shard);
    } else {
      // Shard is CLOSED, check if children exist
      List<Shard> children = findChildShards(shard.shardId());
      for (Shard child : children) {
        processRecords(child);
      }
    }
  }

  // 3. Sleep and poll for new shards
  Thread.sleep(10000);
}
```

---

## Pretender's Implementation

### Single-Shard Model

Pretender uses a **simplified single-shard model** for all streams:

```java
// PdbStreamManager.java
private static final String SHARD_ID = "shard-00000"; // Single shard for all streams
```

**Architecture**:

```
Table: Users
└─ Stream: arn:aws:dynamodb:us-east-1:000000000000:table/Users/stream/1704153600000
    └─ Shard: shard-00000 (ALWAYS OPEN)
        ├─ SequenceNumberRange:
        │   ├─ startingSequenceNumber: 0
        │   └─ endingSequenceNumber: null (never closes)
        ├─ Records stored in pdb_stream_users table
        └─ Auto-incrementing sequence numbers (1, 2, 3, ...)
```

### How It Works

**1. Stream Creation** (when table enables streams):

```java
// PdbTableManager.java - updateStreamSpecification()
if (streamEnabled) {
  String streamArn = generateStreamArn(tableName);
  String streamLabel = generateStreamLabel();

  // Create stream table
  streamTableManager.createStreamTable(tableName);

  // Update metadata
  metadataDao.updateStreamConfig(tableName, true, streamViewType, streamArn, streamLabel);
}
```

**2. Record Insertion** (on item modification):

```java
// PdbItemManager.java - putItem(), updateItem(), deleteItem()
if (metadata.streamEnabled()) {
  PdbStreamRecord record = ImmutablePdbStreamRecord.builder()
      .tableName(tableName)
      .eventType(eventType) // INSERT, MODIFY, REMOVE
      .eventTime(Instant.now())
      .keysJson(...)
      .newImageJson(...)
      .oldImageJson(...)
      .build();

  streamDao.insertStreamRecord(streamTableName, record);
  // Sequence number auto-generated by database (auto-increment)
}
```

**Database Schema**:

```sql
CREATE TABLE pdb_stream_users (
  sequence_number BIGINT PRIMARY KEY AUTO_INCREMENT,
  event_type VARCHAR(32) NOT NULL,
  event_time TIMESTAMP NOT NULL,
  keys_json CLOB NOT NULL,
  old_image_json CLOB,
  new_image_json CLOB
);
```

**3. Reading Records**:

```java
// PdbStreamManager.java - getRecords()
public GetRecordsResponse getRecords(GetRecordsRequest request) {
  ShardIterator iterator = shardIteratorCodec.decode(request.shardIterator());

  // Simple SQL query - no hash key partitioning needed
  List<PdbStreamRecord> records = streamDao.getRecords(
      streamTableName,
      iterator.sequenceNumber(), // Start after this sequence
      request.limit()            // How many to fetch
  );
  // SQL: SELECT * FROM pdb_stream_users
  //      WHERE sequence_number > ?
  //      ORDER BY sequence_number
  //      LIMIT ?

  return GetRecordsResponse.builder()
      .records(convertRecords(records))
      .nextShardIterator(buildNextIterator(records))
      .build();
}
```

**4. Shard Discovery**:

```java
// PdbStreamManager.java - describeStream()
public DescribeStreamResponse describeStream(DescribeStreamRequest request) {
  // Always returns single shard
  Shard shard = Shard.builder()
      .shardId("shard-00000")
      .sequenceNumberRange(SequenceNumberRange.builder()
          .startingSequenceNumber("0")
          .endingSequenceNumber(null) // Never closes
          .build())
      .build();

  return DescribeStreamResponse.builder()
      .streamDescription(StreamDescription.builder()
          .shards(shard) // Single-element list
          .build())
      .build();
}
```

### Key Design Decisions

**Why Single Shard?**

1. **Simplicity**: No complex shard splitting/merging logic
2. **Sequential Guarantees**: All records globally ordered by sequence number
3. **SQL-Friendly**: Leverages database auto-increment for sequence numbers
4. **Sufficient for Testing**: Local development/testing doesn't need AWS-level scale
5. **Deterministic Behavior**: Easier to debug and test

**Trade-offs Accepted**:

| Aspect | Real DynamoDB | Pretender | Impact |
|--------|---------------|-----------|--------|
| Parallel Processing | Yes (multiple shards) | No (single shard) | Lower throughput for consumers |
| Automatic Scaling | Yes (shard splitting) | No (static) | Fixed max throughput |
| Hash Key Distribution | Yes (partitioned) | No (sequential) | All records in one shard |
| Parent-Child Tracking | Yes (complex) | No (not needed) | Simpler consumer code |
| Global Ordering | No (eventually consistent) | Yes (strict) | Stronger guarantees |

---

## Shard Management

### Current Implementation Details

**Shard ID Generation**:
```java
// Always returns "shard-00000"
private static final String SHARD_ID = "shard-00000";
```

**Shard Iterator Encoding**:
```java
// ShardIterator model
{
  "tableName": "users",
  "shardId": "shard-00000",
  "sequenceNumber": 42,
  "type": "AFTER_SEQUENCE_NUMBER"
}
// Encoded as Base64: eyJ0YWJsZU5hbWUiOiJ1c2Vycy...
```

**Sequence Number Management**:
```sql
-- PostgreSQL
CREATE SEQUENCE pdb_stream_users_seq;
ALTER TABLE pdb_stream_users ALTER COLUMN sequence_number
  SET DEFAULT nextval('pdb_stream_users_seq');

-- HSQLDB
CREATE TABLE pdb_stream_users (
  sequence_number BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  ...
);
```

**Trim Horizon Calculation**:
```java
// PdbStreamDao.java
public long getTrimHorizon(String streamTableName) {
  Instant cutoff = Instant.now().minus(24, ChronoUnit.HOURS);

  return jdbi.withHandle(handle ->
      handle.select("SELECT MIN(sequence_number) FROM " + streamTableName +
                    " WHERE event_time >= :cutoff")
          .bind("cutoff", cutoff)
          .mapTo(Long.class)
          .findOne()
          .orElse(0L)
  );
}
```

### Shard Lifecycle (Pretender vs AWS)

**AWS DynamoDB Streams**:
```
Time  | Shard State
------|-------------
T0    | shard-00000 (OPEN, range: 0000-FFFF)
T1    | High traffic detected
T2    | shard-00000 (CLOSED, endSeq: 1000)
      | shard-00001 (OPEN, range: 0000-7FFF, parent: shard-00000)
      | shard-00002 (OPEN, range: 8000-FFFF, parent: shard-00000)
T3    | Consumer must switch from shard-00000 to shard-00001 and shard-00002
```

**Pretender**:
```
Time  | Shard State
------|-------------
T0    | shard-00000 (OPEN, range: all, endSeq: null)
T1    | shard-00000 (OPEN, range: all, endSeq: null)
T2    | shard-00000 (OPEN, range: all, endSeq: null)
...   | Always the same
```

---

## Limitations and Trade-offs

### 1. No Parallel Stream Processing

**Real DynamoDB**:
```java
// Multiple consumers can process different shards in parallel
ExecutorService executor = Executors.newFixedThreadPool(10);
for (Shard shard : shards) {
  executor.submit(() -> processShardRecords(shard));
}
// Total throughput: 10 shards × 1000 records/sec = 10,000 records/sec
```

**Pretender**:
```java
// Single consumer reads from single shard
processRecords("shard-00000");
// Total throughput: Limited by single database query performance
```

**Impact**:
- **High-throughput scenarios**: Pretender may not keep up with 10,000+ writes/second
- **Low/medium throughput**: No noticeable difference (<1,000 writes/second)
- **Mitigation**: Increase database query performance (indexes, connection pool)

### 2. No Hash Key Partitioning

**Real DynamoDB**:
```
Record 1: userId=123 → hash(123) = 0x2A4F → Shard-00000 (range: 0000-7FFF)
Record 2: userId=456 → hash(456) = 0x9B2C → Shard-00001 (range: 8000-FFFF)
Record 3: userId=789 → hash(789) = 0x1F3A → Shard-00000 (range: 0000-7FFF)
```

**Pretender**:
```
Record 1: userId=123 → Sequence 1 → Shard-00000
Record 2: userId=456 → Sequence 2 → Shard-00000
Record 3: userId=789 → Sequence 3 → Shard-00000
```

**Impact**:
- **Load distribution**: No automatic distribution across multiple consumers
- **Hot keys**: Hot hash keys don't cause separate shard creation
- **Benefit**: Simpler consumer code (no need to track multiple iterators)

### 3. Stronger Ordering Guarantees

**Real DynamoDB** (cross-shard ordering not guaranteed):
```
Shard-00000: [Record A (seq: 100), Record C (seq: 102)]
Shard-00001: [Record B (seq: 101), Record D (seq: 103)]

Consumer processing:
  Time T0: Process Record A, Record B (could happen in either order)
  Time T1: Process Record C, Record D (could happen in either order)
```

**Pretender** (global ordering guaranteed):
```
Shard-00000: [Record A (seq: 100), Record B (seq: 101), Record C (seq: 102), Record D (seq: 103)]

Consumer processing:
  Time T0: Process Record A
  Time T1: Process Record B
  Time T2: Process Record C
  Time T3: Process Record D
```

**Impact**:
- **Benefit**: Easier to reason about event ordering
- **Drawback**: May give false confidence when migrating to real DynamoDB
- **Recommendation**: Don't rely on cross-partition ordering in real DynamoDB

### 4. No Shard Discovery Complexity

**Real DynamoDB** (complex consumer logic):
```java
// Must periodically check for new shards
while (true) {
  DescribeStreamResponse stream = client.describeStream(request);
  List<Shard> currentShards = stream.streamDescription().shards();

  // Track which shards we're already processing
  Set<String> newShards = findNewShards(currentShards, processedShards);

  // Start processing new shards
  for (String shardId : newShards) {
    startProcessingShard(shardId);
  }

  // Check for closed shards and find their children
  for (Shard shard : currentShards) {
    if (shard.sequenceNumberRange().endingSequenceNumber() != null) {
      // Shard closed, find children
      List<Shard> children = findChildShards(currentShards, shard.shardId());
      for (Shard child : children) {
        startProcessingShard(child.shardId());
      }
    }
  }

  Thread.sleep(30000); // Poll every 30 seconds
}
```

**Pretender** (simple consumer logic):
```java
// Single shard, never changes
String shardIterator = client.getShardIterator(GetShardIteratorRequest.builder()
    .streamArn(streamArn)
    .shardId("shard-00000")
    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
    .build()).shardIterator();

while (true) {
  GetRecordsResponse response = client.getRecords(GetRecordsRequest.builder()
      .shardIterator(shardIterator)
      .build());

  processRecords(response.records());
  shardIterator = response.nextShardIterator();

  Thread.sleep(1000);
}
```

**Impact**:
- **Benefit**: Much simpler consumer code
- **Drawback**: Consumer code won't work correctly if migrated to real DynamoDB with multiple shards
- **Recommendation**: Use AWS Kinesis Client Library (KCL) patterns even with Pretender for production code

### 5. Fixed Throughput Capacity

**Real DynamoDB**:
```
Table Writes: 100/sec  → Stream Shards: 1-2
Table Writes: 1,000/sec → Stream Shards: 5-10
Table Writes: 10,000/sec → Stream Shards: 50-100
```

**Pretender**:
```
Table Writes: 100/sec  → Stream Shards: 1
Table Writes: 1,000/sec → Stream Shards: 1
Table Writes: 10,000/sec → Stream Shards: 1 (may struggle)
```

**Impact**:
- **Bottleneck**: Database insert/query performance limits throughput
- **Typical limits**:
  - PostgreSQL: 5,000-10,000 inserts/second (single table)
  - HSQLDB (in-memory): 20,000+ inserts/second
- **Mitigation**: Use batch inserts, optimize indexes, increase connection pool

---

## When This Matters

### Scenarios Where Single-Shard Works Fine

1. **Local Development**
   - Typical workload: <100 writes/second
   - No need for multi-consumer parallelism
   - Simpler debugging with global ordering

2. **Integration Testing**
   - Test suites typically generate <1,000 events
   - Sequential processing is acceptable
   - Deterministic ordering helps test assertions

3. **Small-Scale Production** (<1,000 writes/sec)
   - Single consumer can keep up
   - Database performance sufficient
   - No need for horizontal scaling

4. **Event Replay/Testing**
   - Replaying production events locally
   - Global ordering makes debugging easier
   - Performance not critical

### Scenarios Where Limitations Matter

1. **High-Throughput Load Testing** (>5,000 writes/sec)
   - **Problem**: Single shard can't keep up with write volume
   - **Solution**: Use real DynamoDB Streams or implement shard splitting

2. **Multi-Consumer Parallel Processing**
   - **Problem**: Can't distribute work across multiple consumers efficiently
   - **Solution**: Implement application-level partitioning or use real streams

3. **Production Traffic Simulation**
   - **Problem**: Doesn't accurately model real DynamoDB behavior
   - **Solution**: Use real DynamoDB for load testing, Pretender for unit tests

4. **Testing Shard Split/Merge Logic**
   - **Problem**: Consumer code that handles shard lifecycle won't be tested
   - **Solution**: Test shard handling logic separately against real DynamoDB

### Compatibility Checklist

**Code that Works Identically**:
- ✅ Reading records with TRIM_HORIZON iterator
- ✅ Reading records with LATEST iterator
- ✅ Using AT_SEQUENCE_NUMBER / AFTER_SEQUENCE_NUMBER
- ✅ 24-hour retention enforcement
- ✅ StreamViewType filtering (KEYS_ONLY, NEW_IMAGE, etc.)
- ✅ Record structure (eventName, keys, oldImage, newImage)

**Code that Behaves Differently**:
- ⚠️ Relying on specific shard IDs (Pretender always uses "shard-00000")
- ⚠️ Discovering new shards (Pretender never creates new shards)
- ⚠️ Parent-child shard relationships (Pretender has no hierarchy)
- ⚠️ Cross-shard ordering assumptions (Pretender provides global ordering)
- ⚠️ High-throughput parallel processing (Pretender is sequential)

**Migration Recommendations**:

```java
// Good: Works with both Pretender and real DynamoDB
void processStream(DynamoDbStreamsClient client, String streamArn) {
  // Get initial shard list (Pretender returns 1, AWS returns many)
  List<Shard> shards = client.describeStream(...).streamDescription().shards();

  // Process each shard (Pretender: 1 shard, AWS: multiple shards)
  for (Shard shard : shards) {
    processShard(client, shard);
  }
}

// Bad: Assumes single shard
void processStream(DynamoDbStreamsClient client, String streamArn) {
  String shardIterator = client.getShardIterator(
      GetShardIteratorRequest.builder()
          .shardId("shard-00000") // Hardcoded shard ID
          .build()
  ).shardIterator();
}
```

---

## Future Enhancements

### Option 1: Configurable Shard Count (Simple)

**Implementation**: Allow configuration of static shard count

```java
// Configuration
public interface StreamConfiguration {
  @Default
  default int shardCount() {
    return 1; // Default: single shard
  }
}

// Modified PdbStreamManager
private static final String SHARD_PREFIX = "shard-";
private final int shardCount;

public DescribeStreamResponse describeStream(...) {
  List<Shard> shards = new ArrayList<>();
  for (int i = 0; i < shardCount; i++) {
    shards.add(Shard.builder()
        .shardId(String.format("shard-%05d", i))
        .build());
  }
  return DescribeStreamResponse.builder().shards(shards).build();
}
```

**Hash Key Partitioning**:
```java
private int getShardIndex(String hashKeyValue) {
  return Math.abs(hashKeyValue.hashCode() % shardCount);
}

// Insert record to specific shard table
String shardTableName = String.format("pdb_stream_%s_shard_%d",
    tableName, getShardIndex(hashKeyValue));
```

**Pros**:
- More realistic multi-shard testing
- Enables parallel consumer testing
- Relatively simple to implement

**Cons**:
- Static shard count (no dynamic splitting)
- Requires multiple stream tables
- Increased complexity

**Estimated Effort**: 12-16 hours

### Option 2: Dynamic Shard Splitting (Complex)

**Implementation**: Automatically split shards based on traffic

```java
// Shard metadata table
CREATE TABLE pdb_shard_metadata (
  shard_id VARCHAR(64) PRIMARY KEY,
  table_name VARCHAR(256) NOT NULL,
  parent_shard_id VARCHAR(64),
  hash_key_start VARCHAR(16),
  hash_key_end VARCHAR(16),
  status VARCHAR(16), -- OPEN, CLOSED
  starting_sequence BIGINT,
  ending_sequence BIGINT,
  created_at TIMESTAMP,
  closed_at TIMESTAMP
);

// Split trigger
public void maybeSplitShard(String shardId) {
  ShardMetrics metrics = getShardMetrics(shardId);

  if (metrics.recordsPerSecond() > SPLIT_THRESHOLD) {
    // Close current shard
    closeShardAt(shardId, currentSequenceNumber);

    // Create two child shards
    createShard(shardId + "-child-0", shardId, "0000", "7FFF");
    createShard(shardId + "-child-1", shardId, "8000", "FFFF");
  }
}
```

**Pros**:
- Fully compatible with AWS behavior
- Realistic load testing
- Horizontal scalability

**Cons**:
- Very complex implementation
- Requires background monitoring
- Parent-child tracking complexity
- Database schema changes

**Estimated Effort**: 40-60 hours

### Option 3: Document and Accept Limitations (Current)

**Implementation**: Comprehensive documentation (this document!)

**Pros**:
- No code changes required
- Clear expectations set upfront
- Focus on use cases where Pretender excels

**Cons**:
- Doesn't address high-throughput scenarios
- Consumer code may not be production-ready

**Estimated Effort**: 2-4 hours (completed)

---

## Recommendations

### For Maintainers

1. **Keep single-shard for now**: It's simple, works well for intended use cases
2. **Add inline documentation**: Clarify shard ID constant with detailed comments
3. **Monitor issues**: Track user feedback about throughput limitations
4. **Consider Option 1 if needed**: If users request multi-shard testing support

### For Users

1. **Understand the limitations**: Single shard = sequential processing
2. **Don't hardcode "shard-00000"**: Use `describeStream()` to discover shards
3. **Test consumer code against real DynamoDB**: Before production deployment
4. **Use Pretender for**: Unit tests, local development, CI/CD pipelines
5. **Use real DynamoDB for**: Load testing, production validation, shard handling tests

### Code Review Guidelines

**When reviewing stream consumer code**:

```java
// ✅ GOOD: Discovers shards dynamically
List<Shard> shards = describeStream(streamArn).streamDescription().shards();
for (Shard shard : shards) {
  processShardRecords(shard.shardId());
}

// ❌ BAD: Hardcoded shard ID
String iterator = getShardIterator("shard-00000", ShardIteratorType.TRIM_HORIZON);

// ✅ GOOD: Handles multiple shards
ExecutorService executor = Executors.newCachedThreadPool();
for (Shard shard : shards) {
  executor.submit(() -> processShardRecords(shard));
}

// ❌ BAD: Assumes single shard
processAllRecords(getShardIterator("shard-00000"));

// ✅ GOOD: Checks if shard is closed
if (shard.sequenceNumberRange().endingSequenceNumber() != null) {
  findAndProcessChildShards(shard);
}

// ❌ BAD: Assumes shard never closes
while (true) {
  processRecords(shardIterator);
}
```

---

## Summary

**Current State**:
- ✅ Single shard per stream ("shard-00000")
- ✅ Works perfectly for local development and testing
- ✅ Simple, maintainable implementation
- ✅ Guaranteed global record ordering

**Limitations**:
- ⚠️ No parallel stream processing
- ⚠️ Fixed throughput (no automatic scaling)
- ⚠️ Doesn't match real DynamoDB shard lifecycle
- ⚠️ Single point of contention for high-throughput scenarios

**Best Use Cases**:
- Local development environments
- Unit and integration testing
- CI/CD pipelines
- Small to medium scale applications (<1,000 writes/second)

**When to Use Real DynamoDB Streams**:
- Production workloads
- High-throughput scenarios (>5,000 writes/second)
- Testing shard split/merge handling
- Load testing and capacity planning

**Conclusion**: Pretender's single-shard model is a pragmatic choice that trades AWS-level scalability for simplicity and deterministic behavior. For its intended use cases (development, testing, small-scale deployments), this is an excellent trade-off. For production-scale workloads or AWS compatibility testing, real DynamoDB Streams should be used.

---

**Document Version**: 1.0
**Last Updated**: 2026-01-03
**Author**: Pretender Development Team
