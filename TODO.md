# Pretender TODO List

This document outlines potential improvements, bug fixes, and enhancements for the Pretender DynamoDB-compatible client.

## Summary

**Total Completed**: 15 major items
**Total Remaining**: 10 items across all priorities

See [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) for details on completed features.

---

## 🟢 Low Priority / Enhancements

### 2. Code Duplication in DAO Mapping
**Priority:** LOW
**File:** `PdbItemDao.java` (query, scan methods)

**Problem:**
ResultSet to PdbItem mapping code is duplicated across multiple methods:
```java
.map((rs, ctx) -> {
    final PdbItem item = ImmutablePdbItem.builder()
        .tableName(tableName)
        .hashKeyValue(rs.getString("hash_key_value"))
        // ... repeated in 3+ places
```

**Solution:**
- Extract to private method or RowMapper class
- Reduces duplication and makes changes easier

**Estimated Effort:** 1-2 hours

---

### 3. Improved Error Messages
**Priority:** LOW
**Files:** Various

**Problem:**
Some error messages could be more helpful:
- "Table not found: X" could include list of available tables
- "Condition check failed" could show what condition was evaluated
- Expression parsing errors could show position in expression

**Solution:**
- Enhance exception messages throughout
- Add more context to errors

**Estimated Effort:** 2-4 hours

---

### 4. Comprehensive JavaDoc Documentation
**Priority:** LOW
**Files:** Various

**Problem:**
Some methods lack detailed JavaDoc, especially:
- Edge case behavior
- Exception conditions
- Parameter constraints

**Solution:**
- Add @throws tags for all exceptions
- Document all parameters and return values
- Add @see tags for related methods

**Estimated Effort:** 4-6 hours

---

### 5. Parallel Scan Not Supported
**Priority:** LOW
**File:** `PdbItemManager.java`

**Problem:**
DynamoDB supports parallel scan using `Segment` and `TotalSegments` parameters. Pretender doesn't implement this.

**Impact:**
- Cannot parallelize large table scans
- Performance gap vs real DynamoDB

**Solution:**
- Add support for Segment/TotalSegments
- Use MODULO on hash key for segmentation
- `WHERE MOD(hash_code(hash_key_value), :total) = :segment`

**Estimated Effort:** 4-6 hours

---

### 6. Local Secondary Indexes (LSI)
**Priority:** LOW
**File:** New feature

**Problem:**
LSIs are mentioned in IMPLEMENTATION_SUMMARY.md as a future enhancement but not implemented.

**Solution:**
- Similar to GSI implementation
- LSI shares partition key with main table
- Simpler than GSI in some ways

**Estimated Effort:** 12-16 hours

---

### 7. PartiQL Support
**Priority:** LOW
**File:** New feature

**Problem:**
DynamoDB now supports PartiQL (SQL-like query language). Pretender doesn't.

**Solution:**
- Add new `executeStatement()` method
- Parse PartiQL to internal query representation
- Could be complex to implement fully

**Estimated Effort:** 40+ hours

---

### 8. Performance Metrics and Monitoring
**Priority:** LOW
**File:** All managers

**Problem:**
No built-in metrics for:
- Operation latency
- Item counts
- Storage size
- Query patterns

**Solution:**
- Add Micrometer metrics
- Track operation counts and latencies
- Expose via JMX or Prometheus

**Estimated Effort:** 8-12 hours

---

### 9. Integration Tests for Edge Cases
**Priority:** LOW
**File:** Test files

**Missing test coverage:**
- Concurrent updates to same item (optimistic locking)
- Transaction with 25 items (max limit)
- Batch operations with max items
- Items near 400KB size limit
- Complex nested UpdateExpression (multiple list operations)
- GSI with ALL projection type and item updates
- Stream with 24-hour retention boundary
- TTL expiration during scan/query

**Solution:**
- Add targeted integration tests for each edge case

**Estimated Effort:** 8-12 hours

---

### 10. Database Connection Pool Configuration
**Priority:** LOW
**File:** Configuration, JDBI setup

**Problem:**
Connection pool settings may not be optimized for production use.

**Solution:**
- Review HikariCP configuration
- Add configurable pool size, timeout, etc.
- Document recommended settings

**Estimated Effort:** 2-4 hours

---

### 11. SQL Query Performance Analysis
**Priority:** LOW
**File:** All DAOs

**Problem:**
May be missing indexes or using inefficient queries.

**Solution:**
- Run EXPLAIN on all queries
- Identify missing indexes
- Optimize query patterns
- Add composite indexes where beneficial

**Estimated Effort:** 4-8 hours

---

## Completed Items (Archive)

For details on the 15 completed items (Transaction Atomicity, Batch Write Error Handling, Query/Scan Pagination, Validation, Performance Optimizations, ReturnConsumedCapacity, etc.), see the git history or [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md).

**Major Completed Features:**
- ✅ Transaction Atomicity
- ✅ Batch Write Error Handling
- ✅ Query & Scan Pagination
- ✅ Transaction & Batch Operation Limits Validation
- ✅ NULL and Empty String Validation
- ✅ Item Size Validation (400KB)
- ✅ Attribute-Level Encryption (AES-256-GCM)
- ✅ GSI Maintenance Performance Optimization
- ✅ BatchGetItem Performance Optimization
- ✅ TransactGetItems Projection Verification
- ✅ Consistent Reads Documentation
- ✅ ReturnConsumedCapacity Implementation
- ✅ DynamoDB Streams (complete with 24-hour retention)
- ✅ FilterExpression Support
- ✅ **Streams Shard Management Documentation** (2026-01-03)
  - Comprehensive documentation in `STREAMS_ARCHITECTURE.md`
  - Inline code documentation explaining single-shard model
  - Trade-offs, limitations, and use case guidance
  - Integration with `DEVELOPER_GUIDE.md`

---

## Estimated Remaining Effort

- **Medium Priority:** 0 hours
- **Low Priority:** 82-106 hours
- **Total Estimated:** 82-106 hours

**Note:** These estimates assume familiarity with the codebase and may vary based on testing requirements and code review time.
