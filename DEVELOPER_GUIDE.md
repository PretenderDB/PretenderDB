# Pretender Developer Guide

This guide is for developers who need to maintain, extend, or debug the Pretender module. For user documentation, see the main project README.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Module Structure](#module-structure)
3. [Data Flow](#data-flow)
4. [Adding New DynamoDB Operations](#adding-new-dynamodb-operations)
5. [Expression Parsing](#expression-parsing)
6. [Database Schema Management](#database-schema-management)
7. [Testing Strategy](#testing-strategy)
8. [Common Patterns](#common-patterns)
9. [Troubleshooting](#troubleshooting)
10. [Specialized Topics](#specialized-topics)

## Architecture Overview

### What is Pretender?

Pretender is a DynamoDB-compatible client that uses SQL databases (PostgreSQL or HSQLDB) as the storage backend. It implements the AWS DynamoDB SDK interfaces, allowing applications to use it as a drop-in replacement for the real DynamoDB client.

### Design Philosophy

**Hybrid Storage Model**: Pretender uses a hybrid approach:
- **Relational indexing** for fast primary key and GSI lookups
- **JSON storage** for schemaless AttributeValue data
- **No schema evolution** required when adding new attributes

Example table structure:
```sql
CREATE TABLE pdb_item_users (
  hash_key_value VARCHAR(2048) NOT NULL,      -- Indexed for fast lookup
  sort_key_value VARCHAR(2048),               -- Indexed for range queries
  attributes_json JSONB NOT NULL,             -- Complete DynamoDB item as JSON
  create_date TIMESTAMP NOT NULL,
  update_date TIMESTAMP NOT NULL,
  PRIMARY KEY (hash_key_value, sort_key_value)
);
```

This allows:
- Fast PK/SK queries using indexed columns
- Complete DynamoDB fidelity (all data types preserved)
- No DDL changes when item schemas evolve

### Key Components

```
┌─────────────────────────────────────────────────────────────┐
│                 DynamoDbPretenderClient                     │
│         (implements AWS DynamoDB SDK interface)             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Manager Layer                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │PdbItemManager│  │PdbTableMgr   │  │PdbStreamMgr  │     │
│  │(1663 lines)  │  │              │  │              │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Converter Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │AttributeValue│  │ItemConverter │  │StreamRecord  │     │
│  │Converter     │  │              │  │Converter     │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      DAO Layer                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │PdbItemDao    │  │PdbMetadataDao│  │PdbStreamDao  │     │
│  │(JDBI)        │  │(JDBI)        │  │(JDBI)        │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   SQL Database                              │
│           (PostgreSQL or HSQLDB)                            │
└─────────────────────────────────────────────────────────────┘
```

## Module Structure

### Package Organization

```
io.github.pretenderdb/
├── converter/              # Type conversions (DynamoDB ↔ SQL)
│   ├── AttributeValueConverter.java      # Core: AttributeValue ↔ JSON
│   ├── ItemConverter.java                # Request/Response ↔ PdbItem
│   ├── StreamRecordConverter.java        # Stream records
│   ├── PdbTableConverter.java            # Table metadata
│   └── ShardIteratorCodec.java           # Iterator encoding
│
├── dagger/                 # Dependency injection
│   ├── PretenderComponent.java           # Main component
│   ├── PretenderModule.java              # Provider methods
│   ├── ConfigurationModule.java          # Config binding
│   └── CommonModule.java                 # Shared utilities
│
├── dao/                    # Data access (JDBI interfaces)
│   ├── PdbMetadataDao.java               # PDB_TABLE operations
│   ├── PdbItemDao.java                   # Item CRUD operations
│   └── PdbStreamDao.java                 # Stream record operations
│
├── encryption/             # Attribute-level encryption
│   ├── EncryptionService.java            # Interface
│   ├── AesGcmEncryptionService.java      # AES-256-GCM implementation
│   ├── NoOpEncryptionService.java        # Default (no encryption)
│   └── AttributeEncryptionHelper.java    # Selective encryption
│
├── expression/             # DynamoDB expression parsers
│   ├── UpdateExpressionParser.java       # SET, REMOVE, ADD, DELETE
│   ├── KeyConditionExpressionParser.java # Query conditions
│   ├── ConditionExpressionParser.java    # Conditional writes
│   └── FilterExpressionParser.java       # Post-query filtering
│
├── helper/                 # Cross-cutting concerns
│   ├── ValidationHelper.java             # Item validation
│   ├── TtlHelper.java                    # TTL expiration checks
│   └── ...
│
├── manager/                # Business logic
│   ├── PdbItemManager.java               # Item operations (LARGEST FILE)
│   ├── PdbTableManager.java              # Table lifecycle
│   ├── PdbItemTableManager.java          # Dynamic DDL generation
│   └── PdbStreamManager.java             # Streams API
│
├── model/                  # Immutables data models
│   ├── PdbMetadata.java                  # Table definition
│   ├── PdbItem.java                      # Stored item
│   ├── PdbGlobalSecondaryIndex.java      # GSI definition
│   ├── PdbStreamRecord.java              # Stream event
│   └── Configuration.java                # Module config
│
├── service/                # Background services
│   ├── TtlCleanupService.java            # Expired item cleanup
│   └── StreamCleanupService.java         # 24hr retention enforcement
│
├── util/                   # Utilities
│   └── CapacityCalculator.java           # RCU/WCU calculation
│
├── DynamoDbPretenderClient.java          # Main DynamoDB client
└── DynamoDbStreamsPretenderClient.java   # Streams client
```

### Critical Files by Size and Complexity

1. **PdbItemManager.java** (1,663 lines) - Most complex file
   - All item operations (putItem, getItem, updateItem, deleteItem, query, scan, batch, transact)
   - Expression evaluation
   - TTL handling
   - Stream event capture
   - GSI maintenance
   - Encryption/decryption
   - Capacity calculation

2. **PdbItemDao.java** (668 lines) - Database layer
   - Dynamic SQL (table names as parameters)
   - Handles both auto-commit and transactional (Handle) operations
   - Database-specific SQL generation

3. **AttributeValueConverter.java** (222 lines) - Type system
   - Bidirectional conversion between DynamoDB types and JSON
   - Preserves all type information

4. **UpdateExpressionParser.java** (~200 lines) - Expression parsing
   - Regex-based DSL parser
   - Complex state management

## Data Flow

### Example: PutItem Operation

```
1. Client Call
   ─────────────────────────────────────────────────────────
   DynamoDbPretenderClient.putItem(request)

2. Validation
   ─────────────────────────────────────────────────────────
   PdbItemManager.putItem()
   └─> ValidationHelper.validateItem()
       ├─> Check for empty string values (DynamoDB restriction)
       ├─> Check for empty binary values
       └─> Validate item size <= 400KB

3. Conversion
   ─────────────────────────────────────────────────────────
   ItemConverter.toPdbItem()
   ├─> Extract hash key value from item
   ├─> Extract sort key value (if present)
   └─> AttributeValueConverter.toJson(entireItem)
       └─> Convert Map<String, AttributeValue> to JSON string
           Example: {"id":{"S":"123"},"name":{"S":"Alice"},"age":{"N":"30"}}

4. Encryption (if enabled)
   ─────────────────────────────────────────────────────────
   AttributeEncryptionHelper.encryptAttributes()
   └─> For each configured attribute:
       └─> AesGcmEncryptionService.encrypt(value, tableName:attributeName)
           └─> Returns Base64-encoded ciphertext

5. Database Insert
   ─────────────────────────────────────────────────────────
   PdbItemDao.insert(tableName, pdbItem)
   └─> SQL: INSERT INTO pdb_item_<tablename>
            (hash_key_value, sort_key_value, attributes_json, create_date, update_date)
            VALUES (?, ?, ?, ?, ?)

6. GSI Update (if table has GSIs)
   ─────────────────────────────────────────────────────────
   For each GSI:
   └─> Extract GSI hash/sort key values from item
   └─> Build composite sort key: [gsi_sort_key#]main_hash_key[#main_sort_key]
   └─> Filter attributes based on projection (KEYS_ONLY, ALL, INCLUDE)
   └─> PdbItemDao.insert(gsiTableName, gsiItem)

7. Stream Event Capture (if streams enabled)
   ─────────────────────────────────────────────────────────
   PdbStreamDao.insertStreamRecord()
   └─> Event type: INSERT
   └─> Keys: { "id": {"S": "123"} }
   └─> New image: entire item (if StreamViewType includes NEW_IMAGE)

8. Response Building
   ─────────────────────────────────────────────────────────
   ItemConverter.toPutItemResponse()
   └─> Include consumed capacity (if requested)
       └─> CapacityCalculator.calculateWriteCapacity(itemSizeKB)
           └─> RCU = Math.ceil(itemSizeKB / 4.0)
```

### Example: Query Operation

```
1. Parse Key Condition
   ─────────────────────────────────────────────────────────
   KeyConditionExpressionParser.parse("pk = :pk AND sk > :sk")
   └─> Returns SQL components:
       hash_key_value = ?
       sort_key_value > ?

2. Execute SQL Query
   ─────────────────────────────────────────────────────────
   PdbItemDao.query(tableName, hashKeyValue, sortKeyCondition, limit, exclusiveStartKey)
   └─> SQL: SELECT * FROM pdb_item_<table>
            WHERE hash_key_value = ? AND sort_key_value > ?
            ORDER BY hash_key_value, sort_key_value
            LIMIT ?

3. Post-Query Processing
   ─────────────────────────────────────────────────────────
   For each row:
   ├─> AttributeValueConverter.fromJson(attributes_json)
   ├─> TtlHelper.isExpired(item, ttlAttributeName)
   │   └─> If expired: delete and exclude from results
   ├─> FilterExpressionParser.matches(item, filterExpression)
   │   └─> If doesn't match: exclude from results
   ├─> Apply projection expression (select specific attributes)
   └─> Decrypt attributes (if encryption enabled)

4. Build Response
   ─────────────────────────────────────────────────────────
   QueryResponse.builder()
   ├─> .items(processedItems)
   ├─> .lastEvaluatedKey(if more results available)
   ├─> .count(items.size())
   └─> .consumedCapacity(if requested)
```

## Adding New DynamoDB Operations

### Step 1: Add to Client Interface

**File**: `DynamoDbPretenderClient.java`

```java
@Override
public DescribeTableResponse describeTable(DescribeTableRequest request) {
  LOGGER.debug("describeTable({})", request);
  return pdbTableManager.describeTable(request);
}
```

### Step 2: Implement in Manager

**File**: `PdbTableManager.java` (or appropriate manager)

```java
public DescribeTableResponse describeTable(final DescribeTableRequest request) {
  LOGGER.info("describeTable({})", request.tableName());

  // 1. Retrieve metadata from database
  final PdbMetadata metadata = pdbMetadataDao.getTable(request.tableName())
      .orElseThrow(() -> new ResourceNotFoundException(
          ResourceNotFoundException.builder()
              .message("Table not found: " + request.tableName())
              .build()));

  // 2. Convert to AWS response format
  return pdbTableConverter.toDescribeTableResponse(metadata);
}
```

### Step 3: Add DAO Method (if needed)

**File**: `PdbMetadataDao.java`

```java
@SqlQuery("SELECT * FROM PDB_TABLE WHERE NAME = :tableName")
Optional<PdbMetadata> getTable(@Bind("tableName") String tableName);
```

### Step 4: Add Converter Logic (if needed)

**File**: `PdbTableConverter.java`

```java
public DescribeTableResponse toDescribeTableResponse(final PdbMetadata metadata) {
  TableDescription description = TableDescription.builder()
      .tableName(metadata.name())
      .keySchema(buildKeySchema(metadata))
      .attributeDefinitions(buildAttributeDefinitions(metadata))
      .tableStatus(TableStatus.ACTIVE)
      .creationDateTime(metadata.createDate().toInstant())
      .globalSecondaryIndexes(convertGSIs(metadata.globalSecondaryIndexes()))
      .build();

  return DescribeTableResponse.builder()
      .table(description)
      .build();
}
```

### Step 5: Add Tests

**End-to-End Test**: `src/test/java/endToEnd/TableOperationsTest.java`

```java
@Test
void describeTable_existingTable_returnsTableDescription() {
  // Given
  client.createTable(createTableRequest);

  // When
  DescribeTableRequest request = DescribeTableRequest.builder()
      .tableName(TABLE_NAME)
      .build();
  DescribeTableResponse response = client.describeTable(request);

  // Then
  assertThat(response.table().tableName()).isEqualTo(TABLE_NAME);
  assertThat(response.table().tableStatus()).isEqualTo(TableStatus.ACTIVE);
  assertThat(response.table().keySchema()).hasSize(2); // hash + sort
}

@Test
void describeTable_nonExistentTable_throwsResourceNotFoundException() {
  // When/Then
  assertThatThrownBy(() -> client.describeTable(
      DescribeTableRequest.builder().tableName("nonexistent").build()))
      .isInstanceOf(ResourceNotFoundException.class)
      .hasMessageContaining("Table not found: nonexistent");
}
```

**Manager Test**: `src/test/java/manager/PdbTableManagerTest.java`

```java
@Test
void describeTable_validRequest_returnsMetadata() {
  // Given
  when(pdbMetadataDao.getTable("users"))
      .thenReturn(Optional.of(mockMetadata));

  // When
  DescribeTableResponse response = manager.describeTable(
      DescribeTableRequest.builder().tableName("users").build());

  // Then
  assertThat(response.table().tableName()).isEqualTo("users");
  verify(pdbTableConverter).toDescribeTableResponse(mockMetadata);
}
```

## Expression Parsing

Expression parsers convert DynamoDB expression syntax into either SQL conditions or in-memory operations.

### UpdateExpressionParser

**Purpose**: Parse and apply `UpdateExpression` syntax

**Supported Actions**:
- `SET attribute = value` - Set attribute to value
- `SET attribute = attribute + value` - Numeric addition
- `SET attribute = list_append(attribute, :list)` - Append to list
- `SET attribute = if_not_exists(attribute, :default)` - Conditional set
- `REMOVE attribute` - Delete attribute
- `ADD attribute value` - Add to number or set
- `DELETE attribute :values` - Remove from set

**Implementation Pattern**: Regex-based parsing

```java
// Example from UpdateExpressionParser.java
private static final Pattern SET_PATTERN =
    Pattern.compile("SET\\s+(.+?)(?=\\s+REMOVE|\\s+ADD|\\s+DELETE|$)");

public Map<String, AttributeValue> applyUpdate(
    Map<String, AttributeValue> item,
    String updateExpression,
    Map<String, String> expressionAttributeNames,
    Map<String, AttributeValue> expressionAttributeValues) {

  Map<String, AttributeValue> updatedItem = new HashMap<>(item);

  // Parse SET actions
  Matcher setMatcher = SET_PATTERN.matcher(updateExpression);
  if (setMatcher.find()) {
    String setClause = setMatcher.group(1);
    for (String assignment : setClause.split(",")) {
      applySetAssignment(updatedItem, assignment.trim(),
                        expressionAttributeNames, expressionAttributeValues);
    }
  }

  // Parse REMOVE actions
  // ... similar pattern for REMOVE, ADD, DELETE

  return updatedItem;
}
```

**Key Challenges**:
1. **Function parsing**: Nested function calls like `SET a = list_append(if_not_exists(a, :empty), :new)`
2. **Operator precedence**: Math operations in SET
3. **Path expressions**: Nested attributes like `user.address.city`

**Adding New Functions**:

```java
// 1. Add function pattern
private static final Pattern FUNCTION_PATTERN =
    Pattern.compile("(\\w+)\\(([^)]+)\\)");

// 2. Add to applySetAssignment
private void applySetAssignment(Map<String, AttributeValue> item,
                                String assignment, ...) {
  Matcher funcMatcher = FUNCTION_PATTERN.matcher(value);
  if (funcMatcher.find()) {
    String functionName = funcMatcher.group(1);
    String args = funcMatcher.group(2);

    switch (functionName) {
      case "list_append":
        // Implementation
        break;
      case "if_not_exists":
        // Implementation
        break;
      case "your_new_function":  // <-- Add here
        // Implementation
        break;
    }
  }
}
```

### KeyConditionExpressionParser

**Purpose**: Convert query conditions to SQL WHERE clauses

**Supported Operators**:
- `=` - Equality
- `<`, `>`, `<=`, `>=` - Comparison
- `BETWEEN value1 AND value2` - Range
- `begins_with(attribute, :prefix)` - Prefix match

**SQL Generation**:

```java
public static String toSqlCondition(String keyConditionExpression) {
  // Parse: "pk = :pk AND sk > :sk"
  // Returns: "hash_key_value = ? AND sort_key_value > ?"

  String sql = keyConditionExpression;

  // Replace attribute names with column names
  sql = sql.replace(hashKeyName, "hash_key_value");
  sql = sql.replace(sortKeyName, "sort_key_value");

  // Replace expression attribute value placeholders with ?
  sql = sql.replaceAll(":[a-zA-Z0-9_]+", "?");

  // Handle functions
  sql = sql.replace("begins_with(sort_key_value, ?)",
                    "sort_key_value LIKE CONCAT(?, '%')");

  return sql;
}
```

### ConditionExpressionParser

**Purpose**: Evaluate conditional writes in-memory

**Supported Functions**:
- `attribute_exists(path)` - Check if attribute exists
- `attribute_not_exists(path)` - Check if attribute doesn't exist
- `begins_with(path, :value)` - String prefix check
- `contains(path, :value)` - String/set containment

**Evaluation Pattern**:

```java
public boolean evaluate(Map<String, AttributeValue> item,
                       String conditionExpression) {
  // Parse: "attribute_not_exists(email) OR #status = :active"

  // 1. Replace expression attribute names
  String expr = replaceAttributeNames(conditionExpression, attrNames);

  // 2. Handle functions
  expr = evaluateFunctions(expr, item, attrValues);

  // 3. Handle comparisons
  expr = evaluateComparisons(expr, item, attrValues);

  // 4. Evaluate logical operators (AND, OR, NOT) with precedence
  return evaluateLogicalExpression(expr);
}
```

**Adding New Condition Functions**:

```java
// In ConditionExpressionParser.java

private static final Pattern CUSTOM_FUNC_PATTERN =
    Pattern.compile("custom_check\\(([^,]+),\\s*([^)]+)\\)");

private String evaluateFunctions(String expr, Map<String, AttributeValue> item,
                                Map<String, AttributeValue> attrValues) {
  // ... existing function handling

  // Add new function
  Matcher matcher = CUSTOM_FUNC_PATTERN.matcher(expr);
  while (matcher.find()) {
    String attributePath = matcher.group(1).trim();
    String compareValue = matcher.group(2).trim();

    AttributeValue value = getNestedAttribute(item, attributePath);
    boolean result = customCheckLogic(value, compareValue, attrValues);

    expr = expr.replace(matcher.group(0), String.valueOf(result));
  }

  return expr;
}
```

## Database Schema Management

### Liquibase Changesets

**File**: `src/main/resources/liquibase/liquibase-setup.xml`

```xml
<databaseChangeLog>
  <include file="liquibase/db-001.xml"/>
  <include file="liquibase/db-002.xml"/>
  <include file="liquibase/db-003.xml"/>
</databaseChangeLog>
```

### Adding a New Schema Change

**Step 1**: Create new changeset file

**File**: `src/main/resources/liquibase/db-004.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog
    xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
    http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd">

  <changeSet id="004-add-backup-timestamp" author="yourname">
    <addColumn tableName="PDB_TABLE">
      <column name="BACKUP_TIMESTAMP" type="timestamp">
        <constraints nullable="true"/>
      </column>
    </addColumn>
  </changeSet>

</databaseChangeLog>
```

**Step 2**: Include in setup file

```xml
<!-- liquibase-setup.xml -->
<include file="liquibase/db-004.xml"/>
```

**Step 3**: Update model

```java
// PdbMetadata.java
@Value.Immutable
@JsonSerialize(as = ImmutablePdbMetadata.class)
@JsonDeserialize(as = ImmutablePdbMetadata.class)
public interface PdbMetadata {
  // ... existing methods

  @Nullable
  Instant backupTimestamp();  // Add new field
}
```

**Step 4**: Update DAO (if needed)

```java
// PdbMetadataDao.java
@SqlUpdate("UPDATE PDB_TABLE SET BACKUP_TIMESTAMP = :timestamp WHERE NAME = :name")
void updateBackupTimestamp(@Bind("name") String name,
                          @Bind("timestamp") Instant timestamp);
```

### Dynamic Table Creation

Item tables are created dynamically per DynamoDB table.

**File**: `PdbItemTableManager.java`

```java
public void createItemTable(final PdbMetadata metadata) {
  final String tableName = "pdb_item_" + metadata.name().toLowerCase();

  // Database-specific DDL
  if (isPostgreSQL()) {
    jdbi.withHandle(handle -> handle.execute(
        "CREATE TABLE " + tableName + " (" +
        "  hash_key_value VARCHAR(2048) NOT NULL, " +
        "  sort_key_value VARCHAR(2048), " +
        "  attributes_json JSONB NOT NULL, " +  // PostgreSQL JSONB
        "  create_date TIMESTAMP NOT NULL, " +
        "  update_date TIMESTAMP NOT NULL, " +
        "  PRIMARY KEY (hash_key_value, sort_key_value)" +
        ")"
    ));
  } else {
    jdbi.withHandle(handle -> handle.execute(
        "CREATE TABLE " + tableName + " (" +
        "  hash_key_value VARCHAR(2048) NOT NULL, " +
        "  sort_key_value VARCHAR(2048), " +
        "  attributes_json CLOB NOT NULL, " +  // HSQLDB CLOB
        "  create_date TIMESTAMP NOT NULL, " +
        "  update_date TIMESTAMP NOT NULL, " +
        "  PRIMARY KEY (hash_key_value, sort_key_value)" +
        ")"
    ));
  }

  // Create index for query performance
  jdbi.withHandle(handle -> handle.execute(
      "CREATE INDEX idx_" + tableName + "_hash ON " + tableName +
      "(hash_key_value)"
  ));
}
```

**Key Points**:
1. **Table naming**: Always lowercase, prefixed with `pdb_item_`
2. **Database detection**: Use JDBI metadata to detect PostgreSQL vs HSQLDB
3. **JSONB vs CLOB**: PostgreSQL supports JSONB (faster), HSQLDB uses CLOB
4. **Indexes**: Critical for query performance
5. **Composite primary key**: Both hash and sort (NULL if hash-only)

## Testing Strategy

### Test Hierarchy

```
BaseJdbiTest (Abstract)
├─> Sets up in-memory HSQLDB
├─> Runs Liquibase migrations
├─> Registers custom JDBI mappers
└─> Provides jdbi() instance

BaseEndToEndTest (Abstract)
├─> Extends BaseJdbiTest
├─> Creates full PretenderComponent
├─> Provides DynamoDbClient instance
└─> Used for integration tests

BasePostgreSQLTest (Abstract)
├─> Uses Testcontainers
├─> Tests PostgreSQL-specific features
└─> Slower but validates production DB
```

### Test Categories

**1. DAO Tests** (Fast, focused)

```java
@ExtendWith(MockitoExtension.class)
class PdbItemDaoTest extends BaseJdbiTest {

  private PdbItemDao dao;

  @BeforeEach
  void setUp() {
    dao = jdbi.onDemand(PdbItemDao.class);
    // Create test table
    jdbi.withHandle(handle -> handle.execute(
        "CREATE TABLE pdb_item_test (...)"
    ));
  }

  @Test
  void insert_validItem_storesInDatabase() {
    // Given
    PdbItem item = ImmutablePdbItem.builder()
        .tableName("test")
        .hashKeyValue("123")
        .attributesJson("{...}")
        .createDate(Instant.now())
        .updateDate(Instant.now())
        .build();

    // When
    dao.insert("pdb_item_test", item);

    // Then
    Optional<PdbItem> retrieved = dao.get("pdb_item_test", "123", null);
    assertThat(retrieved).isPresent();
    assertThat(retrieved.get().hashKeyValue()).isEqualTo("123");
  }
}
```

**2. Manager Tests** (Unit tests with mocks)

```java
@ExtendWith(MockitoExtension.class)
class PdbTableManagerTest {

  @Mock private PdbMetadataDao metadataDao;
  @Mock private PdbItemTableManager itemTableManager;
  @InjectMocks private PdbTableManager manager;

  @Test
  void createTable_validRequest_createsTableAndMetadata() {
    // Given
    CreateTableRequest request = CreateTableRequest.builder()
        .tableName("users")
        .keySchema(...)
        .build();

    // When
    manager.createTable(request);

    // Then
    verify(metadataDao).insert(argThat(metadata ->
        metadata.name().equals("users")));
    verify(itemTableManager).createItemTable(any());
  }
}
```

**3. End-to-End Tests** (Full stack)

```java
class ItemOperationsTest extends BaseEndToEndTest {

  @Test
  void putItemAndGetItem_roundTrip_preservesAllDataTypes() {
    // Given - Create table
    client.createTable(CreateTableRequest.builder()
        .tableName("test")
        .keySchema(KeySchemaElement.builder()
            .attributeName("id")
            .keyType(KeyType.HASH)
            .build())
        .attributeDefinitions(AttributeDefinition.builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build())
        .build());

    // When - Put item with all data types
    Map<String, AttributeValue> item = Map.of(
        "id", AttributeValue.builder().s("123").build(),
        "name", AttributeValue.builder().s("Alice").build(),
        "age", AttributeValue.builder().n("30").build(),
        "tags", AttributeValue.builder().ss("java", "aws").build(),
        "active", AttributeValue.builder().bool(true).build(),
        "metadata", AttributeValue.builder().m(Map.of(
            "nested", AttributeValue.builder().s("value").build()
        )).build()
    );

    client.putItem(PutItemRequest.builder()
        .tableName("test")
        .item(item)
        .build());

    // Then - Get item and verify all types preserved
    GetItemResponse response = client.getItem(GetItemRequest.builder()
        .tableName("test")
        .key(Map.of("id", AttributeValue.builder().s("123").build()))
        .build());

    assertThat(response.item()).containsAllEntriesOf(item);
    assertThat(response.item().get("age").n()).isEqualTo("30");
    assertThat(response.item().get("tags").ss()).containsExactly("java", "aws");
  }
}
```

**4. PostgreSQL Integration Tests**

```java
class PostgreSQLIntegrationTest extends BasePostgreSQLTest {

  @Test
  void jsonbQuerying_complexItem_handledCorrectly() {
    // Tests PostgreSQL-specific JSONB features
    // Uses Testcontainers to spin up actual PostgreSQL
  }
}
```

### Coverage Goals

- **DAO layer**: 100% (critical for data integrity)
- **Manager layer**: 90%+ (business logic)
- **Converter layer**: 95%+ (type safety critical)
- **Expression parsers**: 85%+ (complex regex logic)

### Running Tests

```bash
# All tests
./gradlew :pretender:test

# Specific test class
./gradlew :pretender:test --tests ItemOperationsTest

# Specific test method
./gradlew :pretender:test --tests ItemOperationsTest.putItemAndGetItem_roundTrip_preservesAllDataTypes

# Coverage report
./gradlew :pretender:jacocoTestReport
# View: pretender/build/reports/jacoco/test/html/index.html
```

## Common Patterns

### Pattern 1: Transactional Operations

When operations span multiple tables (main + GSI + stream), use JDBI `Handle`:

```java
public void putItem(final PutItemRequest request) {
  jdbi.useHandle(handle -> {
    try {
      // 1. Insert into main table
      handle.attach(PdbItemDao.class).insert(tableName, item);

      // 2. Update GSI tables
      for (PdbGlobalSecondaryIndex gsi : metadata.globalSecondaryIndexes()) {
        handle.attach(PdbItemDao.class).insert(gsiTableName, gsiItem);
      }

      // 3. Insert stream record
      if (metadata.streamEnabled()) {
        handle.attach(PdbStreamDao.class).insertStreamRecord(streamRecord);
      }

      // All succeed or all fail (automatic transaction)
    } catch (Exception e) {
      // Transaction automatically rolled back
      throw new RuntimeException("Failed to put item", e);
    }
  });
}
```

**Key**: Use `jdbi.useHandle()` for transactions, not `jdbi.withHandle()` for individual DAOs.

### Pattern 2: Attribute Name Resolution

Expression attribute names (`#name`, `#status`) must be resolved:

```java
private String resolveAttributeName(String name,
                                    Map<String, String> expressionAttributeNames) {
  if (name.startsWith("#")) {
    return expressionAttributeNames.getOrDefault(name, name.substring(1));
  }
  return name;
}

// Usage
String actualName = resolveAttributeName("#pk", Map.of("#pk", "userId"));
// Returns: "userId"
```

### Pattern 3: Type-Safe Immutables

Always use Immutables builders for data models:

```java
// Good
PdbItem item = ImmutablePdbItem.builder()
    .tableName("users")
    .hashKeyValue("123")
    .attributesJson("{...}")
    .createDate(Instant.now())
    .updateDate(Instant.now())
    .build();

// Bad - won't compile
PdbItem item = new PdbItem();
item.setTableName("users");
```

### Pattern 4: Database-Specific SQL

Detect database type and generate appropriate SQL:

```java
private boolean isPostgreSQL(Jdbi jdbi) {
  return jdbi.withHandle(handle -> {
    String dbName = handle.getConnection().getMetaData().getDatabaseProductName();
    return "PostgreSQL".equalsIgnoreCase(dbName);
  });
}

// Usage
String jsonColumn = isPostgreSQL(jdbi) ? "JSONB" : "CLOB";
String sql = "CREATE TABLE ... (attributes_json " + jsonColumn + " NOT NULL)";
```

### Pattern 5: GSI Composite Sort Key

GSI tables use composite sort key to ensure uniqueness:

```java
private String buildGsiCompositeSortKey(PdbGlobalSecondaryIndex gsi,
                                        Map<String, AttributeValue> item,
                                        PdbMetadata metadata) {
  StringBuilder builder = new StringBuilder();

  // 1. GSI sort key (if present)
  if (gsi.sortKey() != null) {
    String gsiSortValue = extractKeyValue(item, gsi.sortKey());
    builder.append(gsiSortValue).append("#");
  }

  // 2. Main table hash key (always)
  String mainHashValue = extractKeyValue(item, metadata.hashKey());
  builder.append(mainHashValue);

  // 3. Main table sort key (if present)
  if (metadata.sortKey() != null) {
    String mainSortValue = extractKeyValue(item, metadata.sortKey());
    builder.append("#").append(mainSortValue);
  }

  return builder.toString();
}

// Example result: "electronics#USER123#ORDER456"
// Ensures uniqueness when multiple items share GSI keys
```

### Pattern 6: TTL Cleanup on Read

Check and cleanup expired items during read operations:

```java
private Optional<Map<String, AttributeValue>> getItemWithTtlCheck(
    String tableName,
    String hashKeyValue,
    String sortKeyValue) {

  Optional<PdbItem> pdbItem = pdbItemDao.get(tableName, hashKeyValue, sortKeyValue);

  if (pdbItem.isPresent()) {
    Map<String, AttributeValue> item =
        attributeValueConverter.fromJson(pdbItem.get().attributesJson());

    // Check TTL
    if (ttlHelper.isExpired(item, metadata.ttlAttributeName())) {
      // Delete expired item
      pdbItemDao.delete(tableName, hashKeyValue, sortKeyValue);
      return Optional.empty();
    }

    return Optional.of(item);
  }

  return Optional.empty();
}
```

## Troubleshooting

### Issue: JSON Conversion Errors

**Symptom**: `com.fasterxml.jackson.databind.JsonMappingException`

**Cause**: AttributeValue contains unsupported type or nested structure too deep

**Solution**:
1. Check `AttributeValueConverter.toJson()` for supported types
2. Validate input data structure
3. Check for circular references in nested maps

**Debug**:
```java
// Add logging in AttributeValueConverter
LOGGER.debug("Converting AttributeValue: {}", attributeValue);
try {
  String json = objectMapper.writeValueAsString(typeWrapper);
  LOGGER.debug("Converted to JSON: {}", json);
  return json;
} catch (JsonProcessingException e) {
  LOGGER.error("Failed to convert: {}", attributeValue, e);
  throw new RuntimeException("JSON conversion failed", e);
}
```

### Issue: Query Returns No Results

**Symptom**: Query returns empty list but scan returns results

**Possible Causes**:
1. **Key condition incorrect**: Check hash/sort key names match table definition
2. **Case sensitivity**: Key values are case-sensitive
3. **Type mismatch**: Querying with N but stored as S
4. **GSI query on main table**: Use correct table name for GSI queries

**Debug Checklist**:
```java
// 1. Verify table metadata
PdbMetadata metadata = pdbMetadataDao.getTable(tableName).orElseThrow();
LOGGER.info("Hash key: {}, Sort key: {}", metadata.hashKey(), metadata.sortKey());

// 2. Log SQL query
LOGGER.info("SQL: {}", sql);
LOGGER.info("Parameters: {}", parameters);

// 3. Check raw database
jdbi.withHandle(handle -> {
  List<Map<String, Object>> rows = handle.select("SELECT * FROM pdb_item_" + tableName)
      .mapToMap()
      .list();
  LOGGER.info("Raw rows: {}", rows);
  return null;
});
```

### Issue: GSI Not Updated

**Symptom**: Main table has item but GSI table doesn't

**Causes**:
1. **Transaction rollback**: Exception after main insert but before GSI update
2. **Projection filtering**: Attribute not included in GSI projection
3. **Missing GSI key**: Item doesn't have required GSI key attribute

**Fix**:
```java
// Ensure transactional consistency
jdbi.useHandle(handle -> {
  PdbItemDao dao = handle.attach(PdbItemDao.class);

  // Main table
  dao.insert(mainTableName, item);

  // GSI tables (within same transaction)
  for (PdbGlobalSecondaryIndex gsi : metadata.globalSecondaryIndexes()) {
    // Check if item has GSI key
    if (!item.containsKey(gsi.hashKey())) {
      LOGGER.warn("Item missing GSI hash key {}, skipping GSI insert", gsi.hashKey());
      continue;
    }

    dao.insert(gsiTableName, gsiItem);
  }
});
```

### Issue: Expression Parsing Fails

**Symptom**: `IllegalArgumentException: Invalid expression syntax`

**Causes**:
1. **Unsupported function**: Function not implemented in parser
2. **Malformed expression**: Missing parentheses, incorrect syntax
3. **Reserved word conflict**: Using DynamoDB reserved word without `#` prefix

**Debug**:
```java
// UpdateExpressionParser
public Map<String, AttributeValue> applyUpdate(...) {
  LOGGER.debug("Original expression: {}", updateExpression);
  LOGGER.debug("Attribute names: {}", expressionAttributeNames);
  LOGGER.debug("Attribute values: {}", expressionAttributeValues);

  try {
    // Parse logic
  } catch (Exception e) {
    LOGGER.error("Failed to parse expression: {}", updateExpression, e);
    throw new IllegalArgumentException("Invalid expression: " + updateExpression, e);
  }
}
```

### Issue: Encryption/Decryption Errors

**Symptom**: Items stored but can't be retrieved, or garbage data returned

**Causes**:
1. **Key mismatch**: Different encryption key used for encrypt vs decrypt
2. **AAD mismatch**: Table name or attribute name changed
3. **Wrong service**: Using `NoOpEncryptionService` when data was encrypted

**Fix**:
```java
// Ensure consistent EncryptionService configuration
@Provides @Singleton
public EncryptionService encryptionService(Configuration config) {
  if (config.encryptionEnabled()) {
    byte[] masterKey = config.encryptionKey();
    return new AesGcmEncryptionService(masterKey);
  } else {
    return new NoOpEncryptionService();
  }
}

// Validate AAD matches
String aad = tableName + ":" + attributeName;
LOGGER.debug("Decrypting with AAD: {}", aad);
byte[] decrypted = encryptionService.decrypt(ciphertext, aad);
```

### Issue: Database Connection Pool Exhausted

**Symptom**: `SQLException: No more connections available`

**Causes**:
1. **Handle leak**: Not closing JDBI handles
2. **Long-running transactions**: Holding connections too long
3. **Pool size too small**: Insufficient connections for load

**Fix**:
```java
// Always use try-with-resources or useHandle/withHandle
// Good
jdbi.useHandle(handle -> {
  // Operations
});

// Bad - handle leak
Handle handle = jdbi.open();
// ... operations ...
// Forgot to close!

// Configure pool size
HikariConfig hikariConfig = new HikariConfig();
hikariConfig.setMaximumPoolSize(20);  // Increase from default 10
hikariConfig.setMinimumIdle(5);
```

## Performance Considerations

### Query Optimization

1. **Use indexed columns**: Always query by hash_key_value when possible
2. **Avoid full table scans**: Prefer query over scan
3. **Limit result size**: Use `limit` parameter to paginate
4. **Filter in SQL**: Push filtering to database when possible

```java
// Good - indexed query
SELECT * FROM pdb_item_users WHERE hash_key_value = ?

// Bad - full table scan
SELECT * FROM pdb_item_users WHERE attributes_json LIKE '%somevalue%'
```

### JSON Storage Optimization

1. **PostgreSQL JSONB**: Much faster than CLOB for queries
2. **Minimize item size**: Smaller JSON = faster serialization
3. **Projection**: Only select needed attributes

```java
// Consider JSONB indexing for PostgreSQL
CREATE INDEX idx_attributes ON pdb_item_users USING GIN (attributes_json);
```

### Batch Operations

1. **Use PreparedBatch**: Faster than individual inserts
2. **Batch size**: 100-500 items per batch optimal
3. **Transaction scope**: Batch within single transaction

```java
// PdbItemDao
default void batchInsert(String tableName, List<PdbItem> items) {
  PreparedBatch batch = prepareBatch(
      "INSERT INTO " + tableName + " (...) VALUES (...)");

  for (PdbItem item : items) {
    batch.bind(0, item.hashKeyValue())
         .bind(1, item.sortKeyValue())
         .bind(2, item.attributesJson())
         .add();
  }

  batch.execute();
}
```

## Future Enhancement Ideas

1. **Parallel GSI updates**: Update GSI tables concurrently
2. **Caching layer**: Add Redis/Caffeine cache for hot items
3. **Metrics**: Add operation timing and throughput metrics
4. **Connection pooling tuning**: Dynamic pool sizing based on load
5. **Full-text search**: Leverage PostgreSQL full-text search for scan filtering
6. **Partitioning**: Partition item tables by hash key ranges for large datasets
7. **Read replicas**: Support read-only database replicas for scaling
8. **Compression**: Compress large JSON blobs before storage

---

## Quick Reference

### Key Files by Function

| Function | File | Lines |
|----------|------|-------|
| Main client | DynamoDbPretenderClient.java | 135 |
| Item operations | PdbItemManager.java | 1,663 |
| Table management | PdbTableManager.java | 199 |
| DDL generation | PdbItemTableManager.java | ~200 |
| Type conversion | AttributeValueConverter.java | 222 |
| Data access | PdbItemDao.java | 668 |
| Update parsing | UpdateExpressionParser.java | ~200 |
| Encryption | AesGcmEncryptionService.java | ~150 |

### Common Commands

```bash
# Build
./gradlew :pretender:build

# Test
./gradlew :pretender:test

# Coverage
./gradlew :pretender:jacocoTestReport

# Specific test
./gradlew :pretender:test --tests ItemOperationsTest

# Clean build
./gradlew :pretender:clean :pretender:build
```

### Useful JDBI Patterns

```java
// Query
List<PdbItem> items = jdbi.withHandle(handle ->
    handle.select("SELECT * FROM pdb_item_users")
          .mapTo(PdbItem.class)
          .list()
);

// Update
int updated = jdbi.withHandle(handle ->
    handle.execute("UPDATE pdb_item_users SET ... WHERE ...")
);

// Transaction
jdbi.useHandle(handle -> {
    // All operations in transaction
    handle.execute("INSERT ...");
    handle.execute("UPDATE ...");
});
```

---

## Specialized Topics

### DynamoDB Streams and Shard Management

For detailed information about how Pretender implements DynamoDB Streams, including:
- Single-shard vs multi-shard architecture
- Shard lifecycle and management
- Limitations compared to real AWS DynamoDB Streams
- When to use Pretender vs real DynamoDB Streams
- Future enhancement options

See **[STREAMS_ARCHITECTURE.md](STREAMS_ARCHITECTURE.md)** for comprehensive documentation.

**Quick Summary**:
- Pretender uses a single static shard ("shard-00000") per stream
- Works perfectly for local development and testing (<1,000 writes/second)
- No parallel stream processing or automatic shard splitting
- Provides stronger ordering guarantees than real DynamoDB Streams

---

**Last Updated**: 2026-01-03
**Maintainer**: Pretender Development Team
