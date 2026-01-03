# Pretender Integration Tests

This module contains integration tests that validate Pretender works as a drop-in replacement for AWS DynamoDB.

## Purpose

The tests in this module run **the exact same test code** against two different DynamoDB implementations:

1. **AWS DynamoDB Local** - The official local DynamoDB implementation from AWS
2. **Pretender with PostgreSQL** - Pretender backed by PostgreSQL via Testcontainers

By running identical tests against both implementations, we ensure Pretender maintains full compatibility with real DynamoDB behavior.

## Test Structure

### Providers

Tests use the `DynamoDbProvider` abstraction to switch between implementations:

- `DynamoDbLocalProvider` - Runs AWS DynamoDB Local (reference implementation)
- `PretenderPostgresProvider` - Runs Pretender with PostgreSQL (implementation under test)

### Parameterized Tests

Each test is parameterized using JUnit 5's `@ParameterizedTest` with `@MethodSource`:

```java
@ParameterizedTest
@MethodSource("dynamoDbProviders")
void testPutItemAndGetItem(DynamoDbProvider provider) throws Exception {
    DynamoDbClient client = provider.getDynamoDbClient();
    // Test runs identically against both implementations
}
```

## Running Tests

```bash
# Run all integration tests (tests both DynamoDB Local and Pretender)
./gradlew :pretender-integ:test

# View test report
open pretender-integ/build/reports/tests/test/index.html
```

## Test Coverage

### Basic CRUD Operations
- ✅ Create Table (hash key only)
- ✅ Put Item (with multiple data types)
- ✅ Get Item (retrieve by key)
- ✅ Update Item (using UpdateExpression)
- ✅ Delete Item
- ✅ Query (hash + range key)
- ✅ Scan (full table scan)

### Coming Soon
- Batch operations (BatchGetItem, BatchWriteItem)
- Transactions (TransactGetItems, TransactWriteItems)
- Global Secondary Indexes (GSI queries)
- Conditional writes (ConditionExpression)
- Filter expressions
- DynamoDB Streams

## Requirements

### DynamoDB Local

DynamoDB Local requires SQLite native libraries. The build automatically downloads and extracts them to `build/libs`.

Supported platforms:
- macOS (ARM64)
- Linux (AMD64)

### PostgreSQL (via Testcontainers)

Pretender tests use Testcontainers to run PostgreSQL. Requirements:
- Docker must be running
- Docker daemon accessible to tests

## Architecture

```
BasicCrudIntegrationTest
├─> DynamoDbProvider (interface)
│   ├─> DynamoDbLocalProvider
│   │   └─> DynamoDB Local JAR
│   │       └─> In-memory storage
│   └─> PretenderPostgresProvider
│       └─> Pretender Component
│           └─> PostgreSQL Container (Testcontainers)
│               └─> SQL database
```

## Why This Matters

This testing approach provides:

1. **Compatibility Validation** - Proves Pretender behaves like real DynamoDB
2. **Regression Detection** - Catches behavior differences early
3. **Confidence** - Users can trust Pretender as a development/testing substitute
4. **Documentation** - Tests serve as examples of correct usage

## Test Output Example

```
=================================================================================
Starting test with provider: DynamoDB Local (AWS Official)
=================================================================================
Testing PUT ITEM and GET ITEM with: DynamoDB Local (AWS Official)
✓ PUT ITEM and GET ITEM test passed for: DynamoDB Local (AWS Official)

=================================================================================
Starting test with provider: Pretender with PostgreSQL (Testcontainers)
=================================================================================
Testing PUT ITEM and GET ITEM with: Pretender with PostgreSQL (Testcontainers)
✓ PUT ITEM and GET ITEM test passed for: Pretender with PostgreSQL (Testcontainers)
```

Each test runs twice (once per provider), and both must pass to ensure compatibility.

## Adding New Tests

To add a new compatibility test:

1. Add a new `@ParameterizedTest` method in `BasicCrudIntegrationTest`
2. Use `@MethodSource("dynamoDbProviders")` to run against both implementations
3. Write the test using standard AWS SDK DynamoDB client APIs
4. The test will automatically run against both DynamoDB Local and Pretender

Example:

```java
@ParameterizedTest
@MethodSource("dynamoDbProviders")
void testYourNewFeature(DynamoDbProvider provider) throws Exception {
    DynamoDbClient client = provider.getDynamoDbClient();

    // Your test code using standard DynamoDB client APIs
    // This will run against BOTH DynamoDB Local and Pretender
}
```

## Troubleshooting

### SQLite Library Issues (DynamoDB Local)

If you see errors about missing SQLite libraries:
```bash
# Clean and rebuild to re-download native libs
./gradlew :pretender-integ:clean :pretender-integ:copyNativeLibs
```

### Docker/Testcontainers Issues (Pretender)

If PostgreSQL container fails to start:
- Verify Docker is running: `docker ps`
- Check Docker resources (memory/CPU)
- Review Testcontainers logs in test output

### Port Conflicts (DynamoDB Local)

If port 8000 is already in use:
- DynamoDB Local may still be running from a previous test
- Find and kill the process: `lsof -i :8000`
