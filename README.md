# Pretender

A DynamoDB-compatible library for Java that stores data in SQL databases (PostgreSQL/HSQLDB) instead of DynamoDB. Perfect for local development and testing without AWS dependencies.

## NOTICE

This module has primarily been written by Claude.AI as an experiment.
It has not been used in production as of 2026-01-02, and not fully reviewed
by a human yet. Use at your own risk.

That said, this seemed like a great way to use Claude to write code as
everything written can be compared against localstack, dynamodb-local, and
AWS DynamoDB itself. I have integ tests to do just that, to validate it.

I am in the process of reviewing the code and validating it's correctness.

## Overview

The ability to run DynamoDB locally for development that is provided
by Amazon's [local dynamodb project](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html) is great feature.
It uses a SQLite instance to store the data, mimicking how DDB works on AWS.
However, there are a few shortcomings that it does not address.

1. It is useful during development to have easy access to the datastore. They do not give that.
2. If you are a startup, it may be a while before you can fully realize the benefit of running in the cloud.

Pretender addresses these shortcomings by providing a full DynamoDB-compatible client backed by standard SQL databases.

## Features

### Core DynamoDB Operations
- **Table Management**: createTable, deleteTable, describeTable, listTables
- **Item Operations**: putItem, getItem, updateItem, deleteItem, query, scan
- **Global Secondary Indexes (GSI)**: Full support for GSI creation and querying
- **Time-to-Live (TTL)**: Automatic item expiration with background cleanup
- **DynamoDB Streams**: Change data capture with 24-hour retention (see below)

### DynamoDB Streams Support

Pretender now includes full support for DynamoDB Streams:

- **Stream Configuration**: Enable/disable streams per table with configurable StreamViewType
- **Event Capture**: Automatic capture of INSERT, MODIFY, and REMOVE events
- **Stream Consumption**: Complete implementation of Streams API (describeStream, getShardIterator, getRecords, listStreams)
- **24-Hour Retention**: Automatic cleanup matching AWS behavior
- **Stream View Types**: KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES

See [STREAMS_IMPLEMENTATION.md](STREAMS_IMPLEMENTATION.md) for detailed documentation.

## Documentation

- **[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)** - Complete implementation details, architecture, and usage examples
- **[STREAMS_IMPLEMENTATION.md](STREAMS_IMPLEMENTATION.md)** - DynamoDB Streams specific documentation
- **[TODO.md](TODO.md)** - Roadmap and future enhancements

## Limitations

1. Not all DynamoDB APIs are implemented (PartiQL, global tables)
2. Single shard implementation for streams (sufficient for local development)
3. Intended for development/testing - use AWS DynamoDB for production workloads

For complete feature list and implementation status, see [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md).


