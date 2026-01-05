# Pretender

A DynamoDB-compatible library for Java that stores data in SQL databases (PostgreSQL/HSQLDB) instead of DynamoDB. Perfect for local development and testing without AWS dependencies.

## NOTICE

This module has primarily been written by Claude.AI as an experiment. The idea
started during a discussion on Mastodon about having a durable/production-like
DynamoDB alternative for local development other than localstack or dynamodb-local.
But honestly this is kinda annoying to write. It's very fiddly to write, and
will be time consuming. But there is a known implementation. Having Claude
write this and be able to test it easily against the original implementation
seemed like a perfect AI project.

Be aware that as of yet, this has not been used in production (as of 2026-01-02,)
and not fully reviewed by a human yet. Use at your own risk.

I am in the process of reviewing the code and validating it's correctness. 
Integration tests have been written to compare against DynamoDB Local, to 
validate it.

## TL;DR

Pretender is a Java library that implements a DynamoDB-compatible API that lets
you develop and deploy your application with PostgreSQL, but migrate easily to
DynamoDB should you actually need the scale AWS provides.

## Overview

The ability to run DynamoDB locally for development that is provided
by Amazon's [local dynamodb project](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html) (or localstack) is great feature.
It uses a SQLite instance to store the data, mimicking how DDB works on AWS.
Having PretenderDB implement DynamoDB but use an open-source SQL database
like PostgreSQL or HSQLDB as a backend has several advantages:

1. While existing tools are useful during development, PretenderDB provides a production version without deploying to AWS and paying AWS costs.
2. Having a (durable) production-quality version of DynamoDB that does not require AWS means you can use it until you need the real scale AWS provides.
3. Normal DBM tooling can be use for backups, migrations, and analysis, though your analysis will be limited here by the PretenderDB architecture.

By using PretenderDB for your project, you can develop and launch your application while building
up a user-base. It saves you money on AWS costs, and allows you to switch to AWS DynamoDB should 
you really need the scale.


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


