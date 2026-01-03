# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a multi-module Gradle project demonstrating a complete Dropwizard server implementation using Dagger for dependency injection (instead of Spring). The project includes a key generation/storage service with examples of metrics, testing, feature flags, state machines, and mocking dependencies.

### Module Structure

The project consists of 5 modules with specific purposes:

- **api**: REST API contracts using JAX-RS interfaces and Immutables-based data models
- **server-base**: Reusable Dropwizard framework with Dagger integration, metrics, and resource/health check initialization
- **database-utils**: JDBI factory and Liquibase integration utilities
- **pretender**: DynamoDB-compatible client that uses SQL databases (PostgreSQL/HSQLDB) as backend for local development
- **keys-server**: Main application implementing key generation and storage service

Module dependencies:
```
keys-server → api, server-base, database-utils
pretender → database-utils
```

### Developer requirements
- Always run a full clean build and test of the whole project before even writing a commit.
- Ensure tests are meaningful. Do not test getters/setters or trivial code.

## Build Commands

### Building the Project

```bash
# Build all modules
./gradlew build

# Build specific module
./gradlew :keys-server:build

# Clean and build
./gradlew clean build

# Build without tests
./gradlew build -x test
```

### Running Tests

```bash
# Run all tests
./gradlew test

# Run tests for specific module
./gradlew :keys-server:test

# Run specific test class
./gradlew :keys-server:test --tests KeyManagerTest

# Run specific test method
./gradlew :keys-server:test --tests KeyManagerTest.generateRawKey_oneByte

# Run all verification (includes tests and coverage)
./gradlew check
```

### Code Coverage

```bash
# Generate coverage reports
./gradlew jacocoTestReport

# Verify coverage meets thresholds
./gradlew jacocoTestCoverageVerification

# Reports are generated in: <module>/build/reports/jacoco/test/html/index.html
```

### Running the Application

```bash
# Run keys-server with default config
./gradlew :keys-server:run --args="server config.yml"

# Alternative: Install distribution and run
./gradlew :keys-server:installDist
./keys-server/build/install/keys-server/bin/keys-server server config.yml
```

## Architecture Patterns

### Dagger Dependency Injection

The project uses Dagger 2 for compile-time dependency injection:

1. **Component Structure**: Each module/application defines a Dagger component extending `DropWizardComponent`
   ```java
   @Singleton
   @Component(modules = {KeysServerModule.class, DropWizardModule.class})
   public interface KeysServerComponent extends DropWizardComponent {
   }
   ```

2. **Module Pattern**: Modules combine `@Provides` methods and `@Binds` interfaces
   ```java
   @Module(includes = KeysServerModule.Binder.class)
   public class KeysServerModule {
     @Provides @Singleton
     public Jdbi jdbi(JdbiFactory factory, LiquibaseHelper helper) { }

     @Module
     interface Binder {
       @Binds @IntoSet JerseyResource keysResource(KeysResource resource);
     }
   }
   ```

3. **Set Injection**: Resources, health checks, and managed objects are contributed to sets
   - Use `@IntoSet` for contributing single items
   - Use `@Multibinds` for declaring empty sets

### Server-Base Framework

Applications extend the `Server<T extends ServerConfiguration>` abstract class:

1. Create a configuration class extending `ServerConfiguration`
2. Create a Dagger component extending `DropWizardComponent`
3. Implement `Server.setupComponent()` to build the Dagger component
4. Jersey resources implement both the API interface and `JerseyResource` marker

### Immutables Pattern

All data models use Immutables for immutable value objects:

1. Define interface with `@Value.Immutable` annotation
2. Use `@JsonSerialize(as = ImmutableClassName.class)` and `@JsonDeserialize(as = ImmutableClassName.class)`
3. Generated class will be named `Immutable<InterfaceName>`
4. Use builder pattern: `ImmutableKey.builder().id("x").build()`

### Database Management

Database setup follows this pattern:

1. **JDBI Configuration**: Use `JdbiFactory` to create JDBI instances with Immutables plugin support
2. **Migrations**: Use `LiquibaseHelper` to run migrations on application startup
3. **Liquibase Structure**:
   - Entry point: `liquibase/liquibase-setup.xml`
   - Individual changesets: `liquibase/db-001.xml`, `liquibase/db-002.xml`, etc.
4. **DAOs**: Use JDBI's SqlObject pattern with interfaces

### Metrics

The project uses codehead-metrics with declarative metrics via AspectJ:

1. **Declarative Metrics**: Annotate methods with `@Metrics`
2. **AspectJ Weaving**: Build file must include AspectJ post-compile weaving plugin
3. **Automatic Tags**: All metrics include `host`, `stage`, `endpoint`, and `status` tags
4. **Request Filters**: `MetricTagsResource` adds request/response context to metrics

When adding metrics to a module:
```kotlin
// In build.gradle.kts
plugins {
    id("io.freefair.aspectj.post-compile-weaving") version "9.1.0"
}

dependencies {
    implementation(libs.codehead.metrics.declarative)
    aspect(libs.codehead.metrics.declarative)
}
```

### Testing Patterns

1. **Unit Tests**: Use Mockito with `@ExtendWith(MockitoExtension.class)`
   - Mock dependencies with `@Mock`
   - Inject with `@InjectMocks`

2. **Integration Tests**: Use `DropwizardAppExtension` with `@ExtendWith(DropwizardExtensionsSupport.class)`
   - Create test configuration YAML in `src/test/resources`
   - Use `EXT.client()` to make HTTP requests

3. **Base Test Classes**:
   - `BaseJdbiTest`: Sets up JDBI with in-memory HSQLDB and Liquibase
   - `BaseEndToEndTest`: Sets up complete Dagger component

## Key Technologies

- **Java 21**: Language version with toolchain support
- **Gradle 8.10**: Build system with Kotlin DSL
- **Dropwizard 5.0.0**: REST framework (Jersey, Jetty, Jackson, Metrics)
- **Dagger 2.57**: Compile-time dependency injection
- **JDBI 3.51**: SQL interaction layer
- **Liquibase 5.0.1**: Database migrations
- **Micrometer 1.16**: Metrics facade
- **Immutables 2.12**: Immutable value objects
- **JUnit Jupiter 6.0**: Testing framework

## File Organization

Standard Maven structure:
- `src/main/java`: Production code
- `src/test/java`: Test code
- `src/main/resources`: Resources, Liquibase migrations, configurations
- `src/test/resources`: Test configurations

Package naming convention:
```
com.codeheadsystems.<module>.<layer>
```

Layers: `component`, `module`, `dao`, `manager`, `resource`, `converter`, `model`, `exception`

## Pretender Module

The `pretender` module provides a DynamoDB-compatible client backed by SQL:

- Implements AWS DynamoDB SDK interfaces
- Stores data in PostgreSQL or HSQLDB
- Useful for local development without running actual DynamoDB
- Has its own standalone Dagger component

## Common Development Workflow

1. Make code changes in appropriate module
2. Run tests: `./gradlew :module-name:test`
3. Check coverage: `./gradlew :module-name:jacocoTestReport`
4. Build module: `./gradlew :module-name:build`
5. For keys-server changes, test locally: `./gradlew :keys-server:run --args="server config.yml"`

## Violet (Rust Client)

The `violet/` directory contains a Rust-based envelope encryption client that integrates with the Keys server.

### Structure
- **violet-core**: Crypto primitives (AES-256-GCM, AES-256-GCM-SIV) and envelope encryption logic
- **violet-client**: HTTP client for Keys server REST API
- **violet-cli**: Command-line interface for encryption/decryption
- **violet-daemon**: Unix socket daemon for IPC

### Building Violet
```bash
cd violet
cargo build --release
```

Binary location: `violet/target/release/violet`

### Usage
```bash
# Encrypt data (requires Keys server running)
echo "secret" | ./target/release/violet encrypt > envelope.json

# Decrypt data
./target/release/violet decrypt -i envelope.json

# Run as daemon
./target/release/violet daemon --socket /tmp/violet.sock
```

### Testing Violet
```bash
cd violet
cargo test                    # Unit tests
cargo test -- --ignored       # Integration tests (requires Keys server)
```

See `violet/README.md` for complete documentation.
