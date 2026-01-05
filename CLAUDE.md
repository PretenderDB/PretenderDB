# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a multi-module Gradle project demonstrating a complete Dropwizard server implementation using Dagger for dependency injection (instead of Spring). The project includes a key generation/storage service with examples of metrics, testing, feature flags, state machines, and mocking dependencies.

### Module Structure

The project consists of 5 modules with specific purposes:

- **database-utils**: JDBI factory and Liquibase integration utilities
- **pretender**: DynamoDB-compatible client that uses SQL databases (PostgreSQL/HSQLDB) as backend for local development
- **pretender-integ**: Testing code that only knows about the DynamoDB client, but can work against any implementation.

Module dependencies:
```
pretender-integ → pretender → database-utils
```

### Developer requirements
- Always run a full clean build and test of the whole project before even writing a commit.
- Ensure tests are meaningful. Do not test getters/setters or trivial code.
- When looking at integration tests, assume DynamoDB Local jar is correct and pretender is doing something wrong if they differ.

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
io.github.pretenderdb.<module>.<layer>
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
2. Build module: `./gradlew :module-name:build`
3. Run tests: `./gradlew :module-name:test`
4. Check coverage: `./gradlew :module-name:jacocoTestReport`
5. Before commits, test everything locally: `./gradlew test`
