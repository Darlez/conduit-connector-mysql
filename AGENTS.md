# AGENTS.md

This file contains guidelines for agentic coding agents working on this repository.

## Build, Lint, and Test Commands

### Build
```bash
make build
```
Builds the connector binary from `cmd/connector/main.go`.

### Lint
```bash
make lint
```
Runs golangci-lint with strict configuration. Fix issues before committing.

### Format
```bash
make fmt
```
Formats code using `gofumpt` and organizes imports with `gci`.

### Test
```bash
make test
```
Runs all tests with verbose output and race detection. Requires MySQL via Docker.

### Run Single Test
```bash
go test -v -race ./path/to/package -run TestSpecificFunction
```
Replace `./path/to/package` with the package path and `TestSpecificFunction` with the test name.

### Integration Tests
Integration tests require MySQL running locally:
```bash
make up-database  # Start MySQL container
make test         # Run tests
make down         # Stop containers
```

Enable trace logs with: `TRACE=true make test`

## Code Style Guidelines

### File Headers
All non-generated files must include the Apache 2.0 license header:
```go
// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
```

### Import Organization
Imports are organized using `gci write --skip-generated .`:
1. Standard library
2. Third-party packages
3. Local packages

Example:
```go
import (
    "context"
    "fmt"  // stdlib

    "github.com/conduitio/conduit-connector-sdk"
    "github.com/jmoiron/sqlx"  // third-party

    "github.com/conduitio-labs/conduit-connector-mysql/common"  // local
)
```

### Naming Conventions
- **Public identifiers**: PascalCase (`NewSource`, `TableConfig`)
- **Private identifiers**: camelCase (`db`, `iterator`)
- **Constants**: PascalCase for exported, UPPER_SNAKE_CASE for unexported (`DefaultFetchSize`, `AllTablesWildcard`)
- **Type aliases**: PascalCase (`Action`, `recordBatchKind`)
- **Interfaces**: PascalCase (`keyQuerier`)

### Error Handling
- Always wrap errors with context: `fmt.Errorf("context: %w", err)`
- Use `//nolint:wrapcheck` when wrapping errors that are already wrapped
- Use `//nolint:errcheck` when intentionally ignoring errors (with defer rollback pattern)
- Check errors from defer operations when they matter

### Testing
- Use `github.com/matryer/is` for assertions: `is := is.New(t)`
- Integration tests use `*_integration_test.go` suffix
- Test files excluded from some linters (dogsled, dupl, gosec, etc.)

### Logging
- Use structured logging via `sdk.Logger(ctx)`
- Log levels: `Debug()`, `Info()`, `Warn()`, `Error()`
- Always pass context to logger
- Example: `sdk.Logger(ctx).Info().Msg("Successfully detected tables")`

### Database Operations
- Use `sqlx` for database operations (extends standard database/sql)
- Use transactions with defer rollback pattern:
```go
tx, err := db.BeginTxx(ctx, nil)
if err != nil {
    return fmt.Errorf("failed to begin transaction: %w", err)
}
//nolint:errcheck // will always error if committed, no need to check
defer tx.Rollback()
```
- Use `github.com/Masterminds/squirrel` for query building
- Always check rows.Err() after iterating with Next()

### Struct Tags
- Use JSON tags for config: `json:"dsn"`
- Use validate tags for required fields: `validate:"required"`
- Use default tags for default values: `default:"10000"`
- Use db tags for database mapping: `db:"server_id"`

### Linting Notes
- 60+ linters enabled via golangci-lint
- nolint comments require: `//nolint:lintername explanation`
- Generated code has lax exclusions
- Test files have relaxed rules for certain linters
- Cyclomatic complexity limit: 20
- Max function complexity enforced by revive

### Configuration
- Config structs implement validation via `Validate(context.Context) error`
- Use connector SDK middleware for defaults
- Parse DSN with `mysql.ParseDSN()` and validate in Validate method

### Iterator Pattern
- Implement `common.Iterator` interface for data iteration
- Use combined iterators for snapshot + CDC
- Always properly close/teardown iterators
- Return structured error messages with context

### Constants
- Define package-level constants for magic numbers
- Use descriptive names: `DefaultFetchSize`, `DefaultBatchDelay`
- Document units when applicable (bytes, seconds, etc.)
