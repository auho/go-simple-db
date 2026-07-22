# go-simple-db

A simple Go library that provides a unified interface for database operations across MySQL and ClickHouse, built on top of GORM.

## Features

- **Unified Interface** - A single `SimpleDB` API that abstracts away database-specific details
- **Multiple Database Support** - MySQL and ClickHouse out of the box
- **GORM Integration** - Direct access to the underlying `*gorm.DB` when needed
- **Schema Operations** - Copy table structures, truncate, drop tables
- **Bulk Operations** - Bulk insert and bulk update by ID
- **Column Introspection** - Query table columns and their schema

## Installation

```bash
go get github.com/auho/go-simple-db/v3
```

## Quick Start

### MySQL

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/auho/go-simple-db/v3"
)

func main() {
    dsn := "user:password@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local"
    db, gormDB, err := simpledb.NewMySQLGorm(dsn)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ctx := context.Background()

    // Ping
    if err := db.Ping(ctx); err != nil {
        log.Fatal(err)
    }

    // Get database name
    name, err := db.Name(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("database:", name)

    // Use the underlying gorm.DB for queries
    _ = gormDB

    // Bulk insert
    data := []map[string]any{
        {"name": "alice", "age": 30},
        {"name": "bob", "age": 25},
    }
    err = db.BulkInsertFromSliceMap(ctx, "users", data, 100)
    if err != nil {
        log.Fatal(err)
    }

    // Row count
    count, err := db.RowCount(ctx, "users")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("row count:", count)
}
```

### ClickHouse

```go
db, gormDB, err := simpledb.NewClickHouseGorm("clickhouse://user:password@127.0.0.1:9000/dbname")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

## API Overview

### Connection

| Method | Description |
|--------|-------------|
| `NewMySQLGorm(dsn, opts...)` | Create a MySQL-backed SimpleDB |
| `NewClickHouseGorm(dsn, opts...)` | Create a ClickHouse-backed SimpleDB |
| `Ping(ctx)` | Ping the database |
| `Close()` | Close the connection |
| `Clone()` | Clone the SimpleDB instance |
| `GormDB()` | Get the underlying `*gorm.DB` |
| `SqlDB()` | Get the underlying `*sql.DB` |

### Schema Operations

| Method | Description |
|--------|-------------|
| `Truncate(ctx, table)` | Truncate a table |
| `Drop(ctx, table)` | Drop a table |
| `CopyStructure(ctx, src, dst)` | Copy table structure from src to dst |
| `DropAndCopyStructure(ctx, src, dst)` | Drop dst then copy structure from src |

### Data Operations

| Method | Description |
|--------|-------------|
| `CopyData(ctx, src, dst)` | Copy data from src table to dst table |
| `BulkInsertFromSliceMap(ctx, table, data, batchSize)` | Bulk insert from `[]map[string]any` |
| `BulkInsertFromSliceSlice(ctx, table, fields, data, batchSize)` | Bulk insert from `[][]any` with explicit fields |
| `BulkUpdateFromSliceMapByID(ctx, table, id, data)` | Bulk update by ID field |

### Query Operations

| Method | Description |
|--------|-------------|
| `RowCount(ctx, table)` | Get the row count of a table |
| `GetTableColumnsSchema(ctx, table)` | Get column schema (`[]schema.Column`) |
| `GetTableColumns(ctx, table)` | Get column names |
| `GetDatabase(ctx)` | Get the current database name |

## License

[MIT](LICENSE)
