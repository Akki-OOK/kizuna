# Kizuna (V0.4)

Kizuna is a lightweight, teaching-focused DBMS written in modern C++ (C++20). V0.4 builds on the storage and catalog layers (V0.1/V0.2) with a predicate-aware SQL DML pipeline: projected SELECTs with WHERE/LIMIT, UPDATE expressions, filtered DELETE, and a REPL that mirrors row counts and projections.

## Feature Highlights

- **Storage Core (V0.1)**: fixed-size paged file manager, slotted data pages, buffer pool with LRU, SQLite-style freelist trunks, typed record encode/decode, structured exceptions, logger, and REPL scaffolding.
- **Catalog & DDL (V0.2)**: persistent table/column catalog, SQL lexer/parser for CREATE/DROP TABLE, DDL executor wiring, REPL schema inspection and DROP IF EXISTS UX.
- **SQL DML (V0.4)**:
  - Expression-aware parser for projection lists, WHERE comparisons/logicals, UPDATE assignments, and LIMIT clauses.
  - DML executor with predicate pushdown, projection materialisation, typed UPDATE/DELETE, and LIMIT enforcement.
  - Table heap update API that reuses slots when possible and relocates safely when payloads grow.
  - REPL SELECT output that mirrors projection headers and reports row counts for SELECT/UPDATE/DELETE.
  - Expanded unit tests covering expression evaluation, predicate semantics (NULL/tri-state), LIMIT edge cases, and update relocation.

## Build

Prereqs: CMake 3.10+, a C++20 compiler (MSVC 2022, GCC 11+, Clang 13+).

**Windows (Developer PowerShell for VS 2022)**

- `cmake -S . -B build-msvc -G "Visual Studio 17 2022" -A x64`
- `cmake --build build-msvc --config Debug -- /m`

**Linux/WSL/macOS**

- `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug`
- `cmake --build build --config Debug -- -j`

## Test & Run

- Tests: `build-msvc\Debug\run_tests.exe` (Windows) or `./build/run_tests` (POSIX).
- REPL: `build-msvc\Debug\kizuna.exe` (Windows) or `./build/kizuna` (POSIX).

See `docs/DEMO.md` for an end-to-end walkthrough that exercises the SQL pipeline.

## SQL Quick Reference

```
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL, active BOOLEAN, nickname VARCHAR(32));
DROP TABLE [IF EXISTS] users;
INSERT INTO users (id, name, active, nickname) VALUES (1, 'miku', TRUE, 'diva');
SELECT name, active FROM users WHERE active LIMIT 5;
UPDATE users SET nickname = NULL WHERE id = 1;
DELETE FROM users WHERE active = FALSE;
TRUNCATE TABLE users;
```


Additional REPL helpers:

- `show tables`
- `schema <table>`

## Project Layout

- `src/common/` configuration, types, exceptions, logger
- `src/storage/` file manager, page/page_manager, record helpers, table heap
- `src/catalog/` table/column catalog persistence
- `src/sql/` AST definitions plus DDL/DML parsers
- `src/engine/` DDL/DML executors
- `src/cli/` REPL and command wiring
- `tests/` storage, catalog, parser, engine suites

## Notes

- Page 1 remains reserved for metadata; user pages start from 2.
- No WAL/concurrency yet future versions will build on this foundation.

