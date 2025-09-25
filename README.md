# Kizuna (V0.3)

Kizuna is a lightweight, teaching-focused DBMS written in modern C++ (C++20). V0.3 layers catalogued SQL on top of the storage core delivered in V0.1, adds SQL DDL support from V0.2, and now introduces the first slice of SQL DML (INSERT / SELECT / DELETE / TRUNCATE) with a friendlier REPL experience.

## Feature Highlights

- **Storage Core (V0.1)**: fixed-size paged file manager, slotted data pages, buffer pool with LRU, SQLite-style freelist trunks, typed record encode/decode, structured exceptions, logger, and REPL scaffolding.
- **Catalog & DDL (V0.2)**: persistent table/column catalog, SQL lexer/parser for CREATE/DROP TABLE, DDL executor wiring, REPL schema inspection and DROP IF EXISTS UX.
- **SQL DML (V0.3)**:
  - Table heap over chained storage pages with tombstones and truncate support.
  - AST + parser for INSERT, SELECT (full table scan), DELETE (table-wide), TRUNCATE.
  - DML executor enforcing column order, type/null checks, and simple row materialisation.
  - REPL commands `show tables` / `schema <table>` plus SELECT output aligned like catalog listings.
  - Expanded unit tests covering parser, storage, engine, and REPL plumbing.

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
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL);
DROP TABLE [IF EXISTS] users;
INSERT INTO users (id, name) VALUES (1, 'miku');
SELECT * FROM users;
DELETE FROM users;        -- clears all rows (no WHERE yet)
TRUNCATE TABLE users;     -- wipes table heap fast
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
- Default database extension is `.kz` (tunable via `config::DB_FILE_EXTENSION`).
- No WAL/concurrency yet future versions will build on this foundation.
