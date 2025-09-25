Kizuna - Developer Notes (V0.3)

Overview
- Storage-first teaching database that now covers catalogued SQL DDL (V0.2) and basic SQL DML (V0.3).
- Pipeline: SQL text -> lexer/parser -> AST -> engine executor -> catalog + storage (TableHeap + PageManager).
- Focus areas this sprint: record format with null bitmap, chained heap pages, DML executor, REPL ergonomics.

Modules
- common/config.h: Tunables (page size, directories, version toggles) plus helper funcs.
- common/types.h: Shared enums, ids, and SQL value helpers (see LiteralValue for DML binding).
- common/exception.h/.cpp: StatusCode taxonomy + DBException/QueryException wrappers that carry source location.
- common/logger.h/.cpp: Lightweight singleton logger with level control and rotating file sink.
- storage/file_manager.h/.cpp: Backing file I/O and page allocation (1-based ids, freelist trunk layout).
- storage/page.h: Slotted page header, null-aware slots, tombstone flags, next-page pointer for heap chaining.
- storage/record.h/.cpp: Encode/decode routines that honor column metadata, null bitmap, and boolean literals.
- storage/page_manager.h/.cpp: Buffer manager + freelist allocator; now exposes helpers for heap growth and trunk maintenance.
- storage/table_heap.h/.cpp: Table-level helper that appends, iterates, tombstones, and truncates across chained DATA pages.
- catalog/schema.h/.cpp: Serializable structs for tables/columns; versions stay in sync with SRS schema spec.
- catalog/catalog_manager.h/.cpp: CRUD on catalog records + table root tracking + file lifecycle.
- sql/ast.h: AST nodes for both DDL and DML (CreateTable, DropTable, InsertStmt, SelectStmt, DeleteStmt, TruncateStmt).
- sql/ddl_parser.h/.cpp & sql/dml_parser.h/.cpp: Hand-written recursive-descent parsers with friendlier error text.
- engine/ddl_executor.h/.cpp: Bind DDL ASTs into catalog mutations and storage allocations, with constraint enforcement.
- engine/dml_executor.h/.cpp: Execute INSERT/SELECT/DELETE/TRUNCATE via TableHeap, type checking, and row materialisation.
- cli/repl.h/.cpp: Command handlers (status/show/schema) plus SQL dispatcher that routes DDL/DML and prints results.

Testing
- Configure: cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
- Build: cmake --build build --config Debug -- /m
- Run: build\Debug\run_tests.exe (or ./build/run_tests on POSIX)
- Suites cover: record encode/decode, page manager freelist, table heap insert/delete/truncate, SQL parser errors, DML executor flows.
- Tests write under ./temp/; safe to purge between runs.

Change Log (append new bullets as we iterate)
- Added: exceptions implementation (src/common/exception.cpp).
- Added: logger (src/common/logger.*) with console + file sinks.
- Added: file manager (src/storage/file_manager.*) for paged I/O.
- Added: page layout (src/storage/page.*) with slot directory and guard rails.
- Added: page cache (src/storage/page_manager.*) with freelist trunking.
- Added: record helpers (src/storage/record.*) covering INT/BIGINT/DOUBLE/BOOLEAN/VARCHAR.
- Added: catalog schema + manager (src/catalog/*) persisting __tables__/__columns__.
- Added: SQL AST + DDL executor path (src/sql/*, src/engine/ddl_executor.*) for CREATE/DROP TABLE.
- Added: REPL schema/show tables commands and nicer DROP IF EXISTS UX.
- Added: Record format retrofit with null bitmap + heap page linkage (V0.3 Step 1).
- Added: TableHeap abstraction that handles append, tombstone delete, and truncate with iterator (V0.3 Step 2).
- Added: SQL DML parser for INSERT/SELECT/DELETE/TRUNCATE (V0.3 Step 3).
- Added: DML executor + REPL integration + row printing (V0.3 Step 4 & 5).
- Added: Storage, SQL, and engine unit tests for V0.3 features; hooked into run_tests target (V0.3 Step 6).

Troubleshooting Log (Issues & Fixes)
- IntelliSense C++20 mismatch: set IDE standard to C++20 to match CMake flags.
- MSVC /Wextra invalid: switch to /W4 on MSVC via generator expressions.
- Slotted page overlap: fix by reserving slot space before writes and bounding by free_space_offset.
- Disk offset bug (1-based ids): use (page_id - 1) * PAGE_SIZE.
- Metadata misuse: guard REPL commands from writing to page 1.
- Freelist scaling: trunk/leaf freelist like SQLite, stored in metadata + dedicated pages.
- Post-free access: enforce page type checks before record ops.
- Null bitmap regression: align slot payload to decode helper and zero unused bitmap bits (tests cover it).
- Heap chain corruption: always set next_page_id and flush parent before allocating another page.

Test Enhancements (Edge Cases)
- Page capacity: fill until full; verify sum of slot lengths matches page usage.
- File I/O edges: guard invalid reads/writes; ensure allocate increments count once.
- Freelist persistence: trunk reuse validated after reopen.
- Record roundtrip: mix types, nulls, and bool literal text.
- TableHeap: insert across multiple pages, delete some rows, reinsert to reuse tombstones.
- SQL parsers: cover happy path + malformed tokens for DDL and DML.
- DML executor: insert/select/delete/truncate all rows through catalog + storage.

Demo Script
- Walkthrough lives in docs/DEMO.md; shows new DML flow (INSERT -> SELECT -> DELETE/TRUNCATE) plus legacy storage ops.


