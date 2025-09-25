Kizuna V0.3 - Demo Script

Goal
- Pitch the storage-to-SQL story: CREATE table, INSERT rows, SELECT them, DELETE/TRUNCATE, and peek at catalog + storage helpers.

Prerequisites
- CMake 3.20+
- C++20 toolchain (MSVC 2022, GCC 11+, or Clang 13+)

Build
- Windows (Developer PowerShell for VS 2022):
  - cmake -S . -B build-msvc -G "Visual Studio 17 2022" -A x64
  - cmake --build build-msvc --config Debug -- /m
- Linux/WSL/macOS:
  - cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
  - cmake --build build --config Debug -- -j

Run Tests
- Windows: build-msvc\Debug\run_tests.exe
- Linux/WSL/macOS: ./build/run_tests
- Expect all suites PASS (storage, catalog, parser, engine, REPL plumbing).

Reset Demo (optional)
- Remove previous demo DB so catalog starts clean:
  - Windows: Remove-Item .\data\demo.kz -ErrorAction SilentlyContinue
  - Linux/WSL/macOS: rm -f ./data/demo.kz

Launch REPL
- Windows: build-msvc\Debug\kizuna.exe
- Linux/WSL/macOS: ./build/kizuna

Scripted Walkthrough (type in REPL)
1. help
   - Shows legacy page commands plus SQL DDL/DML support (CREATE/DROP/INSERT/SELECT/DELETE/TRUNCATE).
2. open
   - Opens/creates ./data/demo.kz, loads catalog roots, instantiates executors.
3. show tables
   - Lists catalog entries. Fresh DB prints "(no tables yet)".
4. CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL, age INT DEFAULT 0, active BOOLEAN DEFAULT TRUE);
   - Parser -> DDL executor -> catalog manager. Expect: Table created: users.
5. schema users
   - Dumps column metadata (names, types, nullability, defaults) from catalog.
6. INSERT INTO users (id, name, age, active) VALUES (1, 'miku', 16, TRUE), (2, 'rin', 15, FALSE);
   - DML executor binds literals, writes two rows via TableHeap. Expect: Inserted 2 row(s).
7. SELECT * FROM users;
   - Full-table scan. Shows column header + each row + "2 row(s)."
8. DELETE FROM users;
   - Marks tombstones; next SELECT shows "(no rows)" but table structure intact.
9. INSERT INTO users VALUES (3, 'len', 15, TRUE);
   - Demonstrates tombstone reuse (page free space reused).
10. TRUNCATE TABLE users;
    - Drops all table heap pages (except root) and resets row count.
11. DROP TABLE users;
    - Clears catalog metadata and deletes table file. Trying again without IF EXISTS triggers TABLE_NOT_FOUND; with IF EXISTS prints "Table not found (no-op)."
12. INSERT INTO ook VALUES (1, 'Akki');
    - Strings must be wrapped in single quotes; leaving them bare makes the parser think they are identifiers.

Optional Page Nerd Out
- newpage DATA
- write_demo <page_id>
- read_demo <page_id> <slot>
- status (shows page count, freelist trunk size, table count)
- freepage <page_id>

Error Handling Demos
- INSERT INTO users VALUES (1);                  -> COLUMN_COUNT_MISMATCH
- INSERT INTO users (id, name) VALUES (1, 42);    -> TYPE_MISMATCH (INT literal into VARCHAR)
- Malformed SQL: INSERT users VALUES (1);         -> SQL error: expected INTO ...
- DROP TABLE ghosts;                              -> [TABLE_NOT_FOUND]

Logging
- Logs go to kizuna.log (rotates at config::MAX_LOG_SIZE_BYTES).
- Set verbosity: loglevel DEBUG.

Notes to Mention
- TableHeap chains DATA pages via next_page_id; tombstones let us avoid rewrite on DELETE.
- Null bitmap lives at the front of each record so SELECT can quickly skip NULL fields.
- Parser is hand-written like early SQLite; easy to extend for WHERE once needed.
- Catalog backed by __tables__ and __columns__ pages; TRUNCATE keeps catalog entry but clears heap pages.

