# Kizuna V0.5 – Demo Script

This walkthrough highlights the current SQL surface (DDL + richer DML + secondary indexes/ORDER BY) and the legacy storage tooling that still ships in the REPL. Use it as a lab script or talking points when walking someone through the project.

## 0. Prep
- Configure once: `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug`
- Build & test: `cmake --build build --config Debug` then `build\Debug\run_tests.exe`
- Launch the REPL: `build\Debug\kizuna.exe`

On first run the REPL creates `build\Debug\database\…` (catalog, data, indexes, logs, temp) beside the executable.

## 1. Quick tour
```
> help
```
Call out:
- `show tables` lists catalog entries (name, ids, root pages, column counts)
- `schema <table>` prints column metadata plus the original CREATE statement
- DDL covers `CREATE TABLE` and `DROP TABLE [IF EXISTS]`
- DML covers INSERT, SELECT with WHERE/LIMIT, UPDATE with WHERE, DELETE with WHERE, TRUNCATE, and ORDER BY with single-column support

Show the fresh catalog:
```
> open
> show tables
```
New databases report `Tables (0)`.

## 2. CREATE + schema
```
> CREATE TABLE ook (id INT PRIMARY KEY, name VARCHAR(25), active BOOLEAN, joined DATE, nickname VARCHAR(16));
> show tables
> schema ook
```
Explain catalog (page 1) vs. table heap (root page shown in `show tables`).

## 3. INSERT and SELECT (projection, predicates, LIMIT, ORDER BY)
```
> INSERT INTO ook VALUES (1, 'nice', TRUE,  '2023-06-01', 'ace');
> INSERT INTO ook VALUES (2, 'not nice', FALSE, '2022-05-05', NULL);
> INSERT INTO ook VALUES (3, 'still nice', TRUE, '2021-02-14', NULL);
> SELECT name, active FROM ook WHERE active LIMIT 2;
```
Projection order matches the SELECT list; LIMIT stops scanning once satisfied.

```
> SELECT *, name FROM ook WHERE nickname IS NULL;
```
Projection can mix `*` and named columns (expect `name` twice).

```
> SELECT id, joined FROM ook ORDER BY joined DESC;
```
ORDER BY reuses an index when available; otherwise performs a stable in-memory sort.

## 4. UPDATE with filters
```
> UPDATE ook SET name = 'ally', nickname = NULL WHERE id = 1;
> SELECT id, name, nickname FROM ook WHERE id = 1;
```
Updates are type-checked; short payload changes reuse space, longer rows relocate safely.

## 5. DELETE vs TRUNCATE
```
> DELETE FROM ook WHERE active = FALSE;
> SELECT id, name, active FROM ook;
> INSERT INTO ook VALUES (4, 'back again', TRUE, '2020-12-12', 'back');
> TRUNCATE TABLE ook;
> SELECT * FROM ook;
```
DELETE tombstones matching rows; TRUNCATE resets the heap and freelist pointers.

## 6. Secondary indexes & indexed scans
```
> CREATE INDEX idx_ook_name ON ook(name);
> show tables
```
Point out the index entry in the catalog and the new file under `database\indexes\`.

```
> INSERT INTO ook VALUES (1, 'amy', TRUE, '2020-01-01', NULL), (2, 'beth', TRUE, '2021-01-01', NULL);
> SELECT id, name FROM ook WHERE name = 'beth';
```
Highlight that the executor plans an index lookup (DEBUG log shows index usage).

```
> UPDATE ook SET name = 'bethany' WHERE id = 2;
> SELECT id, name FROM ook WHERE name = 'bethany';
```
Index entries stay consistent across UPDATEs.

```
> DROP INDEX idx_ook_name;
```
Demonstrate catalog/index manager lifecycle.

## 7. DROP TABLE cleanup
```
> DROP TABLE IF EXISTS nope;
> DROP TABLE ook;
> show tables
```
`DROP TABLE` without the clause raises an error; `IF EXISTS` is a no-op with a message.

## 8. Legacy storage commands (optional)
```
> newpage DATA
> write_demo 5
> read_demo 5 0
> status
```
Useful for discussing the V0.1 page layout: catalog and freelist share the same page manager API as user data.

## 9. Error showcase
- Syntax: `CREATE TABLE broken id INT);` → `[SYNTAX_ERROR]`
- Duplicate column: `CREATE TABLE dup (c INT, c INT);`
- Missing table: `DROP TABLE ghosts;` → `[TABLE_NOT_FOUND]`
- Arity: `INSERT INTO ook VALUES (1);`
- Constraint: `UPDATE ook SET name = NULL WHERE id = 1;`
- Duplicate index: `CREATE INDEX dup ON ook(name);` → `[DUPLICATE_KEY]`
- ORDER BY misuse: `SELECT id FROM ook ORDER BY name, joined;` → `[SYNTAX_ERROR]`

## 10. Logging tips
```
> loglevel DEBUG
```
Debug level prints every AST, executor call, and storage mutation. Drop back to INFO to reduce noise.

## 11. Talking points
- Parsers handle projection lists, WHERE expressions, LIMIT, UPDATE assignments, NULL tests, and single-column ORDER BY.
- Expression evaluator uses tri-valued logic (NULL-aware).
- Table heap updates reuse slots when possible, relocate otherwise.
- B+ tree indexes back CREATE/DROP INDEX and auto PK indexes; DML uses them for equality/range scans and keeps them consistent on INSERT/UPDATE/DELETE.
- ORDER BY leverages single-column indexes when present; otherwise falls back to a stable in-memory sort.
- REPL output mirrors projections, ordering, and row counts.
- Page 1 remains catalog metadata; freelist uses SQLite-style trunks.
- Concurrency, WAL, and joins remain future roadmap items (see `docs/V0_5_PLAN.md`).
