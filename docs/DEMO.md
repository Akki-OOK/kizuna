# Kizuna V0.4 - Demo Script

This walkthrough shows the current SQL surface (DDL + richer DML) and the legacy storage tooling that still ships in the REPL. Use it as a lab script or as talking points when you walk someone through the project.

## 0. Prep
- Build Debug once: cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
- Compile/run tests: cmake --build build --config Debug -- /m and uild\Debug\run_tests.exe
- Launch the REPL: uild\Debug\kizuna.exe

If the demo database does not exist yet, the REPL creates ./data/demo.kz when open runs.

## 1. Quick tour
`
> help
`
Call out the helper commands:
- show tables lists the catalog (name, ids, root pages, column counts)
- schema <table> prints column metadata plus the original CREATE statement
- DDL section handles CREATE TABLE and DROP TABLE [IF EXISTS]
- DML section now covers INSERT, SELECT <columns> ... WHERE ... LIMIT, UPDATE ... SET ... WHERE ..., DELETE [WHERE ...], and TRUNCATE

Show initial catalog:
`
> open
> show tables
`
Fresh databases report Tables (0).

## 2. CREATE + schema
`
> CREATE TABLE ook (id INT PRIMARY KEY, name VARCHAR(25), active BOOLEAN, joined DATE, nickname VARCHAR(16));
> show tables
> schema ook
`
Explain that table metadata lives in the catalog heap (page 1) and the table heap starts on the listed root page.

## 3. INSERT and SELECT with projection, predicates, and LIMIT
`
> INSERT INTO ook VALUES (1, 'nice', TRUE,  '2023-06-01', 'ace');
> INSERT INTO ook VALUES (2, 'not nice', FALSE, '2022-05-05', NULL);
> INSERT INTO ook VALUES (3, 'still nice', TRUE, '2021-02-14', NULL);
> SELECT name, active FROM ook WHERE active LIMIT 2;
`
Point out that column order follows the projection list and LIMIT stops scanning once the requested rows are produced.

`
> SELECT *, name FROM ook WHERE nickname IS NULL;
`
Projection mixes * with named columns. Call out that the extra 
ame column appears twice, matching the SQL order.

## 4. UPDATE with WHERE filters
`
> UPDATE ook SET name = 'ally', nickname = NULL WHERE id = 1;
> SELECT id, name, nickname FROM ook WHERE id = 1;
`
Updates are type-checked (DATE strings must parse, VARCHAR lengths respected) and reuse space in-place when possible. Mention that longer payloads relocate safely to a new slot.

## 5. DELETE vs TRUNCATE
`
> DELETE FROM ook WHERE active = FALSE;
> SELECT id, name, active FROM ook;
> INSERT INTO ook VALUES (4, 'back again', TRUE, '2020-12-12', 'back');
> TRUNCATE TABLE ook;
> SELECT * FROM ook;
`
Explain that DELETE tombstones matching rows while TRUNCATE resets the heap and freelist linkage.

## 6. DROP TABLE cleanup
`
> DROP TABLE IF EXISTS nope;
> DROP TABLE ook;
> show tables
`
DROP TABLE without the clause throws if the name is missing; the IF EXISTS form logs a "no-op" message instead of an error.

## 7. Legacy storage commands (optional)
These still help discuss the V0.1 page layout:
`
> newpage DATA
> write_demo 5
> read_demo 5 0
> status
`
Mention that the catalog and freelist share the same page manager API as user data.

## 8. Error showcase
- Syntax: CREATE TABLE broken id INT); -> [SYNTAX_ERROR]
- Duplicate column: CREATE TABLE dup (c INT, c INT);
- Bad drop: DROP TABLE ghosts; -> [TABLE_NOT_FOUND]
- Type mismatch: INSERT INTO ook VALUES (1); (wrong arity)
- Constraint: UPDATE ook SET name = NULL WHERE id = 1; (NOT NULL violation)

## 9. Logging tips
`
> loglevel DEBUG
`
This prints every AST, executor call, and storage mutation. Call out how to drop back to INFO to cut the noise.

## 10. Talking points
- Parser now handles column projections, WHERE expressions (logical + comparison operators), LIMIT, UPDATE assignments, and NULL tests.
- Expression evaluator uses tri-valued logic so NULL predicates propagate correctly.
- Table heap updates reuse space when the payload fits, otherwise relocate without double-updating.
- The REPL mirrors SQL output with projection-aware headers and row counts so manual verification is easy.
- Page 1 remains the metadata root; free pages are tracked in a linked freelist.
- Concurrency, WAL, and joins are future roadmap items (see docs/V0_4_PLAN.md).
