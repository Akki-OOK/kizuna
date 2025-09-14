Kizuna — Developer Notes (V0.1)

Overview
- Scope: minimal common utilities (config, types), exceptions, logger, and file manager with page I/O.
- Standard: C++20 (for std::source_location).
- Build: CMake. Library target: `kizuna_common`. Tests: `run_tests`.

Modules
- common/config.h: Central configuration and limits. Keep config-only constants here.
- common/types.h: Core enums and type aliases. Avoid duplicating config or error codes here.
- common/exception.h/.cpp: Structured exceptions with StatusCode and source location. Helpers to classify errors.
- common/logger.h/.cpp: Thread-safe logging to console + rolling file. Default file: `kizuna.log`.
- storage/file_manager.h/.cpp: Minimal fixed-page file I/O (open, read_page, write_page, allocate_page).
- storage/page.h: Page layout (24B header), slot directory, length-prefixed records, basic insert/read/erase.
- storage/page_manager.h/.cpp: Tiny LRU page cache on top of FileManager with pin/unpin, fetch, flush, evict, and new_page.
- storage/record.h/.cpp: Simple record encode/decode utilities for fixed and variable-length fields.

Testing
- Minimal tests live under `tests/` and build into `run_tests` (no external deps).
- Keep tests deterministic and fast. Use `./temp/` for any files created during tests.
- Run: `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug` then `cmake --build build --config Debug -- -j` and `build/run_tests` (or `cmake --build build --target test`).

Examples
- Optionally add `examples/` with tiny programs demonstrating modules (not included yet in V0.1).

Coding Guidelines
- Keep headers self-contained (include what they use).
- Prefer clear interfaces; keep implementations in `.cpp`.
- Use `StatusCode` + typed exceptions for error reporting.
- Use `kizuna::config` for tunables.

Change Log (append entries as you go)
- Added: exceptions implementation (`src/common/exception.cpp`)
- Added: logger (`src/common/logger.h/.cpp`) with console+file and rotation.
- Added: file manager (`src/storage/file_manager.h/.cpp`) page I/O API.
- Added: minimal tests and `run_tests` target.
- Added: page layout/ops in `src/storage/page.h` (header-only for V0.1).
- Added: page cache `src/storage/page_manager.h/.cpp` with simple LRU and persistence.
- Added: record helpers `src/storage/record.h/.cpp` with typed fields and little-endian framing.

Troubleshooting Log (Issues & Fixes)
- IntelliSense C++20 mismatch: Squiggles on `std::source_location` when IDE was set to C++17. Fixed by setting C++20 in VS Code; CMake uses C++20 (`CMakeLists.txt`).
- MSVC flags error: `/Wextra` invalid on MSVC caused build failures. Fixed by conditional flags in `CMakeLists.txt` (use `/Zi /W4` on MSVC; `-Wall -Wextra` on GCC/Clang).
- Slotted-page overlap (early): Records placed near page end overlapped with growing slot directory after many inserts → corrupted early slots. Fixed by writing records upward from header (`free_space_offset`) and reserving slot space before insert; reads validate against `free_space_offset` (`src/storage/page.h`).
- Disk page offset bug: Used `offset = page_id * PAGE_SIZE` with 1-based ids, causing file to grow 2 pages per allocation. Fixed to `(page_id - 1) * PAGE_SIZE` (`src/storage/file_manager.h`).
- Metadata page misuse: Writing to page 1 (metadata) led to later metadata updates making reads fail. Fixed by REPL guardrails: block `write_demo/read_demo/freepage` on page 1 and add clear messages (`src/cli/repl.cpp`).
- Free-list scalability cap: Single metadata page array capped ~1,016 free ids. Replaced with SQLite-style freelist using trunk pages: metadata stores `first_trunk_id`+`free_count`; trunk pages store `next_trunk`+`leaf_count`+leaf ids (`src/storage/page_manager.h/.cpp`).
- Extension consistency: Switched DB extension from `.kdb` to `.kz` via `config::DB_FILE_EXTENSION`; updated REPL defaults and tests.
- REPL UX improvements: Added existence checks, page-type checks, clearer error messages for invalid slots and pages; prevents common mistakes before hitting storage errors (`src/cli/repl.cpp`).
- Unauthorized access after free: It was possible to read/write a page after `freepage`. Fixed by enforcing page-type checks in `Page::insert/read/erase` (throw `INVALID_PAGE_TYPE` unless `PageType::DATA`) and keeping REPL guards for non-data and metadata pages (`src/storage/page.h`, `src/cli/repl.cpp`).

Test Enhancements (Edge Cases)
- Page capacity: Insert until full; verify slot 0, middle, and last read/decoded (`tests/page_test.cpp`).
- File I/O edges: Page 0 read throws; invalid write size throws; allocation increments page count by 1 (`tests/file_manager_edge.cpp`).
- Freelist persistence & reuse: Allocate N, free all, reopen, allocate N/2 and verify ids are reused (SQLite-like trunk freelist) (`tests/page_manager_freelist_test.cpp`).
- Record limits: Oversized record triggers `RECORD_TOO_LARGE`; mixed-type encode/decode roundtrip (`tests/record_test.cpp`).
Demo Script
- See `docs/DEMO.md` for a step-by-step build, test, and REPL walkthrough you can follow during demos/interviews.
