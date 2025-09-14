Kizuna V0.1 — Demo Script

Goal
- Show storage foundation working end-to-end: file I/O, pages, records, page cache, freelist, and the REPL.

Prerequisites
- CMake 3.10+
- Compiler with C++20: MSVC 2022, GCC 11+, or Clang 13+

Build
- Windows (Developer PowerShell for VS 2022):
  - `cmake -S . -B build-msvc -G "Visual Studio 17 2022" -A x64`
  - `cmake --build build-msvc --config Debug -- /m`
- Linux/WSL/macOS:
  - `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug`
  - `cmake --build build --config Debug -- -j`

Run Tests
- Windows: `build-msvc\Debug\run_tests.exe`
- Linux/WSL/macOS: `./build/run_tests`
- Expect: all tests PASS

Start Fresh (optional but recommended for clean demo)
- Delete previous demo DB file if present:
  - Windows: `del .\data\demo.kz`
  - Linux/WSL/macOS: `rm -f ./data/demo.kz`

Launch REPL
- Windows: `build-msvc\Debug\kizuna.exe`
- Linux/WSL/macOS: `./build/kizuna`

Scripted Walkthrough (type the following in REPL)
- `help`
  - Shows available commands and usage.
- `open`
  - Opens/creates default DB at `./data/demo.kz`.
- `status`
  - Shows DB path, size, and page count (should be 1 — metadata page only).
- `newpage`
  - Allocates a data page; prints its id (e.g., 2). `status` now shows 2 pages.
- `write_demo 2`
  - Writes a sample record (INTEGER=42, VARCHAR='hello world'); prints slot (likely 0).
  - Repeat 3–5 times to create more slots.
- `read_demo 2 0`
  - Reads and decodes the first record. Expect INTEGER=42 and VARCHAR='hello world'.
- `read_demo 2 1`
  - Reads the next record. Expect the same fields.
- `freepage 2`
  - Frees page 2 to the freelist.
- `newpage`
  - Allocates a new page; should reuse id 2 (from freelist trunk).
- `newpage`
  - Allocates another page; id increments (e.g., 3). `status` reflects page count growth by 1.

Error Handling Demos
- `read_demo 1 0`
  - REPL blocks with: "Page 1 is reserved for metadata; use a page >= 2".
- `write_demo 99` (when page count < 99)
  - REPL prints: "Page 99 does not exist (page count = N). Use 'newpage'."
- `read_demo 2 999`
  - REPL prints: "No such slot (slot_count=K)" (or "Empty/tombstoned or invalid record").

Logging
- Logs write to `kizuna.log` in the working directory.
- Increase verbosity: `loglevel DEBUG`.
- Rotation and retention are controlled in `src/common/config.h`.

Notes You Can Mention
- Page 1 is METADATA; user data pages start at 2.
- Records grow upward from the header; slot directory grows downward — avoiding overlap.
- Freelist is SQLite-inspired: metadata stores freelist head and count; trunk pages store leaf ids.
- Disk addressing uses 1-based page ids but zero-based offsets: `(page_id - 1) * PAGE_SIZE`.

