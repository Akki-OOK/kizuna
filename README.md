# Kizuna (V0.1)

A lightweight, educational DBMS implemented in modern C++ (C++20). V0.1 delivers the storage foundation: file I/O, slotted pages, a tiny buffer pool with LRU, a scalable freelist (SQLite‑style trunk pages), typed record encoding, structured exceptions, logging, tests, and a REPL demo.

## Features (V0.1)
- File I/O with fixed‑size pages (default 4KB)
- Slotted page layout: records grow upward from header; slot directory grows downward
- Page cache with pin/unpin, flush/evict (LRU)
- SQLite‑inspired freelist using trunk pages (persistent reuse of freed pages)
- Typed record encode/decode (INTEGER, BIGINT, DOUBLE, VARCHAR, etc.)
- Structured exceptions with status codes + `std::source_location`
- Thread‑safe logger (console + rotating file)
- Minimal REPL for interactive demo
- Edge‑case tests for storage, pages, records, freelist

## Build
Prereqs: CMake 3.10+, a C++20 compiler

Windows (Developer PowerShell for VS 2022)
- `cmake -S . -B build-msvc -G "Visual Studio 17 2022" -A x64`
- `cmake --build build-msvc --config Debug -- /m`

Linux/WSL/macOS
- `cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug`
- `cmake --build build --config Debug -- -j`

## Test & Run
Run tests
- Windows: `build-msvc\Debug\run_tests.exe`
- Linux/WSL/macOS: `./build/run_tests`

Run REPL
- Windows: `build-msvc\Debug\kizuna.exe`
- Linux/WSL/macOS: `./build/kizuna`

See `docs/DEMO.md` for a step‑by‑step demo script.

## Project Layout
- `src/common/`: config, types, exceptions, logger
- `src/storage/`: file manager, page (slotted), page manager (LRU + freelist), record helpers
- `src/cli/`: REPL
- `tests/`: minimal edge‑case tests
- `docs/`: developer notes and demo script

## Docs
- Developer Notes: `docs/DEV_NOTES.md` (design, troubleshooting log, test notes)
- Demo Script: `docs/DEMO.md` (build/test/run walkthrough)
- SRS: `kizuna-SRS.md`

## Notes
- Page 1 is reserved for metadata; user pages start at 2.
- DB file extension is `.kz` (configurable via `config::DB_FILE_EXTENSION`).
- V0.1 prioritizes correctness and clarity; WAL/concurrency will arrive in later versions.

