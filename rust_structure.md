# 🗂️ Arrow-SUS Project Structure

This document defines the high-level file and directory structure of the `arrow-sus` project, organizing it into clean components across Rust core, metadata handling, Python bindings, CLI tools, and tests.

______________________________________________________________________

## 📁 Top-Level Layout

```text
arrow-sus/
├── crates/
│   ├── core/                # Core logic for parsing, decoding, and querying
│   ├── metadata/            # Metadata loader from S3/local + validation
│   ├── cli/                 # Command-line interface (Typer-like)
│   └── python/              # PyO3 bindings
├── examples/                # Real-world use cases and scripts
├── tests/                   # Integration tests (Rust, Python)
├── scripts/                 # Utilities for syncing metadata, generating code, etc.
├── pyproject.toml           # Python build with maturin
├── Cargo.toml               # Rust workspace root
└── README.md
```

______________________________________________________________________

## 📦 Crate: `core/`

Handles:

- DBC decoding
- Lazy loading
- Arrow/Polars interop
- File filtering logic

```text
core/
├── src/
│   ├── lib.rs
│   ├── loader.rs           # File discovery, filtering, UF/competência logic
│   ├── parser.rs           # Fast DBC decoding
│   ├── reader.rs           # Arrow-compatible streaming reader
│   ├── polars_adapter.rs   # Arrow -> Polars LazyFrame
│   └── types.rs            # Core types: Row, FieldLayout, SubsystemMetadata
└── Cargo.toml
```

______________________________________________________________________

## 📦 Crate: `metadata/`

Handles:

- S3-based metadata loading
- Local caching
- Schema validation
- Versioning support

```text
metadata/
├── src/
│   ├── lib.rs
│   ├── model.rs            # FieldLayout, SubsystemMetadata, etc.
│   ├── loader.rs           # Load from S3, local, or override path
│   ├── cache.rs            # Local caching by ETag
│   └── validate.rs         # Layout validation utilities
├── build.rs                # (Optional) JSON → Rust static gen
└── Cargo.toml
```

______________________________________________________________________

## 📦 Crate: `cli/`

Handles:

- End-user commands
- JSON or table output
- Developer tools (inspect, validate, profile)

```text
cli/
├── src/
│   ├── main.rs
│   ├── commands/
│   │   ├── scan.rs
│   │   ├── inspect.rs
│   │   ├── validate.rs
│   │   └── profile.rs
│   └── output.rs           # Rich output formatting
└── Cargo.toml
```

______________________________________________________________________

## 🐍 Crate: `python/`

Handles:

- PyO3 interface
- Python API surface
- Automatic `.pyi` generation
- Expose LazyFrames

```text
python/
├── src/
│   ├── lib.rs              # PyO3 module
│   ├── bindings.rs         # Bindings to Rust structs
│   └── docgen.rs           # Optional: auto-gen docstrings
├── pyproject.toml
└── Cargo.toml
```

______________________________________________________________________

## 🧪 Tests

```text
tests/
├── golden_files/           # Reference files with expected parsed output
├── test_core.rs
├── test_metadata.rs
└── test_integration.rs     # Cross-crate / CLI tests
```

______________________________________________________________________

## 🧾 Scripts

```text
scripts/
├── sync_metadata.py        # Pull metadata and validate
├── generate_layouts.rs     # Optional codegen from JSON to Rust consts
└── profile_decoder.rs      # Benchmarks
```

______________________________________________________________________

## 📁 Examples

```text
examples/
├── minimal_scan.py
├── export_to_parquet.py
└── batch_query_all_ufs.py
```

______________________________________________________________________

## 📦 Python Package Structure (after build)

After compiling with `maturin`, Python users will see:

```text
arrow_sus/
├── __init__.py
├── scan.py                # Entry points to scan_xxx() APIs
├── describe.py            # Schema inspection
├── types.py               # Domain enums, constants, etc.
├── _arrow_sus.so          # Compiled PyO3 module
└── py.typed                # MyPy support
```

______________________________________________________________________

## 🧠 Summary

| Area        | Responsibility                             |
| ----------- | ------------------------------------------ |
| `core/`     | Fast parsing + Arrow/Polars logic          |
| `metadata/` | Schema management (JSON + S3)              |
| `cli/`      | Rich command-line tools                    |
| `python/`   | PyO3 bindings with ergonomic Polars output |
| `tests/`    | Full-stack validation                      |
| `scripts/`  | Tooling and utilities                      |
| `examples/` | Real-world usage patterns                  |

This structure makes `arrow-sus` highly maintainable, extensible, and performant, with clear boundaries between runtime logic, user interface, and developer tooling.
