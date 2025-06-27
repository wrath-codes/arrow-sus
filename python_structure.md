# 🧱 Arrow-SUS Python Workspace Structure (Polylith)

This document outlines the structure for the `arrow-sus` Python workspace using the [Polylith architecture](https://polylith.gitbook.io/docs/). It follows the standard **components + bases** layout and integrates with the Rust/PyO3 core to provide a clean, composable, and scalable Python interface.

______________________________________________________________________

## 🧩 Overall Workspace Layout

```text
arrow_sus/
├── components/                 # Reusable components (Polylith "components")
│   ├── scan/               # scan_xxx() logic
│   ├── describe/           # Schema inspection
│   ├── metadata/           # Metadata client
│   ├── types/              # Shared types, enums, constants
│   ├── io/                 # Exporters (Parquet, Arrow IPC, etc.)
│   └── cli/                # CLI tools (Typer-compatible)
├── bases/                  # Entry points for applications (CLI, API)
│   ├── python_cli/         # Main Typer app
│   └── python_sdk/         # SDK for user-level API
├── projects/               # Virtualenv entry (editable install)
│   └── arrow_sus/          # Combines all components + chosen base
├── tests/                  # Unit + integration tests
│   ├── scan/
│   ├── describe/
│   ├── metadata/
│   └── ...
├── pyproject.toml
└── workspace.toml         # Polylith workspace config
```

______________________________________________________________________

## 🔨 Bricks

### 📦 `components/scan/`

- Public API:

  ```python
  scan_sih(), scan_cnes(), scan(subsystem: str)
  ```

- Internally:

  - Delegates to PyO3-backed core (e.g., `core.scan("SIH")`)
  - Applies dynamic filters
  - Returns `polars.LazyFrame`

______________________________________________________________________

### 📦 `components/describe/`

- Public API:

  ```python
  describe_schema("SIH")
  ```

- Uses metadata component

- Can return `Dict[str, FieldSchema]` or print Markdown table

______________________________________________________________________

### 📦 `components/metadata/`

- Downloads from S3
- Caches metadata locally
- Converts to Pydantic models or dataclasses

______________________________________________________________________

### 📦 `components/types/`

- Shared enums and constants (e.g., `UF`, `Subsystem`, `FieldType`)
- Shared helper functions (e.g., `parse_competencia()`)

______________________________________________________________________

### 📦 `components/io/`

- Export tools:

  ```python
  export_to_parquet(lf: LazyFrame, path: Path)
  export_to_arrow_ipc(lf: LazyFrame, path: Path)
  ```

______________________________________________________________________

### 📦 `components/cli/`

- Typer commands:

  ```bash
  arrow-sus scan sih --uf SP --competencia 202401
  arrow-sus describe cnes
  ```

______________________________________________________________________

## 🚀 Bases

### 🧰 `bases/python_cli/`

- Wraps `components.cli` into a `typer.Typer()` CLI
- CLI is exposed via `[project.scripts]` in `pyproject.toml`

### 🧠 `bases/python_sdk/`

- Public SDK:

  ```python
  from arrow_sus import scan_sih, describe_schema
  ```

- Depends on all core components (`scan`, `metadata`, `types`, etc.)

- Used in notebooks, pipelines, and tests

______________________________________________________________________

## 🧪 Tests

```text
tests/
├── scan/
│   └── test_scan_sih.py
├── describe/
│   └── test_describe_schema.py
├── metadata/
│   └── test_load_metadata.py
```

Each test module only imports its **own component** — true to the Polylith principle.

______________________________________________________________________

## 🧭 Polylith Files

### `workspace.toml`

```toml
[tool.polylith.workspace]
name = "arrow_sus"
components = ["components"]
bases = ["bases"]
projects = ["projects"]
```

### `pyproject.toml`

```toml
[project]
name = "arrow_sus"
version = "0.1.0"
dependencies = [
    "polars",
    "typer",
    "boto3",
    "pydantic>=2",
    "httpx",
    ...
]
[project.scripts]
arrow-sus = "arrow_sus.cli:app"
```

______________________________________________________________________

## 📚 Generated Python Package Structure

After build:

```text
arrow_sus/
├── __init__.py               # Re-exports from python_sdk base
├── scan.py                   # From scan component
├── describe.py               # From describe component
├── types.py                  # Enums + constants
├── _core.so                  # PyO3 binding to Rust core
└── py.typed
```

______________________________________________________________________

## 🧠 Summary

| Layer         | Responsibility                            |
| ------------- | ----------------------------------------- |
| `components/` | Pure, reusable, testable building blocks  |
| `bases/`      | Entry points for CLI and SDK              |
| `projects/`   | App deployment targets (editable install) |
| `tests/`      | Mirror each component for full isolation  |

This structure keeps Python code:

- Modular and fast to test
- Aligned with Rust core without tight coupling
- Cleanly separated between public interface and internal logic
