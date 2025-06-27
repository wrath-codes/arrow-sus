# arrow-sus: Static Metadata Pipeline and Storage Layer Plan

This document defines the architecture for building a fully static, metadata-driven pipeline and storage layer for the `arrow-sus` project, using `phf_codegen`, Arrow, and Rust.

______________________________________________________________________

## 📦 Goal

Build a high-performance, zero-alloc, introspectable metadata system for processing and enriching SUS datasets using statically defined sources such as:

- Subsystems (SIH, SIA, CIHA, CNES, etc.)
- UFs and municipalities (IBGE codes, geo, timezone)
- Group codes (e.g., RD, SP, AB, LT)
- DBC layout schemas (field name, type, width)
- Source paths (FTP or S3-compatible)

______________________________________________________________________

## 📁 Directory Layout

```text
arrow-sus/
├── Cargo.toml
├── build.rs
├── data/                         # Static JSON metadata inputs
│   ├── subsystems.json
│   ├── municipalities.json
│   ├── ufs.json
│   ├── groups.json
│   └── schemas/
│       ├── SIH_RD.json
│       ├── SIA_AB.json
│       └── ...
├── src/
│   ├── lib.rs
│   ├── types.rs                 # Shared structs: Subsystem, UF, etc.
│   ├── parser.rs                # Arrow + DBC parsing
│   ├── generated/               # Auto-generated Rust code via build.rs
│   │   ├── subsystems.rs
│   │   ├── municipalities.rs
│   │   ├── ufs.rs
│   │   ├── groups.rs
│   │   └── schemas.rs
```

______________________________________________________________________

## ⚙️ Metadata Pipeline Overview

### 1. Input Files

All metadata is stored in machine-readable JSON files in the `data/` directory. These files are designed to be versionable, readable, and easy to edit.

| File                  | Purpose                                                |
| --------------------- | ------------------------------------------------------ |
| `subsystems.json`     | List of SUS subsystems, their groups, UFs, paths, docs |
| `municipalities.json` | IBGE code → name, UF, lat/lon, timezone                |
| `ufs.json`            | UF abbreviation → full name, code, timezone            |
| `groups.json`         | Group code → description                               |
| `schemas/*.json`      | (subsystem, group) → field layout definitions          |

______________________________________________________________________

### 2. Build-Time Codegen (`build.rs`)

At compile time:

- `build.rs` reads all JSON files using `serde_json`
- Each dataset is compiled into a `phf::Map` using `phf_codegen`
- The generated maps are written to `src/generated/*.rs`

#### Example: Generate Municipality Map

```rust
let mut map = Map::new();
map.entry("3550308", r#"Municipality { name: "São Paulo", uf: "SP", latitude: -23.5, longitude: -46.6, timezone: "America/Sao_Paulo" }"#);
```

______________________________________________________________________

## 🧱 Core Types

```rust
pub struct SubsystemMetadata {
    pub groups: &'static [&'static str],
    pub ufs: &'static [&'static str],
    pub path: &'static str,
    pub description: &'static str,
    pub long_name: &'static str,
    pub long_description: &'static str,
    pub source: &'static str,
}

pub struct Municipality {
    pub ibge_code: &'static str,
    pub name: &'static str,
    pub uf: &'static str,
    pub latitude: f64,
    pub longitude: f64,
    pub timezone: &'static str,
}

pub struct UfMetadata {
    pub code: &'static str,
    pub name: &'static str,
    pub region: &'static str,
    pub timezone: &'static str,
}

pub struct SchemaField {
    pub name: &'static str,
    pub dtype: &'static str,
    pub width: usize,
}
```

______________________________________________________________________

## 🧠 Static Maps to Generate

| Map Name         | Type                                                             |
| ---------------- | ---------------------------------------------------------------- |
| `SUBSYSTEMS`     | `phf::Map<&'static str, SubsystemMetadata>`                      |
| `MUNICIPALITIES` | `phf::Map<&'static str, Municipality>`                           |
| `UFS`            | `phf::Map<&'static str, UfMetadata>`                             |
| `GROUPS`         | `phf::Map<&'static str, &'static str>`                           |
| `SCHEMAS`        | `phf::Map<(&'static str, &'static str), &'static [SchemaField]>` |

______________________________________________________________________

## 📦 Storage Layer Plan

### Inputs

- Compressed DBC files from FTP or S3 mirror
- Metadata from generated `phf` maps

### Processing Steps

1. Parse DBC file name → subsystem, UF, group, date
1. Lookup schema: `SCHEMAS.get((subsystem, group))`
1. Decode fixed-width fields using schema info
1. Build Arrow `RecordBatch` with appropriate types
1. Optionally enrich:
   - `ibge_code` → municipality info
   - `uf` → state region / timezone
1. Write to Arrow or Parquet

______________________________________________________________________

## 🧪 CLI / API Integration

### CLI Usage

```bash
arrow-sus parse --subsystem SIH --group RD --uf SP --year 2018 --month 01
```

- Validated by `SUBSYSTEMS`, `UFS`, `GROUPS`
- Parsed using static schema
- Enriched via `MUNICIPALITIES`

______________________________________________________________________

## 📚 External Reference

This pipeline takes inspiration from:

- [`dankkom/datasus-metadata`](https://github.com/dankkom/datasus-metadata) — for group/UFS/subsystem mapping
- [`phf_codegen`](https://docs.rs/phf_codegen) — for static lookup tables
- [`arrow2`](https://docs.rs/arrow2) or `polars` — for building the actual record batches

______________________________________________________________________

## ✅ Summary

The static metadata layer powers a robust, high-performance SUS data pipeline with:

- Zero runtime metadata parsing
- Fast compile-time lookup
- Full compatibility with Arrow
- Optional TUI/CLI/SDK enrichment

This design is extensible, maintainable, and built for scale.
