# 📦 Arrow-SUS Metadata Architecture

This document describes the complete architecture for the metadata system used by the `arrow-sus` project. It draws from `dankkom/datasus-metadata` but defines a stricter, more performance-oriented, and extensible model, with support for S3-based hosting and structured evolution.

______________________________________________________________________

## ✅ Goals

- Define a **canonical metadata schema** for all SUS subsystems (SIH, CNES, SIA, etc.)
- Store metadata centrally in **S3** for scalable, versioned access
- Support dynamic file presence (e.g. `competência` and `UF` availability)
- Enable **zero-copy** decoding by encoding offset + length statically
- Allow local caching for offline use and testing
- Provide a consistent API for both **Rust core** and **Python client**
- Automatically generate documentation, stubs, and metadata-based utilities

______________________________________________________________________

## 🧱 Metadata Schema (JSON)

Each metadata file describes a subsystem layout and characteristics.

### Example: `s3://arrow-sus-metadata/sih/v1.json`

```json
{
  "subsystem": "SIH",
  "version": "v1",
  "layout": [
    {
      "name": "IDADE",
      "offset": 42,
      "length": 2,
      "type": "int",
      "nullable": true,
      "domain": {
        "00": "less than 1 year",
        "01": "1 year",
        "99": "unknown"
      },
      "description": "Idade em anos completos"
    }
  ],
  "uf_codes": ["AC", "AL", "AM", "BA", "SP", ...],
  "competencias": {
    "SP": ["2023-01", "2023-02"],
    "RJ": ["2022-12"]
  },
  "start_year": 2008,
  "last_updated": "2025-06-25T00:00:00Z"
}
```

______________________________________________________________________

## 📁 S3 Layout

```text
s3://arrow-sus-metadata/
├── sih/
│   ├── v1.json
│   └── v2.json
├── cnes/
│   └── v1.json
...
```

- Each subsystem has its own folder
- Multiple versions can coexist
- You can fetch `latest` or pin specific versions

______________________________________________________________________

## 🔧 Rust API

### Load metadata

```rust
fn load_metadata(subsystem: &str, version: Option<&str>) -> Result<SubsystemMetadata>
```

### `SubsystemMetadata` struct (example)

```rust
pub struct FieldLayout {
    pub name: &'static str,
    pub offset: usize,
    pub length: usize,
    pub dtype: FieldType,
    pub nullable: bool,
    pub domain: Option<HashMap<&'static str, &'static str>>,
    pub description: Option<&'static str>,
}

pub struct SubsystemMetadata {
    pub name: &'static str,
    pub version: &'static str,
    pub layout: &'static [FieldLayout],
    pub ufs: &'static [&'static str],
    pub competencias: HashMap<String, Vec<String>>,
    pub start_year: u16,
}
```

- Metadata is cached locally using ETag/Last-Modified headers
- Parsed using `serde_json` or generated via `build.rs`

______________________________________________________________________

## 🧪 Testing & Dev

- CLI override to use local metadata:

  ```bash
  arrow-sus scan sih --metadata-path ./local/metadata
  ```

- Hot-reloading supported for iterative development

- Unit tests against golden layouts

______________________________________________________________________

## 🐍 Python Interface

Python bindings will expose:

### Schema inspection

```python
arrow_sus.describe_schema("SIH")
```

Returns:

```json
[
  {
    "name": "IDADE",
    "offset": 42,
    "length": 2,
    "type": "int",
    "nullable": true,
    "domain": {
      "00": "less than 1 year",
      ...
    }
  }
]
```

- Will be used to generate `.pyi` stubs with full docstrings
- Usable by IDEs, MkDocs, Sphinx, etc.

______________________________________________________________________

## 📦 Metadata Advantages

| Feature                          | Supported |
| -------------------------------- | --------- |
| Static layout per subsystem      | ✅        |
| Dynamic discovery of years/files | ✅        |
| Versioning and compatibility     | ✅        |
| Offset-based decoding            | ✅        |
| Nullable & domain support        | ✅        |
| Local + S3 fallback              | ✅        |
| Automatic Python docgen          | ✅        |

______________________________________________________________________

## 🧩 Future Plans

- CLI for schema editing/validation:

  ```bash
  arrow-sus metadata validate ./layout.json
  ```

- Web UI for editing & previewing schemas

- Auto-sync with `dankkom/datasus-metadata` for field suggestions

- Integration with Arrow Flight SQL + ADBC metadata reflection

- Auto-generate test datasets based on layout for fuzz testing

______________________________________________________________________

## 🧠 Summary

This metadata architecture offers a **flexible**, **extensible**, and **high-performance** foundation for all `arrow-sus` pipelines, while keeping developer experience and versioning under tight control.

It gives you:

- Full control over semantics and layout
- S3-backed global access
- Static typing and performance on the Rust side
- Ergonomic interfaces in Python
