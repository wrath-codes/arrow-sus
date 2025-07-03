use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FileExtension {
    pub extension: &'static str,         // ".dbc"
    pub description: &'static str,       // "DBC file (Data Base Compressed)"
    pub mime_type: Option<&'static str>, // e.g., "application/octet-stream"
}

pub const FILE_EXTENSIONS: &[FileExtension] = &[
    FileExtension {
        extension: ".dbc",
        description: "DBC file (Data Base Compressed)",
        mime_type: Some("application/octet-stream"),
    },
    FileExtension {
        extension: ".zip",
        description: "ZIP archive",
        mime_type: Some("application/zip"),
    },
    FileExtension {
        extension: ".gzip",
        description: "GZIP compressed archive",
        mime_type: Some("application/gzip"),
    },
    FileExtension {
        extension: ".parquet",
        description: "Apache Parquet columnar storage",
        mime_type: Some("application/octet-stream"),
    },
    FileExtension {
        extension: ".dbf",
        description: "DBF (dBase) file",
        mime_type: Some("application/octet-stream"),
    },
    FileExtension {
        extension: ".csv",
        description: "Comma-separated values file",
        mime_type: Some("text/csv"),
    },
    FileExtension {
        extension: ".json",
        description: "JSON (JavaScript Object Notation) file",
        mime_type: Some("application/json"),
    },
];
