# Arrow SUS - Polars IO Plugins Implementation Plan

> Comprehensive plan for implementing Polars IO plugins while maintaining the builder API

## Table of Contents

- [Overview](#overview)
- [Architecture Design](#architecture-design)
- [Implementation Phases](#implementation-phases)
- [Technical Specifications](#technical-specifications)

## Overview

This document outlines the implementation plan for adding Polars IO plugins to Arrow SUS while maintaining the existing builder API. The goal is to provide both high-performance direct Polars integration and user-friendly builder patterns.

### Core Principles

- **Two-layer architecture**: IO plugins (performance) + Builder API (usability)
- **Seamless interoperability**: Both layers produce compatible LazyFrames
- **Progressive disclosure**: Simple API for beginners, advanced features for experts
- **Backward compatibility**: Existing builder API remains unchanged
- **Performance first**: Leverage all Polars optimizations (pushdown, streaming, etc.)

### Benefits

- **Predicate pushdown**: Filter data during DBF processing, not after
- **Projection pushdown**: Only extract needed columns from DBF files
- **Streaming support**: Process large files without loading entirely into memory
- **Ecosystem integration**: Native Polars composition with other data sources
- **Zero-copy operations**: Leverage Arrow's columnar format throughout

## Architecture Design

### Layer 1: IO Plugins (Low-level)

Direct Polars scan functions for maximum performance and ecosystem integration.

```python
import polars as pl

# Individual scan functions per DataSUS system
pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
pl.scan_datasus_sia(uf="RJ", group="PA", year=2023, month=6)
pl.scan_datasus_sim(uf="MG", year=2023)
pl.scan_datasus_cnes(uf="SP", group="LT", year=2023, month=12)
```

### Layer 2: Builder API (High-level)

User-friendly builder pattern that internally uses IO plugins.

```python
from arrow_sus import DataSUS

client = DataSUS(settings)

# Same API, but internally uses IO plugins
client.sih().uf("SP").group("RD").year(2023).month(1).collect()
```

### Integration Points

```python
# Builder API can return raw LazyFrames from IO plugins
lazy_frame = client.sih().uf("SP").group("RD").year(2023).month(1).lazy()

# These LazyFrames compose with any other Polars operations
result = lazy_frame.join(other_data, on="key").collect()
```

## Implementation Phases

### 🔥 Phase 1: Foundation & SIH Proof of Concept

**Timeline**: 2-3 weeks

#### 1.1 Core IO Plugin Infrastructure

- [ ] **Rust crate structure**

  ```text
  arrow_sus_polars/
  ├── src/
  │   ├── lib.rs              # Main PyO3 module
  │   ├── sources/            # DataSUS source implementations
  │   │   ├── mod.rs
  │   │   ├── sih.rs         # SIH IO plugin
  │   │   └── common.rs      # Shared utilities
  │   ├── dbf/               # DBF processing (reuse existing)
  │   ├── cache/             # Cache management
  │   └── error.rs           # Error handling
  ├── Cargo.toml
  └── pyproject.toml
  ```

- [ ] **Base traits and structures**

  ```rust
  pub trait DataSusSource: Send {
      fn name(&self) -> &str;
      fn schema(&self) -> Schema;
      fn next_batch(&mut self) -> PolarsResult<Option<DataFrame>>;
      fn set_predicate(&mut self, predicate: Expr);
      fn set_projection(&mut self, columns: Vec<String>);
  }
  ```

#### 1.2 SIH IO Plugin Implementation

- [ ] **SIH source struct**

  ```rust
  pub struct SihSource {
      uf: String,
      group: String,
      year: u16,
      month: u8,
      batch_size: usize,
      n_rows: Option<usize>,
      predicate: Option<Expr>,
      projection: Option<Vec<String>>,
      // Internal state
      current_file: Option<DbfReader>,
      files_queue: VecDeque<PathBuf>,
  }
  ```

- [ ] **File discovery and URL generation**

- [ ] **DBF streaming with Arrow conversion**

- [ ] **Predicate and projection pushdown**

#### 1.3 Python Integration

- [ ] **PyO3 bindings**

  ```python
  def scan_datasus_sih(
      uf: str,
      group: str, 
      year: int,
      month: int,
      cache_dir: Optional[str] = None,
      s3_mirror: Optional[str] = None,
  ) -> pl.LazyFrame:
  ```

- [ ] **Registration with Polars**

- [ ] **Error handling and validation**

#### 1.4 Builder API Integration

- [ ] **Update SIHQuery class**

  ```python
  class SIHQuery:
      def lazy(self) -> pl.LazyFrame:
          return pl.scan_datasus_sih(
              uf=self._uf,
              group=self._group,
              year=self._year,
              month=self._month,
              cache_dir=self._client.settings.cache_dir,
              s3_mirror=self._client.settings.datasus_s3_mirror,
          )

      def collect(self) -> pl.DataFrame:
          return self.lazy().collect()
  ```

### 🚀 Phase 2: Core DataSUS Systems

**Timeline**: 3-4 weeks

#### 2.1 SIA - Ambulatory Information System

- [ ] **SIA source implementation**
- [ ] **scan_datasus_sia() function**
- [ ] **Builder API integration**
- [ ] **Group handling (PA, PS, etc.)**

#### 2.2 SIM - Mortality Information System

- [ ] **SIM source implementation** (year-only pattern)
- [ ] **scan_datasus_sim() function**
- [ ] **ICD-10 code handling**
- [ ] **Builder API integration**

#### 2.3 CNES - Health Establishments Registry

- [ ] **CNES source implementation**
- [ ] **scan_datasus_cnes() function**
- [ ] **Multiple group types (LT, ST, DC, etc.)**
- [ ] **Builder API integration**

#### 2.4 SINASC - Live Birth Information System

- [ ] **SINASC source implementation**
- [ ] **scan_datasus_sinasc() function**
- [ ] **Builder API integration**

### 📊 Phase 3: Advanced Features & External Sources

**Timeline**: 2-3 weeks

#### 3.1 Advanced DataSUS Systems

- [ ] **PNI - National Immunization Program**
- [ ] **SINAM - Disease Notification System**
- [ ] **CIHA - Hospital Census**

#### 3.2 External API Sources

- [ ] **IBGE integration**

  ```python
  pl.scan_ibge_population(year=2023)
  pl.scan_ibge_gdp(year_range=(2020, 2023))
  ```

- [ ] **InfoDengue API**

  ```python
  pl.scan_infodengue(disease="dengue", geocode="3550308", year=2023)
  ```

#### 3.3 Multi-file Operations

- [ ] **Year range support**

  ```python
  pl.scan_datasus_sih(uf="SP", group="RD", year=(2020, 2023), month=1)
  ```

- [ ] **Month range support**

  ```python
  pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=(1, 12))
  ```

- [ ] **Multi-state support**

  ```python
  pl.scan_datasus_sih(uf=["SP", "RJ", "MG"], group="RD", year=2023, month=1)
  ```

### 🔐 Phase 4: Authenticated Sources & Performance

**Timeline**: 2-3 weeks

#### 4.1 Authenticated Sources

- [ ] **eSUS ElasticSearch integration**
- [ ] **Vaccine data API integration**
- [ ] **Credential management in IO plugins**

#### 4.2 Performance Optimizations

- [ ] **Parallel file processing**
- [ ] **Smart caching integration**
- [ ] **Memory usage optimization**
- [ ] **Progress reporting**

#### 4.3 Advanced Streaming

- [ ] **Chunk size optimization**
- [ ] **Streaming across multiple files**
- [ ] **Memory pressure handling**

## Technical Specifications

### IO Plugin Function Signatures

#### File-based Sources (UF + Group + Year + Month)

```python
def scan_datasus_sih(
    uf: str | list[str],
    group: str,
    year: int | tuple[int, int],
    month: int | tuple[int, int],
    cache_dir: Optional[str] = None,
    s3_mirror: Optional[str] = None,
    max_concurrent_downloads: int = 4,
) -> pl.LazyFrame:

def scan_datasus_sia(
    uf: str | list[str],
    group: str,  # PA, PS, etc.
    year: int | tuple[int, int],
    month: int | tuple[int, int],
    cache_dir: Optional[str] = None,
    s3_mirror: Optional[str] = None,
) -> pl.LazyFrame:

def scan_datasus_cnes(
    uf: str | list[str],
    group: str,  # LT, ST, DC, etc.
    year: int | tuple[int, int],
    month: int | tuple[int, int],
    cache_dir: Optional[str] = None,
    s3_mirror: Optional[str] = None,
) -> pl.LazyFrame:
```

#### Annual Sources (UF + Year)

```python
def scan_datasus_sim(
    uf: str | list[str],
    year: int | tuple[int, int],
    cache_dir: Optional[str] = None,
    s3_mirror: Optional[str] = None,
) -> pl.LazyFrame:

def scan_datasus_sinasc(
    uf: str | list[str],
    year: int | tuple[int, int], 
    cache_dir: Optional[str] = None,
    s3_mirror: Optional[str] = None,
) -> pl.LazyFrame:

def scan_datasus_pni(
    uf: str | list[str],
    year: int | tuple[int, int],
    cache_dir: Optional[str] = None,
    s3_mirror: Optional[str] = None,
) -> pl.LazyFrame:
```

#### Disease-based Sources

```python
def scan_datasus_sinam(
    disease: str | list[str],
    year: int | tuple[int, int],
    cache_dir: Optional[str] = None,
    s3_mirror: Optional[str] = None,
) -> pl.LazyFrame:
```

### Rust Implementation Structure

#### Core Traits

```rust
pub trait DataSusSource: Send {
    fn name(&self) -> &str;
    fn schema(&self) -> PolarsResult<Schema>;
    fn next_batch(&mut self) -> PolarsResult<Option<DataFrame>>;
    fn set_predicate(&mut self, predicate: Expr) -> PolarsResult<()>;
    fn set_projection(&mut self, columns: Vec<String>);
    fn estimate_rows(&self) -> Option<usize>;
}

pub trait FileSource: DataSusSource {
    fn file_urls(&self) -> Vec<String>;
    fn download_file(&self, url: &str) -> PolarsResult<PathBuf>;
    fn process_file(&mut self, path: &PathBuf) -> PolarsResult<Option<DataFrame>>;
}
```

#### SIH Implementation Example

```rust
#[pyclass]
pub struct SihSource {
    // Parameters
    uf: Vec<String>,
    group: String,
    year: RangeInclusive<u16>,
    month: RangeInclusive<u8>,
    
    // Configuration
    cache_dir: Option<PathBuf>,
    s3_mirror: Option<String>,
    batch_size: usize,
    
    // State
    n_rows: Option<usize>,
    predicate: Option<Expr>,
    projection: Option<Vec<String>>,
    
    // Internal processing state
    current_reader: Option<DbfReader>,
    files_queue: VecDeque<FileSpec>,
    rows_read: usize,
}

impl DataSusSource for SihSource {
    fn schema(&self) -> PolarsResult<Schema> {
        // Return SIH schema based on group
        match self.group.as_str() {
            "RD" => Ok(sih_rd_schema()),
            "ER" => Ok(sih_er_schema()),
            "RJ" => Ok(sih_rj_schema()),
            _ => Err(PolarsError::InvalidOperation(
                format!("Unknown SIH group: {}", self.group).into()
            ))
        }
    }
    
    fn next_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        // Check if we've read enough rows
        if let Some(limit) = self.n_rows {
            if self.rows_read >= limit {
                return Ok(None);
            }
        }
        
        // Process current file or get next file
        if self.current_reader.is_none() {
            if let Some(file_spec) = self.files_queue.pop_front() {
                let path = self.download_and_cache_file(&file_spec)?;
                self.current_reader = Some(DbfReader::new(path)?);
            } else {
                return Ok(None); // No more files
            }
        }
        
        // Read batch from current file
        if let Some(reader) = &mut self.current_reader {
            let mut batch = Vec::new();
            let target_size = std::cmp::min(
                self.batch_size,
                self.n_rows.map(|n| n - self.rows_read).unwrap_or(usize::MAX)
            );
            
            for _ in 0..target_size {
                match reader.read_record() {
                    Ok(Some(record)) => {
                        // Apply predicate pushdown if possible
                        if self.should_include_record(&record) {
                            batch.push(record);
                        }
                    },
                    Ok(None) => {
                        // End of file, close current reader
                        self.current_reader = None;
                        break;
                    },
                    Err(e) => return Err(e.into()),
                }
            }
            
            if !batch.is_empty() {
                self.rows_read += batch.len();
                let mut df = self.records_to_dataframe(batch)?;
                
                // Apply projection pushdown
                if let Some(ref columns) = self.projection {
                    df = df.select(columns)?;
                }
                
                // Apply predicate if not pushed down
                if let Some(ref predicate) = self.predicate {
                    if !self.predicate_pushed_down() {
                        df = df.lazy().filter(predicate.clone()).collect()?;
                    }
                }
                
                return Ok(Some(df));
            }
        }
        
        // Try next file if current is exhausted
        self.next_batch()
    }
}
```

### Builder API Integration

#### Updated Query Classes

```python
class SIHQuery:
    def __init__(self, client: DataSUS):
        self._client = client
        self._uf: Optional[str] = None
        self._group: Optional[str] = None
        self._year: Optional[int] = None
        self._month: Optional[int] = None
        self._filters: List[pl.Expr] = []
        self._selections: Optional[List[str]] = None

    def uf(self, uf: str) -> "SIHQuery":
        self._uf = uf
        return self

    def group(self, group: str) -> "SIHQuery":
        self._group = group
        return self

    def year(self, year: int) -> "SIHQuery":
        self._year = year
        return self

    def month(self, month: int) -> "SIHQuery":
        self._month = month
        return self

    def filter(self, expr: pl.Expr) -> "SIHQuery":
        self._filters.append(expr)
        return self

    def select(self, columns: List[str]) -> "SIHQuery":
        self._selections = columns
        return self

    def lazy(self) -> pl.LazyFrame:
        """Return LazyFrame using IO plugin"""
        if not all([self._uf, self._group, self._year, self._month]):
            raise ValueError("UF, group, year, and month must be specified")

        # Create IO plugin LazyFrame
        lazy_frame = pl.scan_datasus_sih(
            uf=self._uf,
            group=self._group,
            year=self._year,
            month=self._month,
            cache_dir=self._client.settings.cache_dir,
            s3_mirror=str(self._client.settings.datasus_s3_mirror),
        )

        # Apply any accumulated operations
        if self._selections:
            lazy_frame = lazy_frame.select(self._selections)

        for filter_expr in self._filters:
            lazy_frame = lazy_frame.filter(filter_expr)

        return lazy_frame

    def collect(self) -> pl.DataFrame:
        """Execute query and return DataFrame"""
        return self.lazy().collect()

    def stream(self, chunk_size: int = 10000) -> Iterator[pl.DataFrame]:
        """Stream results in chunks"""
        return self.lazy().collect_streaming(chunk_size=chunk_size)
```

## Code Examples

### Basic Usage Comparison

#### Using IO Plugins Directly

```python
import polars as pl

# Direct IO plugin usage - maximum performance
hospital_data = (
    pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
    .filter(pl.col("VAL_TOT") > 1000)
    .select(["MUNIC_RES", "VAL_TOT", "DT_INTER"])
    .collect()
)

# Multi-file operations
historical_data = (
    pl.scan_datasus_sih(uf="SP", group="RD", year=(2020, 2023), month=1)
    .group_by("ANO_CMPT")
    .agg([pl.sum("VAL_TOT").alias("total_cost"), pl.count().alias("procedures")])
    .collect()
)

# Ecosystem integration
mortality = pl.scan_datasus_sim(uf="SP", year=2023)
population = pl.scan_parquet("population.parquet")

analysis = (
    mortality.join(population, left_on="CODMUNRES", right_on="municipality_code")
    .with_columns(
        [(pl.col("deaths") / pl.col("population") * 100000).alias("mortality_rate")]
    )
    .collect()
)
```

#### Using Builder API

```python
from arrow_sus import DataSUS

client = DataSUS(settings)

# Builder API - user-friendly, same performance
hospital_data = (
    client.sih()
    .uf("SP")
    .group("RD")
    .year(2023)
    .month(1)
    .filter(pl.col("VAL_TOT") > 1000)
    .select(["MUNIC_RES", "VAL_TOT", "DT_INTER"])
    .collect()
)

# Can mix with IO plugins
lazy_hospital = client.sih().uf("SP").group("RD").year(2023).month(1).lazy()
lazy_population = pl.scan_parquet("population.parquet")

result = lazy_hospital.join(lazy_population, on="municipality").collect()
```

### Advanced Operations

#### Streaming Large Datasets

```python
# Process large multi-year dataset without memory issues
total_procedures = 0
total_cost = 0

for chunk in pl.scan_datasus_sih(
    uf=["SP", "RJ", "MG"], group="RD", year=(2020, 2023), month=(1, 12)
).collect_streaming(chunk_size="100MB"):
    # Process each chunk
    chunk_summary = chunk.group_by("UF_ZI").agg(
        [pl.sum("VAL_TOT").alias("cost"), pl.count().alias("procedures")]
    )

    total_procedures += chunk_summary.select(pl.sum("procedures")).item()
    total_cost += chunk_summary.select(pl.sum("cost")).item()

print(f"Total procedures: {total_procedures:,}")
print(f"Total cost: R$ {total_cost:,.2f}")
```

#### Complex Multi-source Analysis

```python
# Combine multiple DataSUS systems
hospital = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=(1, 12))
ambulatory = pl.scan_datasus_sia(uf="SP", group="PA", year=2023, month=(1, 12))
mortality = pl.scan_datasus_sim(uf="SP", year=2023)
establishments = pl.scan_datasus_cnes(uf="SP", group="ST", year=2023, month=12)

# Complex analysis with joins and aggregations
health_profile = (
    hospital.group_by("MUNIC_RES")
    .agg(
        [
            pl.sum("VAL_TOT").alias("hospital_cost"),
            pl.count().alias("hospital_procedures"),
        ]
    )
    .join(
        ambulatory.group_by("MUNIC_RES").agg(
            [
                pl.sum("VAL_TOT").alias("ambulatory_cost"),
                pl.count().alias("ambulatory_procedures"),
            ]
        ),
        on="MUNIC_RES",
        how="outer",
    )
    .join(
        mortality.group_by("CODMUNRES").agg([pl.count().alias("deaths")]),
        left_on="MUNIC_RES",
        right_on="CODMUNRES",
        how="left",
    )
    .join(
        establishments.group_by("CODUFMUN").agg([pl.count().alias("establishments")]),
        left_on="MUNIC_RES",
        right_on="CODUFMUN",
        how="left",
    )
    .with_columns(
        [
            (pl.col("hospital_cost") + pl.col("ambulatory_cost").fill_null(0)).alias(
                "total_cost"
            ),
            (
                pl.col("hospital_procedures")
                + pl.col("ambulatory_procedures").fill_null(0)
            ).alias("total_procedures"),
            (pl.col("deaths").fill_null(0) / pl.col("total_procedures") * 1000).alias(
                "mortality_per_1k_procedures"
            ),
        ]
    )
    .sort("total_cost", descending=True)
    .collect()
)
```

## Performance Optimizations

### 1. Predicate Pushdown Implementation

```rust
impl SihSource {
    fn try_pushdown_predicate(&mut self, predicate: &Expr) -> bool {
        // Analyze predicate to see if it can be pushed down during DBF processing
        match predicate {
            // Simple column comparisons can be pushed down
            Expr::BinaryExpr { left, op, right } => {
                if let (Expr::Column(col), BinaryOperator::Gt | BinaryOperator::Lt | BinaryOperator::Eq, Expr::Literal(_)) = (left.as_ref(), op, right.as_ref()) {
                    self.pushdown_filters.insert(col.clone(), predicate.clone());
                    true
                } else {
                    false
                }
            },
            // Complex predicates processed after DataFrame creation
            _ => false
        }
    }
    
    fn should_include_record(&self, record: &DbfRecord) -> bool {
        // Apply pushed-down filters during DBF processing
        for (column, filter) in &self.pushdown_filters {
            if !self.evaluate_filter_on_record(record, column, filter) {
                return false;
            }
        }
        true
    }
}
```

### 2. Smart Caching Integration

```rust
impl SihSource {
    fn download_and_cache_file(&self, file_spec: &FileSpec) -> PolarsResult<PathBuf> {
        let cache_key = format!("sih_{}_{}_{}_{}", 
            file_spec.uf, file_spec.group, file_spec.year, file_spec.month);
        
        if let Some(cached_path) = self.check_cache(&cache_key) {
            if self.is_cache_valid(&cached_path, &file_spec.url) {
                return Ok(cached_path);
            }
        }
        
        // Download and convert to Arrow/Parquet for faster subsequent access
        let downloaded_path = self.download_file(&file_spec.url)?;
        let arrow_path = self.convert_to_arrow(&downloaded_path, &cache_key)?;
        
        Ok(arrow_path)
    }
    
    fn convert_to_arrow(&self, dbf_path: &Path, cache_key: &str) -> PolarsResult<PathBuf> {
        // Convert DBF to Parquet for faster subsequent reads
        let df = self.read_dbf_to_dataframe(dbf_path)?;
        let parquet_path = self.cache_dir.join(format!("{}.parquet", cache_key));
        df.write_parquet(&parquet_path, WriteOptions::default())?;
        Ok(parquet_path)
    }
}
```

### 3. Parallel Processing

```rust
impl SihSource {
    fn download_files_parallel(&mut self) -> PolarsResult<()> {
        use rayon::prelude::*;
        
        let downloads: Vec<_> = self.files_queue
            .par_iter()
            .map(|file_spec| {
                self.download_and_cache_file(file_spec)
            })
            .collect();
        
        // Check for any download errors
        for result in downloads {
            result?;
        }
        
        Ok(())
    }
}
```

## Testing Strategy

### Unit Tests

#### Rust Layer Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sih_source_creation() {
        let source = SihSource::new("SP", "RD", 2023, 1, None, None);
        assert_eq!(source.name(), "sih");
        assert!(source.schema().is_ok());
    }
    
    #[test]
    fn test_predicate_pushdown() {
        let mut source = SihSource::new("SP", "RD", 2023, 1, None, None);
        let predicate = col("VAL_TOT").gt(lit(1000));
        
        assert!(source.try_pushdown_predicate(&predicate));
        assert_eq!(source.pushdown_filters.len(), 1);
    }
    
    #[test]
    fn test_file_url_generation() {
        let source = SihSource::new("SP", "RD", 2023, 1, None, None);
        let urls = source.file_urls();
        
        assert_eq!(urls.len(), 1);
        assert!(urls[0].contains("RDSP2301.dbc"));
    }
    
    #[test]
    fn test_multi_file_operations() {
        let source = SihSource::new_range(
            vec!["SP".to_string()], 
            "RD".to_string(), 
            2023..=2023, 
            1..=3,  // 3 months
            None, 
            None
        );
        
        let urls = source.file_urls();
        assert_eq!(urls.len(), 3); // 3 months
    }
}
```

#### Python Integration Tests

```python
import pytest
import polars as pl
from arrow_sus_polars import scan_datasus_sih


def test_scan_datasus_sih_basic():
    """Test basic SIH scanning functionality"""
    # This would use mock data or a test file
    df = pl.scan_datasus_sih(
        uf="SP", group="RD", year=2023, month=1, cache_dir="test_cache"
    ).collect()

    assert len(df) > 0
    assert "VAL_TOT" in df.columns
    assert "MUNIC_RES" in df.columns


def test_predicate_pushdown():
    """Test that predicates are properly pushed down"""
    lazy_frame = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).filter(
        pl.col("VAL_TOT") > 1000
    )

    # Verify the predicate is in the execution plan
    plan = lazy_frame.explain()
    assert "FILTER" in plan
    assert "VAL_TOT" in plan


def test_projection_pushdown():
    """Test that column selection is pushed down"""
    df = (
        pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
        .select(["MUNIC_RES", "VAL_TOT"])
        .collect()
    )

    assert len(df.columns) == 2
    assert set(df.columns) == {"MUNIC_RES", "VAL_TOT"}


def test_multi_file_operations():
    """Test operations across multiple files"""
    df = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=(1, 3)).collect()

    # Should have data from 3 months
    months = df.select(pl.col("MES_CMPT").unique()).to_series().to_list()
    assert len(months) == 3
    assert set(months) == {1, 2, 3}


def test_streaming_operations():
    """Test streaming functionality"""
    total_rows = 0

    for chunk in pl.scan_datasus_sih(
        uf="SP", group="RD", year=2023, month=1
    ).collect_streaming(chunk_size=1000):
        assert len(chunk) <= 1000
        total_rows += len(chunk)

    assert total_rows > 0


def test_builder_api_integration():
    """Test that builder API uses IO plugins internally"""
    from arrow_sus import DataSUS

    client = DataSUS(test_settings)

    # Builder API result
    builder_df = client.sih().uf("SP").group("RD").year(2023).month(1).collect()

    # Direct IO plugin result
    plugin_df = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).collect()

    # Should be identical
    assert builder_df.equals(plugin_df)


def test_error_handling():
    """Test proper error handling for invalid parameters"""
    with pytest.raises(ValueError, match="Invalid UF code"):
        pl.scan_datasus_sih(uf="XX", group="RD", year=2023, month=1).collect()

    with pytest.raises(ValueError, match="Invalid group"):
        pl.scan_datasus_sih(uf="SP", group="XX", year=2023, month=1).collect()

    with pytest.raises(ValueError, match="Data not available"):
        pl.scan_datasus_sih(uf="SP", group="RD", year=2030, month=1).collect()
```

### Integration Tests

#### End-to-End Data Pipeline Tests

```python
def test_complete_analysis_pipeline():
    """Test complete analysis using multiple IO plugins"""

    # Multi-source analysis
    hospital_data = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
    mortality_data = pl.scan_datasus_sim(uf="SP", year=2023)

    result = (
        hospital_data.group_by("MUNIC_RES")
        .agg([pl.sum("VAL_TOT").alias("hospital_cost"), pl.count().alias("procedures")])
        .join(
            mortality_data.group_by("CODMUNRES").agg([pl.count().alias("deaths")]),
            left_on="MUNIC_RES",
            right_on="CODMUNRES",
            how="left",
        )
        .with_columns(
            [
                (pl.col("deaths") / pl.col("procedures") * 1000).alias(
                    "mortality_per_1k_procedures"
                )
            ]
        )
        .collect()
    )

    assert len(result) > 0
    assert "mortality_per_1k_procedures" in result.columns


def test_cache_consistency():
    """Test that cached results are consistent"""

    # First query - should download and cache
    df1 = pl.scan_datasus_sih(
        uf="SP", group="RD", year=2023, month=1, cache_dir="test_cache"
    ).collect()

    # Second query - should use cache
    df2 = pl.scan_datasus_sih(
        uf="SP", group="RD", year=2023, month=1, cache_dir="test_cache"
    ).collect()

    assert df1.equals(df2)


def test_performance_vs_current_implementation():
    """Benchmark IO plugins vs current implementation"""
    import time

    # Current implementation (for comparison)
    start_time = time.time()
    current_df = client.sih().uf("SP").group("RD").year(2023).month(1).collect()
    current_time = time.time() - start_time

    # IO plugin implementation
    start_time = time.time()
    plugin_df = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).collect()
    plugin_time = time.time() - start_time

    # IO plugin should be faster or comparable
    assert plugin_time <= current_time * 1.2  # Allow 20% tolerance
    assert plugin_df.equals(current_df)
```

### Performance Tests

#### Memory Usage Tests

```python
import psutil
import os


def test_memory_usage_streaming():
    """Test that streaming doesn't exceed memory limits"""
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss

    max_memory = initial_memory

    for chunk in pl.scan_datasus_sih(
        uf="SP", group="RD", year=(2020, 2023), month=(1, 12)
    ).collect_streaming(chunk_size="50MB"):
        current_memory = process.memory_info().rss
        max_memory = max(max_memory, current_memory)

        # Process chunk
        _ = chunk.filter(pl.col("VAL_TOT") > 0).select(["MUNIC_RES", "VAL_TOT"])

    # Memory usage shouldn't grow beyond reasonable bounds
    memory_growth = max_memory - initial_memory
    assert memory_growth < 200_000_000  # Less than 200MB growth


def test_concurrent_access():
    """Test multiple concurrent IO plugin operations"""
    import concurrent.futures

    def query_data(uf):
        return pl.scan_datasus_sih(uf=uf, group="RD", year=2023, month=1).collect()

    states = ["SP", "RJ", "MG", "RS"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(query_data, uf) for uf in states]
        results = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]

    assert len(results) == 4
    assert all(len(df) > 0 for df in results)
```

#### Benchmark Suite

```python
def benchmark_io_plugins():
    """Comprehensive benchmark suite"""
    import time
    import json

    benchmarks = {}

    # Test 1: Single file processing
    start = time.time()
    df1 = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).collect()
    benchmarks["single_file"] = {
        "time": time.time() - start,
        "rows": len(df1),
        "memory_mb": df1.estimated_size("mb"),
    }

    # Test 2: Multi-file processing
    start = time.time()
    df2 = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=(1, 6)).collect()
    benchmarks["multi_file"] = {
        "time": time.time() - start,
        "rows": len(df2),
        "memory_mb": df2.estimated_size("mb"),
    }

    # Test 3: Streaming performance
    start = time.time()
    chunk_count = 0
    total_rows = 0

    for chunk in pl.scan_datasus_sih(
        uf="SP", group="RD", year=2023, month=(1, 12)
    ).collect_streaming(chunk_size="100MB"):
        chunk_count += 1
        total_rows += len(chunk)

    benchmarks["streaming"] = {
        "time": time.time() - start,
        "chunks": chunk_count,
        "total_rows": total_rows,
    }

    # Test 4: Predicate pushdown efficiency
    start = time.time()
    df4 = (
        pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
        .filter(pl.col("VAL_TOT") > 1000)
        .collect()
    )
    benchmarks["predicate_pushdown"] = {
        "time": time.time() - start,
        "rows": len(df4),
        "selectivity": len(df4) / len(df1),
    }

    # Save benchmark results
    with open("benchmark_results.json", "w") as f:
        json.dump(benchmarks, f, indent=2)

    return benchmarks
```

## Documentation Plan

### 1. User Documentation

#### Quick Start Guide

````markdown
# Arrow SUS with Polars IO Plugins - Quick Start

## Installation

```bash
pip install arrow-sus[polars]
````

## Basic Usage

### Direct IO Plugin Usage (Recommended for Performance)

```python
import polars as pl

# Hospital data for São Paulo, January 2023
hospital_data = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)

# Apply filters and transformations (with pushdown optimizations)
result = (
    hospital_data.filter(pl.col("VAL_TOT") > 1000)  # Pushed down to source
    .select(["MUNIC_RES", "VAL_TOT", "DT_INTER"])  # Only these columns extracted
    .group_by("MUNIC_RES")
    .agg([pl.sum("VAL_TOT").alias("total_cost"), pl.count().alias("procedures")])
    .sort("total_cost", descending=True)
    .collect()  # Execute the query
)
```

### Builder API (User-Friendly Alternative)

```python
from arrow_sus import DataSUS

client = DataSUS(settings)

# Same result, friendlier API
result = (
    client.sih()
    .uf("SP")
    .group("RD")
    .year(2023)
    .month(1)
    .filter(pl.col("VAL_TOT") > 1000)
    .select(["MUNIC_RES", "VAL_TOT", "DT_INTER"])
    .group_by("MUNIC_RES")
    .agg([pl.sum("VAL_TOT").alias("total_cost"), pl.count().alias("procedures")])
    .sort("total_cost", descending=True)
    .collect()
)
```

## Multi-File Operations

```python
# Process multiple months
yearly_data = pl.scan_datasus_sih(
    uf="SP",
    group="RD",
    year=2023,
    month=(1, 12),  # All 12 months
).collect()

# Process multiple states
multi_state = pl.scan_datasus_sih(
    uf=["SP", "RJ", "MG"], group="RD", year=2023, month=1
).collect()

# Process multiple years
historical = pl.scan_datasus_sih(
    uf="SP", group="RD", year=(2020, 2023), month=1
).collect()
```

## Streaming Large Datasets

```python
# Process data in chunks to avoid memory issues
total_cost = 0
procedure_count = 0

for chunk in pl.scan_datasus_sih(
    uf=["SP", "RJ", "MG"], group="RD", year=(2020, 2023), month=(1, 12)
).collect_streaming(chunk_size="100MB"):
    chunk_summary = chunk.select([pl.sum("VAL_TOT"), pl.count()])

    total_cost += chunk_summary.item(0, 0)
    procedure_count += chunk_summary.item(0, 1)

print(f"Total: R$ {total_cost:,.2f} across {procedure_count:,} procedures")
```

## API Reference

### IO Plugin API Reference

#### Hospital Information System (SIH)

##### `pl.scan_datasus_sih()`

Scan SIH (Sistema de Informações Hospitalares) data files.

**Parameters:**

- `uf` (str | list[str]): State code(s) - "SP", "RJ", etc.
- `group` (str): Data group - "RD" (Reduced), "ER" (Enhanced), "RJ" (Rejected)
- `year` (int | tuple[int, int]): Year or year range
- `month` (int | tuple[int, int]): Month (1-12) or month range
- `cache_dir` (str, optional): Cache directory path
- `s3_mirror` (str, optional): S3 mirror URL for faster downloads

**Returns:** `pl.LazyFrame`

**Examples:**

```python
# Single month
df = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).collect()

# Year range, single month
df = pl.scan_datasus_sih(uf="SP", group="RD", year=(2020, 2023), month=1).collect()

# Single year, month range
df = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=(1, 12)).collect()

# Multiple states
df = pl.scan_datasus_sih(uf=["SP", "RJ"], group="RD", year=2023, month=1).collect()
```

**Available Groups:**

- `"RD"`: Reduced dataset (most common)
- `"ER"`: Enhanced/complete dataset
- `"RJ"`: Rejected records

**Schema:** See [SIH Schema Documentation](sih_schema.md)

### 2. Developer Documentation

#### Architecture Overview

## Arrow SUS Polars IO Plugins - Architecture

### Overview

The Arrow SUS Polars IO plugins provide a two-layer architecture:

1. **IO Plugin Layer (Rust)**: High-performance data source implementations
1. **Builder API Layer (Python)**: User-friendly interface that delegates to IO plugins

## Component Architecture

```text

┌─────────────────────┐    ┌─────────────────────┐
│   Builder API       │    │   Direct IO Plugin  │
│   (Python)          │    │   Usage (Python)    │
├─────────────────────┤    ├─────────────────────┤
│ client.sih()        │    │ pl.scan_datasus_sih │
│   .uf("SP")         ├───►│   (uf="SP", ...)    │
│   .collect()        │    │   .collect()        │
└─────────────────────┘    └─────────────────────┘
           │                          │
           └──────────┬─────────────────┘
                      ▼
        ┌─────────────────────────────┐
        │     IO Plugin Registry      │
        │        (Python)             │
        ├─────────────────────────────┤
        │ register_io_source()        │
        │ source_generator()          │
        └─────────────────────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │   DataSUS Sources (Rust)    │
        ├─────────────────────────────┤
        │ SihSource                   │
        │ SiaSource                   │
        │ SimSource                   │
        │ CnesSource                  │
        └─────────────────────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │   Core Services (Rust)      │
        ├─────────────────────────────┤
        │ DBF Reader                  │
        │ Cache Manager               │
        │ Download Manager            │
        │ Arrow Conversion            │
        └─────────────────────────────┘

```

## Data Flow

1. **Query Construction**: User builds query using Builder API or direct IO plugin
1. **Parameter Validation**: Rust layer validates UF codes, years, groups, etc.
1. **File Discovery**: Generate URLs for required DataSUS files
1. **Cache Check**: Check if files already cached locally
1. **Download**: Download missing files (with parallel downloads)
1. **DBF Processing**: Stream DBF files with pushdown optimizations
1. **Arrow Conversion**: Convert to Arrow format for Polars
1. **Query Execution**: Apply remaining Polars operations

## Key Design Decisions

### 1. Rust Implementation for Performance

- DBF parsing in Rust for maximum speed
- Zero-copy Arrow conversion
- Parallel file processing
- Memory-efficient streaming

### 2. Polars Integration Patterns

- Use `register_io_source()` for proper lazy evaluation
- Support predicate and projection pushdown
- Implement proper batch sizing
- Handle streaming and chunked operations

### 3. Cache Strategy

- Convert DBF to Parquet for faster subsequent reads
- Cache at the file level (not query level)
- Implement cache invalidation based on upstream changes
- Support distributed cache backends

## Error Handling Strategy

```rust
// Rust error types
#[derive(Debug, thiserror::Error)]
pub enum DataSusError {
    #[error("Invalid UF code: {0}")]
    InvalidUf(String),
    
    #[error("Data not available for {uf} {group} {year}-{month:02}")]
    DataNotAvailable { uf: String, group: String, year: u16, month: u8 },
    
    #[error("Download failed: {0}")]
    DownloadError(String),
    
    #[error("DBF parsing error: {0}")]
    DbfError(String),
    
    #[error("Cache error: {0}")]
    CacheError(String),
}
```

```python
# Python error mapping
class ArrowSusIOError(Exception):
    """Base exception for IO plugin errors"""


class DataNotFoundError(ArrowSusIOError):
    """Data not available for specified parameters"""

    def __init__(self, message, available_years=None, available_states=None):
        super().__init__(message)
        self.available_years = available_years
        self.available_states = available_states
```

#### Contributing Guidelines

## Contributing to Arrow SUS IO Plugins

## Development Setup

### Prerequisites

- Rust 1.70+
- Python 3.8+
- Polars development environment

### Environment Setup

```bash
# Clone repository
git clone https://github.com/your-org/arrow-sus
cd arrow-sus

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install development dependencies
pip install -e ".[dev,polars]"

# Install Rust dependencies
cd arrow_sus_polars
cargo build
```

## Project Structure

```text
arrow_sus/
├── arrow_sus/                 # Main Python package
│   ├── __init__.py
│   ├── client.py             # DataSUS client
│   └── settings.py           # Configuration
├── arrow_sus_polars/         # Rust IO plugins
│   ├── src/
│   │   ├── lib.rs           # PyO3 bindings
│   │   ├── sources/         # DataSUS source implementations
│   │   ├── dbf/            # DBF processing
│   │   └── cache/          # Cache management
│   ├── Cargo.toml
│   └── pyproject.toml
├── tests/
│   ├── unit/               # Unit tests
│   ├── integration/        # Integration tests
│   └── benchmarks/         # Performance tests
└── docs/                   # Documentation
```

## Adding New Data Sources

### 1. Implement Rust Source

```rust
// arrow_sus_polars/src/sources/new_source.rs

use crate::sources::DataSusSource;
use polars::prelude::*;

pub struct NewSource {
    // Parameters specific to this source
    uf: String,
    year: u16,
    // ... other parameters
    
    // Internal state
    batch_size: usize,
    n_rows: Option<usize>,
    predicate: Option<Expr>,
    projection: Option<Vec<String>>,
}

impl DataSusSource for NewSource {
    fn name(&self) -> &str {
        "new_source"
    }
    
    fn schema(&self) -> PolarsResult<Schema> {
        // Define schema for this data source
        Ok(Schema::from_iter([
            ("FIELD1", DataType::Utf8),
            ("FIELD2", DataType::Int64),
            // ... more fields
        ]))
    }
    
    fn next_batch(&mut self) -> PolarsResult<Option<DataFrame>> {
        // Implement data reading logic
        todo!()
    }
}
```

### 2. Add PyO3 Bindings

```rust
// arrow_sus_polars/src/lib.rs

#[pyfunction]
fn scan_datasus_new_source(
    uf: String,
    year: u16,
    cache_dir: Option<String>,
    s3_mirror: Option<String>,
) -> PyResult<PyLazyFrame> {
    let source = NewSource::new(uf, year, cache_dir, s3_mirror);
    
    let generator = move |with_columns, predicate, n_rows, batch_size| {
        // Create source instance and configure
        let mut src = source.clone();
        if let Some(cols) = with_columns {
            src.set_projection(cols);
        }
        if let Some(pred) = predicate {
            src.set_predicate(pred);
        }
        
        // Return iterator
        std::iter::from_fn(move || src.next_batch().transpose())
    };
    
    Ok(PyLazyFrame(
        LazyFrame::scan_sources(
            vec![source.schema()?],
            generator,
            None,
        )?
    ))
}

#[pymodule]
fn arrow_sus_polars(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_datasus_new_source, m)?)?;
    // ... other functions
    Ok(())
}
```

### 3. Add Builder API Integration

```python
# arrow_sus/new_source.py


class NewSourceQuery:
    def __init__(self, client: DataSUS):
        self._client = client
        self._uf: Optional[str] = None
        self._year: Optional[int] = None

    def uf(self, uf: str) -> "NewSourceQuery":
        self._uf = uf
        return self

    def year(self, year: int) -> "NewSourceQuery":
        self._year = year
        return self

    def lazy(self) -> pl.LazyFrame:
        if not all([self._uf, self._year]):
            raise ValueError("UF and year must be specified")

        return pl.scan_datasus_new_source(
            uf=self._uf,
            year=self._year,
            cache_dir=self._client.settings.cache_dir,
            s3_mirror=str(self._client.settings.datasus_s3_mirror),
        )

    def collect(self) -> pl.DataFrame:
        return self.lazy().collect()
```

### 4. Add Tests

```python
# tests/unit/test_new_source.py


def test_scan_datasus_new_source():
    df = pl.scan_datasus_new_source(
        uf="SP", year=2023, cache_dir="test_cache"
    ).collect()

    assert len(df) > 0
    assert "FIELD1" in df.columns


def test_new_source_builder_api():
    client = DataSUS(test_settings)

    df = client.new_source().uf("SP").year(2023).collect()

    assert len(df) > 0
```

## Testing Guidelines

### Running Tests

```bash
# Unit tests
pytest tests/unit/

# Integration tests (requires network)
pytest tests/integration/

# Benchmarks
pytest tests/benchmarks/

# Rust tests
cd arrow_sus_polars
cargo test
```

### Test Data

- Use mock data for unit tests
- Use cached real data for integration tests
- Document data dependencies clearly
- Provide test data fixtures

### Performance Testing

```python
def test_performance_benchmark():
    """Template for performance tests"""
    import time

    start_time = time.time()

    # Your operation
    df = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).collect()

    execution_time = time.time() - start_time

    # Assert reasonable performance
    assert execution_time < 30.0  # Should complete within 30 seconds
    assert len(df) > 1000  # Should return meaningful data
```

## Code Style

### Rust Code Style

- Follow standard Rust formatting (`cargo fmt`)
- Use `cargo clippy` for linting
- Document public APIs with rustdoc
- Use meaningful error messages

### Python Code Style

- Follow PEP 8
- Use type hints throughout
- Document functions with docstrings
- Use meaningful variable names

## Pull Request Process

1. **Create Feature Branch**: `git checkout -b feature/new-data-source`
1. **Implement Changes**: Add Rust source, Python bindings, tests
1. **Run Tests**: Ensure all tests pass
1. **Update Documentation**: Add API docs and examples
1. **Submit PR**: Include description of changes and test results
1. **Code Review**: Address reviewer feedback
1. **Merge**: Squash and merge when approved

## Release Process

1. **Version Bump**: Update version in `Cargo.toml` and `pyproject.toml`
1. **Changelog**: Update CHANGELOG.md with new features/fixes
1. **Build**: Test build process locally
1. **Tag**: Create git tag with version
1. **Release**: Automated CI/CD builds and publishes packages

### 3. Technical Documentation

#### Performance Optimization Guide

## Performance Optimization Guide

## Understanding IO Plugin Performance

Arrow SUS IO plugins provide several optimization opportunities:

### 1. Predicate Pushdown

Move filtering operations to the data source level:

```python
# ❌ Inefficient - filters after loading entire dataset
df = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).collect()
filtered = df.filter(pl.col("VAL_TOT") > 1000)

# ✅ Efficient - predicate pushed down to source
df = (
    pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
    .filter(pl.col("VAL_TOT") > 1000)  # Applied during DBF processing
    .collect()
)
```

### 2. Projection Pushdown

Only extract needed columns:

```python
# ❌ Inefficient - extracts all columns then selects
df = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).collect()
result = df.select(["MUNIC_RES", "VAL_TOT"])

# ✅ Efficient - only extracts needed columns from DBF
df = (
    pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
    .select(["MUNIC_RES", "VAL_TOT"])  # Applied during DBF processing
    .collect()
)
```

### 3. Streaming for Large Datasets

Process data in chunks to manage memory:

```python
# ✅ Memory-efficient streaming
total_cost = 0
for chunk in pl.scan_datasus_sih(
    uf="SP", group="RD", year=(2020, 2023), month=(1, 12)
).collect_streaming(chunk_size="100MB"):
    chunk_cost = chunk.select(pl.sum("VAL_TOT")).item()
    total_cost += chunk_cost
```

## Caching Strategies

### 1. File-Level Caching

```python
# Configure cache location
client = DataSUS(
    settings=datasus_settings,
    cache_dir="/fast/ssd/cache",  # Use SSD for better performance
)

# Files automatically cached after first download
df1 = pl.scan_datasus_sih(
    uf="SP", group="RD", year=2023, month=1
).collect()  # Downloads
df2 = pl.scan_datasus_sih(
    uf="SP", group="RD", year=2023, month=1
).collect()  # From cache
```

### 2. Cache Warming

```python
# Pre-download and cache frequently used data
cache_states = ["SP", "RJ", "MG", "RS", "PR"]
cache_years = range(2020, 2024)

for uf in cache_states:
    for year in cache_years:
        # This will download and cache the files
        pl.scan_datasus_sih(uf=uf, group="RD", year=year, month=1).schema()
```

## Parallel Processing

### 1. Multi-State Queries

```python
# ✅ Parallel downloads for multiple states
df = pl.scan_datasus_sih(
    uf=["SP", "RJ", "MG", "RS", "PR"],  # Downloads happen in parallel
    group="RD",
    year=2023,
    month=1,
).collect()
```

### 2. Concurrent Operations

```python
import concurrent.futures


def analyze_state(uf: str) -> pl.DataFrame:
    return (
        pl.scan_datasus_sih(uf=uf, group="RD", year=2023, month=1)
        .group_by("MUNIC_RES")
        .agg([pl.sum("VAL_TOT").alias("total_cost")])
        .collect()
    )


# Process multiple states concurrently
states = ["SP", "RJ", "MG", "RS", "PR", "SC", "GO", "DF"]

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(analyze_state, states))

# Combine results
combined = pl.concat(results)
```

## Memory Management

### 1. Optimal Chunk Sizes

```python
# Memory usage vs performance tradeoff
import psutil


def get_optimal_chunk_size():
    """Calculate optimal chunk size based on available memory"""
    available_memory = psutil.virtual_memory().available
    # Use 10% of available memory per chunk
    chunk_size_bytes = available_memory * 0.1
    return f"{int(chunk_size_bytes / 1024 / 1024)}MB"


# Use dynamic chunk sizing
chunk_size = get_optimal_chunk_size()
for chunk in pl.scan_datasus_sih(
    uf="SP", group="RD", year=(2020, 2023), month=(1, 12)
).collect_streaming(chunk_size=chunk_size):
    # Process chunk
    pass
```

### 2. Memory Monitoring

```python
def monitor_memory_usage(func):
    """Decorator to monitor memory usage"""
    import functools
    import psutil
    import os

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())

        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        result = func(*args, **kwargs)
        final_memory = process.memory_info().rss / 1024 / 1024  # MB

        print(
            f"Memory usage: {initial_memory:.1f}MB -> {final_memory:.1f}MB "
            f"(+{final_memory - initial_memory:.1f}MB)"
        )

        return result

    return wrapper


@monitor_memory_usage
def large_query():
    return pl.scan_datasus_sih(
        uf=["SP", "RJ", "MG"], group="RD", year=(2020, 2023), month=(1, 12)
    ).collect()
```

## Query Optimization Patterns

### 1. Early Filtering

```python
# ✅ Apply filters as early as possible
result = (
    pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
    .filter(pl.col("VAL_TOT") > 1000)  # Applied during DBF read
    .filter(pl.col("MUNIC_RES").str.starts_with("35"))  # Also pushed down
    .select(["MUNIC_RES", "VAL_TOT", "DT_INTER"])  # Only extract needed columns
    .group_by("MUNIC_RES")
    .agg([pl.sum("VAL_TOT").alias("total")])
    .collect()
)
```

### 2. Efficient Joins

```python
# ✅ Join on filtered datasets
hospital_summary = (
    pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
    .filter(pl.col("VAL_TOT") > 0)
    .group_by("MUNIC_RES")
    .agg([pl.sum("VAL_TOT").alias("hospital_cost")])
)

ambulatory_summary = (
    pl.scan_datasus_sia(uf="SP", group="PA", year=2023, month=1)
    .filter(pl.col("VAL_TOT") > 0)
    .group_by("MUNIC_RES")
    .agg([pl.sum("VAL_TOT").alias("ambulatory_cost")])
)

# Join the pre-aggregated results
combined = hospital_summary.join(ambulatory_summary, on="MUNIC_RES", how="outer")
```

## Troubleshooting Performance Issues

### 1. Query Profiling

```python
# Enable query profiling
import os

os.environ["POLARS_VERBOSE"] = "1"

# Run query with timing information
df = (
    pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
    .filter(pl.col("VAL_TOT") > 1000)
    .collect()
)

# Check execution plan
lazy_query = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).filter(
    pl.col("VAL_TOT") > 1000
)

print(lazy_query.explain())
```

### 2. Common Performance Issues

#### Issue: Slow Downloads

```python
# ❌ Single-threaded downloads
for uf in ["SP", "RJ", "MG"]:
    df = pl.scan_datasus_sih(uf=uf, group="RD", year=2023, month=1).collect()

# ✅ Parallel downloads
df = pl.scan_datasus_sih(
    uf=["SP", "RJ", "MG"],  # Downloaded in parallel
    group="RD",
    year=2023,
    month=1,
).collect()
```

#### Issue: Memory Exhaustion

```python
# ❌ Loading entire multi-year dataset
df = pl.scan_datasus_sih(
    uf="SP", group="RD", year=(2015, 2023), month=(1, 12)
).collect()  # May exceed memory

# ✅ Process in streaming fashion
results = []
for chunk in pl.scan_datasus_sih(
    uf="SP", group="RD", year=(2015, 2023), month=(1, 12)
).collect_streaming(chunk_size="200MB"):
    # Process chunk and extract summary
    summary = chunk.group_by("ANO_CMPT").agg([pl.sum("VAL_TOT").alias("annual_cost")])
    results.append(summary)

# Combine summaries
final_result = pl.concat(results).group_by("ANO_CMPT").agg([pl.sum("annual_cost")])
```

#### Issue: Inefficient Predicates

```python
# ❌ Complex predicates that can't be pushed down
df = (
    pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
    .filter(
        pl.col("VAL_TOT").map_elements(
            lambda x: some_complex_function(x)  # Can't push down
        )
    )
    .collect()
)

# ✅ Simple predicates that can be pushed down
df = (
    pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
    .filter(pl.col("VAL_TOT") > 1000)  # Can push down
    .filter(pl.col("VAL_TOT") < 100000)  # Can push down
    .with_columns(
        [
            pl.col("VAL_TOT")
            .map_elements(some_complex_function)
            .alias("processed_value")  # Applied after pushdown
        ]
    )
    .collect()
)
```

## Benchmarking Your Queries

### 1. Benchmark Framework

```python
import time
import functools


def benchmark_query(name: str):
    """Decorator to benchmark query performance"""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            print(f"\n=== Benchmarking: {name} ===")

            # Warm up (first run may include download time)
            start_time = time.time()
            result = func(*args, **kwargs)
            warmup_time = time.time() - start_time

            # Actual benchmark (should use cache)
            start_time = time.time()
            result = func(*args, **kwargs)
            benchmark_time = time.time() - start_time

            print(f"Warmup time: {warmup_time:.2f}s")
            print(f"Benchmark time: {benchmark_time:.2f}s")
            print(f"Rows returned: {len(result):,}")
            print(f"Throughput: {len(result) / benchmark_time:,.0f} rows/sec")

            return result

        return wrapper

    return decorator


@benchmark_query("SIH Single Month Query")
def benchmark_sih_single():
    return pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).collect()


@benchmark_query("SIH Filtered Query")
def benchmark_sih_filtered():
    return (
        pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1)
        .filter(pl.col("VAL_TOT") > 1000)
        .collect()
    )


# Run benchmarks
benchmark_sih_single()
benchmark_sih_filtered()
```

### 2. Performance Comparison

```python
def compare_implementations():
    """Compare IO plugin vs original implementation"""

    # Original implementation
    start = time.time()
    original_df = client.sih().uf("SP").group("RD").year(2023).month(1).collect()
    original_time = time.time() - start

    # IO plugin implementation
    start = time.time()
    plugin_df = pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).collect()
    plugin_time = time.time() - start

    print(f"Original implementation: {original_time:.2f}s")
    print(f"IO plugin implementation: {plugin_time:.2f}s")
    print(f"Speedup: {original_time / plugin_time:.1f}x")

    # Verify results are identical
    assert original_df.equals(plugin_df)


compare_implementations()
```

## Implementation Checklist

### Phase 1: Foundation & SIH (Weeks 1-3)

#### Week 1: Core Infrastructure

- [ ] **Rust Project Setup**

  - [ ] Create `arrow_sus_polars` crate with PyO3 bindings
  - [ ] Set up CI/CD for Rust compilation
  - [ ] Configure cross-platform builds (Linux, macOS, Windows)
  - [ ] Set up Python wheel generation

- [ ] **Core Traits and Structures**

  - [ ] Define `DataSusSource` trait
  - [ ] Implement error handling system
  - [ ] Create configuration structures
  - [ ] Set up logging and debugging

- [ ] **DBF Processing Integration**

  - [ ] Integrate existing DBF reading code
  - [ ] Implement Arrow conversion
  - [ ] Add streaming support
  - [ ] Optimize memory usage

#### Week 2: SIH Implementation

- [ ] **SIH Source Implementation**

  - [ ] `SihSource` struct with all parameters
  - [ ] File URL generation logic
  - [ ] Schema definition for RD, ER, RJ groups
  - [ ] Batch processing implementation

- [ ] **Optimization Features**

  - [ ] Predicate pushdown for simple filters
  - [ ] Projection pushdown for column selection
  - [ ] Multi-file handling (states, months, years)
  - [ ] Parallel download management

- [ ] **Python Bindings**

  - [ ] `scan_datasus_sih()` function
  - [ ] Parameter validation
  - [ ] Error conversion to Python exceptions
  - [ ] Documentation strings

#### Week 3: Integration & Testing

- [ ] **Builder API Integration**

  - [ ] Update `SIHQuery` class to use IO plugin
  - [ ] Maintain backward compatibility
  - [ ] Add `.lazy()` method for direct LazyFrame access
  - [ ] Performance comparison tests

- [ ] **Testing Suite**

  - [ ] Unit tests for Rust components
  - [ ] Integration tests for Python bindings
  - [ ] Performance benchmarks
  - [ ] Memory usage tests

- [ ] **Documentation**

  - [ ] API documentation
  - [ ] Performance optimization guide
  - [ ] Migration guide from current implementation

### Phase 2: Core DataSUS Systems (Weeks 4-7)

#### Week 4: SIA Implementation

- [ ] `SiaSource` implementation with group handling (PA, PS, etc.)
- [ ] `scan_datasus_sia()` function
- [ ] Builder API integration
- [ ] Tests and documentation

#### Week 5: SIM Implementation

- [ ] `SimSource` implementation (annual files)
- [ ] `scan_datasus_sim()` function
- [ ] ICD-10 code handling
- [ ] Builder API integration
- [ ] Tests and documentation

#### Week 6: CNES Implementation

- [ ] `CnesSource` implementation with multiple groups (LT, ST, DC, etc.)
- [ ] `scan_datasus_cnes()` function
- [ ] Builder API integration
- [ ] Tests and documentation

#### Week 7: SINASC Implementation

- [ ] `SinascSource` implementation
- [ ] `scan_datasus_sinasc()` function
- [ ] Builder API integration
- [ ] Tests and documentation

### Phase 3: Advanced Features (Weeks 8-10)

#### Week 8: Remaining DataSUS Systems

- [ ] PNI implementation
- [ ] SINAM implementation
- [ ] CIHA implementation
- [ ] Comprehensive testing

#### Week 9: Multi-file Operations

- [ ] Year range support across all systems
- [ ] Month range support where applicable
- [ ] Multi-state parallel processing
- [ ] Memory optimization for large operations

#### Week 10: Performance & Polish

- [ ] Performance benchmarking and optimization
- [ ] Memory usage optimization
- [ ] Error handling improvements
- [ ] Documentation completion

### Phase 4: External Sources & Auth (Weeks 11-13)

#### Week 11: External APIs

- [ ] IBGE integration (`scan_ibge_*` functions)
- [ ] InfoDengue API integration
- [ ] InfoGripe CSV integration
- [ ] Builder API integration for external sources

#### Week 12: Authenticated Sources

- [ ] eSUS ElasticSearch integration
- [ ] Vaccine data API integration
- [ ] Credential management
- [ ] Security best practices

#### Week 13: Final Integration

- [ ] Complete testing suite
- [ ] Performance validation
- [ ] Documentation finalization
- [ ] Release preparation

## Success Metrics

### Performance Targets

- [ ] **Query Performance**: 2-5x faster than current implementation
- [ ] **Memory Usage**: 50% reduction in peak memory usage for large datasets
- [ ] **Download Speed**: 3x faster with parallel downloads and CDN usage
- [ ] **Cache Hit Rate**: 95%+ for repeated queries
- [ ] **Streaming Throughput**: Process 1M+ records/second

### Quality Targets

- [ ] **Test Coverage**: 90%+ code coverage
- [ ] **Error Handling**: Comprehensive error messages with actionable suggestions
- [ ] **Documentation**: Complete API docs and user guides
- [ ] **Compatibility**: Support Python 3.8+ and major platforms
- [ ] **Stability**: Zero data corruption, consistent results

### Usability Targets

- [ ] **API Consistency**: Unified interface across all data sources
- [ ] **Migration Path**: Seamless upgrade from current implementation
- [ ] **Learning Curve**: New users productive within 30 minutes
- [ ] **Debugging**: Clear error messages and query explanation
- [ ] **Ecosystem Integration**: Works with existing Polars workflows

## Risk Mitigation

### Technical Risks

#### Risk: Performance Regression

**Mitigation**:

- Continuous benchmarking in CI/CD
- Performance budgets for each operation
- Fallback to current implementation if needed

#### Risk: Memory Issues with Large Datasets

**Mitigation**:

- Streaming-first design
- Memory usage monitoring in tests
- Configurable chunk sizes
- Automatic memory management

#### Risk: Compatibility Issues

**Mitigation**:

- Extensive cross-platform testing
- Version compatibility matrix
- Graceful degradation strategies

### Operational Risks

#### Risk: DataSUS Infrastructure Changes

**Mitigation**:

- Monitor upstream changes
- Flexible URL generation
- Fallback mechanisms
- User notification system

#### Risk: Network Reliability

**Mitigation**:

- Retry mechanisms with exponential backoff
- Mirror servers (S3 CDN)
- Offline mode with cached data
- Connection pooling

#### Risk: Data Corruption

**Mitigation**:

- Checksum verification
- Data validation after download
- Rollback capabilities
- Automated integrity checks

## Future Roadmap

### Phase 5: Advanced Analytics (Future)

#### Machine Learning Integration

```python
# Future: Direct ML model training
model = pl.scan_datasus_sih(uf="SP", group="RD", year=(2020, 2023))
    .pipe(ml.feature_engineering)
    .pipe(ml.train_model, target="VAL_TOT")
```

#### Real-time Streaming

```python
# Future: Real-time data processing
stream = pl.scan_datasus_stream("sih")
    .window(duration="1h")
    .agg([pl.sum("VAL_TOT").alias("hourly_cost")])
    .sink_parquet("real_time_analytics/")
```

#### Distributed Computing

```python
# Future: Spark/Ray integration
df = pl.scan_datasus_sih(uf="all", group="RD", year=(2015, 2023))
    .collect_distributed(engine="spark")
```

### Phase 6: Ecosystem Expansion

#### Integration with Other Health Data

- International health databases
- Research datasets
- Clinical trial data
- Genomic data integration

#### Advanced Visualization

```python
# Future: Integrated plotting
(
    pl.scan_datasus_sih(uf="SP", group="RD", year=2023)
    .group_by("MUNIC_RES")
    .agg([pl.sum("VAL_TOT")])
    .plot.choropleth(geography="municipalities")
)
```

#### Business Intelligence Integration

- Tableau/PowerBI connectors
- Dashboard templates
- Automated reporting
- KPI monitoring

## Conclusion

The Arrow SUS Polars IO plugins represent a significant architectural improvement over the current implementation:

### Key Benefits

1. **Performance**: 2-5x faster queries with optimized predicate/projection pushdown
1. **Memory Efficiency**: Streaming-first design handles datasets larger than RAM
1. **Ecosystem Integration**: Native Polars LazyFrame support enables advanced analytics
1. **Scalability**: Parallel processing and caching for production workloads
1. **Usability**: Consistent API across all Brazilian health data sources

### Technical Advantages

1. **Zero-Copy Operations**: Arrow-native processing eliminates data conversion overhead
1. **Lazy Evaluation**: Query optimization happens at the engine level
1. **Pushdown Optimizations**: Filters and projections applied during data reading
1. **Parallel Processing**: Concurrent downloads and processing
1. **Smart Caching**: Automatic conversion to optimized formats

### Implementation Strategy

The phased approach ensures:

- **Incremental Value**: Each phase delivers working functionality
- **Risk Management**: Early testing and validation
- **Compatibility**: Seamless migration path
- **Quality**: Comprehensive testing at each stage

### Long-term Vision

Arrow SUS will become the standard library for Brazilian health data analysis, providing:

- **Unified Access**: Single API for all major health data sources
- **Production Ready**: Enterprise-grade performance and reliability
- **Research Enablement**: Advanced analytics capabilities for researchers
- **Policy Support**: Real-time insights for health policy decisions

This comprehensive implementation plan provides a roadmap for building a world-class health data access library that will serve the Brazilian public health community for years to come.
