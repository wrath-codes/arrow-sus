# Arrow SUS - Complete API Specification

> Comprehensive API for Brazilian health data access with Arrow/Polars lazy evaluation

## Table of Contents

- [Overview](#overview)
- [Priority Implementation Order](#priority-implementation-order)
- [Core API Design](#core-api-design)
- [Data Sources](#data-sources)
- [Advanced Features](#advanced-features)
- [Rust Extensions](#rust-extensions)

## Overview

Arrow SUS provides a unified, lazy-first API for accessing Brazilian health data from multiple sources. Built on Apache Arrow and Polars for maximum performance.

### Core Principles

- **Lazy-first**: All operations return LazyFrames until `.collect()`
- **Zero-copy**: Leverage Arrow's columnar format
- **Unified API**: Consistent interface across all data sources
- **Type-safe**: Full type hints and validation
- **Dependency injection**: RequiresContext pattern throughout

## Priority Implementation Order

### 🔥 Priority 1 - Core Infrastructure

- [ ] Settings and configuration system
- [ ] Base client with RequiresContext
- [ ] Caching system (Parquet-based)
- [ ] Error handling framework
- [ ] Basic SIH implementation (most common use case)

### 🚀 Priority 2 - DataSUS Systems

- [ ] SIA (Ambulatory Information System)
- [ ] CNES (National Registry of Health Establishments)
- [ ] SIM (Mortality Information System)
- [ ] SINASC (Live Birth Information System)
- [ ] PNI (National Immunization Program)

### 📊 Priority 3 - External APIs

- [ ] IBGE (Brazilian Institute of Geography and Statistics)
- [ ] InfoDengue API integration
- [ ] InfoGripe CSV integration
- [ ] Territory/Maps from FTP

### 🔐 Priority 4 - Authenticated Sources

- [ ] eSUS (requires ElasticSearch credentials)
- [ ] Vaccine data (requires gov website credentials)
- [ ] SINAM (Disease notification system)

### ⚡ Priority 5 - Advanced Features

- [ ] DuckDB integration
- [ ] Rust compute modules
- [ ] Streaming operations
- [ ] Distributed processing
- [ ] Multi Storage integration

## Core API Design

### Base Client Structure

```python
from arrow_sus import DataSUS
from arrow_sus.settings import datasus_settings

# Initialize client
client = DataSUS(settings=datasus_settings)

# Or with dependency injection
from returns.context import RequiresContext


def get_client() -> RequiresContext[DataSUS, DataSusDeps]:
    return RequiresContext(lambda deps: DataSUS(deps.datasus_settings))
```

### Common Patterns

#### Pattern 1: File-based with UF + Group + Month + Year

**Systems**: SIH, SIA, CNES, CIHA

```python
# SIH - Hospital Information System
client.sih()
    .uf("SP")                    # Required: State code
    .group("RD")                 # Required: Group (RD, ER, etc.)
    .year(2023)                  # Required: Year
    .month(1)                    # Required: Month (1-12)
    .collect()                   # -> pl.DataFrame

# SIA - Ambulatory Information System  
client.sia()
    .uf("RJ")
    .group("PA")                 # Groups: PA, PS, etc.
    .year(2023)
    .month(6)
    .collect()

# CNES - Health Establishments
client.cnes()
    .uf("MG")
    .group("LT")                 # Groups: LT, ST, DC, etc.
    .year(2023)
    .month(12)
    .collect()

# CIHA - Hospital Census (only one group)
client.ciha()
    .uf("SP")
    .year(2023)
    .month(3)
    .collect()                   # Group is implicit (CIHA)
```

#### Pattern 2: File-based with UF + Year (no month)

**Systems**: SIM, PNI, SINASC

```python
# SIM - Mortality Information
client.sim()
    .uf("SP")
    .year(2023)
    .collect()

# PNI - National Immunization Program
client.pni()
    .uf("RJ")
    .year(2023)
    .collect()

# SINASC - Live Birth Information
client.sinasc()
    .uf("MG")
    .year(2023)
    .collect()
```

#### Pattern 3: Disease-based with Code + Year

**Systems**: SINAM

```python
# SINAM - Disease Notification System
client.sinam()
    .disease("A90")              # Disease code (Dengue, etc.)
    .year(2023)
    .collect()

# Multiple diseases
client.sinam()
    .diseases(["A90", "A91"])    # Multiple disease codes
    .year_range(2020, 2023)      # Year range
    .collect()
```

### Chaining and Filtering

```python
# Complex lazy query
result = (
    client.sih()
    .uf("SP")
    .group("RD")
    .year(2023)
    .month_range(1, 6)  # January to June
    .filter(pl.col("VAL_TOT") > 1000)
    .select(["MUNIC_RES", "VAL_TOT", "DT_INTER"])
    .group_by("MUNIC_RES")
    .agg([pl.sum("VAL_TOT").alias("total_value"), pl.count().alias("procedures")])
    .sort("total_value", descending=True)
    .collect()  # Execute entire pipeline
)
```

### Multiple Data Sources

```python
# Combine multiple months/states
data = (
    client.sih()
    .uf(["SP", "RJ", "MG"])  # Multiple states
    .group("RD")
    .year(2023)
    .month_range(1, 12)  # Full year
    .collect()
)

# Combine multiple years
historical = (
    client.sim()
    .uf("SP")
    .year_range(2018, 2023)  # 5-year range
    .collect()
)
```

## Data Sources

### DataSUS FTP Sources

#### SIH - Hospital Information System

```python
# Available groups: RD (Reduced), ER (Enhanced), RJ (Rejected)
client.sih().uf("SP").group("RD").year(2023).month(1).collect()

# Bulk operations
client.sih().uf("SP").group("RD").year(2023).all_months().collect()
```

#### SIA - Ambulatory Information System

```python
# Available groups: PA (Procedures), PS (Psychosocial), etc.
client.sia().uf("RJ").group("PA").year(2023).month(6).collect()
```

#### CNES - National Registry of Health Establishments

```python
# Available groups: LT (Beds), ST (Establishments), DC (Professionals), etc.
client.cnes().uf("MG").group("LT").year(2023).month(12).collect()
```

#### SIM - Mortality Information System

```python
client.sim().uf("SP").year(2023).collect()
```

#### SINASC - Live Birth Information System

```python
client.sinasc().uf("RJ").year(2023).collect()
```

#### PNI - National Immunization Program

```python
client.pni().uf("MG").year(2023).collect()
```

#### SINAM - Disease Notification System

```python
client.sinam().disease("A90").year(2023).collect()  # Dengue
```

### External APIs

#### IBGE - Brazilian Institute of Geography and Statistics

```python
# Population data
client.ibge()
    .dataset("population")
    .year(2023)
    .collect()

# Economic indicators
client.ibge()
    .dataset("gdp")
    .year_range(2020, 2023)
    .collect()
```

#### InfoDengue - Dengue Information System

```python
# Requires multiple API calls - handled internally
client.infodengue()
    .disease("dengue")
    .geocode("3550308")          # São Paulo city code
    .year_range(2020, 2023)
    .collect()
```

#### InfoGripe - Influenza Surveillance

```python
# Fetches from CSV repository
client.infogripe()
    .year(2023)
    .week_range(1, 52)
    .collect()
```

#### Territory/Maps

```python
# Geographic data from FTP
client.territory()
    .level("municipality")       # state, municipality, health_region
    .year(2023)
    .format("geojson")          # geojson, shapefile
    .collect()
```

### Authenticated Sources

#### eSUS - Primary Care Information System

```python
# Requires ElasticSearch credentials in settings
client.esus()
    .index("citizens")
    .date_range("2023-01-01", "2023-12-31")
    .query({"match": {"city": "São Paulo"}})
    .collect()
```

#### Vaccine Data

```python
# Requires government website credentials
client.vaccine()
    .vaccine_type("covid19")
    .uf("SP")
    .date_range("2023-01-01", "2023-12-31")
    .collect()
```

## Advanced Features

### DuckDB Integration

```python
# Send data directly to DuckDB
client.sih()
    .uf("SP")
    .group("RD")
    .year(2023)
    .month(1)
    .to_duckdb("sih_data.db", table="sih_sp_2023_01")

# Query from DuckDB
result = client.duckdb("sih_data.db").sql("""
    SELECT MUNIC_RES, SUM(VAL_TOT) as total
    FROM sih_sp_2023_01 
    GROUP BY MUNIC_RES
    ORDER BY total DESC
""").collect()
```

### Caching System

```python
# Configure caching
client = DataSUS(
    settings=datasus_settings,
    cache_dir="/path/to/cache",
    cache_strategy="parquet",  # parquet, arrow, disabled
)

# Cache management
client.cache.clear()  # Clear all cache
client.cache.clear("sih")  # Clear specific system
client.cache.info()  # Cache statistics
```

### Streaming Operations

```python
# Stream large datasets
for chunk in client.sih().uf("SP").group("RD").year(2023).all_months().stream():
    # Process chunk by chunk
    processed = chunk.filter(pl.col("VAL_TOT") > 1000)
    # Save or process incrementally
```

### Schema Management

```python
# Get schema information
schema = client.sih().schema()
print(schema.columns)           # Available columns
print(schema.dtypes)            # Column types

# Schema evolution handling
client.sih()
    .uf("SP")
    .group("RD")
    .year_range(2018, 2023)     # May have schema changes
    .schema_strategy("flexible") # flexible, strict, ignore
    .collect()
```

### Error Handling and Debugging

```python
# Query explanation
query = client.sih().uf("SP").group("RD").year(2023).month(1)
print(query.explain())  # Show execution plan

# Preview without full collection
preview = query.preview(n=1000)  # First 1000 rows
print(preview)

# Validation
query.validate()  # Check if data exists before collect()
```

## Performance & Storage Backends

### Execution Engine Configuration

```python
# Default local execution
client = DataSUS(settings=datasus_settings)

# Polars Cloud distributed execution
client = DataSUS(
    settings=datasus_settings,
    execution_engine="polars_cloud",
    polars_cloud_token="your_token",
    polars_cloud_cluster_size="large",
)

# Same API, different execution backend
data = client.sih().uf("SP").year(2023).collect()  # Runs on cluster
```

### Storage Backend Extensions

```python
# Core API unchanged - add materialization options
query = client.sih().uf("SP").group("RD").year(2023).month(1)

# Enhanced collection methods
query.collect()  # -> pl.DataFrame (unchanged)
query.collect_to_delta("s3://bucket/health/sih/")  # Materialize to Delta Lake
query.collect_to_iceberg("s3://warehouse/health.sih")  # Materialize to Iceberg
query.collect_to_parquet("s3://cache/sih/")  # Partitioned Parquet
query.collect_streaming(chunk_size="100MB")  # Streaming for large datasets
```

### Smart Caching Backends

```python
# Delta Lake caching with time travel
client = DataSUS(
    settings=datasus_settings,
    cache_backend="delta",
    cache_location="s3://bucket/cache/",
)

# Iceberg caching with schema evolution
client = DataSUS(
    settings=datasus_settings,
    cache_backend="iceberg",
    iceberg_catalog="s3://bucket/catalog/",
)
```

## Rust Extensions

### High-Performance Modules

#### DBF Processing (Already Implemented)

```python
from arrow_sus.rust import dbf_info, process_dbf

# Get DBF file information
info = dbf_info("data.dbf")
print(f"Records: {info.record_count}")

# Process DBF to Arrow
arrow_table = process_dbf("data.dbf")
```

#### Planned Rust Modules

```python
# Geographic operations
from arrow_sus.rust import geo_ops

distances = geo_ops.calculate_distances(points1, points2)

# Statistical computations
from arrow_sus.rust import stats

correlation_matrix = stats.correlation_matrix(dataframe)

# Data validation
from arrow_sus.rust import validation

validation_result = validation.validate_sus_data(dataframe, "sih")
```

## Configuration

### Settings Structure

```python
class ArrowSusSettings(BaseSettings):
    # DataSUS FTP
    datasus_ftp_host: str = "ftp.datasus.gov.br"
    datasus_ftp_base_path: str = "/dissemin/publicos"
    datasus_s3_mirror: HttpUrl = (
        "https://datasus-ftp-mirror.nyc3.digitaloceanspaces.com"
    )
    datasus_s3_cdn: HttpUrl = (
        "https://datasus-ftp-mirror.nyc3.cdn.digitaloceanspaces.com"
    )

    # eSUS ElasticSearch
    esus_elasticsearch_host: str
    esus_elasticsearch_user: str
    esus_elasticsearch_password: str
    esus_elasticsearch_base_path: str = "/esus"

    # Vaccine API
    vaccine_api_user: str
    vaccine_api_password: str
    vaccine_api_base_url: HttpUrl

    # Caching
    cache_dir: str = "~/.arrow_sus_cache"
    cache_max_size_gb: int = 10

    # Performance
    max_concurrent_downloads: int = 4
    chunk_size_mb: int = 100

    # Performance settings
    execution_engine: Literal["polars", "polars_cloud"] = "polars"
    polars_cloud_token: Optional[str] = None
    polars_cloud_cluster_size: Optional[str] = None

    # Storage backends
    default_storage_backend: Literal["local", "s3", "gcs"] = "local"
    s3_bucket: Optional[str] = None
    iceberg_catalog: Optional[str] = None
    delta_table_location: Optional[str] = None

    # Enhanced caching
    cache_backend: Literal["parquet", "delta", "iceberg"] = "parquet"
    cache_partitioning: List[str] = ["system", "year", "state"]
    enable_distributed_downloads: bool = True
    max_concurrent_files: int = 10
```

## Usage Examples

### Basic Usage

```python
from arrow_sus import DataSUS
from arrow_sus.settings import arrow_sus_settings

client = DataSUS(arrow_sus_settings)

# Get hospital data for São Paulo, January 2023
data = client.sih().uf("SP").group("RD").year(2023).month(1).collect()

print(f"Downloaded {len(data)} records")
```

### Advanced Analysis

```python
# Multi-state, multi-year analysis
analysis = (
    client.sih()
    .uf(["SP", "RJ", "MG"])
    .group("RD")
    .year_range(2020, 2023)
    .all_months()
    .filter(pl.col("VAL_TOT") > 0)
    .group_by(["ANO_CMPT", "UF_ZI"])
    .agg(
        [
            pl.sum("VAL_TOT").alias("total_cost"),
            pl.count().alias("procedures"),
            pl.mean("VAL_TOT").alias("avg_cost"),
        ]
    )
    .sort(["ANO_CMPT", "total_cost"], descending=[False, True])
    .collect()
)

# Save to DuckDB for further analysis
analysis.to_duckdb("health_analysis.db", "hospital_summary")
```

### With Dependency Injection

```python
from returns.context import RequiresContext
from arrow_sus.deps import ArrowSusDeps


def analyze_mortality(
    uf: str, year: int
) -> RequiresContext[pl.DataFrame, ArrowSusDeps]:
    def _analyze(deps: ArrowSusDeps) -> pl.DataFrame:
        client = DataSUS(deps.arrow_sus_settings)
        return (
            client.sim()
            .uf(uf)
            .year(year)
            .group_by("CAUSABAS")
            .agg([pl.count().alias("deaths"), pl.mean("IDADE").alias("avg_age")])
            .sort("deaths", descending=True)
            .collect()
        )

    return RequiresContext(_analyze)


# Usage with dependency injection
deps = ArrowSusDeps(arrow_sus_settings=settings)
mortality_data = analyze_mortality("SP", 2023)(deps)
```

## Implementation Checklist

### Phase 1: Foundation (Priority 1)

- [ ] **Settings System**

  - [ ] `ArrowSusSettings` class with all data source configurations
  - [ ] Environment variable support
  - [ ] Validation for required credentials
  - [ ] Configuration file support (.env, .toml)

- [ ] **Core Client Architecture**

  - [ ] Base `DataSUS` client class
  - [ ] RequiresContext integration
  - [ ] Lazy evaluation framework
  - [ ] Error handling system

- [ ] **Caching Infrastructure**

  - [ ] Parquet-based local caching
  - [ ] Cache key generation (source + parameters)
  - [ ] Cache invalidation strategies
  - [ ] Cache size management
  - [ ] Metadata tracking (download dates, file sizes)

- [ ] **SIH Implementation (Proof of Concept)**

  - [ ] File URL generation
  - [ ] Download management
  - [ ] DBF to Arrow conversion
  - [ ] Schema handling
  - [ ] Basic filtering operations

### Phase 2: DataSUS Core Systems (Priority 2)

- [ ] **SIA - Ambulatory Information System**

  - [ ] Group handling (PA, PS, etc.)
  - [ ] File naming pattern implementation
  - [ ] Schema mapping

- [ ] **CNES - Health Establishments Registry**

  - [ ] Multiple group types (LT, ST, DC, etc.)
  - [ ] Establishment data processing
  - [ ] Geographic code handling

- [ ] **SIM - Mortality Information System**

  - [ ] Annual file processing
  - [ ] Cause of death coding (ICD-10)
  - [ ] Age group standardization

- [ ] **SINASC - Live Birth Information System**

  - [ ] Birth certificate data processing
  - [ ] Mother/child health indicators
  - [ ] Geographic analysis support

- [ ] **PNI - National Immunization Program**

  - [ ] Vaccine coverage data
  - [ ] Age group analysis
  - [ ] Coverage rate calculations

### Phase 3: External Data Sources (Priority 3)

- [ ] **IBGE Integration**

  - [ ] Population data by year
  - [ ] Economic indicators
  - [ ] Geographic boundaries
  - [ ] Census data access

- [ ] **InfoDengue API**

  - [ ] Multiple API endpoint handling
  - [ ] Rate limiting and retry logic
  - [ ] Geocode to municipality mapping
  - [ ] Time series data aggregation

- [ ] **InfoGripe CSV Processing**

  - [ ] Remote CSV download
  - [ ] Data validation and cleaning
  - [ ] Epidemiological week handling
  - [ ] Influenza surveillance metrics

- [ ] **Territory/Geographic Data**

  - [ ] Shapefile processing
  - [ ] GeoJSON conversion
  - [ ] Multiple administrative levels
  - [ ] Coordinate system handling

### Phase 4: Authenticated Sources (Priority 4)

- [ ] **eSUS ElasticSearch Integration**

  - [ ] Authentication handling
  - [ ] Query DSL construction
  - [ ] Result pagination
  - [ ] Index management

- [ ] **Vaccine Data API**

  - [ ] Government portal authentication
  - [ ] Session management
  - [ ] Data extraction from web forms
  - [ ] Rate limiting compliance

- [ ] **SINAM Disease Notification**

  - [ ] Disease code mapping
  - [ ] Notification data processing
  - [ ] Epidemiological analysis support

### Phase 5: Advanced Features (Priority 5)

- [ ] **DuckDB Integration**

  - [ ] Automatic table creation
  - [ ] SQL query interface
  - [ ] Join operations across data sources
  - [ ] Analytical query optimization

- [ ] **Rust Performance Modules**

  - [ ] Geographic distance calculations
  - [ ] Statistical computations
  - [ ] Data validation routines
  - [ ] String processing optimizations

- [ ] **Streaming and Distributed Processing**

  - [ ] Chunked data processing
  - [ ] Memory-efficient operations
  - [ ] Parallel download management
  - [ ] Progress tracking

### Phase 6: Performance & Storage Backends (Future)

- [ ] **Polars Cloud Integration**

  - [ ] Authentication and token management
  - [ ] Cluster size configuration
  - [ ] Distributed query execution
  - [ ] Cost monitoring and limits

- [ ] **Storage Backend Support**

  - [ ] Delta Lake write support
  - [ ] Iceberg table creation and writes
  - [ ] S3/GCS backend abstraction
  - [ ] Partitioning strategies

- [ ] **Enhanced Caching**

  - [ ] Delta Lake cache backend
  - [ ] Iceberg cache backend
  - [ ] Time travel capabilities
  - [ ] Schema evolution handling
  - [ ] Distributed cache invalidation

- [ ] **Streaming Operations**

  - [ ] Chunked collection methods
  - [ ] Memory-efficient processing
  - [ ] Progress tracking for large datasets
  - [ ] Async collection support

## API Method Reference

### Common Methods (All Data Sources)

#### Filtering Methods

```python
.uf(code: str | list[str])              # State filter
.year(year: int)                        # Single year
.year_range(start: int, end: int)       # Year range
.month(month: int)                      # Single month (1-12)
.month_range(start: int, end: int)      # Month range
.all_months()                           # All 12 months
```

#### Data Methods

```python
.collect() -> pl.DataFrame              # Execute and return DataFrame
.lazy() -> pl.LazyFrame                 # Return LazyFrame (default)
.stream() -> Iterator[pl.DataFrame]     # Stream chunks
.preview(n: int = 1000) -> pl.DataFrame # Preview first n rows
.count() -> int                         # Count total records
.schema() -> pl.Schema                  # Get schema information
.collect_to_delta(path: str, **kwargs) -> None     # Materialize to Delta Lake
.collect_to_iceberg(path: str, **kwargs) -> None   # Materialize to Iceberg  
.collect_to_parquet(path: str, **kwargs) -> None   # Partitioned Parquet
.collect_streaming(chunk_size: str) -> Iterator[pl.DataFrame] # Stream large datasets
.collect_async() -> Awaitable[pl.DataFrame]        # Async collection
```

#### Polars Integration

```python
.filter(expr: pl.Expr)                  # Filter with Polars expression
.select(columns: list[str] | pl.Expr)   # Select columns
.group_by(by: str | list[str])          # Group by columns
.agg(aggs: pl.Expr | list[pl.Expr])     # Aggregate functions
.sort(by: str, descending: bool = False) # Sort results
.with_columns(exprs: pl.Expr | list[pl.Expr]) # Add computed columns
```

#### Export Methods

```python
.to_parquet(path: str)                  # Save as Parquet
.to_csv(path: str)                      # Save as CSV
.to_duckdb(db_path: str, table: str)    # Save to DuckDB
.to_arrow() -> pa.Table                 # Convert to Arrow Table
```

#### Utility Methods

```python
.explain() -> str                       # Show execution plan
.validate() -> bool                     # Validate query before execution
.cache_info() -> dict                   # Cache statistics
.clear_cache()                          # Clear cached data
```

### System-Specific Methods

#### SIH/SIA/CNES/CIHA Specific

```python
.group(group_code: str)                 # Data group (RD, ER, PA, etc.)
.groups(group_codes: list[str])         # Multiple groups
```

#### SINAM Specific

```python
.disease(code: str)                     # Single disease code
.diseases(codes: list[str])             # Multiple disease codes
```

#### InfoDengue Specific

```python
.geocode(code: str)                     # Municipality geocode
.disease_type(disease: str)             # Disease type (dengue, chikungunya, etc.)
```

#### eSUS Specific

```python
.index(index_name: str)                 # ElasticSearch index
.query(query_dict: dict)                # ElasticSearch query DSL
.date_range(start: str, end: str)       # Date range filter
```

## Error Handling

### Exception Hierarchy

```python
class ArrowSusError(Exception):
    """Base exception for Arrow SUS"""


class DataNotFoundError(ArrowSusError):
    """Data not available for specified parameters"""


class AuthenticationError(ArrowSusError):
    """Authentication failed for protected resources"""


class SchemaError(ArrowSusError):
    """Schema validation or conversion error"""


class CacheError(ArrowSusError):
    """Cache operation error"""


class NetworkError(ArrowSusError):
    """Network or download error"""
```

### Error Context

```python
try:
    data = client.sih().uf("XX").group("RD").year(2025).collect()
except DataNotFoundError as e:
    print(f"Data not available: {e}")
    print(f"Available years: {e.available_years}")
    print(f"Available states: {e.available_states}")
```

## Performance Considerations

### Memory Management

- **Lazy evaluation**: Defer computation until `.collect()`
- **Chunked processing**: Handle datasets larger than RAM
- **Column pruning**: Only load required columns
- **Predicate pushdown**: Filter data before loading

### Network Optimization

- **Concurrent downloads**: Multiple files simultaneously
- **Resume capability**: Resume interrupted downloads
- **Compression**: Use compressed formats when available
- **CDN usage**: Prefer CDN over FTP when possible

### Caching Strategy

- **Intelligent caching**: Cache based on file modification dates
- **Partial updates**: Update only changed data
- **Compression**: Store cache as compressed Parquet
- **Metadata tracking**: Track data lineage and freshness

## Testing Strategy

### Unit Tests

- [ ] Settings validation
- [ ] URL generation
- [ ] Schema handling
- [ ] Cache operations
- [ ] Error conditions

### Integration Tests

- [ ] End-to-end data download
- [ ] Multi-source queries
- [ ] DuckDB integration
- [ ] Authentication flows
- [ ] **Cloud Integration Tests**
  - [ ] Polars Cloud authentication and execution
  - [ ] S3/GCS storage backend integration
  - [ ] Delta Lake catalog integration
  - [ ] Iceberg catalog integration
  - [ ] Cross-backend data consistency

### Performance Tests

- [ ] Large dataset handling
- [ ] Memory usage profiling
- [ ] Concurrent operation testing
- [ ] Cache performance
- [ ] **Backend Performance Tests**
  - [ ] Local vs Polars Cloud execution comparison
  - [ ] Storage backend write performance (Delta, Iceberg, Parquet)
  - [ ] Cache backend performance comparison
  - [ ] Distributed download performance testing
  - [ ] Memory usage across different execution engines
- [ ] **Storage Backend Tests**
  - [ ] Delta Lake write/read consistency
  - [ ] Iceberg schema evolution handling
  - [ ] S3/GCS integration testing
  - [ ] Partitioning strategy validation
  - [ ] Time travel functionality (Delta/Iceberg)

### Configuration Tests

- [ ] **Backend Configuration**
  - [ ] Settings validation for different backends
  - [ ] Credential management testing
  - [ ] Fallback behavior when backends unavailable
  - [ ] Configuration migration testing

## Documentation Plan

### User Documentation

- [ ] Quick start guide

- [ ] API reference

- [ ] Data source descriptions

- [ ] Performance tuning guide

- [ ] Troubleshooting guide

- [ ] **Migration Guide**

  - [ ] Upgrading from basic to cloud backends
  - [ ] Migrating cache backends
  - [ ] Configuration migration scripts
  - [ ] Performance migration checklist

- [ ] **Performance & Configuration Guide**

  - [ ] Backend selection guide (when to use what)
  - [ ] Polars Cloud setup and configuration
  - [ ] Storage backend comparison and selection
  - [ ] Cache strategy recommendations
  - [ ] Cost optimization guide for cloud backends

- [ ] **Storage Backend Guides**

  - [ ] Delta Lake setup and usage patterns
  - [ ] Iceberg setup and schema management
  - [ ] S3/GCS configuration and best practices
  - [ ] Partitioning strategy selection guide
  - [ ] Time travel and versioning guide

### Developer Documentation

- [ ] Architecture overview
- [ ] Contributing guidelines
- [ ] Rust extension development
- [ ] Testing procedures

This comprehensive API specification provides a roadmap for building a powerful, efficient, and user-friendly Brazilian health data access library. The lazy-first approach with Arrow/Polars will provide excellent performance, while the unified API will make it much more ergonomic than existing solutions.
