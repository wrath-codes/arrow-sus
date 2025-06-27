# Arrow SUS - Implementation Plan

> Comprehensive implementation strategy for building a high-performance Brazilian health data access library

## Table of Contents

- [Overview](#overview)
- [Unified Implementation Strategy](#unified-implementation-strategy)
- [Implementation Phases](#implementation-phases)
- [Key Technical Decisions](#key-technical-decisions)
- [Performance Targets](#performance-targets)
- [Project Structure](#project-structure)
- [API Design Preview](#api-design-preview)
- [Success Criteria](#success-criteria)
- [Next Steps](#next-steps)

## Overview

Arrow SUS will be a blazingly fast, streaming-first library for accessing Brazilian health data (SUS) built with Rust core and Python bindings. The project combines multiple architectural patterns to achieve maximum performance while maintaining ergonomic APIs.

### Core Principles

- **Streaming-first**: All operations return streams by default, collect() is just convenience
- **Zero-copy**: Leverage Arrow's columnar format throughout the pipeline
- **Async-native**: All I/O operations are async from the ground up
- **Type-safe**: Compile-time validation of queries and parameters
- **Static metadata**: Zero-alloc lookups using phf_codegen
- **Multi-level caching**: Memory → SSD → Object storage hierarchy

## Unified Implementation Strategy

The project combines five key architectural patterns:

### 1. **Streaming-First Architecture**

- Everything returns `Stream<RecordBatch>` by default
- Zero-copy operations throughout the pipeline
- Backpressure-aware streaming for memory management
- Collect is just a convenience method for materialization

### 2. **Static Metadata Pipeline**

- All metadata compiled at build-time using `phf_codegen`
- S3-based metadata storage with local caching
- Zero-allocation lookups for subsystems, schemas, UFs
- Versioned metadata with automatic updates

### 3. **Polylith Architecture (Python)**

- Modular components: scan/, describe/, metadata/, types/, io/
- Clean separation between bases (CLI, SDK) and components
- Independent testing and development of each component
- Composable and reusable building blocks

### 4. **Multi-Level Caching**

- L1: In-memory RecordBatch cache with LRU eviction
- L2: Compressed Parquet on disk
- L3: Object storage cache (S3/GCS)
- Query result caching with semantic keys
- Predictive prefetching based on usage patterns

### 5. **Type-Safe Query Building**

- Phantom types for compile-time validation
- Required parameters enforced at compile time
- Fluent API that guides users to valid queries
- Cross-source join optimization

## Implementation Phases

### 🔥 **Phase 1: Foundation (Weeks 1-2)**

```
#### **Week 1: Project Structure & Metadata Pipeline**
```

**Goals:**

- Restructure into proper Rust workspace with crates
- Implement static metadata generation pipeline
- Set up S3-based metadata storage
- Create schema definitions for all SUS subsystems

**Tasks:**

- [ ] Create Rust workspace structure: `crates/{core,metadata,cli,python}/`
- [ ] Implement `build.rs` with `phf_codegen` for static metadata
- [ ] Design JSON schema files for SIH, SIA, SIM, CNES, etc.
- [ ] Set up S3 metadata bucket with versioning
- [ ] Implement metadata loader with local caching
- [ ] Create static maps: `SUBSYSTEMS`, `SCHEMAS`, `UFS`, `MUNICIPALITIES`

**Deliverables:**

- Rust workspace with proper crate organization
- Static metadata pipeline generating zero-alloc lookups
- JSON schema definitions for all major SUS subsystems
- S3-based metadata storage with local fallback

#### **Week 2: Core Streaming Foundation**

**Goals:**

- Implement async streaming architecture
- Create basic I/O manager with connection pooling
- Build DBC file processing pipeline
- Add predicate pushdown and column projection

**Tasks:**

- [ ] Implement `DataSusStream` trait with Arrow integration
- [ ] Create `AsyncIOManager` with connection pooling
- [ ] Build streaming DBC decoder outputting `RecordBatch`
- [ ] Add predicate pushdown to reduce I/O
- [ ] Implement column projection for memory efficiency
- [ ] Create basic error handling framework

**Deliverables:**

- Async streaming foundation with `DataSusStream` trait
- DBC file processing with streaming output
- Basic I/O manager with concurrent downloads
- Predicate pushdown and projection capabilities

### 🚀 **Phase 2: Python Integration (Weeks 3-4)**

#### **Week 3: PyO3 Bindings**

**Goals:**

- Set up PyO3 integration with streaming support
- Implement Python-compatible async interfaces
- Create basic scan functions
- Add Polars LazyFrame integration

**Tasks:**

- [ ] Set up PyO3 with async runtime integration
- [ ] Create Python-compatible streaming interfaces
- [ ] Implement `scan_sih()`, `scan_sia()`, `scan_sim()`, `scan_cnes()`
- [ ] Add Polars LazyFrame conversion
- [ ] Create async collection methods
- [ ] Add basic error handling for Python

**Deliverables:**

- PyO3 bindings with async streaming support
- Basic scan functions for major SUS subsystems
- Polars integration with LazyFrame conversion
- Python-compatible async interfaces

#### **Week 4: Polylith Architecture**

**Goals:**

- Restructure Python code into Polylith components
- Create modular, reusable components
- Set up proper workspace configuration
- Implement bases for CLI and SDK

**Tasks:**

- [ ] Create `components/{scan,describe,metadata,types,io,cli}/`
- [ ] Create `bases/{python_cli,python_sdk}/`
- [ ] Set up `workspace.toml` configuration
- [ ] Implement component-based testing
- [ ] Create proper Python package structure
- [ ] Add CLI application with Typer

**Deliverables:**

- Polylith-structured Python codebase
- Modular components with independent testing
- CLI application and SDK base
- Proper workspace configuration

### 📊 **Phase 3: Advanced Features (Weeks 5-6)**

#### **Week 5: Multi-Level Caching**

**Goals:**

- Implement intelligent caching system
- Add memory, disk, and remote cache layers
- Create cache key generation and invalidation
- Add prefetching based on usage patterns

**Tasks:**

- [ ] Implement in-memory cache with LRU eviction
- [ ] Add disk cache with compressed Parquet storage
- [ ] Create semantic cache key generation
- [ ] Implement cache invalidation strategies
- [ ] Add query result caching with TTL
- [ ] Create prefetching based on access patterns

**Deliverables:**

- Multi-level caching system (memory/disk/remote)
- Intelligent cache key generation
- Query result caching with invalidation
- Predictive prefetching capabilities

#### **Week 6: Type-Safe Query API**

**Goals:**

- Implement phantom type-based query builder
- Add compile-time parameter validation
- Create cross-source join optimization
- Add streaming join operations

**Tasks:**

- [ ] Implement phantom type query builder
- [ ] Add compile-time parameter validation
- [ ] Create cross-source query planning
- [ ] Implement streaming joins for large datasets
- [ ] Add join optimization algorithms
- [ ] Create query cost estimation

**Deliverables:**

- Type-safe query API with compile-time validation
- Cross-source join optimization
- Streaming join operations
- Query planning and cost estimation

### ⚡ **Phase 4: Performance & Polish (Weeks 7-8)**

#### **Week 7: Performance Optimization**

**Goals:**

- Optimize streaming memory usage
- Implement concurrent operations with backpressure
- Add query planning and optimization
- Benchmark against existing solutions

**Tasks:**

- [ ] Optimize memory usage to \<500MB for any query
- [ ] Implement concurrent downloads with backpressure
- [ ] Add advanced query optimization
- [ ] Create performance benchmarks
- [ ] Profile and optimize hot paths
- [ ] Add memory and CPU profiling tools

**Deliverables:**

- Optimized streaming performance (1M+ records/sec)
- Memory-efficient operations (\<500MB usage)
- Comprehensive performance benchmarks
- Profiling and optimization tools

#### **Week 8: Production Readiness**

**Goals:**

- Add comprehensive error handling
- Implement pluggable execution backends
- Create extensive test suite
- Polish documentation and examples

**Tasks:**

- [ ] Implement comprehensive error handling
- [ ] Add pluggable execution backends (Polars, DataFusion, DuckDB)
- [ ] Create extensive unit and integration tests
- [ ] Add property-based testing for correctness
- [ ] Write comprehensive documentation
- [ ] Create usage examples and tutorials

**Deliverables:**

- Production-ready error handling
- Pluggable execution backends
- Comprehensive test suite (>90% coverage)
- Complete documentation and examples

## Key Technical Decisions

### **Metadata Architecture**

```rust
// Static metadata with zero-alloc lookups
pub static SUBSYSTEMS: phf::Map<&'static str, SubsystemMetadata> = // Generated by build.rs
pub static SCHEMAS: phf::Map<(&'static str, &'static str), &'static [SchemaField]> = // Generated
pub static MUNICIPALITIES: phf::Map<&'static str, Municipality> = // Generated

pub struct SubsystemMetadata {
    pub groups: &'static [&'static str],
    pub ufs: &'static [&'static str],
    pub path: &'static str,
    pub description: &'static str,
}
```

### **Streaming Interface**

```rust
pub trait DataSusStream: Stream<Item = PolarsResult<RecordBatch>> + Send + Unpin {
    fn schema(&self) -> &ArrowSchema;
    fn estimated_size(&self) -> Option<usize>;
    fn set_predicate(&mut self, predicate: Expr);
    fn set_projection(&mut self, columns: Vec<String>);
}

// All scan functions return streams
pub fn scan_datasus_sih(...) -> impl DataSusStream
pub fn scan_datasus_sia(...) -> impl DataSusStream
pub fn scan_datasus_sim(...) -> impl DataSusStream
```

### **Type-Safe Queries**

```rust
// Compile-time validation ensures required parameters
QueryBuilder::new()
    .sih()           // Sets system type
    .uf("SP")        // Sets location  
    .group("RD")     // Sets group
    .year(2023)      // Completes query - now can execute
    .collect()       // ✅ Valid

// This would be a compile error:
// QueryBuilder::new().sih().collect()  // ❌ Missing required parameters
```

### **Caching System**

```rust
pub struct CacheManager {
    // L1: In-memory RecordBatch cache
    memory_cache: Arc<LruCache<CacheKey, Vec<RecordBatch>>>,
    
    // L2: Compressed Parquet on disk
    disk_cache: Arc<DiskCache>,
    
    // L3: Object storage cache
    remote_cache: Option<Arc<RemoteCache>>,
    
    // Query result cache
    result_cache: Arc<ResultCache>,
}

#[derive(Hash, Eq, PartialEq)]
pub struct CacheKey {
    source: DataSource,
    parameters: QueryParameters,
    schema_version: u64,
}
```

## Performance Targets

### **Throughput Goals**

- **Single source queries**: 1M+ records/second processing
- **Multi-source joins**: 500K+ records/second
- **Cache hit performance**: \<10ms for cached queries
- **Cold cache performance**: \<30s for typical monthly datasets

### **Memory Efficiency**

- **Streaming memory usage**: \<500MB for any query size
- **Cache efficiency**: 80%+ compression ratio vs raw data
- **Memory growth**: Linear with concurrent queries, not data size

### **Scalability**

- **Concurrent queries**: 100+ simultaneous streams
- **Multi-year queries**: Handle 10+ years of data via streaming
- **Multi-state queries**: Process all 27 states in parallel

## Project Structure

### **Final Rust Workspace Structure**

```text
arrow-sus/
├── crates/
│   ├── core/                # Core streaming logic, DBC processing
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── loader.rs    # File discovery and filtering
│   │   │   ├── parser.rs    # Fast DBC decoding
│   │   │   ├── reader.rs    # Arrow-compatible streaming
│   │   │   └── stream.rs    # DataSusStream implementation
│   │   └── Cargo.toml
│   ├── metadata/            # S3 metadata management
│   │   ├── build.rs         # phf_codegen static generation
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── loader.rs    # S3 and local metadata loading
│   │   │   ├── cache.rs     # Metadata caching
│   │   │   └── generated/   # Auto-generated static maps
│   │   └── Cargo.toml
│   ├── cli/                 # Command-line interface
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   └── commands/
│   │   └── Cargo.toml
│   └── python/              # PyO3 bindings
│       ├── src/
│       │   ├── lib.rs
│       │   └── bindings.rs
│       └── Cargo.toml
├── components/              # Python Polylith components
│   ├── scan/
│   ├── describe/
│   ├── metadata/
│   ├── types/
│   ├── io/
│   └── cli/
├── bases/                   # Python entry points
│   ├── python_cli/
│   └── python_sdk/
├── data/                    # Static metadata JSON files
│   ├── subsystems.json
│   ├── municipalities.json
│   ├── ufs.json
│   └── schemas/
├── tests/
├── examples/
└── docs/
```

### **Python Package Structure (After Build)**

```text
arrow_sus/
├── __init__.py              # Re-exports from python_sdk base
├── scan.py                  # From scan component
├── describe.py              # From describe component
├── types.py                 # Enums + constants
├── _core.so                 # PyO3 binding to Rust core
└── py.typed                 # MyPy support
```

## API Design Preview

### **Streaming-First Usage (Python)**

```python
import polars as pl
import asyncio


# Everything is async and streaming by default
async def main():
    # Stream processing
    total_cost = 0
    async for batch in pl.scan_datasus_sih(
        uf="SP", group="RD", year=2023, month=1
    ).stream():
        batch_cost = batch.select(pl.sum("VAL_TOT")).item()
        total_cost += batch_cost

    # Materialization when needed
    df = await pl.scan_datasus_sih(uf="SP", group="RD", year=2023, month=1).collect()

    # Cross-source streaming joins
    hospital_stream = pl.scan_datasus_sih(uf="SP", group="RD", year=2023)
    mortality_stream = pl.scan_datasus_sim(uf="SP", year=2023)

    async for joined_batch in hospital_stream.stream_join(
        mortality_stream, on="municipality"
    ):
        process_batch(joined_batch)


asyncio.run(main())
```

### **Type-Safe Query Building**

```python
# Compile-time validated queries
query = (
    DataSusQuery.new()
    .sih()  # Selects SIH system
    .uf("SP")  # Sets location
    .group("RD")  # Sets group
    .year(2023)  # Sets temporal filter
    .month(1)  # Completes the query
)

# This would be a compile error:
# DataSusQuery.new().sih().collect()  # Missing required parameters

# Runtime execution
df = await query.collect()
stream = query.stream()
```

### **Advanced Caching Control**

```python
# Cache configuration
cache_config = CacheConfig(
    memory_limit="2GB",
    disk_cache_dir="/fast/ssd/cache",
    remote_cache_url="s3://bucket/cache/",
    ttl_hours=24,
    prefetch_enabled=True,
)

client = DataSusClient(cache_config=cache_config)

# Cache warming
await client.warm_cache(
    [
        ("sih", {"uf": "SP", "group": "RD", "year": 2023}),
        ("sia", {"uf": "SP", "group": "PA", "year": 2023}),
    ]
)

# Cache management
cache_stats = client.cache_stats()
await client.clear_cache(older_than_days=7)
```

## Success Criteria

### **Performance Benchmarks**

- **2-5x faster** than current implementation for typical queries
- **10x better memory efficiency** for large datasets
- **50% faster** than pandas-based alternatives
- **Comparable performance** to DuckDB for analytical queries

### **Usability Goals**

- **Zero-config streaming**: Works out of the box for any query size
- **Predictable performance**: Linear scaling with data size
- **Intuitive API**: Familiar to Polars users
- **Comprehensive error messages**: Clear guidance for common issues

### **Technical Metrics**

- **Test coverage**: >90% for all components
- **Documentation coverage**: 100% of public APIs
- **Memory safety**: Zero unsafe code in Python bindings
- **Compatibility**: Support Python 3.12+ and Rust 1.70+

## Next Steps

### **Immediate Actions (This Week)**

1. **Start Phase 1 Week 1**: Restructure project into Rust workspace
1. **Set up build pipeline**: Configure phf_codegen and static metadata generation
1. **Design JSON schemas**: Create initial schema definitions for SIH, SIA, SIM
1. **Set up S3 infrastructure**: Create metadata bucket and access policies

### **Week 1 Milestones**

- [ ] Rust workspace with proper crate structure
- [ ] Static metadata pipeline generating zero-alloc lookups
- [ ] JSON schema definitions for major SUS subsystems
- [ ] S3-based metadata storage with local caching

### **Success Validation**

Each phase will be validated with:

- **Unit tests**: Component-level functionality
- **Integration tests**: End-to-end query execution
- **Performance tests**: Benchmark against targets
- **Memory tests**: Validate streaming efficiency

This implementation plan provides a clear roadmap for building a world-class Brazilian health data access library that will significantly outperform existing solutions while maintaining excellent developer experience.
