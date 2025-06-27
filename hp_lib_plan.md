# High-Performance Arrow SUS Architecture Plan

## Core Design Principles

### 1. Streaming-First Architecture

- Everything returns `Stream<RecordBatch>` by default
- Zero-copy operations throughout the pipeline
- Backpressure-aware streaming for memory management
- Collect is just a convenience method for materialization

### 2. Async-Native Pipeline

- All I/O operations are async from the ground up
- Parallel downloads with connection pooling
- Async file processing with proper error handling
- Concurrent multi-source operations

### 3. Advanced Caching Strategy

- Multi-level cache hierarchy (memory → SSD → object storage)
- Query-result caching with semantic keys
- Predictive prefetching based on usage patterns
- Compressed storage formats (Parquet/Arrow)

## Architecture Components

### Core Streaming Interface

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

### Async I/O Subsystem

```rust
pub struct AsyncIOManager {
    http_client: Arc<reqwest::Client>,
    connection_pool: ConnectionPool,
    download_semaphore: Arc<Semaphore>, // Limit concurrent downloads
    cache_manager: Arc<CacheManager>,
}

impl AsyncIOManager {
    pub async fn download_files(&self, urls: Vec<Url>) -> Vec<PolarsResult<Bytes>>;
    pub async fn download_with_cache(&self, url: Url) -> PolarsResult<PathBuf>;
    pub async fn stream_file(&self, path: PathBuf) -> PolarsResult<impl AsyncRead>;
}
```

### Smart Caching System

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

impl CacheManager {
    pub async fn get_or_compute<F, Fut>(&self, key: CacheKey, compute: F) -> PolarsResult<Vec<RecordBatch>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = PolarsResult<Vec<RecordBatch>>>;
        
    pub fn invalidate_source(&self, source: &DataSource);
    pub fn cache_stats(&self) -> CacheStats;
}
```

### Cross-Source Query Optimization

```rust
pub struct QueryPlanner {
    cost_model: CostModel,
    statistics: SourceStatistics,
    join_optimizer: JoinOptimizer,
}

pub struct OptimizedPlan {
    stages: Vec<ExecutionStage>,
    estimated_cost: f64,
    parallelism_opportunities: Vec<ParallelStage>,
}

impl QueryPlanner {
    pub fn optimize_query(&self, query: MultiSourceQuery) -> OptimizedPlan;
    pub fn can_pushdown_join(&self, left: &DataSource, right: &DataSource) -> bool;
    pub fn optimize_predicate_placement(&self, predicates: Vec<Expr>) -> Vec<(DataSource, Expr)>;
}
```

### Type-Safe Configuration

```rust
// Compile-time query validation
pub struct QueryBuilder<System, Location, Temporal, State> {
    system: System,
    location: Location,
    temporal: Temporal,
    _state: PhantomData<State>,
}

pub struct Incomplete;
pub struct Complete;

impl QueryBuilder<(), (), (), Incomplete> {
    pub fn new() -> Self;
}

impl<L, T> QueryBuilder<(), L, T, Incomplete> {
    pub fn sih(self) -> QueryBuilder<SihSystem, L, T, Incomplete>;
    pub fn sia(self) -> QueryBuilder<SiaSystem, L, T, Incomplete>;
    pub fn sim(self) -> QueryBuilder<SimSystem, L, T, Incomplete>;
}

impl<S, T> QueryBuilder<S, (), T, Incomplete> {
    pub fn uf<U: Into<UfCode>>(self, uf: U) -> QueryBuilder<S, UfCode, T, Incomplete>;
    pub fn ufs<U: Into<Vec<UfCode>>>(self, ufs: U) -> QueryBuilder<S, Vec<UfCode>, T, Incomplete>;
}

// Only complete queries can execute
impl<S, L> QueryBuilder<S, L, TemporalFilter, Complete> {
    pub async fn stream(self) -> PolarsResult<impl DataSusStream>;
    pub async fn collect(self) -> PolarsResult<DataFrame>;
}
```

### Pluggable Execution Backends

```rust
pub trait ExecutionEngine: Send + Sync {
    type Stream: DataSusStream;
    
    async fn execute(&self, plan: OptimizedPlan) -> PolarsResult<Self::Stream>;
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> EngineCapabilities;
}

pub struct EngineRegistry {
    engines: HashMap<String, Box<dyn ExecutionEngine>>,
    default_engine: String,
}

pub enum Engine {
    LocalPolars,
    PolarsCloud { token: String, cluster: String },
    DataFusion,
    DuckDB { connection: String },
}
```

## Implementation Phases

### Phase 1: Core Streaming Foundation (Weeks 1-2)

**Week 1: Basic Infrastructure**

- [ ] Set up Rust project with async runtime (tokio)
- [ ] Implement basic `DataSusStream` trait
- [ ] Create async I/O manager with connection pooling
- [ ] Basic file download and caching

**Week 2: SIH Streaming Implementation**

- [ ] Implement `SihStream` with async DBF processing
- [ ] Add predicate and projection pushdown
- [ ] Implement basic memory management
- [ ] Add error handling and recovery

### Phase 2: Advanced Caching (Weeks 3-4)

**Week 3: Multi-Level Cache**

- [ ] Implement in-memory cache with LRU eviction
- [ ] Add disk cache with Parquet storage
- [ ] Implement cache key generation and invalidation
- [ ] Add cache warming strategies

**Week 4: Query Result Caching**

- [ ] Implement semantic cache keys
- [ ] Add query result caching with TTL
- [ ] Implement cache consistency mechanisms
- [ ] Add prefetching based on access patterns

### Phase 3: Cross-Source Optimization (Weeks 5-6)

**Week 5: Query Planning**

- [ ] Implement basic cost model
- [ ] Add cross-source join optimization
- [ ] Implement predicate pushdown across sources
- [ ] Add parallel execution planning

**Week 6: Multi-Source Queries**

- [ ] Implement remaining DataSUS sources (SIA, SIM, CNES, etc.)
- [ ] Add cross-source query execution
- [ ] Implement join optimization
- [ ] Add streaming joins for large datasets

### Phase 4: Type Safety & Polish (Weeks 7-8)

**Week 7: Type-Safe API**

- [ ] Implement compile-time query validation
- [ ] Add type-safe parameter validation
- [ ] Implement builder pattern with state types
- [ ] Add comprehensive error types

**Week 8: Execution Backends**

- [ ] Implement pluggable execution engine interface
- [ ] Add local Polars backend
- [ ] Add DataFusion backend integration
- [ ] Implement engine selection logic

## API Design

### Streaming-First Usage

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
        # Process joined data in streaming fashion
        process_batch(joined_batch)


asyncio.run(main())
```

### Type-Safe Query Building

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

### Advanced Caching Control

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

## Performance Targets

### Throughput Goals

- **Single source queries**: 1M+ records/second processing
- **Multi-source joins**: 500K+ records/second
- **Cache hit performance**: \<10ms for cached queries
- **Cold cache performance**: \<30s for typical monthly datasets

### Memory Efficiency

- **Streaming memory usage**: \<500MB for any query size
- **Cache efficiency**: 80%+ compression ratio vs raw data
- **Memory growth**: Linear with concurrent queries, not data size

### Scalability

- **Concurrent queries**: 100+ simultaneous streams
- **Multi-year queries**: Handle 10+ years of data via streaming
- **Multi-state queries**: Process all 27 states in parallel

## Implementation Guidelines

### Error Handling Strategy

```rust
#[derive(Debug, thiserror::Error)]
pub enum DataSusError {
    #[error("Data not available: {source} for {parameters}")]
    DataNotAvailable { source: String, parameters: String },
    
    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),
    
    #[error("Cache error: {0}")]
    CacheError(String),
    
    #[error("Query planning error: {0}")]
    PlanningError(String),
    
    #[error("Execution error: {0}")]
    ExecutionError(String),
}
```

### Testing Strategy

- **Unit tests**: Each component tested in isolation
- **Integration tests**: End-to-end query execution
- **Performance tests**: Benchmarks against existing libraries
- **Stress tests**: Memory usage and concurrent access
- **Property tests**: Query correctness with random parameters

### Documentation Requirements

- **API documentation**: Complete rustdoc coverage
- **User guide**: Streaming patterns and best practices
- **Performance guide**: Optimization techniques
- **Migration guide**: From existing Arrow SUS implementation

## Success Criteria

### Performance Benchmarks

- **2-5x faster** than current implementation for typical queries
- **10x better memory efficiency** for large datasets
- **50% faster** than pandas-based alternatives
- **Comparable performance** to DuckDB for analytical queries

### Usability Goals

- **Zero-config streaming**: Works out of the box for any query size
- **Predictable performance**: Linear scaling with data size
- **Intuitive API**: Familiar to Polars users
- **Comprehensive error messages**: Clear guidance for common issues

This architecture provides a solid foundation for high-performance Brazilian health data processing while remaining implementable and maintainable.
