# DuckDB + Lua for Game ECS Spatial Queries

Research project exploring **DuckDB as a spatial query backend for ECS game engines**, with **Lua scripting for moddability**.

## 🎯 Project Goal

Evaluate whether DuckDB can replace custom spatial data structures (R-trees, spatial hashing) in a game engine while allowing mods to define custom logic via Lua UDFs.

**Target:** 60 FPS = 16.67ms per tick. Spatial queries should consume ≤10% of frame budget.

---

## 📊 Key Findings

### DuckDB Performance

| Metric | Value | Notes |
|--------|-------|-------|
| Per-query overhead | ~180 µs | Fixed cost per SQL query execution |
| Built-in sqrt (100×100) | 0.75 ms | Native DuckDB spatial math |
| Rust VScalar UDF | 0.86 ms | Custom Rust function, 1.1× overhead |
| Lua VScalar UDF | 2.01 ms | LuaJIT via VArrowScalar, 2.7× overhead |

### Lua VM Performance (Isolated)

| VM | Per-Call | Cross-Join at 10% Budget |
|----|----------|--------------------------|
| **mlua/LuaJIT** | 145 ns | 107×107 entities |
| Piccolo (pure Rust) | 312 ns | 73×73 entities |
| Native Rust | 2 ns | (baseline) |

**LuaJIT is 2.2× faster than Piccolo** but Piccolo offers better sandboxing.

### Spatial Query Approaches

| Approach | 100×100 Query Time | Verdict |
|----------|-------------------|---------|
| DuckDB cross-join + sqrt | 0.75 ms | ✅ Viable for small counts |
| DuckDB + Lua UDF | 2.01 ms | ✅ Viable for mods |
| Rust HashMap spatial hash | 0.001 ms | 100-700× faster than DuckDB |

**Conclusion:** DuckDB is viable for complex queries (JOINs, aggregations) but pure Rust spatial hashing is far faster for simple proximity queries.

---

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────────┐
│  SQL Query                                                 │
│  SELECT * FROM e1, e2                                      │
│  WHERE lua_distance(e1.x, e1.y, e2.x, e2.y) < range        │
└───────────────────────────┬────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  DuckDB Query Executor                                     │
│  • Cross-join produces candidate pairs                     │
│  • Calls lua_distance VScalar for filtering                │
│  • Vectorized: processes ~2048 rows per call               │
└───────────────────────────┬────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  Rust VArrowScalar (LuaDistanceScalar)                     │
│  • Receives Arrow RecordBatch with (x1, y1, x2, y2)        │
│  • For each row: call thread-local Lua                     │
│  • Return Arrow Float64Array to DuckDB                     │
└───────────────────────────┬────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────┐
│  Thread-Local LuaJIT VM                                    │
│  • Lazily initialized per DuckDB worker thread             │
│  • JIT-compiled Lua functions                              │
│  • ~200 ns per distance() call                             │
└────────────────────────────────────────────────────────────┘
```

---

## 📁 File Structure

### Core Benchmarks

| File | Purpose |
|------|---------|
| `src/main.rs` | Polars baseline benchmarks |
| `src/duckdb_bench.rs` | Basic DuckDB vs Polars comparison |
| `src/duckdb_latency.rs` | DuckDB per-query latency analysis |
| `src/duckdb_min_overhead.rs` | Minimal overhead measurement |

### Spatial Query Experiments

| File | Purpose |
|------|---------|
| `src/duckdb_spatial_opt.rs` | Spatial query optimization strategies |
| `src/duckdb_deep_dive.rs` | Deep analysis of spatial query performance |
| `src/duckdb_sparse.rs` | Sparse world simulation |
| `src/duckdb_simulation.rs` | Full game tick simulation |

### Arrow Integration

| File | Purpose |
|------|---------|
| `src/duckdb_arrow.rs` | DuckDB → Arrow data transfer |
| `src/duckdb_arrow_spatial.rs` | Arrow-based spatial queries |
| `src/duckdb_arrow_dive.rs` | Arrow zero-copy analysis |

### Lua Integration (Key Files)

| File | Purpose |
|------|---------|
| `src/lua_vm_comparison.rs` | **Piccolo vs mlua/LuaJIT benchmark** |
| `src/lua_udf_threadlocal.rs` | Thread-local Lua VM proof-of-concept |
| `src/duckdb_lua_vscalar.rs` | **DuckDB VArrowScalar + Lua UDF integration** |
| `src/lua_query_overhead.rs` | Lua query caching analysis |
| `src/piccolo_duckdb_poc.rs` | Piccolo Lua integration attempt |

---

## 🚀 Running Benchmarks

```bash
# Build all
cargo build --release

# Key benchmarks
./target/release/lua_vm_comparison      # Compare Piccolo vs LuaJIT
./target/release/duckdb_lua_vscalar     # Full DuckDB + Lua pipeline
./target/release/duckdb_latency         # DuckDB overhead analysis
./target/release/duckdb_deep_dive       # Spatial query strategies
```

---

## 📦 Dependencies

```toml
[dependencies]
duckdb = { version = "1.1.1", features = [
    "bundled",       # Embed DuckDB
    "vtab",          # Virtual tables
    "vtab-arrow",    # Arrow integration
    "vscalar",       # Scalar UDFs
    "vscalar-arrow"  # Arrow-based scalar UDFs
] }
mlua = { version = "0.10", features = ["luajit", "vendored"] }  # LuaJIT bindings
piccolo = "0.3.3"    # Pure Rust Lua 5.4 (alternative)
polars = "0.46"      # DataFrame library (baseline comparison)
```

---

## 🎮 Use Cases

### When to Use DuckDB

- **Complex queries:** Multi-table JOINs, aggregations, window functions
- **Mod-defined logic:** Lua UDFs for scoring, filtering, prioritization
- **Debugging:** SQL is human-readable, easy to inspect game state
- **Prototyping:** Quick iteration without recompiling Rust

### When to Use Native Rust

- **Simple proximity:** Spatial hashing is 100-700× faster
- **Hot paths:** Per-entity updates every tick
- **Large entity counts:** 1000+ entities need native structures

### Hybrid Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Game Tick                              │
├─────────────────────────────────────────────────────────────┤
│ 1. Rust spatial hash → find nearby candidates (< 0.1 ms)    │
│ 2. DuckDB query with Lua UDF → complex scoring (< 2 ms)     │
│ 3. Return filtered results to game logic                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔑 Technical Insights

### Thread-Local Lua VMs

DuckDB executes VScalar functions on worker threads. Use `thread_local!` to lazily initialize Lua VMs:

```rust
thread_local! {
    static LUA_VM: RefCell<mlua::Lua> = RefCell::new({
        let lua = mlua::Lua::new();
        lua.load(SCRIPT).exec().unwrap();
        lua
    });
}
```

### VArrowScalar vs VScalar

- `VScalar`: Low-level, requires manual `DataChunkHandle` manipulation
- `VArrowScalar`: Higher-level, receives `RecordBatch`, returns `Arc<dyn Array>`

Use `VArrowScalar` for numeric types—it's cleaner and just as fast.

### Piccolo Lifetime Issues

Piccolo uses branded lifetimes (`Lua<'gc>`) that don't work with DuckDB's `'static` state requirement. Use mlua for DuckDB integration.

---

## 📈 Frame Budget Summary

For a 60 FPS game with 10% spatial query budget (1.67 ms):

| Approach | Max Cross-Join Size |
|----------|---------------------|
| DuckDB built-in | ~150×150 |
| DuckDB + Lua UDF | ~91×91 |
| Pure Rust HashMap | ~1000×1000+ |

---

## 🔮 Future Work

1. **Spatial indexing:** R-tree with DuckDB spatial extension (caveats: no JOINs)
2. **Batch Lua calls:** Pass entire arrays to Lua, reduce FFI overhead
3. **Query caching:** Pre-compile SQL statements, cache Lua functions
4. **Parallel Lua:** One VM per DuckDB worker thread (already implemented)

---

## 📝 License

Research/experimental code. Use at your own risk.
