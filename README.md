# DuckDB + Lua for Game ECS Spatial Queries

Research project exploring **DuckDB as a spatial query backend for ECS game engines**, with **Lua scripting for moddability**.

**Target:** 60 FPS = 16.67ms per tick. Spatial queries should consume ≤10% of frame budget (~1.67ms).

---

## ⚡ TL;DR

| Approach | 10K entities | vs Rust | Verdict |
|----------|-------------|---------|---------|
| **Rust HashMap** | **3 ms** | baseline | Best for real-time |
| **DuckDB Ultimate (12 threads)** | **8 ms** | 2.4× | ✅ Viable for mods |
| DuckDB Ultimate (1 thread) | 27 ms | 8.5× | Single-threaded only |
| DuckDB `abs()` filter | 278 ms | 90× | ❌ Don't use |

**Ultimate DuckDB Stack:**
1. `DOUBLE[2]` columns → enables SIMD `array_distance()` (1.7× faster)
2. 9× equality JOINs → forces hash join, not nested loop (10× faster)
3. UNION ALL → parallel execution across cores (4× with 12 threads)
4. Pre-computed cell indices → O(N×K) not O(N²)

**Bottom line:** DuckDB with all optimizations is viable for ~20K entities at 60 FPS. For larger counts, use Rust spatial hashing.

---

## 📊 Performance Deep Dive

### 1. Use `array_distance` for SIMD Speedup

DuckDB's `array_distance()` for fixed-size arrays is **SIMD-optimized**:

| Method | Time (5K cross-join) | Speedup |
|--------|---------------------|---------|
| **`array_distance(pos1, pos2)`** | **114 ms** | **1.7× faster** |
| Manual `dx²+dy² < r²` | 198 ms | baseline |
| `ST_Distance` | 2,447 ms | 12× slower |

```sql
-- ✅ BEST: Store positions as DOUBLE[2] for SIMD
CREATE TABLE entities (id INT, pos DOUBLE[2], cx INT, cy INT);

SELECT * FROM entities e1, entities e2
WHERE e1.id < e2.id AND array_distance(e1.pos, e2.pos) < 50;
```

### 2. Use Equality JOINs for Hash Join

**The critical insight:** DuckDB only uses hash join for **equality** conditions.

```sql
-- ❌ SLOW (90×): abs() forces O(N²) nested loop scan
WHERE abs(e2.cx - e1.cx) <= 1 AND abs(e2.cy - e1.cy) <= 1

-- ✅ FAST (2.5×): Explicit equality enables O(N) hash join
SELECT * FROM (
    SELECT e1.id, e2.id FROM entities e1
    INNER JOIN entities e2 ON e1.cx = e2.cx AND e1.cy = e2.cy
    WHERE e1.id < e2.id AND dist_sq < radius_sq
    UNION ALL
    SELECT e1.id, e2.id FROM entities e1
    INNER JOIN entities e2 ON e1.cx = e2.cx - 1 AND e1.cy = e2.cy
    WHERE e1.id < e2.id AND dist_sq < radius_sq
    -- ... 7 more for all 9 cell neighbor offsets
)
```

### 2. Multi-Threading Scales Well

DuckDB parallelizes the 9 UNION ALL branches:

| Threads | 10K entities | Speedup |
|---------|-------------|---------|
| 1 | 27 ms | baseline |
| 12 | 8 ms | **3.5×** |

At 20K entities: 92 ms (1 thread) → 21 ms (12 threads) = **1.6× vs Rust**

### 3. R-Tree Index Limitations

R-tree only works with **constant** geometry—not useful for entity-to-entity queries:

| Query Type | R-Tree? | Time (10M rows) |
|------------|---------|-----------------|
| `ST_Within(geom, constant_box)` | ✅ | 5.5 ms |
| `ST_DWithin(geom, point, dist)` | ❌ | 769 ms |
| Entity-to-entity JOIN | ❌ | Full scan |

**Use R-tree for:** Rectangle queries, analytics, tools  
**Don't use for:** Real-time proximity, combat targeting

---

## 🎮 Lua Integration

### LuaJIT FFI: Near-Native Performance

By passing Arrow buffer pointers directly to LuaJIT FFI:

| Method | Per-Element | vs Per-Row Lua |
|--------|-------------|----------------|
| Pure Rust | 2 ns | baseline |
| **LuaJIT FFI Batch** | 2-10 ns | **75× faster** |
| Lua Per-Row | 150 ns | baseline |

```lua
-- LuaJIT FFI operates directly on Arrow memory
function distance_ffi_batch(batch_ptr)
    local batch = ffi.cast("DistanceBatch*", batch_ptr)
    for i = 0, batch.n-1 do
        local dx = batch.x2[i] - batch.x1[i]
        local dy = batch.y2[i] - batch.y1[i]
        batch.out[i] = math.sqrt(dx*dx + dy*dy)
    end
end
```

### Lua VM Comparison

| VM | Per-Call | Notes |
|----|----------|-------|
| **mlua/LuaJIT** | 145 ns | 2.2× faster, requires unsafe |
| Piccolo (pure Rust) | 312 ns | Better sandboxing |

---

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────────┐
│  SQL Query                                                 │
│  SELECT * FROM e1, e2 WHERE lua_distance(...) < range      │
└───────────────────────────┬────────────────────────────────┘
                            ▼
┌────────────────────────────────────────────────────────────┐
│  DuckDB Query Executor (multi-threaded)                    │
│  • 9× hash join with UNION ALL                             │
│  • Vectorized processing (~2048 rows/batch)                │
└───────────────────────────┬────────────────────────────────┘
                            ▼
┌────────────────────────────────────────────────────────────┐
│  Rust VArrowScalar UDF                                     │
│  • Receives Arrow RecordBatch                              │
│  • Calls thread-local LuaJIT via FFI                       │
└───────────────────────────┬────────────────────────────────┘
                            ▼
┌────────────────────────────────────────────────────────────┐
│  Thread-Local LuaJIT VM                                    │
│  • Zero-copy batch processing via FFI                      │
│  • ~10 ns per element                                      │
└────────────────────────────────────────────────────────────┘
```

---

## 🎯 When to Use What

| Use Case | Best Approach |
|----------|---------------|
| Simple proximity (< 1000 entities) | DuckDB cross-join |
| Large-scale proximity (> 1000) | Rust spatial hash |
| Complex mod logic | DuckDB + LuaJIT FFI |
| Rectangle queries | DuckDB R-tree |
| Debug/analytics | DuckDB (SQL is readable) |

### Hybrid Architecture

```
Game Tick:
1. Rust spatial hash → find candidates      (< 0.1 ms)
2. DuckDB + Lua UDF → complex scoring       (< 2 ms)  
3. Return results to game logic
```

---

## 📁 Key Files

| File | Purpose |
|------|---------|
| `src/duckdb_hash_join.rs` | ⭐ **Best DuckDB approach** - 9× hash join |
| `src/duckdb_union_parallel.rs` | UNION ALL thread scaling test |
| `src/duckdb_luajit_ffi.rs` | DuckDB + LuaJIT FFI integration |
| `src/spatial_hashing_explained.rs` | Rust HashMap benchmark |
| `src/lua_vm_comparison.rs` | Piccolo vs LuaJIT benchmark |
| `src/duckdb_rtree_correct.rs` | R-Tree analysis (what works/fails) |

---

## 🚀 Quick Start

```bash
cargo build --release

# Best benchmark: DuckDB vs Rust spatial hash
./target/release/duckdb_hash_join

# Thread scaling test
./target/release/duckdb_union_parallel

# LuaJIT integration
./target/release/duckdb_luajit_ffi
```

---

## 📦 Dependencies

```toml
[dependencies]
duckdb = { version = "1.1.1", features = [
    "bundled", "vtab", "vtab-arrow", "vscalar", "vscalar-arrow"
] }
mlua = { version = "0.10", features = ["luajit", "vendored"] }
```

---

## 🔑 Key Technical Details

### Thread-Local Lua VMs

```rust
thread_local! {
    static LUA_VM: RefCell<mlua::Lua> = RefCell::new({
        let lua = unsafe { mlua::Lua::unsafe_new() }; // Required for FFI
        lua.load(SCRIPT).exec().unwrap();
        lua
    });
}
```

### Statement Caching

```rust
// ❌ SLOW: Re-parses every call
conn.query_row("SELECT ...", params, |r| ...)?;

// ✅ FAST: Parse once, execute many
let mut stmt = conn.prepare("SELECT ...")?;
stmt.query_row(params, |r| ...)?; // 2-4× faster
```

---

## 📝 License

Research/experimental code. MIT license.
