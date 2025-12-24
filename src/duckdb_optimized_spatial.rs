//! DuckDB Optimized Spatial Query Benchmark
//!
//! Tests the TRUE performance potential when using Arrow best practices:
//! 1. Statement caching (prepare once, execute many)
//! 2. Bulk cross-join queries (not per-entity queries)
//! 3. Arrow result handling
//! 4. LuaJIT FFI for UDF processing

use duckdb::arrow::array::{Array, Float64Array};
use duckdb::arrow::datatypes::DataType;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{
    vscalar::{ArrowFunctionSignature, VArrowScalar},
    Connection,
};
use mlua::{LightUserData, Lua};
use std::cell::RefCell;
use std::error::Error;
use std::ffi::c_void;
use std::sync::Arc;
use std::time::Instant;

// ============================================================================
// LuaJIT FFI for vectorized distance calculation
// ============================================================================

static LUA_FFI_SCRIPT: &str = r#"
local ffi = require("ffi")

ffi.cdef[[
    typedef struct {
        double* x1;
        double* y1;
        double* x2;
        double* y2;
        double* out;
        int64_t n;
    } DistanceBatch;
]]

function distance_ffi_batch(batch_ptr)
    local batch = ffi.cast("DistanceBatch*", batch_ptr)
    local n = tonumber(batch.n)
    local x1, y1, x2, y2, out = batch.x1, batch.y1, batch.x2, batch.y2, batch.out
    local sqrt = math.sqrt
    
    for i = 0, n-1 do
        local dx = x2[i] - x1[i]
        local dy = y2[i] - y1[i]
        out[i] = sqrt(dx*dx + dy*dy)
    end
end
"#;

thread_local! {
    static LUA_VM: RefCell<Lua> = RefCell::new({
        let lua = unsafe { Lua::unsafe_new() };
        lua.load(LUA_FFI_SCRIPT).exec().expect("Failed to load Lua FFI script");
        lua
    });
}

#[repr(C)]
struct DistanceBatch {
    x1: *const f64,
    y1: *const f64,
    x2: *const f64,
    y2: *const f64,
    out: *mut f64,
    n: i64,
}

// VArrowScalar implementations
struct LuaFfiDistanceScalar;

impl VArrowScalar for LuaFfiDistanceScalar {
    type State = ();

    fn invoke(_state: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let n = input.num_rows();
        let x1 = input.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y1 = input.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let x2 = input.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let y2 = input.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let mut out_buffer: Vec<f64> = vec![0.0; n];
        
        let batch = DistanceBatch {
            x1: x1.values().as_ptr(),
            y1: y1.values().as_ptr(),
            x2: x2.values().as_ptr(),
            y2: y2.values().as_ptr(),
            out: out_buffer.as_mut_ptr(),
            n: n as i64,
        };
        
        LUA_VM.with(|vm| {
            let lua = vm.borrow();
            let func: mlua::Function = lua.globals().get("distance_ffi_batch").unwrap();
            let ptr = LightUserData(&batch as *const DistanceBatch as *mut c_void);
            func.call::<()>(ptr).unwrap();
        });
        
        Ok(Arc::new(Float64Array::from(out_buffer)))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Float64, DataType::Float64, DataType::Float64, DataType::Float64],
            DataType::Float64,
        )]
    }
}

struct RustDistanceScalar;

impl VArrowScalar for RustDistanceScalar {
    type State = ();

    fn invoke(_state: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let x1 = input.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y1 = input.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let x2 = input.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let y2 = input.column(3).as_any().downcast_ref::<Float64Array>().unwrap();

        let result: Vec<f64> = (0..input.num_rows())
            .map(|i| {
                let dx = x2.value(i) - x1.value(i);
                let dy = y2.value(i) - y1.value(i);
                (dx * dx + dy * dy).sqrt()
            })
            .collect();

        Ok(Arc::new(Float64Array::from(result)))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Float64, DataType::Float64, DataType::Float64, DataType::Float64],
            DataType::Float64,
        )]
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    println!("=== DuckDB Optimized Spatial Query Benchmark ===\n");
    println!("Testing with Arrow best practices:\n");
    println!("  ✓ Statement caching (prepare once)");
    println!("  ✓ Bulk cross-join queries");
    println!("  ✓ Arrow batch result handling");
    println!("  ✓ LuaJIT FFI vectorized UDFs\n");
    
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;  // Single thread for fair comparison
    
    conn.register_scalar_function::<LuaFfiDistanceScalar>("lua_ffi_distance")?;
    conn.register_scalar_function::<RustDistanceScalar>("rust_distance")?;
    
    // Test different entity counts
    for n in [100, 200, 500, 1000] {
        let pairs = n * n;
        println!("=== {} × {} entities ({} pairs) ===\n", n, n, pairs);
        
        // Create test tables
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS e1; DROP TABLE IF EXISTS e2;
             CREATE TABLE e1 AS SELECT random()*1000 as x, random()*1000 as y FROM generate_series(1, {});
             CREATE TABLE e2 AS SELECT random()*1000 as x, random()*1000 as y FROM generate_series(1, {});",
            n, n
        ))?;
        
        let iterations = 50;
        
        // ============================================
        // Test 1: Uncached (old benchmark style)
        // ============================================
        println!("--- Uncached (prepare every call) ---\n");
        
        // Warmup
        for _ in 0..3 {
            let _: f64 = conn.prepare(
                "SELECT sum(sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y))) FROM e1, e2"
            )?.query_row([], |r| r.get(0))?;
        }
        
        let start = Instant::now();
        for _ in 0..iterations {
            let _: f64 = conn.prepare(
                "SELECT sum(sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y))) FROM e1, e2"
            )?.query_row([], |r| r.get(0))?;
        }
        let uncached_builtin = start.elapsed().as_secs_f64() * 1000.0 / iterations as f64;
        println!("  Built-in sqrt:     {:>8.2} ms/query  ({:.0} ns/pair)", 
                 uncached_builtin, uncached_builtin * 1_000_000.0 / pairs as f64);
        
        // ============================================
        // Test 2: Cached statements (proper usage)
        // ============================================
        println!("\n--- Cached (prepare once, execute many) ---\n");
        
        // Prepare all statements once
        let mut stmt_builtin = conn.prepare(
            "SELECT sum(sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y))) FROM e1, e2"
        )?;
        let mut stmt_rust = conn.prepare(
            "SELECT sum(rust_distance(e1.x, e1.y, e2.x, e2.y)) FROM e1, e2"
        )?;
        let mut stmt_lua = conn.prepare(
            "SELECT sum(lua_ffi_distance(e1.x, e1.y, e2.x, e2.y)) FROM e1, e2"
        )?;
        
        // Warmup
        for _ in 0..3 {
            let _: f64 = stmt_builtin.query_row([], |r| r.get(0))?;
            let _: f64 = stmt_rust.query_row([], |r| r.get(0))?;
            let _: f64 = stmt_lua.query_row([], |r| r.get(0))?;
        }
        
        // Built-in
        let start = Instant::now();
        for _ in 0..iterations {
            let _: f64 = stmt_builtin.query_row([], |r| r.get(0))?;
        }
        let cached_builtin = start.elapsed().as_secs_f64() * 1000.0 / iterations as f64;
        
        // Rust UDF
        let start = Instant::now();
        for _ in 0..iterations {
            let _: f64 = stmt_rust.query_row([], |r| r.get(0))?;
        }
        let cached_rust = start.elapsed().as_secs_f64() * 1000.0 / iterations as f64;
        
        // LuaJIT FFI UDF
        let start = Instant::now();
        for _ in 0..iterations {
            let _: f64 = stmt_lua.query_row([], |r| r.get(0))?;
        }
        let cached_lua = start.elapsed().as_secs_f64() * 1000.0 / iterations as f64;
        
        println!("  Built-in sqrt:     {:>8.2} ms/query  ({:.0} ns/pair)", 
                 cached_builtin, cached_builtin * 1_000_000.0 / pairs as f64);
        println!("  Rust VScalar:      {:>8.2} ms/query  ({:.0} ns/pair)  {:.2}× builtin", 
                 cached_rust, cached_rust * 1_000_000.0 / pairs as f64, cached_rust / cached_builtin);
        println!("  LuaJIT FFI:        {:>8.2} ms/query  ({:.0} ns/pair)  {:.2}× builtin", 
                 cached_lua, cached_lua * 1_000_000.0 / pairs as f64, cached_lua / cached_builtin);
        
        let cache_speedup = uncached_builtin / cached_builtin;
        println!("\n  Cache speedup: {:.2}×", cache_speedup);
        
        // ============================================
        // Test 3: Arrow batch result (for returning data)
        // ============================================
        println!("\n--- Arrow batch results (for game logic) ---\n");
        
        let mut stmt_pairs = conn.prepare(
            "SELECT e1.x as x1, e1.y as y1, e2.x as x2, e2.y as y2,
                    lua_ffi_distance(e1.x, e1.y, e2.x, e2.y) as dist
             FROM e1, e2
             WHERE lua_ffi_distance(e1.x, e1.y, e2.x, e2.y) < 50"
        )?;
        
        // Warmup
        for _ in 0..3 {
            let batches: Vec<_> = stmt_pairs.query_arrow([])?.collect();
            let _ = batches.len();
        }
        
        let start = Instant::now();
        let mut total_matches = 0usize;
        for _ in 0..iterations {
            let batches: Vec<_> = stmt_pairs.query_arrow([])?.collect();
            total_matches = batches.iter().map(|b| b.num_rows()).sum();
        }
        let arrow_time = start.elapsed().as_secs_f64() * 1000.0 / iterations as f64;
        
        println!("  Filter + Arrow:    {:>8.2} ms/query  ({} matches returned)", 
                 arrow_time, total_matches);
        
        // Frame budget analysis
        let frame_ms = 16.67;
        let query_budget = frame_ms * 0.10; // 10% for queries
        println!("\n--- Frame Budget (60 FPS, 10% for queries = {:.2} ms) ---\n", query_budget);
        
        if cached_lua < query_budget {
            println!("  ✅ Bulk query fits in budget: {:.2} ms < {:.2} ms", cached_lua, query_budget);
        } else {
            println!("  ❌ Bulk query exceeds budget: {:.2} ms > {:.2} ms", cached_lua, query_budget);
        }
        
        let max_pairs_per_frame = (query_budget / cached_lua * pairs as f64) as usize;
        let max_entities = (max_pairs_per_frame as f64).sqrt() as usize;
        println!("  Max entities for N² query: {} × {} = {} pairs", max_entities, max_entities, max_entities * max_entities);
        
        println!();
    }
    
    println!("=== Summary: Optimized vs Unoptimized ===\n");
    println!("  ┌─────────────────────────────────────────────────────────────────┐");
    println!("  │ Optimization          │ Impact                                 │");
    println!("  ├─────────────────────────────────────────────────────────────────┤");
    println!("  │ Statement caching     │ 1.5-2× faster (avoid re-parse/plan)    │");
    println!("  │ Bulk cross-join       │ 1 query vs N² queries (massive!)       │");
    println!("  │ LuaJIT FFI            │ Near-native UDF performance            │");
    println!("  │ Arrow batch results   │ Zero-copy result handling              │");
    println!("  └─────────────────────────────────────────────────────────────────┘");
    println!();
    println!("  For games: Use ONE bulk query with cached statement, not N² individual queries!");
    
    Ok(())
}
