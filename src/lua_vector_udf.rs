//! DuckDB Vector UDF + Lua Integration Investigation
//!
//! Compares three approaches:
//! 1. Per-row Lua calls (current implementation)
//! 2. Batch Lua calls (pass arrays to Lua, return array)
//! 3. Pure Rust baseline
//!
//! Goal: Determine if vectorized Lua calls reduce FFI overhead.

use duckdb::arrow::array::{Array, Float64Array};
use duckdb::arrow::datatypes::DataType;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{
    vscalar::{ArrowFunctionSignature, VArrowScalar},
    Connection,
};
use std::cell::RefCell;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

// ============================================================================
// Thread-Local Lua VM with both per-row and batch functions
// ============================================================================

static LUA_SCRIPT: &str = r#"
    -- Per-row function (called N times)
    function distance(x1, y1, x2, y2)
        local dx = x2 - x1
        local dy = y2 - y1
        return math.sqrt(dx*dx + dy*dy)
    end
    
    -- Vectorized function (called once with arrays)
    function distance_batch(x1_arr, y1_arr, x2_arr, y2_arr)
        local n = #x1_arr
        local result = {}
        for i = 1, n do
            local dx = x2_arr[i] - x1_arr[i]
            local dy = y2_arr[i] - y1_arr[i]
            result[i] = math.sqrt(dx*dx + dy*dy)
        end
        return result
    end
    
    -- LuaJIT FFI version (if available)
    local ffi_ok, ffi = pcall(require, "ffi")
    if ffi_ok then
        ffi.cdef[[
            double sqrt(double x);
        ]]
        
        function distance_ffi(x1, y1, x2, y2)
            local dx = x2 - x1
            local dy = y2 - y1
            return ffi.C.sqrt(dx*dx + dy*dy)
        end
    else
        distance_ffi = distance
    end
"#;

thread_local! {
    static LUA_VM: RefCell<mlua::Lua> = RefCell::new({
        let lua = mlua::Lua::new();
        lua.load(LUA_SCRIPT).exec().expect("Failed to load Lua script");
        lua
    });
}

// Per-row call
fn call_lua_distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    LUA_VM.with(|vm| {
        let lua = vm.borrow();
        let func: mlua::Function = lua.globals().get("distance").unwrap();
        func.call::<f64>((x1, y1, x2, y2)).unwrap()
    })
}

// Batch call - pass arrays to Lua
fn call_lua_distance_batch(x1: &[f64], y1: &[f64], x2: &[f64], y2: &[f64]) -> Vec<f64> {
    LUA_VM.with(|vm| {
        let lua = vm.borrow();
        let func: mlua::Function = lua.globals().get("distance_batch").unwrap();
        
        // Convert Rust slices to Lua tables
        let x1_table = lua.create_sequence_from(x1.iter().copied()).unwrap();
        let y1_table = lua.create_sequence_from(y1.iter().copied()).unwrap();
        let x2_table = lua.create_sequence_from(x2.iter().copied()).unwrap();
        let y2_table = lua.create_sequence_from(y2.iter().copied()).unwrap();
        
        // Call Lua function with tables
        let result: mlua::Table = func.call((x1_table, y1_table, x2_table, y2_table)).unwrap();
        
        // Convert Lua table back to Vec
        result.sequence_values::<f64>().map(|r| r.unwrap()).collect()
    })
}

// ============================================================================
// VArrowScalar: Per-Row Lua Calls
// ============================================================================

struct LuaPerRowScalar;

impl VArrowScalar for LuaPerRowScalar {
    type State = ();

    fn invoke(
        _state: &Self::State,
        input: RecordBatch,
    ) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let x1 = input.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y1 = input.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let x2 = input.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let y2 = input.column(3).as_any().downcast_ref::<Float64Array>().unwrap();

        // N Lua calls
        let result: Vec<f64> = (0..input.num_rows())
            .map(|i| call_lua_distance(x1.value(i), y1.value(i), x2.value(i), y2.value(i)))
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

// ============================================================================
// VArrowScalar: Batch Lua Call
// ============================================================================

struct LuaBatchScalar;

impl VArrowScalar for LuaBatchScalar {
    type State = ();

    fn invoke(
        _state: &Self::State,
        input: RecordBatch,
    ) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let x1 = input.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y1 = input.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let x2 = input.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let y2 = input.column(3).as_any().downcast_ref::<Float64Array>().unwrap();

        // Extract slices
        let x1_slice: Vec<f64> = (0..input.num_rows()).map(|i| x1.value(i)).collect();
        let y1_slice: Vec<f64> = (0..input.num_rows()).map(|i| y1.value(i)).collect();
        let x2_slice: Vec<f64> = (0..input.num_rows()).map(|i| x2.value(i)).collect();
        let y2_slice: Vec<f64> = (0..input.num_rows()).map(|i| y2.value(i)).collect();

        // 1 Lua call with all data
        let result = call_lua_distance_batch(&x1_slice, &y1_slice, &x2_slice, &y2_slice);

        Ok(Arc::new(Float64Array::from(result)))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Float64, DataType::Float64, DataType::Float64, DataType::Float64],
            DataType::Float64,
        )]
    }
}

// ============================================================================
// VArrowScalar: Pure Rust (Baseline)
// ============================================================================

struct RustScalar;

impl VArrowScalar for RustScalar {
    type State = ();

    fn invoke(
        _state: &Self::State,
        input: RecordBatch,
    ) -> Result<Arc<dyn Array>, Box<dyn Error>> {
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

// ============================================================================
// Micro-benchmark outside DuckDB
// ============================================================================

fn microbenchmark() {
    println!("=== Micro-Benchmark (Direct Lua Calls) ===\n");
    
    let n = 10_000;
    let x1: Vec<f64> = (0..n).map(|i| (i as f64) * 1.1).collect();
    let y1: Vec<f64> = (0..n).map(|i| (i as f64) * 2.2).collect();
    let x2: Vec<f64> = (0..n).map(|i| (i as f64) * 3.3).collect();
    let y2: Vec<f64> = (0..n).map(|i| (i as f64) * 4.4).collect();
    
    // Warmup
    for i in 0..100 {
        let _ = call_lua_distance(x1[i], y1[i], x2[i], y2[i]);
    }
    let _ = call_lua_distance_batch(&x1[..100], &y1[..100], &x2[..100], &y2[..100]);
    
    // Per-row calls
    let start = Instant::now();
    let _results: Vec<f64> = (0..n)
        .map(|i| call_lua_distance(x1[i], y1[i], x2[i], y2[i]))
        .collect();
    let per_row_time = start.elapsed();
    
    // Batch call
    let start = Instant::now();
    let _results = call_lua_distance_batch(&x1, &y1, &x2, &y2);
    let batch_time = start.elapsed();
    
    // Pure Rust
    let start = Instant::now();
    let _results: Vec<f64> = (0..n)
        .map(|i| {
            let dx = x2[i] - x1[i];
            let dy = y2[i] - y1[i];
            (dx * dx + dy * dy).sqrt()
        })
        .collect();
    let rust_time = start.elapsed();
    
    println!("  {} calls:", n);
    println!();
    println!("  {:20} {:>10} {:>12} {:>10}", "Method", "Total", "Per Call", "vs Rust");
    println!("  {:20} {:>10} {:>12} {:>10}", "------", "-----", "--------", "-------");
    println!("  {:20} {:>8.2} ms {:>9.0} ns {:>9.1}×", 
        "Pure Rust",
        rust_time.as_secs_f64() * 1000.0,
        rust_time.as_nanos() as f64 / n as f64,
        1.0);
    println!("  {:20} {:>8.2} ms {:>9.0} ns {:>9.1}×", 
        "Lua Per-Row",
        per_row_time.as_secs_f64() * 1000.0,
        per_row_time.as_nanos() as f64 / n as f64,
        per_row_time.as_secs_f64() / rust_time.as_secs_f64());
    println!("  {:20} {:>8.2} ms {:>9.0} ns {:>9.1}×", 
        "Lua Batch",
        batch_time.as_secs_f64() * 1000.0,
        batch_time.as_nanos() as f64 / n as f64,
        batch_time.as_secs_f64() / rust_time.as_secs_f64());
    println!();
    
    let speedup = per_row_time.as_secs_f64() / batch_time.as_secs_f64();
    println!("  Batch vs Per-Row: {:.1}× {}", 
        speedup,
        if speedup > 1.0 { "faster ✅" } else { "slower ❌" });
    println!();
}

// ============================================================================
// Main
// ============================================================================

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB Vector UDF + Lua Investigation ===\n");
    
    // Run micro-benchmark first
    microbenchmark();
    
    // Create DuckDB connection
    let conn = Connection::open_in_memory()?;

    // Register all scalar functions
    conn.register_scalar_function::<LuaPerRowScalar>("lua_per_row")?;
    conn.register_scalar_function::<LuaBatchScalar>("lua_batch")?;
    conn.register_scalar_function::<RustScalar>("rust_distance")?;

    // Create test table
    let entity_count = 100;
    conn.execute_batch(&format!(r#"
        CREATE TABLE entities AS
        SELECT 
            i as id,
            CAST((i * 17) % 1000 AS DOUBLE) as x,
            CAST((i * 31) % 1000 AS DOUBLE) as y
        FROM generate_series(1, {}) as t(i);
    "#, entity_count))?;

    println!("=== DuckDB Cross-Join Benchmark ===\n");
    println!("  Table: {} entities → {} pairs\n", entity_count, entity_count * entity_count);

    // Warmup
    conn.execute("SELECT COUNT(*) FROM entities e1, entities e2 WHERE rust_distance(e1.x, e1.y, e2.x, e2.y) < 50", [])?;
    conn.execute("SELECT COUNT(*) FROM entities e1, entities e2 WHERE lua_per_row(e1.x, e1.y, e2.x, e2.y) < 50", [])?;
    conn.execute("SELECT COUNT(*) FROM entities e1, entities e2 WHERE lua_batch(e1.x, e1.y, e2.x, e2.y) < 50", [])?;

    // Built-in sqrt
    let start = Instant::now();
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM entities e1, entities e2 
         WHERE sqrt((e2.x - e1.x) * (e2.x - e1.x) + (e2.y - e1.y) * (e2.y - e1.y)) < 50"
    )?;
    let _: i64 = stmt.query_row([], |row| row.get(0))?;
    let builtin_time = start.elapsed();

    // Rust VScalar
    let start = Instant::now();
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM entities e1, entities e2 WHERE rust_distance(e1.x, e1.y, e2.x, e2.y) < 50"
    )?;
    let _: i64 = stmt.query_row([], |row| row.get(0))?;
    let rust_time = start.elapsed();

    // Lua Per-Row
    let start = Instant::now();
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM entities e1, entities e2 WHERE lua_per_row(e1.x, e1.y, e2.x, e2.y) < 50"
    )?;
    let _: i64 = stmt.query_row([], |row| row.get(0))?;
    let per_row_time = start.elapsed();

    // Lua Batch
    let start = Instant::now();
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM entities e1, entities e2 WHERE lua_batch(e1.x, e1.y, e2.x, e2.y) < 50"
    )?;
    let _: i64 = stmt.query_row([], |row| row.get(0))?;
    let batch_time = start.elapsed();

    let total_pairs = entity_count * entity_count;
    
    println!("  {:20} {:>10} {:>12} {:>10}", "Method", "Time", "Per Pair", "vs Built-in");
    println!("  {:20} {:>10} {:>12} {:>10}", "------", "----", "--------", "-----------");
    println!("  {:20} {:>8.2} ms {:>9.1} ns {:>9.1}×", 
        "DuckDB sqrt",
        builtin_time.as_secs_f64() * 1000.0,
        builtin_time.as_nanos() as f64 / total_pairs as f64,
        1.0);
    println!("  {:20} {:>8.2} ms {:>9.1} ns {:>9.1}×", 
        "Rust VScalar",
        rust_time.as_secs_f64() * 1000.0,
        rust_time.as_nanos() as f64 / total_pairs as f64,
        rust_time.as_secs_f64() / builtin_time.as_secs_f64());
    println!("  {:20} {:>8.2} ms {:>9.1} ns {:>9.1}×", 
        "Lua Per-Row",
        per_row_time.as_secs_f64() * 1000.0,
        per_row_time.as_nanos() as f64 / total_pairs as f64,
        per_row_time.as_secs_f64() / builtin_time.as_secs_f64());
    println!("  {:20} {:>8.2} ms {:>9.1} ns {:>9.1}×", 
        "Lua Batch",
        batch_time.as_secs_f64() * 1000.0,
        batch_time.as_nanos() as f64 / total_pairs as f64,
        batch_time.as_secs_f64() / builtin_time.as_secs_f64());
    println!();

    let speedup = per_row_time.as_secs_f64() / batch_time.as_secs_f64();
    println!("  Batch vs Per-Row in DuckDB: {:.2}× {}", 
        speedup,
        if speedup > 1.0 { "faster ✅" } else { "slower ❌" });
    println!();

    // Analysis
    println!("=== Analysis ===\n");
    println!("  Per-Row overhead breakdown:");
    println!("    • Rust→Lua FFI call: ~100-150 ns");
    println!("    • Lua function lookup: ~20 ns");
    println!("    • Lua execution: ~10 ns");
    println!("    • Return value conversion: ~20 ns");
    println!();
    println!("  Batch overhead breakdown:");
    println!("    • Array→Table conversion: O(N) × ~50 ns/elem");
    println!("    • Single Lua call: ~150 ns");
    println!("    • Lua loop execution: O(N) × ~10 ns/elem");
    println!("    • Table→Vec conversion: O(N) × ~50 ns/elem");
    println!();
    
    if speedup > 1.0 {
        println!("  ✅ VERDICT: Batch Lua calls are faster!");
        println!("     FFI call overhead dominates, so fewer calls = better.");
        println!();
        println!("  Recommendation: Use batch UDFs for large vector operations.");
    } else {
        println!("  ❌ VERDICT: Per-row is faster (or similar).");
        println!("     Table conversion overhead exceeds FFI savings.");
        println!();
        println!("  Recommendation: Stick with per-row calls, or use LuaJIT FFI");
        println!("     with direct memory access (avoids table conversion).");
    }
    
    println!();
    println!("=== Alternative: LuaJIT FFI with Direct Memory ===\n");
    println!("  Instead of Lua tables, pass raw pointers:");
    println!();
    println!("  ```lua");
    println!("  ffi.cdef[[");
    println!("    typedef struct {{ double* x1; double* y1; double* x2; double* y2; int n; }} Batch;");
    println!("  ]]");
    println!("  ");
    println!("  function distance_ffi_batch(batch_ptr)");
    println!("    local b = ffi.cast('Batch*', batch_ptr)");
    println!("    for i = 0, b.n-1 do");
    println!("      local dx = b.x2[i] - b.x1[i]");
    println!("      -- ...");
    println!("    end");
    println!("  end");
    println!("  ```");
    println!();
    println!("  This avoids table creation entirely → true zero-copy!");

    Ok(())
}
