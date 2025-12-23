//! DuckDB VArrowScalar + LuaJIT FFI Zero-Copy Integration
//!
//! This demonstrates passing Arrow buffer pointers directly to LuaJIT FFI
//! for true zero-copy vectorized UDF operations in DuckDB.
//!
//! Architecture:
//! 1. DuckDB calls VArrowScalar::invoke with a RecordBatch
//! 2. We extract raw pointers from Arrow Float64Array buffers
//! 3. Pass pointers to Lua via LightUserData
//! 4. LuaJIT FFI casts and operates directly on memory
//! 5. Return result as Arrow array (allocated once, written by Lua)

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
// LuaJIT FFI Script - Direct memory access!
// ============================================================================

static LUA_FFI_SCRIPT: &str = r#"
local ffi = require("ffi")

-- Define C types matching our Rust structs
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

-- Per-row function (for baseline comparison)
function distance(x1, y1, x2, y2)
    local dx = x2 - x1
    local dy = y2 - y1
    return math.sqrt(dx*dx + dy*dy)
end

-- FFI batch function - operates directly on Arrow buffer memory!
function distance_ffi_batch(batch_ptr)
    local batch = ffi.cast("DistanceBatch*", batch_ptr)
    local n = tonumber(batch.n)
    local x1 = batch.x1
    local y1 = batch.y1
    local x2 = batch.x2
    local y2 = batch.y2
    local out = batch.out
    local sqrt = math.sqrt
    
    for i = 0, n-1 do
        local dx = x2[i] - x1[i]
        local dy = y2[i] - y1[i]
        out[i] = sqrt(dx*dx + dy*dy)
    end
end

-- Example: Custom threshold filter (writes 1.0 if within threshold, 0.0 otherwise)
function within_distance_ffi_batch(batch_ptr, threshold)
    local batch = ffi.cast("DistanceBatch*", batch_ptr)
    local n = tonumber(batch.n)
    local x1 = batch.x1
    local y1 = batch.y1
    local x2 = batch.x2
    local y2 = batch.y2
    local out = batch.out
    local sqrt = math.sqrt
    local thresh_sq = threshold * threshold
    
    for i = 0, n-1 do
        local dx = x2[i] - x1[i]
        local dy = y2[i] - y1[i]
        local dist_sq = dx*dx + dy*dy
        out[i] = dist_sq <= thresh_sq and 1.0 or 0.0
    end
end

print("LuaJIT FFI + DuckDB integration loaded")
print("JIT status: " .. (jit and jit.status() and "ON" or "OFF"))
"#;

// ============================================================================
// Thread-Local Lua VM with FFI support
// ============================================================================

thread_local! {
    static LUA_VM: RefCell<Lua> = RefCell::new({
        // Must use unsafe_new to enable FFI module
        let lua = unsafe { Lua::unsafe_new() };
        lua.load(LUA_FFI_SCRIPT).exec().expect("Failed to load Lua FFI script");
        lua
    });
}

// ============================================================================
// Batch struct matching the Lua FFI definition
// ============================================================================

#[repr(C)]
struct DistanceBatch {
    x1: *const f64,
    y1: *const f64,
    x2: *const f64,
    y2: *const f64,
    out: *mut f64,
    n: i64,
}

// ============================================================================
// Helper: Get raw pointer from Arrow Float64Array
// ============================================================================

fn get_f64_ptr(array: &Float64Array) -> *const f64 {
    // Arrow arrays store data in a buffer, we can get a raw pointer
    // The values() method returns a slice, we get a pointer to the first element
    array.values().as_ptr()
}

// ============================================================================
// LuaJIT FFI VArrowScalar: Zero-copy batch processing
// ============================================================================

struct LuaFfiDistanceScalar;

impl VArrowScalar for LuaFfiDistanceScalar {
    type State = ();

    fn invoke(
        _state: &Self::State,
        input: RecordBatch,
    ) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let n = input.num_rows();
        
        // Get input arrays
        let x1 = input.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y1 = input.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let x2 = input.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let y2 = input.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Allocate output buffer (will be written by Lua)
        let mut out_buffer: Vec<f64> = vec![0.0; n];
        
        // Create batch struct with raw pointers
        let batch = DistanceBatch {
            x1: get_f64_ptr(x1),
            y1: get_f64_ptr(y1),
            x2: get_f64_ptr(x2),
            y2: get_f64_ptr(y2),
            out: out_buffer.as_mut_ptr(),
            n: n as i64,
        };
        
        // Call Lua FFI function
        LUA_VM.with(|vm| {
            let lua = vm.borrow();
            let func: mlua::Function = lua.globals().get("distance_ffi_batch").unwrap();
            let ptr = LightUserData(&batch as *const DistanceBatch as *mut c_void);
            func.call::<()>(ptr).unwrap();
        });
        
        // Convert to Arrow array (the buffer was already written by Lua)
        Ok(Arc::new(Float64Array::from(out_buffer)))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ],
            DataType::Float64,
        )]
    }
}

// ============================================================================
// Per-row Lua VArrowScalar (for comparison)
// ============================================================================

struct LuaPerRowDistanceScalar;

impl VArrowScalar for LuaPerRowDistanceScalar {
    type State = ();

    fn invoke(
        _state: &Self::State,
        input: RecordBatch,
    ) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let x1 = input.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y1 = input.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let x2 = input.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let y2 = input.column(3).as_any().downcast_ref::<Float64Array>().unwrap();

        let result: Vec<f64> = LUA_VM.with(|vm| {
            let lua = vm.borrow();
            let func: mlua::Function = lua.globals().get("distance").unwrap();
            
            (0..input.num_rows())
                .map(|i| {
                    func.call::<f64>((
                        x1.value(i),
                        y1.value(i),
                        x2.value(i),
                        y2.value(i),
                    )).unwrap()
                })
                .collect()
        });

        Ok(Arc::new(Float64Array::from(result)))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ],
            DataType::Float64,
        )]
    }
}

// ============================================================================
// Pure Rust VArrowScalar (baseline)
// ============================================================================

struct RustDistanceScalar;

impl VArrowScalar for RustDistanceScalar {
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
            vec![
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
                DataType::Float64,
            ],
            DataType::Float64,
        )]
    }
}

// ============================================================================
// Benchmark
// ============================================================================

fn run_benchmark(conn: &Connection, name: &str, query: &str, iterations: usize) -> f64 {
    // Warmup
    for _ in 0..5 {
        let _: f64 = conn.prepare(query).unwrap().query_row([], |row| row.get(0)).unwrap();
    }
    
    let start = Instant::now();
    for _ in 0..iterations {
        let _: f64 = conn.prepare(query).unwrap().query_row([], |row| row.get(0)).unwrap();
    }
    let elapsed = start.elapsed();
    
    let per_iter = elapsed.as_secs_f64() * 1000.0 / iterations as f64;
    println!("  {:<25} {:>8.2} ms/query", name, per_iter);
    per_iter
}

fn main() -> Result<(), Box<dyn Error>> {
    println!("=== DuckDB + LuaJIT FFI Zero-Copy VArrowScalar ===\n");
    
    let conn = Connection::open_in_memory()?;
    
    // Register all three scalar functions
    conn.register_scalar_function::<LuaFfiDistanceScalar>("lua_ffi_distance")?;
    conn.register_scalar_function::<LuaPerRowDistanceScalar>("lua_perrow_distance")?;
    conn.register_scalar_function::<RustDistanceScalar>("rust_distance")?;
    
    // Test with different cross-join sizes
    for n in [50, 100, 200] {
        println!("--- {} × {} entities ({} pairs) ---\n", n, n, n * n);
        
        // Create test tables
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS e1; DROP TABLE IF EXISTS e2;
             CREATE TABLE e1 AS SELECT random()*1000 as x, random()*1000 as y FROM range({});
             CREATE TABLE e2 AS SELECT random()*1000 as x, random()*1000 as y FROM range({});",
            n, n
        ))?;
        
        let iterations = 20;
        
        // Built-in sqrt (baseline)
        let builtin_time = run_benchmark(
            &conn,
            "DuckDB sqrt (built-in)",
            "SELECT sum(sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y))) FROM e1, e2",
            iterations
        );
        
        // Pure Rust VScalar
        let rust_time = run_benchmark(
            &conn,
            "Rust VArrowScalar",
            "SELECT sum(rust_distance(e1.x, e1.y, e2.x, e2.y)) FROM e1, e2",
            iterations
        );
        
        // Lua per-row
        let perrow_time = run_benchmark(
            &conn,
            "Lua Per-Row",
            "SELECT sum(lua_perrow_distance(e1.x, e1.y, e2.x, e2.y)) FROM e1, e2",
            iterations
        );
        
        // LuaJIT FFI batch
        let ffi_time = run_benchmark(
            &conn,
            "LuaJIT FFI Batch",
            "SELECT sum(lua_ffi_distance(e1.x, e1.y, e2.x, e2.y)) FROM e1, e2",
            iterations
        );
        
        let pairs = n * n;
        println!();
        println!("  Per-pair timing (ns):");
        println!("    Built-in:     {:>6.0} ns", builtin_time * 1_000_000.0 / pairs as f64);
        println!("    Rust:         {:>6.0} ns", rust_time * 1_000_000.0 / pairs as f64);
        println!("    Lua Per-Row:  {:>6.0} ns", perrow_time * 1_000_000.0 / pairs as f64);
        println!("    LuaJIT FFI:   {:>6.0} ns", ffi_time * 1_000_000.0 / pairs as f64);
        println!();
        println!("  Speedup:");
        println!("    FFI vs Per-Row:   {:>5.1}×", perrow_time / ffi_time);
        println!("    FFI vs Built-in:  {:>5.1}×", ffi_time / builtin_time);
        println!();
    }
    
    println!("=== Summary ===\n");
    println!("  ┌────────────────────────┬──────────────┬──────────────┐");
    println!("  │ Method                 │ Overhead     │ Use Case     │");
    println!("  ├────────────────────────┼──────────────┼──────────────┤");
    println!("  │ DuckDB Built-in        │ 1.0×         │ Standard SQL │");
    println!("  │ Rust VArrowScalar      │ ~1×          │ Engine code  │");
    println!("  │ LuaJIT FFI Batch       │ ~2-3×        │ Mod vectors  │");
    println!("  │ Lua Per-Row            │ ~5-10×       │ Simple mods  │");
    println!("  └────────────────────────┴──────────────┴──────────────┘");
    println!();
    println!("  LuaJIT FFI eliminates per-row call overhead!");
    println!("  Modders can write vectorized Lua code with near-native perf.");
    
    Ok(())
}
