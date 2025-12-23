//! DuckDB VScalar + Lua UDF Integration
//!
//! This demonstrates a real DuckDB scalar function that calls into Lua.
//! Uses VArrowScalar for easy numeric type handling via Arrow arrays.
//!
//! Key insight: DuckDB may call VScalar on multiple threads, so we use
//! thread-local Lua VMs that are lazily initialized with the script.
//!
//! Example: SELECT * FROM e1, e2 WHERE lua_distance(e1.x, e1.y, e2.x, e2.y) < 50

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
// Thread-Local Lua VM (using mlua/LuaJIT for performance)
// ============================================================================

// The Lua script is stored in a static so all threads can access it
static LUA_SCRIPT: &str = r#"
    function distance(x1, y1, x2, y2)
        local dx = x2 - x1
        local dy = y2 - y1
        return math.sqrt(dx*dx + dy*dy)
    end
"#;

thread_local! {
    static LUA_VM: RefCell<mlua::Lua> = RefCell::new({
        let lua = mlua::Lua::new();
        lua.load(LUA_SCRIPT).exec().expect("Failed to load Lua script");
        lua
    });
}

fn call_lua_distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    LUA_VM.with(|vm| {
        let lua = vm.borrow();
        let func: mlua::Function = lua.globals().get("distance").expect("distance not found");
        func.call::<f64>((x1, y1, x2, y2)).expect("distance call failed")
    })
}

// ============================================================================
// DuckDB VArrowScalar: lua_distance(x1, y1, x2, y2) -> DOUBLE
// ============================================================================

struct LuaDistanceScalar;

impl VArrowScalar for LuaDistanceScalar {
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
                call_lua_distance(
                    x1.value(i),
                    y1.value(i),
                    x2.value(i),
                    y2.value(i),
                )
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
// Pure Rust VArrowScalar for comparison
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
// Main
// ============================================================================

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB VScalar + Lua UDF Integration ===\n");

    // Create DuckDB connection
    let conn = Connection::open_in_memory()?;

    // Register VScalar functions
    conn.register_scalar_function::<LuaDistanceScalar>("lua_distance")?;
    conn.register_scalar_function::<RustDistanceScalar>("rust_distance")?;

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

    println!("Created table with {} entities\n", entity_count);

    // === Test 1: Simple UDF call ===
    println!("=== Test 1: Simple UDF Calls ===\n");
    
    let start = Instant::now();
    let mut stmt = conn.prepare("SELECT lua_distance(0.0, 0.0, 3.0, 4.0)")?;
    let result: f64 = stmt.query_row([], |row| row.get(0))?;
    let lua_simple = start.elapsed();
    println!("  lua_distance(0, 0, 3, 4) = {} ({:.2} µs)", result, lua_simple.as_secs_f64() * 1_000_000.0);

    let start = Instant::now();
    let mut stmt = conn.prepare("SELECT rust_distance(0.0, 0.0, 3.0, 4.0)")?;
    let result: f64 = stmt.query_row([], |row| row.get(0))?;
    let rust_simple = start.elapsed();
    println!("  rust_distance(0, 0, 3, 4) = {} ({:.2} µs)", result, rust_simple.as_secs_f64() * 1_000_000.0);
    println!();

    // === Test 2: Cross-join with UDF in WHERE ===
    println!("=== Test 2: Cross-Join with UDF in WHERE ===\n");
    
    // Warm up
    conn.execute("SELECT COUNT(*) FROM entities e1, entities e2 WHERE lua_distance(e1.x, e1.y, e2.x, e2.y) < 50", [])?;

    // Lua UDF
    let start = Instant::now();
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM entities e1, entities e2 WHERE lua_distance(e1.x, e1.y, e2.x, e2.y) < 50"
    )?;
    let lua_count: i64 = stmt.query_row([], |row| row.get(0))?;
    let lua_query = start.elapsed();

    // Rust UDF
    let start = Instant::now();
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM entities e1, entities e2 WHERE rust_distance(e1.x, e1.y, e2.x, e2.y) < 50"
    )?;
    let rust_count: i64 = stmt.query_row([], |row| row.get(0))?;
    let rust_query = start.elapsed();

    // Built-in sqrt
    let start = Instant::now();
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM entities e1, entities e2 
         WHERE sqrt((e2.x - e1.x) * (e2.x - e1.x) + (e2.y - e1.y) * (e2.y - e1.y)) < 50"
    )?;
    let builtin_count: i64 = stmt.query_row([], |row| row.get(0))?;
    let builtin_query = start.elapsed();

    let total_pairs = entity_count * entity_count;
    
    println!("  100×100 cross-join ({} pairs):", total_pairs);
    println!();
    println!("  {:15} {:>10} {:>12} {:>10}", "Method", "Time", "Per Pair", "Matches");
    println!("  {:15} {:>10} {:>12} {:>10}", "------", "----", "--------", "-------");
    println!("  {:15} {:>8.2} ms {:>9.1} ns {:>10}", 
        "Built-in sqrt", 
        builtin_query.as_secs_f64() * 1000.0,
        builtin_query.as_nanos() as f64 / total_pairs as f64,
        builtin_count);
    println!("  {:15} {:>8.2} ms {:>9.1} ns {:>10}", 
        "Rust VScalar", 
        rust_query.as_secs_f64() * 1000.0,
        rust_query.as_nanos() as f64 / total_pairs as f64,
        rust_count);
    println!("  {:15} {:>8.2} ms {:>9.1} ns {:>10}", 
        "Lua VScalar", 
        lua_query.as_secs_f64() * 1000.0,
        lua_query.as_nanos() as f64 / total_pairs as f64,
        lua_count);
    println!();

    // === Test 3: Return actual data ===
    println!("=== Test 3: Return Nearby Pairs ===\n");
    
    let start = Instant::now();
    let mut stmt = conn.prepare(
        "SELECT e1.id, e2.id, lua_distance(e1.x, e1.y, e2.x, e2.y) as dist
         FROM entities e1, entities e2 
         WHERE lua_distance(e1.x, e1.y, e2.x, e2.y) < 50
         ORDER BY dist
         LIMIT 10"
    )?;
    let pairs: Vec<(i64, i64, f64)> = stmt.query_map([], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })?.collect::<Result<Vec<_>, _>>()?;
    let return_query = start.elapsed();

    println!("  Top 10 closest pairs ({:.2} ms):", return_query.as_secs_f64() * 1000.0);
    for (e1, e2, dist) in pairs.iter().take(5) {
        println!("    Entity {} ↔ Entity {}: {:.2}", e1, e2, dist);
    }
    println!("    ...");
    println!();

    // === Frame Budget Analysis ===
    println!("=== Frame Budget Analysis ===\n");
    
    let tick_budget_ms = 16.67;
    let lua_pct = (lua_query.as_secs_f64() * 1000.0 / tick_budget_ms) * 100.0;
    let rust_pct = (rust_query.as_secs_f64() * 1000.0 / tick_budget_ms) * 100.0;
    let builtin_pct = (builtin_query.as_secs_f64() * 1000.0 / tick_budget_ms) * 100.0;

    println!("  100×100 cross-join budget consumption:");
    println!("    Built-in sqrt: {:>5.1}%", builtin_pct);
    println!("    Rust VScalar:  {:>5.1}%", rust_pct);
    println!("    Lua VScalar:   {:>5.1}%", lua_pct);
    println!();

    // Extrapolate max entity count
    let lua_per_pair_ns = lua_query.as_nanos() as f64 / total_pairs as f64;
    let budget_10pct_ns = tick_budget_ms * 0.10 * 1_000_000.0;
    let max_pairs = budget_10pct_ns / lua_per_pair_ns;
    let max_entities = (max_pairs.sqrt()) as i32;
    
    println!("  At 10% budget with Lua UDF:");
    println!("    Max pairs:    {:>6.0}", max_pairs);
    println!("    Max entities: {:>6} ({}×{} cross-join)", max_entities, max_entities, max_entities);
    println!();

    // === Verdict ===
    println!("=== VERDICT ===\n");
    
    let overhead_vs_builtin = lua_query.as_secs_f64() / builtin_query.as_secs_f64();
    let overhead_vs_rust = lua_query.as_secs_f64() / rust_query.as_secs_f64();
    
    if overhead_vs_builtin < 5.0 {
        println!("  ✅ Lua UDF in DuckDB VScalar is VIABLE!");
        println!();
        println!("  Overhead vs built-in: {:.1}×", overhead_vs_builtin);
        println!("  Overhead vs Rust UDF: {:.1}×", overhead_vs_rust);
        println!();
        println!("  Use cases:");
        println!("    • Mod-defined scoring functions");
        println!("    • Complex business logic in SQL");
        println!("    • Dynamic behavior without recompilation");
    } else {
        println!("  ⚠️ Lua UDF overhead is significant: {:.1}× vs built-in", overhead_vs_builtin);
        println!();
        println!("  Consider:");
        println!("    • Pre-computing Lua results in a temp table");
        println!("    • Using Rust VScalar for hot paths");
        println!("    • Limiting cross-join sizes");
    }
    
    println!();
    println!("=== Architecture ===");
    println!();
    println!("  ┌────────────────────────────────────────────────────────────┐");
    println!("  │  SQL Query                                                 │");
    println!("  │  SELECT * FROM e1, e2                                      │");
    println!("  │  WHERE lua_distance(e1.x, e1.y, e2.x, e2.y) < range        │");
    println!("  └───────────────────────────┬────────────────────────────────┘");
    println!("                              │");
    println!("                              ▼");
    println!("  ┌────────────────────────────────────────────────────────────┐");
    println!("  │  DuckDB Query Executor                                     │");
    println!("  │  • Cross-join produces candidate pairs                     │");
    println!("  │  • Calls lua_distance VScalar for filtering                │");
    println!("  │  • Vectorized: processes ~2048 rows per call               │");
    println!("  └───────────────────────────┬────────────────────────────────┘");
    println!("                              │");
    println!("                              ▼");
    println!("  ┌────────────────────────────────────────────────────────────┐");
    println!("  │  Rust VArrowScalar (LuaDistanceScalar)                     │");
    println!("  │  • Receives Arrow RecordBatch with (x1, y1, x2, y2)        │");
    println!("  │  • For each row: call thread-local Lua                     │");
    println!("  │  • Return Arrow Float64Array to DuckDB                     │");
    println!("  └───────────────────────────┬────────────────────────────────┘");
    println!("                              │");
    println!("                              ▼");
    println!("  ┌────────────────────────────────────────────────────────────┐");
    println!("  │  Thread-Local LuaJIT VM                                    │");
    println!("  │  • Lazily initialized per DuckDB worker thread             │");
    println!("  │  • JIT-compiled Lua functions                              │");
    println!("  │  • ~{:.0} ns per distance() call                            │", lua_per_pair_ns);
    println!("  └────────────────────────────────────────────────────────────┘");

    Ok(())
}
