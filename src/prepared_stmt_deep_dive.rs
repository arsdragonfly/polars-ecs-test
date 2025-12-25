//! Prepared Statement Caching Deep Dive
//! 
//! Where does the ~60-100µs per-query overhead come from?
//! - Query parsing? (prepared statements cache this)
//! - Query planning? (should be cached too)
//! - Execution setup?
//! - Result materialization?
//! - Rust FFI?

use duckdb::{Connection, Statement, Arrow};
use duckdb::arrow::array::Float64Array;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Prepared Statement Caching Deep Dive ===\n");

    let n = 5000;
    let conn = Connection::open_in_memory()?;
    
    // Create test data
    conn.execute_batch(&format!(
        "CREATE TABLE pairs AS 
         SELECT i as id,
                [(i * 17) % 1000, (i * 23) % 1000]::DOUBLE[2] as pos1,
                [(i * 31) % 1000, (i * 37) % 1000]::DOUBLE[2] as pos2
         FROM generate_series(0, {}) AS t(i)",
        n - 1
    ))?;
    
    println!("Testing with {} rows, 1000 iterations each\n", n);

    // ============================================================
    // 1. Unprepared: Parse + Plan + Execute every time
    // ============================================================
    let query = "SELECT sum(array_distance(pos1, pos2)) FROM pairs";
    
    let start = Instant::now();
    let mut sum = 0.0f64;
    for _ in 0..1000 {
        sum = conn.query_row(query, [], |r| r.get(0))?;
    }
    let unprepared_time = start.elapsed();
    println!("  Unprepared (parse+plan each): {:>7.3} ms  ({:.1} µs/query)", 
             unprepared_time.as_secs_f64() * 1000.0,
             unprepared_time.as_secs_f64() * 1_000_000.0 / 1000.0);

    // ============================================================
    // 2. Prepared once, execute many times
    // ============================================================
    let mut stmt = conn.prepare(query)?;
    
    // Warmup
    let _: f64 = stmt.query_row([], |r| r.get(0))?;
    
    let start = Instant::now();
    for _ in 0..1000 {
        sum = stmt.query_row([], |r| r.get(0))?;
    }
    let prepared_time = start.elapsed();
    println!("  Prepared (plan once):         {:>7.3} ms  ({:.1} µs/query)  {:.2}× vs unprepared", 
             prepared_time.as_secs_f64() * 1000.0,
             prepared_time.as_secs_f64() * 1_000_000.0 / 1000.0,
             prepared_time.as_secs_f64() / unprepared_time.as_secs_f64());

    // ============================================================
    // 3. Prepared with parameters (does binding add overhead?)
    // ============================================================
    let param_query = "SELECT sum(array_distance(pos1, pos2)) FROM pairs WHERE id < ?";
    let mut param_stmt = conn.prepare(param_query)?;
    
    let start = Instant::now();
    for _ in 0..1000 {
        sum = param_stmt.query_row([n as i32], |r| r.get(0))?;
    }
    let param_time = start.elapsed();
    println!("  Prepared + params:            {:>7.3} ms  ({:.1} µs/query)  {:.2}× vs prepared", 
             param_time.as_secs_f64() * 1000.0,
             param_time.as_secs_f64() * 1_000_000.0 / 1000.0,
             param_time.as_secs_f64() / prepared_time.as_secs_f64());

    // ============================================================
    // 4. Raw execution timing (what's the minimum overhead?)
    // ============================================================
    // Try a trivial query to measure baseline overhead
    let trivial_query = "SELECT 42";
    let mut trivial_stmt = conn.prepare(trivial_query)?;
    
    let start = Instant::now();
    for _ in 0..10000 {
        let _: i32 = trivial_stmt.query_row([], |r| r.get(0))?;
    }
    let trivial_time = start.elapsed();
    println!("\n  Trivial 'SELECT 42' (10K):    {:>7.3} ms  ({:.1} µs/query)  ← MINIMUM OVERHEAD", 
             trivial_time.as_secs_f64() * 1000.0,
             trivial_time.as_secs_f64() * 1_000_000.0 / 10000.0);

    // ============================================================
    // 5. Empty query (just FFI round-trip)
    // ============================================================
    let start = Instant::now();
    for _ in 0..10000 {
        // Just prepare and drop - measures parsing overhead
        let _ = conn.prepare("SELECT 1")?;
    }
    let parse_time = start.elapsed();
    println!("  Parse 'SELECT 1' (10K):       {:>7.3} ms  ({:.1} µs/query)  ← PARSE OVERHEAD", 
             parse_time.as_secs_f64() * 1000.0,
             parse_time.as_secs_f64() * 1_000_000.0 / 10000.0);

    // ============================================================
    // 6. Execute prepared without fetching result
    // ============================================================
    let mut exec_stmt = conn.prepare("SELECT sum(array_distance(pos1, pos2)) FROM pairs")?;
    
    let start = Instant::now();
    for _ in 0..1000 {
        let mut rows = exec_stmt.query([])?;
        // Don't fetch the result, just advance
        let _ = rows.next()?;
    }
    let exec_only_time = start.elapsed();
    println!("  Execute + minimal fetch:      {:>7.3} ms  ({:.1} µs/query)  {:.2}× vs query_row", 
             exec_only_time.as_secs_f64() * 1000.0,
             exec_only_time.as_secs_f64() * 1_000_000.0 / 1000.0,
             exec_only_time.as_secs_f64() / prepared_time.as_secs_f64());

    // ============================================================
    // 7. Arrow interface (does it reduce FFI overhead?)
    // ============================================================
    let start = Instant::now();
    for _ in 0..1000 {
        let arrow: Arrow<'_> = stmt.query_arrow([])?;
        for batch in arrow {
            sum = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
        }
    }
    let arrow_time = start.elapsed();
    println!("  Arrow interface:              {:>7.3} ms  ({:.1} µs/query)  {:.2}× vs query_row", 
             arrow_time.as_secs_f64() * 1000.0,
             arrow_time.as_secs_f64() * 1_000_000.0 / 1000.0,
             arrow_time.as_secs_f64() / prepared_time.as_secs_f64());

    // ============================================================
    // 8. Multiple statements in one call
    // ============================================================
    // Can we batch multiple operations?
    let batch_query = format!(
        "SELECT sum(array_distance(pos1, pos2)) FROM pairs;
         SELECT sum(array_distance(pos1, pos2)) FROM pairs;
         SELECT sum(array_distance(pos1, pos2)) FROM pairs;
         SELECT sum(array_distance(pos1, pos2)) FROM pairs;
         SELECT sum(array_distance(pos1, pos2)) FROM pairs"
    );
    
    let start = Instant::now();
    for _ in 0..200 {
        conn.execute_batch(&batch_query)?;
    }
    let batch_time = start.elapsed();
    println!("\n  Batch 5 queries (200×):       {:>7.3} ms  ({:.1} µs/query effective)", 
             batch_time.as_secs_f64() * 1000.0,
             batch_time.as_secs_f64() * 1_000_000.0 / 1000.0);

    // ============================================================
    // Breakdown
    // ============================================================
    println!("\n=== Overhead Breakdown ===\n");
    
    let parse_overhead = parse_time.as_secs_f64() * 1_000_000.0 / 10000.0;
    let exec_overhead = trivial_time.as_secs_f64() * 1_000_000.0 / 10000.0;
    let prepared_overhead = prepared_time.as_secs_f64() * 1_000_000.0 / 1000.0;
    let work_time = prepared_overhead - exec_overhead;
    
    println!("  Parse overhead:       {:>6.1} µs (cached by prepare())", parse_overhead);
    println!("  Min execution:        {:>6.1} µs (SELECT 42 baseline)", exec_overhead);
    println!("  Your query:           {:>6.1} µs total", prepared_overhead);
    println!("  └─ Actual work:       {:>6.1} µs (query - baseline)", work_time);
    println!();
    println!("  The {:.0} µs baseline is IRREDUCIBLE in DuckDB.", exec_overhead);
    println!("  It's the cost of: FFI → execute → materialize → FFI");
    println!();
    println!("  Solutions:");
    println!("  1. Batch more work per query (amortize overhead)");
    println!("  2. Use Lua→Rust SIMD for per-frame operations");
    println!("  3. Accept ~50-60µs overhead for DuckDB queries");

    Ok(())
}
