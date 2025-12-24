//! DuckDB + Arrow Best Practices Analysis
//!
//! Investigating whether we're using Arrow APIs correctly to minimize overhead:
//! 1. Prepared statement caching
//! 2. Arrow IPC for bulk data transfer
//! 3. Appender for bulk inserts
//! 4. Parameterized queries vs string interpolation

use duckdb::Connection;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB Query Overhead: Best Practices Analysis ===\n");
    
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;
    
    // Setup test table
    conn.execute_batch("CREATE TABLE t AS SELECT i as x, i*2 as y FROM generate_series(1, 1000) AS t(i)")?;
    
    let iterations = 1000;
    
    println!("--- Statement Preparation ---\n");
    
    // ❌ BAD: Prepare on every call (what our benchmarks do!)
    let start = Instant::now();
    for _ in 0..iterations {
        let _: i64 = conn.prepare("SELECT sum(x) FROM t")?.query_row([], |r| r.get(0))?;
    }
    let prepare_every = start.elapsed();
    println!("  Prepare every call:   {:>8.2} µs/query", 
             prepare_every.as_secs_f64() * 1_000_000.0 / iterations as f64);
    
    // ✅ GOOD: Reuse prepared statement
    let mut stmt = conn.prepare("SELECT sum(x) FROM t")?;
    let start = Instant::now();
    for _ in 0..iterations {
        let _: i64 = stmt.query_row([], |r| r.get(0))?;
    }
    let stmt_cached = start.elapsed();
    println!("  Cached Statement:     {:>8.2} µs/query", 
             stmt_cached.as_secs_f64() * 1_000_000.0 / iterations as f64);
    
    let prep_speedup = prepare_every.as_secs_f64() / stmt_cached.as_secs_f64();
    println!("\n  Statement caching speedup: {:.2}×\n", prep_speedup);
    
    println!("--- Result Fetching Methods ---\n");
    
    // Method 1: query_row (single value)
    let mut stmt = conn.prepare("SELECT sum(x) FROM t")?;
    let start = Instant::now();
    for _ in 0..iterations {
        let _: i64 = stmt.query_row([], |r| r.get(0))?;
    }
    let query_row_time = start.elapsed();
    println!("  query_row (scalar):   {:>8.2} µs/query", 
             query_row_time.as_secs_f64() * 1_000_000.0 / iterations as f64);
    
    // Method 2: query_arrow (Arrow batches)
    let start = Instant::now();
    for _ in 0..iterations {
        let batches: Vec<_> = stmt.query_arrow([])?.collect();
        let _ = batches.len(); // Force materialization
    }
    let arrow_time = start.elapsed();
    println!("  query_arrow (batch):  {:>8.2} µs/query", 
             arrow_time.as_secs_f64() * 1_000_000.0 / iterations as f64);
    
    println!("\n--- Parameterized vs String Interpolation ---\n");
    
    // ❌ BAD: String interpolation (requires re-parse)
    let start = Instant::now();
    for i in 0..iterations {
        let query = format!("SELECT sum(x) FROM t WHERE x > {}", i % 100);
        let _: i64 = conn.prepare(&query)?.query_row([], |r| r.get(0))?;
    }
    let interp_time = start.elapsed();
    println!("  String interpolation: {:>8.2} µs/query", 
             interp_time.as_secs_f64() * 1_000_000.0 / iterations as f64);
    
    // ✅ GOOD: Parameterized query (can cache plan)
    let mut param_stmt = conn.prepare("SELECT sum(x) FROM t WHERE x > ?")?;
    let start = Instant::now();
    for i in 0..iterations {
        let _: i64 = param_stmt.query_row([i as i64 % 100], |r| r.get(0))?;
    }
    let param_time = start.elapsed();
    println!("  Parameterized query:  {:>8.2} µs/query", 
             param_time.as_secs_f64() * 1_000_000.0 / iterations as f64);
    
    let param_speedup = interp_time.as_secs_f64() / param_time.as_secs_f64();
    println!("\n  Parameterization speedup: {:.2}×\n", param_speedup);
    
    println!("--- Bulk Data Transfer ---\n");
    
    // Create larger result set
    conn.execute_batch("CREATE TABLE big AS SELECT i as x, i*2 as y FROM generate_series(1, 100000) AS t(i)")?;
    
    // Row-by-row iteration
    let mut stmt = conn.prepare("SELECT x, y FROM big")?;
    let start = Instant::now();
    let mut count = 0i64;
    {
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let _x: i64 = row.get(0)?;
            let _y: i64 = row.get(1)?;
            count += 1;
        }
    }
    let row_iter_time = start.elapsed();
    println!("  Row iteration (100K): {:>8.2} ms ({} rows)", 
             row_iter_time.as_secs_f64() * 1000.0, count);
    
    // Arrow bulk fetch
    let start = Instant::now();
    let batches: Vec<_> = stmt.query_arrow([])?.collect();
    let arrow_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let arrow_bulk_time = start.elapsed();
    println!("  Arrow bulk (100K):    {:>8.2} ms ({} rows)", 
             arrow_bulk_time.as_secs_f64() * 1000.0, arrow_rows);
    
    let bulk_speedup = row_iter_time.as_secs_f64() / arrow_bulk_time.as_secs_f64();
    println!("\n  Arrow bulk speedup: {:.2}×\n", bulk_speedup);
    
    println!("--- Cross-Join with UDF (Simulated Game Query) ---\n");
    
    // Create entity tables
    conn.execute_batch(
        "CREATE TABLE e1 AS SELECT random()*1000 as x, random()*1000 as y FROM generate_series(1, 100);
         CREATE TABLE e2 AS SELECT random()*1000 as x, random()*1000 as y FROM generate_series(1, 100);"
    )?;
    
    // Warm up
    for _ in 0..5 {
        let _: f64 = conn.prepare(
            "SELECT sum(sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y))) FROM e1, e2"
        )?.query_row([], |r| r.get(0))?;
    }
    
    // ❌ Prepare every time
    let start = Instant::now();
    for _ in 0..100 {
        let _: f64 = conn.prepare(
            "SELECT sum(sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y))) FROM e1, e2"
        )?.query_row([], |r| r.get(0))?;
    }
    let uncached = start.elapsed();
    println!("  100×100 uncached:     {:>8.2} ms/query", 
             uncached.as_secs_f64() * 1000.0 / 100.0);
    
    // ✅ Cached statement
    let mut cached = conn.prepare(
        "SELECT sum(sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y))) FROM e1, e2"
    )?;
    let start = Instant::now();
    for _ in 0..100 {
        let _: f64 = cached.query_row([], |r| r.get(0))?;
    }
    let cached_time = start.elapsed();
    println!("  100×100 cached:       {:>8.2} ms/query", 
             cached_time.as_secs_f64() * 1000.0 / 100.0);
    
    let cross_speedup = uncached.as_secs_f64() / cached_time.as_secs_f64();
    println!("\n  Cache speedup on cross-join: {:.2}×\n", cross_speedup);
    
    println!("=== Summary ===\n");
    println!("  ┌────────────────────────────────────────────────────────────┐");
    println!("  │ Optimization                        │ Speedup │ Status    │");
    println!("  ├────────────────────────────────────────────────────────────┤");
    println!("  │ Statement caching                   │ {:.1}×     │ ❌ Not used│", prep_speedup);
    println!("  │ Parameterized queries               │ {:.1}×     │ ❌ Not used│", param_speedup);
    println!("  │ Arrow bulk fetch                    │ {:.1}×     │ ✅ VScalar │", bulk_speedup);
    println!("  │ Cross-join caching                  │ {:.1}×     │ ❌ Not used│", cross_speedup);
    println!("  └────────────────────────────────────────────────────────────┘");
    println!();
    println!("  Key insight: Statement caching could significantly reduce overhead!");
    println!("  Our benchmarks re-prepare on every call, which is suboptimal.");
    
    Ok(())
}
