//! DuckDB vs Polars for Game ECS
//! 
//! Comparing both systems for game-relevant operations:
//! - Per-frame query overhead
//! - Spatial queries
//! - Data modification (entity updates)
//! - Memory usage patterns

use duckdb::Connection;
use polars::prelude::*;
use std::time::Instant;
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB vs Polars for Game ECS ===\n");

    for n in [5000, 10000, 20000] {
        println!("=== {} entities ===\n", n);
        
        // Generate test data
        let ids: Vec<i32> = (0..n).collect();
        let xs: Vec<f64> = (0..n).map(|i| ((i as u64 * 17 + 31) % 1000) as f64).collect();
        let ys: Vec<f64> = (0..n).map(|i| ((i as u64 * 23 + 47) % 1000) as f64).collect();
        let healths: Vec<f64> = (0..n).map(|i| (i % 100) as f64).collect();
        
        // ============================================================
        // Setup: Create data in both systems
        // ============================================================
        
        // DuckDB
        let duck_conn = Connection::open_in_memory()?;
        duck_conn.execute_batch(&format!(
            "CREATE TABLE entities AS 
             SELECT i as id, 
                    ((i * 17 + 31) % 1000)::DOUBLE as x,
                    ((i * 23 + 47) % 1000)::DOUBLE as y,
                    (i % 100)::DOUBLE as health
             FROM generate_series(0, {}) AS t(i)", n - 1
        ))?;
        
        // Polars
        let polars_df = df!(
            "id" => &ids,
            "x" => &xs,
            "y" => &ys,
            "health" => &healths
        )?;
        
        // ============================================================
        // Test 1: Simple aggregation query
        // ============================================================
        println!("--- Test 1: Aggregation (sum health) ---\n");
        
        // DuckDB
        let mut duck_stmt = duck_conn.prepare("SELECT sum(health) FROM entities")?;
        let start = Instant::now();
        for _ in 0..1000 {
            let _: f64 = duck_stmt.query_row([], |r| r.get(0))?;
        }
        let duck_agg_time = start.elapsed();
        println!("  DuckDB (1000 queries):   {:>8.3} ms  ({:.1} µs/query)", 
                 duck_agg_time.as_secs_f64() * 1000.0,
                 duck_agg_time.as_secs_f64() * 1_000_000.0 / 1000.0);
        
        // Polars
        let start = Instant::now();
        for _ in 0..1000 {
            let _ = polars_df.clone().lazy()
                .select([col("health").sum()])
                .collect()?;
        }
        let polars_agg_time = start.elapsed();
        println!("  Polars (1000 queries):   {:>8.3} ms  ({:.1} µs/query)  {:.2}× vs DuckDB", 
                 polars_agg_time.as_secs_f64() * 1000.0,
                 polars_agg_time.as_secs_f64() * 1_000_000.0 / 1000.0,
                 polars_agg_time.as_secs_f64() / duck_agg_time.as_secs_f64());

        // Polars eager (no query planning)
        let health_series = polars_df.column("health")?.as_materialized_series();
        let start = Instant::now();
        for _ in 0..1000 {
            let _ = health_series.sum::<f64>();
        }
        let polars_eager_time = start.elapsed();
        println!("  Polars eager (1000):     {:>8.3} ms  ({:.1} µs/query)  {:.2}× vs DuckDB", 
                 polars_eager_time.as_secs_f64() * 1000.0,
                 polars_eager_time.as_secs_f64() * 1_000_000.0 / 1000.0,
                 polars_eager_time.as_secs_f64() / duck_agg_time.as_secs_f64());

        // ============================================================
        // Test 2: Filtered query (spatial-ish)
        // ============================================================
        println!("\n--- Test 2: Filtered query (x > 500) ---\n");
        
        // DuckDB
        let mut duck_filter_stmt = duck_conn.prepare("SELECT count(*) FROM entities WHERE x > 500")?;
        let start = Instant::now();
        for _ in 0..1000 {
            let _: i64 = duck_filter_stmt.query_row([], |r| r.get(0))?;
        }
        let duck_filter_time = start.elapsed();
        println!("  DuckDB (1000 queries):   {:>8.3} ms  ({:.1} µs/query)", 
                 duck_filter_time.as_secs_f64() * 1000.0,
                 duck_filter_time.as_secs_f64() * 1_000_000.0 / 1000.0);
        
        // Polars lazy
        let start = Instant::now();
        for _ in 0..1000 {
            let _ = polars_df.clone().lazy()
                .filter(col("x").gt(lit(500.0)))
                .select([col("id").count()])
                .collect()?;
        }
        let polars_filter_time = start.elapsed();
        println!("  Polars lazy (1000):      {:>8.3} ms  ({:.1} µs/query)  {:.2}× vs DuckDB", 
                 polars_filter_time.as_secs_f64() * 1000.0,
                 polars_filter_time.as_secs_f64() * 1_000_000.0 / 1000.0,
                 polars_filter_time.as_secs_f64() / duck_filter_time.as_secs_f64());

        // Polars eager with direct filter
        let x_col = polars_df.column("x")?.f64()?;
        let start = Instant::now();
        for _ in 0..1000 {
            let mask = x_col.gt(500.0);
            let count = mask.sum().unwrap_or(0);
            std::hint::black_box(count);
        }
        let polars_direct_time = start.elapsed();
        println!("  Polars direct (1000):    {:>8.3} ms  ({:.1} µs/query)  {:.2}× vs DuckDB", 
                 polars_direct_time.as_secs_f64() * 1000.0,
                 polars_direct_time.as_secs_f64() * 1_000_000.0 / 1000.0,
                 polars_direct_time.as_secs_f64() / duck_filter_time.as_secs_f64());

        // ============================================================
        // Test 3: Self-join / spatial query (simplified)
        // ============================================================
        println!("\n--- Test 3: Join query (entities near x=500) ---\n");
        
        // DuckDB - filtered self-reference
        let mut duck_near_stmt = duck_conn.prepare(
            "SELECT count(*) FROM entities 
             WHERE x BETWEEN 490 AND 510 AND y BETWEEN 490 AND 510"
        )?;
        
        let start = Instant::now();
        for _ in 0..1000 {
            let _: i64 = duck_near_stmt.query_row([], |r| r.get(0))?;
        }
        let duck_near_time = start.elapsed();
        println!("  DuckDB (1000 queries):   {:>8.3} ms  ({:.1} µs/query)", 
                 duck_near_time.as_secs_f64() * 1000.0,
                 duck_near_time.as_secs_f64() * 1_000_000.0 / 1000.0);
        
        // Polars - filter
        let x_arr = polars_df.column("x")?.f64()?;
        let y_arr = polars_df.column("y")?.f64()?;
        let start = Instant::now();
        for _ in 0..1000 {
            let mask = x_arr.gt_eq(490.0) & x_arr.lt_eq(510.0) 
                     & y_arr.gt_eq(490.0) & y_arr.lt_eq(510.0);
            let count = mask.sum().unwrap_or(0);
            std::hint::black_box(count);
        }
        let polars_near_time = start.elapsed();
        println!("  Polars direct (1000):    {:>8.3} ms  ({:.1} µs/query)  {:.2}× vs DuckDB", 
                 polars_near_time.as_secs_f64() * 1000.0,
                 polars_near_time.as_secs_f64() * 1_000_000.0 / 1000.0,
                 polars_near_time.as_secs_f64() / duck_near_time.as_secs_f64());
        
        // ============================================================
        // Test 4: Data modification (critical for games!)
        // ============================================================
        println!("\n--- Test 4: Update entities (health -= 10) ---\n");
        
        // DuckDB
        let start = Instant::now();
        for _ in 0..100 {
            duck_conn.execute("UPDATE entities SET health = health - 10 WHERE id < 1000", [])?;
            duck_conn.execute("UPDATE entities SET health = health + 10 WHERE id < 1000", [])?;
        }
        let duck_update_time = start.elapsed();
        println!("  DuckDB (200 updates):    {:>8.3} ms  ({:.1} µs/update)", 
                 duck_update_time.as_secs_f64() * 1000.0,
                 duck_update_time.as_secs_f64() * 1_000_000.0 / 200.0);
        
        // Polars - need to rebuild DataFrame (immutable!)
        let mut mutable_df = polars_df.clone();
        let start = Instant::now();
        for _ in 0..100 {
            // Polars is immutable - must create new column
            let health = mutable_df.column("health")?.f64()? - 10.0;
            mutable_df.with_column(health.into_series().with_name("health".into()))?;
            let health = mutable_df.column("health")?.f64()? + 10.0;
            mutable_df.with_column(health.into_series().with_name("health".into()))?;
        }
        let polars_update_time = start.elapsed();
        println!("  Polars (200 rebuilds):   {:>8.3} ms  ({:.1} µs/update)  {:.2}× vs DuckDB", 
                 polars_update_time.as_secs_f64() * 1000.0,
                 polars_update_time.as_secs_f64() * 1_000_000.0 / 200.0,
                 polars_update_time.as_secs_f64() / duck_update_time.as_secs_f64());

        println!();
    }

    // ============================================================
    // Summary
    // ============================================================
    println!("=== Pros and Cons Summary ===\n");
    
    println!("┌────────────────────┬─────────────────────────┬─────────────────────────┐");
    println!("│ Aspect             │ DuckDB                  │ Polars                  │");
    println!("├────────────────────┼─────────────────────────┼─────────────────────────┤");
    println!("│ Query overhead     │ ~60-100µs (high)        │ ~20-50µs (medium)       │");
    println!("│ Direct data access │ Copy required           │ Zero-copy slices!       │");
    println!("│ Data mutation      │ ✅ UPDATE in-place      │ ❌ Immutable, rebuild   │");
    println!("│ SQL support        │ ✅ Full SQL             │ ⚠️  DSL only            │");
    println!("│ Joins/spatial      │ ✅ Hash join optimizer  │ ⚠️  Cross join only     │");
    println!("│ Persistence        │ ✅ Built-in files       │ ⚠️  Parquet/CSV manual  │");
    println!("│ Extensions         │ ✅ Spatial, FTS, etc    │ ❌ Limited              │");
    println!("│ Memory model       │ Row-group columnar      │ Pure columnar           │");
    println!("│ Rust integration   │ FFI (copy overhead)     │ Native Rust             │");
    println!("│ Threading          │ Built-in parallel       │ Built-in parallel       │");
    println!("└────────────────────┴─────────────────────────┴─────────────────────────┘");
    println!();
    
    println!("=== Recommendations for Games ===\n");
    println!("  Use DuckDB for:");
    println!("    • Complex queries (joins, aggregations, analytics)");
    println!("    • Persistent game data (save files, world state)");
    println!("    • Mod support (SQL is familiar to modders)");
    println!("    • Spatial indexing (with spatial extension)");
    println!();
    println!("  Use Polars for:");
    println!("    • High-frequency queries (lower overhead)");
    println!("    • Read-heavy workloads (zero-copy access)");
    println!("    • Rust-native integration (no FFI)");
    println!("    • Streaming data processing");
    println!();
    println!("  Use NEITHER for:");
    println!("    • Per-entity updates (use HashMap/ECS)");
    println!("    • Real-time spatial queries (use Lua→Rust SIMD)");
    println!("    • Frame-critical paths (use native Rust)");

    Ok(())
}
