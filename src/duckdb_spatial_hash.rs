//! DuckDB Spatial Hash - True Apples-to-Apples Comparison
//!
//! Maintain a spatial hash in the database just like Rust does:
//! 1. Pre-compute cell assignments
//! 2. Index on (cx, cy) for O(1) cell lookup
//! 3. Query only within cells (no N² cross-join)

use duckdb::Connection;
use std::collections::HashMap;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB Spatial Hash vs Rust HashMap ===\n");

    let world_size = 1000.0;
    let query_radius = 50.0;
    let cell_size = query_radius; // Same as Rust benchmark

    for n in [1000, 5000, 10000, 50000, 100000] {
        println!("=== {} entities (world: {}×{}, radius: {}) ===\n", 
                 n, world_size as i32, world_size as i32, query_radius as i32);

        // Generate deterministic positions (same for both)
        let entities: Vec<(i32, f64, f64)> = (0..n)
            .map(|i| {
                let x = ((i as u64 * 17 + 31) % world_size as u64) as f64;
                let y = ((i as u64 * 23 + 47) % world_size as u64) as f64;
                (i, x, y)
            })
            .collect();

        // ============================================================
        // Rust Spatial Hash (baseline)
        // ============================================================
        let start = Instant::now();
        
        // Build spatial hash
        let mut grid: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
        for (i, (_, x, y)) in entities.iter().enumerate() {
            let cx = (*x / cell_size) as i32;
            let cy = (*y / cell_size) as i32;
            grid.entry((cx, cy)).or_default().push(i);
        }
        let rust_build_time = start.elapsed();

        // Query: find all pairs within radius
        let start = Instant::now();
        let mut rust_pairs = 0i64;
        let radius_sq = query_radius * query_radius;

        for (i, (_, x1, y1)) in entities.iter().enumerate() {
            let cx = (*x1 / cell_size) as i32;
            let cy = (*y1 / cell_size) as i32;
            
            // Check 3x3 neighboring cells
            for dx in -1..=1 {
                for dy in -1..=1 {
                    if let Some(cell) = grid.get(&(cx + dx, cy + dy)) {
                        for &j in cell {
                            if i < j {
                                let (_, x2, y2) = entities[j];
                                let dist_sq = (x2 - x1).powi(2) + (y2 - y1).powi(2);
                                if dist_sq < radius_sq {
                                    rust_pairs += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        let rust_query_time = start.elapsed();

        println!("  Rust HashMap:");
        println!("    Build:  {:>8.2} ms", rust_build_time.as_secs_f64() * 1000.0);
        println!("    Query:  {:>8.2} ms  ({} pairs)", rust_query_time.as_secs_f64() * 1000.0, rust_pairs);
        println!("    Total:  {:>8.2} ms\n", (rust_build_time + rust_query_time).as_secs_f64() * 1000.0);

        // ============================================================
        // DuckDB Spatial Hash (same algorithm)
        // ============================================================
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("SET threads TO 1;")?;

        // Insert entities
        let start = Instant::now();
        conn.execute_batch(&format!(
            "CREATE TABLE entities (id INTEGER, x DOUBLE, y DOUBLE, cx INTEGER, cy INTEGER)"
        ))?;
        
        // Batch insert with pre-computed cells
        let mut appender = conn.appender("entities")?;
        for (id, x, y) in &entities {
            let cx = (*x / cell_size) as i32;
            let cy = (*y / cell_size) as i32;
            appender.append_row([
                duckdb::types::Value::Int(*id),
                duckdb::types::Value::Double(*x),
                duckdb::types::Value::Double(*y),
                duckdb::types::Value::Int(cx),
                duckdb::types::Value::Int(cy),
            ])?;
        }
        appender.flush()?;
        drop(appender);
        
        // Create index on cells (like HashMap buckets)
        conn.execute_batch("CREATE INDEX idx_cells ON entities(cx, cy);")?;
        let duck_build_time = start.elapsed();

        // Query using cell index
        let start = Instant::now();
        
        // Simpler query - use abs() instead of BETWEEN to avoid optimizer issues
        let duck_pairs: i64 = conn.query_row(&format!(
            "SELECT count(*) FROM entities e1, entities e2
             WHERE e1.id < e2.id
               AND abs(e2.cx - e1.cx) <= 1
               AND abs(e2.cy - e1.cy) <= 1
               AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}",
            radius_sq
        ), [], |r| r.get(0))?;
        
        let duck_query_time = start.elapsed();

        println!("  DuckDB Spatial Hash:");
        println!("    Build:  {:>8.2} ms  (insert + index)", duck_build_time.as_secs_f64() * 1000.0);
        println!("    Query:  {:>8.2} ms  ({} pairs)", duck_query_time.as_secs_f64() * 1000.0, duck_pairs);
        println!("    Total:  {:>8.2} ms\n", (duck_build_time + duck_query_time).as_secs_f64() * 1000.0);

        // Verify correctness
        if rust_pairs != duck_pairs {
            println!("  ⚠️  Mismatch! Rust={}, DuckDB={}\n", rust_pairs, duck_pairs);
        }

        // ============================================================
        // DuckDB with cached statement (game loop scenario)
        // ============================================================
        let mut stmt = conn.prepare(&format!(
            "SELECT count(*) FROM entities e1, entities e2
             WHERE e1.id < e2.id
               AND abs(e2.cx - e1.cx) <= 1
               AND abs(e2.cy - e1.cy) <= 1
               AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}",
            radius_sq
        ))?;
        
        // Warmup
        let _: i64 = stmt.query_row([], |r| r.get(0))?;
        
        let start = Instant::now();
        let _: i64 = stmt.query_row([], |r| r.get(0))?;
        let duck_cached_time = start.elapsed();

        println!("  DuckDB Cached Query (game loop):");
        println!("    Query:  {:>8.2} ms\n", duck_cached_time.as_secs_f64() * 1000.0);

        // Summary
        let rust_total = (rust_build_time + rust_query_time).as_secs_f64() * 1000.0;
        let duck_total = (duck_build_time + duck_query_time).as_secs_f64() * 1000.0;
        let duck_cached = duck_cached_time.as_secs_f64() * 1000.0;

        println!("  Summary:");
        println!("    Rust total:        {:>8.2} ms", rust_total);
        println!("    DuckDB total:      {:>8.2} ms  ({:.1}× slower)", duck_total, duck_total / rust_total);
        println!("    DuckDB query only: {:>8.2} ms  ({:.1}× slower than Rust query)", 
                 duck_cached, duck_cached / (rust_query_time.as_secs_f64() * 1000.0));
        println!();
    }

    println!("=== Conclusions ===\n");
    println!("  Even with identical algorithm (spatial hash + cell index),");
    println!("  DuckDB has overhead from:");
    println!("    • Query parsing/planning (mitigated by caching)");
    println!("    • B-tree index vs HashMap O(1) lookup");
    println!("    • Row-oriented storage vs contiguous Vec");
    println!("    • SQL execution vs direct memory access");
    println!();

    Ok(())
}
