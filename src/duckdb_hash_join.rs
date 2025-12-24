//! DuckDB Optimized Spatial Hash - 9x Hash Join
//!
//! Key insight: DuckDB can only use hash join for EQUALITY conditions.
//! abs(cx1 - cx2) <= 1 forces N² scan, but explicit JOINs with offsets work!

use duckdb::Connection;
use std::collections::HashMap;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB Optimized Spatial Hash (9x Hash Join) ===\n");

    let world_size = 1000.0;
    let query_radius = 50.0;
    let cell_size = query_radius;
    let radius_sq = query_radius * query_radius;

    for n in [1000, 5000, 10000, 50000] {
        println!("=== {} entities ===\n", n);

        // Generate deterministic positions
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
        
        let mut grid: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
        for (i, (_, x, y)) in entities.iter().enumerate() {
            let cx = (*x / cell_size) as i32;
            let cy = (*y / cell_size) as i32;
            grid.entry((cx, cy)).or_default().push(i);
        }

        let mut rust_pairs = 0i64;
        for (i, (_, x1, y1)) in entities.iter().enumerate() {
            let cx = (*x1 / cell_size) as i32;
            let cy = (*y1 / cell_size) as i32;
            
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
        let rust_time = start.elapsed();
        println!("  Rust HashMap:        {:>8.2} ms  ({} pairs)", rust_time.as_secs_f64() * 1000.0, rust_pairs);

        // ============================================================
        // DuckDB with 9x Hash Join
        // ============================================================
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("SET threads TO 1;")?;

        // Insert with pre-computed cells
        conn.execute_batch("CREATE TABLE entities (id INTEGER, x DOUBLE, y DOUBLE, cx INTEGER, cy INTEGER)")?;
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

        // 9x UNION ALL with explicit cell offsets - forces hash join!
        let query = format!(
            "SELECT count(*) FROM (
                SELECT e1.id, e2.id FROM entities e1
                INNER JOIN entities e2 ON e1.cx = e2.cx AND e1.cy = e2.cy
                WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {0}
                UNION ALL
                SELECT e1.id, e2.id FROM entities e1
                INNER JOIN entities e2 ON e1.cx = e2.cx - 1 AND e1.cy = e2.cy
                WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {0}
                UNION ALL
                SELECT e1.id, e2.id FROM entities e1
                INNER JOIN entities e2 ON e1.cx = e2.cx + 1 AND e1.cy = e2.cy
                WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {0}
                UNION ALL
                SELECT e1.id, e2.id FROM entities e1
                INNER JOIN entities e2 ON e1.cx = e2.cx AND e1.cy = e2.cy - 1
                WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {0}
                UNION ALL
                SELECT e1.id, e2.id FROM entities e1
                INNER JOIN entities e2 ON e1.cx = e2.cx AND e1.cy = e2.cy + 1
                WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {0}
                UNION ALL
                SELECT e1.id, e2.id FROM entities e1
                INNER JOIN entities e2 ON e1.cx = e2.cx - 1 AND e1.cy = e2.cy - 1
                WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {0}
                UNION ALL
                SELECT e1.id, e2.id FROM entities e1
                INNER JOIN entities e2 ON e1.cx = e2.cx + 1 AND e1.cy = e2.cy - 1
                WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {0}
                UNION ALL
                SELECT e1.id, e2.id FROM entities e1
                INNER JOIN entities e2 ON e1.cx = e2.cx - 1 AND e1.cy = e2.cy + 1
                WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {0}
                UNION ALL
                SELECT e1.id, e2.id FROM entities e1
                INNER JOIN entities e2 ON e1.cx = e2.cx + 1 AND e1.cy = e2.cy + 1
                WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {0}
            )",
            radius_sq
        );

        // Warmup
        let _: i64 = conn.query_row(&query, [], |r| r.get(0))?;

        let start = Instant::now();
        let duck_pairs: i64 = conn.query_row(&query, [], |r| r.get(0))?;
        let duck_time = start.elapsed();
        
        println!("  DuckDB 9x Hash Join: {:>8.2} ms  ({} pairs)  {:.1}× slower", 
                 duck_time.as_secs_f64() * 1000.0, duck_pairs,
                 duck_time.as_secs_f64() / rust_time.as_secs_f64());

        // Old slow query for comparison
        let start = Instant::now();
        let _: i64 = conn.query_row(&format!(
            "SELECT count(*) FROM entities e1, entities e2
             WHERE e1.id < e2.id
               AND abs(e2.cx - e1.cx) <= 1
               AND abs(e2.cy - e1.cy) <= 1
               AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}",
            radius_sq
        ), [], |r| r.get(0))?;
        let old_time = start.elapsed();
        
        println!("  DuckDB abs() filter: {:>8.2} ms           {:.1}× slower (old)", 
                 old_time.as_secs_f64() * 1000.0,
                 old_time.as_secs_f64() / rust_time.as_secs_f64());

        // Verify
        if rust_pairs != duck_pairs {
            println!("  ⚠️  Mismatch! Rust={}, DuckDB={}", rust_pairs, duck_pairs);
        }

        println!();
    }

    println!("=== Conclusion ===\n");
    println!("  DuckDB hash join is MUCH faster than abs() filter!");
    println!("  The key: explicit equality JOINs enable O(N) hash join.");
    println!("  abs(cx1-cx2) <= 1 forces O(N²) nested loop scan.");
    println!();

    Ok(())
}
