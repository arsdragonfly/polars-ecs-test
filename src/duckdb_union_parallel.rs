//! Check if DuckDB parallelizes UNION ALL

use duckdb::Connection;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB UNION ALL Parallelization Test ===\n");

    let conn = Connection::open_in_memory()?;
    
    let n = 10000;
    let radius_sq = 2500.0;

    // Create table
    conn.execute_batch(&format!(
        "CREATE TABLE entities AS 
         SELECT i as id, random()*1000 as x, random()*1000 as y,
                CAST(floor(random()*1000/50) AS INTEGER) as cx,
                CAST(floor(random()*1000/50) AS INTEGER) as cy
         FROM generate_series(1, {}) AS t(i)",
        n
    ))?;

    let query_9x = format!(
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

    // Show explain plan
    println!("--- EXPLAIN Plan ---\n");
    let plan: String = conn.query_row(&format!("EXPLAIN {}", query_9x), [], |r| r.get(0))?;
    println!("{}\n", plan);

    // Test with different thread counts
    println!("--- Thread Scaling ---\n");
    
    for threads in [1, 2, 4, 8, 12] {
        conn.execute_batch(&format!("SET threads TO {};", threads))?;
        
        // Warmup
        let _: i64 = conn.query_row(&query_9x, [], |r| r.get(0))?;
        
        let start = Instant::now();
        let count: i64 = conn.query_row(&query_9x, [], |r| r.get(0))?;
        let time = start.elapsed();
        
        println!("  {} threads: {:>6.2} ms  ({} pairs)", threads, time.as_secs_f64() * 1000.0, count);
    }

    println!();
    Ok(())
}
