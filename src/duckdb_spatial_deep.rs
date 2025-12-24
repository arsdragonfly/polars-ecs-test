//! DuckDB Spatial Hash Deep Dive - Why is it slow?
//!
//! Investigate:
//! 1. Is the index being used?
//! 2. What join algorithm is DuckDB choosing?
//! 3. Can we force a better plan?

use duckdb::Connection;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB Spatial Hash Deep Dive ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;

    let world_size = 1000.0;
    let query_radius = 50.0;
    let cell_size = query_radius;
    let n = 5000;
    let radius_sq = query_radius * query_radius;

    println!("Setup: {} entities, cell_size={}, radius={}\n", n, cell_size, query_radius);

    // Create entity table with pre-computed cells
    conn.execute_batch(&format!(
        "CREATE TABLE entities AS 
         SELECT i as id,
                (i * 17 + 31) % {} as x, 
                (i * 23 + 47) % {} as y,
                CAST(((i * 17 + 31) % {}) / {} AS INTEGER) as cx,
                CAST(((i * 23 + 47) % {}) / {} AS INTEGER) as cy
         FROM generate_series(1, {}) AS t(i)",
        world_size as i32, world_size as i32,
        world_size as i32, cell_size as i32,
        world_size as i32, cell_size as i32,
        n
    ))?;

    // Check data distribution
    let cell_count: i64 = conn.query_row("SELECT count(DISTINCT (cx, cy)) FROM entities", [], |r| r.get(0))?;
    let max_per_cell: i64 = conn.query_row("SELECT max(cnt) FROM (SELECT count(*) as cnt FROM entities GROUP BY cx, cy)", [], |r| r.get(0))?;
    println!("  Cells used: {}", cell_count);
    println!("  Max entities per cell: {}", max_per_cell);
    println!();

    // ============================================================
    // Test 1: Explain the base query
    // ============================================================
    println!("=== Query Plans ===\n");

    println!("--- Query 1: Cell filter (current approach) ---\n");
    let explain1: String = conn.query_row(&format!(
        "EXPLAIN SELECT count(*) FROM entities e1, entities e2
         WHERE e1.id < e2.id
           AND abs(e2.cx - e1.cx) <= 1
           AND abs(e2.cy - e1.cy) <= 1
           AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}",
        radius_sq
    ), [], |r| r.get(0))?;
    println!("{}\n", explain1);

    // ============================================================
    // Test 2: Try with index on (cx, cy)
    // ============================================================
    println!("--- Query 2: With composite index ---\n");
    conn.execute_batch("CREATE INDEX idx_cell ON entities(cx, cy);")?;
    
    let explain2: String = conn.query_row(&format!(
        "EXPLAIN SELECT count(*) FROM entities e1, entities e2
         WHERE e1.id < e2.id
           AND abs(e2.cx - e1.cx) <= 1
           AND abs(e2.cy - e1.cy) <= 1
           AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}",
        radius_sq
    ), [], |r| r.get(0))?;
    println!("{}\n", explain2);

    // ============================================================
    // Test 3: Try explicit hash join
    // ============================================================
    println!("--- Query 3: Hash join on cell ---\n");
    let explain3: String = conn.query_row(&format!(
        "EXPLAIN SELECT count(*) FROM entities e1
         INNER JOIN entities e2 ON e1.cx = e2.cx AND e1.cy = e2.cy
         WHERE e1.id < e2.id
           AND abs(e2.cx - e1.cx) <= 1
           AND abs(e2.cy - e1.cy) <= 1
           AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}",
        radius_sq
    ), [], |r| r.get(0))?;
    println!("{}\n", explain3);

    // ============================================================
    // Test 4: Group by cell, then self-join within cell
    // ============================================================
    println!("--- Query 4: Cell-based aggregation approach ---\n");
    
    // Create cell lookup table
    conn.execute_batch("CREATE TABLE cells AS SELECT DISTINCT cx, cy FROM entities;")?;
    
    let explain4: String = conn.query_row(&format!(
        "EXPLAIN 
         SELECT sum(pairs) FROM (
             SELECT c.cx, c.cy,
                    (SELECT count(*) FROM entities e1, entities e2
                     WHERE e1.cx = c.cx AND e1.cy = c.cy
                       AND e2.cx BETWEEN c.cx-1 AND c.cx+1
                       AND e2.cy BETWEEN c.cy-1 AND c.cy+1
                       AND e1.id < e2.id
                       AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}) as pairs
             FROM cells c
         )",
        radius_sq
    ), [], |r| r.get(0))?;
    println!("{}\n", explain4);

    // ============================================================
    // Benchmark all approaches
    // ============================================================
    println!("=== Benchmarks ===\n");

    // Query 1: Original
    let start = Instant::now();
    let count1: i64 = conn.query_row(&format!(
        "SELECT count(*) FROM entities e1, entities e2
         WHERE e1.id < e2.id
           AND abs(e2.cx - e1.cx) <= 1
           AND abs(e2.cy - e1.cy) <= 1
           AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}",
        radius_sq
    ), [], |r| r.get(0))?;
    let time1 = start.elapsed();
    println!("  1. Cell filter (abs):         {:>8.2} ms  ({} pairs)", time1.as_secs_f64() * 1000.0, count1);

    // Query 2: Same-cell only (subset, just for comparison)
    let start = Instant::now();
    let count2: i64 = conn.query_row(&format!(
        "SELECT count(*) FROM entities e1
         INNER JOIN entities e2 ON e1.cx = e2.cx AND e1.cy = e2.cy
         WHERE e1.id < e2.id
           AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}",
        radius_sq
    ), [], |r| r.get(0))?;
    let time2 = start.elapsed();
    println!("  2. Same-cell only (hash):     {:>8.2} ms  ({} pairs)", time2.as_secs_f64() * 1000.0, count2);

    // Query 3: UNION of 9 cell offsets (explicit)
    let start = Instant::now();
    let count3: i64 = conn.query_row(&format!(
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
    ), [], |r| r.get(0))?;
    let time3 = start.elapsed();
    println!("  3. 9x hash join (UNION ALL):  {:>8.2} ms  ({} pairs)", time3.as_secs_f64() * 1000.0, count3);

    // Query 4: Create materialized cell_id
    conn.execute_batch("ALTER TABLE entities ADD COLUMN cell_id INTEGER;")?;
    conn.execute_batch("UPDATE entities SET cell_id = cx * 1000 + cy;")?;
    conn.execute_batch("CREATE INDEX idx_cellid ON entities(cell_id);")?;
    
    // Build list of (cell_id, neighbor_cell_id) pairs
    conn.execute_batch("
        CREATE TABLE cell_neighbors AS
        SELECT DISTINCT 
            e1.cell_id as c1,
            e2.cell_id as c2
        FROM entities e1, entities e2
        WHERE abs(e1.cx - e2.cx) <= 1 AND abs(e1.cy - e2.cy) <= 1
    ")?;
    
    let start = Instant::now();
    let count4: i64 = conn.query_row(&format!(
        "SELECT count(*) FROM entities e1
         INNER JOIN cell_neighbors cn ON e1.cell_id = cn.c1
         INNER JOIN entities e2 ON e2.cell_id = cn.c2
         WHERE e1.id < e2.id
           AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}",
        radius_sq
    ), [], |r| r.get(0))?;
    let time4 = start.elapsed();
    println!("  4. Cell neighbor table:       {:>8.2} ms  ({} pairs)", time4.as_secs_f64() * 1000.0, count4);

    // Query 5: Pure N² for reference
    let start = Instant::now();
    let count5: i64 = conn.query_row(&format!(
        "SELECT count(*) FROM entities e1, entities e2
         WHERE e1.id < e2.id
           AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < {}",
        radius_sq
    ), [], |r| r.get(0))?;
    let time5 = start.elapsed();
    println!("  5. Pure N² (baseline):        {:>8.2} ms  ({} pairs)", time5.as_secs_f64() * 1000.0, count5);

    println!();

    // Verify correctness
    if count1 != count5 {
        println!("  ⚠️  Query 1 mismatch! {} vs {}", count1, count5);
    }
    if count3 != count5 {
        println!("  ⚠️  Query 3 mismatch! {} vs {}", count3, count5);
    }

    Ok(())
}
