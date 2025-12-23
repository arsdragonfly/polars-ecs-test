//! Prior Art: What We're Missing from PostGIS/CockroachDB
//!
//! Testing techniques from real spatial databases

use duckdb::{Connection, Result};
use std::time::Instant;

fn main() -> Result<()> {
    println!("=== Prior Art from Spatial Databases ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;
    conn.execute_batch("INSTALL spatial; LOAD spatial;")?;

    let num_entities = 10_000;
    println!("Creating {} entities...\n", num_entities);
    
    conn.execute_batch(&format!(
        "CREATE TABLE entities AS
         SELECT 
            i AS id,
            ST_Point((hash(i) % 1000)::DOUBLE, (hash(i*2) % 1000)::DOUBLE) AS geom,
            (hash(i) % 1000)::INTEGER AS x,
            (hash(i*2) % 1000)::INTEGER AS y,
            CASE WHEN hash(i*7) % 10 < 3 THEN true ELSE false END AS is_enemy
         FROM generate_series(1, {}) AS t(i);
         
         CREATE INDEX rtree_idx ON entities USING RTREE(geom);", num_entities
    ))?;

    // =========================================================================
    // Technique 1: PostGIS KNN operator <-> (ORDER BY with index)
    // =========================================================================
    println!("--- Technique 1: KNN Operator <-> ---");
    println!("  PostGIS: ORDER BY geom <-> 'POINT(x,y)' uses index!\n");

    // Check if DuckDB has <-> operator
    println!("  Testing DuckDB <-> operator...");
    match conn.query_row(
        "SELECT id FROM entities ORDER BY geom <-> ST_Point(500, 500) LIMIT 1",
        [],
        |r| r.get::<_, i64>(0)
    ) {
        Ok(id) => println!("    ✓ Found! Nearest entity: {}", id),
        Err(e) => println!("    ✗ Not available: {}", e),
    }

    // Check for alternative KNN function
    println!("\n  Checking for KNN functions...");
    let mut stmt = conn.prepare(
        "SELECT DISTINCT function_name FROM duckdb_functions() 
         WHERE function_name LIKE '%knn%' OR function_name LIKE '%nearest%' OR function_name LIKE '%neighbor%'
         LIMIT 5"
    )?;
    let mut rows = stmt.query([])?;
    let mut found_any = false;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        println!("    Found: {}", name);
        found_any = true;
    }
    if !found_any {
        println!("    No KNN-specific functions found");
    }

    // =========================================================================
    // Technique 2: Batch all queries into one using LATERAL JOIN or UNION
    // =========================================================================
    println!("\n--- Technique 2: Batch Queries with LATERAL/CROSS JOIN ---");
    println!("  Amortize query overhead by batching\n");

    // Create a small set of query points
    conn.execute_batch(
        "CREATE TABLE query_points AS
         SELECT 
            i AS qid,
            (i * 100 % 1000)::DOUBLE AS qx,
            (i * 77 % 1000)::DOUBLE AS qy,
            ST_Point((i * 100 % 1000)::DOUBLE, (i * 77 % 1000)::DOUBLE) AS qgeom
         FROM generate_series(1, 100) AS t(i);"
    )?;

    // Individual queries (100 queries)
    let start = Instant::now();
    for i in 1..=100 {
        let qx = (i * 100 % 1000) as f64;
        let qy = (i * 77 % 1000) as f64;
        let _: Option<i64> = conn.query_row(
            &format!(
                "SELECT id FROM entities 
                 WHERE ST_Intersects(geom, ST_MakeEnvelope({}, {}, {}, {}))
                 LIMIT 1",
                qx - 50.0, qy - 50.0, qx + 50.0, qy + 50.0
            ),
            [],
            |r| r.get(0)
        ).ok();
    }
    let individual_time = start.elapsed();
    println!("  100 individual queries: {:>8.2} ms ({:.2} ms each)", 
             individual_time.as_secs_f64() * 1000.0,
             individual_time.as_secs_f64() * 10.0);

    // Single LATERAL JOIN query
    let start = Instant::now();
    let batch_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM (
            SELECT q.qid, (
                SELECT e.id FROM entities e
                WHERE ST_Intersects(e.geom, ST_MakeEnvelope(q.qx - 50, q.qy - 50, q.qx + 50, q.qy + 50))
                LIMIT 1
            ) AS nearest
            FROM query_points q
        ) WHERE nearest IS NOT NULL",
        [],
        |r| r.get(0)
    )?;
    let lateral_time = start.elapsed();
    println!("  LATERAL JOIN (1 query):  {:>8.2} ms ({} matches)", 
             lateral_time.as_secs_f64() * 1000.0, batch_count);
    
    // Check if R-tree used in LATERAL
    let plan: String = conn.query_row(
        "EXPLAIN SELECT q.qid, (
            SELECT e.id FROM entities e
            WHERE ST_Intersects(e.geom, ST_MakeEnvelope(q.qx - 50, q.qy - 50, q.qx + 50, q.qy + 50))
            LIMIT 1
        ) FROM query_points q",
        [],
        |r| r.get(1)
    )?;
    let uses_rtree = plan.contains("RTREE");
    println!("    R-tree used in LATERAL: {}", if uses_rtree { "✓ YES" } else { "✗ NO (subquery not constant!)" });

    // =========================================================================
    // Technique 3: Materialized spatial index table (like CockroachDB S2)
    // =========================================================================
    println!("\n--- Technique 3: Pre-computed Grid Cells ---");
    println!("  Store grid cell ID per entity for fast lookup\n");

    let cell_size = 50;
    conn.execute_batch(&format!(
        "ALTER TABLE entities ADD COLUMN cell_x INTEGER;
         ALTER TABLE entities ADD COLUMN cell_y INTEGER;
         UPDATE entities SET cell_x = x / {}, cell_y = y / {};
         CREATE INDEX cell_idx ON entities(cell_x, cell_y, is_enemy);", 
        cell_size, cell_size
    ))?;

    // Query using cell index
    let start = Instant::now();
    for i in 0..100 {
        let qx = (i * 100 % 1000) / cell_size;
        let qy = (i * 77 % 1000) / cell_size;
        let _: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM entities 
                 WHERE cell_x BETWEEN {} AND {} 
                 AND cell_y BETWEEN {} AND {}
                 AND is_enemy",
                qx - 1, qx + 1, qy - 1, qy + 1
            ),
            [],
            |r| r.get(0)
        )?;
    }
    let cell_time = start.elapsed();
    println!("  100 grid cell queries:  {:>8.2} ms ({:.2} ms each)", 
             cell_time.as_secs_f64() * 1000.0,
             cell_time.as_secs_f64() * 10.0);

    // =========================================================================
    // Technique 4: Bulk distance matrix (trade memory for speed)
    // =========================================================================
    println!("\n--- Technique 4: One-shot Combat Resolution ---");
    println!("  Single query returns ALL attacker-target pairs\n");

    let start = Instant::now();
    let pairs: i64 = conn.query_row(
        "SELECT COUNT(*) FROM (
            SELECT a.id as attacker, b.id as target,
                   (a.x - b.x)*(a.x - b.x) + (a.y - b.y)*(a.y - b.y) as dist_sq
            FROM entities a
            JOIN entities b ON a.cell_x BETWEEN b.cell_x - 1 AND b.cell_x + 1
                           AND a.cell_y BETWEEN b.cell_y - 1 AND b.cell_y + 1
            WHERE NOT a.is_enemy AND b.is_enemy
              AND a.id != b.id
              AND (a.x - b.x)*(a.x - b.x) + (a.y - b.y)*(a.y - b.y) <= 2500
        )",
        [],
        |r| r.get(0)
    )?;
    let bulk_time = start.elapsed();
    println!("  All pairs in one query: {:>8.2} ms ({} pairs)", 
             bulk_time.as_secs_f64() * 1000.0, pairs);

    // With FIRST_VALUE to get nearest only
    let start = Instant::now();
    let nearest_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT attacker) FROM (
            SELECT a.id as attacker, 
                   FIRST_VALUE(b.id) OVER (
                       PARTITION BY a.id 
                       ORDER BY (a.x - b.x)*(a.x - b.x) + (a.y - b.y)*(a.y - b.y)
                   ) as target
            FROM entities a
            JOIN entities b ON a.cell_x BETWEEN b.cell_x - 1 AND b.cell_x + 1
                           AND a.cell_y BETWEEN b.cell_y - 1 AND b.cell_y + 1
            WHERE NOT a.is_enemy AND b.is_enemy
              AND a.id != b.id
              AND (a.x - b.x)*(a.x - b.x) + (a.y - b.y)*(a.y - b.y) <= 2500
        )",
        [],
        |r| r.get(0)
    )?;
    let nearest_time = start.elapsed();
    println!("  Nearest per attacker:   {:>8.2} ms ({} attackers)", 
             nearest_time.as_secs_f64() * 1000.0, nearest_count);

    println!("\n--- Summary: What We're Missing ---");
    println!("  ✗ KNN operator (<->) - DuckDB doesn't have it");
    println!("  ✗ Index-accelerated JOINs - DuckDB R-tree only for constant predicates");
    println!("  ✓ Grid cell indexing helps! (B-tree on cell_x, cell_y)");
    println!("  ✓ Bulk query reduces per-query overhead");
    println!("\n  The fundamental issue: DuckDB is OLAP, not designed for");
    println!("  thousands of small point queries per second.");

    Ok(())
}
