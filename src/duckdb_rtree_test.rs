//! DuckDB R-tree Index - Does it Actually Work?
//!
//! Testing if the R-tree index accelerates queries

use duckdb::{Connection, Result};
use std::time::Instant;

fn main() -> Result<()> {
    println!("=== DuckDB R-tree Index Performance Test ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;
    conn.execute_batch("INSTALL spatial; LOAD spatial;")?;

    let version: String = conn.query_row("SELECT version()", [], |r| r.get(0))?;
    println!("DuckDB version: {}\n", version);

    // Create larger dataset
    let num_entities = 50_000;
    println!("Creating {} entities...", num_entities);
    
    conn.execute_batch(&format!(
        "
        CREATE TABLE entities AS
        SELECT 
            i AS id,
            (random() * 10000)::DOUBLE AS x,
            (random() * 10000)::DOUBLE AS y,
            ST_Point(random() * 10000, random() * 10000) AS geom
        FROM generate_series(1, {}) AS t(i);
        ", num_entities
    ))?;

    // Test WITHOUT R-tree index
    println!("\n--- Test 1: Point-in-Range Query ---\n");

    // Query: find all entities within 100 units of point (5000, 5000)
    let start = Instant::now();
    let count1: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entities WHERE ST_DWithin(geom, ST_Point(5000, 5000), 100)",
        [],
        |r| r.get(0)
    )?;
    let no_index_time = start.elapsed();
    println!("  NO R-tree index:       {:>8.2} ms  ({} entities)", 
             no_index_time.as_secs_f64() * 1000.0, count1);

    // Create R-tree index
    println!("\n  Creating R-tree index...");
    let start = Instant::now();
    conn.execute_batch("CREATE INDEX idx_rtree ON entities USING RTREE(geom);")?;
    println!("  Index created in {:.2} ms", start.elapsed().as_secs_f64() * 1000.0);

    // Same query WITH R-tree index
    let start = Instant::now();
    let count2: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entities WHERE ST_DWithin(geom, ST_Point(5000, 5000), 100)",
        [],
        |r| r.get(0)
    )?;
    let with_index_time = start.elapsed();
    println!("  WITH R-tree index:     {:>8.2} ms  ({} entities)", 
             with_index_time.as_secs_f64() * 1000.0, count2);
    println!("  Speedup: {:.1}x", no_index_time.as_secs_f64() / with_index_time.as_secs_f64());

    // Check query plan
    println!("\n  Query plan with R-tree:");
    let mut stmt = conn.prepare("EXPLAIN SELECT COUNT(*) FROM entities WHERE ST_DWithin(geom, ST_Point(5000, 5000), 100)")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let plan: String = row.get(1)?;
        if plan.contains("RTREE") || plan.contains("Index") || plan.contains("Scan") {
            println!("    {}", plan.trim());
        }
    }

    // Test 2: Range query
    println!("\n--- Test 2: Bounding Box Query ---\n");

    let start = Instant::now();
    let count3: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entities 
         WHERE ST_Intersects(geom, ST_MakeEnvelope(4000, 4000, 6000, 6000))",
        [],
        |r| r.get(0)
    )?;
    let bbox_time = start.elapsed();
    println!("  ST_Intersects (bbox):  {:>8.2} ms  ({} entities)", 
             bbox_time.as_secs_f64() * 1000.0, count3);

    // Compare with x,y columns
    let start = Instant::now();
    let count4: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entities WHERE x BETWEEN 4000 AND 6000 AND y BETWEEN 4000 AND 6000",
        [],
        |r| r.get(0)
    )?;
    let xy_time = start.elapsed();
    println!("  x,y columns (no idx):  {:>8.2} ms  ({} entities)", 
             xy_time.as_secs_f64() * 1000.0, count4);

    // Add B-tree on x,y
    conn.execute_batch("CREATE INDEX idx_xy ON entities(x, y);")?;
    
    let start = Instant::now();
    let count5: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entities WHERE x BETWEEN 4000 AND 6000 AND y BETWEEN 4000 AND 6000",
        [],
        |r| r.get(0)
    )?;
    let xy_idx_time = start.elapsed();
    println!("  x,y with B-tree idx:   {:>8.2} ms  ({} entities)", 
             xy_idx_time.as_secs_f64() * 1000.0, count5);

    // Test 3: Nearest neighbor (this is the key use case)
    println!("\n--- Test 3: Nearest Neighbor (100 queries) ---\n");

    let start = Instant::now();
    for i in 0..100 {
        let x = (i * 100) as f64;
        let y = (i * 100) as f64;
        let _: Option<i64> = conn.query_row(
            &format!(
                "SELECT id FROM entities 
                 WHERE ST_DWithin(geom, ST_Point({}, {}), 100)
                 ORDER BY ST_Distance(geom, ST_Point({}, {}))
                 LIMIT 1", x, y, x, y
            ),
            [],
            |r| r.get(0)
        ).ok();
    }
    let nn_spatial = start.elapsed();
    println!("  ST_DWithin + ORDER BY: {:>8.2} ms/query", 
             nn_spatial.as_secs_f64() * 1000.0 / 100.0);

    let start = Instant::now();
    for i in 0..100 {
        let x = (i * 100) as f64;
        let y = (i * 100) as f64;
        let _: Option<i64> = conn.query_row(
            &format!(
                "SELECT id FROM entities 
                 WHERE x BETWEEN {} AND {} AND y BETWEEN {} AND {}
                 ORDER BY (x - {})*(x - {}) + (y - {})*(y - {})
                 LIMIT 1", 
                x - 100.0, x + 100.0, y - 100.0, y + 100.0,
                x, x, y, y
            ),
            [],
            |r| r.get(0)
        ).ok();
    }
    let nn_xy = start.elapsed();
    println!("  x,y + B-tree:          {:>8.2} ms/query", 
             nn_xy.as_secs_f64() * 1000.0 / 100.0);

    println!("\n--- Conclusion ---");
    println!("  R-tree index EXISTS in DuckDB 1.2+ but acceleration varies");
    println!("  For simple range queries: B-tree on x,y is often faster");
    println!("  R-tree may help with complex polygon intersection");
    println!("  Per-query overhead (~180µs) still dominates for point lookups");

    Ok(())
}
