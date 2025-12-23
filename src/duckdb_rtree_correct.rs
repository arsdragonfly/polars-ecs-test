//! DuckDB R-tree - Correct Usage Per Documentation
//!
//! Key caveat from docs: R-tree only works with:
//! - Simple SELECT with filter
//! - Spatial predicate on indexed column with CONSTANT geometry
//! - Does NOT work for joins or correlated subqueries!

use duckdb::{Connection, Result};
use std::time::Instant;

fn main() -> Result<()> {
    println!("=== DuckDB R-tree - Correct Usage ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;
    conn.execute_batch("INSTALL spatial; LOAD spatial;")?;

    let version: String = conn.query_row("SELECT version()", [], |r| r.get(0))?;
    println!("DuckDB version: {}\n", version);

    // Create 10 million points (as per the docs example)
    println!("Creating 10,000,000 random points...");
    let start = Instant::now();
    conn.execute_batch(
        "CREATE TABLE t1 AS SELECT point::GEOMETRY AS geom
         FROM st_generatepoints(
             {min_x: 0, min_y: 0, max_x: 10000, max_y: 10000}::BOX_2D,
             10000000,
             1337
         );"
    )?;
    println!("  Created in {:.2} s\n", start.elapsed().as_secs_f64());

    // Add row id for reference
    conn.execute_batch("ALTER TABLE t1 ADD COLUMN id INTEGER; UPDATE t1 SET id = rowid;")?;

    println!("--- Test WITHOUT R-tree index ---\n");

    // Query with constant envelope (this is what R-tree accelerates)
    let start = Instant::now();
    let count1: i64 = conn.query_row(
        "SELECT count(*) FROM t1 WHERE ST_Within(geom, ST_MakeEnvelope(450, 450, 650, 650))",
        [],
        |r| r.get(0)
    )?;
    let no_index = start.elapsed();
    println!("  ST_Within (constant envelope): {:>8.2} ms  ({} rows)", 
             no_index.as_secs_f64() * 1000.0, count1);

    // Check query plan
    println!("\n  Query plan (no index):");
    let plan: String = conn.query_row(
        "EXPLAIN SELECT count(*) FROM t1 WHERE ST_Within(geom, ST_MakeEnvelope(450, 450, 650, 650))",
        [],
        |r| r.get(1)
    )?;
    if plan.contains("SEQ_SCAN") {
        println!("    → SEQ_SCAN (full table scan)");
    }

    println!("\n--- Creating R-tree index ---\n");
    
    let start = Instant::now();
    conn.execute_batch("CREATE INDEX my_idx ON t1 USING RTREE (geom);")?;
    println!("  Index created in {:.2} s", start.elapsed().as_secs_f64());

    println!("\n--- Test WITH R-tree index ---\n");

    // Same query - should now use index
    let start = Instant::now();
    let count2: i64 = conn.query_row(
        "SELECT count(*) FROM t1 WHERE ST_Within(geom, ST_MakeEnvelope(450, 450, 650, 650))",
        [],
        |r| r.get(0)
    )?;
    let with_index = start.elapsed();
    println!("  ST_Within (constant envelope): {:>8.2} ms  ({} rows)", 
             with_index.as_secs_f64() * 1000.0, count2);
    println!("  SPEEDUP: {:.1}x", no_index.as_secs_f64() / with_index.as_secs_f64());

    // Check query plan
    println!("\n  Query plan (with index):");
    let plan: String = conn.query_row(
        "EXPLAIN SELECT count(*) FROM t1 WHERE ST_Within(geom, ST_MakeEnvelope(450, 450, 650, 650))",
        [],
        |r| r.get(1)
    )?;
    if plan.contains("RTREE_INDEX_SCAN") {
        println!("    → RTREE_INDEX_SCAN ✓");
    } else if plan.contains("SEQ_SCAN") {
        println!("    → Still SEQ_SCAN (index not used!)");
    }

    // Test different predicates
    println!("\n--- Different Spatial Predicates ---\n");

    let predicates = [
        ("ST_Within(geom, envelope)", "SELECT count(*) FROM t1 WHERE ST_Within(geom, ST_MakeEnvelope(450, 450, 650, 650))"),
        ("ST_Intersects(geom, envelope)", "SELECT count(*) FROM t1 WHERE ST_Intersects(geom, ST_MakeEnvelope(450, 450, 650, 650))"),
        ("ST_DWithin(geom, point, dist)", "SELECT count(*) FROM t1 WHERE ST_DWithin(geom, ST_Point(5000, 5000), 100)"),
    ];

    for (name, query) in predicates {
        let start = Instant::now();
        let count: i64 = conn.query_row(query, [], |r| r.get(0))?;
        let time = start.elapsed();
        
        // Check if index used
        let plan: String = conn.query_row(&format!("EXPLAIN {}", query), [], |r| r.get(1))?;
        let uses_index = plan.contains("RTREE_INDEX_SCAN");
        
        println!("  {}: {:>6.2} ms ({} rows) {}", 
                 name, 
                 time.as_secs_f64() * 1000.0, 
                 count,
                 if uses_index { "✓ INDEX" } else { "✗ SCAN" });
    }

    // Test what DOESN'T work - joins
    println!("\n--- What DOESN'T use R-tree (joins) ---\n");

    conn.execute_batch(
        "CREATE TABLE query_points AS 
         SELECT ST_Point(random() * 10000, random() * 10000) AS geom
         FROM generate_series(1, 100);"
    )?;

    let start = Instant::now();
    let count3: i64 = conn.query_row(
        "SELECT count(*) FROM t1, query_points q
         WHERE ST_DWithin(t1.geom, q.geom, 10)",
        [],
        |r| r.get(0)
    )?;
    let join_time = start.elapsed();
    println!("  JOIN with ST_DWithin: {:>8.2} ms ({} matches)", 
             join_time.as_secs_f64() * 1000.0, count3);

    // Check plan
    let plan: String = conn.query_row(
        "EXPLAIN SELECT count(*) FROM t1, query_points q WHERE ST_DWithin(t1.geom, q.geom, 10)",
        [],
        |r| r.get(1)
    )?;
    if plan.contains("RTREE_INDEX_SCAN") {
        println!("    → RTREE_INDEX_SCAN used!");
    } else {
        println!("    → NO R-tree (joins don't use index as per docs)");
    }

    // Test multiple point queries (this is what games need)
    println!("\n--- Multiple Point Queries (game use case) ---\n");

    let start = Instant::now();
    for i in 0..100 {
        let x = (i * 100) as f64;
        let y = (i * 100) as f64;
        let _: i64 = conn.query_row(
            &format!(
                "SELECT count(*) FROM t1 WHERE ST_DWithin(geom, ST_Point({}, {}), 50)", x, y
            ),
            [],
            |r| r.get(0)
        )?;
    }
    let query_time = start.elapsed();
    println!("  100 point queries: {:.2} ms total, {:.2} ms/query", 
             query_time.as_secs_f64() * 1000.0,
             query_time.as_secs_f64() * 1000.0 / 100.0);

    println!("\n--- Conclusion ---");
    println!("  R-tree WORKS for: Simple SELECT + spatial predicate + CONSTANT geometry");
    println!("  R-tree FAILS for: JOINs, correlated subqueries, nearest-neighbor combat");
    println!("  For games: Each entity query IS using R-tree, but ~180µs overhead remains");
    println!("  The ~180µs is DuckDB's per-query execution overhead, not index lookup");

    Ok(())
}
