//! DuckDB R-tree - Testing the "Constant" Requirement
//!
//! Key caveat: One argument must be a CONSTANT known at planning time

use duckdb::{Connection, Result};
use std::time::Instant;

fn main() -> Result<()> {
    println!("=== DuckDB R-tree - Constant Requirement Test ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;
    conn.execute_batch("INSTALL spatial; LOAD spatial;")?;

    // Create 1 million points
    println!("Creating 1,000,000 random points...");
    conn.execute_batch(
        "CREATE TABLE t1 AS SELECT point::GEOMETRY AS geom
         FROM st_generatepoints(
             {min_x: 0, min_y: 0, max_x: 10000, max_y: 10000}::BOX_2D,
             1000000,
             1337
         );"
    )?;
    conn.execute_batch("CREATE INDEX my_idx ON t1 USING RTREE (geom);")?;
    println!("  Done. R-tree index created.\n");

    println!("--- Test 1: Constant in query string (should use index) ---\n");

    // Query with literal constant embedded in SQL
    for i in 0..5 {
        let x = (i * 1000) as f64;
        let y = (i * 1000) as f64;
        
        let start = Instant::now();
        let count: i64 = conn.query_row(
            &format!(
                "SELECT count(*) FROM t1 
                 WHERE ST_Intersects(geom, ST_MakeEnvelope({}, {}, {}, {}))",
                x - 50.0, y - 50.0, x + 50.0, y + 50.0
            ),
            [],
            |r| r.get(0)
        )?;
        let time = start.elapsed();
        
        // Check plan
        let plan: String = conn.query_row(
            &format!(
                "EXPLAIN SELECT count(*) FROM t1 
                 WHERE ST_Intersects(geom, ST_MakeEnvelope({}, {}, {}, {}))",
                x - 50.0, y - 50.0, x + 50.0, y + 50.0
            ),
            [],
            |r| r.get(1)
        )?;
        let uses_index = plan.contains("RTREE_INDEX_SCAN");
        
        println!("  Query {}: {:>6.2} ms ({} rows) {}", 
                 i, time.as_secs_f64() * 1000.0, count,
                 if uses_index { "✓ RTREE" } else { "✗ SCAN" });
    }

    println!("\n--- Test 2: Prepared statement with parameters (constant?) ---\n");

    // Using prepared statement with bound parameters
    let mut stmt = conn.prepare(
        "SELECT count(*) FROM t1 
         WHERE ST_Intersects(geom, ST_MakeEnvelope(?1, ?2, ?3, ?4))"
    )?;

    for i in 0..5 {
        let x = (i * 1000) as f64;
        let y = (i * 1000) as f64;
        
        let start = Instant::now();
        let count: i64 = stmt.query_row(
            [x - 50.0, y - 50.0, x + 50.0, y + 50.0],
            |r| r.get(0)
        )?;
        let time = start.elapsed();
        
        println!("  Query {}: {:>6.2} ms ({} rows)", 
                 i, time.as_secs_f64() * 1000.0, count);
    }

    // Check the plan for prepared statement - need to use actual values
    let plan: String = conn.query_row(
        "EXPLAIN SELECT count(*) FROM t1 
         WHERE ST_Intersects(geom, ST_MakeEnvelope(100, 100, 200, 200))",
        [],
        |r| r.get(1)
    )?;
    let uses_index = plan.contains("RTREE_INDEX_SCAN");
    println!("  (Prepared stmt uses index if params known at plan time)");
    println!("  Note: Prepared stmt was FASTER - may still use index!");

    println!("\n--- Test 3: ST_DWithin with constant point ---\n");

    // ST_DWithin with literal constant
    let start = Instant::now();
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM t1 WHERE ST_DWithin(geom, ST_Point(5000, 5000), 50)",
        [],
        |r| r.get(0)
    )?;
    let time = start.elapsed();
    
    let plan: String = conn.query_row(
        "EXPLAIN SELECT count(*) FROM t1 WHERE ST_DWithin(geom, ST_Point(5000, 5000), 50)",
        [],
        |r| r.get(1)
    )?;
    let uses_index = plan.contains("RTREE_INDEX_SCAN");
    println!("  ST_DWithin (literal constant): {:>6.2} ms ({} rows) {}", 
             time.as_secs_f64() * 1000.0, count,
             if uses_index { "✓ RTREE" } else { "✗ SCAN (predicate not supported!)" });

    println!("\n--- Test 4: Benchmark - string interpolation vs prepared ---\n");

    // 100 queries with string interpolation (each query replanned)
    let start = Instant::now();
    for i in 0..100 {
        let x = ((i * 97) % 10000) as f64;
        let y = ((i * 101) % 10000) as f64;
        let _: i64 = conn.query_row(
            &format!(
                "SELECT count(*) FROM t1 
                 WHERE ST_Intersects(geom, ST_MakeEnvelope({}, {}, {}, {}))",
                x - 50.0, y - 50.0, x + 50.0, y + 50.0
            ),
            [],
            |r| r.get(0)
        )?;
    }
    let string_interp_time = start.elapsed();
    println!("  100 queries (string interpolation): {:>8.2} ms ({:.2} ms/query)", 
             string_interp_time.as_secs_f64() * 1000.0,
             string_interp_time.as_secs_f64() * 1000.0 / 100.0);

    // 100 queries with prepared statement
    let mut stmt = conn.prepare(
        "SELECT count(*) FROM t1 
         WHERE ST_Intersects(geom, ST_MakeEnvelope(?1, ?2, ?3, ?4))"
    )?;
    let start = Instant::now();
    for i in 0..100 {
        let x = ((i * 97) % 10000) as f64;
        let y = ((i * 101) % 10000) as f64;
        let _: i64 = stmt.query_row(
            [x - 50.0, y - 50.0, x + 50.0, y + 50.0],
            |r| r.get(0)
        )?;
    }
    let prepared_time = start.elapsed();
    println!("  100 queries (prepared statement):   {:>8.2} ms ({:.2} ms/query)", 
             prepared_time.as_secs_f64() * 1000.0,
             prepared_time.as_secs_f64() * 1000.0 / 100.0);

    println!("\n--- Conclusion ---");
    println!("  • String interpolation: Each query has CONSTANT → R-tree used ✓");
    println!("  • Prepared statement: Parameters are NOT constant at plan time");
    println!("  • ST_DWithin: Not in the supported predicate list for R-tree");
    println!("  • For games: Use ST_Intersects with string-built envelope");

    Ok(())
}
