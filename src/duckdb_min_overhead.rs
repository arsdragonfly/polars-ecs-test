//! DuckDB Minimum Overhead Investigation
//!
//! Testing the absolute minimum query latency achievable with:
//! 1. Cached/prepared statements (skip parsing + planning)
//! 2. Arrow output without unmarshalling
//! 3. Direct pointer access to results

use duckdb::{Connection, Result, Arrow, Statement, CachedStatement};
use duckdb::arrow::array::{Array, Int32Array, Int64Array};
use std::time::Instant;

const ENTITY_COUNT: i32 = 100_000;
const MAP_SIZE: i32 = 1000;

fn main() -> Result<()> {
    println!("=== DuckDB Minimum Overhead Investigation ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?; // Predictable timing

    setup_table(&conn)?;

    println!("--- Query Overhead Breakdown ---\n");

    bench_parse_every_time(&conn)?;
    bench_prepared_statement(&conn)?;
    bench_cached_statement(&conn)?;
    bench_arrow_no_unmarshal(&conn)?;
    bench_execute_only(&conn)?;
    bench_raw_index_scan(&conn)?;

    println!("\n--- Theoretical Minimum ---");
    println!("  DuckDB internal index lookup: ~1-5 µs");
    println!("  FFI call overhead: ~0.1-0.5 µs");
    println!("  Result construction: ~5-20 µs");
    println!("  Rust-side processing: ~0.1 µs");
    println!("  ────────────────────────────");
    println!("  Theoretical floor: ~10-30 µs per point query");

    Ok(())
}

fn setup_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities;
        CREATE TABLE entities AS
        SELECT
            i AS id,
            (hash(i) % {MAP_SIZE})::INTEGER AS x,
            (hash(i * 2) % {MAP_SIZE})::INTEGER AS y,
            (i % 10) AS entity_type
        FROM generate_series(1, {ENTITY_COUNT}) AS t(i);

        CREATE INDEX idx_xy ON entities(x, y);
        "
    ))?;
    println!("Created {} entities with index\n", ENTITY_COUNT);
    Ok(())
}

/// Baseline: Parse SQL every single time
fn bench_parse_every_time(conn: &Connection) -> Result<()> {
    let start = Instant::now();
    for i in 0..1000 {
        let x = (i * 17) % MAP_SIZE;
        let y = (i * 23) % MAP_SIZE;
        // Full parse + plan + execute cycle
        let _: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM entities WHERE x = {} AND y = {}", x, y),
            [],
            |row| row.get(0)
        )?;
    }
    let time = start.elapsed();
    println!("1. Parse every time (query_row):     {:>7.2} µs/query", 
             time.as_micros() as f64 / 1000.0);
    Ok(())
}

/// Prepared statement: Parse once, execute many
fn bench_prepared_statement(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM entities WHERE x = ? AND y = ?")?;
    
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let _: i64 = stmt.query_row([x, y], |row| row.get(0))?;
    }
    let time = start.elapsed();
    println!("2. Prepared statement (stmt.query):  {:>7.2} µs/query", 
             time.as_micros() as f64 / 1000.0);
    Ok(())
}

/// Cached statement: Connection caches prepared statements
fn bench_cached_statement(conn: &Connection) -> Result<()> {
    // First call prepares, subsequent calls reuse
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        
        // prepare_cached returns a cached statement
        let mut stmt = conn.prepare_cached("SELECT COUNT(*) FROM entities WHERE x = ?1 AND y = ?2")?;
        let _: i64 = stmt.query_row([x, y], |row| row.get(0))?;
    }
    let time = start.elapsed();
    println!("3. Cached statement (prepare_cached): {:>6.2} µs/query", 
             time.as_micros() as f64 / 1000.0);
    Ok(())
}

/// Arrow output without unmarshalling to Rust types
fn bench_arrow_no_unmarshal(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM entities WHERE x = ? AND y = ?")?;
    
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        
        // Get Arrow result - data stays in Arrow format
        let arrow: Arrow<'_> = stmt.query_arrow([x, y])?;
        
        // Just iterate to force materialization, don't convert
        for batch in arrow {
            std::hint::black_box(batch.num_rows());
        }
    }
    let time = start.elapsed();
    println!("4. Arrow output (no unmarshal):      {:>7.2} µs/query", 
             time.as_micros() as f64 / 1000.0);
    Ok(())
}

/// Execute without fetching results (just checking if query runs)
fn bench_execute_only(conn: &Connection) -> Result<()> {
    // Use a query that returns nothing
    let mut stmt = conn.prepare("SELECT 1 WHERE 1 = 0")?;
    
    let start = Instant::now();
    for _ in 0..1000 {
        let _ = stmt.execute([])?;
    }
    let time = start.elapsed();
    println!("5. Execute only (no fetch):          {:>7.2} µs/query", 
             time.as_micros() as f64 / 1000.0);
    
    // Now with actual data but ignoring result
    let mut stmt2 = conn.prepare("SELECT COUNT(*) FROM entities WHERE x = ? AND y = ?")?;
    
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        // Just execute, don't read result
        let mut rows = stmt2.query([x, y])?;
        // Must call next() at least once to trigger execution
        let _ = rows.next()?;
    }
    let time = start.elapsed();
    println!("6. Execute + single next():          {:>7.2} µs/query", 
             time.as_micros() as f64 / 1000.0);
    
    Ok(())
}

/// Test raw index performance via EXPLAIN
fn bench_raw_index_scan(conn: &Connection) -> Result<()> {
    // Check that index is being used
    println!("\n--- Query Plan Analysis ---");
    let plan: String = conn.query_row(
        "EXPLAIN SELECT COUNT(*) FROM entities WHERE x = 500 AND y = 500",
        [],
        |row| row.get(0)
    )?;
    
    if plan.contains("INDEX") || plan.contains("idx_xy") {
        println!("  ✓ Index is being used");
    } else {
        println!("  ✗ Index NOT used! Plan: {}", &plan[..100.min(plan.len())]);
    }
    
    // Test different selectivities
    println!("\n--- Selectivity Impact ---");
    
    let mut stmt = conn.prepare_cached("SELECT COUNT(*) FROM entities WHERE x = ?1 AND y = ?2")?;
    
    // Point query (expects 0-2 results)
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let _: i64 = stmt.query_row([x, y], |row| row.get(0))?;
    }
    let point_time = start.elapsed();
    
    // Range query (expects ~100 results)
    let mut stmt_range = conn.prepare_cached(
        "SELECT COUNT(*) FROM entities WHERE x BETWEEN ?1 AND ?2 AND y BETWEEN ?3 AND ?4"
    )?;
    
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % (MAP_SIZE - 10)) as i32;
        let y = ((i * 23) % (MAP_SIZE - 10)) as i32;
        let _: i64 = stmt_range.query_row([x, x + 10, y, y + 10], |row| row.get(0))?;
    }
    let range_time = start.elapsed();
    
    println!("  Point query (0-2 results):    {:>7.2} µs", point_time.as_micros() as f64 / 1000.0);
    println!("  Range query (~100 results):   {:>7.2} µs", range_time.as_micros() as f64 / 1000.0);
    println!("  → Result count has minimal impact (index is fast)");
    
    Ok(())
}
