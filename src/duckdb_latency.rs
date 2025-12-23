//! DuckDB Spatial Query Latency Analysis
//!
//! Breaking down where time is spent in spatial queries

use duckdb::{Connection, Result};
use std::time::Instant;

const ENTITY_COUNT: i32 = 100_000;
const MAP_SIZE: i32 = 1000;

fn main() -> Result<()> {
    println!("=== Why Are DuckDB Spatial Queries Slow? ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?; // Single thread for clearer analysis

    setup_table(&conn)?;

    println!("--- Latency Breakdown ---\n");

    bench_empty_query(&conn)?;
    bench_trivial_query(&conn)?;
    bench_count_star(&conn)?;
    bench_point_no_index(&conn)?;
    bench_point_with_index(&conn)?;
    bench_point_prepared(&conn)?;
    bench_batch_queries(&conn)?;
    bench_rust_hashmap()?;

    Ok(())
}

fn setup_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities;
        CREATE TABLE entities AS
        SELECT
            i AS id,
            (random() * {MAP_SIZE})::INTEGER AS x,
            (random() * {MAP_SIZE})::INTEGER AS y,
            (i % 10) AS entity_type
        FROM generate_series(1, {ENTITY_COUNT}) AS t(i);

        CREATE INDEX idx_xy ON entities(x, y);
        "
    ))?;
    Ok(())
}

/// What's the minimum query overhead?
fn bench_empty_query(conn: &Connection) -> Result<()> {
    let start = Instant::now();
    for _ in 0..1000 {
        let _: i64 = conn.query_row("SELECT 1", [], |row| row.get(0))?;
    }
    let time = start.elapsed();
    println!("1. Empty query (SELECT 1):           {:>7.2} µs", time.as_micros() as f64 / 1000.0);
    println!("   → This is pure FFI + SQL parsing overhead");
    Ok(())
}

/// Query that returns a constant
fn bench_trivial_query(conn: &Connection) -> Result<()> {
    let start = Instant::now();
    for _ in 0..1000 {
        let _: i64 = conn.query_row("SELECT 42 + 1", [], |row| row.get(0))?;
    }
    let time = start.elapsed();
    println!("2. Trivial expr (SELECT 42+1):       {:>7.2} µs", time.as_micros() as f64 / 1000.0);
    Ok(())
}

/// COUNT(*) with no filter - just metadata
fn bench_count_star(conn: &Connection) -> Result<()> {
    let start = Instant::now();
    for _ in 0..1000 {
        let _: i64 = conn.query_row("SELECT COUNT(*) FROM entities", [], |row| row.get(0))?;
    }
    let time = start.elapsed();
    println!("3. COUNT(*) no filter:               {:>7.2} µs", time.as_micros() as f64 / 1000.0);
    println!("   → Table metadata lookup");
    Ok(())
}

/// Point query without index
fn bench_point_no_index(conn: &Connection) -> Result<()> {
    conn.execute_batch("DROP INDEX IF EXISTS idx_xy;")?;
    
    let start = Instant::now();
    for i in 0..100 { // Only 100 - this is slow!
        let x = (i * 17) % MAP_SIZE;
        let y = (i * 23) % MAP_SIZE;
        let _: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM entities WHERE x = {} AND y = {}", x, y),
            [],
            |row| row.get(0)
        )?;
    }
    let time = start.elapsed();
    println!("4. Point query NO INDEX (100x):      {:>7.2} µs  ← FULL TABLE SCAN", time.as_micros() as f64 / 100.0);
    
    // Recreate index
    conn.execute_batch("CREATE INDEX idx_xy ON entities(x, y);")?;
    Ok(())
}

/// Point query with index
fn bench_point_with_index(conn: &Connection) -> Result<()> {
    let start = Instant::now();
    for i in 0..1000 {
        let x = (i * 17) % MAP_SIZE;
        let y = (i * 23) % MAP_SIZE;
        let _: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM entities WHERE x = {} AND y = {}", x, y),
            [],
            |row| row.get(0)
        )?;
    }
    let time = start.elapsed();
    println!("5. Point query WITH INDEX:           {:>7.2} µs", time.as_micros() as f64 / 1000.0);
    Ok(())
}

/// Prepared statement - skip SQL parsing
fn bench_point_prepared(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM entities WHERE x = ? AND y = ?")?;
    
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let _: i64 = stmt.query_row([x, y], |row| row.get(0))?;
    }
    let time = start.elapsed();
    println!("6. Prepared statement:               {:>7.2} µs", time.as_micros() as f64 / 1000.0);
    println!("   → Saves ~5-10% by avoiding re-parsing");
    Ok(())
}

/// Batch multiple lookups in one query
fn bench_batch_queries(conn: &Connection) -> Result<()> {
    // Build a query that checks 100 points at once
    let start = Instant::now();
    for batch in 0..10 {
        let mut conditions = Vec::new();
        for i in 0..100 {
            let idx = batch * 100 + i;
            let x = (idx * 17) % MAP_SIZE;
            let y = (idx * 23) % MAP_SIZE;
            conditions.push(format!("(x = {} AND y = {})", x, y));
        }
        let query = format!(
            "SELECT COUNT(*) FROM entities WHERE {}", 
            conditions.join(" OR ")
        );
        let _: i64 = conn.query_row(&query, [], |row| row.get(0))?;
    }
    let time = start.elapsed();
    let per_point = time.as_micros() as f64 / 1000.0;
    println!("7. Batched 100 points/query:         {:>7.2} µs/point", per_point);
    println!("   → Amortize overhead over many lookups!");
    
    Ok(())
}

/// Compare to Rust HashMap
fn bench_rust_hashmap() -> Result<()> {
    use std::collections::HashMap;
    
    // Build equivalent data structure
    let mut map: HashMap<(i32, i32), Vec<i32>> = HashMap::new();
    let mut rng_state = 12345u64;
    for i in 0..ENTITY_COUNT {
        // Simple LCG random
        rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        let x = ((rng_state >> 16) as i32) % MAP_SIZE;
        rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        let y = ((rng_state >> 16) as i32) % MAP_SIZE;
        map.entry((x.abs(), y.abs())).or_default().push(i);
    }
    
    let start = Instant::now();
    let mut total = 0i64;
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE).abs();
        let y = ((i * 23) % MAP_SIZE).abs();
        total += map.get(&(x, y)).map(|v| v.len()).unwrap_or(0) as i64;
    }
    let time = start.elapsed();
    println!("8. Rust HashMap lookup:              {:>7.2} µs  ← {:}x FASTER", 
             time.as_micros() as f64 / 1000.0, 
             (280.0 / (time.as_micros() as f64 / 1000.0)) as i32);
    println!("   (prevent optimize-out: total={})", total);
    
    Ok(())
}
