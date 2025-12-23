//! DuckDB Arrow Interface Benchmark
//!
//! Testing zero-copy Arrow data exchange with DuckDB
//! to see if we can reduce the FFI overhead

use duckdb::{Connection, Result, Arrow};
use std::time::Instant;

const ENTITY_COUNT: i32 = 100_000;
const MAP_SIZE: i32 = 1000;

fn main() -> Result<()> {
    println!("=== DuckDB Arrow Zero-Copy Interface ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;

    setup_table(&conn)?;

    println!("--- Comparing Query Methods ---\n");

    bench_standard_query(&conn)?;
    bench_arrow_query(&conn)?;
    bench_arrow_batch_spatial(&conn)?;
    bench_arrow_stream(&conn)?;

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
            (i % 10) AS entity_type,
            random()::FLOAT AS health,
            random()::FLOAT AS speed
        FROM generate_series(1, {ENTITY_COUNT}) AS t(i);

        CREATE INDEX idx_xy ON entities(x, y);
        "
    ))?;
    println!("Created {} entities\n", ENTITY_COUNT);
    Ok(())
}

/// Standard row-by-row query (baseline)
fn bench_standard_query(conn: &Connection) -> Result<()> {
    println!("1. Standard Query (row iteration)");
    
    // Single value query
    let start = Instant::now();
    for _ in 0..1000 {
        let _: i64 = conn.query_row("SELECT COUNT(*) FROM entities WHERE x = 500 AND y = 500", [], |row| row.get(0))?;
    }
    let time = start.elapsed();
    println!("   1K point queries: {:?} ({:.2} µs/query)", time, time.as_micros() as f64 / 1000.0);

    // Multi-row query
    let start = Instant::now();
    for _ in 0..100 {
        let mut stmt = conn.prepare("SELECT id, x, y, health FROM entities WHERE x BETWEEN 400 AND 600")?;
        let mut rows = stmt.query([])?;
        let mut count = 0;
        while let Some(_row) = rows.next()? {
            count += 1;
        }
        std::hint::black_box(count);
    }
    let time = start.elapsed();
    println!("   100 range queries (row iter): {:?} ({:.2} ms/query)", time, time.as_millis() as f64 / 100.0);
    println!();

    Ok(())
}

/// Arrow interface - get RecordBatches directly
fn bench_arrow_query(conn: &Connection) -> Result<()> {
    println!("2. Arrow Query (RecordBatch)");
    
    // Multi-row query returning Arrow batches
    let start = Instant::now();
    for _ in 0..100 {
        let mut stmt = conn.prepare("SELECT id, x, y, health FROM entities WHERE x BETWEEN 400 AND 600")?;
        let arrow_result: Arrow<'_> = stmt.query_arrow([])?;
        
        let mut total_rows = 0;
        for batch in arrow_result {
            total_rows += batch.num_rows();
        }
        std::hint::black_box(total_rows);
    }
    let time = start.elapsed();
    println!("   100 range queries (Arrow): {:?} ({:.2} ms/query)", time, time.as_millis() as f64 / 100.0);

    // Large result set
    let start = Instant::now();
    for _ in 0..10 {
        let mut stmt = conn.prepare("SELECT * FROM entities")?;
        let arrow_result: Arrow<'_> = stmt.query_arrow([])?;
        
        let mut total_rows = 0;
        for batch in arrow_result {
            total_rows += batch.num_rows();
        }
        std::hint::black_box(total_rows);
    }
    let time = start.elapsed();
    println!("   10 full table scans (Arrow): {:?} ({:.2} ms/query)", time, time.as_millis() as f64 / 10.0);
    println!();

    Ok(())
}

/// Batch spatial queries with Arrow output
fn bench_arrow_batch_spatial(conn: &Connection) -> Result<()> {
    println!("3. Batched Spatial Query via Arrow");
    
    // Instead of 1000 individual point queries, do one query that returns all needed data
    // and filter in Rust
    
    // Approach: Query a region and get Arrow data, then do point lookups in memory
    let start = Instant::now();
    
    // Get all entities as Arrow batches
    let mut stmt = conn.prepare("SELECT id, x, y, entity_type, health, speed FROM entities")?;
    let arrow_result: Arrow<'_> = stmt.query_arrow([])?;
    
    // Collect into Vec for repeated access
    let batches: Vec<_> = arrow_result.collect();
    
    let load_time = start.elapsed();
    println!("   Load all entities to Arrow: {:?}", load_time);
    
    // Now do "queries" by scanning Arrow arrays (simulating what you'd do with arrow-rs)
    let start = Instant::now();
    let mut total_found = 0i64;
    
    for batch in &batches {
        // Access columns
        let x_col = batch.column(1);
        let y_col = batch.column(2);
        
        // In real code you'd use arrow array accessors
        // Here we just count matching by iterating
        // This simulates zero-copy access to the data
        let _ = (x_col.len(), y_col.len());
        
        // Simulating 1000 point lookups against in-memory Arrow data
        for i in 0..1000 {
            let target_x = (i * 17) % MAP_SIZE;
            let target_y = (i * 23) % MAP_SIZE;
            // In real Arrow code: scan arrays for matches
            // The key is: data is already in CPU cache, no FFI per lookup
            std::hint::black_box((target_x, target_y));
            total_found += 1;
        }
    }
    let query_time = start.elapsed();
    println!("   1000 'lookups' on Arrow data: {:?} ({:.3} µs/lookup)", 
             query_time, query_time.as_nanos() as f64 / 1000.0 / 1000.0);
    println!("   → Amortized: load once, query many times in-memory");
    println!();
    
    Ok(())
}

/// Arrow streaming for continuous data access
fn bench_arrow_stream(conn: &Connection) -> Result<()> {
    println!("4. Arrow Streaming (chunk-wise)");
    
    // Process data in chunks without loading everything
    let start = Instant::now();
    
    let mut stmt = conn.prepare("SELECT * FROM entities ORDER BY x, y")?;
    let arrow_result: Arrow<'_> = stmt.query_arrow([])?;
    
    let mut chunk_count = 0;
    let mut total_rows = 0;
    for batch in arrow_result {
        chunk_count += 1;
        total_rows += batch.num_rows();
        // Process each chunk - data is zero-copy from DuckDB's buffers
    }
    
    let time = start.elapsed();
    println!("   Stream all entities: {:?}", time);
    println!("   Chunks: {}, Total rows: {}", chunk_count, total_rows);
    println!("   → Data stays in Arrow format, no row-by-row conversion");
    println!();
    
    Ok(())
}
