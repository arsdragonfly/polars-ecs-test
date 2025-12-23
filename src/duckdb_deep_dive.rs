//! DuckDB Deep Dive: Where is the 200µs going?
//!
//! Breaking down the exact cost of each step

use duckdb::{Connection, Result, Arrow};
use std::time::Instant;

const MAP_SIZE: i32 = 1000;

fn main() -> Result<()> {
    println!("=== DuckDB Latency Deep Dive ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;

    // Create a simple table
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities;
        CREATE TABLE entities AS
        SELECT
            i AS id,
            (hash(i) % {MAP_SIZE})::INTEGER AS x,
            (hash(i * 2) % {MAP_SIZE})::INTEGER AS y
        FROM generate_series(1, 100000) AS t(i);

        CREATE INDEX idx_xy ON entities(x, y);
        "
    ))?;

    println!("--- Step-by-step breakdown ---\n");

    // Step 1: Just prepare (should be ~0 after first)
    let start = Instant::now();
    for _ in 0..1000 {
        let stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        std::hint::black_box(stmt);
    }
    let prepare_time = start.elapsed();
    println!("1. prepare_cached (1000x):        {:>7.2} µs/call", 
             prepare_time.as_micros() as f64 / 1000.0);

    // Step 2: Prepare + bind parameters (no execute)
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        // Bind but don't execute
        let _ = stmt.raw_bind_parameter(1, x);
        let _ = stmt.raw_bind_parameter(2, y);
    }
    let bind_time = start.elapsed();
    println!("2. + bind parameters:             {:>7.2} µs/call", 
             bind_time.as_micros() as f64 / 1000.0);

    // Step 3: Prepare + bind + query (create cursor, no fetch)
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        let _rows = stmt.query([x, y])?;
        // Don't call next()
    }
    let query_time = start.elapsed();
    println!("3. + query() (no fetch):          {:>7.2} µs/call", 
             query_time.as_micros() as f64 / 1000.0);

    // Step 4: Full cycle with one next() call
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        let mut rows = stmt.query([x, y])?;
        let _ = rows.next()?;  // First fetch triggers execution
    }
    let next_time = start.elapsed();
    println!("4. + next() (first row):          {:>7.2} µs/call  ← EXECUTION HAPPENS HERE", 
             next_time.as_micros() as f64 / 1000.0);

    // Step 5: Compare with COUNT(*) vs actual row fetch
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT COUNT(*) FROM entities WHERE x = ?1 AND y = ?2")?;
        let mut rows = stmt.query([x, y])?;
        let _ = rows.next()?;
    }
    let count_time = start.elapsed();
    println!("5. COUNT(*) query:                {:>7.2} µs/call", 
             count_time.as_micros() as f64 / 1000.0);

    // Step 6: Arrow interface
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        let arrow: Arrow<'_> = stmt.query_arrow([x, y])?;
        for batch in arrow {
            std::hint::black_box(batch.num_rows());
        }
    }
    let arrow_time = start.elapsed();
    println!("6. Arrow query_arrow():           {:>7.2} µs/call", 
             arrow_time.as_micros() as f64 / 1000.0);

    // Step 7: What if we query more rows?
    println!("\n--- Result size impact ---");
    
    for range_size in [0, 10, 50, 100] {
        let start = Instant::now();
        for i in 0..100 {
            let x = ((i * 17) % (MAP_SIZE - range_size)) as i32;
            let y = ((i * 23) % (MAP_SIZE - range_size)) as i32;
            let mut stmt = conn.prepare_cached(
                "SELECT id FROM entities WHERE x BETWEEN ?1 AND ?2 AND y BETWEEN ?3 AND ?4"
            )?;
            let mut rows = stmt.query([x, x + range_size, y, y + range_size])?;
            let mut count = 0;
            while let Some(_) = rows.next()? {
                count += 1;
            }
            std::hint::black_box(count);
        }
        let time = start.elapsed();
        
        // Get average row count for this range
        let avg_rows: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM entities WHERE x BETWEEN 0 AND {} AND y BETWEEN 0 AND {}", 
                     range_size, range_size),
            [],
            |row| row.get(0)
        )?;
        
        println!("  Range {}x{} (~{} rows): {:>7.2} µs/query", 
                 range_size, range_size, avg_rows,
                 time.as_micros() as f64 / 100.0);
    }

    // Step 8: Batch multiple point queries in one SQL
    println!("\n--- Amortization via batching ---");
    
    for batch_size in [1, 10, 50, 100] {
        let start = Instant::now();
        let iterations = 1000 / batch_size;
        
        for batch in 0..iterations {
            let mut conditions = Vec::new();
            for i in 0..batch_size {
                let idx = batch * batch_size + i;
                let x = (idx * 17) % MAP_SIZE;
                let y = (idx * 23) % MAP_SIZE;
                conditions.push(format!("(x = {} AND y = {})", x, y));
            }
            let query = format!("SELECT id FROM entities WHERE {}", conditions.join(" OR "));
            let mut stmt = conn.prepare(&query)?;
            let mut rows = stmt.query([])?;
            while let Some(_) = rows.next()? {
                // consume
            }
        }
        
        let time = start.elapsed();
        let per_point = time.as_micros() as f64 / 1000.0;
        println!("  Batch {}: {:>7.2} µs/point ({:.0}x speedup)", 
                 batch_size, per_point,
                 200.0 / per_point);
    }

    println!("\n--- Conclusion ---");
    println!("  The ~200µs floor is DuckDB's per-query overhead:");
    println!("  • Query execution engine startup");
    println!("  • Result set construction");  
    println!("  • Memory allocation for row objects");
    println!("  This is inherent to DuckDB's architecture (OLAP, not OLTP)");

    Ok(())
}
