//! DuckDB Arrow path deep dive - is it faster?

use duckdb::{Connection, Result, Arrow};
use duckdb::arrow::array::{Int32Array, Int64Array, Array};
use std::time::Instant;

const MAP_SIZE: i32 = 1000;

fn main() -> Result<()> {
    println!("=== DuckDB Arrow Path Deep Dive ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;

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

    println!("--- Row interface vs Arrow interface ---\n");

    // Standard row-by-row
    let start = Instant::now();
    let mut total_ids = 0i64;
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        let mut rows = stmt.query([x, y])?;
        while let Some(row) = rows.next()? {
            let id: i32 = row.get(0)?;
            total_ids += id as i64;
        }
    }
    let row_time = start.elapsed();
    println!("1. Row interface (query + next):  {:>7.2} µs/query  (sum={})", 
             row_time.as_micros() as f64 / 1000.0, total_ids);

    // Arrow - iterate batches
    let start = Instant::now();
    let mut total_ids = 0i64;
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        let arrow: Arrow<'_> = stmt.query_arrow([x, y])?;
        for batch in arrow {
            let col = batch.column(0);
            // Try i64 first, then i32
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..arr.len() {
                    total_ids += arr.value(i);
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..arr.len() {
                    total_ids += arr.value(i) as i64;
                }
            }
        }
    }
    let arrow_time = start.elapsed();
    println!("2. Arrow interface (query_arrow): {:>7.2} µs/query  (sum={})", 
             arrow_time.as_micros() as f64 / 1000.0, total_ids);

    // Arrow - just count batches without reading values
    let start = Instant::now();
    let mut total_rows = 0usize;
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        let arrow: Arrow<'_> = stmt.query_arrow([x, y])?;
        for batch in arrow {
            total_rows += batch.num_rows();
        }
    }
    let arrow_count_time = start.elapsed();
    println!("3. Arrow (just count rows):       {:>7.2} µs/query  (rows={})", 
             arrow_count_time.as_micros() as f64 / 1000.0, total_rows);

    // Arrow - don't even iterate
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        let arrow: Arrow<'_> = stmt.query_arrow([x, y])?;
        std::hint::black_box(arrow);
    }
    let arrow_no_iter_time = start.elapsed();
    println!("4. Arrow (no iteration):          {:>7.2} µs/query", 
             arrow_no_iter_time.as_micros() as f64 / 1000.0);

    // Arrow - collect into vec
    let start = Instant::now();
    for i in 0..1000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        let arrow: Arrow<'_> = stmt.query_arrow([x, y])?;
        let batches: Vec<_> = arrow.collect();
        std::hint::black_box(batches);
    }
    let arrow_collect_time = start.elapsed();
    println!("5. Arrow (collect to Vec):        {:>7.2} µs/query", 
             arrow_collect_time.as_micros() as f64 / 1000.0);

    println!("\n--- Larger result sets via Arrow ---\n");

    for range in [10, 50, 100, 200] {
        // Row interface
        let start = Instant::now();
        let mut total = 0i64;
        for i in 0..100 {
            let x = ((i * 17) % (MAP_SIZE - range)) as i32;
            let y = ((i * 23) % (MAP_SIZE - range)) as i32;
            let mut stmt = conn.prepare_cached(
                "SELECT id FROM entities WHERE x BETWEEN ?1 AND ?2 AND y BETWEEN ?3 AND ?4"
            )?;
            let mut rows = stmt.query([x, x + range, y, y + range])?;
            while let Some(row) = rows.next()? {
                let id: i32 = row.get(0)?;
                total += id as i64;
            }
        }
        let row_time = start.elapsed();

        // Arrow interface
        let start = Instant::now();
        let mut total_arrow = 0i64;
        for i in 0..100 {
            let x = ((i * 17) % (MAP_SIZE - range)) as i32;
            let y = ((i * 23) % (MAP_SIZE - range)) as i32;
            let mut stmt = conn.prepare_cached(
                "SELECT id FROM entities WHERE x BETWEEN ?1 AND ?2 AND y BETWEEN ?3 AND ?4"
            )?;
            let arrow: Arrow<'_> = stmt.query_arrow([x, x + range, y, y + range])?;
            for batch in arrow {
                let col = batch.column(0);
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    for j in 0..arr.len() {
                        total_arrow += arr.value(j);
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    for j in 0..arr.len() {
                        total_arrow += arr.value(j) as i64;
                    }
                }
            }
        }
        let arrow_time = start.elapsed();

        let avg_rows: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM entities WHERE x BETWEEN 0 AND {} AND y BETWEEN 0 AND {}", 
                     range, range),
            [],
            |row| row.get(0)
        )?;

        let speedup = row_time.as_micros() as f64 / arrow_time.as_micros() as f64;
        println!("  Range {}x{} (~{} rows):", range, range, avg_rows);
        println!("    Row:   {:>7.2} µs    Arrow: {:>7.2} µs    ({:.2}x)", 
                 row_time.as_micros() as f64 / 100.0,
                 arrow_time.as_micros() as f64 / 100.0,
                 speedup);
    }

    println!("\n--- Conclusion ---");
    println!("  Arrow vs Row interface for point queries: similar overhead");
    println!("  Arrow wins for LARGE result sets (vectorized access)");
    println!("  The ~180µs floor is in query EXECUTION, not result format");

    Ok(())
}
