//! Arrow Zero-Copy Overhead Test
//! 
//! Can Arrow reduce DuckDB's per-query overhead (~60-100µs)?
//! 
//! The overhead comes from:
//! 1. Query parsing/planning (unavoidable without prepared statements)
//! 2. Result materialization (Arrow could help here!)
//! 3. Rust FFI crossing (Arrow could help here!)

use duckdb::{Connection, Arrow};
use duckdb::arrow::array::Float64Array;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Arrow Zero-Copy Overhead Test ===\n");
    println!("Goal: Minimize DuckDB per-query overhead\n");

    for n in [1000, 5000, 10000] {
        println!("=== {} rows ===\n", n);
        
        let conn = Connection::open_in_memory()?;
        
        // Create test data
        conn.execute_batch(&format!(
            "CREATE TABLE pairs AS 
             SELECT i as id,
                    [(i * 17) % 1000, (i * 23) % 1000]::DOUBLE[2] as pos1,
                    [(i * 31) % 1000, (i * 37) % 1000]::DOUBLE[2] as pos2
             FROM generate_series(0, {}) AS t(i)",
            n - 1
        ))?;
        
        // ============================================================
        // 1. Standard: query_row with scalar result
        // ============================================================
        let query = "SELECT sum(array_distance(pos1, pos2)) FROM pairs";
        let mut stmt = conn.prepare(query)?;
        
        // Warmup
        let _: f64 = stmt.query_row([], |r| r.get(0))?;
        
        let start = Instant::now();
        let mut sum = 0.0f64;
        for _ in 0..1000 {
            sum = stmt.query_row([], |r| r.get(0))?;
        }
        let standard_time = start.elapsed();
        println!("  Standard query_row (1000):  {:>8.3} ms  (sum: {:.2})", 
                 standard_time.as_secs_f64() * 1000.0, sum);
        println!("    └─ Per-query:             {:>8.1} µs", 
                 standard_time.as_secs_f64() * 1_000_000.0 / 1000.0);

        // ============================================================
        // 2. Arrow: query_arrow with RecordBatch
        // ============================================================
        // Warmup
        {
            let arrow: Arrow<'_> = stmt.query_arrow([])?;
            for batch in arrow {
                let _ = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
            }
        }
        
        let start = Instant::now();
        for _ in 0..1000 {
            let arrow: Arrow<'_> = stmt.query_arrow([])?;
            for batch in arrow {
                sum = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
            }
        }
        let arrow_time = start.elapsed();
        println!("  Arrow query_arrow (1000):   {:>8.3} ms  (sum: {:.2})", 
                 arrow_time.as_secs_f64() * 1000.0, sum);
        println!("    └─ Per-query:             {:>8.1} µs  ({:.2}× vs standard)", 
                 arrow_time.as_secs_f64() * 1_000_000.0 / 1000.0,
                 arrow_time.as_secs_f64() / standard_time.as_secs_f64());

        // ============================================================
        // 3. Arrow with multi-row results (where Arrow shines)
        // ============================================================
        let multi_query = "SELECT array_distance(pos1, pos2) as dist FROM pairs";
        let mut multi_stmt = conn.prepare(multi_query)?;
        
        // Warmup
        {
            let arrow: Arrow<'_> = multi_stmt.query_arrow([])?;
            let mut s = 0.0f64;
            for batch in arrow {
                let col = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..col.len() {
                    s += col.value(i);
                }
            }
            std::hint::black_box(s);
        }
        
        let start = Instant::now();
        for _ in 0..1000 {
            let arrow: Arrow<'_> = multi_stmt.query_arrow([])?;
            sum = 0.0;
            for batch in arrow {
                let col = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
                // Zero-copy access to the data!
                let slice = col.values();
                sum += slice.iter().sum::<f64>();
            }
        }
        let arrow_multi_time = start.elapsed();
        println!("  Arrow multi-row (1000):     {:>8.3} ms  (sum: {:.2})", 
                 arrow_multi_time.as_secs_f64() * 1000.0, sum);
        println!("    └─ Per-query:             {:>8.1} µs  ({:.2}× vs standard)", 
                 arrow_multi_time.as_secs_f64() * 1_000_000.0 / 1000.0,
                 arrow_multi_time.as_secs_f64() / standard_time.as_secs_f64());

        // ============================================================
        // 4. Standard row iteration (for comparison)
        // ============================================================
        let start = Instant::now();
        for _ in 0..1000 {
            let mut rows = multi_stmt.query([])?;
            sum = 0.0;
            while let Some(row) = rows.next()? {
                let dist: f64 = row.get(0)?;
                sum += dist;
            }
        }
        let row_iter_time = start.elapsed();
        println!("  Row iteration (1000):       {:>8.3} ms  (sum: {:.2})", 
                 row_iter_time.as_secs_f64() * 1000.0, sum);
        println!("    └─ Per-query:             {:>8.1} µs  ({:.2}× vs standard)", 
                 row_iter_time.as_secs_f64() * 1_000_000.0 / 1000.0,
                 row_iter_time.as_secs_f64() / standard_time.as_secs_f64());

        // ============================================================
        // 5. Batch multiple operations in one query
        // ============================================================
        // Instead of 1000 separate queries, do 1 query with 1000× data
        conn.execute_batch("CREATE TABLE pairs_big AS SELECT * FROM pairs, generate_series(1, 1000)")?;
        let big_query = "SELECT sum(array_distance(pos1, pos2)) FROM pairs_big";
        let mut big_stmt = conn.prepare(big_query)?;
        
        // Warmup
        let _: f64 = big_stmt.query_row([], |r| r.get(0))?;
        
        let start = Instant::now();
        let big_sum: f64 = big_stmt.query_row([], |r| r.get(0))?;
        let single_big_time = start.elapsed();
        println!("  Single query ({}K rows):  {:>9.3} ms  (sum: {:.2})", 
                 n, single_big_time.as_secs_f64() * 1000.0, big_sum / 1000.0);
        println!("    └─ Effective per-{}:    {:>9.1} µs", 
                 n, single_big_time.as_secs_f64() * 1_000_000.0 / 1000.0);

        println!();
    }

    println!("=== Key Insights ===\n");
    println!("  1. Arrow doesn't reduce per-QUERY overhead");
    println!("     (query parsing/planning still happens)");
    println!();
    println!("  2. Arrow DOES help with per-ROW overhead");
    println!("     (zero-copy access to result buffers)");
    println!();
    println!("  3. To minimize overhead, BATCH operations:");
    println!("     - 1 query over 1M rows >> 1000 queries over 1K rows");
    println!();
    println!("  4. For real-time games with many small queries:");
    println!("     - Use Rust/Lua SIMD directly");
    println!("     - DuckDB is better for analytics, not per-frame queries");

    Ok(())
}
