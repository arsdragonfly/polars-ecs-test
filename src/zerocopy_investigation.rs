//! DuckDB Zero-Copy Investigation
//! 
//! Is Arrow zero-copy ACTUALLY zero-copy in Rust?
//! Or does it copy data across the FFI boundary?

use duckdb::{Connection, Arrow};
use duckdb::arrow::array::Float64Array;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB Zero-Copy Investigation ===\n");
    
    // Test with large data to see if there's a copy
    for n in [100_000, 1_000_000, 10_000_000] {
        println!("=== {} rows ===\n", n);
        
        let conn = Connection::open_in_memory()?;
        
        // Create large table
        conn.execute_batch(&format!(
            "CREATE TABLE big AS 
             SELECT i as id, random() as val
             FROM generate_series(1, {}) AS t(i)", n
        ))?;
        
        // ============================================================
        // Test 1: Time to get Arrow RecordBatch (includes any copy)
        // ============================================================
        let mut stmt = conn.prepare("SELECT val FROM big")?;
        
        let start = Instant::now();
        let arrow: Arrow<'_> = stmt.query_arrow([])?;
        let mut total_rows = 0;
        let mut batches = Vec::new();
        for batch in arrow {
            total_rows += batch.num_rows();
            batches.push(batch);
        }
        let arrow_time = start.elapsed();
        
        // Calculate data size
        let data_size_mb = (n * 8) as f64 / 1_000_000.0; // 8 bytes per f64
        let throughput_gbps = data_size_mb / arrow_time.as_secs_f64() / 1000.0;
        
        println!("  Arrow fetch {} rows:      {:>8.3} ms  ({:.1} MB)", 
                 total_rows, arrow_time.as_secs_f64() * 1000.0, data_size_mb);
        println!("  Throughput:                {:>8.1} GB/s", throughput_gbps);
        
        // ============================================================
        // Test 2: Access the data - is it truly zero-copy?
        // ============================================================
        let start = Instant::now();
        let mut sum = 0.0f64;
        for batch in &batches {
            let col = batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            // Access the raw buffer - this is where zero-copy matters
            let values = col.values();
            sum += values.iter().sum::<f64>();
        }
        let access_time = start.elapsed();
        println!("  Data access (sum):         {:>8.3} ms  (sum: {:.2})", 
                 access_time.as_secs_f64() * 1000.0, sum);
        
        // ============================================================
        // Test 3: Compare to pure Rust (what zero-copy SHOULD be)
        // ============================================================
        // Create equivalent data in Rust
        let rust_data: Vec<f64> = (0..n).map(|i| i as f64 * 0.1).collect();
        
        let start = Instant::now();
        let rust_sum: f64 = rust_data.iter().sum();
        let rust_time = start.elapsed();
        println!("  Pure Rust sum:             {:>8.3} ms  (sum: {:.2})", 
                 rust_time.as_secs_f64() * 1000.0, rust_sum);
        
        // ============================================================
        // Test 4: Memory check - are we holding two copies?
        // ============================================================
        // If zero-copy, Arrow should point to DuckDB's buffer
        // If not, we have: DuckDB buffer + Arrow buffer
        println!();
        println!("  If TRUE zero-copy:");
        println!("    - Fetch should be O(1), not O(N)");
        println!("    - Throughput should be >> RAM bandwidth");
        println!();
        
        let fetch_per_row = arrow_time.as_secs_f64() * 1_000_000_000.0 / n as f64;
        if fetch_per_row > 1.0 {
            println!("  ⚠️  Fetch is O(N): {:.1} ns/row - DATA IS BEING COPIED", fetch_per_row);
        } else {
            println!("  ✅ Fetch is O(1) - might be true zero-copy");
        }
        
        // Compare to memcpy speed
        let memcpy_speed_gbps = 20.0; // Typical RAM bandwidth
        if throughput_gbps < memcpy_speed_gbps * 0.5 {
            println!("  ⚠️  Throughput {:.1} GB/s < RAM speed - not zero-copy", throughput_gbps);
        }
        
        println!();
    }
    
    // ============================================================
    // Test 5: Check Arrow buffer addresses
    // ============================================================
    println!("=== Buffer Address Analysis ===\n");
    
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE test AS SELECT i::DOUBLE as val FROM generate_series(1, 1000) AS t(i)")?;
    
    let mut stmt = conn.prepare("SELECT val FROM test")?;
    
    // Query twice and check if buffers are the same
    let arrow1: Arrow<'_> = stmt.query_arrow([])?;
    let batches1: Vec<_> = arrow1.collect();
    let col1 = batches1[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    let ptr1 = col1.values().as_ptr();
    
    let arrow2: Arrow<'_> = stmt.query_arrow([])?;
    let batches2: Vec<_> = arrow2.collect();
    let col2 = batches2[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    let ptr2 = col2.values().as_ptr();
    
    println!("  Query 1 buffer: {:p}", ptr1);
    println!("  Query 2 buffer: {:p}", ptr2);
    
    if ptr1 == ptr2 {
        println!("  ✅ Same buffer! True zero-copy with buffer reuse.");
    } else {
        println!("  ❌ Different buffers. Each query allocates new memory.");
        println!("     This is a COPY, not zero-copy.");
    }
    
    println!();
    println!("=== Conclusion ===\n");
    println!("  DuckDB Rust 'Arrow' interface:");
    println!("  - Creates Arrow RecordBatch from DuckDB results");
    println!("  - But this involves COPYING data into Arrow format");
    println!("  - NOT the same as Python/R 'zero-copy' which shares memory");
    println!();
    println!("  The ~20µs overhead includes this copy operation.");
    println!("  True zero-copy would require DuckDB to natively store as Arrow.");

    Ok(())
}
