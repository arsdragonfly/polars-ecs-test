//! Benchmark: Arrow Zero-Copy to Polars overhead analysis
//! 
//! Tests the cost of:
//! 1. DuckDB query → Arrow RecordBatch
//! 2. Arrow RecordBatch → Polars DataFrame
//! 3. Direct Polars operations vs going through Arrow

use duckdb::{Connection, Result, arrow::record_batch::RecordBatch, arrow::array::Array};
use polars::prelude::*;
use std::time::Instant;

fn setup_duckdb(n: usize) -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    
    conn.execute_batch("
        CREATE TABLE entities (
            id INTEGER PRIMARY KEY,
            x DOUBLE,
            y DOUBLE,
            vx DOUBLE,
            vy DOUBLE,
            hp INTEGER,
            entity_type VARCHAR,
            name VARCHAR
        );
    ")?;
    
    // Bulk insert
    conn.execute_batch(&format!("
        INSERT INTO entities 
        SELECT 
            i as id,
            random() * 1000 as x,
            random() * 1000 as y,
            (random() - 0.5) * 10 as vx,
            (random() - 0.5) * 10 as vy,
            (random() * 100)::INTEGER as hp,
            CASE WHEN random() < 0.3 THEN 'enemy' 
                 WHEN random() < 0.6 THEN 'player'
                 ELSE 'npc' END as entity_type,
            'entity_' || i as name
        FROM generate_series(1, {}) as t(i);
    ", n))?;
    
    Ok(conn)
}

/// Convert DuckDB Arrow RecordBatches to Polars DataFrame
/// This requires converting through Arrow IPC format since the Arrow types differ
fn arrow_batches_to_polars(batches: &[RecordBatch]) -> PolarsResult<DataFrame> {
    if batches.is_empty() {
        return Ok(DataFrame::empty());
    }
    
    // Get schema from first batch
    let schema = batches[0].schema();
    
    // Build columns manually - this is the most direct path
    let mut columns: Vec<Column> = Vec::new();
    
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let name = field.name();
        
        // Collect all chunks for this column across batches
        let mut all_values_f64: Vec<f64> = Vec::new();
        let mut all_values_i32: Vec<i32> = Vec::new();
        let mut all_values_str: Vec<String> = Vec::new();
        
        let is_float = matches!(field.data_type(), duckdb::arrow::datatypes::DataType::Float64);
        let is_int = matches!(field.data_type(), duckdb::arrow::datatypes::DataType::Int32);
        
        for batch in batches {
            let col = batch.column(col_idx);
            
            if is_float {
                if let Some(arr) = col.as_any().downcast_ref::<duckdb::arrow::array::Float64Array>() {
                    all_values_f64.extend(arr.values().iter().copied());
                }
            } else if is_int {
                if let Some(arr) = col.as_any().downcast_ref::<duckdb::arrow::array::Int32Array>() {
                    all_values_i32.extend(arr.values().iter().copied());
                }
            } else {
                // String columns
                if let Some(arr) = col.as_any().downcast_ref::<duckdb::arrow::array::StringArray>() {
                    for i in 0..arr.len() {
                        all_values_str.push(arr.value(i).to_string());
                    }
                }
            }
        }
        
        let series = if !all_values_f64.is_empty() {
            Series::new(name.into(), all_values_f64)
        } else if !all_values_i32.is_empty() {
            Series::new(name.into(), all_values_i32)
        } else {
            Series::new(name.into(), all_values_str)
        };
        
        columns.push(series.into());
    }
    
    DataFrame::new(columns)
}

fn main() -> Result<()> {
    println!("=== Arrow Zero-Copy to Polars Overhead Analysis ===\n");
    
    for n in [1_000, 10_000, 50_000, 100_000] {
        println!("--- {} entities ---", n);
        
        let conn = setup_duckdb(n)?;
        
        // Warm up
        let _: Vec<RecordBatch> = conn
            .prepare("SELECT * FROM entities")?
            .query_arrow([])?
            .collect();
        
        // === Benchmark 1: DuckDB query to Arrow ===
        let iterations = 20;
        let start = Instant::now();
        let mut batches: Vec<RecordBatch> = Vec::new();
        for _ in 0..iterations {
            batches = conn
                .prepare("SELECT * FROM entities")?
                .query_arrow([])?
                .collect();
        }
        let duckdb_to_arrow = start.elapsed() / iterations;
        
        // === Benchmark 2: Arrow to Polars conversion ===
        let start = Instant::now();
        let mut df: Option<DataFrame> = None;
        for _ in 0..iterations {
            df = Some(arrow_batches_to_polars(&batches).expect("conversion failed"));
        }
        let arrow_to_polars_time = start.elapsed() / iterations;
        
        let df = df.unwrap();
        
        // === Benchmark 3: Polars operations on the DataFrame ===
        let start = Instant::now();
        for _ in 0..iterations {
            // Filter operation
            let _filtered = df.clone().lazy()
                .filter(col("entity_type").eq(lit("enemy")))
                .collect()
                .unwrap();
        }
        let polars_filter = start.elapsed() / iterations;
        
        // === Benchmark 4: Spatial-like query in Polars ===
        let start = Instant::now();
        for _ in 0..iterations {
            let _nearby = df.clone().lazy()
                .filter(
                    ((col("x") - lit(500.0)).pow(lit(2)) + 
                     (col("y") - lit(500.0)).pow(lit(2))).lt(lit(100.0 * 100.0))
                )
                .collect()
                .unwrap();
        }
        let polars_spatial = start.elapsed() / iterations;
        
        // === Benchmark 5: Full pipeline (DuckDB → Arrow → Polars → Filter) ===
        let start = Instant::now();
        for _ in 0..iterations {
            let batches: Vec<RecordBatch> = conn
                .prepare("SELECT * FROM entities")?
                .query_arrow([])?
                .collect();
            let df = arrow_batches_to_polars(&batches).unwrap();
            let _filtered = df.lazy()
                .filter(col("entity_type").eq(lit("enemy")))
                .collect()
                .unwrap();
        }
        let full_pipeline = start.elapsed() / iterations;
        
        // === Benchmark 6: Let DuckDB do the filter (comparison) ===
        let start = Instant::now();
        for _ in 0..iterations {
            let _batches: Vec<RecordBatch> = conn
                .prepare("SELECT * FROM entities WHERE entity_type = 'enemy'")?
                .query_arrow([])?
                .collect();
        }
        let duckdb_filter = start.elapsed() / iterations;
        
        // === Benchmark 7: Arrow batch metadata inspection ===
        let start = Instant::now();
        for _ in 0..iterations {
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let total_cols = batches.first().map(|b| b.num_columns()).unwrap_or(0);
            std::hint::black_box((total_rows, total_cols));
        }
        let arrow_metadata = start.elapsed() / iterations;
        
        // Memory analysis
        let arrow_bytes: usize = batches.iter()
            .flat_map(|b| b.columns())
            .map(|col| col.get_array_memory_size())
            .sum();
        
        println!("  DuckDB → Arrow:        {:>8.3} ms", duckdb_to_arrow.as_secs_f64() * 1000.0);
        println!("  Arrow → Polars:        {:>8.3} ms", arrow_to_polars_time.as_secs_f64() * 1000.0);
        println!("  Arrow metadata only:   {:>8.3} µs", arrow_metadata.as_secs_f64() * 1_000_000.0);
        println!("  Polars filter:         {:>8.3} ms", polars_filter.as_secs_f64() * 1000.0);
        println!("  Polars spatial:        {:>8.3} ms", polars_spatial.as_secs_f64() * 1000.0);
        println!("  Full pipeline:         {:>8.3} ms", full_pipeline.as_secs_f64() * 1000.0);
        println!("  DuckDB filter direct:  {:>8.3} ms", duckdb_filter.as_secs_f64() * 1000.0);
        println!("  Arrow data size:       {:>8.2} KB", arrow_bytes as f64 / 1024.0);
        println!("  Bytes per entity:      {:>8.2} bytes", arrow_bytes as f64 / n as f64);
        println!("  DataFrame shape:       {} rows × {} cols", df.height(), df.width());
        println!();
        
        // Breakdown analysis
        let overhead = arrow_to_polars_time.as_secs_f64() * 1000.0;
        let per_entity_ns = (arrow_to_polars_time.as_nanos() as f64) / n as f64;
        println!("  Arrow→Polars overhead: {:.1} ns/entity", per_entity_ns);
        
        // Is it truly zero-copy?
        if overhead < 0.1 && n >= 10_000 {
            println!("  ✅ Appears to be zero-copy (< 0.1ms for {}K entities)", n / 1000);
        } else if overhead < 1.0 {
            println!("  ⚠️  Minimal overhead, likely zero-copy with some wrapper allocation");
        } else {
            println!("  ❌ Non-trivial overhead, may involve copying");
        }
        println!();
    }
    
    // === Deep dive: What's in the Arrow batches? ===
    println!("=== Arrow Batch Structure Analysis ===\n");
    let conn = setup_duckdb(10_000)?;
    let batches: Vec<RecordBatch> = conn
        .prepare("SELECT * FROM entities")?
        .query_arrow([])?
        .collect();
    
    println!("Number of batches: {}", batches.len());
    for (i, batch) in batches.iter().enumerate() {
        println!("Batch {}: {} rows", i, batch.num_rows());
        println!("  Schema: {:?}", batch.schema().fields().iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect::<Vec<_>>());
        println!("  Column memory:");
        for (j, col) in batch.columns().iter().enumerate() {
            let schema = batch.schema();
            let name = schema.field(j).name();
            let size = col.get_array_memory_size();
            println!("    {}: {} bytes ({:.1} bytes/row)", 
                name, size, size as f64 / batch.num_rows() as f64);
        }
    }
    
    // === Compare: Reading into Vec vs staying in Arrow ===
    println!("\n=== Vec Materialization vs Arrow ===\n");
    
    let iterations = 20;
    
    // Arrow stays as Arrow
    let start = Instant::now();
    for _ in 0..iterations {
        let batches: Vec<RecordBatch> = conn
            .prepare("SELECT id, x, y FROM entities")?
            .query_arrow([])?
            .collect();
        std::hint::black_box(&batches);
    }
    let arrow_only = start.elapsed() / iterations;
    
    // Materialize to Vec<(i32, f64, f64)>
    let start = Instant::now();
    for _ in 0..iterations {
        let mut stmt = conn.prepare("SELECT id, x, y FROM entities")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, i32>(0)?, row.get::<_, f64>(1)?, row.get::<_, f64>(2)?))
        })?;
        let vec: Vec<_> = rows.collect::<Result<Vec<_>, _>>()?;
        std::hint::black_box(&vec);
    }
    let to_vec = start.elapsed() / iterations;
    
    println!("Arrow query (10K × 3 cols):   {:>6.3} ms", arrow_only.as_secs_f64() * 1000.0);
    println!("Materialize to Vec:           {:>6.3} ms", to_vec.as_secs_f64() * 1000.0);
    println!("Vec overhead:                 {:>6.1}×", to_vec.as_secs_f64() / arrow_only.as_secs_f64());
    
    Ok(())
}
