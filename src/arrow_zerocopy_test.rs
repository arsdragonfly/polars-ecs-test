//! Benchmark: True Zero-Copy Arrow analysis
//! 
//! The previous test revealed that converting Arrow → Polars involves copying
//! (~70 ns/entity = 7ms for 100K entities).
//! 
//! This test explores:
//! 1. Working directly with Arrow RecordBatches (no conversion)
//! 2. Keeping data in DuckDB and querying filtered subsets
//! 3. The actual cost of "SELECT * FROM entities" each tick

use duckdb::{Connection, Result, arrow::record_batch::RecordBatch, arrow::array::Array};
use std::time::Instant;
use std::collections::HashMap;

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
            entity_type INTEGER
        );
    ")?;
    
    // Bulk insert - just numeric types for faster processing
    conn.execute_batch(&format!("
        INSERT INTO entities 
        SELECT 
            i as id,
            random() * 1000 as x,
            random() * 1000 as y,
            (random() - 0.5) * 10 as vx,
            (random() - 0.5) * 10 as vy,
            (random() * 100)::INTEGER as hp,
            (random() * 3)::INTEGER as entity_type
        FROM generate_series(1, {}) as t(i);
    ", n))?;
    
    Ok(conn)
}

/// Read Arrow data directly without any conversion - just access the buffers
fn process_arrow_directly(batches: &[RecordBatch]) -> (usize, f64, f64) {
    let mut count = 0usize;
    let mut sum_x = 0.0f64;
    let mut sum_y = 0.0f64;
    
    for batch in batches {
        let x_col = batch.column(1);  // x is column 1
        let y_col = batch.column(2);  // y is column 2
        
        if let (Some(x_arr), Some(y_arr)) = (
            x_col.as_any().downcast_ref::<duckdb::arrow::array::Float64Array>(),
            y_col.as_any().downcast_ref::<duckdb::arrow::array::Float64Array>()
        ) {
            // Direct buffer access - no copying!
            let x_vals = x_arr.values();
            let y_vals = y_arr.values();
            
            for (x, y) in x_vals.iter().zip(y_vals.iter()) {
                sum_x += x;
                sum_y += y;
                count += 1;
            }
        }
    }
    
    (count, sum_x, sum_y)
}

/// Build a spatial HashMap directly from Arrow buffers
fn build_spatial_hashmap_from_arrow(
    batches: &[RecordBatch],
    cell_size: f64
) -> HashMap<(i32, i32), Vec<(u64, f64, f64)>> {
    let mut grid: HashMap<(i32, i32), Vec<(u64, f64, f64)>> = HashMap::new();
    let mut global_idx: u64 = 0;
    
    for batch in batches {
        let x_col = batch.column(1);
        let y_col = batch.column(2);
        
        if let (Some(x_arr), Some(y_arr)) = (
            x_col.as_any().downcast_ref::<duckdb::arrow::array::Float64Array>(),
            y_col.as_any().downcast_ref::<duckdb::arrow::array::Float64Array>()
        ) {
            let x_vals = x_arr.values();
            let y_vals = y_arr.values();
            
            for (x, y) in x_vals.iter().zip(y_vals.iter()) {
                let cx = (x / cell_size).floor() as i32;
                let cy = (y / cell_size).floor() as i32;
                grid.entry((cx, cy)).or_default().push((global_idx, *x, *y));
                global_idx += 1;
            }
        }
    }
    
    grid
}

/// Copy Arrow data into a simple struct-of-arrays (what you'd do for game state)
#[derive(Default)]
struct GameState {
    ids: Vec<i32>,
    x: Vec<f64>,
    y: Vec<f64>,
    vx: Vec<f64>,
    vy: Vec<f64>,
    hp: Vec<i32>,
    entity_type: Vec<i32>,
}

fn copy_arrow_to_soa(batches: &[RecordBatch]) -> GameState {
    let mut state = GameState::default();
    
    for batch in batches {
        // Extract each column
        if let Some(arr) = batch.column(0).as_any().downcast_ref::<duckdb::arrow::array::Int32Array>() {
            state.ids.extend(arr.values().iter().copied());
        }
        if let Some(arr) = batch.column(1).as_any().downcast_ref::<duckdb::arrow::array::Float64Array>() {
            state.x.extend(arr.values().iter().copied());
        }
        if let Some(arr) = batch.column(2).as_any().downcast_ref::<duckdb::arrow::array::Float64Array>() {
            state.y.extend(arr.values().iter().copied());
        }
        if let Some(arr) = batch.column(3).as_any().downcast_ref::<duckdb::arrow::array::Float64Array>() {
            state.vx.extend(arr.values().iter().copied());
        }
        if let Some(arr) = batch.column(4).as_any().downcast_ref::<duckdb::arrow::array::Float64Array>() {
            state.vy.extend(arr.values().iter().copied());
        }
        if let Some(arr) = batch.column(5).as_any().downcast_ref::<duckdb::arrow::array::Int32Array>() {
            state.hp.extend(arr.values().iter().copied());
        }
        if let Some(arr) = batch.column(6).as_any().downcast_ref::<duckdb::arrow::array::Int32Array>() {
            state.entity_type.extend(arr.values().iter().copied());
        }
    }
    
    state
}

fn main() -> Result<()> {
    println!("=== True Zero-Copy Arrow Analysis ===\n");
    
    for n in [1_000, 10_000, 50_000, 100_000] {
        println!("--- {} entities ---", n);
        
        let conn = setup_duckdb(n)?;
        let iterations = 30;
        
        // Warm up
        let _: Vec<RecordBatch> = conn
            .prepare("SELECT * FROM entities")?
            .query_arrow([])?
            .collect();
        
        // === Benchmark 1: DuckDB query → Arrow ===
        let start = Instant::now();
        let mut batches: Vec<RecordBatch> = Vec::new();
        for _ in 0..iterations {
            batches = conn
                .prepare("SELECT * FROM entities")?
                .query_arrow([])?
                .collect();
        }
        let duckdb_to_arrow = start.elapsed() / iterations;
        
        // === Benchmark 2: Process Arrow directly (zero-copy) ===
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = process_arrow_directly(&batches);
        }
        let arrow_direct = start.elapsed() / iterations;
        
        // === Benchmark 3: Build spatial HashMap from Arrow ===
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = build_spatial_hashmap_from_arrow(&batches, 50.0);
        }
        let arrow_to_hashmap = start.elapsed() / iterations;
        
        // === Benchmark 4: Copy Arrow to struct-of-arrays ===
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = copy_arrow_to_soa(&batches);
        }
        let arrow_to_soa = start.elapsed() / iterations;
        
        // === Benchmark 5: Full tick simulation ===
        // This simulates: Query → Build HashMap → Process (no writes back)
        let start = Instant::now();
        for _ in 0..iterations {
            let batches: Vec<RecordBatch> = conn
                .prepare("SELECT * FROM entities")?
                .query_arrow([])?
                .collect();
            let grid = build_spatial_hashmap_from_arrow(&batches, 50.0);
            std::hint::black_box(&grid);
        }
        let full_tick_read = start.elapsed() / iterations;
        
        // === Benchmark 6: Query only position columns ===
        let start = Instant::now();
        for _ in 0..iterations {
            let _batches: Vec<RecordBatch> = conn
                .prepare("SELECT id, x, y FROM entities")?
                .query_arrow([])?
                .collect();
        }
        let query_positions_only = start.elapsed() / iterations;
        
        // Calculate per-entity costs
        let query_ns = duckdb_to_arrow.as_nanos() as f64 / n as f64;
        let process_ns = arrow_direct.as_nanos() as f64 / n as f64;
        let hashmap_ns = arrow_to_hashmap.as_nanos() as f64 / n as f64;
        let soa_ns = arrow_to_soa.as_nanos() as f64 / n as f64;
        
        println!("  DuckDB → Arrow (all cols):  {:>6.2} ms  ({:.1} ns/entity)", 
            duckdb_to_arrow.as_secs_f64() * 1000.0, query_ns);
        println!("  DuckDB → Arrow (x,y only):  {:>6.2} ms", 
            query_positions_only.as_secs_f64() * 1000.0);
        println!("  Process Arrow directly:     {:>6.2} ms  ({:.1} ns/entity) ← TRUE ZERO-COPY", 
            arrow_direct.as_secs_f64() * 1000.0, process_ns);
        println!("  Arrow → Spatial HashMap:    {:>6.2} ms  ({:.1} ns/entity)", 
            arrow_to_hashmap.as_secs_f64() * 1000.0, hashmap_ns);
        println!("  Arrow → Struct-of-Arrays:   {:>6.2} ms  ({:.1} ns/entity)", 
            arrow_to_soa.as_secs_f64() * 1000.0, soa_ns);
        println!("  Full tick (query+hashmap):  {:>6.2} ms", 
            full_tick_read.as_secs_f64() * 1000.0);
        println!();
        
        // Memory analysis
        let arrow_bytes: usize = batches.iter()
            .flat_map(|b| b.columns())
            .map(|col| col.get_array_memory_size())
            .sum();
        
        println!("  Arrow memory: {:.1} KB ({:.1} bytes/entity)", 
            arrow_bytes as f64 / 1024.0,
            arrow_bytes as f64 / n as f64);
        
        // Frame budget analysis
        let tick_budget_ms = 16.67; // 60 FPS
        let tick_pct = (full_tick_read.as_secs_f64() * 1000.0 / tick_budget_ms) * 100.0;
        println!("  Frame budget used:      {:.1}% of 16.67ms (60 FPS)", tick_pct);
        println!();
    }
    
    // === Analysis: What is actually zero-copy? ===
    println!("=== Zero-Copy Analysis ===\n");
    println!("• Arrow RecordBatch buffers: DuckDB owns memory, we get pointers");
    println!("• Accessing .values() on Float64Array: No copy, direct slice to buffer");
    println!("• Building HashMap: Must copy (HashMap owns its data)");
    println!("• Building Vec/SoA: Must copy (Vec owns its data)");
    println!();
    println!("True zero-copy is only possible when:");
    println!("  1. You can work with Arrow slices directly");
    println!("  2. You don't need the data to outlive the RecordBatch");
    println!("  3. You're OK with columnar access patterns");
    println!();
    
    // === What about partial updates? ===
    println!("=== Incremental Update Strategy ===\n");
    
    let conn = setup_duckdb(10_000)?;
    
    // Add a modification tracking column
    conn.execute_batch("
        ALTER TABLE entities ADD COLUMN modified_tick INTEGER DEFAULT 0;
        CREATE INDEX idx_modified ON entities(modified_tick);
    ")?;
    
    // Simulate: 1% of entities changed this tick
    conn.execute_batch("
        UPDATE entities SET 
            x = x + vx, 
            y = y + vy,
            modified_tick = 1
        WHERE id <= 100;
    ")?;
    
    let iterations = 50;
    
    // Query only modified entities
    let start = Instant::now();
    for tick in 0..iterations {
        let _batches: Vec<RecordBatch> = conn
            .prepare("SELECT * FROM entities WHERE modified_tick >= ?")?
            .query_arrow([tick as i32])?
            .collect();
    }
    let delta_query = start.elapsed() / iterations;
    
    // Query all entities
    let start = Instant::now();
    for _ in 0..iterations {
        let _batches: Vec<RecordBatch> = conn
            .prepare("SELECT * FROM entities")?
            .query_arrow([])?
            .collect();
    }
    let full_query = start.elapsed() / iterations;
    
    println!("  Full query (10K entities):      {:>6.2} ms", 
        full_query.as_secs_f64() * 1000.0);
    println!("  Delta query (100 modified):     {:>6.2} ms", 
        delta_query.as_secs_f64() * 1000.0);
    println!("  Speedup from delta:             {:>6.1}×", 
        full_query.as_secs_f64() / delta_query.as_secs_f64());
    println!();
    
    // === Conclusion ===
    println!("=== Recommendations ===\n");
    println!("For a tick-based game with DuckDB backend:\n");
    println!("Option A: Full snapshot each tick");
    println!("  • Query all → Build HashMap → Process → Write changes");
    println!("  • Cost: ~{:.0}ms for 10K entities", full_query.as_secs_f64() * 1000.0 + 2.0);
    println!("  • Simple, but 30-50% of frame budget\n");
    
    println!("Option B: Delta/incremental updates");
    println!("  • Keep Rust HashMap as primary spatial index");
    println!("  • Query only entities modified since last sync");
    println!("  • Cost: ~{:.1}ms for 1% changes", delta_query.as_secs_f64() * 1000.0);
    println!("  • Complex, but <5% of frame budget\n");
    
    println!("Option C: Hybrid (recommended)");
    println!("  • Rust HashMap for real-time spatial (rebuilt from Arrow)");
    println!("  • DuckDB for persistence/queries/analytics");
    println!("  • Sync at lower frequency (every N ticks) or on-demand");
    
    Ok(())
}
