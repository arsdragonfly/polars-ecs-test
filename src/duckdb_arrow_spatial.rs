//! DuckDB + Arrow Zero-Copy Spatial Lookups
//!
//! The pattern: Load entity data via Arrow once, query in-memory many times

use duckdb::{Connection, Result, Arrow};
use duckdb::arrow::array::{Array, Int32Array, Int64Array, Float32Array, Float64Array, RecordBatch};
use std::time::Instant;
use std::collections::HashMap;

const ENTITY_COUNT: i32 = 100_000;
const MAP_SIZE: i32 = 1000;

fn main() -> Result<()> {
    println!("=== Arrow Zero-Copy Spatial Query Pattern ===\n");

    let conn = Connection::open_in_memory()?;
    
    // Setup
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities;
        CREATE TABLE entities AS
        SELECT
            i AS id,
            (random() * {MAP_SIZE})::INTEGER AS x,
            (random() * {MAP_SIZE})::INTEGER AS y,
            (i % 10) AS entity_type,
            (50 + random() * 50)::FLOAT AS health,
            (1 + random() * 5)::FLOAT AS speed
        FROM generate_series(1, {ENTITY_COUNT}) AS t(i);
        "
    ))?;

    println!("Entities: {}, Map: {}x{}\n", ENTITY_COUNT, MAP_SIZE, MAP_SIZE);

    // =========================================================================
    // Pattern: Load via Arrow, build spatial index, query in-memory
    // =========================================================================

    println!("--- Step 1: Load entities via Arrow ---");
    let start = Instant::now();
    
    let mut stmt = conn.prepare("SELECT id, x, y, entity_type, health, speed FROM entities")?;
    let arrow_result: Arrow<'_> = stmt.query_arrow([])?;
    
    // Collect Arrow batches
    let batches: Vec<_> = arrow_result.collect();
    let load_time = start.elapsed();
    
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("  Loaded {} rows in {:?}", total_rows, load_time);
    println!("  Batches: {}", batches.len());

    // =========================================================================
    // Step 2: Build spatial index from Arrow data
    // =========================================================================
    
    println!("\n--- Step 2: Build spatial HashMap from Arrow ---");
    let start = Instant::now();
    
    // Extract Arrow arrays and build a spatial hash
    let mut spatial_index: HashMap<(i32, i32), Vec<EntityData>> = HashMap::new();
    
    for batch in &batches {
        // Get typed arrays from Arrow RecordBatch
        // DuckDB may return different int types, so we handle them
        let id_array = batch.column(0);
        let x_array = batch.column(1);
        let y_array = batch.column(2);
        let type_array = batch.column(3);
        let health_array = batch.column(4);
        let speed_array = batch.column(5);
        
        // Use dynamic access for compatibility
        for i in 0..batch.num_rows() {
            // Extract values using arrow's ScalarValue or direct casting
            let id = get_i64(id_array, i);
            let x = get_i32(x_array, i);
            let y = get_i32(y_array, i);
            let entity_type = get_i32(type_array, i);
            let health = get_f32(health_array, i);
            let speed = get_f32(speed_array, i);
            
            let entity = EntityData {
                id,
                x,
                y,
                entity_type,
                health,
                speed,
            };
            spatial_index.entry((entity.x, entity.y)).or_default().push(entity);
        }
    }
    
    let index_time = start.elapsed();
    println!("  Built spatial index in {:?}", index_time);
    println!("  Unique positions: {}", spatial_index.len());

    // =========================================================================
    // Step 3: Fast in-memory spatial queries
    // =========================================================================
    
    println!("\n--- Step 3: In-memory spatial queries ---");
    
    // Point queries
    let start = Instant::now();
    let mut found = 0i64;
    for i in 0..10_000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        if let Some(entities) = spatial_index.get(&(x, y)) {
            found += entities.len() as i64;
        }
    }
    let point_time = start.elapsed();
    println!("  10K point queries: {:?} ({:.3} µs/query)", point_time, point_time.as_nanos() as f64 / 10_000.0 / 1000.0);
    println!("    Found: {} entities", found);
    
    // Range queries (10x10 area)
    let start = Instant::now();
    let mut found = 0i64;
    for i in 0..1_000 {
        let x = ((i * 17) % (MAP_SIZE - 10)) as i32;
        let y = ((i * 23) % (MAP_SIZE - 10)) as i32;
        for dx in 0..10 {
            for dy in 0..10 {
                if let Some(entities) = spatial_index.get(&(x + dx, y + dy)) {
                    found += entities.len() as i64;
                }
            }
        }
    }
    let range_time = start.elapsed();
    println!("  1K range queries (10x10): {:?} ({:.2} µs/query)", range_time, range_time.as_micros() as f64 / 1000.0);
    println!("    Found: {} entities", found);

    // =========================================================================
    // Compare: DuckDB query vs Arrow+HashMap
    // =========================================================================
    
    println!("\n--- Comparison ---");
    
    // DuckDB point query
    let start = Instant::now();
    for i in 0..1_000 {
        let x = (i * 17) % MAP_SIZE;
        let y = (i * 23) % MAP_SIZE;
        let _: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM entities WHERE x = {} AND y = {}", x, y),
            [],
            |row| row.get(0)
        )?;
    }
    let duckdb_time = start.elapsed();
    
    // Arrow + HashMap point query
    let start = Instant::now();
    for i in 0..1_000 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let _ = spatial_index.get(&(x, y)).map(|v| v.len()).unwrap_or(0);
    }
    let arrow_time = start.elapsed();
    
    println!("  1K point queries:");
    println!("    DuckDB direct:    {:>10.2} µs/query", duckdb_time.as_micros() as f64 / 1000.0);
    println!("    Arrow + HashMap:  {:>10.3} µs/query", arrow_time.as_nanos() as f64 / 1000.0 / 1000.0);
    println!("    Speedup:          {:>10.0}x", duckdb_time.as_nanos() as f64 / arrow_time.as_nanos() as f64);

    // =========================================================================
    // Pattern summary
    // =========================================================================
    
    println!("\n--- Recommended Pattern for Games ---");
    println!("
  ┌─────────────────────────────────────────────────────────┐
  │  WRITE PATH (infrequent):                               │
  │    Game logic → DuckDB (UPDATE/INSERT)                  │
  │                                                         │
  │  READ PATH (every frame):                               │
  │    1. Arrow query: SELECT * FROM entities               │
  │    2. Build HashMap/spatial index from Arrow arrays     │
  │    3. Use HashMap for all frame queries                 │
  │                                                         │
  │  Sync frequency: Once per frame or less                 │
  │    (2.5ms to load 100K entities is ~15% of frame)       │
  └─────────────────────────────────────────────────────────┘
");

    let total_sync_time = load_time + index_time;
    println!("  Total sync cost: {:?} ({:.1}% of 16.67ms frame)", 
             total_sync_time, 
             total_sync_time.as_micros() as f64 / 16670.0 * 100.0);

    Ok(())
}

#[derive(Debug, Clone)]
struct EntityData {
    id: i64,
    x: i32,
    y: i32,
    entity_type: i32,
    health: f32,
    speed: f32,
}

// Helper functions to extract values from Arrow arrays regardless of exact type
fn get_i64(array: &dyn Array, i: usize) -> i64 {
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return a.value(i);
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return a.value(i) as i64;
    }
    0
}

fn get_i32(array: &dyn Array, i: usize) -> i32 {
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return a.value(i);
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return a.value(i) as i32;
    }
    0
}

fn get_f32(array: &dyn Array, i: usize) -> f32 {
    if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        return a.value(i);
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        return a.value(i) as f32;
    }
    0.0
}
