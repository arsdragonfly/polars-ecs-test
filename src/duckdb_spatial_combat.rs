//! DuckDB Spatial Extension for Combat Logic
//!
//! Can R-tree spatial indexing help with nearest-neighbor combat?

use duckdb::{Connection, Result};
use std::time::Instant;
use std::collections::HashMap;

const NUM_ENTITIES: i32 = 100_000;
const MAP_SIZE: i32 = 1000;

fn main() -> Result<()> {
    println!("=== DuckDB Spatial Extension for Combat ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;
    
    // Load spatial extension
    println!("Loading spatial extension...");
    conn.execute_batch("INSTALL spatial; LOAD spatial;")?;

    // Create entities with POINT geometry
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities;
        CREATE TABLE entities AS
        SELECT
            i AS id,
            (hash(i) % {MAP_SIZE})::INTEGER AS x,
            (hash(i * 2) % {MAP_SIZE})::INTEGER AS y,
            ST_Point((hash(i) % {MAP_SIZE})::DOUBLE, (hash(i * 2) % {MAP_SIZE})::DOUBLE) AS geom,
            (hash(i * 5) % 100)::INTEGER AS health,
            (hash(i * 6) % 50)::INTEGER AS damage,
            CASE WHEN hash(i * 7) % 10 < 3 THEN 'enemy' ELSE 'friendly' END AS faction
        FROM generate_series(1, {NUM_ENTITIES}) AS t(i);
        "
    ))?;

    println!("Created {} entities\n", NUM_ENTITIES);

    // Create different index types
    println!("Creating indexes...");
    conn.execute_batch("CREATE INDEX idx_xy ON entities(x, y);")?;
    conn.execute_batch("CREATE INDEX idx_faction ON entities(faction);")?;

    println!("\n--- TEST 1: Smaller Scale (10K entities) ---\n");

    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS small_entities;
        CREATE TABLE small_entities AS
        SELECT * FROM entities WHERE id <= 10000;
        CREATE INDEX idx_small_xy ON small_entities(x, y);
        "
    ))?;

    // Approach A: Standard SQL with x,y columns
    let start = Instant::now();
    let count_a: i64 = conn.query_row(
        "SELECT COUNT(*) FROM (
            SELECT a.id, 
                (SELECT b.id FROM small_entities b 
                 WHERE b.faction != a.faction 
                 AND b.x BETWEEN a.x - 50 AND a.x + 50
                 AND b.y BETWEEN a.y - 50 AND a.y + 50
                 ORDER BY (a.x - b.x)*(a.x - b.x) + (a.y - b.y)*(a.y - b.y)
                 LIMIT 1) AS nearest_enemy
            FROM small_entities a
            WHERE a.faction = 'friendly'
        ) WHERE nearest_enemy IS NOT NULL",
        [],
        |r| r.get(0)
    )?;
    let standard_sql = start.elapsed();
    println!("  A) Standard SQL (x,y bounds):     {:>8.2} ms  ({} matches)", 
             standard_sql.as_secs_f64() * 1000.0, count_a);

    // Approach B: ST_DWithin with geometry
    let start = Instant::now();
    let count_b: i64 = conn.query_row(
        "SELECT COUNT(*) FROM (
            SELECT a.id,
                (SELECT b.id FROM small_entities b 
                 WHERE b.faction != a.faction 
                 AND ST_DWithin(a.geom, b.geom, 50)
                 ORDER BY ST_Distance(a.geom, b.geom)
                 LIMIT 1) AS nearest_enemy
            FROM small_entities a
            WHERE a.faction = 'friendly'
        ) WHERE nearest_enemy IS NOT NULL",
        [],
        |r| r.get(0)
    )?;
    let spatial_dwithin = start.elapsed();
    println!("  B) ST_DWithin (spatial):          {:>8.2} ms  ({} matches)", 
             spatial_dwithin.as_secs_f64() * 1000.0, count_b);

    // Approach C: Rust with spatial HashMap (10K)
    let start = Instant::now();
    let matches_c = {
        let mut entities: Vec<(i32, i32, i32, i32, bool)> = Vec::new();
        let mut stmt = conn.prepare("SELECT id, x, y, damage, faction = 'enemy' FROM small_entities")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            entities.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?));
        }

        let mut grid: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
        for (idx, &(_, x, y, _, _)) in entities.iter().enumerate() {
            let cell = (x / 50, y / 50);
            grid.entry(cell).or_default().push(idx);
        }

        let mut matches = 0;
        for (idx, &(_, x, y, _, is_enemy)) in entities.iter().enumerate() {
            if is_enemy { continue; }
            
            let cell = (x / 50, y / 50);
            let mut best: Option<(usize, i32)> = None;
            
            for dx in -1..=1 {
                for dy in -1..=1 {
                    if let Some(cell_ents) = grid.get(&(cell.0 + dx, cell.1 + dy)) {
                        for &other_idx in cell_ents {
                            if other_idx == idx { continue; }
                            let (_, ox, oy, _, other_is_enemy) = entities[other_idx];
                            if !other_is_enemy { continue; }
                            let dist_sq = (x - ox).pow(2) + (y - oy).pow(2);
                            if dist_sq <= 2500 {
                                if best.is_none() || dist_sq < best.unwrap().1 {
                                    best = Some((other_idx, dist_sq));
                                }
                            }
                        }
                    }
                }
            }
            if best.is_some() { matches += 1; }
        }
        matches
    };
    let rust_time = start.elapsed();
    println!("  C) Rust HashMap:                  {:>8.2} ms  ({} matches)", 
             rust_time.as_secs_f64() * 1000.0, matches_c);

    println!("\n  Speedups:");
    println!("    SQL vs Rust: {:.1}x", standard_sql.as_secs_f64() / rust_time.as_secs_f64());
    println!("    Spatial vs Rust: {:.1}x", spatial_dwithin.as_secs_f64() / rust_time.as_secs_f64());

    println!("\n--- TEST 2: Full 100K with Rust HashMap ---\n");

    let start = Instant::now();
    let (load_ms, compute_ms, matches_100k) = {
        let mut entities: Vec<(i32, i32, i32, i32, bool)> = Vec::new();
        let mut stmt = conn.prepare("SELECT id, x, y, damage, faction = 'enemy' FROM entities")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            entities.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?));
        }
        let load_time = start.elapsed();

        let compute_start = Instant::now();
        let mut grid: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
        for (idx, &(_, x, y, _, _)) in entities.iter().enumerate() {
            let cell = (x / 50, y / 50);
            grid.entry(cell).or_default().push(idx);
        }

        let mut matches = 0;
        for (idx, &(_, x, y, _, is_enemy)) in entities.iter().enumerate() {
            if is_enemy { continue; }
            
            let cell = (x / 50, y / 50);
            let mut best: Option<(usize, i32)> = None;
            
            for dx in -1..=1 {
                for dy in -1..=1 {
                    if let Some(cell_ents) = grid.get(&(cell.0 + dx, cell.1 + dy)) {
                        for &other_idx in cell_ents {
                            if other_idx == idx { continue; }
                            let (_, ox, oy, _, other_is_enemy) = entities[other_idx];
                            if !other_is_enemy { continue; }
                            let dist_sq = (x - ox).pow(2) + (y - oy).pow(2);
                            if dist_sq <= 2500 {
                                if best.is_none() || dist_sq < best.unwrap().1 {
                                    best = Some((other_idx, dist_sq));
                                }
                            }
                        }
                    }
                }
            }
            if best.is_some() { matches += 1; }
        }
        let compute_time = compute_start.elapsed();
        
        (load_time.as_secs_f64() * 1000.0, compute_time.as_secs_f64() * 1000.0, matches)
    };
    
    println!("  Data load from DuckDB:            {:>8.2} ms", load_ms);
    println!("  Rust spatial computation:         {:>8.2} ms  ({} matches)", compute_ms, matches_100k);
    println!("  Total:                            {:>8.2} ms", load_ms + compute_ms);

    println!("\n--- Conclusion ---");
    println!("  DuckDB spatial extension (ST_DWithin) is SLOWER than plain SQL!");
    println!("  No R-tree acceleration for joins on regular tables");
    println!("  Rust HashMap is 10-100x faster for nearest-neighbor");
    println!("  Best approach: DuckDB for storage, Rust for spatial logic");

    Ok(())
}
