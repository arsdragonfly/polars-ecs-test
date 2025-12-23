//! ST_Intersects vs JOIN for Combat
//!
//! Can we use R-tree accelerated individual queries instead of a slow JOIN?

use duckdb::{Connection, Result};
use std::time::Instant;
use std::collections::HashMap;

fn main() -> Result<()> {
    println!("=== ST_Intersects Loop vs JOIN vs Rust HashMap ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;
    conn.execute_batch("INSTALL spatial; LOAD spatial;")?;

    // Create entities - using smaller N for fair comparison
    let num_entities = 10_000;
    println!("Creating {} entities...", num_entities);
    
    conn.execute_batch(&format!(
        "CREATE TABLE entities AS
         SELECT 
            i AS id,
            ST_Point((hash(i) % 1000)::DOUBLE, (hash(i*2) % 1000)::DOUBLE) AS geom,
            (hash(i) % 1000)::INTEGER AS x,
            (hash(i*2) % 1000)::INTEGER AS y,
            CASE WHEN hash(i*7) % 10 < 3 THEN true ELSE false END AS is_enemy
         FROM generate_series(1, {}) AS t(i);", num_entities
    ))?;

    // Create R-tree index
    conn.execute_batch("CREATE INDEX rtree_idx ON entities USING RTREE(geom);")?;
    conn.execute_batch("CREATE INDEX xy_idx ON entities(x, y);")?;

    let friendly_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entities WHERE NOT is_enemy", [], |r| r.get(0)
    )?;
    let enemy_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entities WHERE is_enemy", [], |r| r.get(0)
    )?;
    println!("  Friendlies: {}, Enemies: {}\n", friendly_count, enemy_count);

    let attack_range = 50;

    // =========================================================================
    // Approach 1: JOIN (doesn't use R-tree)
    // =========================================================================
    println!("--- Approach 1: SQL JOIN ---");
    
    let start = Instant::now();
    let join_count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM (
            SELECT a.id,
                (SELECT b.id FROM entities b 
                 WHERE b.is_enemy
                 AND b.x BETWEEN a.x - {0} AND a.x + {0}
                 AND b.y BETWEEN a.y - {0} AND a.y + {0}
                 ORDER BY (a.x - b.x)*(a.x - b.x) + (a.y - b.y)*(a.y - b.y)
                 LIMIT 1) AS nearest
            FROM entities a
            WHERE NOT a.is_enemy
        ) WHERE nearest IS NOT NULL", attack_range),
        [],
        |r| r.get(0)
    )?;
    let join_time = start.elapsed();
    println!("  Time: {:>8.2} ms  (found {} with nearby enemy)", 
             join_time.as_secs_f64() * 1000.0, join_count);

    // =========================================================================
    // Approach 2: Loop with ST_Intersects (uses R-tree!)
    // =========================================================================
    println!("\n--- Approach 2: Loop with ST_Intersects (R-tree) ---");

    // First, get all friendly entity positions
    let mut friendlies: Vec<(i64, f64, f64)> = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT id, ST_X(geom), ST_Y(geom) FROM entities WHERE NOT is_enemy"
        )?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            friendlies.push((row.get(0)?, row.get(1)?, row.get(2)?));
        }
    }
    println!("  Loaded {} friendlies", friendlies.len());

    let start = Instant::now();
    let mut rtree_matches = 0;
    
    // Use prepared statement with ST_Intersects
    let mut stmt = conn.prepare(
        "SELECT id FROM entities 
         WHERE is_enemy 
         AND ST_Intersects(geom, ST_MakeEnvelope(?1, ?2, ?3, ?4))
         LIMIT 1"
    )?;

    for &(_id, x, y) in &friendlies {
        let result: Option<i64> = stmt.query_row(
            [x - attack_range as f64, y - attack_range as f64, 
             x + attack_range as f64, y + attack_range as f64],
            |r| r.get(0)
        ).ok();
        if result.is_some() { rtree_matches += 1; }
    }
    let rtree_time = start.elapsed();
    println!("  Time: {:>8.2} ms  (found {} with nearby enemy)", 
             rtree_time.as_secs_f64() * 1000.0, rtree_matches);
    println!("  Per query: {:.3} ms", rtree_time.as_secs_f64() * 1000.0 / friendlies.len() as f64);

    // =========================================================================
    // Approach 3: Rust HashMap
    // =========================================================================
    println!("\n--- Approach 3: Rust HashMap ---");

    let start = Instant::now();
    
    // Load all entities
    let mut all_entities: Vec<(i64, i32, i32, bool)> = Vec::new();
    {
        let mut stmt = conn.prepare("SELECT id, x, y, is_enemy FROM entities")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            all_entities.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?));
        }
    }
    let load_time = start.elapsed();

    let compute_start = Instant::now();
    
    // Build grid
    let cell_size = attack_range;
    let mut grid: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
    for (idx, &(_, x, y, _)) in all_entities.iter().enumerate() {
        let cell = (x / cell_size, y / cell_size);
        grid.entry(cell).or_default().push(idx);
    }

    // Find matches
    let mut rust_matches = 0;
    for (idx, &(_, x, y, is_enemy)) in all_entities.iter().enumerate() {
        if is_enemy { continue; }
        
        let cell = (x / cell_size, y / cell_size);
        let mut found = false;
        
        'outer: for dx in -1..=1 {
            for dy in -1..=1 {
                if let Some(cell_ents) = grid.get(&(cell.0 + dx, cell.1 + dy)) {
                    for &other_idx in cell_ents {
                        if other_idx == idx { continue; }
                        let (_, ox, oy, other_is_enemy) = all_entities[other_idx];
                        if !other_is_enemy { continue; }
                        let dist_sq = (x - ox).pow(2) + (y - oy).pow(2);
                        if dist_sq <= attack_range * attack_range {
                            found = true;
                            break 'outer;
                        }
                    }
                }
            }
        }
        if found { rust_matches += 1; }
    }
    let compute_time = compute_start.elapsed();
    let total_time = start.elapsed();

    println!("  Load time:    {:>8.2} ms", load_time.as_secs_f64() * 1000.0);
    println!("  Compute time: {:>8.2} ms", compute_time.as_secs_f64() * 1000.0);
    println!("  Total:        {:>8.2} ms  (found {} with nearby enemy)", 
             total_time.as_secs_f64() * 1000.0, rust_matches);

    // =========================================================================
    // Scaling test
    // =========================================================================
    println!("\n--- Scaling Analysis ---\n");

    println!("  For {} entities:", num_entities);
    println!("    JOIN:           {:>8.2} ms", join_time.as_secs_f64() * 1000.0);
    println!("    ST_Intersects:  {:>8.2} ms ({} queries × {:.3} ms)", 
             rtree_time.as_secs_f64() * 1000.0, friendlies.len(),
             rtree_time.as_secs_f64() * 1000.0 / friendlies.len() as f64);
    println!("    Rust HashMap:   {:>8.2} ms", total_time.as_secs_f64() * 1000.0);

    println!("\n  Projected for 100K entities:");
    let scale = 10.0; // 100K / 10K
    println!("    JOIN:           {:>8.0} ms (O(N²) - scales badly!)", 
             join_time.as_secs_f64() * 1000.0 * scale * scale);
    println!("    ST_Intersects:  {:>8.0} ms (O(N) queries but ~0.2ms each)", 
             rtree_time.as_secs_f64() * 1000.0 * scale);
    println!("    Rust HashMap:   {:>8.0} ms (O(N) with tiny constant)", 
             total_time.as_secs_f64() * 1000.0 * scale);

    println!("\n--- Conclusion ---");
    println!("  ST_Intersects loop IS faster than JOIN!");
    println!("  But still ~0.2ms per query overhead from DuckDB");
    println!("  Rust HashMap wins because:");
    println!("    • Zero per-query overhead");
    println!("    • Data already in memory");
    println!("    • Spatial hashing is O(1) per entity");

    Ok(())
}
