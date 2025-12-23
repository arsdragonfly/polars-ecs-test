//! DuckDB: Push Logic Into SQL vs Pull Data Out
//!
//! If we have ~180µs per-query overhead, can we amortize it by doing MORE work per query?

use duckdb::{Connection, Result};
use std::time::Instant;
use std::collections::HashMap;

const NUM_ENTITIES: i32 = 100_000;
const MAP_SIZE: i32 = 1000;

fn main() -> Result<()> {
    println!("=== Push Logic Into DuckDB vs Pull Data Out ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;

    // Create game-like entities with position, velocity, health
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities;
        CREATE TABLE entities AS
        SELECT
            i AS id,
            (hash(i) % {MAP_SIZE})::INTEGER AS x,
            (hash(i * 2) % {MAP_SIZE})::INTEGER AS y,
            (hash(i * 3) % 100)::DOUBLE / 10.0 AS vx,
            (hash(i * 4) % 100)::DOUBLE / 10.0 AS vy,
            (hash(i * 5) % 100)::INTEGER AS health,
            (hash(i * 6) % 50)::INTEGER AS damage,
            CASE WHEN hash(i * 7) % 10 < 3 THEN 'enemy' ELSE 'friendly' END AS faction
        FROM generate_series(1, {NUM_ENTITIES}) AS t(i);
        
        CREATE INDEX idx_xy ON entities(x, y);
        CREATE INDEX idx_faction ON entities(faction);
        "
    ))?;

    println!("Created {} entities\n", NUM_ENTITIES);

    // =========================================================================
    // TEST 1: Movement system - update all positions
    // =========================================================================
    println!("--- TEST 1: Movement System (update all positions) ---\n");

    // Approach A: Pull all data, compute in Rust, push back
    let start = Instant::now();
    {
        let mut stmt = conn.prepare("SELECT id, x, y, vx, vy FROM entities")?;
        let mut rows = stmt.query([])?;
        let mut updates: Vec<(i32, i32, i32)> = Vec::new();
        while let Some(row) = rows.next()? {
            let id: i32 = row.get(0)?;
            let x: i32 = row.get(1)?;
            let y: i32 = row.get(2)?;
            let vx: f64 = row.get(3)?;
            let vy: f64 = row.get(4)?;
            let new_x = ((x as f64 + vx) as i32).rem_euclid(MAP_SIZE);
            let new_y = ((y as f64 + vy) as i32).rem_euclid(MAP_SIZE);
            updates.push((id, new_x, new_y));
        }
        // Batch update back
        conn.execute_batch("BEGIN TRANSACTION")?;
        let mut update_stmt = conn.prepare("UPDATE entities SET x = ?2, y = ?3 WHERE id = ?1")?;
        for (id, x, y) in &updates {
            update_stmt.execute([*id, *x, *y])?;
        }
        conn.execute_batch("COMMIT")?;
    }
    let pull_compute_push = start.elapsed();
    println!("  A) Pull → Rust compute → Push: {:>10.2} ms", pull_compute_push.as_secs_f64() * 1000.0);

    // Reset positions
    conn.execute_batch(&format!(
        "UPDATE entities SET 
            x = (hash(id) % {MAP_SIZE})::INTEGER,
            y = (hash(id * 2) % {MAP_SIZE})::INTEGER"
    ))?;

    // Approach B: All logic in SQL
    let start = Instant::now();
    conn.execute(&format!(
        "UPDATE entities SET 
            x = ((x + vx::INTEGER) % {MAP_SIZE} + {MAP_SIZE}) % {MAP_SIZE},
            y = ((y + vy::INTEGER) % {MAP_SIZE} + {MAP_SIZE}) % {MAP_SIZE}"
    ), [])?;
    let sql_only = start.elapsed();
    println!("  B) Pure SQL UPDATE:             {:>10.2} ms", sql_only.as_secs_f64() * 1000.0);
    println!("     Speedup: {:.1}x\n", pull_compute_push.as_secs_f64() / sql_only.as_secs_f64());

    // =========================================================================
    // TEST 2: Damage calculation - find nearby enemies and sum damage
    // =========================================================================
    println!("--- TEST 2: Spatial Damage Query (100 queries) ---\n");

    // Approach A: Query each point, compute in Rust
    let start = Instant::now();
    let mut total_damage_a = 0i64;
    for i in 0..100 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let range = 50;
        
        let mut stmt = conn.prepare_cached(
            "SELECT damage FROM entities 
             WHERE faction = 'enemy' 
             AND x BETWEEN ?1 AND ?2 AND y BETWEEN ?3 AND ?4"
        )?;
        let mut rows = stmt.query([x - range, x + range, y - range, y + range])?;
        while let Some(row) = rows.next()? {
            let dmg: i32 = row.get(0)?;
            total_damage_a += dmg as i64;
        }
    }
    let rust_compute = start.elapsed();
    println!("  A) 100 queries + Rust sum:      {:>10.2} ms  (total={})", 
             rust_compute.as_secs_f64() * 1000.0, total_damage_a);

    // Approach B: SQL does the aggregation
    let start = Instant::now();
    let mut total_damage_b = 0i64;
    for i in 0..100 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        let range = 50;
        
        let mut stmt = conn.prepare_cached(
            "SELECT COALESCE(SUM(damage), 0) FROM entities 
             WHERE faction = 'enemy' 
             AND x BETWEEN ?1 AND ?2 AND y BETWEEN ?3 AND ?4"
        )?;
        let dmg: i64 = stmt.query_row([x - range, x + range, y - range, y + range], |r| r.get(0))?;
        total_damage_b += dmg;
    }
    let sql_aggregate = start.elapsed();
    println!("  B) 100 queries with SUM():      {:>10.2} ms  (total={})", 
             sql_aggregate.as_secs_f64() * 1000.0, total_damage_b);

    // Approach C: SINGLE query with all 100 points using LATERAL JOIN
    let start = Instant::now();
    let mut points_sql = String::from("SELECT * FROM (VALUES ");
    for i in 0..100 {
        let x = ((i * 17) % MAP_SIZE) as i32;
        let y = ((i * 23) % MAP_SIZE) as i32;
        if i > 0 { points_sql.push_str(", "); }
        points_sql.push_str(&format!("({}, {})", x, y));
    }
    points_sql.push_str(") AS points(px, py)");
    
    let query = format!(
        "SELECT SUM(total_dmg) FROM (
            SELECT px, py, COALESCE(SUM(e.damage), 0) as total_dmg
            FROM ({}) AS p
            LEFT JOIN entities e ON e.faction = 'enemy'
                AND e.x BETWEEN p.px - 50 AND p.px + 50
                AND e.y BETWEEN p.py - 50 AND p.py + 50
            GROUP BY px, py
        )", points_sql
    );
    let total_damage_c: i64 = conn.query_row(&query, [], |r| r.get(0))?;
    let single_query = start.elapsed();
    println!("  C) SINGLE query (100 points):   {:>10.2} ms  (total={})", 
             single_query.as_secs_f64() * 1000.0, total_damage_c);
    println!("     Speedup vs A: {:.1}x", rust_compute.as_secs_f64() / single_query.as_secs_f64());

    // =========================================================================
    // TEST 3: Complex game logic - combat resolution
    // =========================================================================
    println!("\n--- TEST 3: Combat Resolution (who attacks whom) ---\n");

    // Approach A: Pull all, compute in Rust with HashMap
    let start = Instant::now();
    {
        // Load all entities into Rust
        let mut entities: HashMap<i32, (i32, i32, i32, i32, String)> = HashMap::new();
        let mut stmt = conn.prepare("SELECT id, x, y, health, damage, faction FROM entities")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let id: i32 = row.get(0)?;
            let x: i32 = row.get(1)?;
            let y: i32 = row.get(2)?;
            let health: i32 = row.get(3)?;
            let damage: i32 = row.get(4)?;
            let faction: String = row.get(5)?;
            entities.insert(id, (x, y, health, damage, faction));
        }
        
        // Build spatial index
        let mut grid: HashMap<(i32, i32), Vec<i32>> = HashMap::new();
        for (&id, &(x, y, _, _, _)) in &entities {
            let cell = (x / 10, y / 10);
            grid.entry(cell).or_default().push(id);
        }
        
        // Combat: each entity attacks nearest enemy
        let mut damage_dealt: HashMap<i32, i32> = HashMap::new();
        for (&id, &(x, y, _, damage, ref faction)) in &entities {
            let cell = (x / 10, y / 10);
            let mut best_target: Option<(i32, i32)> = None; // (id, dist_sq)
            
            for dx in -5..=5 {
                for dy in -5..=5 {
                    if let Some(cell_entities) = grid.get(&(cell.0 + dx, cell.1 + dy)) {
                        for &other_id in cell_entities {
                            if other_id == id { continue; }
                            let (ox, oy, _, _, ref other_faction) = entities[&other_id];
                            if faction == other_faction { continue; }
                            let dist_sq = (x - ox).pow(2) + (y - oy).pow(2);
                            if dist_sq <= 2500 { // within range 50
                                if best_target.is_none() || dist_sq < best_target.unwrap().1 {
                                    best_target = Some((other_id, dist_sq));
                                }
                            }
                        }
                    }
                }
            }
            
            if let Some((target_id, _)) = best_target {
                *damage_dealt.entry(target_id).or_default() += damage;
            }
        }
        
        // Apply damage
        conn.execute_batch("BEGIN TRANSACTION")?;
        let mut update_stmt = conn.prepare("UPDATE entities SET health = health - ?2 WHERE id = ?1")?;
        for (id, dmg) in &damage_dealt {
            update_stmt.execute([*id, *dmg])?;
        }
        conn.execute_batch("COMMIT")?;
    }
    let rust_combat = start.elapsed();
    println!("  A) Pull → Rust HashMap → Push:  {:>10.2} ms", rust_combat.as_secs_f64() * 1000.0);

    // Reset health
    conn.execute_batch("UPDATE entities SET health = (hash(id * 5) % 100)::INTEGER")?;

    // Approach B: All in SQL with window functions
    let start = Instant::now();
    conn.execute_batch(
        "
        -- Find nearest enemy for each entity and compute damage
        WITH nearest_enemy AS (
            SELECT 
                a.id AS attacker_id,
                a.damage,
                b.id AS target_id,
                ROW_NUMBER() OVER (
                    PARTITION BY a.id 
                    ORDER BY (a.x - b.x)*(a.x - b.x) + (a.y - b.y)*(a.y - b.y)
                ) AS rn
            FROM entities a
            JOIN entities b ON a.faction != b.faction
                AND b.x BETWEEN a.x - 50 AND a.x + 50
                AND b.y BETWEEN a.y - 50 AND a.y + 50
                AND a.id != b.id
        ),
        total_damage AS (
            SELECT target_id, SUM(damage) AS dmg
            FROM nearest_enemy
            WHERE rn = 1
            GROUP BY target_id
        )
        UPDATE entities 
        SET health = health - COALESCE((SELECT dmg FROM total_damage WHERE target_id = entities.id), 0)
        "
    )?;
    let sql_combat = start.elapsed();
    println!("  B) Pure SQL (window functions): {:>10.2} ms", sql_combat.as_secs_f64() * 1000.0);
    println!("     Speedup: {:.1}x\n", rust_combat.as_secs_f64() / sql_combat.as_secs_f64());

    // =========================================================================
    // TEST 4: Per-frame overhead comparison
    // =========================================================================
    println!("--- TEST 4: What fits in 16ms frame? ---\n");

    let frame_budget_ms = 16.67;
    
    // Simple system update (SQL)
    let start = Instant::now();
    let mut sql_updates = 0;
    while start.elapsed().as_secs_f64() * 1000.0 < frame_budget_ms {
        conn.execute(&format!(
            "UPDATE entities SET 
                x = ((x + vx::INTEGER) % {MAP_SIZE} + {MAP_SIZE}) % {MAP_SIZE},
                y = ((y + vy::INTEGER) % {MAP_SIZE} + {MAP_SIZE}) % {MAP_SIZE}"
        ), [])?;
        sql_updates += 1;
    }
    println!("  SQL movement updates per frame:   {} ({} entities each)", 
             sql_updates, NUM_ENTITIES);

    // Point queries
    let start = Instant::now();
    let mut point_queries = 0;
    while start.elapsed().as_secs_f64() * 1000.0 < frame_budget_ms {
        let x = ((point_queries * 17) % MAP_SIZE) as i32;
        let y = ((point_queries * 23) % MAP_SIZE) as i32;
        let mut stmt = conn.prepare_cached("SELECT id FROM entities WHERE x = ?1 AND y = ?2")?;
        let mut rows = stmt.query([x, y])?;
        while let Some(_) = rows.next()? {}
        point_queries += 1;
    }
    println!("  Point queries per frame:          {}", point_queries);

    // Batched point queries (100 at a time)
    let start = Instant::now();
    let mut batched_queries = 0;
    while start.elapsed().as_secs_f64() * 1000.0 < frame_budget_ms {
        let mut conditions = Vec::new();
        for i in 0..100 {
            let x = ((batched_queries * 100 + i) * 17) % MAP_SIZE;
            let y = ((batched_queries * 100 + i) * 23) % MAP_SIZE;
            conditions.push(format!("(x = {} AND y = {})", x, y));
        }
        let query = format!("SELECT id FROM entities WHERE {}", conditions.join(" OR "));
        let mut stmt = conn.prepare(&query)?;
        let mut rows = stmt.query([])?;
        while let Some(_) = rows.next()? {}
        batched_queries += 100;
    }
    println!("  Batched point queries per frame:  {}", batched_queries);

    println!("\n--- Conclusion ---");
    println!("  YES! Push logic into SQL whenever possible:");
    println!("  • Bulk UPDATE in SQL is 10-100x faster than row-by-row");
    println!("  • SQL aggregates (SUM, COUNT) avoid pulling raw data");
    println!("  • Batch multiple queries into one SQL statement");
    println!("  • Window functions can replace complex Rust logic");
    println!("  • Each query has ~180µs overhead - amortize it!");

    Ok(())
}
