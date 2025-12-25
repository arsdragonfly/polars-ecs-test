//! Patterns for minimizing DuckDB calls from Lua mods
//!
//! The goal: Amortize ~30-40µs overhead over many operations

use duckdb::{Connection, params};
use std::time::Instant;
use std::hint::black_box;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Minimizing DuckDB Calls for Lua Mods ===\n");
    
    let conn = Connection::open_in_memory()?;
    
    // Setup: 10K entities
    conn.execute_batch(r#"
        CREATE TABLE entities(id INTEGER PRIMARY KEY, x DOUBLE, y DOUBLE, health INTEGER);
        INSERT INTO entities 
        SELECT i, random()*1000, random()*1000, 100 
        FROM generate_series(1, 10000) t(i);
    "#)?;
    
    const ENTITY_COUNT: usize = 100;  // How many entities a Lua mod wants to process
    let target_ids: Vec<i32> = (1..=ENTITY_COUNT as i32).collect();
    
    // ============================================================
    // Pattern 1: ANTI-PATTERN - One query per entity (Lua style)
    // ============================================================
    println!("--- ANTI-PATTERN: One query per entity ---\n");
    println!("  Lua code: for id in targets do db:query('SELECT * FROM entities WHERE id = ' .. id) end\n");
    
    let start = Instant::now();
    for id in &target_ids {
        let _: (i32, f64, f64, i32) = conn.query_row(
            "SELECT id, x, y, health FROM entities WHERE id = ?1",
            params![id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
        )?;
    }
    let one_per = start.elapsed();
    println!("  Time for {} queries:        {:>8.2} ms  ({:.1} µs/entity)", 
             ENTITY_COUNT, one_per.as_secs_f64() * 1000.0,
             one_per.as_micros() as f64 / ENTITY_COUNT as f64);
    
    // ============================================================
    // Pattern 2: GOOD - Batch with IN clause
    // ============================================================
    println!("\n--- GOOD: Batch with IN clause ---\n");
    println!("  Lua code: db:query('SELECT * FROM entities WHERE id IN (' .. table.concat(targets, ',') .. ')')\n");
    
    // Build IN clause (what Lua would do with string concat)
    let in_clause = target_ids.iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let query = format!("SELECT id, x, y, health FROM entities WHERE id IN ({})", in_clause);
    
    let start = Instant::now();
    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query([])?;
    let mut count = 0;
    while let Some(row) = rows.next()? {
        black_box((row.get::<_, i32>(0)?, row.get::<_, f64>(1)?));
        count += 1;
    }
    let batch_in = start.elapsed();
    println!("  Time for 1 query ({} rows): {:>8.2} ms  ({:.1} µs/entity)", 
             count, batch_in.as_secs_f64() * 1000.0,
             batch_in.as_micros() as f64 / count as f64);
    println!("  Speedup: {:.0}×", one_per.as_nanos() as f64 / batch_in.as_nanos() as f64);
    
    // ============================================================
    // Pattern 3: BETTER - Arrow batch for large results
    // ============================================================
    println!("\n--- BETTER: Arrow batch (for large results) ---\n");
    println!("  Return Arrow RecordBatch to Lua, iterate in Lua\n");
    
    let start = Instant::now();
    let mut stmt = conn.prepare(&query)?;
    let arrow = stmt.query_arrow([])?;
    let mut arrow_count = 0;
    for batch in arrow {
        arrow_count += batch.num_rows();
    }
    let batch_arrow = start.elapsed();
    println!("  Time for 1 Arrow query:     {:>8.2} ms  ({:.1} µs/entity)", 
             batch_arrow.as_secs_f64() * 1000.0,
             batch_arrow.as_micros() as f64 / arrow_count as f64);
    println!("  Speedup: {:.0}×", one_per.as_nanos() as f64 / batch_arrow.as_nanos() as f64);
    
    // ============================================================
    // Pattern 4: BULK UPDATE - One UPDATE instead of many
    // ============================================================
    println!("\n--- BULK UPDATE: One UPDATE instead of many ---\n");
    
    // ANTI-PATTERN: Update one by one
    println!("  ANTI-PATTERN: for id in targets do db:exec('UPDATE entities SET health = health - 10 WHERE id = ' .. id) end\n");
    
    let start = Instant::now();
    for id in &target_ids {
        conn.execute(
            "UPDATE entities SET health = health - 10 WHERE id = ?1",
            params![id]
        )?;
    }
    let update_one = start.elapsed();
    println!("  {} individual updates:      {:>8.2} ms  ({:.1} µs/update)", 
             ENTITY_COUNT, update_one.as_secs_f64() * 1000.0,
             update_one.as_micros() as f64 / ENTITY_COUNT as f64);
    
    // GOOD: Batch update
    println!("\n  GOOD: db:exec('UPDATE entities SET health = health - 10 WHERE id IN (...)')\n");
    
    let update_query = format!("UPDATE entities SET health = health - 10 WHERE id IN ({})", in_clause);
    let start = Instant::now();
    conn.execute(&update_query, [])?;
    let update_batch = start.elapsed();
    println!("  1 batch update:             {:>8.2} ms  ({:.1} µs/update)", 
             update_batch.as_secs_f64() * 1000.0,
             update_batch.as_micros() as f64 / ENTITY_COUNT as f64);
    println!("  Speedup: {:.0}×", update_one.as_nanos() as f64 / update_batch.as_nanos() as f64);
    
    // ============================================================
    // Pattern 5: SPATIAL - One range query vs many point queries
    // ============================================================
    println!("\n--- SPATIAL: Range query instead of point-by-point ---\n");
    
    // ANTI-PATTERN: Check each entity individually
    let check_count = 50;
    println!("  ANTI-PATTERN: for each entity, query if any neighbor within radius\n");
    
    let start = Instant::now();
    for i in 1..=check_count {
        let _count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM entities WHERE id != ?1 AND (x - ?2)*(x - ?2) + (y - ?3)*(y - ?3) < 100*100",
            params![i, i as f64 * 10.0, i as f64 * 5.0],
            |r| r.get(0)
        )?;
    }
    let spatial_one = start.elapsed();
    println!("  {} individual spatial checks: {:>6.2} ms  ({:.1} µs/check)", 
             check_count, spatial_one.as_secs_f64() * 1000.0,
             spatial_one.as_micros() as f64 / check_count as f64);
    
    // GOOD: One query that returns all pairs
    println!("\n  GOOD: Get all nearby pairs at once, filter in Lua\n");
    
    let start = Instant::now();
    let mut stmt = conn.prepare(
        "SELECT a.id, b.id, 
                sqrt((a.x - b.x)*(a.x - b.x) + (a.y - b.y)*(a.y - b.y)) as dist
         FROM entities a, entities b 
         WHERE a.id < b.id 
           AND a.id <= ?1
           AND abs(a.x - b.x) < 100 
           AND abs(a.y - b.y) < 100
           AND (a.x - b.x)*(a.x - b.x) + (a.y - b.y)*(a.y - b.y) < 100*100"
    )?;
    let mut rows = stmt.query(params![check_count])?;
    let mut pair_count = 0;
    while rows.next()?.is_some() {
        pair_count += 1;
    }
    let spatial_batch = start.elapsed();
    println!("  1 query returning {} pairs:   {:>6.2} ms", 
             pair_count, spatial_batch.as_secs_f64() * 1000.0);
    println!("  Speedup: {:.0}× (if processing same entities)", 
             spatial_one.as_nanos() as f64 / spatial_batch.as_nanos() as f64);
    
    // ============================================================
    // Pattern 6: PREPARED STATEMENTS in Lua
    // ============================================================
    println!("\n--- PREPARED STATEMENTS: Reuse parsed queries ---\n");
    
    // Unprepared (parse each time)
    let start = Instant::now();
    for id in &target_ids {
        let _: i32 = conn.query_row(
            &format!("SELECT health FROM entities WHERE id = {}", id),
            [],
            |r| r.get(0)
        )?;
    }
    let unprepared = start.elapsed();
    
    // Prepared (parse once)
    let mut stmt = conn.prepare("SELECT health FROM entities WHERE id = ?1")?;
    let start = Instant::now();
    for id in &target_ids {
        let _: i32 = stmt.query_row(params![id], |r| r.get(0))?;
    }
    let prepared = start.elapsed();
    
    println!("  Unprepared (parse each):    {:>8.2} ms  ({:.1} µs/query)", 
             unprepared.as_secs_f64() * 1000.0,
             unprepared.as_micros() as f64 / ENTITY_COUNT as f64);
    println!("  Prepared (parse once):      {:>8.2} ms  ({:.1} µs/query)", 
             prepared.as_secs_f64() * 1000.0,
             prepared.as_micros() as f64 / ENTITY_COUNT as f64);
    println!("  Speedup: {:.1}×", unprepared.as_nanos() as f64 / prepared.as_nanos() as f64);
    
    // ============================================================
    // Summary
    // ============================================================
    println!("\n=== Summary: Lua Mod Best Practices ===\n");
    println!("  ┌─────────────────────────────────────────────┬─────────────┐");
    println!("  │ Pattern                                     │ Difficulty  │");
    println!("  ├─────────────────────────────────────────────┼─────────────┤");
    println!("  │ 1. Use IN clause instead of loops           │ Easy        │");
    println!("  │ 2. Batch UPDATEs with WHERE IN              │ Easy        │");
    println!("  │ 3. Prepared statements for repeated queries │ Medium      │");
    println!("  │ 4. Return result sets, filter in Lua        │ Medium      │");
    println!("  │ 5. One spatial query vs many point checks   │ Medium      │");
    println!("  │ 6. Arrow batches for large data             │ Hard (FFI)  │");
    println!("  └─────────────────────────────────────────────┴─────────────┘");
    println!();
    println!("  Key insight: Each DuckDB call costs ~30-100µs overhead.");
    println!("  Reduce calls from 100 → 1 = 100× speedup for small operations!");
    println!();
    println!("  For Lua modders, provide helper functions like:");
    println!("    - db:select_many(ids)         -- SELECT WHERE id IN");
    println!("    - db:update_many(ids, values) -- batch UPDATE");
    println!("    - db:nearby_pairs(radius)     -- spatial query");
    println!();
    
    Ok(())
}
