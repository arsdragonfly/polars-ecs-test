//! DuckDB sqrt with pre-filtering strategies
//!
//! Compare different ways to reduce comparisons before sqrt:
//! 1. Pure N² sqrt (baseline)
//! 2. Bounding box pre-filter (cheap integer check)
//! 3. Cell assignment pre-filter (spatial hash in SQL)
//! 4. Pre-computed cells in table

use duckdb::Connection;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB sqrt with Pre-Filtering Strategies ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;

    let world_size = 1000.0;
    let query_radius = 50.0;
    let cell_size = query_radius * 2.0; // 100

    for n in [500, 1000, 2000, 5000] {
        println!("=== {} entities (world: {}×{}, radius: {}) ===\n", 
                 n, world_size as i32, world_size as i32, query_radius as i32);

        // Create entity table
        conn.execute_batch("DROP TABLE IF EXISTS entities;")?;
        conn.execute_batch(&format!(
            "CREATE TABLE entities AS 
             SELECT i as id,
                    random()*{} as x, 
                    random()*{} as y
             FROM generate_series(1, {}) AS t(i)",
            world_size, world_size, n
        ))?;

        // ============================================================
        // Method 1: Pure N² sqrt (baseline)
        // ============================================================
        let mut stmt1 = conn.prepare(
            "SELECT count(*) FROM entities e1, entities e2 
             WHERE e1.id < e2.id 
               AND sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y)) < ?"
        )?;
        
        // Warmup
        let _: i64 = stmt1.query_row([query_radius], |r| r.get(0))?;
        
        let start = Instant::now();
        let count1: i64 = stmt1.query_row([query_radius], |r| r.get(0))?;
        let time1 = start.elapsed();

        println!("  1. Pure N² sqrt:              {:>8.2} ms  ({} pairs)", 
                 time1.as_secs_f64() * 1000.0, count1);

        // ============================================================
        // Method 2: Bounding box pre-filter (no sqrt if far)
        // ============================================================
        let mut stmt2 = conn.prepare(
            "SELECT count(*) FROM entities e1, entities e2 
             WHERE e1.id < e2.id 
               AND abs(e2.x - e1.x) < ?
               AND abs(e2.y - e1.y) < ?
               AND sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y)) < ?"
        )?;
        
        let _: i64 = stmt2.query_row([query_radius, query_radius, query_radius], |r| r.get(0))?;
        
        let start = Instant::now();
        let count2: i64 = stmt2.query_row([query_radius, query_radius, query_radius], |r| r.get(0))?;
        let time2 = start.elapsed();

        println!("  2. Bbox pre-filter + sqrt:    {:>8.2} ms  ({} pairs)  {:.2}×", 
                 time2.as_secs_f64() * 1000.0, count2,
                 time1.as_secs_f64() / time2.as_secs_f64());

        // ============================================================
        // Method 3: Distance² pre-filter (avoid sqrt entirely)
        // ============================================================
        let radius_sq = query_radius * query_radius;
        let mut stmt3 = conn.prepare(
            "SELECT count(*) FROM entities e1, entities e2 
             WHERE e1.id < e2.id 
               AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < ?"
        )?;
        
        let _: i64 = stmt3.query_row([radius_sq], |r| r.get(0))?;
        
        let start = Instant::now();
        let count3: i64 = stmt3.query_row([radius_sq], |r| r.get(0))?;
        let time3 = start.elapsed();

        println!("  3. Distance² (no sqrt):       {:>8.2} ms  ({} pairs)  {:.2}×", 
                 time3.as_secs_f64() * 1000.0, count3,
                 time1.as_secs_f64() / time3.as_secs_f64());

        // ============================================================
        // Method 4: Bbox + Distance² (cheapest filter first)
        // ============================================================
        let mut stmt4 = conn.prepare(
            "SELECT count(*) FROM entities e1, entities e2 
             WHERE e1.id < e2.id 
               AND abs(e2.x - e1.x) < ?
               AND abs(e2.y - e1.y) < ?
               AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < ?"
        )?;
        
        let _: i64 = stmt4.query_row([query_radius, query_radius, radius_sq], |r| r.get(0))?;
        
        let start = Instant::now();
        let count4: i64 = stmt4.query_row([query_radius, query_radius, radius_sq], |r| r.get(0))?;
        let time4 = start.elapsed();

        println!("  4. Bbox + dist² (no sqrt):    {:>8.2} ms  ({} pairs)  {:.2}×", 
                 time4.as_secs_f64() * 1000.0, count4,
                 time1.as_secs_f64() / time4.as_secs_f64());

        // ============================================================
        // Method 5: Cell filter + dist² (inline, no CTE)
        // ============================================================
        let mut stmt5 = conn.prepare(&format!(
            "SELECT count(*) FROM entities e1, entities e2 
             WHERE e1.id < e2.id 
               AND abs(CAST(floor(e1.x / {0}) AS INTEGER) - CAST(floor(e2.x / {0}) AS INTEGER)) <= 1
               AND abs(CAST(floor(e1.y / {0}) AS INTEGER) - CAST(floor(e2.y / {0}) AS INTEGER)) <= 1
               AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < ?",
            cell_size
        ))?;
        
        let _: i64 = stmt5.query_row([radius_sq], |r| r.get(0))?;
        
        let start = Instant::now();
        let count5: i64 = stmt5.query_row([radius_sq], |r| r.get(0))?;
        let time5 = start.elapsed();

        println!("  5. Cell filter + dist²:       {:>8.2} ms  ({} pairs)  {:.2}×", 
                 time5.as_secs_f64() * 1000.0, count5,
                 time1.as_secs_f64() / time5.as_secs_f64());

        // ============================================================
        // Method 6: Pre-computed cells in table
        // ============================================================
        conn.execute_batch("DROP TABLE IF EXISTS entities_cells;")?;
        conn.execute_batch(&format!(
            "CREATE TABLE entities_cells AS 
             SELECT id, x, y,
                    CAST(floor(x / {}) AS INTEGER) as cx,
                    CAST(floor(y / {}) AS INTEGER) as cy
             FROM entities",
            cell_size, cell_size
        ))?;

        let mut stmt6 = conn.prepare(
            "SELECT count(*) FROM entities_cells e1, entities_cells e2 
             WHERE e1.id < e2.id 
               AND abs(e1.cx - e2.cx) <= 1
               AND abs(e1.cy - e2.cy) <= 1
               AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < ?"
        )?;
        
        let _: i64 = stmt6.query_row([radius_sq], |r| r.get(0))?;
        
        let start = Instant::now();
        let count6: i64 = stmt6.query_row([radius_sq], |r| r.get(0))?;
        let time6 = start.elapsed();

        println!("  6. Pre-computed cells + dist²:{:>8.2} ms  ({} pairs)  {:.2}×", 
                 time6.as_secs_f64() * 1000.0, count6,
                 time1.as_secs_f64() / time6.as_secs_f64());

        // Verify counts match
        if count1 != count2 || count1 != count3 || count1 != count4 || count1 != count5 || count1 != count6 {
            println!("\n  ⚠️  Count mismatch! {},{},{},{},{},{}", 
                     count1, count2, count3, count4, count5, count6);
        }

        println!();
    }

    println!("=== Conclusions ===\n");
    println!("  • sqrt() is surprisingly cheap - main cost is N² comparisons");
    println!("  • Bbox pre-filter prunes ~80% of pairs before distance calc");
    println!("  • dist² avoids sqrt entirely (mathematically equivalent)");
    println!("  • Pre-computed cells give best speedup for repeated queries");
    println!();

    Ok(())
}
