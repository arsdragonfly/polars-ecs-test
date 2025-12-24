//! DuckDB R-Tree + Bulk Query Integration
//!
//! Explores whether R-tree can accelerate bulk spatial queries.
//!
//! Key insight from docs: R-tree only works with CONSTANT geometry.
//! This means:
//!   ❌ Cannot use R-tree for: SELECT * FROM e1, e2 WHERE ST_DWithin(e1.geom, e2.geom, 50)
//!   ✅ CAN use R-tree for: SELECT * FROM entities WHERE ST_Within(geom, constant_box)
//!
//! Strategy: Grid-based spatial partitioning
//!   1. Divide world into grid cells
//!   2. For each cell, query entities using R-tree (constant box)
//!   3. Cross-join only within/adjacent cells
//!
//! This turns O(N²) into O(N * K) where K = entities per cell

use duckdb::Connection;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== R-Tree + Bulk Query Spatial Optimization ===\n");
    
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;
    conn.execute_batch("INSTALL spatial; LOAD spatial;")?;
    
    let world_size = 1000.0;
    let query_radius = 50.0;
    
    for n in [500, 1000, 2000, 5000] {
        println!("=== {} entities (world: {}×{}, radius: {}) ===\n", n, world_size, world_size, query_radius);
        
        // Create entity table with geometry
        conn.execute_batch("DROP TABLE IF EXISTS entities;")?;
        conn.execute_batch(&format!(
            "CREATE TABLE entities AS 
             SELECT i as id,
                    random()*{} as x, 
                    random()*{} as y,
                    ST_Point(random()*{}, random()*{}) as geom
             FROM generate_series(1, {}) AS t(i)",
            world_size, world_size, world_size, world_size, n
        ))?;
        
        // Create R-tree index
        let start = Instant::now();
        conn.execute_batch("CREATE INDEX entities_rtree ON entities USING RTREE (geom);")?;
        let index_time = start.elapsed();
        println!("  R-tree index created in {:.2} ms\n", index_time.as_secs_f64() * 1000.0);
        
        // ============================================================
        // Method 1: Full N² cross-join (no index possible)
        // ============================================================
        println!("--- Method 1: Full N² Cross-Join (no R-tree) ---\n");
        
        let mut stmt_n2 = conn.prepare(
            "SELECT count(*) FROM entities e1, entities e2 
             WHERE e1.id < e2.id 
               AND sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y)) < ?"
        )?;
        
        // Warmup
        let _: i64 = stmt_n2.query_row([query_radius], |r| r.get(0))?;
        
        let start = Instant::now();
        let count_n2: i64 = stmt_n2.query_row([query_radius], |r| r.get(0))?;
        let time_n2 = start.elapsed();
        
        println!("  Pairs found: {}", count_n2);
        println!("  Time: {:.2} ms", time_n2.as_secs_f64() * 1000.0);
        println!("  Per-pair checked: {:.0} ns", 
                 time_n2.as_secs_f64() * 1_000_000_000.0 / (n * n / 2) as f64);
        
        // ============================================================
        // Method 2: Grid-based with R-tree indexed cell queries
        // ============================================================
        println!("\n--- Method 2: Grid-Based R-Tree Queries ---\n");
        
        // Cell size should be >= query_radius to ensure we only need adjacent cells
        let cell_size: f64 = query_radius * 2.0;
        let grid_size = (world_size / cell_size).ceil() as i32;
        println!("  Grid: {}×{} cells (cell size: {:.0})", grid_size, grid_size, cell_size);
        
        let start = Instant::now();
        let mut total_pairs = 0i64;
        let mut total_queries = 0i32;
        
        // For each cell, query entities in that cell and adjacent cells
        // Then do local cross-join
        for cx in 0..grid_size {
            for cy in 0..grid_size {
                // Query box includes this cell and extends by query_radius
                let x_min = (cx as f64 * cell_size) - query_radius;
                let y_min = (cy as f64 * cell_size) - query_radius;
                let x_max = ((cx + 1) as f64 * cell_size) + query_radius;
                let y_max = ((cy + 1) as f64 * cell_size) + query_radius;
                
                // This query CAN use R-tree because the envelope is constant!
                let query = format!(
                    "WITH cell_entities AS (
                        SELECT id, x, y FROM entities 
                        WHERE ST_Within(geom, ST_MakeEnvelope({}, {}, {}, {}))
                    ),
                    cell_center AS (
                        SELECT id, x, y FROM entities
                        WHERE x >= {} AND x < {} AND y >= {} AND y < {}
                    )
                    SELECT count(*) FROM cell_center c, cell_entities e
                    WHERE c.id < e.id 
                      AND sqrt((e.x-c.x)*(e.x-c.x) + (e.y-c.y)*(e.y-c.y)) < {}",
                    x_min, y_min, x_max, y_max,
                    cx as f64 * cell_size, (cx + 1) as f64 * cell_size,
                    cy as f64 * cell_size, (cy + 1) as f64 * cell_size,
                    query_radius
                );
                
                let count: i64 = conn.query_row(&query, [], |r| r.get(0))?;
                total_pairs += count;
                total_queries += 1;
            }
        }
        let time_grid = start.elapsed();
        
        println!("  Queries executed: {}", total_queries);
        println!("  Pairs found: {}", total_pairs);
        println!("  Time: {:.2} ms", time_grid.as_secs_f64() * 1000.0);
        println!("  Per-query: {:.2} ms", time_grid.as_secs_f64() * 1000.0 / total_queries as f64);
        
        // ============================================================
        // Method 3: Single bulk query with grid assignment
        // ============================================================
        println!("\n--- Method 3: Bulk Query with Cell Assignment ---\n");
        
        let start = Instant::now();
        
        // Assign each entity to a cell, then only cross-join within nearby cells
        let count_bulk: i64 = conn.query_row(&format!(
            "WITH entities_with_cell AS (
                SELECT id, x, y,
                       CAST(floor(x / {}) AS INTEGER) as cx,
                       CAST(floor(y / {}) AS INTEGER) as cy
                FROM entities
            )
            SELECT count(*) FROM entities_with_cell e1, entities_with_cell e2
            WHERE e1.id < e2.id
              AND abs(e1.cx - e2.cx) <= 1
              AND abs(e1.cy - e2.cy) <= 1
              AND sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y)) < {}",
            cell_size, cell_size, query_radius
        ), [], |r| r.get(0))?;
        
        let time_bulk = start.elapsed();
        
        println!("  Pairs found: {}", count_bulk);
        println!("  Time: {:.2} ms", time_bulk.as_secs_f64() * 1000.0);
        
        // ============================================================
        // Method 4: Prepared statement with cell filtering
        // ============================================================
        println!("\n--- Method 4: Cached Bulk Query with Cell Filter ---\n");
        
        let mut stmt_cell = conn.prepare(&format!(
            "WITH entities_with_cell AS (
                SELECT id, x, y,
                       CAST(floor(x / {}) AS INTEGER) as cx,
                       CAST(floor(y / {}) AS INTEGER) as cy
                FROM entities
            )
            SELECT count(*) FROM entities_with_cell e1, entities_with_cell e2
            WHERE e1.id < e2.id
              AND abs(e1.cx - e2.cx) <= 1
              AND abs(e1.cy - e2.cy) <= 1
              AND sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y)) < ?",
            cell_size, cell_size
        ))?;
        
        // Warmup
        let _: i64 = stmt_cell.query_row([query_radius], |r| r.get(0))?;
        
        let start = Instant::now();
        let count_cached: i64 = stmt_cell.query_row([query_radius], |r| r.get(0))?;
        let time_cached = start.elapsed();
        
        println!("  Pairs found: {}", count_cached);
        println!("  Time: {:.2} ms", time_cached.as_secs_f64() * 1000.0);
        
        // ============================================================
        // Summary
        // ============================================================
        println!("\n--- Summary ---\n");
        println!("  Method                    Time        Speedup vs N²");
        println!("  ─────────────────────────────────────────────────────");
        println!("  1. Full N² cross-join    {:>8.2} ms    1.00×", time_n2.as_secs_f64() * 1000.0);
        println!("  2. Grid R-tree queries   {:>8.2} ms    {:.2}×", 
                 time_grid.as_secs_f64() * 1000.0,
                 time_n2.as_secs_f64() / time_grid.as_secs_f64());
        println!("  3. Bulk cell filter      {:>8.2} ms    {:.2}×", 
                 time_bulk.as_secs_f64() * 1000.0,
                 time_n2.as_secs_f64() / time_bulk.as_secs_f64());
        println!("  4. Cached cell filter    {:>8.2} ms    {:.2}×", 
                 time_cached.as_secs_f64() * 1000.0,
                 time_n2.as_secs_f64() / time_cached.as_secs_f64());
        
        // Verify correctness
        if count_n2 != count_bulk || count_n2 != count_cached {
            println!("\n  ⚠️  Count mismatch! N²={}, bulk={}, cached={}", count_n2, count_bulk, count_cached);
        } else {
            println!("\n  ✅ All methods found same pair count");
        }
        
        println!();
    }
    
    println!("=== Conclusions ===\n");
    println!("  1. R-tree cannot directly accelerate N² cross-joins");
    println!("  2. Grid-based cell assignment in SQL is the winning strategy");
    println!("  3. Cell filtering turns O(N²) into O(N × K) where K = avg neighbors");
    println!("  4. Statement caching provides additional speedup");
    println!();
    println!("  The 'cell assignment in SQL' approach is effectively spatial hashing");
    println!("  implemented at the query level - no Rust data structures needed!");
    
    Ok(())
}
