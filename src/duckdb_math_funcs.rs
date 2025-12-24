//! Test DuckDB math functions for distance calculation

use duckdb::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    
    println!("=== DuckDB Math Functions for Distance ===\n");
    
    // hypot?
    match conn.query_row("SELECT hypot(3.0, 4.0)", [], |r| r.get::<_, f64>(0)) {
        Ok(v) => println!("hypot(3, 4) = {} ✅", v),
        Err(e) => println!("hypot(3, 4): ❌ Not available - {}", e),
    }
    
    // sqrt
    match conn.query_row("SELECT sqrt(3.0*3.0 + 4.0*4.0)", [], |r| r.get::<_, f64>(0)) {
        Ok(v) => println!("sqrt(3²+4²) = {} ✅", v),
        Err(e) => println!("sqrt: ❌ {}", e),
    }
    
    // power
    match conn.query_row("SELECT power(3.0*3.0 + 4.0*4.0, 0.5)", [], |r| r.get::<_, f64>(0)) {
        Ok(v) => println!("power(3²+4², 0.5) = {} ✅", v),
        Err(e) => println!("power: ❌ {}", e),
    }

    // squared distance (avoid sqrt entirely)
    match conn.query_row("SELECT 3.0*3.0 + 4.0*4.0", [], |r| r.get::<_, f64>(0)) {
        Ok(v) => println!("dist² = {} (compare to radius²=25) ✅", v),
        Err(e) => println!("dist²: ❌ {}", e),
    }

    // Check if there's a list_hypot or similar
    println!("\n--- Checking for other distance functions ---\n");
    
    // Try euclidean distance function
    match conn.query_row("SELECT list_distance([0,0], [3,4])", [], |r| r.get::<_, f64>(0)) {
        Ok(v) => println!("list_distance([0,0], [3,4]) = {} ✅", v),
        Err(e) => println!("list_distance: ❌ {}", e),
    }

    // Try list_inner_product, list_cosine_similarity
    match conn.query_row("SELECT list_inner_product([3.0, 4.0], [3.0, 4.0])", [], |r| r.get::<_, f64>(0)) {
        Ok(v) => println!("list_inner_product = {} (should be 25) ✅", v),
        Err(e) => println!("list_inner_product: ❌ {}", e),
    }

    // Benchmark: sqrt vs dist²
    println!("\n--- Performance Comparison ---\n");
    
    conn.execute_batch("
        CREATE TABLE test AS 
        SELECT i as id, random()*1000 as x1, random()*1000 as y1,
               random()*1000 as x2, random()*1000 as y2
        FROM generate_series(1, 100000) AS t(i)
    ")?;

    use std::time::Instant;

    // sqrt version
    let start = Instant::now();
    let _: i64 = conn.query_row(
        "SELECT count(*) FROM test WHERE sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1)) < 50", 
        [], |r| r.get(0)
    )?;
    let sqrt_time = start.elapsed();
    println!("sqrt(dx²+dy²) < 50:  {:>6.2} ms", sqrt_time.as_secs_f64() * 1000.0);

    // dist² version (avoid sqrt)
    let start = Instant::now();
    let _: i64 = conn.query_row(
        "SELECT count(*) FROM test WHERE (x2-x1)*(x2-x1) + (y2-y1)*(y2-y1) < 2500", 
        [], |r| r.get(0)
    )?;
    let distsq_time = start.elapsed();
    println!("dx²+dy² < 2500:      {:>6.2} ms  ({:.2}× faster)", 
             distsq_time.as_secs_f64() * 1000.0,
             sqrt_time.as_secs_f64() / distsq_time.as_secs_f64());

    // hypot if available
    match conn.query_row(
        "SELECT count(*) FROM test WHERE hypot(x2-x1, y2-y1) < 50", 
        [], |r| r.get::<_, i64>(0)
    ) {
        Ok(_) => {
            let start = Instant::now();
            let _: i64 = conn.query_row(
                "SELECT count(*) FROM test WHERE hypot(x2-x1, y2-y1) < 50", 
                [], |r| r.get(0)
            )?;
            let hypot_time = start.elapsed();
            println!("hypot(dx, dy) < 50:  {:>6.2} ms  ({:.2}× vs sqrt)", 
                     hypot_time.as_secs_f64() * 1000.0,
                     sqrt_time.as_secs_f64() / hypot_time.as_secs_f64());
        }
        Err(_) => println!("hypot: not available"),
    }

    // list_distance with cross-join
    println!("\n--- Cross-Join Benchmark (5000 entities) ---\n");
    
    conn.execute_batch("DROP TABLE IF EXISTS test;")?;
    conn.execute_batch("
        CREATE TABLE entities AS 
        SELECT i as id, random()*1000 as x, random()*1000 as y,
               CAST(floor(random()*1000/50) AS INTEGER) as cx,
               CAST(floor(random()*1000/50) AS INTEGER) as cy
        FROM generate_series(1, 5000) AS t(i)
    ")?;

    // Manual dist²
    let start = Instant::now();
    let count1: i64 = conn.query_row(
        "SELECT count(*) FROM entities e1, entities e2 
         WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < 2500", 
        [], |r| r.get(0)
    )?;
    let distsq_time = start.elapsed();
    println!("dx²+dy² < 2500:                {:>6.2} ms ({} pairs)", 
             distsq_time.as_secs_f64() * 1000.0, count1);

    // list_distance
    let start = Instant::now();
    let count2: i64 = conn.query_row(
        "SELECT count(*) FROM entities e1, entities e2 
         WHERE e1.id < e2.id AND list_distance([e1.x, e1.y], [e2.x, e2.y]) < 50", 
        [], |r| r.get(0)
    )?;
    let listdist_time = start.elapsed();
    println!("list_distance([x1,y1],[x2,y2]): {:>6.2} ms ({} pairs) {:.2}× vs dist²", 
             listdist_time.as_secs_f64() * 1000.0, count2,
             listdist_time.as_secs_f64() / distsq_time.as_secs_f64());

    // sqrt
    let start = Instant::now();
    let count3: i64 = conn.query_row(
        "SELECT count(*) FROM entities e1, entities e2 
         WHERE e1.id < e2.id AND sqrt((e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y)) < 50", 
        [], |r| r.get(0)
    )?;
    let sqrt_time = start.elapsed();
    println!("sqrt(dx²+dy²) < 50:            {:>6.2} ms ({} pairs) {:.2}× vs dist²", 
             sqrt_time.as_secs_f64() * 1000.0, count3,
             sqrt_time.as_secs_f64() / distsq_time.as_secs_f64());

    println!("\n--- Recommendation ---\n");
    println!("Use dist² comparison when possible - mathematically equivalent and faster!");
    println!("  Instead of: sqrt(dx²+dy²) < radius");
    println!("  Use:        dx²+dy² < radius²");

    Ok(())
}
