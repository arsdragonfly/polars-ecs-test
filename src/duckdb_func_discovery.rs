//! DuckDB Function Discovery - Find all distance/vector-related built-ins

use duckdb::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    
    println!("=== DuckDB Function Discovery ===\n");
    
    // List all functions with 'dist' in name
    println!("--- Functions containing 'dist' ---");
    let mut stmt = conn.prepare("SELECT function_name, function_type FROM duckdb_functions() WHERE function_name LIKE '%dist%' ORDER BY function_name")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let ftype: String = row.get(1)?;
        println!("  {} ({})", name, ftype);
    }
    
    println!("\n--- Functions containing 'eucl' ---");
    let mut stmt = conn.prepare("SELECT function_name, function_type FROM duckdb_functions() WHERE function_name LIKE '%eucl%' ORDER BY function_name")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let ftype: String = row.get(1)?;
        println!("  {} ({})", name, ftype);
    }
    
    println!("\n--- Functions containing 'vector' ---");
    let mut stmt = conn.prepare("SELECT function_name, function_type FROM duckdb_functions() WHERE function_name LIKE '%vector%' ORDER BY function_name")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let ftype: String = row.get(1)?;
        println!("  {} ({})", name, ftype);
    }
    
    println!("\n--- Functions containing 'array' (first 30) ---");
    let mut stmt = conn.prepare("SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name LIKE 'array%' ORDER BY function_name LIMIT 30")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        println!("  {}", name);
    }
    
    println!("\n--- Functions containing 'list' (distance-related) ---");
    let mut stmt = conn.prepare("SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name LIKE 'list_%' AND (function_name LIKE '%dist%' OR function_name LIKE '%cos%' OR function_name LIKE '%inner%' OR function_name LIKE '%dot%' OR function_name LIKE '%norm%') ORDER BY function_name")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        println!("  {}", name);
    }

    // Test specific functions
    println!("\n--- Testing Vector/List Functions ---");
    
    let tests = [
        ("list_distance([0,0], [3,4])", "Euclidean distance"),
        ("list_cosine_similarity([1,0], [0,1])", "Cosine similarity"),
        ("list_inner_product([3,4], [3,4])", "Inner product (dot)"),
        ("list_negative_inner_product([3,4], [3,4])", "Negative inner product"),
        ("array_distance([0,0]::FLOAT[2], [3,4]::FLOAT[2])", "Array distance"),
        ("array_inner_product([3,4]::FLOAT[2], [3,4]::FLOAT[2])", "Array inner product"),
        ("array_cosine_similarity([1,0]::FLOAT[2], [0,1]::FLOAT[2])", "Array cosine"),
    ];
    
    for (sql, desc) in tests {
        match conn.query_row(&format!("SELECT {}", sql), [], |r| r.get::<_, f64>(0)) {
            Ok(v) => println!("  ✅ {} = {:.4} ({})", sql, v, desc),
            Err(e) => {
                let err_str = e.to_string();
                let short_err = err_str.lines().next().unwrap_or(&err_str);
                println!("  ❌ {} - {}", sql, short_err);
            }
        }
    }

    // Check spatial extension functions
    println!("\n--- Spatial Extension Functions ---");
    match conn.execute_batch("INSTALL spatial; LOAD spatial;") {
        Ok(_) => {
            let spatial_tests = [
                ("ST_Distance(ST_Point(0,0), ST_Point(3,4))", "ST_Distance"),
                ("ST_DWithin(ST_Point(0,0), ST_Point(3,4), 10)", "ST_DWithin (within 10)"),
            ];
            
            for (sql, desc) in spatial_tests {
                match conn.query_row(&format!("SELECT {}", sql), [], |r| r.get::<_, f64>(0)) {
                    Ok(v) => println!("  ✅ {} = {:.4} ({})", sql, v, desc),
                    Err(e) => {
                        // Try as bool
                        match conn.query_row(&format!("SELECT {}", sql), [], |r| r.get::<_, bool>(0)) {
                            Ok(v) => println!("  ✅ {} = {} ({})", sql, v, desc),
                            Err(_) => println!("  ❌ {} - {}", sql, e),
                        }
                    }
                }
            }
        }
        Err(e) => println!("  Spatial extension not available: {}", e),
    }

    // Benchmark ARRAY vs manual distance
    println!("\n--- Performance: ARRAY vs Manual Distance (5K entities) ---");
    
    use std::time::Instant;
    
    // Create table with both representations
    conn.execute_batch("
        CREATE TABLE bench_ent AS 
        SELECT i as id, 
               random()*1000 as x, random()*1000 as y,
               [random()*1000, random()*1000]::DOUBLE[2] as pos,
               CAST(floor(random()*1000/50) AS INTEGER) as cx,
               CAST(floor(random()*1000/50) AS INTEGER) as cy
        FROM generate_series(1, 5000) AS t(i)
    ")?;

    // Manual dist²
    let start = Instant::now();
    let count1: i64 = conn.query_row(
        "SELECT count(*) FROM bench_ent e1, bench_ent e2 
         WHERE e1.id < e2.id AND (e2.x-e1.x)*(e2.x-e1.x) + (e2.y-e1.y)*(e2.y-e1.y) < 2500", 
        [], |r| r.get(0)
    )?;
    let manual_time = start.elapsed();
    println!("  Manual dist² < 2500:     {:>6.2} ms ({} pairs)", manual_time.as_secs_f64() * 1000.0, count1);

    // array_distance
    let start = Instant::now();
    let count2: i64 = conn.query_row(
        "SELECT count(*) FROM bench_ent e1, bench_ent e2 
         WHERE e1.id < e2.id AND array_distance(e1.pos, e2.pos) < 50", 
        [], |r| r.get(0)
    )?;
    let array_time = start.elapsed();
    println!("  array_distance < 50:     {:>6.2} ms ({} pairs) {:.2}× vs manual", 
             array_time.as_secs_f64() * 1000.0, count2,
             array_time.as_secs_f64() / manual_time.as_secs_f64());

    // array_inner_product (dot product = dist²)
    let start = Instant::now();
    let count3: i64 = conn.query_row(
        "SELECT count(*) FROM bench_ent e1, bench_ent e2 
         WHERE e1.id < e2.id AND array_inner_product(
             [e2.pos[1]-e1.pos[1], e2.pos[2]-e1.pos[2]]::DOUBLE[2],
             [e2.pos[1]-e1.pos[1], e2.pos[2]-e1.pos[2]]::DOUBLE[2]
         ) < 2500", 
        [], |r| r.get(0)
    )?;
    let inner_time = start.elapsed();
    println!("  array_inner_product:     {:>6.2} ms ({} pairs) {:.2}× vs manual", 
             inner_time.as_secs_f64() * 1000.0, count3,
             inner_time.as_secs_f64() / manual_time.as_secs_f64());

    // ST_Distance
    let start = Instant::now();
    let count4: i64 = conn.query_row(
        "SELECT count(*) FROM bench_ent e1, bench_ent e2 
         WHERE e1.id < e2.id AND ST_Distance(ST_Point(e1.x, e1.y), ST_Point(e2.x, e2.y)) < 50", 
        [], |r| r.get(0)
    )?;
    let st_time = start.elapsed();
    println!("  ST_Distance < 50:        {:>6.2} ms ({} pairs) {:.2}× vs manual", 
             st_time.as_secs_f64() * 1000.0, count4,
             st_time.as_secs_f64() / manual_time.as_secs_f64());

    Ok(())
}
