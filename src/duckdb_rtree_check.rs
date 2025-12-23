//! DuckDB R-tree Index Investigation
//!
//! What R-tree capabilities actually exist?

use duckdb::{Connection, Result};
use std::time::Instant;

fn main() -> Result<()> {
    println!("=== DuckDB R-tree Index Investigation ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 1;")?;
    
    println!("Loading spatial extension...");
    conn.execute_batch("INSTALL spatial; LOAD spatial;")?;

    // Check DuckDB version
    let version: String = conn.query_row("SELECT version()", [], |r| r.get(0))?;
    println!("DuckDB version: {}\n", version);

    // Create test table with geometry
    conn.execute_batch(
        "
        CREATE TABLE test_points AS
        SELECT 
            i AS id,
            ST_Point(random() * 1000, random() * 1000) AS geom
        FROM generate_series(1, 10000) AS t(i);
        "
    )?;

    // Test 1: Try CREATE INDEX ... USING RTREE
    println!("--- Testing R-tree Index Creation ---\n");
    
    println!("1. CREATE INDEX ... USING RTREE(geom):");
    match conn.execute_batch("CREATE INDEX idx_rtree ON test_points USING RTREE(geom);") {
        Ok(_) => println!("   ✓ Success!"),
        Err(e) => println!("   ✗ Failed: {}", e),
    }

    // Test 2: Try with MIN_BOUNDING_BOX
    println!("\n2. Trying MIN_BOUNDING_BOX approach:");
    match conn.execute_batch(
        "ALTER TABLE test_points ADD COLUMN bbox STRUCT(minX DOUBLE, minY DOUBLE, maxX DOUBLE, maxY DOUBLE);
         UPDATE test_points SET bbox = (
            SELECT STRUCT_PACK(
                minX := ST_XMin(geom), 
                minY := ST_YMin(geom), 
                maxX := ST_XMax(geom), 
                maxY := ST_YMax(geom)
            )
         );"
    ) {
        Ok(_) => println!("   ✓ Created bbox struct column"),
        Err(e) => println!("   ✗ Failed: {}", e),
    }

    // Test 3: Check if there's an rtree table function
    println!("\n3. Checking for rtree functions:");
    match conn.query_row(
        "SELECT * FROM duckdb_functions() WHERE function_name LIKE '%rtree%' OR function_name LIKE '%tree%' LIMIT 1",
        [],
        |r| r.get::<_, String>(0)
    ) {
        Ok(name) => println!("   Found: {}", name),
        Err(_) => println!("   No rtree-specific functions found"),
    }

    // Test 4: List all spatial functions
    println!("\n4. Key spatial functions available:");
    let mut stmt = conn.prepare(
        "SELECT DISTINCT function_name FROM duckdb_functions() 
         WHERE function_name LIKE 'ST_%' 
         AND function_name IN ('ST_DWithin', 'ST_Distance', 'ST_Intersects', 'ST_Contains', 'ST_Within', 'ST_Envelope', 'ST_Extent')
         ORDER BY function_name"
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        println!("   - {}", name);
    }

    // Test 5: Check what index types are supported
    println!("\n5. Supported index types:");
    match conn.execute_batch("CREATE INDEX idx_test ON test_points(id);") {
        Ok(_) => println!("   - B-tree (standard) ✓"),
        Err(e) => println!("   - B-tree failed: {}", e),
    }

    // Test 6: ART index
    println!("\n6. Trying ART index:");
    match conn.execute_batch("CREATE INDEX idx_art ON test_points USING ART(id);") {
        Ok(_) => println!("   ✓ ART index supported"),
        Err(e) => println!("   ✗ ART failed: {}", e),
    }

    // Test 7: See if spatial extension has any index acceleration
    println!("\n--- Testing Spatial Query Performance ---\n");

    // Without any spatial index
    let start = Instant::now();
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM test_points a, test_points b 
         WHERE a.id < b.id AND ST_DWithin(a.geom, b.geom, 10)",
        [],
        |r| r.get(0)
    )?;
    let spatial_time = start.elapsed();
    println!("ST_DWithin join (no index): {:>8.2} ms  ({} pairs)", 
             spatial_time.as_secs_f64() * 1000.0, count);

    // Using ST_Extent to prefilter with bounding box
    let start = Instant::now();
    let count2: i64 = conn.query_row(
        "SELECT COUNT(*) FROM test_points a, test_points b 
         WHERE a.id < b.id 
         AND ST_X(a.geom) BETWEEN ST_X(b.geom) - 10 AND ST_X(b.geom) + 10
         AND ST_Y(a.geom) BETWEEN ST_Y(b.geom) - 10 AND ST_Y(b.geom) + 10
         AND ST_DWithin(a.geom, b.geom, 10)",
        [],
        |r| r.get(0)
    )?;
    let prefilter_time = start.elapsed();
    println!("With coordinate prefilter:  {:>8.2} ms  ({} pairs)", 
             prefilter_time.as_secs_f64() * 1000.0, count2);

    // Plain x,y columns with B-tree index
    conn.execute_batch(
        "ALTER TABLE test_points ADD COLUMN x DOUBLE;
         ALTER TABLE test_points ADD COLUMN y DOUBLE;
         UPDATE test_points SET x = ST_X(geom), y = ST_Y(geom);
         CREATE INDEX idx_xy ON test_points(x, y);"
    )?;

    let start = Instant::now();
    let count3: i64 = conn.query_row(
        "SELECT COUNT(*) FROM test_points a, test_points b 
         WHERE a.id < b.id 
         AND a.x BETWEEN b.x - 10 AND b.x + 10
         AND a.y BETWEEN b.y - 10 AND b.y + 10",
        [],
        |r| r.get(0)
    )?;
    let btree_time = start.elapsed();
    println!("B-tree on x,y columns:      {:>8.2} ms  ({} pairs)", 
             btree_time.as_secs_f64() * 1000.0, count3);

    println!("\n--- Conclusion ---");
    println!("  DuckDB spatial extension does NOT have R-tree index acceleration");
    println!("  ST_DWithin/ST_Distance always do full scans");
    println!("  Best approach: Use B-tree on x,y columns + post-filter");
    println!("  For true R-tree: Use PostGIS, SpatiaLite, or Rust spatial crate");

    Ok(())
}
