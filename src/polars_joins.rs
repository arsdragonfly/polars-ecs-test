//! Polars Join Investigation
//! 
//! Exploring complex joins in Polars, especially cross joins for spatial queries

use polars::prelude::*;
use polars_ops::frame::MaintainOrderJoin;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Polars Join Investigation ===\n");

    // Create test data
    let n = 100;
    let ids: Vec<i32> = (0..n).collect();
    let xs: Vec<f64> = (0..n).map(|i| (i * 17 % 100) as f64).collect();
    let ys: Vec<f64> = (0..n).map(|i| (i * 23 % 100) as f64).collect();
    
    let df = df!(
        "id" => &ids,
        "x" => &xs,
        "y" => &ys
    )?;
    
    println!("Test DataFrame: {} rows\n", df.height());
    println!("{}\n", df.head(Some(5)));

    // ============================================================
    // Method 1: cross_join (dedicated method)
    // ============================================================
    println!("--- Method 1: cross_join() ---\n");
    
    let start = Instant::now();
    let cross = df.cross_join(&df, Some("_right".into()), None, MaintainOrderJoin::None)?;
    let cross_time = start.elapsed();
    
    println!("  Result: {} rows ({}×{})", cross.height(), n, n);
    println!("  Time: {:.3} ms", cross_time.as_secs_f64() * 1000.0);
    println!("  Columns: {:?}\n", cross.get_column_names());
    
    // ============================================================
    // Method 2: Filter for upper triangle (e1.id < e2.id)
    // ============================================================
    println!("--- Method 2: Cross join + filter (id < id_right) ---\n");
    
    let start = Instant::now();
    let filtered = cross.lazy()
        .filter(col("id").lt(col("id_right")))
        .collect()?;
    let filter_time = start.elapsed();
    
    println!("  Result: {} rows (n*(n-1)/2 = {})", filtered.height(), n*(n-1)/2);
    println!("  Time: {:.3} ms\n", filter_time.as_secs_f64() * 1000.0);

    // ============================================================
    // Method 3: Compute distances
    // ============================================================
    println!("--- Method 3: Cross join + distance calculation ---\n");
    
    let start = Instant::now();
    let with_dist = df.cross_join(&df, Some("_right".into()), None, MaintainOrderJoin::None)?
        .lazy()
        .filter(col("id").lt(col("id_right")))
        .with_column(
            ((col("x") - col("x_right")).pow(2) + (col("y") - col("y_right")).pow(2))
                .sqrt()
                .alias("distance")
        )
        .collect()?;
    let dist_time = start.elapsed();
    
    println!("  Result shape: {:?}", with_dist.shape());
    println!("  Time: {:.3} ms", dist_time.as_secs_f64() * 1000.0);
    println!("\n  Sample:\n{}\n", with_dist.head(Some(5)));

    // ============================================================
    // Method 4: Filter by distance (spatial query!)
    // ============================================================
    println!("--- Method 4: Pairs within distance 20 ---\n");
    
    let start = Instant::now();
    let nearby = df.cross_join(&df, Some("_right".into()), None, MaintainOrderJoin::None)?
        .lazy()
        .filter(col("id").lt(col("id_right")))
        .with_column(
            ((col("x") - col("x_right")).pow(2) + (col("y") - col("y_right")).pow(2))
                .sqrt()
                .alias("distance")
        )
        .filter(col("distance").lt(lit(20.0)))
        .collect()?;
    let nearby_time = start.elapsed();
    
    println!("  Pairs within dist 20: {} (of {})", nearby.height(), n*(n-1)/2);
    println!("  Time: {:.3} ms\n", nearby_time.as_secs_f64() * 1000.0);

    // ============================================================
    // Method 5: Aggregation (sum of all distances)
    // ============================================================
    println!("--- Method 5: Sum of all distances ---\n");
    
    let start = Instant::now();
    let sum_result = df.cross_join(&df, Some("_right".into()), None, MaintainOrderJoin::None)?
        .lazy()
        .filter(col("id").lt(col("id_right")))
        .select([
            ((col("x") - col("x_right")).pow(2) + (col("y") - col("y_right")).pow(2))
                .sqrt()
                .sum()
                .alias("total_distance")
        ])
        .collect()?;
    let sum_time = start.elapsed();
    
    println!("  Result:\n{}", sum_result);
    println!("  Time: {:.3} ms\n", sum_time.as_secs_f64() * 1000.0);

    // ============================================================
    // Method 6: LazyFrame cross join (the one that failed before)
    // ============================================================
    println!("--- Method 6: LazyFrame join with JoinType::Cross ---\n");
    
    // NOTE: This appears to be a bug in Polars - LazyFrame cross join with
    // empty key arrays panics. Use DataFrame.cross_join() instead!
    println!("  ⚠️  SKIP: LazyFrame cross join with empty keys panics in Polars 0.46");
    println!("  Use DataFrame.cross_join() instead!\n");

    // ============================================================
    // Scaling test
    // ============================================================
    println!("=== Scaling Test (Cross Join) ===\n");
    
    for size in [100, 500, 1000, 2000] {
        let ids: Vec<i32> = (0..size).collect();
        let xs: Vec<f64> = (0..size).map(|i| (i * 17 % 1000) as f64).collect();
        let ys: Vec<f64> = (0..size).map(|i| (i * 23 % 1000) as f64).collect();
        
        let test_df = df!(
            "id" => &ids,
            "x" => &xs,
            "y" => &ys
        )?;
        
        let start = Instant::now();
        let result = test_df.cross_join(&test_df, Some("_right".into()), None, MaintainOrderJoin::None)?
            .lazy()
            .filter(col("id").lt(col("id_right")))
            .select([
                ((col("x") - col("x_right")).pow(2) + (col("y") - col("y_right")).pow(2))
                    .sqrt()
                    .sum()
                    .alias("total")
            ])
            .collect()?;
        let time = start.elapsed();
        
        let pairs = size * (size - 1) / 2;
        println!("  {} entities → {} pairs: {:>8.2} ms ({:.1} ns/pair)", 
                 size, pairs, time.as_secs_f64() * 1000.0,
                 time.as_secs_f64() * 1_000_000_000.0 / pairs as f64);
    }

    // ============================================================
    // Compare to DuckDB
    // ============================================================
    println!("\n=== Compare to DuckDB (1000 entities) ===\n");
    
    use duckdb::Connection;
    
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        "CREATE TABLE entities AS 
         SELECT i as id, 
                ((i * 17) % 1000)::DOUBLE as x,
                ((i * 23) % 1000)::DOUBLE as y
         FROM generate_series(0, 999) AS t(i)"
    )?;
    
    // DuckDB cross join
    let mut stmt = conn.prepare(
        "SELECT sum(sqrt((e1.x - e2.x)^2 + (e1.y - e2.y)^2))
         FROM entities e1, entities e2 
         WHERE e1.id < e2.id"
    )?;
    
    let start = Instant::now();
    let _: f64 = stmt.query_row([], |r| r.get(0))?;
    let duck_time = start.elapsed();
    println!("  DuckDB:  {:>8.2} ms", duck_time.as_secs_f64() * 1000.0);
    
    // Polars
    let ids: Vec<i32> = (0..1000).collect();
    let xs: Vec<f64> = (0..1000).map(|i| (i * 17 % 1000) as f64).collect();
    let ys: Vec<f64> = (0..1000).map(|i| (i * 23 % 1000) as f64).collect();
    let polars_df = df!("id" => &ids, "x" => &xs, "y" => &ys)?;
    
    let start = Instant::now();
    let _ = polars_df.cross_join(&polars_df, Some("_right".into()), None, MaintainOrderJoin::None)?
        .lazy()
        .filter(col("id").lt(col("id_right")))
        .select([
            ((col("x") - col("x_right")).pow(2) + (col("y") - col("y_right")).pow(2))
                .sqrt()
                .sum()
        ])
        .collect()?;
    let polars_time = start.elapsed();
    println!("  Polars:  {:>8.2} ms  ({:.2}× vs DuckDB)", 
             polars_time.as_secs_f64() * 1000.0,
             polars_time.as_secs_f64() / duck_time.as_secs_f64());

    println!("\n=== Summary ===\n");
    println!("  Polars DOES support cross joins!");
    println!("  - DataFrame.cross_join() - eager, works great");
    println!("  - LazyFrame.join(..., JoinType::Cross) - needs empty key arrays");
    println!();
    println!("  For spatial queries:");
    println!("  - Polars cross join is ~similar speed to DuckDB");
    println!("  - Both are O(N²) without spatial indexing");
    println!("  - Use spatial hashing for real games!");

    Ok(())
}
