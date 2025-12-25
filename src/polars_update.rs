//! Investigation: SQL UPDATE-like operations in Polars
//!
//! SQL UPDATE modifies rows in-place based on conditions:
//!   UPDATE entities SET health = health - 10 WHERE id IN (SELECT target_id FROM attacks)
//!
//! This explores how to achieve similar semantics in Polars.

use polars::prelude::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Polars UPDATE Investigation ===\n");

    // Create test entities
    let n: usize = 10;
    let df = df!(
        "id" => (0..n as i32).collect::<Vec<i32>>(),
        "name" => (0..n).map(|i| format!("entity_{}", i)).collect::<Vec<String>>(),
        "health" => vec![100i32; n],
        "x" => (0..n).map(|i| (i * 10) as f64).collect::<Vec<f64>>(),
        "y" => (0..n).map(|i| (i * 5) as f64).collect::<Vec<f64>>()
    )?;

    println!("Initial state:");
    println!("{}\n", df);

    // ============================================================
    // Method 1: with_column + when/then/otherwise (conditional update)
    // SQL: UPDATE entities SET health = health - 10 WHERE id IN (1, 3, 5)
    // ============================================================
    println!("--- Method 1: with_column + when/then/otherwise ---");
    println!("  SQL: UPDATE entities SET health = health - 10 WHERE id IN (1, 3, 5)\n");

    let targets = vec![1i32, 3, 5];
    let target_series = Series::new("targets".into(), &targets);

    let updated1 = df.clone().lazy()
        .with_column(
            when(col("id").is_in(target_series.clone().lit(), false))
                .then(col("health") - lit(10))
                .otherwise(col("health"))
                .alias("health")
        )
        .collect()?;

    println!("After update (targets: {:?}):", targets);
    println!("{}\n", updated1);

    // ============================================================
    // Method 2: Multiple column update
    // SQL: UPDATE entities SET health = health - 10, x = x + 5 WHERE id > 5
    // ============================================================
    println!("--- Method 2: Multiple column update ---");
    println!("  SQL: UPDATE entities SET health = health - 10, x = x + 5 WHERE id > 5\n");

    let updated2 = df.clone().lazy()
        .with_columns([
            when(col("id").gt(lit(5)))
                .then(col("health") - lit(10))
                .otherwise(col("health"))
                .alias("health"),
            when(col("id").gt(lit(5)))
                .then(col("x") + lit(5.0))
                .otherwise(col("x"))
                .alias("x"),
        ])
        .collect()?;

    println!("After update (id > 5):");
    println!("{}\n", updated2);

    // ============================================================
    // Method 3: Join-based update (like UPDATE with subquery)
    // SQL: UPDATE e SET health = health - d.damage 
    //      FROM entities e JOIN damage_events d ON e.id = d.target_id
    // ============================================================
    println!("--- Method 3: Join-based update ---");
    println!("  SQL: UPDATE e SET health = health - d.damage FROM damage_events d WHERE e.id = d.target_id\n");

    // Damage events table
    let damage_events = df!(
        "target_id" => vec![2i32, 4, 6, 2],  // entity 2 hit twice!
        "damage" => vec![15i32, 20, 25, 10]
    )?;
    println!("Damage events:");
    println!("{}\n", damage_events);

    // Option A: Group damage first, then join
    let total_damage = damage_events.clone().lazy()
        .group_by([col("target_id")])
        .agg([col("damage").sum().alias("total_damage")])
        .collect()?;

    println!("Aggregated damage per target:");
    println!("{}\n", total_damage);

    let updated3a = df.clone().lazy()
        .join(
            total_damage.lazy(),
            [col("id")],
            [col("target_id")],
            JoinArgs::new(JoinType::Left)
        )
        .with_column(
            (col("health") - col("total_damage").fill_null(lit(0)))
                .alias("health")
        )
        .drop(Selector::ByName { 
            names: vec![PlSmallStr::from("total_damage")].into(), 
            strict: false 
        })
        .collect()?;

    println!("After join-based update:");
    println!("{}\n", updated3a);

    // ============================================================
    // Method 4: Semi-join pattern (check existence)
    // SQL: UPDATE entities SET status = 'attacked' WHERE id IN (SELECT target_id FROM damage_events)
    // ============================================================
    println!("--- Method 4: Semi-join pattern ---");
    println!("  SQL: UPDATE entities SET status = 'attacked' WHERE id IN (SELECT target_id FROM damage_events)\n");

    let attacked_ids = damage_events.clone().lazy()
        .select([col("target_id")])
        .unique(None, UniqueKeepStrategy::First)
        .collect()?;

    let attacked_series = attacked_ids.column("target_id")?.as_materialized_series().clone();

    let updated4 = df.clone().lazy()
        .with_column(
            when(col("id").is_in(attacked_series.lit(), false))
                .then(lit("attacked"))
                .otherwise(lit("normal"))
                .alias("status")
        )
        .collect()?;

    println!("After adding status:");
    println!("{}\n", updated4);

    // ============================================================
    // Method 5: Performance test - large scale updates
    // ============================================================
    println!("=== Performance Test: Large Scale Updates ===\n");

    for size in [1000usize, 10000, 100000, 1000000] {
        let large_df = df!(
            "id" => (0..size as i32).collect::<Vec<i32>>(),
            "health" => vec![100i32; size],
            "x" => (0..size).map(|i| (i % 1000) as f64).collect::<Vec<f64>>(),
            "y" => (0..size).map(|i| (i % 1000) as f64).collect::<Vec<f64>>()
        )?;

        // Update ~10% of rows
        let update_count = size / 10;
        let update_ids: Vec<i32> = (0..update_count).map(|i| (i * 10) as i32).collect();
        let update_series = Series::new("ids".into(), &update_ids);

        let start = Instant::now();
        let _result = large_df.lazy()
            .with_columns([
                when(col("id").is_in(update_series.clone().lit(), false))
                    .then(col("health") - lit(10))
                    .otherwise(col("health"))
                    .alias("health"),
                when(col("id").is_in(update_series.clone().lit(), false))
                    .then(col("x") + lit(1.0))
                    .otherwise(col("x"))
                    .alias("x"),
            ])
            .collect()?;
        let elapsed = start.elapsed();

        println!("  {} rows, {} updates: {:.2} ms ({:.0} ns/row)", 
                 size, update_count, 
                 elapsed.as_secs_f64() * 1000.0,
                 elapsed.as_nanos() as f64 / size as f64);
    }

    // ============================================================
    // Method 6: In-place mutation (if needed - breaks immutability)
    // ============================================================
    println!("\n--- Method 6: In-place mutation (ChunkedArray) ---\n");

    let mut mutable_df = df.clone();
    println!("Before in-place update:");
    println!("{}\n", mutable_df);

    // Get mutable access to health column
    {
        let health_col = mutable_df.column("health")?.i32()?.clone();
        let id_col = mutable_df.column("id")?.i32()?.clone();
        
        // Create new values based on condition
        let new_health: Int32Chunked = health_col.iter()
            .zip(id_col.iter())
            .map(|(h, id)| {
                match (h, id) {
                    (Some(health), Some(id)) if id % 2 == 0 => Some(health - 5),
                    (h, _) => h,
                }
            })
            .collect();
        
        // Replace column (this creates a new DataFrame internally)
        let _ = mutable_df.replace("health", new_health.into_series());
    }

    println!("After in-place update (even ids -= 5):");
    println!("{}\n", mutable_df);

    // ============================================================
    // Summary
    // ============================================================
    println!("=== Summary: UPDATE-like Operations in Polars ===\n");
    println!("  ✅ Method 1: when/then/otherwise - Simple conditional updates");
    println!("  ✅ Method 2: Multiple with_columns - Update multiple columns");
    println!("  ✅ Method 3: Join + compute - Complex updates from other tables");
    println!("  ✅ Method 4: is_in() - Update based on subquery results");
    println!("  ✅ Method 5: Scales well - 1M rows update in ~ms");
    println!("  ⚠️  Method 6: In-place mutation - Possible but not idiomatic");
    println!();
    println!("  Key differences from SQL UPDATE:");
    println!("    - Polars is immutable by default (creates new DataFrame)");
    println!("    - No row-level locking or transactions");
    println!("    - Vectorized operations are MUCH faster than row-by-row");
    println!("    - Pattern: filter + transform + union OR when/then/otherwise");

    Ok(())
}
