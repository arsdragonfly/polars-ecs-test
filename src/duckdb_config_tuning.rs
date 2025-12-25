//! Test DuckDB configuration parameters for overhead reduction
//!
//! Explore settings that might reduce per-query latency

use duckdb::{Connection, Config, params};
use std::time::Instant;
use std::hint::black_box;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== DuckDB Configuration Tuning for Low Latency ===\n");
    
    const ITERS: usize = 1000;
    
    // ============================================================
    // First, list all available settings
    // ============================================================
    println!("--- Available Settings (performance-related) ---\n");
    
    let conn = Connection::open_in_memory()?;
    let mut stmt = conn.prepare("SELECT name, COALESCE(CAST(value AS VARCHAR), 'NULL'), description FROM duckdb_settings() WHERE name LIKE '%thread%' OR name LIKE '%parallel%' OR name LIKE '%optim%' OR name LIKE '%profil%' OR name LIKE '%preserve%' OR name LIKE '%external%' ORDER BY name")?;
    let mut rows = stmt.query([])?;
    
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let value: String = row.get(1)?;
        let desc: String = row.get(2)?;
        println!("  {}: {} \n    → {}\n", name, value, desc);
    }
    
    // ============================================================
    // Test: Default config baseline
    // ============================================================
    println!("\n--- Baseline (default config) ---\n");
    
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t(id INTEGER, v DOUBLE)")?;
    conn.execute_batch("INSERT INTO t SELECT i, random() FROM generate_series(1,1000) t(i)")?;
    
    let baseline = measure_overhead(&conn, ITERS)?;
    println!("  Prepared SELECT 42:          {:.1} µs", baseline.0);
    println!("  Prepared table lookup:       {:.1} µs", baseline.1);
    
    // ============================================================
    // Test: Single thread (disable parallelism overhead?)
    // ============================================================
    println!("\n--- Single thread (threads=1) ---\n");
    
    let config = Config::default().with("threads", "1")?;
    let conn = Connection::open_in_memory_with_flags(config)?;
    conn.execute_batch("CREATE TABLE t(id INTEGER, v DOUBLE)")?;
    conn.execute_batch("INSERT INTO t SELECT i, random() FROM generate_series(1,1000) t(i)")?;
    
    let single = measure_overhead(&conn, ITERS)?;
    println!("  Prepared SELECT 42:          {:.1} µs  ({:+.1}%)", 
             single.0, (single.0 - baseline.0) / baseline.0 * 100.0);
    println!("  Prepared table lookup:       {:.1} µs  ({:+.1}%)", 
             single.1, (single.1 - baseline.1) / baseline.1 * 100.0);
    
    // ============================================================
    // Test: Disable optimizer (new syntax in 1.4.3)
    // ============================================================
    println!("\n--- Optimizer disabled (via disabled_optimizers) ---\n");
    
    // Note: In 1.4.3, you can't fully disable optimizer, only specific ones
    // Let's skip this and test what we can
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t(id INTEGER, v DOUBLE)")?;
    conn.execute_batch("INSERT INTO t SELECT i, random() FROM generate_series(1,1000) t(i)")?;
    
    let no_opt = measure_overhead(&conn, ITERS)?;
    println!("  (Cannot fully disable in 1.4.3 - skipping)");
    
    // ============================================================
    // Test: Disable progress bar - skip as setting syntax changed
    // ============================================================
    println!("\n--- Progress bar disabled ---\n");
    
    // Progress bar setting changed in 1.4.3, use baseline
    let no_progress = baseline;
    println!("  (Skipped - setting syntax changed in 1.4.3)");
    
    // ============================================================
    // Test: Disable object cache
    // ============================================================
    println!("\n--- Object cache (legacy setting - does nothing in 1.4.3) ---\n");
    
    let conn = Connection::open_in_memory()?;
    // Skip - this is a legacy placeholder in 1.4.3
    conn.execute_batch("CREATE TABLE t(id INTEGER, v DOUBLE)")?;
    conn.execute_batch("INSERT INTO t SELECT i, random() FROM generate_series(1,1000) t(i)")?;
    
    let no_cache = measure_overhead(&conn, ITERS)?;
    println!("  (Skipped - legacy placeholder)");
    
    // ============================================================
    // Test: Combined "low latency" settings
    // ============================================================
    println!("\n--- Combined low-latency settings ---\n");
    
    let config = Config::default()
        .with("threads", "1")?
        .with("external_threads", "0")?
        .with("preserve_insertion_order", "false")?;
    let conn = Connection::open_in_memory_with_flags(config)?;
    conn.execute_batch("CREATE TABLE t(id INTEGER, v DOUBLE)")?;
    conn.execute_batch("INSERT INTO t SELECT i, random() FROM generate_series(1,1000) t(i)")?;
    
    let combined = measure_overhead(&conn, ITERS)?;
    println!("  Prepared SELECT 42:          {:.1} µs  ({:+.1}%)", 
             combined.0, (combined.0 - baseline.0) / baseline.0 * 100.0);
    println!("  Prepared table lookup:       {:.1} µs  ({:+.1}%)", 
             combined.1, (combined.1 - baseline.1) / baseline.1 * 100.0);
    
    // ============================================================
    // Test: Preserve insertion order (might affect scans)
    // ============================================================
    println!("\n--- preserve_insertion_order = false ---\n");
    
    let config = Config::default()
        .with("preserve_insertion_order", "false")?;
    let conn = Connection::open_in_memory_with_flags(config)?;
    conn.execute_batch("CREATE TABLE t(id INTEGER, v DOUBLE)")?;
    conn.execute_batch("INSERT INTO t SELECT i, random() FROM generate_series(1,1000) t(i)")?;
    
    let no_order = measure_overhead(&conn, ITERS)?;
    println!("  Prepared SELECT 42:          {:.1} µs  ({:+.1}%)", 
             no_order.0, (no_order.0 - baseline.0) / baseline.0 * 100.0);
    println!("  Prepared table lookup:       {:.1} µs  ({:+.1}%)", 
             no_order.1, (no_order.1 - baseline.1) / baseline.1 * 100.0);
    
    // ============================================================
    // Test: External threads (for game integration)
    // ============================================================
    println!("\n--- external_threads = 0 ---\n");
    
    let config = Config::default()
        .with("external_threads", "0")?;
    let conn = Connection::open_in_memory_with_flags(config)?;
    conn.execute_batch("CREATE TABLE t(id INTEGER, v DOUBLE)")?;
    conn.execute_batch("INSERT INTO t SELECT i, random() FROM generate_series(1,1000) t(i)")?;
    
    let no_ext = measure_overhead(&conn, ITERS)?;
    println!("  Prepared SELECT 42:          {:.1} µs  ({:+.1}%)", 
             no_ext.0, (no_ext.0 - baseline.0) / baseline.0 * 100.0);
    println!("  Prepared table lookup:       {:.1} µs  ({:+.1}%)", 
             no_ext.1, (no_ext.1 - baseline.1) / baseline.1 * 100.0);
    
    // ============================================================
    // Additional settings to explore
    // ============================================================
    println!("\n--- All performance-relevant settings ---\n");
    
    let conn = Connection::open_in_memory()?;
    let settings = [
        "threads", "external_threads", "memory_limit", 
        "max_expression_depth", "perfect_ht_threshold",
        "enable_optimizer", "enable_progress_bar", "enable_object_cache",
        "preserve_insertion_order", "force_index_join",
        "disabled_optimizers", "enable_profiling",
    ];
    
    for setting in settings {
        let result: Result<String, _> = conn.query_row(
            &format!("SELECT value FROM duckdb_settings() WHERE name = '{}'", setting),
            [],
            |r| r.get(0)
        );
        if let Ok(value) = result {
            println!("  {}: {}", setting, value);
        }
    }
    
    // ============================================================
    // Summary
    // ============================================================
    println!("\n=== Summary ===\n");
    println!("  ┌────────────────────────────────┬─────────────┬─────────────┐");
    println!("  │ Configuration                  │ SELECT 42   │ Table Lookup│");
    println!("  ├────────────────────────────────┼─────────────┼─────────────┤");
    println!("  │ Default (baseline)             │ {:>7.1} µs  │ {:>7.1} µs  │", baseline.0, baseline.1);
    println!("  │ threads=1                      │ {:>7.1} µs  │ {:>7.1} µs  │", single.0, single.1);
    println!("  │ optimizer=off                  │ {:>7.1} µs  │ {:>7.1} µs  │", no_opt.0, no_opt.1);
    println!("  │ progress_bar=off               │ {:>7.1} µs  │ {:>7.1} µs  │", no_progress.0, no_progress.1);
    println!("  │ object_cache=off               │ {:>7.1} µs  │ {:>7.1} µs  │", no_cache.0, no_cache.1);
    println!("  │ preserve_insertion_order=off   │ {:>7.1} µs  │ {:>7.1} µs  │", no_order.0, no_order.1);
    println!("  │ external_threads=0             │ {:>7.1} µs  │ {:>7.1} µs  │", no_ext.0, no_ext.1);
    println!("  │ Combined (low-latency)         │ {:>7.1} µs  │ {:>7.1} µs  │", combined.0, combined.1);
    println!("  └────────────────────────────────┴─────────────┴─────────────┘");
    
    let best_select = [baseline.0, single.0, no_opt.0, no_progress.0, no_cache.0, no_order.0, no_ext.0, combined.0]
        .iter().cloned().fold(f64::MAX, f64::min);
    let best_lookup = [baseline.1, single.1, no_opt.1, no_progress.1, no_cache.1, no_order.1, no_ext.1, combined.1]
        .iter().cloned().fold(f64::MAX, f64::min);
    
    println!("\n  Best achieved: SELECT 42 = {:.1} µs, Table lookup = {:.1} µs", best_select, best_lookup);
    println!("  Reduction from baseline: {:.0}% / {:.0}%", 
             (1.0 - best_select / baseline.0) * 100.0,
             (1.0 - best_lookup / baseline.1) * 100.0);
    
    Ok(())
}

fn measure_overhead(conn: &Connection, iters: usize) -> Result<(f64, f64), Box<dyn std::error::Error>> {
    // Test 1: SELECT 42
    let mut stmt = conn.prepare("SELECT 42")?;
    let start = Instant::now();
    for _ in 0..iters {
        let mut rows = stmt.query([])?;
        black_box(rows.next()?);
    }
    let select_time = start.elapsed().as_micros() as f64 / iters as f64;
    
    // Test 2: Table lookup
    let mut stmt = conn.prepare("SELECT v FROM t WHERE id = ?1")?;
    let start = Instant::now();
    for i in 0..iters {
        let mut rows = stmt.query(params![(i % 1000) as i32 + 1])?;
        black_box(rows.next()?);
    }
    let lookup_time = start.elapsed().as_micros() as f64 / iters as f64;
    
    Ok((select_time, lookup_time))
}
