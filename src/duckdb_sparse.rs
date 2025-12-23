//! DuckDB Sparse Column Exploration
//!
//! Tests different approaches for handling sparse/optional components in DuckDB:
//! 1. Traditional nullable columns (current approach)
//! 2. MAP<VARCHAR, ANY> for dynamic sparse components
//! 3. Separate component tables with JOINs (relational ECS pattern)
//! 4. JSON/STRUCT columns for component groups

use duckdb::{Connection, Result};
use std::time::Instant;

const SIZE: usize = 1_000_000; // 1M entities

fn main() -> Result<()> {
    println!("=== DuckDB Sparse Component Benchmarks ===\n");
    println!("Entity count: {}\n", SIZE);

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 8;")?;

    // Test 1: Traditional nullable columns (baseline)
    bench_nullable_columns(&conn)?;

    // Test 2: MAP type for sparse components
    bench_map_components(&conn)?;

    // Test 3: Separate component tables (relational pattern)
    bench_component_tables(&conn)?;

    // Test 4: Adding columns dynamically
    bench_alter_table_add(&conn)?;

    // Test 5: UNION type (DuckDB's dynamic typing)
    bench_union_type(&conn)?;

    Ok(())
}

/// Baseline: Nullable columns for optional components
fn bench_nullable_columns(conn: &Connection) -> Result<()> {
    println!("--- Test 1: Nullable Columns (Baseline) ---");

    let start = Instant::now();

    // Create table with nullable component columns
    // Only ~10% of entities have the "rare_component"
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities_nullable;
        CREATE TABLE entities_nullable AS
        SELECT
            i AS id,
            i * 1.0 AS x,
            i * 2.0 AS y,
            -- Sparse component: only 10% have it
            CASE WHEN i % 10 = 0 THEN i * 100 ELSE NULL END AS rare_value,
            CASE WHEN i % 10 = 0 THEN 'rare_'|| i ELSE NULL END AS rare_name
        FROM generate_series(0, {SIZE} - 1) AS t(i);
        "
    ))?;

    let create_time = start.elapsed();
    println!("  Create with sparse nulls: {:?}", create_time);

    // Query only entities with the rare component
    let start = Instant::now();
    for _ in 0..100 {
        conn.execute_batch(
            "SELECT id, rare_value FROM entities_nullable WHERE rare_value IS NOT NULL;"
        )?;
    }
    let query_time = start.elapsed();
    println!("  Query sparse (100x): {:?} ({:.2} µs/query)", query_time, query_time.as_nanos() as f64 / 100.0 / 1000.0);

    // Update sparse component
    let start = Instant::now();
    for _ in 0..100 {
        conn.execute_batch(
            "UPDATE entities_nullable SET rare_value = rare_value + 1 WHERE rare_value IS NOT NULL;"
        )?;
    }
    let update_time = start.elapsed();
    println!("  Update sparse (100x): {:?} ({:.2} µs/query)", update_time, update_time.as_nanos() as f64 / 100.0 / 1000.0);

    // Add a new sparse column
    let start = Instant::now();
    conn.execute_batch(
        "ALTER TABLE entities_nullable ADD COLUMN new_sparse INTEGER DEFAULT NULL;"
    )?;
    let add_time = start.elapsed();
    println!("  Add nullable column: {:?}", add_time);

    println!();
    Ok(())
}

/// MAP type for truly dynamic sparse components
fn bench_map_components(conn: &Connection) -> Result<()> {
    println!("--- Test 2: MAP Type for Sparse Components ---");

    let start = Instant::now();

    // Use MAP to store arbitrary key-value components
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities_map;
        CREATE TABLE entities_map AS
        SELECT
            i AS id,
            i * 1.0 AS x,
            i * 2.0 AS y,
            -- Sparse components stored in a MAP
            CASE 
                WHEN i % 10 = 0 THEN MAP {{'rare_value': i * 100, 'rare_score': i % 50}}
                ELSE MAP {{}}
            END AS components
        FROM generate_series(0, {SIZE} - 1) AS t(i);
        "
    ))?;

    let create_time = start.elapsed();
    println!("  Create with MAP: {:?}", create_time);

    // Query entities with a specific component
    let start = Instant::now();
    for _ in 0..100 {
        conn.execute_batch(
            "SELECT id, components['rare_value'] 
             FROM entities_map 
             WHERE map_contains(components, 'rare_value');"
        )?;
    }
    let query_time = start.elapsed();
    println!("  Query MAP key (100x): {:?} ({:.2} µs/query)", query_time, query_time.as_nanos() as f64 / 100.0 / 1000.0);

    // Add a new component to some entities (via UPDATE)
    let start = Instant::now();
    conn.execute_batch(
        "UPDATE entities_map 
         SET components = map_concat(components, MAP {'new_component': id * 5})
         WHERE id % 20 = 0;"
    )?;
    let add_component_time = start.elapsed();
    println!("  Add component to 5%: {:?}", add_component_time);

    // No schema change needed for new component types!
    println!("  (No ALTER TABLE needed for new component types)");

    println!();
    Ok(())
}

/// Separate tables per component (classic relational ECS)
fn bench_component_tables(conn: &Connection) -> Result<()> {
    println!("--- Test 3: Separate Component Tables ---");

    let start = Instant::now();

    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entity_base;
        DROP TABLE IF EXISTS component_position;
        DROP TABLE IF EXISTS component_rare;

        -- Base entity table (just IDs)
        CREATE TABLE entity_base AS
        SELECT i AS id FROM generate_series(0, {SIZE} - 1) AS t(i);

        -- Position component (all entities have it)
        CREATE TABLE component_position AS
        SELECT i AS entity_id, i * 1.0 AS x, i * 2.0 AS y
        FROM generate_series(0, {SIZE} - 1) AS t(i);

        -- Rare component (only 10% of entities)
        CREATE TABLE component_rare AS
        SELECT i AS entity_id, i * 100 AS value, 'rare_' || i AS name
        FROM generate_series(0, {SIZE} - 1) AS t(i)
        WHERE i % 10 = 0;

        CREATE INDEX idx_rare_entity ON component_rare(entity_id);
        "
    ))?;

    let create_time = start.elapsed();
    println!("  Create component tables: {:?}", create_time);

    // Query entities with rare component
    let start = Instant::now();
    for _ in 0..100 {
        conn.execute_batch(
            "SELECT e.id, r.value 
             FROM entity_base e
             JOIN component_rare r ON e.id = r.entity_id;"
        )?;
    }
    let query_time = start.elapsed();
    println!("  Query with JOIN (100x): {:?} ({:.2} µs/query)", query_time, query_time.as_nanos() as f64 / 100.0 / 1000.0);

    // Add new component type (just create a new table!)
    let start = Instant::now();
    conn.execute_batch(&format!(
        "
        CREATE TABLE component_new AS
        SELECT i AS entity_id, i * 5 AS new_value
        FROM generate_series(0, {SIZE} - 1) AS t(i)
        WHERE i % 50 = 0;
        "
    ))?;
    let add_type_time = start.elapsed();
    println!("  Add new component type (2%): {:?}", add_type_time);

    // Insert component for specific entity
    let start = Instant::now();
    for i in 0..1000 {
        conn.execute(
            "INSERT INTO component_rare VALUES (?, ?, ?)",
            duckdb::params![SIZE + i, i * 100, format!("new_{}", i)]
        )?;
    }
    let insert_time = start.elapsed();
    println!("  Insert 1000 components: {:?} ({:.2} µs/insert)", insert_time, insert_time.as_nanos() as f64 / 1000.0 / 1000.0);

    println!();
    Ok(())
}

/// ALTER TABLE ADD COLUMN performance
fn bench_alter_table_add(conn: &Connection) -> Result<()> {
    println!("--- Test 4: ALTER TABLE ADD COLUMN ---");

    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities_alter;
        CREATE TABLE entities_alter AS
        SELECT i AS id, i * 1.0 AS x
        FROM generate_series(0, {SIZE} - 1) AS t(i);
        "
    ))?;

    // Add columns one by one
    let column_types = [
        ("col_int", "INTEGER DEFAULT NULL"),
        ("col_float", "FLOAT DEFAULT NULL"),
        ("col_varchar", "VARCHAR DEFAULT NULL"),
        ("col_bool", "BOOLEAN DEFAULT NULL"),
        ("col_int_default", "INTEGER DEFAULT 0"),
        ("col_float_default", "FLOAT DEFAULT 0.0"),
    ];

    for (name, def) in column_types {
        let start = Instant::now();
        conn.execute_batch(&format!(
            "ALTER TABLE entities_alter ADD COLUMN {name} {def};"
        ))?;
        let time = start.elapsed();
        println!("  Add {} ({}): {:?}", name, def, time);
    }

    // DuckDB optimization: DEFAULT NULL columns are very fast because
    // they don't need to write any data until a non-NULL value is set
    println!("\n  Note: DEFAULT NULL columns are O(1) in DuckDB!");
    println!("  They use 'lazy materialization' - no storage until needed.");

    println!();
    Ok(())
}

/// UNION type for heterogeneous values
fn bench_union_type(conn: &Connection) -> Result<()> {
    println!("--- Test 5: UNION Type for Variant Components ---");

    let start = Instant::now();

    // UNION type allows different types in same column
    // Must explicitly construct with all union members
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities_union;
        CREATE TABLE entities_union (
            id INTEGER,
            variant_data UNION(int_val INTEGER, str_val VARCHAR, float_val FLOAT)
        );

        INSERT INTO entities_union
        SELECT
            i AS id,
            CASE 
                WHEN i % 3 = 0 THEN i * 10
                WHEN i % 3 = 1 THEN NULL  
                ELSE NULL
            END::UNION(int_val INTEGER, str_val VARCHAR, float_val FLOAT) AS variant_data
        FROM generate_series(0, {SIZE} - 1) AS t(i)
        WHERE i % 3 = 0;

        INSERT INTO entities_union
        SELECT
            i AS id,
            ('entity_' || i)::UNION(int_val INTEGER, str_val VARCHAR, float_val FLOAT)
        FROM generate_series(0, {SIZE} - 1) AS t(i)
        WHERE i % 3 = 1;

        INSERT INTO entities_union
        SELECT
            i AS id,
            (i * 0.5)::UNION(int_val INTEGER, str_val VARCHAR, float_val FLOAT)
        FROM generate_series(0, {SIZE} - 1) AS t(i)
        WHERE i % 3 = 2;
        "
    ))?;

    let create_time = start.elapsed();
    println!("  Create with UNION: {:?}", create_time);

    // Query by type
    let start = Instant::now();
    for _ in 0..100 {
        conn.execute_batch(
            "SELECT id, union_extract(variant_data, 'int_val') 
             FROM entities_union 
             WHERE union_tag(variant_data) = 'int_val';"
        )?;
    }
    let query_time = start.elapsed();
    println!("  Query by tag (100x): {:?} ({:.2} µs/query)", query_time, query_time.as_nanos() as f64 / 100.0 / 1000.0);

    println!();
    Ok(())
}
