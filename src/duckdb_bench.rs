//! DuckDB ECS Microbenchmarks
//!
//! This benchmark suite tests DuckDB's viability as an Entity Component System (ECS)
//! for data-intensive games. It mirrors the Polars benchmark to enable fair comparison.
//!
//! Benchmark scenarios:
//! 1. Entity creation with multiple components
//! 2. Simple system updates (movement, data processing)
//! 3. Complex relational queries (spaceship/faction example from the article)
//! 4. Component addition/removal
//! 5. Bulk entity operations

use duckdb::{Connection, Result};
use std::time::Instant;

const SIZE: usize = 1024 * 1024 * 2; // 2M entities, matching Polars benchmark

// Entity types
const PLAYER_TYPE_NPC: i32 = 0;
const PLAYER_TYPE_MONSTER: i32 = 1;
const PLAYER_TYPE_HERO: i32 = 2;

// Health status
const HEALTH_SPAWN: i32 = 0;
#[allow(dead_code)]
const HEALTH_DEAD: i32 = 1;
const HEALTH_ALIVE: i32 = 2;

fn main() -> Result<()> {
    println!("=== DuckDB ECS Microbenchmarks ===\n");
    println!("Entity count: {}\n", SIZE);

    // Use in-memory database for maximum performance
    let conn = Connection::open_in_memory()?;

    // Enable optimizations
    conn.execute_batch(
        "
        SET threads TO 8;
        SET memory_limit = '4GB';
        ",
    )?;

    // Run benchmarks
    bench_entity_creation(&conn)?;
    bench_simple_systems(&conn)?;
    bench_complex_query(&conn)?;
    bench_component_operations(&conn)?;
    bench_filtered_updates(&conn)?;

    Ok(())
}

/// Benchmark 1: Entity Creation with Multiple Components
/// Similar to the Polars benchmark initialization phase
fn bench_entity_creation(conn: &Connection) -> Result<()> {
    println!("--- Benchmark 1: Entity Creation ---");

    let start = Instant::now();

    // Create the main entities table with all components
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities;
        CREATE TABLE entities AS
        SELECT
            i AS id,
            -- Position component
            0.0::FLOAT AS x,
            0.0::FLOAT AS y,
            -- Velocity component
            1.0::FLOAT AS vx,
            1.0::FLOAT AS vy,
            -- Data component
            0::INTEGER AS data_thingy,
            0.0::DOUBLE AS data_dingy,
            false::BOOLEAN AS data_mingy,
            (random() * 1000000)::INTEGER AS data_numgy,
            -- Player component
            CASE
                WHEN random() < 0.03 THEN {PLAYER_TYPE_NPC}
                WHEN random() < 0.30 THEN {PLAYER_TYPE_HERO}
                ELSE {PLAYER_TYPE_MONSTER}
            END AS player_type,
            -- Health component
            0::INTEGER AS health_hp,
            (5 + (random() * 10)::INTEGER) AS health_maxhp,
            {HEALTH_SPAWN}::INTEGER AS health_status,
            -- Damage component
            (2 + (random() * 5)::INTEGER) AS damage_atk,
            (2 + (random() * 5)::INTEGER) AS damage_def,
            -- Sprite component
            32::INTEGER AS sprite_char,
            -- Spawn position (like the Polars benchmark)
            ((random() * 420) - 100)::FLOAT AS spawn_x,
            ((random() * 340) - 100)::FLOAT AS spawn_y
        FROM generate_series(0, {SIZE} - 1) AS t(i);
        "
    ))?;

    let duration = start.elapsed();
    let per_entity = duration.as_nanos() as f64 / SIZE as f64;

    println!("  Total time: {:?}", duration);
    println!("  Per entity: {:.2} ns", per_entity);
    println!();

    // Verify row count
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM entities", [], |row| row.get(0))?;
    println!("  Entities created: {}", count);
    println!();

    Ok(())
}

/// Benchmark 2: Simple System Updates
/// Tests movement and data systems like the Polars benchmark
fn bench_simple_systems(conn: &Connection) -> Result<()> {
    println!("--- Benchmark 2: Simple System Updates ---");

    let iterations = 1000;
    let dt = 1.0f32 / 60.0f32;

    let start = Instant::now();

    for _ in 0..iterations {
        // Movement system + Data system combined (like Polars benchmark)
        conn.execute_batch(&format!(
            "
            UPDATE entities SET
                -- Movement system
                x = x + vx * {dt},
                y = y + vy * {dt},
                -- Data system
                data_thingy = (data_thingy + 1) % 1000000,
                data_dingy = data_dingy + 0.0001 * {dt},
                data_mingy = NOT data_mingy,
                data_numgy = ((data_numgy::BIGINT * 1103515245 + 12345) % 2147483648)::INTEGER;
            "
        ))?;
    }

    let duration = start.elapsed();
    let per_iteration = duration.as_nanos() as f64 / iterations as f64;
    let per_entity_per_iteration = per_iteration / SIZE as f64;

    println!("  Iterations: {}", iterations);
    println!("  Total time: {:?}", duration);
    println!("  Per iteration: {:.2} µs", per_iteration / 1000.0);
    println!("  Per entity per iteration: {:.2} ns", per_entity_per_iteration);
    println!();

    Ok(())
}

/// Benchmark 3: Complex Relational Query
/// Based on the spaceship/faction query from the Medium article
fn bench_complex_query(conn: &Connection) -> Result<()> {
    println!("--- Benchmark 3: Complex Relational Query ---");

    // Setup: Create tables for the spaceship example
    let setup_start = Instant::now();

    let entity_count = 100_000; // Smaller dataset for relational query

    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS game_entities;
        DROP TABLE IF EXISTS docking_status;
        DROP TABLE IF EXISTS faction_relations;

        -- Entity types: 0=Spaceship, 1=Faction, 2=SpaceStation, 3=Planet
        CREATE TABLE game_entities AS
        SELECT
            i AS entity_id,
            CASE
                WHEN random() < 0.4 THEN 0  -- Spaceship
                WHEN random() < 0.5 THEN 1  -- Faction
                WHEN random() < 0.7 THEN 2  -- SpaceStation
                ELSE 3                       -- Planet
            END AS entity_type,
            (random() * 100)::INTEGER AS owning_faction
        FROM generate_series(1, {entity_count}) AS t(i);

        -- Docking relationships (spaceships docked to planets/stations)
        CREATE TABLE docking_status AS
        SELECT
            s.entity_id AS spaceship_id,
            p.entity_id AS target_id
        FROM game_entities s, game_entities p
        WHERE s.entity_type = 0  -- Spaceship
          AND p.entity_type IN (2, 3)  -- SpaceStation or Planet
          AND random() < 0.001  -- Sparse docking relationships
        LIMIT 50000;

        -- Faction alliance relationships
        CREATE TABLE faction_relations AS
        SELECT
            f1.entity_id AS from_faction_id,
            f2.entity_id AS to_faction_id
        FROM game_entities f1, game_entities f2
        WHERE f1.entity_type = 1
          AND f2.entity_type = 1
          AND f1.entity_id != f2.entity_id
          AND random() < 0.1
        LIMIT 10000;

        -- Create indexes for join performance
        CREATE INDEX idx_entities_type ON game_entities(entity_type);
        CREATE INDEX idx_entities_faction ON game_entities(owning_faction);
        CREATE INDEX idx_docking_spaceship ON docking_status(spaceship_id);
        CREATE INDEX idx_docking_target ON docking_status(target_id);
        CREATE INDEX idx_relations_from ON faction_relations(from_faction_id);
        CREATE INDEX idx_relations_to ON faction_relations(to_faction_id);
        "
    ))?;

    let setup_duration = setup_start.elapsed();
    println!("  Setup time: {:?}", setup_duration);

    // The actual query from the article:
    // Find all spaceships docked to a planet owned by a different friendly faction
    let query = "
        SELECT DISTINCT ds.spaceship_id
        FROM docking_status ds
        JOIN game_entities target ON ds.target_id = target.entity_id
        JOIN game_entities spaceship ON ds.spaceship_id = spaceship.entity_id
        JOIN faction_relations fr ON spaceship.owning_faction = fr.from_faction_id
        WHERE target.entity_type = 3  -- Planet
          AND target.owning_faction = fr.to_faction_id
    ";

    // Warm up
    let _ = conn.execute(query, [])?;

    // Benchmark the query
    let query_iterations = 100;
    let start = Instant::now();

    for _ in 0..query_iterations {
        let mut stmt = conn.prepare(query)?;
        let _results: Vec<i64> = stmt.query_map([], |row| row.get(0))?.filter_map(|r| r.ok()).collect();
    }

    let duration = start.elapsed();
    let per_query = duration.as_nanos() as f64 / query_iterations as f64;

    println!("  Query iterations: {}", query_iterations);
    println!("  Total time: {:?}", duration);
    println!("  Per query: {:.2} µs", per_query / 1000.0);
    println!();

    // Show query plan for analysis
    println!("  Query plan:");
    let mut stmt = conn.prepare(&format!("EXPLAIN {}", query))?;
    let plan: Vec<String> = stmt.query_map([], |row| {
        let s: String = row.get(1)?;
        Ok(s)
    })?.filter_map(|r| r.ok()).collect();
    for line in plan.iter().take(10) {
        println!("    {}", line);
    }
    println!();

    Ok(())
}

/// Benchmark 4: Component Addition/Removal (Schema Changes)
/// Tests adding new components to entities dynamically
fn bench_component_operations(conn: &Connection) -> Result<()> {
    println!("--- Benchmark 4: Component Operations ---");

    // Benchmark adding a new component (column)
    let start = Instant::now();

    conn.execute_batch(
        "
        ALTER TABLE entities ADD COLUMN IF NOT EXISTS new_component_a FLOAT DEFAULT 0.0;
        ALTER TABLE entities ADD COLUMN IF NOT EXISTS new_component_b INTEGER DEFAULT 0;
        ALTER TABLE entities ADD COLUMN IF NOT EXISTS new_component_c BOOLEAN DEFAULT false;
        "
    )?;

    let add_duration = start.elapsed();
    println!("  Add 3 components: {:?}", add_duration);

    // Initialize the new components with values
    let start = Instant::now();

    conn.execute_batch(
        "
        UPDATE entities SET
            new_component_a = id * 0.1,
            new_component_b = id % 100,
            new_component_c = (id % 2) = 0;
        "
    )?;

    let init_duration = start.elapsed();
    println!("  Initialize new components: {:?}", init_duration);
    println!("  Per entity: {:.2} ns", init_duration.as_nanos() as f64 / SIZE as f64);

    // Remove components
    let start = Instant::now();

    conn.execute_batch(
        "
        ALTER TABLE entities DROP COLUMN IF EXISTS new_component_a;
        ALTER TABLE entities DROP COLUMN IF EXISTS new_component_b;
        ALTER TABLE entities DROP COLUMN IF EXISTS new_component_c;
        "
    )?;

    let remove_duration = start.elapsed();
    println!("  Remove 3 components: {:?}", remove_duration);
    println!();

    Ok(())
}

/// Benchmark 5: Filtered Updates (Conditional System Processing)
/// Common ECS pattern: only update entities matching certain criteria
fn bench_filtered_updates(conn: &Connection) -> Result<()> {
    println!("--- Benchmark 5: Filtered Updates ---");

    // Reset health status for testing
    conn.execute_batch(&format!(
        "UPDATE entities SET health_status = {HEALTH_ALIVE}, health_hp = health_maxhp;"
    ))?;

    let iterations = 100;

    // Benchmark: Update only heroes
    let start = Instant::now();

    for _ in 0..iterations {
        conn.execute(&format!(
            "UPDATE entities SET health_hp = health_hp + 1 WHERE player_type = {PLAYER_TYPE_HERO};"
        ), [])?;
    }

    let hero_duration = start.elapsed();
    let hero_count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM entities WHERE player_type = {PLAYER_TYPE_HERO}"),
        [],
        |row| row.get(0)
    )?;
    println!("  Update heroes only ({} entities):", hero_count);
    println!("    Total time: {:?}", hero_duration);
    println!("    Per iteration: {:.2} µs", hero_duration.as_nanos() as f64 / iterations as f64 / 1000.0);

    // Benchmark: Update monsters with health < 50%
    let start = Instant::now();

    for _ in 0..iterations {
        conn.execute(&format!(
            "UPDATE entities SET damage_atk = damage_atk + 1
             WHERE player_type = {PLAYER_TYPE_MONSTER}
               AND health_hp < health_maxhp / 2;"
        ), [])?;
    }

    let monster_duration = start.elapsed();
    let monster_count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM entities WHERE player_type = {PLAYER_TYPE_MONSTER} AND health_hp < health_maxhp / 2"),
        [],
        |row| row.get(0)
    )?;
    println!("  Update low-health monsters ({} entities):", monster_count);
    println!("    Total time: {:?}", monster_duration);
    println!("    Per iteration: {:.2} µs", monster_duration.as_nanos() as f64 / iterations as f64 / 1000.0);

    // Benchmark: Spatial query - entities in a region
    let start = Instant::now();

    for _ in 0..iterations {
        conn.execute(
            "UPDATE entities SET sprite_char = 42
             WHERE x BETWEEN 0 AND 100
               AND y BETWEEN 0 AND 100;",
            []
        )?;
    }

    let spatial_duration = start.elapsed();
    let spatial_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM entities WHERE x BETWEEN 0 AND 100 AND y BETWEEN 0 AND 100",
        [],
        |row| row.get(0)
    )?;
    println!("  Spatial update ({} entities in region):", spatial_count);
    println!("    Total time: {:?}", spatial_duration);
    println!("    Per iteration: {:.2} µs", spatial_duration.as_nanos() as f64 / iterations as f64 / 1000.0);
    println!();

    Ok(())
}
