//! DuckDB Simulation Game Benchmark
//!
//! Tests DuckDB for OpenTTD/Factorio-style games:
//! - Logistics networks (belt items, train routing)
//! - Factory production chains
//! - Spatial queries (tile lookups)
//! - Entity ticking at 60 UPS

use duckdb::{Connection, Result};
use std::time::Instant;

fn main() -> Result<()> {
    println!("=== DuckDB for Simulation Games (OpenTTD/Factorio style) ===\n");

    let conn = Connection::open_in_memory()?;
    conn.execute_batch("SET threads TO 8;")?;

    // Factorio-scale: ~100K-500K active entities is common
    // OpenTTD: ~10K-50K vehicles, 100K+ cargo packets

    bench_conveyor_belt_simulation(&conn)?;
    bench_train_network(&conn)?;
    bench_factory_production(&conn)?;
    bench_spatial_queries(&conn)?;
    bench_60ups_feasibility(&conn)?;

    Ok(())
}

/// Conveyor belt simulation - items moving along belts
/// Factorio has millions of items on belts
fn bench_conveyor_belt_simulation(conn: &Connection) -> Result<()> {
    println!("--- Conveyor Belt Simulation ---");

    let item_count = 500_000; // 500K items on belts

    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS belt_items;
        CREATE TABLE belt_items AS
        SELECT
            i AS item_id,
            (random() * 1000)::INTEGER AS belt_id,
            (random() * 100)::FLOAT AS position,  -- position on belt segment
            (i % 50) AS item_type,
            8.0::FLOAT AS speed  -- tiles per second
        FROM generate_series(1, {item_count}) AS t(i);

        CREATE INDEX idx_belt ON belt_items(belt_id);
        "
    ))?;

    // Simulate one tick: move all items
    let dt = 1.0 / 60.0; // 60 UPS
    let iterations = 60; // 1 second of game time

    let start = Instant::now();
    for _ in 0..iterations {
        conn.execute_batch(&format!(
            "UPDATE belt_items SET position = position + speed * {dt};"
        ))?;
    }
    let duration = start.elapsed();

    let per_tick = duration.as_micros() as f64 / iterations as f64;
    let per_item_per_tick = per_tick * 1000.0 / item_count as f64;

    println!("  Items: {}", item_count);
    println!("  60 ticks: {:?}", duration);
    println!("  Per tick: {:.2} µs ({:.2} ns/item)", per_tick, per_item_per_tick);
    println!("  Budget (16.67ms): {:.1}% used", per_tick / 16670.0 * 100.0);
    println!();

    Ok(())
}

/// Train network - pathfinding queries, cargo routing
fn bench_train_network(conn: &Connection) -> Result<()> {
    println!("--- Train Network Simulation ---");

    let train_count = 5_000;
    let station_count = 500;
    let track_segments = 10_000;

    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS trains;
        DROP TABLE IF EXISTS stations;
        DROP TABLE IF EXISTS cargo;

        CREATE TABLE trains AS
        SELECT
            i AS train_id,
            (random() * {track_segments})::INTEGER AS current_segment,
            (random() * {station_count})::INTEGER AS destination_station,
            (50 + random() * 100)::FLOAT AS speed,
            0.0::FLOAT AS position_on_segment
        FROM generate_series(1, {train_count}) AS t(i);

        CREATE TABLE stations AS
        SELECT
            i AS station_id,
            'Station_' || i AS name,
            (random() * 1000)::INTEGER AS x,
            (random() * 1000)::INTEGER AS y,
            (i % 10) AS accepted_cargo_type
        FROM generate_series(1, {station_count}) AS t(i);

        CREATE TABLE cargo AS
        SELECT
            i AS cargo_id,
            (random() * {train_count})::INTEGER AS train_id,
            (random() * {station_count})::INTEGER AS origin_station,
            (random() * {station_count})::INTEGER AS dest_station,
            (i % 20) AS cargo_type,
            (1 + random() * 100)::INTEGER AS quantity
        FROM generate_series(1, 50000) AS t(i);

        CREATE INDEX idx_cargo_train ON cargo(train_id);
        CREATE INDEX idx_cargo_dest ON cargo(dest_station);
        "
    ))?;

    // Tick: Update train positions
    let start = Instant::now();
    for _ in 0..60 {
        conn.execute_batch(
            "UPDATE trains SET position_on_segment = position_on_segment + speed / 60.0;"
        )?;
    }
    let move_time = start.elapsed();

    // Complex query: Find cargo that should be delivered
    let start = Instant::now();
    for _ in 0..60 {
        let _: i64 = conn.query_row(
            "SELECT COUNT(*) FROM cargo c
             JOIN trains t ON c.train_id = t.train_id
             JOIN stations s ON c.dest_station = s.station_id
             WHERE t.destination_station = c.dest_station",
            [],
            |row| row.get(0)
        )?;
    }
    let query_time = start.elapsed();

    println!("  Trains: {}, Stations: {}, Cargo packets: 50000", train_count, station_count);
    println!("  60 tick movement: {:?} ({:.2} µs/tick)", move_time, move_time.as_micros() as f64 / 60.0);
    println!("  60 delivery queries: {:?} ({:.2} µs/query)", query_time, query_time.as_micros() as f64 / 60.0);
    println!();

    Ok(())
}

/// Factory production chains - assemblers consuming/producing items
fn bench_factory_production(conn: &Connection) -> Result<()> {
    println!("--- Factory Production Chains ---");

    let machine_count = 50_000; // 50K assemblers/furnaces

    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS machines;
        DROP TABLE IF EXISTS inventories;
        DROP TABLE IF EXISTS recipes;

        CREATE TABLE machines AS
        SELECT
            i AS machine_id,
            (i % 20) AS recipe_id,
            0.0::FLOAT AS progress,  -- 0.0 to 1.0 crafting progress
            1.0::FLOAT AS speed_multiplier,
            true AS has_power
        FROM generate_series(1, {machine_count}) AS t(i);

        -- Each machine has input/output inventory slots
        CREATE TABLE inventories AS
        SELECT
            i AS machine_id,
            (i % 20) AS item_type,
            (random() * 100)::INTEGER AS quantity,
            CASE WHEN random() < 0.5 THEN 'input' ELSE 'output' END AS slot_type
        FROM generate_series(1, {machine_count}) AS t(i);

        CREATE TABLE recipes AS
        SELECT
            i AS recipe_id,
            2.0::FLOAT AS craft_time,  -- seconds to craft
            i AS output_item,
            (i + 10) AS input_item_1,
            (i + 20) AS input_item_2
        FROM generate_series(0, 19) AS t(i);

        CREATE INDEX idx_machine_recipe ON machines(recipe_id);
        CREATE INDEX idx_inv_machine ON inventories(machine_id);
        "
    ))?;

    let dt = 1.0 / 60.0;

    // Tick: Advance crafting progress for all machines
    let start = Instant::now();
    for _ in 0..60 {
        // Update progress
        conn.execute_batch(&format!(
            "UPDATE machines 
             SET progress = progress + (speed_multiplier * {dt} / 2.0)
             WHERE has_power = true;"
        ))?;

        // Complete crafting (progress >= 1.0)
        conn.execute_batch(
            "UPDATE machines SET progress = progress - 1.0 WHERE progress >= 1.0;"
        )?;
    }
    let update_time = start.elapsed();

    // Query: Find machines that need input items
    let start = Instant::now();
    for _ in 0..60 {
        let _: i64 = conn.query_row(
            "SELECT COUNT(*) FROM machines m
             JOIN inventories i ON m.machine_id = i.machine_id
             WHERE i.slot_type = 'input' AND i.quantity < 5",
            [],
            |row| row.get(0)
        )?;
    }
    let query_time = start.elapsed();

    println!("  Machines: {}", machine_count);
    println!("  60 tick updates: {:?} ({:.2} µs/tick)", update_time, update_time.as_micros() as f64 / 60.0);
    println!("  60 starving queries: {:?} ({:.2} µs/query)", query_time, query_time.as_micros() as f64 / 60.0);
    println!();

    Ok(())
}

/// Spatial queries - "what's at tile X,Y?"
fn bench_spatial_queries(conn: &Connection) -> Result<()> {
    println!("--- Spatial Queries (Tile Lookups) ---");

    let entity_count = 100_000;
    let map_size = 1000; // 1000x1000 tile map

    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS map_entities;
        CREATE TABLE map_entities AS
        SELECT
            i AS entity_id,
            (random() * {map_size})::INTEGER AS tile_x,
            (random() * {map_size})::INTEGER AS tile_y,
            (i % 10) AS entity_type,
            'Entity_' || i AS name
        FROM generate_series(1, {entity_count}) AS t(i);

        -- Spatial index simulation via compound index
        CREATE INDEX idx_tile ON map_entities(tile_x, tile_y);
        "
    ))?;

    // Single tile lookup (common operation)
    let start = Instant::now();
    for i in 0..10000 {
        let x = i % map_size;
        let y = (i * 7) % map_size;
        let _: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM map_entities WHERE tile_x = {} AND tile_y = {}", x, y),
            [],
            |row| row.get(0)
        )?;
    }
    let point_query_time = start.elapsed();

    // Area query (entities in a 10x10 region)
    let start = Instant::now();
    for i in 0..1000 {
        let x = (i * 13) % (map_size - 10);
        let y = (i * 17) % (map_size - 10);
        let _: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM map_entities 
                 WHERE tile_x BETWEEN {} AND {} AND tile_y BETWEEN {} AND {}", 
                x, x + 10, y, y + 10
            ),
            [],
            |row| row.get(0)
        )?;
    }
    let area_query_time = start.elapsed();

    // Nearby entities (common for collision, targeting)
    let start = Instant::now();
    for i in 0..1000 {
        let x: i32 = (i * 31) % map_size;
        let y: i32 = (i * 37) % map_size;
        let _: i64 = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM map_entities 
                 WHERE tile_x BETWEEN {} AND {} AND tile_y BETWEEN {} AND {}
                 AND entity_type = 5", 
                x.saturating_sub(5), x + 5, y.saturating_sub(5), y + 5
            ),
            [],
            |row| row.get(0)
        )?;
    }
    let nearby_query_time = start.elapsed();

    println!("  Entities: {}, Map: {}x{}", entity_count, map_size, map_size);
    println!("  10K point queries: {:?} ({:.2} µs/query)", point_query_time, point_query_time.as_micros() as f64 / 10000.0);
    println!("  1K area queries (10x10): {:?} ({:.2} µs/query)", area_query_time, area_query_time.as_micros() as f64 / 1000.0);
    println!("  1K nearby+filter queries: {:?} ({:.2} µs/query)", nearby_query_time, nearby_query_time.as_micros() as f64 / 1000.0);
    println!();

    Ok(())
}

/// Can we hit 60 UPS with a realistic workload?
fn bench_60ups_feasibility(conn: &Connection) -> Result<()> {
    println!("--- 60 UPS Feasibility Test ---");
    println!("  Budget per tick: 16.67 ms\n");

    // Realistic Factorio-lite scenario
    let belt_items = 200_000;
    let machines = 20_000;
    let trains = 1_000;
    let map_entities = 50_000;

    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS sim_belts;
        DROP TABLE IF EXISTS sim_machines;
        DROP TABLE IF EXISTS sim_trains;
        DROP TABLE IF EXISTS sim_map;

        CREATE TABLE sim_belts AS
        SELECT i AS id, random()::FLOAT * 100 AS pos, 8.0::FLOAT AS speed
        FROM generate_series(1, {belt_items}) AS t(i);

        CREATE TABLE sim_machines AS
        SELECT i AS id, random()::FLOAT AS progress, true AS active
        FROM generate_series(1, {machines}) AS t(i);

        CREATE TABLE sim_trains AS
        SELECT i AS id, random()::FLOAT * 1000 AS pos, 50.0::FLOAT AS speed
        FROM generate_series(1, {trains}) AS t(i);

        CREATE TABLE sim_map AS
        SELECT i AS id, (random() * 500)::INT AS x, (random() * 500)::INT AS y
        FROM generate_series(1, {map_entities}) AS t(i);
        CREATE INDEX idx_map_xy ON sim_map(x, y);
        "
    ))?;

    println!("  Scenario: {} belt items, {} machines, {} trains, {} map entities",
             belt_items, machines, trains, map_entities);

    let dt = 1.0 / 60.0;

    // Simulate 60 ticks (1 second of game time)
    let start = Instant::now();

    for _ in 0..60 {
        // Belt system
        conn.execute_batch(&format!(
            "UPDATE sim_belts SET pos = pos + speed * {dt};"
        ))?;

        // Machine system
        conn.execute_batch(&format!(
            "UPDATE sim_machines SET progress = progress + {dt} / 2.0 WHERE active;"
        ))?;
        conn.execute_batch(
            "UPDATE sim_machines SET progress = progress - 1.0 WHERE progress >= 1.0;"
        )?;

        // Train system
        conn.execute_batch(&format!(
            "UPDATE sim_trains SET pos = pos + speed * {dt};"
        ))?;

        // A few spatial queries per tick (collision checks, etc)
        for _ in 0..10 {
            let _: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sim_map WHERE x BETWEEN 100 AND 110 AND y BETWEEN 100 AND 110",
                [],
                |row| row.get(0)
            )?;
        }
    }

    let total_time = start.elapsed();
    let per_tick = total_time.as_micros() as f64 / 60.0;
    let budget_used = per_tick / 16670.0 * 100.0;

    println!("\n  Results:");
    println!("  60 ticks total: {:?}", total_time);
    println!("  Per tick: {:.2} µs", per_tick);
    println!("  Budget used: {:.1}%", budget_used);

    if budget_used < 100.0 {
        println!("\n  ✅ 60 UPS is ACHIEVABLE with this workload!");
        println!("  Headroom: {:.1}% remaining for game logic, rendering, etc.", 100.0 - budget_used);
    } else {
        println!("\n  ❌ 60 UPS NOT achievable - would get {:.1} UPS", 60.0 * 100.0 / budget_used);
    }

    // What about larger scale?
    println!("\n  Scaling estimates (linear extrapolation):");
    let base_entities = belt_items + machines + trains;
    for scale in [2, 5, 10] {
        let estimated_tick = per_tick * scale as f64;
        let estimated_ups = 1_000_000.0 / estimated_tick;
        println!("    {}x entities ({}): {:.0} UPS", 
                 scale, base_entities * scale, estimated_ups.min(60.0));
    }

    println!();
    Ok(())
}
