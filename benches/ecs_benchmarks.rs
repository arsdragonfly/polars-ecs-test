//! Criterion-based ECS Benchmarks for DuckDB vs Polars
//!
//! Run with: cargo bench
//!
//! This provides statistically rigorous benchmarks comparing:
//! - DuckDB's SQL-based approach
//! - Polars' DataFrame approach
//!
//! For use as an Entity Component System in data-intensive games

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use duckdb::Connection;
use std::time::Duration;

const ENTITY_COUNTS: [usize; 4] = [1_000, 10_000, 100_000, 1_000_000];

// ============================================================================
// DuckDB Benchmarks
// ============================================================================

fn setup_duckdb_entities(conn: &Connection, size: usize) {
    conn.execute_batch(&format!(
        "
        DROP TABLE IF EXISTS entities;
        CREATE TABLE entities AS
        SELECT
            i AS id,
            0.0::FLOAT AS x,
            0.0::FLOAT AS y,
            1.0::FLOAT AS vx,
            1.0::FLOAT AS vy,
            0::INTEGER AS data_thingy,
            0.0::DOUBLE AS data_dingy,
            false::BOOLEAN AS data_mingy,
            0::INTEGER AS player_type,
            100::INTEGER AS health_hp,
            100::INTEGER AS health_maxhp
        FROM generate_series(0, {size} - 1) AS t(i);
        "
    )).unwrap();
}

fn duckdb_movement_system(conn: &Connection) {
    conn.execute_batch(
        "UPDATE entities SET x = x + vx * 0.016667, y = y + vy * 0.016667;"
    ).unwrap();
}

fn duckdb_data_system(conn: &Connection) {
    conn.execute_batch(
        "UPDATE entities SET
            data_thingy = (data_thingy + 1) % 1000000,
            data_dingy = data_dingy + 0.0000016667,
            data_mingy = NOT data_mingy;"
    ).unwrap();
}

fn duckdb_combined_systems(conn: &Connection) {
    conn.execute_batch(
        "UPDATE entities SET
            x = x + vx * 0.016667,
            y = y + vy * 0.016667,
            data_thingy = (data_thingy + 1) % 1000000,
            data_dingy = data_dingy + 0.0000016667,
            data_mingy = NOT data_mingy;"
    ).unwrap();
}

fn duckdb_filtered_update(conn: &Connection) {
    conn.execute_batch(
        "UPDATE entities SET health_hp = health_hp - 1 WHERE player_type = 2;"
    ).unwrap();
}

fn duckdb_select_query(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM entities WHERE x > 0 AND health_hp > 50",
        [],
        |row| row.get(0)
    ).unwrap()
}

// ============================================================================
// Benchmark Functions
// ============================================================================

fn bench_duckdb_entity_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("DuckDB Entity Creation");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for &size in &ENTITY_COUNTS {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("SET threads TO 4;").unwrap();

            b.iter(|| {
                setup_duckdb_entities(&conn, black_box(size));
            });
        });
    }

    group.finish();
}

fn bench_duckdb_movement_system(c: &mut Criterion) {
    let mut group = c.benchmark_group("DuckDB Movement System");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for &size in &ENTITY_COUNTS {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("SET threads TO 4;").unwrap();
            setup_duckdb_entities(&conn, size);

            b.iter(|| {
                duckdb_movement_system(&conn);
            });
        });
    }

    group.finish();
}

fn bench_duckdb_data_system(c: &mut Criterion) {
    let mut group = c.benchmark_group("DuckDB Data System");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for &size in &ENTITY_COUNTS {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("SET threads TO 4;").unwrap();
            setup_duckdb_entities(&conn, size);

            b.iter(|| {
                duckdb_data_system(&conn);
            });
        });
    }

    group.finish();
}

fn bench_duckdb_combined_systems(c: &mut Criterion) {
    let mut group = c.benchmark_group("DuckDB Combined Systems");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for &size in &ENTITY_COUNTS {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("SET threads TO 4;").unwrap();
            setup_duckdb_entities(&conn, size);

            b.iter(|| {
                duckdb_combined_systems(&conn);
            });
        });
    }

    group.finish();
}

fn bench_duckdb_filtered_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("DuckDB Filtered Update");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(5));

    for &size in &ENTITY_COUNTS {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("SET threads TO 4;").unwrap();
            setup_duckdb_entities(&conn, size);
            // Set ~30% as type 2 for filtering
            conn.execute_batch(
                "UPDATE entities SET player_type = 2 WHERE id % 3 = 0;"
            ).unwrap();

            b.iter(|| {
                duckdb_filtered_update(&conn);
            });
        });
    }

    group.finish();
}

fn bench_duckdb_select_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("DuckDB Select Query");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));

    for &size in &ENTITY_COUNTS {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("SET threads TO 4;").unwrap();
            setup_duckdb_entities(&conn, size);
            // Run some updates to create interesting data
            for _ in 0..10 {
                duckdb_movement_system(&conn);
            }

            b.iter(|| {
                black_box(duckdb_select_query(&conn));
            });
        });
    }

    group.finish();
}

fn bench_duckdb_complex_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("DuckDB Complex Join Query");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    let sizes = [1_000, 10_000, 50_000];

    for &size in &sizes {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("SET threads TO 4;").unwrap();

            // Setup relational tables (spaceship example from article)
            conn.execute_batch(&format!(
                "
                DROP TABLE IF EXISTS game_entities;
                DROP TABLE IF EXISTS docking_status;
                DROP TABLE IF EXISTS faction_relations;

                CREATE TABLE game_entities AS
                SELECT
                    i AS entity_id,
                    CASE
                        WHEN i % 4 = 0 THEN 0  -- Spaceship
                        WHEN i % 4 = 1 THEN 1  -- Faction
                        WHEN i % 4 = 2 THEN 2  -- SpaceStation
                        ELSE 3                  -- Planet
                    END AS entity_type,
                    (i % 50) AS owning_faction
                FROM generate_series(1, {size}) AS t(i);

                CREATE TABLE docking_status AS
                SELECT
                    s.entity_id AS spaceship_id,
                    p.entity_id AS target_id
                FROM game_entities s, game_entities p
                WHERE s.entity_type = 0
                  AND p.entity_type = 3
                  AND (s.entity_id + p.entity_id) % 100 = 0
                LIMIT {limit};

                CREATE TABLE faction_relations AS
                SELECT DISTINCT
                    (i % 50) AS from_faction_id,
                    ((i + 1) % 50) AS to_faction_id
                FROM generate_series(1, 100) AS t(i)
                WHERE (i % 50) != ((i + 1) % 50);

                CREATE INDEX idx_ge_type ON game_entities(entity_type);
                CREATE INDEX idx_ge_faction ON game_entities(owning_faction);
                CREATE INDEX idx_ds_ship ON docking_status(spaceship_id);
                CREATE INDEX idx_ds_target ON docking_status(target_id);
                ",
                limit = size / 10
            )).unwrap();

            let query = "
                SELECT DISTINCT ds.spaceship_id
                FROM docking_status ds
                JOIN game_entities target ON ds.target_id = target.entity_id
                JOIN game_entities spaceship ON ds.spaceship_id = spaceship.entity_id
                JOIN faction_relations fr ON spaceship.owning_faction = fr.from_faction_id
                WHERE target.entity_type = 3
                  AND target.owning_faction = fr.to_faction_id
            ";

            b.iter(|| {
                let mut stmt = conn.prepare(query).unwrap();
                let results: Vec<i64> = stmt.query_map([], |row| row.get(0))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                black_box(results);
            });
        });
    }

    group.finish();
}

fn bench_duckdb_bulk_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("DuckDB Bulk Insert");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    let sizes = [1_000, 10_000, 100_000];

    for &size in &sizes {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("SET threads TO 4;").unwrap();

            // Create base table
            conn.execute_batch(
                "DROP TABLE IF EXISTS entities;
                 CREATE TABLE entities (
                    id INTEGER PRIMARY KEY,
                    x FLOAT, y FLOAT,
                    vx FLOAT, vy FLOAT,
                    data_thingy INTEGER,
                    health_hp INTEGER
                 );"
            ).unwrap();

            b.iter(|| {
                // Simulate spawning new entities
                conn.execute_batch(&format!(
                    "INSERT INTO entities
                     SELECT
                        (SELECT COALESCE(MAX(id), 0) FROM entities) + i AS id,
                        random()::FLOAT * 100 AS x,
                        random()::FLOAT * 100 AS y,
                        (random() - 0.5)::FLOAT * 10 AS vx,
                        (random() - 0.5)::FLOAT * 10 AS vy,
                        0 AS data_thingy,
                        100 AS health_hp
                     FROM generate_series(1, {size}) AS t(i);"
                )).unwrap();

                // Clean up for next iteration
                conn.execute_batch("DELETE FROM entities;").unwrap();
            });
        });
    }

    group.finish();
}

fn bench_duckdb_bulk_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("DuckDB Bulk Delete");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    for &size in &ENTITY_COUNTS[..3] { // Skip 1M for delete benchmark
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("SET threads TO 4;").unwrap();

            b.iter(|| {
                // Create entities
                setup_duckdb_entities(&conn, size);

                // Delete ~10% of entities (dead entities)
                conn.execute_batch(
                    "DELETE FROM entities WHERE id % 10 = 0;"
                ).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_duckdb_entity_creation,
    bench_duckdb_movement_system,
    bench_duckdb_data_system,
    bench_duckdb_combined_systems,
    bench_duckdb_filtered_update,
    bench_duckdb_select_query,
    bench_duckdb_complex_join,
    bench_duckdb_bulk_insert,
    bench_duckdb_bulk_delete,
);

criterion_main!(benches);
