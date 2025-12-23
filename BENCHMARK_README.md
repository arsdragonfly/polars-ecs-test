# DuckDB ECS Microbenchmarks

This project benchmarks DuckDB as a potential Entity Component System (ECS) for data-intensive games, inspired by the article ["DataFrames might be an underrated Entity Component System for game development"](https://medium.com/@arsdragonfly/dataframes-might-be-an-underrated-entity-component-system-for-game-development-dfb72b1819fe).

## Benchmark Scenarios

### 1. Entity Creation
Creates 2M entities with 7 components (position, velocity, data, player, health, damage, sprite) - matching the Polars benchmark structure.

### 2. Simple System Updates (1000 iterations)
Combined movement and data system updates per frame:
- Movement: `x += vx * dt`, `y += vy * dt`
- Data: counter increment, float accumulation, boolean toggle, PRNG

### 3. Complex Relational Query
The spaceship/faction query from the article - finds all spaceships docked to planets owned by a faction allied with the spaceship's faction. Tests join performance across multiple tables.

### 4. Component Operations
Tests schema modification overhead:
- Adding new columns (components)
- Initializing component values
- Removing columns

### 5. Filtered Updates
Common ECS patterns:
- Update only heroes (~30% of entities)
- Update monsters with low health (conditional)
- Spatial queries (entities in a region)

## Running the Benchmarks

### Quick Benchmark (manual timing)
```bash
cargo run --release --bin duckdb_bench
```

### Rigorous Benchmarks (Criterion)
```bash
cargo bench
```

## Results Summary

With 2M entities on a typical system:

| Benchmark | DuckDB Performance |
|-----------|-------------------|
| Entity Creation | ~295 ns/entity |
| System Update (2 systems) | ~21 ns/entity/iteration |
| Complex Join Query | ~2.6 ms/query |
| Add Component | ~37 ms (3 columns) |
| Initialize Components | ~16 ns/entity |
| Filtered Update (30%) | ~4.2 ms/iteration |

## Comparison with Polars

From the original article, Polars achieved ~9 ns/entity for 2 systems over 7 components. DuckDB shows:
- **~21 ns/entity** for combined movement + data systems
- This is roughly **2.3x slower** than the reported Polars performance

## Analysis

### Strengths of DuckDB for ECS:
1. **SQL Query Optimizer**: Complex relational queries (like the spaceship example) benefit from automatic query optimization
2. **Schema Flexibility**: ALTER TABLE operations are fast for adding/removing components
3. **Bulk Operations**: Good performance for batch entity creation/deletion
4. **Indexing**: Can create indexes for common query patterns
5. **Persistence**: Easy save/load via database files

### Weaknesses:
1. **Update Overhead**: Row-wise updates have higher overhead than columnar operations
2. **No SIMD for Updates**: Unlike Polars/Arrow, UPDATE operations don't benefit from vectorization
3. **Transaction Overhead**: Each UPDATE is a transaction
4. **Memory Copy**: Updates often require copying data

### Recommendations:
- DuckDB is viable for ECS if your game is **query-heavy** with complex relationships
- For **update-heavy** games (many systems running each frame), Polars or traditional ECS may be better
- Consider using DuckDB for **infrequent batch operations** (AI decisions, world queries) while using a faster ECS for per-frame updates
- The ~21 ns/entity is still fast enough for many games (48M entities/second for one system)

## Future Improvements

1. Test with DuckDB's Appender API for bulk inserts
2. Compare with prepared statements for repeated queries
3. Test with larger component counts
4. Benchmark entity deletion patterns
5. Test with indexes on commonly queried columns
