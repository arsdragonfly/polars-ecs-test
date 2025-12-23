//! Why Rust HashMap is Fast: Spatial Hashing Explained
//!
//! The key insight: We DON'T compare every entity to every other entity.
//! Instead, we use a grid to only check nearby cells.

use std::collections::HashMap;
use std::time::Instant;

fn main() {
    println!("=== Why Spatial Hashing is O(N) not O(N²) ===\n");

    // Simulate entity positions
    let num_entities = 100_000;
    let map_size = 1000;
    let attack_range = 50;
    let cell_size = attack_range; // Cell size = attack range

    println!("Setup:");
    println!("  Entities: {}", num_entities);
    println!("  Map size: {}x{}", map_size, map_size);
    println!("  Attack range: {}", attack_range);
    println!("  Cell size: {}\n", cell_size);

    // Generate random entities
    let mut entities: Vec<(i32, i32, bool)> = Vec::with_capacity(num_entities);
    for i in 0..num_entities {
        let x = ((i as i64 * 17 + 31) % map_size as i64) as i32;
        let y = ((i as i64 * 23 + 47) % map_size as i64) as i32;
        let is_enemy = i % 3 == 0; // 33% enemies
        entities.push((x, y, is_enemy));
    }

    // =========================================================================
    // Approach 1: Naive O(N²) - check every pair
    // =========================================================================
    println!("--- Approach 1: Naive O(N²) ---");
    println!("  For each entity, check ALL other entities\n");

    // Only do this for small N to avoid waiting forever
    let small_n = 5_000;
    let small_entities: Vec<_> = entities.iter().take(small_n).cloned().collect();

    let start = Instant::now();
    let mut naive_comparisons = 0u64;
    let mut naive_matches = 0;
    
    for (i, &(x1, y1, is_enemy1)) in small_entities.iter().enumerate() {
        if is_enemy1 { continue; } // Only friendlies attack
        
        let mut best: Option<(usize, i32)> = None;
        for (j, &(x2, y2, is_enemy2)) in small_entities.iter().enumerate() {
            if i == j || !is_enemy2 { continue; }
            naive_comparisons += 1;
            
            let dist_sq = (x1 - x2).pow(2) + (y1 - y2).pow(2);
            if dist_sq <= attack_range * attack_range {
                if best.is_none() || dist_sq < best.unwrap().1 {
                    best = Some((j, dist_sq));
                }
            }
        }
        if best.is_some() { naive_matches += 1; }
    }
    let naive_time = start.elapsed();

    println!("  N = {}", small_n);
    println!("  Comparisons: {} ({:.1}M)", naive_comparisons, naive_comparisons as f64 / 1_000_000.0);
    println!("  Expected (N²): {} ({:.1}M)", small_n as u64 * small_n as u64, (small_n as f64).powi(2) / 1_000_000.0);
    println!("  Time: {:.2} ms", naive_time.as_secs_f64() * 1000.0);
    println!("  Matches: {}\n", naive_matches);

    // =========================================================================
    // Approach 2: Spatial Hashing O(N)
    // =========================================================================
    println!("--- Approach 2: Spatial Hashing O(N) ---");
    println!("  Grid cells of size {}x{}", cell_size, cell_size);
    println!("  Only check 3x3 = 9 neighboring cells\n");

    // Build spatial hash grid
    let start = Instant::now();
    let mut grid: HashMap<(i32, i32), Vec<usize>> = HashMap::new();
    
    for (idx, &(x, y, _)) in entities.iter().enumerate() {
        let cell = (x / cell_size, y / cell_size);
        grid.entry(cell).or_default().push(idx);
    }
    let grid_build_time = start.elapsed();

    // Analyze grid distribution
    let total_cells = grid.len();
    let entities_per_cell: f64 = num_entities as f64 / total_cells as f64;
    let max_in_cell = grid.values().map(|v| v.len()).max().unwrap_or(0);
    
    println!("  Grid stats:");
    println!("    Total cells used: {}", total_cells);
    println!("    Avg entities/cell: {:.1}", entities_per_cell);
    println!("    Max entities/cell: {}", max_in_cell);
    println!("    Grid build time: {:.2} ms\n", grid_build_time.as_secs_f64() * 1000.0);

    // Find nearest enemies using grid
    let start = Instant::now();
    let mut grid_comparisons = 0u64;
    let mut grid_matches = 0;

    for (idx, &(x, y, is_enemy)) in entities.iter().enumerate() {
        if is_enemy { continue; }
        
        let cell = (x / cell_size, y / cell_size);
        let mut best: Option<(usize, i32)> = None;
        
        // Only check 3x3 neighboring cells!
        for dx in -1..=1 {
            for dy in -1..=1 {
                if let Some(cell_entities) = grid.get(&(cell.0 + dx, cell.1 + dy)) {
                    for &other_idx in cell_entities {
                        if other_idx == idx { continue; }
                        let (ox, oy, other_is_enemy) = entities[other_idx];
                        if !other_is_enemy { continue; }
                        
                        grid_comparisons += 1;
                        let dist_sq = (x - ox).pow(2) + (y - oy).pow(2);
                        if dist_sq <= attack_range * attack_range {
                            if best.is_none() || dist_sq < best.unwrap().1 {
                                best = Some((other_idx, dist_sq));
                            }
                        }
                    }
                }
            }
        }
        if best.is_some() { grid_matches += 1; }
    }
    let grid_time = start.elapsed();

    println!("  N = {}", num_entities);
    println!("  Comparisons: {} ({:.1}M)", grid_comparisons, grid_comparisons as f64 / 1_000_000.0);
    println!("  Time: {:.2} ms", grid_time.as_secs_f64() * 1000.0);
    println!("  Matches: {}\n", grid_matches);

    // =========================================================================
    // Analysis
    // =========================================================================
    println!("--- Analysis ---\n");

    // Expected comparisons for spatial hashing
    let friendlies = entities.iter().filter(|e| !e.2).count();
    let avg_neighbors = entities_per_cell * 9.0 * 0.33; // 9 cells, 33% enemies
    let expected_comparisons = friendlies as f64 * avg_neighbors;

    println!("  Naive O(N²):");
    println!("    Comparisons ≈ N² = {}M", (num_entities as f64).powi(2) / 1_000_000.0);
    println!("    Time for {}K entities: ~{:.0} ms (estimated)", 
             num_entities / 1000, 
             naive_time.as_secs_f64() * 1000.0 * (num_entities as f64 / small_n as f64).powi(2));

    println!("\n  Spatial Hash O(N):");
    println!("    Comparisons ≈ N × K where K = avg neighbors per cell");
    println!("    K ≈ {} entities/cell × 9 cells × 33% enemies = {:.1}", 
             entities_per_cell as i32, avg_neighbors);
    println!("    Expected: {:.1}M, Actual: {:.1}M", 
             expected_comparisons / 1_000_000.0, 
             grid_comparisons as f64 / 1_000_000.0);
    println!("    Time for {}K entities: {:.2} ms", num_entities / 1000, grid_time.as_secs_f64() * 1000.0);

    println!("\n  Speedup: {:.0}x fewer comparisons", 
             (num_entities as f64).powi(2) / grid_comparisons as f64);

    println!("\n--- Why This Works ---");
    println!("  1. Divide world into cells (cell_size ≥ attack_range)");
    println!("  2. Entities only interact with nearby entities");
    println!("  3. Check 9 cells (3×3) instead of all N entities");
    println!("  4. With uniform distribution: O(N × K) where K is constant");
    println!("  5. K ≈ (entities_per_cell × 9) ≈ constant");
    println!("\n  The HashMap gives O(1) lookup per cell, making total O(N)");
}
