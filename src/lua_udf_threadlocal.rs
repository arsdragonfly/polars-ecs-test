//! Thread-Local Lua VM for DuckDB UDFs
//! 
//! This POC demonstrates using Piccolo Lua VMs in thread-local storage
//! to implement DuckDB scalar functions that call Lua code.
//!
//! The idea:
//! 1. Each DuckDB worker thread gets its own Piccolo Lua VM
//! 2. Lua scripts define functions like `distance(x1, y1, x2, y2)`
//! 3. DuckDB VScalar calls into the thread-local Lua VM
//! 4. Combine SQL power (JOINs, filters) with Lua flexibility (spatial logic)

use piccolo::{Closure, Executor, FromValue, Function, Lua, StashedExecutor};
use std::cell::RefCell;
use std::time::Instant;

// Thread-local Lua VM storage
// Each DuckDB worker thread would have its own Piccolo instance
thread_local! {
    static LUA_VM: RefCell<Option<LuaState>> = RefCell::new(None);
}

/// Wrapper for Lua state that includes a reusable executor
struct LuaState {
    lua: Lua,
    executor: StashedExecutor,
}

/// Initialize the thread-local Lua VM with a script
fn init_lua_vm(script: &str) {
    LUA_VM.with(|vm| {
        let mut vm_ref = vm.borrow_mut();
        if vm_ref.is_none() {
            let mut lua = Lua::full();
            
            // Load and execute the script to define global functions
            let executor = lua.try_enter(|ctx| {
                let closure = Closure::load(ctx, None, script.as_bytes())
                    .expect("Failed to compile Lua script");
                Ok(ctx.stash(Executor::start(ctx, closure.into(), ())))
            }).expect("Failed to create executor");
            
            // Run the script to completion (defines functions in globals)
            lua.execute::<()>(&executor).expect("Failed to execute Lua script");
            
            *vm_ref = Some(LuaState { lua, executor });
        }
    });
}

/// Call a Lua function with f64 arguments, return f64
fn call_lua_f64(func_name: &'static str, args: &[f64]) -> f64 {
    LUA_VM.with(|vm| {
        let mut vm_ref = vm.borrow_mut();
        let state = vm_ref.as_mut().expect("Lua VM not initialized");
        
        // Get function and call it
        state.lua.try_enter(|ctx| {
            // Get the function from globals
            let globals = ctx.globals();
            let func_value = globals.get(ctx, func_name);
            let func: Function = Function::from_value(ctx, func_value)
                .map_err(|e| piccolo::Error::from(e))?;
            
            // Restart executor with the function and arguments
            let executor = ctx.fetch(&state.executor);
            match args.len() {
                1 => executor.restart(ctx, func, (args[0],)),
                2 => executor.restart(ctx, func, (args[0], args[1])),
                4 => executor.restart(ctx, func, (args[0], args[1], args[2], args[3])),
                5 => executor.restart(ctx, func, (args[0], args[1], args[2], args[3], args[4])),
                _ => panic!("Unsupported arg count: {}", args.len()),
            }
            Ok(())
        }).expect("Failed to set up function call");
        
        // Execute and get result
        state.lua.execute::<f64>(&state.executor)
            .expect("Lua function call failed")
    })
}

/// Simple Rust implementation for comparison
fn rust_distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    ((x2 - x1).powi(2) + (y2 - y1).powi(2)).sqrt()
}

fn main() {
    println!("=== Thread-Local Lua VM for DuckDB UDFs ===\n");
    
    // The Lua script that mods would provide
    let lua_script = r#"
        function distance(x1, y1, x2, y2)
            local dx = x2 - x1
            local dy = y2 - y1
            return math.sqrt(dx*dx + dy*dy)
        end
        
        function in_range(x1, y1, x2, y2, range)
            local dx = x2 - x1
            local dy = y2 - y1
            return dx*dx + dy*dy <= range*range
        end
        
        function damage_falloff(distance, max_range)
            if distance >= max_range then
                return 0
            end
            return 1 - (distance / max_range)
        end
    "#;
    
    // Initialize the Lua VM
    println!("Initializing Lua VM...");
    let start = Instant::now();
    init_lua_vm(lua_script);
    println!("  Init time: {:?}\n", start.elapsed());
    
    // === Benchmark: Lua distance vs Rust distance ===
    println!("=== Benchmark: Lua vs Rust ===\n");
    
    let iterations = 10_000;
    
    // Warm up
    for _ in 0..100 {
        let _ = call_lua_f64("distance", &[0.0, 0.0, 3.0, 4.0]);
    }
    
    // Lua distance
    let start = Instant::now();
    let mut lua_sum = 0.0;
    for i in 0..iterations {
        let x1 = (i % 100) as f64;
        let y1 = (i / 100) as f64;
        lua_sum += call_lua_f64("distance", &[x1, y1, 500.0, 500.0]);
    }
    let lua_time = start.elapsed();
    
    // Rust distance
    let start = Instant::now();
    let mut rust_sum = 0.0;
    for i in 0..iterations {
        let x1 = (i % 100) as f64;
        let y1 = (i / 100) as f64;
        rust_sum += rust_distance(x1, y1, 500.0, 500.0);
    }
    let rust_time = start.elapsed();
    
    let lua_per_call = lua_time.as_nanos() as f64 / iterations as f64;
    let rust_per_call = rust_time.as_nanos() as f64 / iterations as f64;
    
    println!("  {} iterations of distance()", iterations);
    println!("  Lua:  {:>8.2} µs total, {:>6.1} ns/call", lua_time.as_secs_f64() * 1_000_000.0, lua_per_call);
    println!("  Rust: {:>8.2} µs total, {:>6.1} ns/call", rust_time.as_secs_f64() * 1_000_000.0, rust_per_call);
    println!("  Overhead: {:>6.1}× slower", lua_per_call / rust_per_call);
    println!("  Results match: {}", (lua_sum - rust_sum).abs() < 0.001);
    println!();
    
    // === Simulate what DuckDB would do ===
    println!("=== Simulated DuckDB Query with Lua UDF ===\n");
    
    // Simulate: SELECT * FROM e1, e2 WHERE lua_distance(e1.x, e1.y, e2.x, e2.y) < 50
    // For 100 entities, that's 10,000 distance calculations
    
    let entity_count = 100;
    let total_pairs = entity_count * entity_count;
    
    // Generate random entity positions
    let positions: Vec<(f64, f64)> = (0..entity_count)
        .map(|i| ((i * 17 % 1000) as f64, (i * 31 % 1000) as f64))
        .collect();
    
    // Lua UDF version
    let start = Instant::now();
    let mut lua_matches = 0;
    for (x1, y1) in &positions {
        for (x2, y2) in &positions {
            let dist = call_lua_f64("distance", &[*x1, *y1, *x2, *y2]);
            if dist < 50.0 {
                lua_matches += 1;
            }
        }
    }
    let lua_query_time = start.elapsed();
    
    // Pure Rust version
    let start = Instant::now();
    let mut rust_matches = 0;
    for (x1, y1) in &positions {
        for (x2, y2) in &positions {
            let dist = rust_distance(*x1, *y1, *x2, *y2);
            if dist < 50.0 {
                rust_matches += 1;
            }
        }
    }
    let rust_query_time = start.elapsed();
    
    println!("  Simulated cross-join: {} × {} = {} pairs", entity_count, entity_count, total_pairs);
    println!("  Matches found: {} (Lua) vs {} (Rust)", lua_matches, rust_matches);
    println!();
    println!("  Lua UDF:   {:>6.2} ms ({:.1} µs/call)", 
        lua_query_time.as_secs_f64() * 1000.0,
        lua_query_time.as_secs_f64() * 1_000_000.0 / total_pairs as f64);
    println!("  Pure Rust: {:>6.2} ms ({:.1} µs/call)", 
        rust_query_time.as_secs_f64() * 1000.0,
        rust_query_time.as_secs_f64() * 1_000_000.0 / total_pairs as f64);
    println!("  Overhead:  {:>6.1}×", lua_query_time.as_secs_f64() / rust_query_time.as_secs_f64());
    println!();
    
    // === Frame budget analysis ===
    println!("=== Frame Budget Analysis ===\n");
    
    let tick_budget_ms = 16.67;
    let lua_pct = (lua_query_time.as_secs_f64() * 1000.0 / tick_budget_ms) * 100.0;
    
    println!("  100×100 cross-join with Lua UDF:");
    println!("    Time: {:.2} ms", lua_query_time.as_secs_f64() * 1000.0);
    println!("    Frame budget: {:.1}%", lua_pct);
    println!();
    
    // Extrapolate
    let calls_per_frame = (tick_budget_ms * 0.1) / (lua_per_call / 1_000_000.0); // 10% budget
    println!("  At 10% frame budget, can do:");
    println!("    ~{:.0} Lua UDF calls per tick", calls_per_frame);
    println!("    ~{:.0}×{:.0} entity cross-join", calls_per_frame.sqrt(), calls_per_frame.sqrt());
    println!();
    
    // === Recommendations ===
    println!("=== VERDICT ===\n");
    
    if lua_per_call < 10_000.0 {  // < 10µs
        println!("  ✅ Thread-local Lua UDFs are VIABLE for:");
        println!("     • Small cross-joins (<100×100 entities)");
        println!("     • Complex business logic in WHERE clauses");
        println!("     • Mod-defined scoring/ranking functions");
        println!();
        println!("  ⚠️  NOT recommended for:");
        println!("     • Large cross-joins (>500×500)");
        println!("     • Simple math that Rust can inline");
        println!("     • Per-frame spatial queries (use HashMap)");
    } else {
        println!("  ❌ Lua UDF overhead too high: {:.1} µs/call", lua_per_call / 1000.0);
        println!("     Consider: mlua (LuaJIT) or keep UDFs in Rust");
    }
    
    println!();
    println!("=== Architecture ===");
    println!();
    println!("  ┌─────────────────────────────────────────────────────────┐");
    println!("  │                    DuckDB Query                         │");
    println!("  │  SELECT e1.id, e2.id                                   │");
    println!("  │  FROM entities e1, entities e2                          │");
    println!("  │  WHERE lua_distance(e1.x, e1.y, e2.x, e2.y) < 50       │");
    println!("  └───────────────────────┬─────────────────────────────────┘");
    println!("                          │");
    println!("                          ▼");
    println!("  ┌─────────────────────────────────────────────────────────┐");
    println!("  │              Rust VScalar (thread-safe)                 │");
    println!("  │  • Receives DataChunk from DuckDB                       │");
    println!("  │  • For each row: call thread-local Lua                  │");
    println!("  │  • Write results to output vector                       │");
    println!("  └───────────────────────┬─────────────────────────────────┘");
    println!("                          │");
    println!("                          ▼");
    println!("  ┌─────────────────────────────────────────────────────────┐");
    println!("  │            Thread-Local Piccolo VM                      │");
    println!("  │  thread_local! {{ static LUA: RefCell<Lua> }}           │");
    println!("  │  • Each worker thread has own VM                        │");
    println!("  │  • Pre-loaded with mod scripts                          │");
    println!("  │  • ~{:.0} ns per function call                            │", lua_per_call);
    println!("  └─────────────────────────────────────────────────────────┘");
}
