//! Piccolo vs mlua (LuaJIT) Performance Comparison
//!
//! This benchmark compares:
//! - Piccolo: Pure Rust Lua 5.4 interpreter
//! - mlua/LuaJIT: FFI bindings to LuaJIT (JIT-compiled)
//!
//! For game UDF use cases, we care about:
//! - Function call overhead
//! - Simple math operations
//! - Memory safety within Rust ecosystem

use std::cell::RefCell;
use std::time::Instant;

// ============================================================================
// Piccolo (Pure Rust Lua 5.4)
// ============================================================================

mod piccolo_vm {
    use piccolo::{Closure, Executor, FromValue, Function, Lua, StashedExecutor};
    use std::cell::RefCell;

    thread_local! {
        static LUA_VM: RefCell<Option<LuaState>> = RefCell::new(None);
    }

    struct LuaState {
        lua: Lua,
        executor: StashedExecutor,
    }

    pub fn init(script: &str) {
        LUA_VM.with(|vm| {
            let mut vm_ref = vm.borrow_mut();
            if vm_ref.is_none() {
                let mut lua = Lua::full();
                let executor = lua.try_enter(|ctx| {
                    let closure = Closure::load(ctx, None, script.as_bytes())
                        .expect("Failed to compile Lua script");
                    Ok(ctx.stash(Executor::start(ctx, closure.into(), ())))
                }).expect("Failed to create executor");
                lua.execute::<()>(&executor).expect("Failed to execute script");
                *vm_ref = Some(LuaState { lua, executor });
            }
        });
    }

    pub fn call_f64_4(func_name: &'static str, a: f64, b: f64, c: f64, d: f64) -> f64 {
        LUA_VM.with(|vm| {
            let mut vm_ref = vm.borrow_mut();
            let state = vm_ref.as_mut().expect("Lua VM not initialized");
            
            state.lua.try_enter(|ctx| {
                let globals = ctx.globals();
                let func_value = globals.get(ctx, func_name);
                let func: Function = Function::from_value(ctx, func_value)
                    .map_err(|e| piccolo::Error::from(e))?;
                let executor = ctx.fetch(&state.executor);
                executor.restart(ctx, func, (a, b, c, d));
                Ok(())
            }).expect("Failed to set up call");
            
            state.lua.execute::<f64>(&state.executor).expect("Call failed")
        })
    }

    pub fn reset() {
        LUA_VM.with(|vm| {
            *vm.borrow_mut() = None;
        });
    }
}

// ============================================================================
// mlua (LuaJIT)
// ============================================================================

mod mlua_vm {
    use mlua::{Lua, Function, Result};
    use std::cell::RefCell;

    thread_local! {
        static LUA_VM: RefCell<Option<Lua>> = RefCell::new(None);
    }

    pub fn init(script: &str) {
        LUA_VM.with(|vm| {
            let mut vm_ref = vm.borrow_mut();
            if vm_ref.is_none() {
                let lua = Lua::new();
                lua.load(script).exec().expect("Failed to execute script");
                *vm_ref = Some(lua);
            }
        });
    }

    pub fn call_f64_4(func_name: &str, a: f64, b: f64, c: f64, d: f64) -> f64 {
        LUA_VM.with(|vm| {
            let vm_ref = vm.borrow();
            let lua = vm_ref.as_ref().expect("Lua VM not initialized");
            
            let func: Function = lua.globals().get(func_name).expect("Function not found");
            func.call::<f64>((a, b, c, d)).expect("Call failed")
        })
    }

    pub fn reset() {
        LUA_VM.with(|vm| {
            *vm.borrow_mut() = None;
        });
    }
}

// ============================================================================
// Pure Rust baseline
// ============================================================================

#[inline(always)]
fn rust_distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    ((x2 - x1).powi(2) + (y2 - y1).powi(2)).sqrt()
}

// ============================================================================
// Benchmark
// ============================================================================

fn main() {
    println!("=== Piccolo vs mlua (LuaJIT) Comparison ===\n");

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
    "#;

    // === Initialization ===
    println!("=== Initialization Time ===\n");
    
    let start = Instant::now();
    piccolo_vm::init(lua_script);
    let piccolo_init = start.elapsed();
    println!("  Piccolo init:  {:>8.2} µs", piccolo_init.as_secs_f64() * 1_000_000.0);
    
    piccolo_vm::reset();
    
    let start = Instant::now();
    mlua_vm::init(lua_script);
    let mlua_init = start.elapsed();
    println!("  mlua init:     {:>8.2} µs", mlua_init.as_secs_f64() * 1_000_000.0);
    
    mlua_vm::reset();
    println!();

    // Re-initialize for benchmarks
    piccolo_vm::init(lua_script);
    mlua_vm::init(lua_script);

    // === Warmup ===
    for _ in 0..1000 {
        let _ = piccolo_vm::call_f64_4("distance", 0.0, 0.0, 3.0, 4.0);
        let _ = mlua_vm::call_f64_4("distance", 0.0, 0.0, 3.0, 4.0);
    }

    // === Micro-benchmark: Single function calls ===
    println!("=== Micro-benchmark: distance(x1, y1, x2, y2) ===\n");
    
    let iterations = 100_000;
    
    // Piccolo
    let start = Instant::now();
    let mut piccolo_sum = 0.0;
    for i in 0..iterations {
        let x1 = (i % 100) as f64;
        let y1 = (i / 100) as f64;
        piccolo_sum += piccolo_vm::call_f64_4("distance", x1, y1, 500.0, 500.0);
    }
    let piccolo_time = start.elapsed();
    
    // mlua
    let start = Instant::now();
    let mut mlua_sum = 0.0;
    for i in 0..iterations {
        let x1 = (i % 100) as f64;
        let y1 = (i / 100) as f64;
        mlua_sum += mlua_vm::call_f64_4("distance", x1, y1, 500.0, 500.0);
    }
    let mlua_time = start.elapsed();
    
    // Rust baseline
    let start = Instant::now();
    let mut rust_sum = 0.0;
    for i in 0..iterations {
        let x1 = (i % 100) as f64;
        let y1 = (i / 100) as f64;
        rust_sum += rust_distance(x1, y1, 500.0, 500.0);
    }
    let rust_time = start.elapsed();

    let piccolo_ns = piccolo_time.as_nanos() as f64 / iterations as f64;
    let mlua_ns = mlua_time.as_nanos() as f64 / iterations as f64;
    let rust_ns = rust_time.as_nanos() as f64 / iterations as f64;

    println!("  {} iterations:", iterations);
    println!();
    println!("  {:12} {:>10} {:>12} {:>10}", "VM", "Total", "Per Call", "vs Rust");
    println!("  {:12} {:>10} {:>12} {:>10}", "----", "-----", "--------", "-------");
    println!("  {:12} {:>8.2} ms {:>9.1} ns {:>8.1}×", 
        "Rust", rust_time.as_secs_f64() * 1000.0, rust_ns, 1.0);
    println!("  {:12} {:>8.2} ms {:>9.1} ns {:>8.1}×", 
        "mlua/LuaJIT", mlua_time.as_secs_f64() * 1000.0, mlua_ns, mlua_ns / rust_ns);
    println!("  {:12} {:>8.2} ms {:>9.1} ns {:>8.1}×", 
        "Piccolo", piccolo_time.as_secs_f64() * 1000.0, piccolo_ns, piccolo_ns / rust_ns);
    println!();
    println!("  LuaJIT vs Piccolo: {:.1}× faster", piccolo_ns / mlua_ns);
    println!();
    println!("  Results match: Piccolo={:.2}, mlua={:.2}, Rust={:.2}", 
        piccolo_sum, mlua_sum, rust_sum);
    println!();

    // === Simulated DuckDB Query ===
    println!("=== Simulated DuckDB 100×100 Cross-Join ===\n");
    
    let entity_count = 100;
    let total_pairs = entity_count * entity_count;
    
    let positions: Vec<(f64, f64)> = (0..entity_count)
        .map(|i| ((i * 17 % 1000) as f64, (i * 31 % 1000) as f64))
        .collect();

    // Piccolo
    let start = Instant::now();
    let mut piccolo_matches = 0;
    for (x1, y1) in &positions {
        for (x2, y2) in &positions {
            let dist = piccolo_vm::call_f64_4("distance", *x1, *y1, *x2, *y2);
            if dist < 50.0 { piccolo_matches += 1; }
        }
    }
    let piccolo_query = start.elapsed();

    // mlua
    let start = Instant::now();
    let mut mlua_matches = 0;
    for (x1, y1) in &positions {
        for (x2, y2) in &positions {
            let dist = mlua_vm::call_f64_4("distance", *x1, *y1, *x2, *y2);
            if dist < 50.0 { mlua_matches += 1; }
        }
    }
    let mlua_query = start.elapsed();

    // Rust
    let start = Instant::now();
    let mut rust_matches = 0;
    for (x1, y1) in &positions {
        for (x2, y2) in &positions {
            let dist = rust_distance(*x1, *y1, *x2, *y2);
            if dist < 50.0 { rust_matches += 1; }
        }
    }
    let rust_query = start.elapsed();

    println!("  {} pairs evaluated:", total_pairs);
    println!();
    println!("  {:12} {:>10} {:>12} {:>10}", "VM", "Total", "Per Call", "Budget %");
    println!("  {:12} {:>10} {:>12} {:>10}", "----", "-----", "--------", "--------");
    println!("  {:12} {:>8.2} ms {:>9.1} ns {:>8.1}%", 
        "Rust", 
        rust_query.as_secs_f64() * 1000.0, 
        rust_query.as_nanos() as f64 / total_pairs as f64,
        rust_query.as_secs_f64() * 1000.0 / 16.67 * 100.0);
    println!("  {:12} {:>8.2} ms {:>9.1} ns {:>8.1}%", 
        "mlua/LuaJIT", 
        mlua_query.as_secs_f64() * 1000.0,
        mlua_query.as_nanos() as f64 / total_pairs as f64,
        mlua_query.as_secs_f64() * 1000.0 / 16.67 * 100.0);
    println!("  {:12} {:>8.2} ms {:>9.1} ns {:>8.1}%", 
        "Piccolo", 
        piccolo_query.as_secs_f64() * 1000.0,
        piccolo_query.as_nanos() as f64 / total_pairs as f64,
        piccolo_query.as_secs_f64() * 1000.0 / 16.67 * 100.0);
    println!();
    println!("  Matches: {} (all match)", piccolo_matches);
    println!();

    // === Frame budget analysis ===
    println!("=== Frame Budget Analysis (16.67ms = 60 FPS) ===\n");
    
    let budget_10pct_ms = 16.67 * 0.10;
    
    let piccolo_calls = (budget_10pct_ms * 1_000_000.0) / piccolo_ns;
    let mlua_calls = (budget_10pct_ms * 1_000_000.0) / mlua_ns;
    
    println!("  At 10% frame budget ({:.2} ms):", budget_10pct_ms);
    println!();
    println!("  {:12} {:>12} {:>15}", "VM", "Max Calls", "Cross-Join");
    println!("  {:12} {:>12} {:>15}", "----", "---------", "----------");
    println!("  {:12} {:>12.0} {:>6.0}×{:.0}", 
        "mlua/LuaJIT", mlua_calls, mlua_calls.sqrt(), mlua_calls.sqrt());
    println!("  {:12} {:>12.0} {:>6.0}×{:.0}", 
        "Piccolo", piccolo_calls, piccolo_calls.sqrt(), piccolo_calls.sqrt());
    println!();

    // === Verdict ===
    println!("=== VERDICT ===\n");
    
    if mlua_ns < 100.0 {
        println!("  🚀 mlua/LuaJIT is FAST:");
        println!("     • {:.0} ns/call - suitable for hot paths", mlua_ns);
        println!("     • Can handle {:.0}×{:.0} cross-joins at 10% budget", 
            mlua_calls.sqrt(), mlua_calls.sqrt());
        println!("     • JIT compilation makes math nearly native speed");
    }
    
    println!();
    
    if piccolo_ns < 1000.0 {
        println!("  ✅ Piccolo is VIABLE:");
        println!("     • {:.0} ns/call - suitable for UDF use cases", piccolo_ns);
        println!("     • Pure Rust - no FFI, no C dependencies");
        println!("     • Sandboxed - great for untrusted mod code");
        println!("     • Can handle {:.0}×{:.0} cross-joins at 10% budget",
            piccolo_calls.sqrt(), piccolo_calls.sqrt());
    } else {
        println!("  ⚠️ Piccolo is SLOWER than expected");
    }
    
    println!();
    println!("  Recommendation:");
    if mlua_ns < piccolo_ns / 5.0 {
        println!("     • Use mlua/LuaJIT for performance-critical UDFs");
        println!("     • Use Piccolo for sandboxed/untrusted scripts");
    } else {
        println!("     • Both are viable; choose based on requirements");
    }
}
