use polars::df;
use polars::prelude::*;
use std::fmt::format;
// use jemallocator::Jemalloc;
use std::time::Instant;
use rand::{Rng};

// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

const size: usize = 1024 * 1024 * 2;

fn main() {

    // let start = Instant::now();

    // let df = df![
    //     "x" => &[0.0; size],
    //     "y" => &[0.0; size],
    //     "vx" => &[1.0; size],
    //     "vy" => &[1.0; size],
    // ]
    // .unwrap();

    // let duration = start.elapsed();

    // println!("{:?}", df);
    // println!("Time taken to create DataFrame: {:?}", duration);
    // println!("Average time taken to create DataFrame: {:?}", duration.div_f32(size as f32));

    let start = Instant::now();
    let lf2 = DataFrame::default()
        .lazy()
        .with_columns(vec![
            lit(LiteralValue::Range {
                low: 0,
                high: size as i64,
                dtype: DataType::UInt32,
            }).alias("id")]
        ).with_columns(vec![
            as_struct(vec![lit(0.0).alias("x"), lit(0.0).alias("y")]).alias("position"),
            as_struct(vec![lit(1.0).alias("vx"), lit(1.0).alias("vy")]).alias("velocity"),
        ])
        .select([col("position"), col("velocity")]);
    // println!("{}", lf2.explain( true).unwrap());
    let df2 = lf2
        .collect()
        .unwrap();
    let duration = start.elapsed();
    println!("{:?}", df2);
    println!("Time taken to create DataFrame: {:?}", duration);
    println!("Average time taken to create DataFrame: {:?}", duration.div_f32(size as f32));

    let mut df3 = DataFrame::default();
    df3 = add_xoshiro_on_df(df3.lazy(), "player").collect().unwrap();
    let start = Instant::now();
    for _ in 0..40 {
        // let (df3_, profile) = xoshiro_advance_state(df3.lazy()).profile().unwrap();
        // df3 = df3_;
        // println!("{}", profile);
        df3 = xoshiro_advance_state(df3.lazy(), "player").collect().unwrap();
    }
    let duration = start.elapsed();
    println!("{:?}", df3);
    println!("Time taken to create DataFrame: {:?}", duration);

}

fn add_xoshiro_on_df(lf: LazyFrame, rng_name: &str) -> LazyFrame {
    // seed the rng
    // mimic the original benchmark seeding logic
    let mut rng = rand::thread_rng();
    let random_number = rng.r#gen::<u32>();
    lf.with_column(
            lit(LiteralValue::Range {
                low: random_number as i64,
                high: (random_number + size as u32) as i64,
                dtype: DataType::UInt32
            }).alias(format!("{}_seed", rng_name)),
    )
    .with_column(as_struct(vec![
        (col(format!("{}_seed", rng_name)) + lit(3)).alias(format!("{}_0", rng_name)),
        (col(format!("{}_seed", rng_name)) + lit(5)).alias(format!("{}_1", rng_name)),
        (col(format!("{}_seed", rng_name)) + lit(7)).alias(format!("{}_2", rng_name)),
        (col(format!("{}_seed", rng_name)) + lit(11)).alias(format!("{}_3", rng_name)),
    ]).alias(format!("{}_rng", rng_name)))
    .select([all().exclude([format!("{}_seed", rng_name)])])
}

fn xoshiro_rotl(input: Expr, k: i32) -> Expr {
    // polars doesn't have broadcasted bit shifting yet, making do with mul/div instead
    (input.clone() * lit(1 << k)) + (input.clone() / lit(1 << (32 - k)))
}

fn xoshiro_current_random_number(rng_name: &str) -> Expr {
    xoshiro_rotl(col(format!("{}_rng", rng_name)).struct_().field_by_index(1) * lit(5), 7) * lit(9)
}

fn xoshiro_advance_state(lf: LazyFrame, rng_name: &str) -> LazyFrame {
    lf.with_columns(vec![
        (col(format!("{}_rng", rng_name)).struct_().field_by_index(1) * lit(1 << 9)).alias("t"),
        as_struct(vec![
            (col(format!("{}_rng", rng_name))).struct_().field_by_index(0).alias(format!("{}_0", rng_name)),
            (col(format!("{}_rng", rng_name))).struct_().field_by_index(1).alias(format!("{}_1", rng_name)),
            (col(format!("{}_rng", rng_name))).struct_().field_by_index(2).xor(col(format!("{}_rng", rng_name)).struct_().field_by_index(0)).alias(format!("{}_2", rng_name)),
            (col(format!("{}_rng", rng_name))).struct_().field_by_index(3).xor(col(format!("{}_rng", rng_name)).struct_().field_by_index(1)).alias(format!("{}_3", rng_name)),
        ]).alias(format!("{}_rng", rng_name)),
    ])
    .with_column(
        as_struct(vec![
            (col(format!("{}_rng", rng_name))).struct_().field_by_index(0).xor(col(format!("{}_rng", rng_name)).struct_().field_by_index(3)).alias(format!("{}_0", rng_name)),
            (col(format!("{}_rng", rng_name))).struct_().field_by_index(1).xor(col(format!("{}_rng", rng_name)).struct_().field_by_index(2)).alias(format!("{}_1", rng_name)),
            (col(format!("{}_rng", rng_name))).struct_().field_by_index(2).xor(col("t")).alias(format!("{}_2", rng_name)),
            xoshiro_rotl((col(format!("{}_rng", rng_name))).struct_().field_by_index(3), 11).alias(format!("{}_3", rng_name)),
        ]).alias(format!("{}_rng", rng_name))
    )
    .select([all().exclude(["t"])])
}