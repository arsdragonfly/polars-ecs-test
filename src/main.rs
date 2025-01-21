use polars::df;
use polars::prelude::*;
use std::fmt::format;
use std::iter;
// use jemallocator::Jemalloc;
use rand::Rng;
use std::time::Instant;

// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

const size: usize = 1024 * 1024 * 2;

enum HealthStatusEffect {
    Spawn = 0,
    Dead,
    Alive,
}

enum PlayerType {
    NPC = 0,
    Monster,
    Hero,
}

enum Sprite {
    Default = ' ' as char as isize,
    Spawn = '_' as char as isize,
}

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
    let mut lf2 = DataFrame::default()
        .lazy()
        .with_columns(vec![
            lit(LiteralValue::Range {
                low: 0,
                high: size as i64,
                dtype: DataType::UInt32,
            })
            .alias("id"),
        ])
        .with_columns(vec![
            // position component
            lit(LiteralValue::Float32(0.0)).alias("x"),
            lit(LiteralValue::Float32(0.0)).alias("y"),
            // lit(LiteralValue::Float32(0.0)).alias("z"),

            // velocity component
            lit(LiteralValue::Float32(1.0)).alias("vx"),
            lit(LiteralValue::Float32(1.0)).alias("vy"),
            // lit(LiteralValue::Float32(1.0)).alias("vz"),
        ])
        .with_columns(vec![
            // position component
            // as_struct(vec![lit(0.0).alias("x"), lit(0.0).alias("y")]).alias("position"),
            // velocity component
            // as_struct(vec![lit(1.0).alias("vx"), lit(1.0).alias("vy")]).alias("velocity"),
            // data component
            lit(LiteralValue::Int32(0)).alias("data_thingy"),
            lit(LiteralValue::Float64(0.0)).alias("data_dingy"),
            lit(LiteralValue::Boolean(false)).alias("data_mingy"),
        ]);
    // add rng for data component
    lf2 = add_xoshiro_seed_df(lf2, "data_rng", 340383);
    lf2 = lf2.with_column(xoshiro_calculate_current_random_number("data_rng").alias("data_numgy"));
    lf2 = xoshiro_advance_state(lf2, "data_rng");

    // add player component
    lf2 = add_xoshiro_on_df(lf2, "player_rng");
    lf2 = lf2.with_columns(vec![
        lit(LiteralValue::Int32(PlayerType::NPC as i32)).alias("player_type"),
    ]);

    // add health component
    lf2 = lf2.with_columns(vec![
        lit(LiteralValue::Int32(0)).alias("health_hp"),
        lit(LiteralValue::Int32(0)).alias("health_maxhp"),
        lit(LiteralValue::Int32(HealthStatusEffect::Spawn as i32))
            .cast(DataType::Int32)
            .alias("health_shield"),
    ]);

    // add damage component
    lf2 = lf2.with_columns(vec![
        lit(LiteralValue::Int32(0)).alias("damage_atk"),
        lit(LiteralValue::Int32(0)).alias("damage_def"),
    ]);

    // add sprite component
    lf2 = lf2.with_columns(vec![
        lit(LiteralValue::UInt8(Sprite::Default as u8)).alias("sprite_char"),
    ]);

    // initComponent
    lf2 = lf2.with_column(
        when((col("player_rng_cur") % lit(100)).lt_eq(lit(2)))
            .then(lit(LiteralValue::Int32(PlayerType::NPC as i32)))
            .otherwise(when(
                (col("player_rng_cur") % lit(100)).lt_eq(lit(29)),
            ).then(lit(LiteralValue::Int32(PlayerType::Hero as i32)))
            .otherwise(lit(LiteralValue::Int32(PlayerType::Monster as i32)))).alias("player_type")
    );

    lf2 = xoshiro_advance_state(lf2, "player_rng");

    lf2 = lf2.with_column(
        when(col("player_type").eq(lit(PlayerType::Hero as i32)))
            .then(col("player_rng_cur") % lit(11) + lit(5))
            .otherwise(when(col("player_type").eq(lit(PlayerType::Monster as i32)))
                .then(col("player_rng_cur") % lit(9) + lit(4))
                .otherwise(col("player_rng_cur") % lit(7) + lit(6))).alias("health_maxhp")
    );

    lf2 = xoshiro_advance_state(lf2, "player_rng");

    lf2 = lf2.with_column(
        when(col("player_type").eq(lit(PlayerType::Hero as i32)))
            .then(col("player_rng_cur") % lit(5) + lit(2))
            .otherwise(when(col("player_type").eq(lit(PlayerType::Monster as i32)))
                .then(col("player_rng_cur") % lit(7) + lit(2))
                .otherwise(col("player_rng_cur") % lit(6) + lit(3))).alias("damage_def")
    );

    lf2 = xoshiro_advance_state(lf2, "player_rng");

    lf2 = lf2.with_column(
        when(col("player_type").eq(lit(PlayerType::Hero as i32)))
            .then(col("player_rng_cur") % lit(6) + lit(4))
            .otherwise(when(col("player_type").eq(lit(PlayerType::Monster as i32)))
                .then(col("player_rng_cur") % lit(7) + lit(3))
                .otherwise(lit(0))).alias("damage_atk")
    );

    lf2 = xoshiro_advance_state(lf2, "player_rng");

    lf2 = lf2.with_column(
        lit(LiteralValue::UInt8(Sprite::Spawn as u8)).alias("sprite_char")
    );

    let spawnAreaMaxX: u32 = 320;
    let spawnAreaMaxY: u32 = 240;
    let spawnAreaMargin: u32 = 100;

    lf2 = lf2.with_column(
        (col("player_rng_cur") % lit(spawnAreaMaxX + spawnAreaMargin + 1).cast(DataType::Float32) - lit(spawnAreaMargin).cast(DataType::Float32)).alias("x")
    );

    lf2 = xoshiro_advance_state(lf2, "player_rng");

    lf2 = lf2.with_column(
        (col("player_rng_cur") % lit(spawnAreaMaxY + spawnAreaMargin + 1).cast(DataType::Float32) - lit(spawnAreaMargin).cast(DataType::Float32)).alias("y")
    );

    lf2 = xoshiro_advance_state(lf2, "player_rng");

    let mut df2 = lf2.collect().unwrap();
    let duration = start.elapsed();
    println!("{:?}", df2);
    println!("Time taken to initialize DataFrame: {:?}", duration);
    println!(
        "Average time taken to initialize DataFrame row: {:?}",
        duration.div_f32(size as f32)
    );

    // let start2 = Instant::now();
    let iterations = 1000;
    for _ in 0..iterations {
        // let (df3_, profile) = xoshiro_advance_state(df3.lazy()).profile().unwrap();
        // df3 = df3_;
        // println!("{}", profile);

        // df3 = xoshiro_advance_state(df3.lazy(), "player").collect().unwrap();

        let mut lf = df2.lazy();
        lf = lf.with_columns(vec![
                // movement system
                (col("x") + col("vx") * lit(1.0f32 / 60.0f32)).alias("x"),
                (col("y") + col("vy") * lit(1.0f32 / 60.0f32)).alias("y"),
                // data system
                ((col("data_thingy") + lit(1)) % lit(1000000)).alias("data_thingy"),
                (col("data_dingy") + lit(0.0001 * 1.0f32 / 60.0f32)).alias("data_dingy"),
                col("data_mingy").not().alias("data_mingy"),
                col("player_rng_cur").alias("data_numgy"),
                // more complex system


            ]);
        lf = xoshiro_advance_state(lf, "player_rng");
        // let pair = lf.profile().unwrap();
        // df3 = pair.0;
        // println!("{}", pair.1);
        df2 = lf.collect().unwrap();
    }
    let duration = start.elapsed();
    println!("{:?}", df2);
    println!("Time taken to update DataFrame: {:?}", duration);
    println!("Time taken to update 1 iteration for DataFrame: {:?}", duration.div_f32(iterations as f32));
    println!(
        "Average time taken to update DataFrame row: {:?}",
        duration.div_f32(size as f32 * iterations as f32)
    );
}

fn add_xoshiro_seed_df(lf: LazyFrame, rng_name: &str, seed: u32) -> LazyFrame {
    // seed the rng with a fixed seed
    lf.with_column(
        lit(seed)
            .cast(DataType::UInt32)
            .alias(format!("{}_seed", rng_name)),
    )
    .with_column(
        as_struct(vec![
            (col(format!("{}_seed", rng_name)) + lit(3)).alias(format!("{}_0", rng_name)),
            (col(format!("{}_seed", rng_name)) + lit(5)).alias(format!("{}_1", rng_name)),
            (col(format!("{}_seed", rng_name)) + lit(7)).alias(format!("{}_2", rng_name)),
            (col(format!("{}_seed", rng_name)) + lit(11)).alias(format!("{}_3", rng_name)),
        ])
        .alias(format!("{}_rng", rng_name)),
    )
    .select([all().exclude([format!("{}_seed", rng_name)])])
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
            dtype: DataType::UInt32,
        })
        .alias(format!("{}_seed", rng_name)),
    )
    .with_columns(vec![
        as_struct(vec![
            (col(format!("{}_seed", rng_name)) + lit(3)).alias(format!("{}_0", rng_name)),
            (col(format!("{}_seed", rng_name)) + lit(5)).alias(format!("{}_1", rng_name)),
            (col(format!("{}_seed", rng_name)) + lit(7)).alias(format!("{}_2", rng_name)),
            (col(format!("{}_seed", rng_name)) + lit(11)).alias(format!("{}_3", rng_name)),
        ])
        .alias(format!("{}_rng", rng_name)),
        (col(format!("{}_seed", rng_name)) + lit(13)).alias(format!("{}_cur", rng_name)),
    ]
    )
    .select([all().exclude([format!("{}_seed", rng_name)])])
}

fn xoshiro_rotl(input: Expr, k: i32) -> Expr {
    // polars doesn't have broadcasted bit shifting yet, making do with mul/div instead
    (input.clone() * lit(1 << k)) + (input.clone() / lit(1 << (32 - k)))
}

fn xoshiro_calculate_current_random_number(rng_name: &str) -> Expr {
    xoshiro_rotl(
        col(format!("{}_rng", rng_name)).struct_().field_by_index(1) * lit(5),
        7,
    ) * lit(9)
}

fn xoshiro_advance_state(lf: LazyFrame, rng_name: &str) -> LazyFrame {
    lf.with_columns(vec![
        (col(format!("{}_rng", rng_name)).struct_().field_by_index(1) * lit(1 << 9)).alias("t"),
        xoshiro_calculate_current_random_number(rng_name).alias(format!("{}_cur", rng_name)),
        as_struct(vec![
            (col(format!("{}_rng", rng_name)))
                .struct_()
                .field_by_index(0)
                .alias(format!("{}_0", rng_name)),
            (col(format!("{}_rng", rng_name)))
                .struct_()
                .field_by_index(1)
                .alias(format!("{}_1", rng_name)),
            (col(format!("{}_rng", rng_name)))
                .struct_()
                .field_by_index(2)
                .xor(col(format!("{}_rng", rng_name)).struct_().field_by_index(0))
                .alias(format!("{}_2", rng_name)),
            (col(format!("{}_rng", rng_name)))
                .struct_()
                .field_by_index(3)
                .xor(col(format!("{}_rng", rng_name)).struct_().field_by_index(1))
                .alias(format!("{}_3", rng_name)),
        ])
        .alias(format!("{}_rng", rng_name)),
    ])
    .with_column(
        as_struct(vec![
            (col(format!("{}_rng", rng_name)))
                .struct_()
                .field_by_index(0)
                .xor(col(format!("{}_rng", rng_name)).struct_().field_by_index(3))
                .alias(format!("{}_0", rng_name)),
            (col(format!("{}_rng", rng_name)))
                .struct_()
                .field_by_index(1)
                .xor(col(format!("{}_rng", rng_name)).struct_().field_by_index(2))
                .alias(format!("{}_1", rng_name)),
            (col(format!("{}_rng", rng_name)))
                .struct_()
                .field_by_index(2)
                .xor(col("t"))
                .alias(format!("{}_2", rng_name)),
            xoshiro_rotl(
                (col(format!("{}_rng", rng_name)))
                    .struct_()
                    .field_by_index(3),
                11,
            )
            .alias(format!("{}_3", rng_name)),
        ])
        .alias(format!("{}_rng", rng_name)),
    )
    .select([all().exclude(["t"])])
}
