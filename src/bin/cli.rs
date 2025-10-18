use clap::{Parser, Subcommand};
use justdb::{Config, StorageEngine};
use std::{
    io::{self, Write},
    path::PathBuf,
};

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "justdbd")]
#[command(about = "A CLI for my database engine")]
#[command(version)]
struct Cli {
    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    #[arg(short, long)]
    verbose: bool,

    #[arg(long)]
    no_compression: bool,

    #[arg(long, default_value = "64")]
    memtable_size: usize,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Put {
        key: String,
        value: String,
    },

    Get {
        key: String,
    },

    Delete {
        key: String,
    },

    Scan {
        start: String,
        end: String,
    },

    Stats,
    Interactive,
    Benchmark {
        #[arg(short, long, default_value = "10000")]
        operations: usize,

        #[arg(long, default_value = "16")]
        key_size: usize,

        #[arg(long, default_value = "128")]
        value_size: usize,

        #[arg(long, default_value = "80")]
        read_percentage: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // init
    let filter = if cli.verbose {
        tracing_subscriber::EnvFilter::new("debug")
    } else {
        tracing_subscriber::EnvFilter::new("info")
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(false)
                .with_file(false)
                .with_line_number(false),
        )
        .with(filter)
        .init();

    // create config
    let config = Config::new()
        .with_memtable_size(cli.memtable_size * 1024 * 1024)
        .with_compression(if cli.no_compression {
            justdb::config::CompressionType::None
        } else {
            justdb::config::CompressionType::Lz4
        });

    // open engine
    let engine = StorageEngine::open(&cli.data_dir, config).await?;

    // execute commands
    match cli.command {
        Commands::Put { key, value } => {
            engine.put(key.as_bytes(), value.as_bytes()).await?;
            println!("OK");
        }

        Commands::Get { key } => match engine.get(key.as_bytes()).await? {
            Some(value) => match String::from_utf8(value.clone()) {
                Ok(s) => println!("{s}"),
                Err(_) => println!("{value:?}"),
            },
            None => println!("(nil)"),
        },

        Commands::Delete { key } => {
            engine.delete(key.as_bytes()).await?;
            println!("OK");
        }

        Commands::Scan { start, end } => {
            let results = engine.scan(start.as_bytes(), end.as_bytes()).await?;

            for (key, value) in results {
                let key_str = String::from_utf8_lossy(&key);
                let value_str = String::from_utf8_lossy(&value);

                println!("{key_str}: {value_str}");
            }
        }

        Commands::Stats => {
            let stats = engine.stats().await;
            print_stats(&stats);
        }

        Commands::Interactive => {
            run_intractive_mode(&engine).await?;
        }

        Commands::Benchmark {
            operations,
            key_size,
            value_size,
            read_percentage,
        } => {
            run_benchmark(&engine, operations, key_size, value_size, read_percentage).await?;
        }
    }

    engine.close().await?;
    Ok(())
}

async fn run_benchmark(
    engine: &StorageEngine,
    operations: usize,
    key_size: usize,
    value_size: usize,
    read_percentage: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    use rand::Rng;
    use std::time::Instant;

    println!("Running benchmark...");
    println!("Operations: {operations}");
    println!("Key size: {key_size}");
    println!("Value size: {value_size}");
    println!("Read percentage: {read_percentage}%");
    println!();

    let mut rng = rand::rng();
    let start_time = Instant::now();

    let populate_count = operations / 10;
    println!("pre-populating with {populate_count} entries....");

    for i in 0..populate_count {
        let key = generate_key(i, key_size);
        let value = generate_value(i, value_size);
        engine.put(&key, &value).await?;
    }

    let populate_time = start_time.elapsed();
    println!("pre-populating completed in {populate_time:?}");

    // run benchmark
    let bench_start = Instant::now();
    let mut read_ops = 0;
    let mut write_ops = 0;
    let mut read_hits = 0;

    for i in 0..operations {
        let is_read = rng.random_range(0..100) < read_percentage;

        if is_read {
            let key_id = rng.random_range(0..populate_count + i);
            let key = generate_key(key_id, key_size);

            match engine.get(&key).await? {
                Some(_) => read_hits += 1,
                None => {}
            }
            read_ops += 1;
        } else {
            let key = generate_key(populate_count + i, key_size);
            let value = generate_key(populate_count + i, value_size);

            engine.put(&key, &value).await?;
            read_ops += 1;
        }

        if (i + 1) % (operations / 10) == 0 {
            println!(
                "
Progress: {}/{operations}
",
                i + 1
            );
        }
    }

    let bench_time = bench_start.elapsed();

    // print results
    println!();
    println!("benchmark resutls");
    println!("===================");
    println!("Total time: {bench_time:?}");

    println!(
        "
operations/sec: {:.2}
",
        operations as f64 / bench_time.as_secs_f64()
    );

    println!(
        "
read operations: {} ({:.1}%)
",
        read_ops,
        read_ops as f64 / operations as f64 * 100.
    );

    println!(
        "
writes operations: {} ({:.1}%)
",
        write_ops,
        write_ops as f64 / operations as f64 * 100.
    );

    println!(
        "
read hit rate: {:.1}% 
",
        read_hits as f64 / read_ops as f64 * 100.
    );

    let stats = engine.stats().await;
    println!();
    print_stats(&stats);

    Ok(())
}

fn print_stats(stats: &justdb::storage::StorageStats) {
    println!("Storage Engine Statistics");
    println!("=========================");
    println!("Sequence number: {}", stats.seqence_number);
    println!("Closed: {}", stats.is_closed);
    println!();

    println!("Memtable:");
    println!("  Entries: {}", stats.memtable_stats.entry_count);
    println!("  Size: {} bytes", stats.memtable_stats.approximate_size);
    println!("  Immutable: {}", stats.memtable_stats.is_immutable);
    println!();

    println!("Immutable memtables: {}", stats.immutable_memtable_count);
    println!();

    println!("SStables:");
    for level in 0..7 {
        if let Some(level_stats) = stats.sstable_stats.get(&level) {
            println!(
                "
  Level: {level}: {} tables, {} entries, {} bytes
",
                level_stats.table_count, level_stats.total_entries, level_stats.total_size
            );
        }
    }
}

async fn run_intractive_mode(engine: &StorageEngine) -> Result<(), Box<dyn std::error::Error>> {
    println!("Interactive Mode");
    println!(
        "
Commands: PUT <key> <value>, GET <key>, DELETE <key>, SCAN <start> <end>, STATS, QUIT "
    );

    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0].to_uppercase().as_str() {
            "PUT" => {
                if parts.len() != 3 {
                    println!("Usage: PUT <key> <value>");
                    continue;
                }

                match engine.put(parts[1].as_bytes(), parts[2].as_bytes()).await {
                    Ok(()) => println!("OK"),
                    Err(e) => eprintln!("Error: {e}"),
                }
            }

            "GET" => {
                if parts.len() != 2 {
                    println!("Usage: GET <key>");
                    continue;
                }

                match engine.get(parts[1].as_bytes()).await {
                    Ok(Some(value)) => match String::from_utf8(value) {
                        Ok(s) => println!("{s}"),
                        Err(_) => println!("(dataaaa)"),
                    },
                    Ok(None) => println!("(nil)"),
                    Err(e) => eprintln!("Error: {e}"),
                }
            }

            "DELETE" => {
                if parts.len() != 2 {
                    println!("Usage: DELETE <key>");
                    continue;
                }

                match engine.delete(parts[1].as_bytes()).await {
                    Ok(()) => println!("OK"),
                    Err(e) => eprintln!("Error {e}"),
                }
            }

            "SCAN" => {
                if parts.len() != 3 {
                    println!("Usage: SCAN <key> <value>");
                    continue;
                }

                match engine.scan(parts[1].as_bytes(), parts[2].as_bytes()).await {
                    Ok(results) => {
                        for (key, value) in &results {
                            let key_str = String::from_utf8_lossy(key);
                            let value_str = String::from_utf8_lossy(value);

                            println!("{key_str}: {value_str}")
                        }
                    }
                    Err(e) => eprintln!("Error: {e}"),
                }
            }

            "STATS" => {
                let stats = engine.stats().await;
                print_stats(&stats);
            }

            "QUIT" | "EXIT" => {
                println!("Goodbye!");
                break;
            }

            _ => {
                println!("Unknown command: {}", parts[0]);
                println!(
                    "Available commands: 
PUT, GET, DELETE, SCAN, STATS, QUIT"
                );
            }
        }
    }

    Ok(())
}

fn generate_key(id: usize, size: usize) -> Vec<u8> {
    let mut key = format!("key{:010}", id).into_bytes();
    key.resize(size, b'0');
    key
}

fn generate_value(id: usize, size: usize) -> Vec<u8> {
    let base = format!("value{:010}", id);
    let mut value = base.into_bytes();

    while value.len() < size {
        let remaining = size - value.len();
        let pattern = b"0123456789abcdef";
        let add = std::cmp::min(remaining, pattern.len());
        value.extend_from_slice(&pattern[..add]);
    }

    value.truncate(size);
    value
}

// ini bahasa Rust
// Pake ArchLinux
// Teks editor NeoVim
// Project Database Sistem
