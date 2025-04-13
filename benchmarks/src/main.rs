use std::{path::PathBuf, time::Duration};

use cellulite::{Database, Writer};
use clap::{Parser, ValueEnum};
use france_regions::{gard, le_vigan, nimes, occitanie};
use geojson::GeoJson;
use heed::EnvOpenOptions;
use tempfile::TempDir;

mod france_cadastre;
mod france_regions;
mod france_shops;

#[derive(Parser, Debug)]
struct Args {
    /// Name of the dataset to use
    #[arg(short, long, value_enum, default_value_t = Dataset::Shop)]
    dataset: Dataset,

    /// Skip indexing if set. You must provide the path to a database
    #[arg(long, default_value_t = false)]
    skip_indexing: bool,

    /// Skip query if set
    #[arg(long, default_value_t = false)]
    skip_queries: bool,

    /// Skip query if set
    #[arg(long)]
    db: Option<PathBuf>,
}

#[derive(Clone, Copy, PartialEq, Eq, Parser, Debug, ValueEnum)]
enum Dataset {
    Shop,
    Cadastre,
}

fn main() {
    let args = Args::parse();

    println!("Starting...");
    let time = std::time::Instant::now();
    let input = match args.dataset {
        Dataset::Shop => &mut france_shops::parse() as &mut dyn Iterator<Item = GeoJson>,
        Dataset::Cadastre => &mut france_cadastre::parse() as &mut dyn Iterator<Item = GeoJson>,
    };

    println!("Deserialized the points in {:?}", time.elapsed());

    println!("Database setup");
    let (_temp_dir, path) = match args.db {
        None => {
            let temp_dir = TempDir::new().unwrap();
            let path = temp_dir.path().to_path_buf();
            (Some(temp_dir), path)
        }
        Some(path) => {
            std::fs::create_dir_all(&path).unwrap();
            (None, path)
        }
    };
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(200 * 1024 * 1024 * 1024)
            .open(path)
    }
    .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let database: Database = env.create_database(&mut wtxn, None).unwrap();
    wtxn.commit().unwrap();

    if !args.skip_indexing {
        println!("Inserting points");
        let time = std::time::Instant::now();
        let mut cpt = 0;
        let writer = Writer::new(database);
        let mut wtxn = env.write_txn().unwrap();

        let mut print_timer = time;
        for coord in input {
            cpt += 1;
            if print_timer.elapsed() > Duration::from_secs(10) {
                let elapsed = time.elapsed();
                println!(
                    "Inserted {cpt} points in {elapsed:?}, throughput: {} points / seconds",
                    cpt as f32 / elapsed.as_secs_f32()
                );
                print_timer = std::time::Instant::now();
            }
            writer.add_item(&mut wtxn, cpt, &coord).unwrap();
        }
        wtxn.commit().unwrap();

        let elapsed = time.elapsed();
        println!("Inserted {cpt} points in {elapsed:?}.");
        println!("One point every {:?}", elapsed / cpt);
    }

    if !args.skip_queries {
        let repeat = 1000;

        let rtxn = env.read_txn().unwrap();
        let writer = Writer::new(database);
        let le_vigan = le_vigan();
        let time = std::time::Instant::now();
        let result = writer.in_shape(&rtxn, &le_vigan, &mut |_| ()).unwrap();
        for _ in 0..repeat {
            let sub_res = writer.in_shape(&rtxn, &le_vigan, &mut |_| ()).unwrap();
            assert_eq!(result.len(), sub_res.len());
        }
        println!(
            "Found {} stores in Le Vigan in {:?}",
            result.len(),
            time.elapsed() / repeat
        );

        let time = std::time::Instant::now();

        let nimes = nimes();
        let result = writer.in_shape(&rtxn, &nimes, &mut |_| ()).unwrap();
        for _ in 0..repeat {
            let sub_res = writer.in_shape(&rtxn, &nimes, &mut |_| ()).unwrap();
            assert_eq!(result.len(), sub_res.len());
        }
        println!(
            "Found {} stores in Nîmes in {:?}",
            result.len(),
            time.elapsed() / repeat
        );

        let repeat = 100;
        let gard = gard();
        let result = writer.in_shape(&rtxn, &gard, &mut |_| ()).unwrap();
        for _ in 0..repeat {
            let sub_res = writer.in_shape(&rtxn, &gard, &mut |_| ()).unwrap();
            assert_eq!(result.len(), sub_res.len());
        }
        println!(
            "Found {} stores in Gard in {:?}",
            result.len(),
            time.elapsed() / repeat
        );

        let repeat = 100;
        let occitanie = occitanie();
        let result = writer.in_shape(&rtxn, &occitanie, &mut |_| ()).unwrap();
        for _ in 0..repeat {
            let sub_res = writer.in_shape(&rtxn, &occitanie, &mut |_| ()).unwrap();
            assert_eq!(result.len(), sub_res.len());
        }
        println!(
            "Found {} stores in Occitanie in {:?}",
            result.len(),
            time.elapsed() / repeat
        );
    }
}
