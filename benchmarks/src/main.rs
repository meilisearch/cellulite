use cellulite::{Database, Writer};
use heed::EnvOpenOptions;
use tempfile::TempDir;

mod france_shops;

fn main() {
    println!("Starting...");
    let time = std::time::Instant::now();
    let input = france_shops::parse();
    println!("Deserialized the points in {:?}", time.elapsed());

    println!("Database setup");
    let (_temp_dir, path) = match std::env::args().nth(1) {
        None => {
            let temp_dir = TempDir::new().unwrap();
            let path = temp_dir.path().to_str().unwrap().to_string();
            (Some(temp_dir), path)
        }
        Some(path) => {
            std::fs::create_dir_all(&path).unwrap();
            (None, path)
        }
    };
    let env = unsafe { EnvOpenOptions::new().map_size(200 * 1024 * 1024).open(path) }.unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let database: Database = env.create_database(&mut wtxn, None).unwrap();
    wtxn.commit().unwrap();

    println!("Inserting {} points", input.len());
    let time = std::time::Instant::now();
    let writer = Writer::new(database);
    let mut wtxn = env.write_txn().unwrap();

    for (i, coord) in input.iter().enumerate() {
        writer.add_item(&mut wtxn, i as u32, *coord).unwrap();
    }
    wtxn.commit().unwrap();

    println!("Points inserted in {:?}", time.elapsed());
}
