// Importer for https://www.data.gouv.fr/fr/datasets/adresses-extraites-du-cadastre/#/resources

use std::{fs, io::BufReader};

use flate2::bufread::GzDecoder;
use geojson::{GeoJson, de::deserialize_feature_collection_to_vec};

pub fn parse() -> impl Iterator<Item = GeoJson> {
    fs::read_dir("assets/cadastre").unwrap().flat_map(|entry| {
        let dir = entry.unwrap();
        println!("Importing {}", dir.path().display());
        let file = std::fs::File::open(dir.path()).unwrap();
        let file = BufReader::new(file);
        let file = GzDecoder::new(file);
        let input: Vec<serde_json::Value> = deserialize_feature_collection_to_vec(file).unwrap();
        input
            .into_iter()
            .map(|value| GeoJson::from_json_value(value["geometry"].clone()).unwrap())
    })
}
