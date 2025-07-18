// Importer for https://cadastre.data.gouv.fr/datasets/cadastre-etalab

use std::{fs, io::BufReader};

use flate2::bufread::GzDecoder;
use geojson::{GeoJson, de::deserialize_feature_collection_to_vec};

pub fn parse(selector: Vec<String>) -> impl Iterator<Item = (String, GeoJson)> {
    fs::read_dir("assets/cadastre_parcelle").unwrap()
    .map(|entry| entry.unwrap())
    .filter(move |dir| {
        selector.is_empty() || selector.iter().any(|s| dir.path().to_str().unwrap().contains(s))
    })
    .flat_map(|dir| {
        println!("Importing {}", dir.path().display());

        let file = std::fs::File::open(dir.path()).unwrap();
        let file = BufReader::new(file);
        let file = GzDecoder::new(file);
        let input: Vec<serde_json::Value> = deserialize_feature_collection_to_vec(file).unwrap();
        input.into_iter().map(|value| {
            (
                format!(
                    "{}", value["properties"]["id"]
                ),
                GeoJson::from_json_value(value["geometry"].clone()).unwrap(),
            )
        })
    })
}
