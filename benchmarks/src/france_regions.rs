// Importer for https://github.com/gregoiredavid/france-geojson

use std::{fs, io::BufReader};

use geojson::{GeoJson, de::deserialize_feature_collection_to_vec};

pub fn parse() -> impl Iterator<Item = (String, GeoJson)> {
    let file = fs::File::open("assets/france-geojson/regions.geojson").unwrap();
    let file = BufReader::new(file);
    let input: Vec<serde_json::Value> = deserialize_feature_collection_to_vec(file).unwrap();
    input.into_iter().map(|value| {
        (
            format!(
                "{}, {} - region",
                value["nom"].as_str().unwrap().to_lowercase(),
                value["code"].as_str().unwrap()
            ),
            GeoJson::from_json_value(value["geometry"].clone()).unwrap(),
        )
    })
}
