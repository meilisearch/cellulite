use geo::algorithm::proj::Proj;
use geojson::{GeoJson, Value};
use std::io::BufReader;

use flate2::bufread::GzDecoder;

#[derive(serde::Deserialize, Debug)]
struct Schema {
    features: Vec<Feature>,
}

#[derive(serde::Deserialize, Debug)]
struct Feature {
    geometry: Geometry,
    #[allow(unused)]
    properties: Properties,
}

#[derive(serde::Deserialize, Debug)]
struct Geometry {
    coordinates: [f64; 2],
}

#[derive(serde::Deserialize, Debug)]
#[allow(unused)]
struct Properties {
    shop: Option<String>,
    name: Option<String>,
}

pub fn parse() -> impl Iterator<Item = (String, GeoJson)> {
    let file = std::fs::File::open("assets/france-shops.json.gz").unwrap();
    let file = BufReader::new(file);
    let file = GzDecoder::new(file);
    let input: Schema = serde_json::from_reader(file).unwrap();
    let projection = Proj::new_known_crs("EPSG:3857", "EPSG:4326", None).unwrap();

    input
        .features
        .into_iter()
        .map(|feature| (feature.properties.name.unwrap_or_else(|| feature.properties.shop.unwrap_or_default()), feature.geometry.coordinates))
        .map(move |(shop, [x, y])| (shop, projection.convert((x, y)).unwrap()))
        .map(|(shop, (lat, lng))| (shop, GeoJson::Geometry(geojson::Geometry::new(Value::Point(vec![lat, lng])))))
}
