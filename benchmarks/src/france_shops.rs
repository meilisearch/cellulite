use geo::algorithm::proj::Proj;
use geojson::{GeoJson, Value};
use std::io::BufReader;

use flate2::bufread::GzDecoder;

#[derive(serde::Deserialize)]
struct Schema {
    features: Vec<Feature>,
}

#[derive(serde::Deserialize)]
struct Feature {
    geometry: Geometry,
    #[allow(unused)]
    properties: Properties,
}

#[derive(serde::Deserialize)]
struct Geometry {
    coordinates: [f64; 2],
}

#[derive(serde::Deserialize)]
#[allow(unused)]
struct Properties {
    shop: Option<String>,
    name: Option<String>,
}

pub fn parse() -> impl Iterator<Item = GeoJson> {
    let file = std::fs::File::open("assets/france-shops.json.gz").unwrap();
    let file = BufReader::new(file);
    let file = GzDecoder::new(file);
    let input: Schema = serde_json::from_reader(file).unwrap();
    let projection = Proj::new_known_crs("EPSG:3857", "EPSG:4326", None).unwrap();

    input
        .features
        .into_iter()
        .map(|feature| feature.geometry.coordinates)
        .map(move |[x, y]| projection.convert((x, y)).unwrap())
        .map(|(lat, lng)| GeoJson::Geometry(geojson::Geometry::new(Value::Point(vec![lat, lng]))))
}
