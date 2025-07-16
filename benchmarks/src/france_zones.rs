use geojson::GeoJson;

pub fn parse() -> impl Iterator<Item = (String, GeoJson)> {
    crate::france_arrondissements::parse()
        .chain(crate::france_cantons::parse())
        .chain(crate::france_communes::parse())
        .chain(crate::france_departements::parse())
        .chain(crate::france_regions::parse())
}
