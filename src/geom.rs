use geo::{Coord, LineString, MultiPolygon, Polygon};
use h3o::{CellIndex, LatLng};

// In meters
const EARTH_RADIUS: f64 = 6378137.0;

pub fn bounding_box(cell: CellIndex) -> MultiPolygon {
    let coord = LatLng::from(cell);
    let center_lat_rad: f64 = coord.lat_radians();
    let center_lng_rad: f64 = coord.lng_radians();
    let distance_in_meters: f64 = cell.edges().next().unwrap().length_m() * 1.6;

    // Angular distance in radians on a great circle
    let angular_distance = distance_in_meters / EARTH_RADIUS;

    // Calculate min and max latitudes
    let mut min_lat = center_lat_rad - angular_distance;
    let mut max_lat = center_lat_rad + angular_distance;

    // Calculate min and max longitudes
    // This needs to account for the fact that longitude degrees vary in distance based on latitude
    let delta_lng = ((angular_distance).sin() / (center_lat_rad).cos()).asin();
    let mut min_lng = center_lng_rad - delta_lng;
    let mut max_lng = center_lng_rad + delta_lng;

    // Handle edge cases near poles and international date line
    if min_lat < -std::f64::consts::FRAC_PI_2 {
        min_lat = -std::f64::consts::FRAC_PI_2;
    }
    if max_lat > std::f64::consts::FRAC_PI_2 {
        max_lat = std::f64::consts::FRAC_PI_2;
    }

    if min_lng < -std::f64::consts::PI {
        min_lng += 2.0 * std::f64::consts::PI;
    }
    if max_lng > std::f64::consts::PI {
        max_lng -= 2.0 * std::f64::consts::PI;
    }

    min_lat = min_lat.to_degrees();
    max_lat = max_lat.to_degrees();
    min_lng = min_lng.to_degrees();
    max_lng = max_lng.to_degrees();

    MultiPolygon::new(vec![Polygon::new(
        LineString(vec![
            Coord {
                x: min_lng,
                y: max_lat,
            },
            Coord {
                x: max_lng,
                y: max_lat,
            },
            Coord {
                x: max_lng,
                y: min_lat,
            },
            Coord {
                x: min_lng,
                y: min_lat,
            },
        ]),
        Vec::default(),
    )])
}
