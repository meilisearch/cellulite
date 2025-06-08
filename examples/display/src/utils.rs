use std::sync::atomic::{AtomicU64, Ordering};

use egui::{epaint::PathStroke, Color32, Painter, Pos2, Ui, Vec2};
use geo::{Contains, Intersects, Point, Rect};
use geo_types::Coord;
use h3o::CellIndex;
use walkers::{Position, Projector};

pub struct AtomicF64 {
    storage: AtomicU64,
}
impl AtomicF64 {
    pub fn new(value: f64) -> Self {
        let as_u64 = value.to_bits();
        Self {
            storage: AtomicU64::new(as_u64),
        }
    }
    pub fn store(&self, value: f64, ordering: Ordering) {
        let as_u64 = value.to_bits();
        self.storage.store(as_u64, ordering)
    }
    pub fn load(&self, ordering: Ordering) -> f64 {
        let as_u64 = self.storage.load(ordering);
        f64::from_bits(as_u64)
    }
}

pub fn project_line_string(projector: &Projector, line: &[Coord]) -> Vec<Pos2> {
    line.iter()
        .map(|point| projector.project(Position::new(point.x, point.y)).to_pos2())
        .collect()
}

pub fn display_cell(projector: &Projector, painter: &Painter, cell: CellIndex, color: Color32) {
    let solvent = h3o::geom::SolventBuilder::new().build();
    let cell_polygon = solvent.dissolve(Some(cell)).unwrap();
    let cell_polygon = &cell_polygon.0[0];
    let line = project_line_string(projector, &cell_polygon.exterior().0);

    // Check if cell is at least 1 pixel in size by comparing projected points
    if line.len() >= 2 {
        let p1 = line[0];
        let p2 = line[1];
        let dx = p2.x - p1.x;
        let dy = p2.y - p1.y;
        let size = (dx * dx + dy * dy).sqrt();

        if size >= 1.0 {
            painter.line(
                line,
                PathStroke::new(16.0 - cell.resolution() as u8 as f32, color),
            );
        }
    }
}

/// Draw a cross at the given center position with the specified size and color
pub fn draw_diagonal_cross(painter: &Painter, point: Pos2, color: Color32) {
    let size: f32 = 8.0;
    let stroke_width: f32 = 4.0;
    painter.line(
        vec![point - Vec2::splat(size), point + Vec2::splat(size)],
        PathStroke::new(stroke_width, color),
    );
    painter.line(
        vec![
            point + Vec2::new(size, -size),
            point + Vec2::new(-size, size),
        ],
        PathStroke::new(stroke_width, color),
    );
}

/// Draw an orthogonal cross (plus sign) at the given center position with the specified size and color
pub fn draw_orthogonal_cross(painter: &Painter, point: Pos2, color: Color32) {
    let size: f32 = 8.0;
    let stroke_width: f32 = 4.0;
    // Draw the horizontal line
    painter.line(
        vec![point + Vec2::new(-size, 0.0), point + Vec2::new(size, 0.0)],
        PathStroke::new(stroke_width, color),
    );

    // Draw the vertical line
    painter.line(
        vec![point + Vec2::new(0.0, -size), point + Vec2::new(0.0, size)],
        PathStroke::new(stroke_width, color),
    );
}

/// Draw geometrical shape on map

pub fn draw_geometry_on_map(
    projector: &walkers::Projector,
    displayed_rect: Rect,
    painter: &egui::Painter,
    value: &geojson::Value,
) {
    match value {
        geojson::Value::Point(coords) => {
            let coord = h3o::LatLng::new(coords[1], coords[0]).unwrap();
            if !displayed_rect.contains(&Point::new(coord.lng(), coord.lat())) {
                return;
            }
            let center = projector.project(Position::new(coord.lng(), coord.lat()));
            draw_orthogonal_cross(&painter, center.to_pos2(), Color32::BLACK);
        }
        geojson::Value::MultiPoint(coords) => {
            for coord in coords {
                let coord = h3o::LatLng::new(coord[1], coord[0]).unwrap();
                if !displayed_rect.contains(&Point::new(coord.lng(), coord.lat())) {
                    continue;
                }
                let center = projector.project(Position::new(coord.lng(), coord.lat()));
                draw_orthogonal_cross(&painter, center.to_pos2(), Color32::BLACK);
            }
        }
        geojson::Value::Polygon(coords) => {
            let polygon: geo::Polygon = geojson::Value::Polygon(coords.clone()).try_into().unwrap();

            if polygon.intersects(&displayed_rect) {
                let points: Vec<_> = polygon
                    .exterior()
                    .points()
                    .map(|point| {
                        let pos = projector.project(Position::new(point.x(), point.y()));
                        pos.to_pos2()
                    })
                    .collect();
                painter.line(points, PathStroke::new(4.0, Color32::BLACK));
            }
        }
        geojson::Value::MultiPolygon(coords) => {
            for polygon in coords {
                let polygon= geojson::Value::Polygon(polygon.clone());
                draw_geometry_on_map(projector, displayed_rect, painter, &polygon);
            }
        }
        _ => todo!(),
    }
}

pub fn extract_displayed_rect(ui: &mut Ui, projector: &Projector) -> Rect {
    let x = ui.available_width();
    let y = ui.available_height();
    let top_left = projector.unproject(Vec2 { x: 0.0, y: 0.0 });
    let bottom_right = projector.unproject(Vec2 { x, y });
    Rect::new(
        Point::new(top_left.x(), top_left.y()),
        Point::new(bottom_right.x(), bottom_right.y()),
    )
}
