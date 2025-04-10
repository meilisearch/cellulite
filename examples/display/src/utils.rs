use std::sync::atomic::{AtomicU64, Ordering};

use cellulite::geom::bounding_box;
use egui::{epaint::PathStroke, Color32, Painter, Pos2};
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

pub fn display_cell(
    projector: &Projector,
    painter: &Painter,
    cell: CellIndex,
    color: Color32,
    as_bounding_box: bool,
) {
    let cell_polygon = if as_bounding_box {
        bounding_box(cell)
    } else {
        let solvent = h3o::geom::SolventBuilder::new().build();
        solvent.dissolve(Some(cell)).unwrap()
    };
    let cell_polygon = &cell_polygon.0[0];
    let line = project_line_string(projector, &cell_polygon.exterior().0);
    painter.line(
        line,
        PathStroke::new(16.0 - cell.resolution() as u8 as f32, color),
    );
}
