use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use cellulite::Writer;
use egui::{epaint::PathStroke, Color32, Vec2};
use h3o::LatLng;
use heed::Env;
use walkers::{Plugin, Position};

/// Plugin used to display the cells
#[derive(Clone)]
pub struct DisplayDbContent {
    pub display_db_cells: Arc<AtomicBool>,
    pub display_items: Arc<AtomicBool>,
    env: Env,
    db: Writer,
}

impl DisplayDbContent {
    pub fn new(env: Env, db: Writer) -> Self {
        DisplayDbContent {
            display_db_cells: Arc::new(AtomicBool::new(true)),
            display_items: Arc::new(AtomicBool::new(true)),
            env,
            db,
        }
    }
}

impl Plugin for DisplayDbContent {
    fn run(
        self: Box<Self>,
        ui: &mut egui::Ui,
        _response: &egui::Response,
        projector: &walkers::Projector,
    ) {
        let painter = ui.painter();
        let rtxn = self.env.read_txn().unwrap();

        if self.display_db_cells.load(Ordering::Relaxed) {
            for entry in self.db.inner_db_cells(&rtxn).unwrap() {
                let (cell, bitmap) = entry.unwrap();
                let polygon = h3o::geom::dissolve(Some(cell)).unwrap().0;
                let lines = polygon[0]
                    .exterior()
                    .0
                    .iter()
                    .map(|point| projector.project(Position::new(point.x, point.y)).to_pos2())
                    .collect();
                painter.line(
                    lines,
                    PathStroke::new(
                        16.0 - cell.resolution() as u8 as f32,
                        Color32::BLUE.lerp_to_gamma(
                            Color32::RED,
                            bitmap.len() as f32 / self.db.threshold as f32,
                        ),
                    ),
                );
            }
        }

        if self.display_items.load(Ordering::Relaxed) {
            for entry in self.db.items(&rtxn).unwrap() {
                let (_item_id, cell) = entry.unwrap();
                let lat_lng = LatLng::from(cell);
                let center = projector.project(Position::new(lat_lng.lng(), lat_lng.lat()));
                let size = 8.0;
                painter.line(
                    vec![
                        (center - Vec2::splat(size)).to_pos2(),
                        (center + Vec2::splat(size)).to_pos2(),
                    ],
                    PathStroke::new(4.0, Color32::BLACK),
                );
                painter.line(
                    vec![
                        (center + Vec2::new(size, -size)).to_pos2(),
                        (center + Vec2::new(-size, size)).to_pos2(),
                    ],
                    PathStroke::new(4.0, Color32::BLACK),
                );
            }
        }
    }
}
