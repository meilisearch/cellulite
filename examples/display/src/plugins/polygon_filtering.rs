use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use cellulite::Writer;
use egui::{epaint::PathStroke, mutex::Mutex, Color32, Pos2, Vec2};
use geo_types::{Coord, LineString, Polygon};
use heed::Env;
use walkers::{Plugin, Position};

/// Plugin used to create or delete a polygon used to select a subset of points
#[derive(Clone)]
pub struct PolygonFiltering {
    pub polygon_points: Arc<Mutex<Vec<Coord<f32>>>>,
    pub in_creation: Arc<AtomicBool>,
    env: Env,
    db: Writer,
}

impl PolygonFiltering {
    pub fn new(env: Env, db: Writer) -> Self {
        PolygonFiltering {
            polygon_points: Arc::default(),
            in_creation: Arc::default(),
            env,
            db,
        }
    }
}

impl Plugin for PolygonFiltering {
    fn run(
        self: Box<Self>,
        ui: &mut egui::Ui,
        response: &egui::Response,
        projector: &walkers::Projector,
    ) {
        let painter = ui.painter();
        let mut line = self.polygon_points.lock();
        let in_creation = self.in_creation.load(Ordering::Relaxed);
        let mut to_display = line.clone();

        let color = if in_creation {
            if let Some(pos) = response.hover_pos() {
                let pos = projector.unproject(Vec2::new(pos.x, pos.y));
                let coord = Coord {
                    x: pos.x() as f32,
                    y: pos.y() as f32,
                };
                if response.secondary_clicked() {
                    line.push(coord);
                }
                to_display.push(coord);
            } else if line.len() >= 2 {
                let first = *line.first().unwrap();
                to_display.push(first);
            }

            Color32::YELLOW
        } else {
            Color32::GREEN
        };

        let line = to_display
            .iter()
            .map(|point| {
                projector
                    .project(Position::new(point.x as f64, point.y as f64))
                    .to_pos2()
            })
            .collect();

        painter.line(line, PathStroke::new(8.0, color));

        // If we have a polygon + it's finished we retrieve the points it contains and display them
        if to_display.len() >= 3 && !in_creation {
            let polygon = Polygon::new(
                LineString(
                    to_display
                        .into_iter()
                        .map(|coord| Coord {
                            x: coord.x as f64,
                            y: coord.y as f64,
                        })
                        .collect(),
                ),
                Vec::new(),
            );
            let rtxn = self.env.read_txn().unwrap();
            let results = self.db.in_shape(&rtxn, polygon).unwrap();

            let size = 8.0;
            for item in results {
                let (lat, lng) = self.db.item(&rtxn, item).unwrap().unwrap();
                let pos = projector.project(Position::new(lng, lat));

                painter.line(
                    vec![
                        Pos2 {
                            x: pos.x,
                            y: pos.y - size,
                        },
                        Pos2 {
                            x: pos.x,
                            y: pos.y + size,
                        },
                    ],
                    PathStroke::new(4.0, Color32::GREEN),
                );

                painter.line(
                    vec![
                        Pos2 {
                            x: pos.x - size,
                            y: pos.y,
                        },
                        Pos2 {
                            x: pos.x + size,
                            y: pos.y,
                        },
                    ],
                    PathStroke::new(4.0, Color32::GREEN),
                );
            }
        }
    }
}
