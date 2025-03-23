use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use cellulite::FilteringStep;
use egui::{epaint::PathStroke, Color32, Pos2, Vec2};
use geo_types::{Coord, LineString, Polygon};
use walkers::{Plugin, Position};

use crate::{
    runner::Runner,
    utils::{display_cell, project_line_string},
};

/// Plugin used to create or delete a polygon used to select a subset of points
#[derive(Clone)]
pub struct PolygonFiltering {
    pub in_creation: Arc<AtomicBool>,
    runner: Runner,
}

impl PolygonFiltering {
    pub fn new(runner: Runner) -> Self {
        PolygonFiltering {
            runner,
            in_creation: Arc::default(),
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
        let mut line = self.runner.polygon_filter.lock();
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
                    line.push(Coord {
                        x: coord.x as f64,
                        y: coord.y as f64,
                    });
                }
                to_display.push(Coord {
                    x: coord.x as f64,
                    y: coord.y as f64,
                });
            } else if line.len() >= 2 {
                let first = *line.first().unwrap();
                to_display.push(first);
            }

            Color32::YELLOW
        } else {
            Color32::GREEN
        };

        let line = project_line_string(projector, &to_display);

        painter.line(line, PathStroke::new(8.0, color));

        // If we have a polygon + it's finished we retrieve the points it contains and display them
        if to_display.len() >= 3 && !in_creation {
            let polygon = Polygon::new(LineString(to_display), Vec::new());

            let size = 8.0;
            for (lat, lng) in self.runner.points_matched.lock().iter().copied() {
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
                    PathStroke::new(4.0, Color32::DARK_GREEN),
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
                    PathStroke::new(4.0, Color32::DARK_GREEN),
                );
            }

            if let Some(stats) = self.runner.filter_stats.lock().as_ref() {
                for (action, cell) in stats.cell_explored.iter().copied() {
                    let color = match action {
                        FilteringStep::NotPresentInDB => Color32::BLACK,
                        FilteringStep::OutsideOfShape => Color32::RED,
                        FilteringStep::Returned => Color32::GREEN,
                        FilteringStep::RequireDoubleCheck => Color32::YELLOW,
                        FilteringStep::DeepDive => Color32::BLUE,
                    };
                    display_cell(projector, painter, cell, color);
                }
            }
        }
    }
}
