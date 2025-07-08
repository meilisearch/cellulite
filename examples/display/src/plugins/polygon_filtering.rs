use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use cellulite::FilteringStep;
use egui::{epaint::PathStroke, Color32, RichText, Ui, Vec2};
use egui_double_slider::DoubleSlider;
use geo_types::Coord;
use h3o::Resolution;
use walkers::{Plugin, Position};

use crate::{
    runner::Runner,
    utils::{display_cell, draw_diagonal_cross, project_line_string},
};

use super::{insert_into_database::InsertMode, InsertIntoDatabase};

/// Plugin used to create or delete a polygon used to select a subset of points
#[derive(Clone)]
pub struct PolygonFiltering {
    pub in_creation: Arc<AtomicBool>,
    pub display_filtering_details: Arc<AtomicUsize>,
    pub display_details_min_res: Arc<AtomicUsize>,
    pub display_details_max_res: Arc<AtomicUsize>,
    runner: Runner,
    pub insert_into_database: InsertIntoDatabase,
}

impl PolygonFiltering {
    pub fn new(runner: Runner, insert_into_database: InsertIntoDatabase) -> Self {
        PolygonFiltering {
            runner,
            in_creation: insert_into_database.filtering.clone(),
            display_filtering_details: Arc::default(),
            display_details_min_res: Arc::new(AtomicUsize::new(0)),
            display_details_max_res: Arc::new(AtomicUsize::new(16)),
            insert_into_database,
        }
    }

    pub fn ui(&self, ui: &mut Ui) {
        ui.collapsing(RichText::new("Filter").heading(), |ui| {
            let in_creation = self.in_creation.load(Ordering::Relaxed);
            let no_polygon = self.runner.polygon_filter.lock().len() <= 2;
            #[allow(clippy::collapsible_if)]
            if !in_creation && no_polygon {
                if ui.button("Create polygon").clicked() {
                    self.in_creation.store(true, Ordering::Relaxed);
                    *self.insert_into_database.insert_mode.lock() = InsertMode::Disable;
                }
            } else if !in_creation && !no_polygon {
                if ui.button("Delete polygon").clicked() {
                    self.runner.polygon_filter.lock().clear();
                    *self.runner.filter_stats.lock() = None;
                }
            } else if in_creation && no_polygon {
                if ui.button("Cancel").clicked() {
                    self.in_creation.store(false, Ordering::Relaxed);
                    self.runner.polygon_filter.lock().clear();
                    *self.runner.filter_stats.lock() = None;
                }
                if ui.button("Remove last point").clicked() {
                    self.runner.polygon_filter.lock().pop();
                }
            } else if in_creation {
                if ui.button("Finish").clicked() {
                    self.in_creation.store(false, Ordering::Relaxed);
                    let mut polygon = self.runner.polygon_filter.lock();
                    let first = *polygon.first().unwrap();
                    polygon.push(first);
                    self.runner.wake_up.signal();
                }
                if ui.button("Remove last point").clicked() {
                    self.runner.polygon_filter.lock().pop();
                }
            }
            let stats = self.runner.filter_stats.lock();
            if let Some(stats) = stats.as_ref() {
                ui.heading("Result");
                ui.label(format!("Matched {} points", stats.nb_points_matched));
                ui.label(format!(
                    "Polygon contains {} points",
                    stats.shape_contains_n_points
                ));
                ui.label(format!("[COLD] Processed in {:?}", stats.processed_in_cold));
                ui.label(format!("[HOT] Processed in {:?}", stats.processed_in_hot));
                let mut display_filtering_details =
                    self.display_filtering_details.load(Ordering::Acquire);
                ui.add(
                    egui::Slider::new(
                        &mut display_filtering_details,
                        0..=stats.cell_explored.len(),
                    )
                    .text("Filtering details")
                    .smart_aim(false),
                );
                ui.vertical(|ui| {
                    if ui.small_button("+").clicked() {
                        display_filtering_details += 1;
                    }
                    if ui.small_button("-").clicked() {
                        display_filtering_details -= 1;
                    }
                });
                self.display_filtering_details
                    .store(display_filtering_details, Ordering::Release);

                let mut display_details_min = self.display_details_min_res.load(Ordering::Relaxed);
                let mut display_details_max = self.display_details_max_res.load(Ordering::Relaxed);
                ui.label(format!(
                    "Cells resolution between {display_details_min} and {display_details_max}"
                ));
                ui.add(DoubleSlider::new(
                    &mut display_details_min,
                    &mut display_details_max,
                    Resolution::Zero as usize..=Resolution::Fifteen as usize,
                ));
                self.display_details_min_res
                    .store(display_details_min, Ordering::Relaxed);
                self.display_details_max_res
                    .store(display_details_max, Ordering::Relaxed);
                let polygon = self.runner.polygon_filter.lock();
                ui.label(format!("Coords: {:?}", *polygon));
            }
        });
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
            for shape in self.runner.points_matched.lock().iter() {
                match shape {
                    geojson::Value::Point(coords) => {
                        let pos = projector.project(Position::new(coords[0], coords[1]));
                        draw_diagonal_cross(&painter, pos.to_pos2(), Color32::DARK_GREEN);
                    }
                    geojson::Value::MultiPoint(coords) => {
                        for coord in coords {
                            let pos = projector.project(Position::new(coord[0], coord[1]));
                            draw_diagonal_cross(&painter, pos.to_pos2(), Color32::DARK_GREEN);
                        }
                    }
                    geojson::Value::Polygon(coords) => {
                        if let Some(exterior) = coords.first() {
                            let points: Vec<_> = exterior
                                .iter()
                                .map(|coord| {
                                    let pos = projector.project(Position::new(coord[0], coord[1]));
                                    pos.to_pos2()
                                })
                                .collect();
                            painter.line(points, PathStroke::new(4.0, Color32::DARK_GREEN));
                        }
                    }
                    geojson::Value::MultiPolygon(coords) => {
                        for polygon in coords {
                            let points: Vec<_> = polygon
                                .iter()
                                .flat_map(|exterior| {
                                    exterior.iter().map(|coord| {
                                        let pos = projector.project(Position::new(coord[0], coord[1]));
                                        pos.to_pos2()
                                    })
                                })
                                .collect();
                            painter.line(points, PathStroke::new(4.0, Color32::DARK_GREEN));
                        }
                    }
                    _ => todo!(),
                }
            }

            let display_filtering_details = self.display_filtering_details.load(Ordering::Relaxed);
            let min = self.display_details_min_res.load(Ordering::Relaxed);
            let max = self.display_details_max_res.load(Ordering::Relaxed);
            if display_filtering_details > 0 {
                if let Some(stats) = self.runner.filter_stats.lock().as_ref() {
                    for (action, cell) in stats
                        .cell_explored
                        .iter()
                        .take(display_filtering_details)
                        .filter(|(_, cell)| (min..max).contains(&(cell.resolution() as usize)))
                        .copied()
                    {
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
}
