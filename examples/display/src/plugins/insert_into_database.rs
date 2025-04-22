use std::mem;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use egui::Pos2;
use egui::{mutex::Mutex, RichText, Ui, Vec2};
use geo_types::{Coord, Point};
use walkers::Plugin;

use crate::runner::Runner;

#[derive(Clone, Copy, PartialEq, Default, Debug)]
pub enum InsertMode {
    #[default]
    Disable,
    Point,
    MultiPoint,
    Polygon,
}

/// Plugin used to insert position when a right click happens.
#[derive(Clone)]
pub struct InsertIntoDatabase {
    pub insert_mode: Arc<Mutex<InsertMode>>,
    insert_shape: Arc<Mutex<Vec<Coord>>>,
    runner: Runner,
    pub filtering: Arc<AtomicBool>,
}

impl InsertIntoDatabase {
    pub fn new(runner: Runner) -> Self {
        InsertIntoDatabase {
            insert_mode: Arc::default(),
            insert_shape: Arc::default(),
            runner,
            filtering: Arc::default(),
        }
    }

    pub fn insert_random_items(&self, n: usize) {
        for _ in 0..n {
            let lat = rand::random_range(-90.0..=90.0);
            let lng = rand::random_range(-180.0..=180.0);
            self.runner.add_shape(geojson::Value::Point(vec![lng, lat]));
        }
    }

    pub fn ui(&self, ui: &mut Ui) {
        ui.collapsing(RichText::new("Insert").heading(), |ui| {
            let mut insert_mode = self.insert_mode.lock();

            egui::ComboBox::from_label("Geometry to insert")
                .selected_text(format!("{insert_mode:?}"))
                .show_ui(ui, |ui| {
                    ui.selectable_value(&mut *insert_mode, InsertMode::Disable, "Disable");
                    ui.selectable_value(&mut *insert_mode, InsertMode::Point, "Point");
                    ui.selectable_value(&mut *insert_mode, InsertMode::MultiPoint, "Multipoint");
                    ui.selectable_value(&mut *insert_mode, InsertMode::Polygon, "Polygon");
                });

            match *insert_mode {
                InsertMode::Disable => (),
                InsertMode::Point => {
                    ui.label("Right click to add a point");
                }
                InsertMode::MultiPoint => {
                    let mut points = self.insert_shape.lock();
                    ui.label(format!("Points collected: {}", points.len()));
                    if !points.is_empty() {
                        if ui.button("Complete multipoint").clicked() {
                            let points = mem::take(&mut *points)
                                .into_iter()
                                .map(|coord| vec![coord.x, coord.y])
                                .collect();
                            self.runner.add_shape(geojson::Value::MultiPoint(points));
                        }
                        if ui.button("Clear points").clicked() {
                            points.clear();
                        }
                    }
                }
                InsertMode::Polygon => {
                    let mut points = self.insert_shape.lock();
                    ui.label(format!("Points collected: {}", points.len()));
                    if points.len() >= 3 {
                        if ui.button("Complete polygon").clicked() {
                            let mut polygon_points = points
                                .iter()
                                .map(|coord| vec![coord.x, coord.y])
                                .collect::<Vec<_>>();
                            // Close the polygon by adding the first point at the end
                            polygon_points.push(polygon_points[0].clone());
                            self.runner
                                .add_shape(geojson::Value::Polygon(vec![polygon_points]));
                            points.clear();
                        }
                    }
                    if !points.is_empty() {
                        if ui.button("Clear points").clicked() {
                            points.clear();
                        }
                    }
                }
            }

            if self.filtering.load(Ordering::Relaxed) {
                *insert_mode = InsertMode::Disable;
            }
        });
    }
}

impl Plugin for InsertIntoDatabase {
    fn run(
        self: Box<Self>,
        ui: &mut egui::Ui,
        response: &egui::Response,
        projector: &walkers::Projector,
    ) {
        match *self.insert_mode.lock() {
            InsertMode::MultiPoint => {
                let points = self.insert_shape.lock();
                for point in points.iter() {
                    let center = projector.project(Point::new(point.x, point.y));
                    let size = 8.0;
                    ui.painter().line(
                        vec![
                            (center - Vec2::splat(size)).to_pos2(),
                            (center + Vec2::splat(size)).to_pos2(),
                        ],
                        egui::Stroke::new(4.0, egui::Color32::YELLOW),
                    );
                    ui.painter().line(
                        vec![
                            (center + Vec2::new(size, -size)).to_pos2(),
                            (center + Vec2::new(-size, size)).to_pos2(),
                        ],
                        egui::Stroke::new(4.0, egui::Color32::YELLOW),
                    );
                }
            }
            InsertMode::Polygon => {
                let points = self.insert_shape.lock();
                if points.len() >= 1 {
                    let mut line: Vec<Pos2> = points
                        .iter()
                        .map(|point| projector.project(Point::new(point.x, point.y)).to_pos2())
                        .collect();

                    // Add mouse position or close polygon
                    if let Some(mouse_pos) = response.hover_pos() {
                        line.push(mouse_pos);
                    } else {
                        line.push(line[0]); // Close polygon by adding first point
                    }

                    ui.painter().add(egui::Shape::line(
                        line,
                        egui::Stroke::new(8.0, egui::Color32::YELLOW),
                    ));
                }

                // Draw yellow crosses at each point
                for point in points.iter() {
                    let center = projector.project(Point::new(point.x, point.y));
                    let size = 8.0;
                    ui.painter().line(
                        vec![
                            (center - Vec2::splat(size)).to_pos2(),
                            (center + Vec2::splat(size)).to_pos2(),
                        ],
                        egui::Stroke::new(4.0, egui::Color32::YELLOW),
                    );
                    ui.painter().line(
                        vec![
                            (center + Vec2::new(size, -size)).to_pos2(),
                            (center + Vec2::new(-size, size)).to_pos2(),
                        ],
                        egui::Stroke::new(4.0, egui::Color32::YELLOW),
                    );
                }
            }
            _ => {}
        }
        let Some(pos) = response.hover_pos() else {
            return;
        };

        if response.secondary_clicked() {
            match *self.insert_mode.lock() {
                InsertMode::Disable => (),
                InsertMode::Point => {
                    let pos = projector.unproject(Vec2::new(pos.x, pos.y));
                    self.runner
                        .add_shape(geojson::Value::Point(vec![pos.x(), pos.y()]));
                }
                InsertMode::MultiPoint => {
                    let pos = projector.unproject(Vec2::new(pos.x, pos.y));
                    self.insert_shape.lock().push(Coord {
                        x: pos.x(),
                        y: pos.y(),
                    });
                }
                InsertMode::Polygon => {
                    let pos = projector.unproject(Vec2::new(pos.x, pos.y));
                    self.insert_shape.lock().push(Coord {
                        x: pos.x(),
                        y: pos.y(),
                    });
                }
            }
        }
    }
}
