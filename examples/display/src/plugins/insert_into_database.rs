use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use egui::{mutex::Mutex, RichText, Ui, Vec2};
use geo::Coord;
use h3o::LatLng;
use walkers::Plugin;

use crate::runner::Runner;

#[derive(Clone, Copy, PartialEq, Default, Debug)]
pub enum InsertMode {
    #[default]
    Disable,
    Point,
    MultiPoint,
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
                });
            if self.filtering.load(Ordering::Relaxed) {
                *insert_mode = InsertMode::Disable;
            }
        });
    }
}

impl Plugin for InsertIntoDatabase {
    fn run(
        self: Box<Self>,
        _ui: &mut egui::Ui,
        response: &egui::Response,
        projector: &walkers::Projector,
    ) {
        let Some(pos) = response.hover_pos() else {
            return;
        };

        if response.secondary_clicked() {
            match *self.insert_mode.lock() {
                InsertMode::Disable => (),
                InsertMode::Point => {
                    let pos = projector.unproject(Vec2::new(pos.x, pos.y));
                    self.runner.add_shape(geojson::Value::Point(vec![pos.x(), pos.y()]));
                }
                InsertMode::MultiPoint => todo!(),
            }
        }
    }
}
