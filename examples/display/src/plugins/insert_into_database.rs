use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use egui::Vec2;
use h3o::LatLng;
use walkers::Plugin;

use crate::runner::Runner;

/// Plugin used to insert position when a right click happens.
#[derive(Clone)]
pub struct InsertIntoDatabase {
    pub disabled: Arc<AtomicBool>,
    runner: Runner,
}

impl InsertIntoDatabase {
    pub fn new(runner: Runner) -> Self {
        InsertIntoDatabase {
            disabled: Arc::default(),
            runner,
        }
    }

    pub fn insert_random_items(&self, n: usize) {
        for _ in 0..n {
            let lat = rand::random_range(-90.0..=90.0);
            let lng = rand::random_range(-180.0..=180.0);
            self.runner.add_point(LatLng::new(lat, lng).unwrap());
        }
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
        if !self.disabled.load(Ordering::Relaxed) && response.secondary_clicked() {
            let pos = projector.unproject(Vec2::new(pos.x, pos.y));
            self.runner
                .add_point(LatLng::new(pos.y(), pos.x()).unwrap());
        }
    }
}
