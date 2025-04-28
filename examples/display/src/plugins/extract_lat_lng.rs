use std::sync::{atomic::Ordering, Arc};

use egui::{Ui, Vec2};
use h3o::{LatLng, Resolution};
use walkers::Plugin;

use crate::utils::AtomicF64;

/// Plugin used to extract the position of the mouse. It's then displayed to the side panel.
#[derive(Clone)]
pub struct ExtractMousePos {
    pub current_lat: Arc<AtomicF64>,
    pub current_lng: Arc<AtomicF64>,

    pub clicked_lat: Arc<AtomicF64>,
    pub clicked_lng: Arc<AtomicF64>,
}

impl Default for ExtractMousePos {
    fn default() -> Self {
        ExtractMousePos {
            current_lat: Arc::new(AtomicF64::new(0.0)),
            current_lng: Arc::new(AtomicF64::new(0.0)),
            clicked_lat: Arc::new(AtomicF64::new(0.0)),
            clicked_lng: Arc::new(AtomicF64::new(0.0)),
        }
    }
}

impl ExtractMousePos {
    pub fn ui(&self, ui: &mut Ui) {
        ui.label(format!(
            "mouse: {:.5},{:.5}",
            self.current_lat.load(Ordering::Relaxed),
            self.current_lng.load(Ordering::Relaxed)
        ));
        let lat = self.clicked_lat.load(Ordering::Relaxed);
        let lng = self.clicked_lng.load(Ordering::Relaxed);
        ui.label(format!("Last click: {lat:.5},{lng:.5}",));
        ui.collapsing("H3 cells at the last click position", |ui| {
            let lat_lng = LatLng::new(lat, lng).unwrap();
            for res in Resolution::range(Resolution::Zero, Resolution::Fifteen) {
                let cell = lat_lng.to_cell(res);
                let cell = u64::from(cell);
                ui.hyperlink_to(
                    format!("Resolution {res}: {cell:x}"),
                    format!("https://h3geo.org/#hex={cell:x}"),
                );
            }
        });
    }
}

impl Plugin for ExtractMousePos {
    fn run(
        self: Box<Self>,
        _ui: &mut egui::Ui,
        response: &egui::Response,
        projector: &walkers::Projector,
    ) {
        if let Some(pos) = response.hover_pos() {
            let pos = projector.unproject(Vec2::new(pos.x, pos.y));
            self.current_lat.store(pos.y(), Ordering::Relaxed);
            self.current_lng.store(pos.x(), Ordering::Relaxed);
        }

        if response.clicked() {
            self.clicked_lat
                .store(self.current_lat.load(Ordering::Relaxed), Ordering::Relaxed);
            self.clicked_lng
                .store(self.current_lng.load(Ordering::Relaxed), Ordering::Relaxed);
        }
    }
}
