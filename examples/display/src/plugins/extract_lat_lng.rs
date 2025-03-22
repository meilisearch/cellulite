use std::sync::{atomic::Ordering, Arc};

use egui::Vec2;
use walkers::Plugin;

use crate::utils::AtomicF64;

/// Plugin used to extract the position of the mouse. It's then displayed to the side panel.
#[derive(Clone)]
pub struct ExtractLatLng {
    pub current_lat: Arc<AtomicF64>,
    pub current_lng: Arc<AtomicF64>,

    pub clicked_lat: Arc<AtomicF64>,
    pub clicked_lng: Arc<AtomicF64>,
}

impl Default for ExtractLatLng {
    fn default() -> Self {
        ExtractLatLng {
            current_lat: Arc::new(AtomicF64::new(0.0)),
            current_lng: Arc::new(AtomicF64::new(0.0)),
            clicked_lat: Arc::new(AtomicF64::new(0.0)),
            clicked_lng: Arc::new(AtomicF64::new(0.0)),
        }
    }
}

impl Plugin for ExtractLatLng {
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
