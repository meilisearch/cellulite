use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use egui::{epaint::PathStroke, Color32, Ui, Vec2};
use egui_double_slider::DoubleSlider;
use geo::{Contains, Point, Rect};
use h3o::Resolution;
use walkers::{Plugin, Position};

use crate::{runner::Runner, utils::display_cell};

/// Plugin used to display the cells
#[derive(Clone)]
pub struct DisplayDbContent {
    pub display_db_cells: Arc<AtomicBool>,
    pub display_db_cells_min_res: Arc<AtomicUsize>,
    pub display_db_cells_max_res: Arc<AtomicUsize>,
    pub display_items: Arc<AtomicBool>,
    runner: Runner,
}

impl DisplayDbContent {
    pub fn new(runner: Runner) -> Self {
        DisplayDbContent {
            display_db_cells: Arc::default(),
            display_db_cells_min_res: Arc::new(AtomicUsize::from(0)),
            display_db_cells_max_res: Arc::new(AtomicUsize::from(16)),
            display_items: Arc::default(),
            runner,
        }
    }

    pub fn ui(&self, ui: &mut Ui) {
        let mut display_items = self.display_items.load(Ordering::Relaxed);
        if ui
            .toggle_value(&mut display_items, "Display items")
            .clicked()
        {
            self.display_items.store(display_items, Ordering::Relaxed);
        }
        let mut display_db_cells = self.display_db_cells.load(Ordering::Relaxed);
        if ui
            .toggle_value(&mut display_db_cells, "Display DB cells")
            .clicked()
        {
            self.display_db_cells
                .store(display_db_cells, Ordering::Relaxed);
        }
        if display_db_cells {
            let mut display_db_cells_min = self.display_db_cells_min_res.load(Ordering::Relaxed);
            let mut display_db_cells_max = self.display_db_cells_max_res.load(Ordering::Relaxed);
            ui.label(format!(
                "Cells resolution between {display_db_cells_min} and {display_db_cells_max}"
            ));
            ui.add(DoubleSlider::new(
                &mut display_db_cells_min,
                &mut display_db_cells_max,
                Resolution::Zero as usize..=Resolution::Fifteen as usize,
            ));
            self.display_db_cells_min_res
                .store(display_db_cells_min, Ordering::Relaxed);
            self.display_db_cells_max_res
                .store(display_db_cells_max, Ordering::Relaxed);
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
        let x = ui.available_width();
        let y = ui.available_height();
        let top_left = projector.unproject(Vec2 { x: 0.0, y: 0.0 });
        let bottom_right = projector.unproject(Vec2 { x, y });
        let displayed_rect = Rect::new(
            Point::new(top_left.x(), top_left.y()),
            Point::new(bottom_right.x(), bottom_right.y()),
        );

        let painter = ui.painter();

        if self.display_db_cells.load(Ordering::Relaxed) {
            let min = self.display_db_cells_min_res.load(Ordering::Relaxed);
            let max = self.display_db_cells_max_res.load(Ordering::Relaxed);

            for (cell, nb_points) in self.runner.all_db_cells.lock().iter().copied() {
                if (min..max).contains(&(cell.resolution() as usize)) {
                    display_cell(
                        projector,
                        painter,
                        cell,
                        Color32::BLUE.lerp_to_gamma(
                            Color32::RED,
                            nb_points as f32 / self.runner.db.threshold as f32,
                        ),
                    );
                }
            }
        }

        if self.display_items.load(Ordering::Relaxed) {
            for coord in self.runner.all_items.lock().iter().copied() {
                if !displayed_rect.contains(&Point::new(coord.lng(), coord.lat())) {
                    continue;
                }
                let center = projector.project(Position::new(coord.lng(), coord.lat()));
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
