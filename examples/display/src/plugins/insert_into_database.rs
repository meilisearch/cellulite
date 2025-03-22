use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};

use cellulite::Writer;
use egui::Vec2;
use heed::Env;
use walkers::Plugin;

/// Plugin used to insert position when a right click happens.
#[derive(Clone)]
pub struct InsertIntoDatabase {
    id: Arc<AtomicU32>,
    pub disabled: Arc<AtomicBool>,
    env: Env,
    db: Writer,
}

impl InsertIntoDatabase {
    pub fn new(env: Env, db: Writer) -> Self {
        InsertIntoDatabase {
            id: Arc::default(),
            disabled: Arc::default(),
            env,
            db,
        }
    }

    pub fn insert_random_items(&self, n: usize) {
        let mut wtxn = self.env.write_txn().unwrap();
        for _ in 0..n {
            let lat = rand::random_range(-90.0..=90.0);
            let lng = rand::random_range(-180.0..=180.0);
            let id = self.id.fetch_add(1, Ordering::Relaxed);
            self.db.add_item(&mut wtxn, id, (lat, lng)).unwrap();
        }
        wtxn.commit().unwrap();
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
            let mut wtxn = self.env.write_txn().unwrap();
            let id = self.id.fetch_add(1, Ordering::Relaxed);
            self.db.add_item(&mut wtxn, id, (pos.y(), pos.x())).unwrap();
            wtxn.commit().unwrap();
        }
    }
}
