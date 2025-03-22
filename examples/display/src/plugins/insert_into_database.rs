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
