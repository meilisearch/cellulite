use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc,
};

use cellulite::{Database, Writer};
use egui::{epaint::PathStroke, CentralPanel, Color32, Vec2};
use heed::{Env, EnvOpenOptions};
use tempfile::TempDir;
use walkers::{lon_lat, sources::OpenStreetMap, HttpTiles, Map, MapMemory, Plugin, Position};

pub struct TemplateApp {
    // Map
    tiles: HttpTiles,
    map_memory: MapMemory,

    // Database
    env: Env,
    db: Writer,
    temp_dir: tempfile::TempDir,

    // Plugins
    extract_lat_lng: ExtractLatLng,
    insert_into_database: InsertIntoDatabase,
    display_db_cells: DisplayDbCells,
}

impl TemplateApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let temp_dir = TempDir::new().unwrap();

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(200 * 1024 * 1024)
                .open(temp_dir.path())
        }
        .unwrap();
        let mut wtxn = env.write_txn().unwrap();
        let database: Database = env.create_database(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();
        let mut db = Writer::new(database);
        db.threshold = 4;

        Self {
            tiles: HttpTiles::new(OpenStreetMap, cc.egui_ctx.clone()),
            map_memory: MapMemory::default(),
            extract_lat_lng: ExtractLatLng::default(),
            insert_into_database: InsertIntoDatabase::new(env.clone(), db.clone()),
            display_db_cells: DisplayDbCells::new(env.clone(), db.clone()),
            env,
            db,
            temp_dir,
        }
    }
}

impl eframe::App for TemplateApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                let is_web = cfg!(target_arch = "wasm32");
                if !is_web {
                    ui.menu_button("File", |ui| {
                        if ui.button("Quit").clicked() {
                            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                        }
                    });
                    ui.add_space(16.0);
                }

                egui::widgets::global_theme_preference_buttons(ui);
            });
        });
        egui::SidePanel::right("side_panel").show(ctx, |ui| {
            ui.label(format!(
                "mouse: {:.5},{:.5}",
                self.extract_lat_lng.current_lat.load(Ordering::Relaxed),
                self.extract_lat_lng.current_lng.load(Ordering::Relaxed)
            ));
            ui.label(format!(
                "clicked: {:.5},{:.5}",
                self.extract_lat_lng.clicked_lat.load(Ordering::Relaxed),
                self.extract_lat_lng.clicked_lng.load(Ordering::Relaxed)
            ));
        });

        CentralPanel::default().show(ctx, |ui| {
            ui.add(
                Map::new(
                    Some(&mut self.tiles),
                    &mut self.map_memory,
                    lon_lat(3.60698, 43.99155),
                )
                .with_plugin(self.extract_lat_lng.clone())
                .with_plugin(self.insert_into_database.clone())
                .with_plugin(self.display_db_cells.clone())
                .zoom_speed(1.0),
            );
        });
    }
}

/// Plugin used to display the cells
#[derive(Clone)]
struct DisplayDbCells {
    display: Arc<bool>,
    env: Env,
    db: Writer,
}

impl DisplayDbCells {
    fn new(env: Env, db: Writer) -> Self {
        DisplayDbCells {
            display: Arc::default(),
            env,
            db,
        }
    }
}

impl Plugin for DisplayDbCells {
    fn run(
        self: Box<Self>,
        ui: &mut egui::Ui,
        _response: &egui::Response,
        projector: &walkers::Projector,
    ) {
        let painter = ui.painter();
        let rtxn = self.env.read_txn().unwrap();

        for entry in self.db.inner_db_cells(&rtxn).unwrap() {
            let (cell, bitmap) = entry.unwrap();
            let polygon = h3o::geom::dissolve(Some(cell)).unwrap().0;
            let lines = polygon[0]
                .exterior()
                .0
                .iter()
                .map(|point| projector.project(Position::new(point.x, point.y)).to_pos2())
                .collect();
            painter.line(
                lines,
                PathStroke::new(
                    16.0 - cell.resolution() as u8 as f32,
                    Color32::BLUE.lerp_to_gamma(Color32::RED, bitmap.len() as f32 / 4.0),
                ),
            );
        }
    }
}

/// Plugin used to insert position when a right click happens.
#[derive(Clone)]
struct InsertIntoDatabase {
    id: Arc<AtomicU32>,
    env: Env,
    db: Writer,
}

impl InsertIntoDatabase {
    fn new(env: Env, db: Writer) -> Self {
        InsertIntoDatabase {
            id: Arc::default(),
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
        if response.secondary_clicked() {
            let pos = projector.unproject(Vec2::new(pos.x, pos.y));
            let mut wtxn = self.env.write_txn().unwrap();
            let id = self.id.fetch_add(1, Ordering::Relaxed);
            self.db.add_item(&mut wtxn, id, (pos.y(), pos.x())).unwrap();
            wtxn.commit().unwrap();
        }
    }
}

/// Plugin used to extract the position of the mouse. It's then displayed to the side panel.
#[derive(Clone)]
struct ExtractLatLng {
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

pub struct AtomicF64 {
    storage: AtomicU64,
}
impl AtomicF64 {
    pub fn new(value: f64) -> Self {
        let as_u64 = value.to_bits();
        Self {
            storage: AtomicU64::new(as_u64),
        }
    }
    pub fn store(&self, value: f64, ordering: Ordering) {
        let as_u64 = value.to_bits();
        self.storage.store(as_u64, ordering)
    }
    pub fn load(&self, ordering: Ordering) -> f64 {
        let as_u64 = self.storage.load(ordering);
        f64::from_bits(as_u64)
    }
}
