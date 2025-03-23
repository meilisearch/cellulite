use std::sync::atomic::Ordering;

use cellulite::{Database, Writer};
use egui::{CentralPanel, Ui};
use heed::{Env, EnvOpenOptions};
use tempfile::TempDir;
use walkers::{lon_lat, sources::OpenStreetMap, HttpTiles, Map, MapMemory};

use crate::{plugins, runner::Runner};

pub struct App {
    // Map
    tiles: HttpTiles,
    map_memory: MapMemory,

    // Database
    #[allow(dead_code)]
    env: Env,
    #[allow(dead_code)]
    db: Writer,
    #[allow(dead_code)]
    temp_dir: Option<tempfile::TempDir>,

    runner: Runner,

    // Plugins
    extract_lat_lng: plugins::ExtractLatLng,
    insert_into_database: plugins::InsertIntoDatabase,
    display_db_content: plugins::DisplayDbContent,
    polygon_filtering: plugins::PolygonFiltering,
}

impl App {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let (temp_dir, path) = match std::env::args().nth(1) {
            None => {
                let temp_dir = TempDir::new().unwrap();
                let path = temp_dir.path().to_str().unwrap().to_string();
                (Some(temp_dir), path)
            }
            Some(path) => (None, path),
        };

        let env = unsafe { EnvOpenOptions::new().map_size(200 * 1024 * 1024).open(path) }.unwrap();
        let mut wtxn = env.write_txn().unwrap();
        let database: Database = env.create_database(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();
        let db = Writer::new(database);
        // db.threshold = 4;

        let runner = Runner::new(env.clone(), db.clone());
        let insert_into_database = plugins::InsertIntoDatabase::new(runner.clone());
        let mut polygon_filtering = plugins::PolygonFiltering::new(runner.clone());
        polygon_filtering.in_creation = insert_into_database.disabled.clone();

        Self {
            tiles: HttpTiles::new(OpenStreetMap, cc.egui_ctx.clone()),
            map_memory: MapMemory::default(),
            extract_lat_lng: plugins::ExtractLatLng::default(),
            insert_into_database,
            display_db_content: plugins::DisplayDbContent::new(runner.clone()),
            polygon_filtering,
            env,
            db,
            temp_dir,
            runner,
        }
    }

    pub fn side_panel(&self, ui: &mut Ui) {
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
        if ui.button("Insert a shit ton of random points").clicked() {
            self.insert_into_database.insert_random_items(1000);
        }
        let mut display_items = self
            .display_db_content
            .display_items
            .load(Ordering::Relaxed);
        if ui
            .toggle_value(&mut display_items, "Display items")
            .clicked()
        {
            self.display_db_content
                .display_items
                .store(display_items, Ordering::Relaxed);
        }
        let mut display_db_cells = self
            .display_db_content
            .display_db_cells
            .load(Ordering::Relaxed);
        if ui
            .toggle_value(&mut display_db_cells, "Display DB cells")
            .clicked()
        {
            self.display_db_content
                .display_db_cells
                .store(display_db_cells, Ordering::Relaxed);
        }
        let in_creation = self.polygon_filtering.in_creation.load(Ordering::Relaxed);
        let no_polygon = self.runner.polygon_filter.lock().len() <= 2;
        #[allow(clippy::collapsible_if)]
        if !in_creation && no_polygon {
            if ui.button("Create polygon").clicked() {
                self.polygon_filtering
                    .in_creation
                    .store(true, Ordering::Relaxed);
            }
        } else if !in_creation && !no_polygon {
            if ui.button("Deletet polygon").clicked() {
                self.runner.polygon_filter.lock().clear();
            }
        } else if in_creation && no_polygon {
            if ui.button("Cancel").clicked() {
                self.polygon_filtering
                    .in_creation
                    .store(false, Ordering::Relaxed);
                self.runner.polygon_filter.lock().clear();
            }
            if ui.button("Remove last point").clicked() {
                self.runner.polygon_filter.lock().pop();
            }
        } else if in_creation {
            if ui.button("Finish").clicked() {
                self.polygon_filtering
                    .in_creation
                    .store(false, Ordering::Relaxed);
                let mut polygon = self.runner.polygon_filter.lock();
                let first = *polygon.first().unwrap();
                polygon.push(first);
                self.runner.wake_up.signal();
            }
            if ui.button("Remove last point").clicked() {
                self.runner.polygon_filter.lock().pop();
            }
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    if ui.button("Quit").clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
                ui.add_space(16.0);

                egui::widgets::global_theme_preference_buttons(ui);
            });
        });
        egui::SidePanel::right("side_panel").show(ctx, |ui| {
            self.side_panel(ui);
        });

        CentralPanel::default().show(ctx, |ui| {
            ui.add(
                Map::new(
                    Some(&mut self.tiles),
                    &mut self.map_memory,
                    lon_lat(3.60698, 43.99155), // best city ever
                )
                .with_plugin(self.extract_lat_lng.clone())
                .with_plugin(self.insert_into_database.clone())
                .with_plugin(self.display_db_content.clone())
                .with_plugin(self.polygon_filtering.clone())
                .zoom_speed(0.5),
            );
        });
    }
}
