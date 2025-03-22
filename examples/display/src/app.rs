use std::sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    Arc,
};

use cellulite::{Database, Writer};
use egui::{epaint::PathStroke, mutex::Mutex, CentralPanel, Color32, Pos2, Vec2};
use geo_types::{Coord, LineString, Polygon};
use h3o::LatLng;
use heed::{Env, EnvOpenOptions};
use tempfile::TempDir;
use walkers::{lon_lat, sources::OpenStreetMap, HttpTiles, Map, MapMemory, Plugin, Position};

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
    temp_dir: tempfile::TempDir,

    // Plugins
    extract_lat_lng: ExtractLatLng,
    insert_into_database: InsertIntoDatabase,
    display_db_content: DisplayDbCells,
    polygon_filtering: PolygonFiltering,
}

impl App {
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

        let insert_into_database = InsertIntoDatabase::new(env.clone(), db.clone());
        let mut polygon_filtering = PolygonFiltering::new(env.clone(), db.clone());
        polygon_filtering.in_creation = insert_into_database.disabled.clone();

        Self {
            tiles: HttpTiles::new(OpenStreetMap, cc.egui_ctx.clone()),
            map_memory: MapMemory::default(),
            extract_lat_lng: ExtractLatLng::default(),
            insert_into_database,
            display_db_content: DisplayDbCells::new(env.clone(), db.clone()),
            polygon_filtering,
            env,
            db,
            temp_dir,
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
            let no_polygon = self.polygon_filtering.polygon_points.lock().len() <= 2;
            #[allow(clippy::collapsible_if)]
            if !in_creation && no_polygon {
                if ui.button("Create polygon").clicked() {
                    self.polygon_filtering
                        .in_creation
                        .store(true, Ordering::Relaxed);
                }
            } else if !in_creation && !no_polygon {
                if ui.button("Deletet polygon").clicked() {
                    self.polygon_filtering.polygon_points.lock().clear();
                }
            } else if in_creation && no_polygon {
                if ui.button("Cancel").clicked() {
                    self.polygon_filtering
                        .in_creation
                        .store(false, Ordering::Relaxed);
                    self.polygon_filtering.polygon_points.lock().clear();
                }
                if ui.button("Remove last point").clicked() {
                    self.polygon_filtering.polygon_points.lock().pop();
                }
            } else if in_creation {
                if ui.button("Finish").clicked() {
                    self.polygon_filtering
                        .in_creation
                        .store(false, Ordering::Relaxed);
                    let mut polygon = self.polygon_filtering.polygon_points.lock();
                    let first = *polygon.first().unwrap();
                    polygon.push(first);
                }
                if ui.button("Remove last point").clicked() {
                    self.polygon_filtering.polygon_points.lock().pop();
                }
            }
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

/// Plugin used to create or delete a polygon used to select a subset of points
#[derive(Clone)]
struct PolygonFiltering {
    polygon_points: Arc<Mutex<Vec<Coord<f32>>>>,
    in_creation: Arc<AtomicBool>,
    env: Env,
    db: Writer,
}

impl PolygonFiltering {
    fn new(env: Env, db: Writer) -> Self {
        PolygonFiltering {
            polygon_points: Arc::default(),
            in_creation: Arc::default(),
            env,
            db,
        }
    }
}

impl Plugin for PolygonFiltering {
    fn run(
        self: Box<Self>,
        ui: &mut egui::Ui,
        response: &egui::Response,
        projector: &walkers::Projector,
    ) {
        let painter = ui.painter();
        let mut line = self.polygon_points.lock();
        let in_creation = self.in_creation.load(Ordering::Relaxed);
        let mut to_display = line.clone();

        let color = if in_creation {
            if let Some(pos) = response.hover_pos() {
                let pos = projector.unproject(Vec2::new(pos.x, pos.y));
                let coord = Coord {
                    x: pos.x() as f32,
                    y: pos.y() as f32,
                };
                if response.secondary_clicked() {
                    line.push(coord);
                }
                to_display.push(coord);
            } else if line.len() >= 2 {
                let first = *line.first().unwrap();
                to_display.push(first);
            }

            Color32::YELLOW
        } else {
            Color32::GREEN
        };

        let line = to_display
            .iter()
            .map(|point| {
                projector
                    .project(Position::new(point.x as f64, point.y as f64))
                    .to_pos2()
            })
            .collect();

        painter.line(line, PathStroke::new(8.0, color));

        // If we have a polygon + it's finished we retrieve the points it contains and display them
        if to_display.len() >= 3 && !in_creation {
            let polygon = Polygon::new(
                LineString(
                    to_display
                        .into_iter()
                        .map(|coord| Coord {
                            x: coord.x as f64,
                            y: coord.y as f64,
                        })
                        .collect(),
                ),
                Vec::new(),
            );
            let rtxn = self.env.read_txn().unwrap();
            let results = self.db.in_shape(&rtxn, polygon).unwrap();

            let size = 8.0;
            for item in results {
                let (lat, lng) = self.db.item(&rtxn, item).unwrap().unwrap();
                let pos = projector.project(Position::new(lng, lat));

                painter.line(
                    vec![
                        Pos2 {
                            x: pos.x,
                            y: pos.y - size,
                        },
                        Pos2 {
                            x: pos.x,
                            y: pos.y + size,
                        },
                    ],
                    PathStroke::new(4.0, Color32::GREEN),
                );

                painter.line(
                    vec![
                        Pos2 {
                            x: pos.x - size,
                            y: pos.y,
                        },
                        Pos2 {
                            x: pos.x + size,
                            y: pos.y,
                        },
                    ],
                    PathStroke::new(4.0, Color32::GREEN),
                );
            }
        }
    }
}

/// Plugin used to display the cells
#[derive(Clone)]
struct DisplayDbCells {
    display_db_cells: Arc<AtomicBool>,
    display_items: Arc<AtomicBool>,
    env: Env,
    db: Writer,
}

impl DisplayDbCells {
    fn new(env: Env, db: Writer) -> Self {
        DisplayDbCells {
            display_db_cells: Arc::new(AtomicBool::new(true)),
            display_items: Arc::new(AtomicBool::new(true)),
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

        if self.display_db_cells.load(Ordering::Relaxed) {
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
                        Color32::BLUE.lerp_to_gamma(
                            Color32::RED,
                            bitmap.len() as f32 / self.db.threshold as f32,
                        ),
                    ),
                );
            }
        }

        if self.display_db_cells.load(Ordering::Relaxed) {
            for entry in self.db.items(&rtxn).unwrap() {
                let (_item_id, cell) = entry.unwrap();
                let lat_lng = LatLng::from(cell);
                let center = projector.project(Position::new(lat_lng.lng(), lat_lng.lat()));
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

/// Plugin used to insert position when a right click happens.
#[derive(Clone)]
struct InsertIntoDatabase {
    id: Arc<AtomicU32>,
    disabled: Arc<AtomicBool>,
    env: Env,
    db: Writer,
}

impl InsertIntoDatabase {
    fn new(env: Env, db: Writer) -> Self {
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
