use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use cellulite::{FilteringStep, Stats, Writer};
use egui::mutex::Mutex;
use geo_types::{Coord, LineString, Polygon};
use geojson::{GeoJson, Value};
use h3o::{CellIndex, LatLng};
use heed::Env;

#[derive(Clone)]
pub struct Runner {
    env: Env,
    pub db: Writer,
    pub wake_up: Arc<synchronoise::SignalEvent>,

    // Communication input
    pub to_insert: Arc<Mutex<Vec<LatLng>>>,
    pub polygon_filter: Arc<Mutex<Vec<Coord<f64>>>>,

    // Communication output
    pub stats: Arc<Mutex<Stats>>,
    pub filter_stats: Arc<Mutex<Option<FilterStats>>>,
    pub points_matched: Arc<Mutex<Vec<(f64, f64)>>>,

    // Current state of the DB
    last_id: Arc<AtomicU32>,
    pub all_items: Arc<Mutex<Vec<LatLng>>>,
    pub all_db_cells: Arc<Mutex<Vec<(CellIndex, usize)>>>,
}

pub struct FilterStats {
    pub nb_points_matched: usize,
    pub processed_in: Duration,
    pub shape_contains_n_points: usize,
    pub cell_explored: Vec<(FilteringStep, CellIndex)>,
}

impl Runner {
    pub fn new(env: Env, db: Writer) -> Self {
        let this = Self {
            env,
            db,
            wake_up: Arc::new(synchronoise::SignalEvent::auto(true)),
            to_insert: Arc::default(),
            last_id: Arc::default(),
            all_items: Arc::default(),
            all_db_cells: Arc::default(),
            polygon_filter: Arc::default(),
            stats: Arc::default(),
            filter_stats: Arc::default(),
            points_matched: Arc::default(),
        };
        this.clone().run();
        this
    }

    pub fn add_point(&self, coord: LatLng) {
        self.to_insert.lock().push(coord);
        self.all_items.lock().push(coord);
        self.wake_up.signal();
    }

    fn run(self) {
        std::thread::spawn(move || {
            // Before entering the main loop we have to:
            // 1. Retrieve the last_id in the DB
            // 2. Retrieve all the items
            // 3. Retrieve all the DB cells
            let rtxn = self.env.read_txn().unwrap();
            let mut last_id = 0;
            let mut all_points = Vec::new();
            for entry in self.db.items(&rtxn).unwrap() {
                let (id, geometry) = entry.unwrap();
                last_id = last_id.max(id);
                match geometry {
                    GeoJson::Geometry(geometry) => match geometry.value {
                        Value::Point(vec) => all_points.push(LatLng::new(vec[1], vec[0]).unwrap()),
                        _ => todo!(),
                    },
                    _ => todo!(),
                }
            }
            *self.all_items.lock() = all_points;
            self.last_id.store(last_id, Ordering::Relaxed);
            let mut all_db_cells = Vec::new();
            for entry in self.db.inner_db_cells(&rtxn).unwrap() {
                let (cell, bitmap) = entry.unwrap();
                all_db_cells.push((cell, bitmap.len() as usize));
            }
            *self.all_db_cells.lock() = all_db_cells;
            drop(rtxn);

            loop {
                self.wake_up.wait();
                let to_insert = std::mem::take(&mut *self.to_insert.lock());
                let mut wtxn = self.env.write_txn().unwrap();

                for point in to_insert.iter() {
                    let id = self.last_id.fetch_add(1, Ordering::Relaxed);
                    self.db
                        .add_item(
                            &mut wtxn,
                            id,
                            &GeoJson::Geometry(geojson::Geometry {
                                bbox: None,
                                value: Value::Point(vec![point.lat(), point.lng()]),
                                foreign_members: None,
                            }),
                        )
                        .unwrap();
                }

                // We must recompute the stats
                if !to_insert.is_empty() {
                    *self.stats.lock() = self.db.stats(&wtxn).unwrap();
                }

                // if a point has been added OR the shape has been modified we must recompute the filter
                let polygon = self.polygon_filter.lock().to_vec();
                if polygon.len() >= 3 {
                    let shape_contains_n_points = polygon.len();

                    let polygon = Polygon::new(LineString(polygon), Vec::new());
                    let mut steps = Vec::new();
                    let now = std::time::Instant::now();
                    let matched = self
                        .db
                        .in_shape(&wtxn, &polygon, &mut |step| steps.push(step))
                        .unwrap();

                    *self.filter_stats.lock() = Some(FilterStats {
                        nb_points_matched: matched.len() as usize,
                        processed_in: now.elapsed(),
                        shape_contains_n_points,
                        cell_explored: steps,
                    });
                    let mut points_matched = Vec::new();
                    for point in matched {
                        let point = self.db.item(&wtxn, point).unwrap().unwrap();
                        let point = match point {
                            GeoJson::Geometry(geometry) => match geometry.value {
                                Value::Point(vec) => (vec[1], vec[0]),
                                _ => todo!(),
                            },
                            _ => todo!(),
                        };
                        points_matched.push(point);
                    }
                    *self.points_matched.lock() = points_matched;
                }

                wtxn.commit().unwrap();
            }
        });
    }
}
