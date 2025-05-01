use cellulite::roaring::RoaringBitmapCodec;
use egui::{Response, RichText, Ui, Color32};
use fst::{
    automaton::{Levenshtein, Str},
    Automaton, IntoStreamer, Streamer,
};
use walkers::{Plugin, Projector};
use egui_extras::syntax_highlighting::CodeTheme;
use geo::Intersects;
use h3o::{geom, CellIndex, Resolution};
use egui_double_slider::DoubleSlider;
use std::ops::RangeInclusive;

use crate::{runner::Runner, utils::{draw_geometry_on_map, extract_displayed_rect, display_cell}};

#[derive(Clone)]
pub struct ItemsInspector {
    pub query: String,
    pub runner: Runner,
    pub selected: Option<(u32, String, geojson::Value, Vec<CellIndex>)>,
    pub resolution_range: RangeInclusive<Resolution>,
}

impl ItemsInspector {
    pub fn new(runner: Runner) -> Self {
        Self {
            query: String::new(),
            runner,
            selected: None,
            resolution_range: Resolution::Zero..=Resolution::Fifteen,
        }
    }

    fn search_fst<A: Automaton>(
        &self,
        fst: &fst::Map<Vec<u8>>,
        automaton: A,
        result: &mut Vec<(String, u32)>,
    ) {
        if result.len() >= result.capacity() {
            return;
        }
        let rtxn = self.runner.env.read_txn().unwrap();
        let mut stream = fst.search(&automaton).into_stream();
        while let Some((s, bitmap_id)) = stream.next() {
            let name = String::from_utf8(s.to_vec()).unwrap();
            // Get the bitmap, we might miss it because there is a race condition in the runner where the fst is updated before the bitmaps are commited
            let Some(bitmap) = self
                .runner
                .metadata
                .remap_data_type::<RoaringBitmapCodec>()
                .get(&rtxn, &format!("bitmap_{bitmap_id:010}"))
                .unwrap()
            else {
                continue;
            };
            result.extend(bitmap.iter().map(|id| (name.clone(), id)));
            if result.len() >= result.capacity() {
                break;
            }
        }
    }

    pub fn ui(&mut self, ui: &mut Ui) {
        ui.collapsing(RichText::new("Inspect item").heading(), |ui| {
            ui.text_edit_singleline(&mut self.query);
            let result = self.search();
            ui.label(format!("result: {:?}", result.len()));
            ui.separator();
            if let Some((item, name, geometry, cells)) = &self.selected {
                let response = ui.selectable_label(true, format!("{}: {}", name, item));
                display_geojson_as_codeblock(ui, geometry);
                ui.label(format!("Made of {} cells", cells.len()));
                
                // Add resolution range slider
                ui.horizontal(|ui| {
                    ui.label("Resolution range:");
                    let mut min = *self.resolution_range.start() as u8;
                    let mut max = *self.resolution_range.end() as u8;
                    let response = ui.add(DoubleSlider::new(&mut min, &mut max, 0..=15));
                    
                    if response.changed() {
                        self.resolution_range = Resolution::try_from(min).unwrap_or(Resolution::Zero)
                            ..=Resolution::try_from(max).unwrap_or(Resolution::Fifteen);
                    }
                });
                
                // Handle deselection using the original label's response
                if response.clicked() {
                    self.selected = None;
                }
            } else {
                for (name, item) in result {
                    let response = ui.selectable_label(
                        self.selected.as_ref().map(|(id, _, _, _)| *id) == Some(item),
                        format!("{}: {}", name, item),
                    );
                    if response.clicked() {
                        let geometry = self.runner.all_items.lock()[item as usize].clone();
                           // Get the cells containing this document from the runner's all_db_cells
                        let cells = self.runner.all_db_cells.lock().iter().filter(|(_, bitmap)| bitmap.contains(item)).map(|(cell, _)| *cell).collect();
                        self.selected = Some((item, name, geometry, cells));
                    }
                }
            }
        });
    }

    fn search(&mut self) -> Vec<(String, u32)> {
        let fst = self.runner.fst.lock();
        let mut result = Vec::with_capacity(50);
        let exact = Str::new(&self.query);
        self.search_fst(&*fst, exact.clone(), &mut result);
        let prefix = exact
            .clone()
            .starts_with()
            .intersection(exact.clone().complement());
        self.search_fst(&*fst, prefix.clone(), &mut result);
        let lev = Levenshtein::new(&self.query, self.query.len() as u32 / 3).unwrap();
        let base = lev.starts_with().intersection(exact.clone().starts_with().complement());
        self.search_fst(&*fst, base, &mut result);
        result
    }
}

fn display_geojson_as_codeblock(ui: &mut Ui, geometry: &geojson::Value) {
    let geojson_obj = geojson::Geometry {
        value: geometry.clone(),
        bbox: None,
        foreign_members: None,
    };
    let pretty_geometry = serde_json::to_string_pretty(&geojson_obj).unwrap();
    let geojson_url = format!(
        "http://geojson.io/#data=data:application/json,{}",
        &geometry
    );

    let mut code_str = pretty_geometry.clone();
    let mut layouter = |ui: &egui::Ui, buf: &str, wrap_width: f32| {
        let mut layout_job = egui_extras::syntax_highlighting::highlight(
            ui.ctx(),
            ui.style(),
            &CodeTheme::from_style(ui.style()),
            buf,
            "json",
        );
        layout_job.wrap.max_width = wrap_width;
        ui.fonts(|f| f.layout_job(layout_job))
    };

    let text_edit = egui::TextEdit::multiline(&mut code_str)
        .code_editor()
        .desired_width(f32::INFINITY)
        .layouter(&mut layouter)
        .interactive(true);

    ui.add(text_edit);
    
    ui.horizontal(|ui| {
        if ui.button("Copy GeoJSON").clicked() {
            ui.ctx().copy_text(pretty_geometry.clone());
        }
        ui.hyperlink_to("View on geojson.io", geojson_url);
    });
}

impl Plugin for ItemsInspector {
    fn run(self: Box<Self>, ui: &mut Ui, _response: &Response, projector: &Projector) {
        if let Some((_, _, geometry, cells)) = self.selected {
            let displayed_rect = extract_displayed_rect(ui, projector);
            let painter = ui.painter();
            draw_geometry_on_map(projector, displayed_rect, painter, &geometry);

            // Get the cells containing this document from the runner's all_db_cells
            for cell in cells.iter() {
                let resolution = cell.resolution();
                if self.resolution_range.contains(&resolution) {
                    let solvent = geom::SolventBuilder::new().build();
                    let cell_polygon = solvent.dissolve(Some(*cell)).unwrap();
                    let cell_polygon = &cell_polygon.0[0];

                    if cell_polygon.intersects(&displayed_rect) {
                        display_cell(projector, painter, *cell, Color32::DARK_GREEN);
                    }
                }
            }
        }
    }
}

