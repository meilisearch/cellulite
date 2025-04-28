use cellulite::roaring::RoaringBitmapCodec;
use egui::{Response, RichText, Ui, Vec2};
use fst::{
    automaton::{Levenshtein, Str},
    Automaton, IntoStreamer, Streamer,
};
use geo::{Point, Rect};
use walkers::{Plugin, Projector};

use crate::{runner::Runner, utils::draw_geometry_on_map};

#[derive(Clone)]
pub struct ItemsInspector {
    pub query: String,
    pub runner: Runner,
    pub selected: Option<(u32, String, geojson::Value)>,
}

impl ItemsInspector {
    pub fn new(runner: Runner) -> Self {
        Self {
            query: String::new(),
            runner,
            selected: None,
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
            let fst = self.runner.fst.lock();
            let mut result = Vec::with_capacity(50);
            let base = Str::new(&self.query);
            self.search_fst(&*fst, base.clone(), &mut result);
            let base = base
                .clone()
                .starts_with()
                .intersection(base.clone().complement());
            self.search_fst(&*fst, base.clone(), &mut result);
            let lev = Levenshtein::new(&self.query, self.query.len() as u32 / 3).unwrap();
            let base = lev.starts_with().intersection(base.complement());
            self.search_fst(&*fst, base, &mut result);
            ui.label(format!("result: {:?}", result.len()));
            ui.separator();
            for (name, item) in result {
                let responso = ui.selectable_label(
                    self.selected.as_ref().map(|(id, _, _)| *id) == Some(item),
                    format!("{}: {}", name, item),
                );
                if responso.clicked() {
                    let geometry = self.runner.all_items.lock()[item as usize].clone();
                    self.selected = Some((item, name, geometry));
                }
            }
        });
    }
}

impl Plugin for ItemsInspector {
    fn run(self: Box<Self>, ui: &mut Ui, _response: &Response, projector: &Projector) {
        if let Some((_item, _name, geometry)) = self.selected {
            let x = ui.available_width();
            let y = ui.available_height();
            let top_left = projector.unproject(Vec2 { x: 0.0, y: 0.0 });
            let bottom_right = projector.unproject(Vec2 { x, y });
            let displayed_rect = Rect::new(
                Point::new(top_left.x(), top_left.y()),
                Point::new(bottom_right.x(), bottom_right.y()),
            );

            let painter = ui.painter();
            draw_geometry_on_map(projector, displayed_rect, painter, &geometry);
        }
    }
}
