use cellulite::roaring::RoaringBitmapCodec;
use egui::{Response, RichText, ScrollArea, Sense, Ui, Vec2};
use egui_code_editor::{CodeEditor, ColorTheme, Syntax};
use fst::{
    automaton::{Levenshtein, Str},
    Automaton, IntoStreamer, Streamer,
};
use geo::{Point, Rect};
use walkers::{Plugin, Projector};
use egui::text::{LayoutJob, TextFormat};
use egui::Color32;

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
            let result = self.search();
            ui.label(format!("result: {:?}", result.len()));
            ui.separator();
            if let Some((item, name, geometry)) = &self.selected {
                let response = ui.selectable_label(true, format!("{}: {}", name, item));
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

                // Display the pretty JSON as a clickable code block with syntax highlighting
                let mut code_str = pretty_geometry.clone();
                let mut layouter = |ui: &egui::Ui, string: &str, wrap_width: f32| {
                    let job = json_highlight(ui, string, wrap_width);
                    ui.fonts(|f| f.layout_job(job))
                };
                let text_edit = egui::TextEdit::multiline(&mut code_str)
                    .font(egui::TextStyle::Monospace)
                    .code_editor()
                    .desired_rows(10)
                    .desired_width(f32::INFINITY)
                    .layouter(&mut layouter)
                    .interactive(false);
                let code_response = ui.add(text_edit).interact(Sense::click());
                if code_response.clicked() {
                    ui.ctx().open_url(egui::output::OpenUrl {
                        url: geojson_url,
                        new_tab: true,
                    });
                }
                if code_response.hovered() {
                    ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
                }

                // Handle deselection using the original label's response
                if response.clicked() {
                    self.selected = None;
                }
            } else {
                for (name, item) in result {
                    let response = ui.selectable_label(
                        self.selected.as_ref().map(|(id, _, _)| *id) == Some(item),
                        format!("{}: {}", name, item),
                    );
                    if response.clicked() {
                        let geometry = self.runner.all_items.lock()[item as usize].clone();
                        self.selected = Some((item, name, geometry));
                    }
                }
            }
        });
    }

    fn search(&mut self) -> Vec<(String, u32)> {
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
        result
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

fn json_highlight(ui: &egui::Ui, json: &str, wrap_width: f32) -> LayoutJob {
    let mut job = LayoutJob::default();
    let mut chars = json.chars().peekable();
    let mut buf = String::new();
    let mut push = |text: &str, color: Color32| {
        let format = TextFormat {
            font_id: egui::FontId::monospace(14.0),
            color,
            ..Default::default()
        };
        job.append(text, 0.0, format);
    };
    while let Some(&c) = chars.peek() {
        match c {
            '"' => {
                if !buf.is_empty() {
                    push(&buf, Color32::LIGHT_GRAY);
                    buf.clear();
                }
                // Parse string
                let mut s = String::new();
                s.push(chars.next().unwrap()); // opening quote
                while let Some(&next) = chars.peek() {
                    s.push(next);
                    chars.next();
                    if next == '"' && !s.ends_with("\\\"") {
                        break;
                    }
                }
                push(&s, Color32::from_rgb(220, 180, 120)); // string color
            }
            '0'..='9' | '-' => {
                if !buf.is_empty() {
                    push(&buf, Color32::LIGHT_GRAY);
                    buf.clear();
                }
                // Parse number
                let mut s = String::new();
                while let Some(&next) = chars.peek() {
                    if next.is_ascii_digit() || next == '.' || next == '-' || next == 'e' || next == 'E' || next == '+' {
                        s.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                push(&s, Color32::from_rgb(120, 200, 220)); // number color
            }
            't' | 'f' => {
                if !buf.is_empty() {
                    push(&buf, Color32::LIGHT_GRAY);
                    buf.clear();
                }
                // Parse true/false
                let mut s = String::new();
                for _ in 0..5 {
                    if let Some(&next) = chars.peek() {
                        if next.is_ascii_alphabetic() {
                            s.push(next);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
                let color = if s == "true" || s == "false" {
                    Color32::from_rgb(180, 220, 120)
                } else {
                    Color32::LIGHT_GRAY
                };
                push(&s, color);
            }
            'n' => {
                if !buf.is_empty() {
                    push(&buf, Color32::LIGHT_GRAY);
                    buf.clear();
                }
                // Parse null
                let mut s = String::new();
                for _ in 0..4 {
                    if let Some(&next) = chars.peek() {
                        if next.is_ascii_alphabetic() {
                            s.push(next);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
                let color = if s == "null" {
                    Color32::from_rgb(180, 120, 220)
                } else {
                    Color32::LIGHT_GRAY
                };
                push(&s, color);
            }
            '{' | '}' | '[' | ']' | ':' | ',' => {
                if !buf.is_empty() {
                    push(&buf, Color32::LIGHT_GRAY);
                    buf.clear();
                }
                push(&c.to_string(), Color32::WHITE);
                chars.next();
            }
            c if c.is_whitespace() => {
                buf.push(c);
                chars.next();
            }
            _ => {
                buf.push(c);
                chars.next();
            }
        }
    }
    if !buf.is_empty() {
        push(&buf, Color32::LIGHT_GRAY);
    }
    job.wrap.max_width = wrap_width;
    job
}
