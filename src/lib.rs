use std::collections::{BTreeMap, HashSet, VecDeque};

use ::roaring::RoaringBitmap;
use geo::{Contains, Coord, Geometry, Intersects};
use geo_types::Polygon;
use geojson::GeoJson;
use geom::bounding_box;
use h3o::{
    error::{InvalidGeometry, InvalidLatLng},
    geom::{ContainmentMode, TilerBuilder},
    CellIndex, LatLng, Resolution,
};
use heed::{types::SerdeJson, RoTxn, RwTxn, Unspecified};
use keys::{CellIndexCodec, Key, KeyCodec, KeyPrefixVariantCodec, KeyVariant};
use ordered_float::OrderedFloat;

pub mod geom;
mod keys;
mod roaring;
#[cfg(test)]
mod test;

use crate::roaring::RoaringBitmapCodec;

pub type Database = heed::Database<KeyCodec, Unspecified>;
pub type ItemId = u32;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    InvalidLatLng(#[from] InvalidLatLng),
    #[error(transparent)]
    InvalidGeometry(#[from] InvalidGeometry),
}

type Result<O, E = Error> = std::result::Result<O, E>;

#[derive(Clone)]
pub struct Writer {
    pub(crate) db: Database,
    /// After how many elements should we break a cell into sub-cells
    pub threshold: u64,
}

impl Writer {
    pub fn new(db: Database) -> Self {
        Self { db, threshold: 200 }
    }

    /// Return all the cells used internally in the database
    pub fn inner_db_cells<'a>(
        &self,
        rtxn: &'a RoTxn,
    ) -> Result<impl Iterator<Item = Result<(CellIndex, RoaringBitmap), heed::Error>> + 'a> {
        Ok(self
            .db
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter(rtxn, &KeyVariant::Cell)?
            .remap_types::<KeyCodec, RoaringBitmapCodec>()
            .map(|res| {
                res.map(|(key, bitmap)| {
                    let Key::Cell(cell) = key else { unreachable!() };
                    (cell, bitmap)
                })
            }))
    }

    /// Return the coordinates of the items rounded down to 50cm if this id exists in the DB. Returns `None` otherwise.
    pub fn item(&self, rtxn: &RoTxn, item: ItemId) -> Result<Option<GeoJson>> {
        self.item_db()
            .get(rtxn, &Key::Item(item))
            .map_err(Error::from)
    }

    /// Iterate over all the items in the database
    pub fn items<'a>(
        &self,
        rtxn: &'a RoTxn,
    ) -> Result<impl Iterator<Item = Result<(ItemId, GeoJson), heed::Error>> + 'a> {
        Ok(self
            .db
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter(rtxn, &KeyVariant::Item)?
            .remap_types::<KeyCodec, SerdeJson<GeoJson>>()
            .map(|res| {
                res.map(|(key, cell)| {
                    let Key::Item(item) = key else { unreachable!() };
                    (item, cell)
                })
            }))
    }

    pub fn add_item(&self, wtxn: &mut RwTxn, item: ItemId, geo: &GeoJson) -> Result<()> {
        let shape = Geometry::try_from(geo.clone()).unwrap();
        self.item_db().put(wtxn, &Key::Item(item), geo)?;

        match shape {
            Geometry::Point(point) => {
                let cell = LatLng::new(point.y(), point.x())
                    .unwrap()
                    .to_cell(Resolution::Zero);
                self.insert_shape_in_cell(wtxn, item, shape, cell)
            }
            Geometry::MultiPoint(multi_point) => {
                for point in multi_point {
                    let cell = LatLng::new(point.y(), point.x())
                        .unwrap()
                        .to_cell(Resolution::Zero);
                    self.insert_shape_in_cell(wtxn, item, point.into(), cell)?;
                }
                Ok(())
            }

            Geometry::Polygon(polygon) => todo!(),
            Geometry::MultiPolygon(multi_polygon) => todo!(),
            Geometry::Rect(rect) => todo!(),
            Geometry::Triangle(triangle) => todo!(),

            Geometry::GeometryCollection(geometry_collection) => todo!(),

            Geometry::Line(_) | Geometry::LineString(_) | Geometry::MultiLineString(_) => {
                return panic!("Doesn't support lines");
            }
        }
    }

    pub fn stats(&self, rtxn: &RoTxn) -> Result<Stats> {
        let total_items = self.items(rtxn)?.count();
        let mut total_cells = 0;
        let mut cells_by_resolution = BTreeMap::new();

        for entry in self.inner_db_cells(rtxn)? {
            let (cell, _) = entry?;
            total_cells += 1;
            *cells_by_resolution.entry(cell.resolution()).or_default() += 1;
        }

        Ok(Stats {
            total_cells,
            total_items,
            cells_by_resolution,
        })
    }

    fn item_db(&self) -> heed::Database<KeyCodec, SerdeJson<GeoJson>> {
        self.db.remap_data_type()
    }

    fn cell_db(&self) -> heed::Database<KeyCodec, RoaringBitmapCodec> {
        self.db.remap_data_type()
    }

    fn insert_shape_in_cell(
        &self,
        wtxn: &mut RwTxn,
        item: ItemId,
        shape: Geometry,
        cell: CellIndex,
    ) -> Result<()> {
        // let solvent = h3o::geom::SolventBuilder::new().build();
        // let cell_polygon = solvent.dissolve(Some(cell)).unwrap();

        let key = Key::Cell(cell);
        match self.cell_db().get(wtxn, &key)? {
            Some(mut bitmap) => {
                let already_splitted = bitmap.len() >= self.threshold;
                bitmap.insert(item);
                self.cell_db().put(wtxn, &key, &bitmap)?;

                if bitmap.len() >= self.threshold {
                    let to_insert = if already_splitted {
                        RoaringBitmap::from_sorted_iter(Some(item)).unwrap()
                    } else {
                        bitmap
                    };
                    for i in to_insert {
                        let geometry = if i == item {
                            shape.clone()
                        } else {
                            self.item_db()
                                .get(wtxn, &Key::Item(i))?
                                .unwrap()
                                .try_into()
                                .unwrap()
                        };
                        match geometry {
                            Geometry::Point(point) => {
                                let latlng = LatLng::new(point.y(), point.x()).unwrap();
                                let Some(res) = cell.resolution().succ() else {
                                    continue;
                                };
                                self.insert_shape_in_cell(wtxn, i, geometry, latlng.to_cell(res))?;
                            }
                            Geometry::Line(line) => todo!(),
                            Geometry::LineString(line_string) => todo!(),
                            Geometry::Polygon(polygon) => todo!(),
                            Geometry::MultiPoint(multi_point) => todo!(),
                            Geometry::MultiLineString(multi_line_string) => todo!(),
                            Geometry::MultiPolygon(multi_polygon) => todo!(),
                            Geometry::GeometryCollection(geometry_collection) => todo!(),
                            Geometry::Rect(rect) => todo!(),
                            Geometry::Triangle(triangle) => todo!(),
                        }
                    }
                }
            }
            None => {
                let bitmap = RoaringBitmap::from_sorted_iter(Some(item)).unwrap();
                self.cell_db().put(wtxn, &key, &bitmap)?;
            }
        }
        Ok(())
    }

    // The strategy to retrieve the points in a shape is to:
    // 1. Retrieve all the cell@res0 that contains the shape
    // 2. Iterate over these cells
    //  2.1.If a cell fit entirely *inside* the shape, add all its items to the result
    //  2.2 Otherwise:
    //   - If the cell is a leaf => iterate over all of its point and add the one that fits in the shape to the result
    //   - Otherwise, increase the precision and iterate on the range of cells => repeat step 2
    pub fn in_shape(
        &self,
        rtxn: &RoTxn,
        polygon: &Polygon,
        inspector: &mut dyn FnMut((FilteringStep, CellIndex)),
    ) -> Result<RoaringBitmap> {
        let mut tiler = TilerBuilder::new(Resolution::Zero)
            .containment_mode(ContainmentMode::Covers)
            .build();
        tiler.add(polygon.clone())?;

        let mut ret = RoaringBitmap::new();
        let mut double_check = RoaringBitmap::new();
        let mut to_explore: VecDeque<_> = tiler.into_coverage().collect();
        let mut already_explored: HashSet<CellIndex> = to_explore.iter().copied().collect();

        while let Some(cell) = to_explore.pop_front() {
            let Some(items) = self.cell_db().get(rtxn, &Key::Cell(cell))? else {
                (inspector)((FilteringStep::NotPresentInDB, cell));
                continue;
            };

            let solvent = h3o::geom::SolventBuilder::new().build();
            let cell_polygon = solvent.dissolve(Some(cell)).unwrap();

            // let cell_polygon = bounding_box(cell);
            let cell_polygon = &cell_polygon.0[0];
            if polygon.contains(cell_polygon) {
                (inspector)((FilteringStep::Returned, cell));
                ret |= items;
            } else if polygon.intersects(cell_polygon) {
                let resolution = cell.resolution();
                if items.len() < self.threshold || resolution == Resolution::Fifteen {
                    (inspector)((FilteringStep::RequireDoubleCheck, cell));
                    double_check |= items;
                } else {
                    (inspector)((FilteringStep::DeepDive, cell));
                    let mut tiler = TilerBuilder::new(resolution.succ().unwrap())
                        .containment_mode(ContainmentMode::Covers)
                        .build();
                    tiler.add(cell_polygon.clone())?;
                    for cell in tiler.into_coverage() {
                        if already_explored.insert(cell) {
                            to_explore.push_front(cell);
                        }
                    }
                }
            } else {
                // else: we can ignore the cell, it's not part of our shape
                (inspector)((FilteringStep::OutsideOfShape, cell));
            }
        }

        // Since we have overlap some items may have been definitely validated somewhere but were also included as something to double check
        double_check -= &ret;

        for item in double_check {
            let geojson = self.item_db().get(rtxn, &Key::Item(item))?.unwrap();
            match Geometry::try_from(geojson).unwrap() {
                Geometry::Point(point) => {
                    if polygon.contains(&Coord {
                        x: point.x(),
                        y: point.y(),
                    }) {
                        ret.insert(item);
                    }
                }
                Geometry::MultiPoint(multi_point) => todo!(),

                Geometry::Polygon(polygon) => todo!(),
                Geometry::MultiPolygon(multi_polygon) => todo!(),
                Geometry::Rect(rect) => todo!(),
                Geometry::Triangle(triangle) => todo!(),

                Geometry::GeometryCollection(geometry_collection) => todo!(),

                Geometry::MultiLineString(_) | Geometry::Line(_) | Geometry::LineString(_) => {
                    unreachable!("lines not supported")
                }
            }
        }

        Ok(ret)
    }

    /*
    // TODO: this is wrong => maybe our point was on a the side of a cell and the point at the top of the cell are further away than the point in the cell below
    pub fn nearest_point(
        &self,
        rtxn: &RoTxn,
        coord: (f64, f64),
        limit: u64,
    ) -> Result<Vec<(ItemId, (f64, f64))>> {
        let lat_lng_cell = LatLng::new(coord.0, coord.1)?;

        let mut res = Resolution::Zero;
        let mut bitmap = RoaringBitmap::new();

        loop {
            let cell = lat_lng_cell.to_cell(res);
            let key = Key::Cell(cell);
            // We're looking for the resolution that gives us just slightly more elements than the limit
            match self.cell_db().get(rtxn, &key)? {
                Some(sub_bitmap) => {
                    if sub_bitmap.len() < limit {
                        break;
                    }
                    bitmap = sub_bitmap;
                    let Some(sub_res) = res.succ() else { break };
                    res = sub_res;
                }
                None => break,
            }
        }

        for cell in lat_lng_cell.to_cell(res).grid_disk::<Vec<_>>(1) {
            if let Some(sub_bitmap) = self.cell_db().get(rtxn, &Key::Cell(cell))? {
                bitmap |= sub_bitmap;
            }
        }

        let mut ret = Vec::with_capacity(bitmap.len() as usize);
        for item in bitmap {
            ret.push((
                item,
                LatLng::from(self.item_db().get(rtxn, &Key::Item(item))?.unwrap()),
            ));
        }
        ret.sort_by_cached_key(|(_, other)| OrderedFloat(lat_lng_cell.distance_m(*other)));
        Ok(ret
            .into_iter()
            .map(|(id, coord)| (id, (coord.lat(), coord.lng())))
            .take(limit as usize)
            .collect())
    }
    */
}

#[derive(Debug, Copy, Clone)]
pub enum FilteringStep {
    NotPresentInDB,
    OutsideOfShape,
    Returned,
    RequireDoubleCheck,
    DeepDive,
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub total_cells: usize,
    pub total_items: usize,
    pub cells_by_resolution: BTreeMap<Resolution, usize>,
}
