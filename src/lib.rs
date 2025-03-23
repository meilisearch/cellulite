use std::collections::BTreeMap;

use ::roaring::RoaringBitmap;
use geo::{Contains, Coord, Intersects};
use geo_types::Polygon;
use h3o::{
    error::{InvalidGeometry, InvalidLatLng},
    geom::{ContainmentMode, TilerBuilder},
    CellIndex, LatLng, Resolution,
};
use heed::{RoTxn, RwTxn, Unspecified};
use keys::{CellIndexCodec, Key, KeyCodec, KeyPrefixVariantCodec, KeyVariant};
use ordered_float::OrderedFloat;

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
    pub fn item(&self, rtxn: &RoTxn, item: ItemId) -> Result<Option<(f64, f64)>> {
        match self.item_db().get(rtxn, &Key::Item(item)) {
            Ok(Some(cell)) => {
                let c = LatLng::from(cell);
                Ok(Some((c.lat(), c.lng())))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Return all the cells used internally in the database
    pub fn items<'a>(
        &self,
        rtxn: &'a RoTxn,
    ) -> Result<impl Iterator<Item = Result<(ItemId, CellIndex), heed::Error>> + 'a> {
        Ok(self
            .db
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter(rtxn, &KeyVariant::Item)?
            .remap_types::<KeyCodec, CellIndexCodec>()
            .map(|res| {
                res.map(|(key, cell)| {
                    let Key::Item(item) = key else { unreachable!() };
                    (item, cell)
                })
            }))
    }

    pub fn add_item(&self, wtxn: &mut RwTxn, item: ItemId, coord: (f64, f64)) -> Result<()> {
        let lat_lng_cell = LatLng::new(coord.0, coord.1)?;
        self.db.remap_data_type::<CellIndexCodec>().put(
            wtxn,
            &Key::Item(item),
            &lat_lng_cell.to_cell(Resolution::Fifteen),
        )?;
        self.insert_items(
            wtxn,
            RoaringBitmap::from_sorted_iter(Some(item)).unwrap(),
            Resolution::Zero,
        )
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

    fn item_db(&self) -> heed::Database<KeyCodec, CellIndexCodec> {
        self.db.remap_data_type()
    }

    fn cell_db(&self) -> heed::Database<KeyCodec, RoaringBitmapCodec> {
        self.db.remap_data_type()
    }

    // TODO: Can be hugely optimized by specifying the base cell + when we split a "leaf" group all items by their sub-level leaf and make just a few calls.
    //       with the current implementation we're deserializing and reserializing and rereading and rewriting the same bitmap once per items instead of once + once for each children (5-6 times more).
    fn insert_items(&self, wtxn: &mut RwTxn, items: RoaringBitmap, res: Resolution) -> Result<()> {
        for item in items {
            let cell = self.item_db().get(wtxn, &Key::Item(item))?.unwrap();
            // This item cells are always at the maximum resolution and have a parent
            let cell = cell.parent(res).unwrap();
            let key = Key::Cell(cell);
            match self.cell_db().get(wtxn, &key)? {
                Some(mut bitmap) => {
                    let already_splitted = bitmap.len() >= self.threshold;
                    bitmap.insert(item);
                    self.cell_db().put(wtxn, &key, &bitmap)?;

                    // If we reached the maximum precision we can stop immediately
                    let Some(next_res) = res.succ() else { continue };

                    if bitmap.len() >= self.threshold {
                        let to_insert = if already_splitted {
                            RoaringBitmap::from_sorted_iter(Some(item)).unwrap()
                        } else {
                            bitmap
                        };
                        self.insert_items(wtxn, to_insert, next_res)?;
                    }
                }
                None => {
                    let bitmap = RoaringBitmap::from_sorted_iter(Some(item)).unwrap();
                    self.cell_db().put(wtxn, &key, &bitmap)?;
                }
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
        polygon: Polygon,
        inspector: &mut dyn FnMut((FilteringStep, CellIndex)),
    ) -> Result<RoaringBitmap> {
        let mut tiler = TilerBuilder::new(Resolution::Zero)
            .containment_mode(ContainmentMode::Covers)
            .build();
        tiler.add(polygon.clone())?;

        let mut ret = RoaringBitmap::new();
        let mut double_check = RoaringBitmap::new();
        let mut to_explore: Vec<_> = tiler.into_coverage().collect();

        while let Some(cell) = to_explore.pop() {
            let Some(items) = self.cell_db().get(rtxn, &Key::Cell(cell))? else {
                (inspector)((FilteringStep::NotPresentInDB, cell));
                continue;
            };

            // Can't fail since we specified only one cell
            let cell_polygon = h3o::geom::dissolve(Some(cell)).unwrap().0;
            let cell_polygon = &cell_polygon[0];
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
                    // unwrap is safe since we checked we're not at the last resolution right above
                    to_explore.extend(cell.children(resolution.succ().unwrap()));
                }
            } else {
                // else: we can ignore the cell, it's not part of our shape
                (inspector)((FilteringStep::OutsideOfShape, cell));
            }
        }

        for item in double_check {
            let cell = self.item_db().get(rtxn, &Key::Item(item))?.unwrap();
            let coord = LatLng::from(cell);
            if polygon.contains(&Coord {
                x: coord.lng(),
                y: coord.lat(),
            }) {
                ret.insert(item);
            }
        }

        Ok(ret)
    }

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
}

pub enum FilteringStep {
    NotPresentInDB,
    OutsideOfShape,
    Returned,
    RequireDoubleCheck,
    DeepDive,
}

#[derive(Debug, Default)]
pub struct Stats {
    pub total_cells: usize,
    pub total_items: usize,
    pub cells_by_resolution: BTreeMap<Resolution, usize>,
}
