use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use ::roaring::RoaringBitmap;
use geo::{BooleanOps, Contains, Coord, Geometry, HasDimensions, Intersects};
use geo_types::Polygon;
use geojson::GeoJson;
use h3o::{
    error::{InvalidGeometry, InvalidLatLng},
    geom::{ContainmentMode, TilerBuilder},
    CellIndex, LatLng, Resolution,
};
use heed::{
    types::{SerdeJson, Unit},
    RoTxn, RwTxn, Unspecified,
};
use keys::{Key, KeyCodec, KeyPrefixVariantCodec, KeyVariant};

mod keys;
pub mod roaring;
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
pub struct Cellulite {
    pub(crate) db: Database,
    /// After how many elements should we break a cell into sub-cells
    pub threshold: u64,
}

impl Cellulite {
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

    /// Return all the cells used internally in the database
    pub fn inner_shape_cells<'a>(
        &self,
        rtxn: &'a RoTxn,
    ) -> Result<impl Iterator<Item = Result<(CellIndex, RoaringBitmap), heed::Error>> + 'a> {
        Ok(self
            .db
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter(rtxn, &KeyVariant::InnerShape)?
            .remap_types::<KeyCodec, RoaringBitmapCodec>()
            .map(|res| {
                res.map(|(key, bitmap)| {
                    let Key::InnerShape(cell) = key else {
                        unreachable!()
                    };
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

    pub fn add(&self, wtxn: &mut RwTxn, item: ItemId, geo: &GeoJson) -> Result<()> {
        self.item_db().put(wtxn, &Key::Item(item), geo)?;
        self.updated_db().put(wtxn, &Key::Update(item), &())?;
        Ok(())
    }

    pub fn delete(&self, wtxn: &mut RwTxn, item: ItemId) -> Result<()> {
        self.deleted_db().put(wtxn, &Key::Remove(item), &())?;
        Ok(())
    }

    fn retrieve_and_clear_inserted_items(&self, wtxn: &mut RwTxn) -> Result<RoaringBitmap> {
        let mut bitmap = RoaringBitmap::new();
        let mut iter = self
            .db
            .remap_types::<KeyPrefixVariantCodec, Unit>()
            .prefix_iter_mut(wtxn, &KeyVariant::Update)?
            .remap_types::<KeyCodec, Unit>();
        while let Some(ret) = iter.next() {
            let (Key::Update(item), ()) = ret? else {
                unreachable!()
            };
            // safe because keys are ordered
            bitmap.push(item);
            // safe because we own the ItemId
            unsafe {
                iter.del_current();
            }
        }
        Ok(bitmap)
    }

    fn retrieve_and_clear_deleted_items(&self, wtxn: &mut RwTxn) -> Result<RoaringBitmap> {
        let mut bitmap = RoaringBitmap::new();
        let mut iter = self
            .db
            .remap_types::<KeyPrefixVariantCodec, Unit>()
            .prefix_iter_mut(wtxn, &KeyVariant::Remove)?
            .remap_types::<KeyCodec, Unit>();
        while let Some(ret) = iter.next() {
            let (Key::Remove(item), ()) = ret? else {
                unreachable!()
            };
            // safe because keys are ordered
            bitmap.push(item);
            // safe because we own the ItemId
            unsafe {
                iter.del_current();
            }
        }
        Ok(bitmap)
    }

    /// Indexing is in 4 steps:
    /// 1. We retrieve all the items that have been updated since the last indexing
    /// 2. We remove the deleted items from the database and remove the empty cells at the same time
    ///    TODO: If a cell becomes too small we cannot delete it so the database won't shrink properly but won't be corrupted either
    /// 3. We insert the new items in the database **only at the level 0**
    /// 4. We take each level-zero cell one by one and if it contains new items we insert them in the database in batch at the next level
    ///    TODO: Could be parallelized fairly easily I think
    pub fn build(&self, wtxn: &mut RwTxn) -> Result<()> {
        // 1.
        let inserted_items = self.retrieve_and_clear_inserted_items(wtxn)?;
        let removed_items = self.retrieve_and_clear_deleted_items(wtxn)?;

        // 2.
        self.remove_deleted_items(wtxn, removed_items)?;

        // 3.
        self.insert_items_at_level_zero(wtxn, inserted_items)?;

        // 4.
        //         let shape = Geometry::try_from(geo.clone()).unwrap();
        let to_insert = self.explode_level_zero_geo(wtxn, item, shape)?;
        let mut to_insert = VecDeque::from(to_insert);

        while let Some((current_item, shape, cell)) = to_insert.pop_front() {
            let cell_polygon = h3o::geom::SolventBuilder::new()
                .build()
                .dissolve(Some(cell))
                .unwrap();
            let cell_polygon = &cell_polygon.0[0];

            // If the cell is entirely contained within the shape, insert it in the inner_shape_cell_db and stop
            if shape.contains(cell_polygon) {
                let mut bitmap = self
                    .inner_shape_cell_db()
                    .get(wtxn, &Key::InnerShape(cell))?
                    .unwrap_or_default();
                bitmap.insert(current_item);
                self.inner_shape_cell_db()
                    .put(wtxn, &Key::InnerShape(cell), &bitmap)?;
            } else {
                // Otherwise, insert it in the cell_db
                let mut bitmap = self
                    .cell_db()
                    .get(wtxn, &Key::Cell(cell))?
                    .unwrap_or_default();
                let already_splitted = bitmap.len() >= self.threshold;
                bitmap.insert(current_item);
                self.cell_db().put(wtxn, &Key::Cell(cell), &bitmap)?;
                // If we reached the max resolution, there is nothing else we can do
                let Some(next_res) = cell.resolution().succ() else {
                    continue;
                };
                // If we exceeded the threshold, we should re-insert ourselves in the queue with a greater resolution
                if bitmap.len() >= self.threshold {
                    // insert ourselves in the queue with a greater resolution
                    match shape {
                        Geometry::Point(point) => {
                            let cell = LatLng::new(point.y(), point.x()).unwrap().to_cell(next_res);
                            to_insert.push_back((current_item, shape, cell));
                        }
                        Geometry::Polygon(polygon) => {
                            let mut tiler = TilerBuilder::new(next_res)
                                .containment_mode(ContainmentMode::Covers)
                                .build();
                            tiler.add(polygon.clone())?;
                            for cell in tiler.into_coverage() {
                                to_insert.push_back((
                                    current_item,
                                    Geometry::Polygon(polygon.clone()),
                                    cell,
                                ));
                            }
                        }
                        Geometry::MultiPoint(_)
                        | Geometry::MultiPolygon(_)
                        | Geometry::Rect(_)
                        | Geometry::Triangle(_) => {
                            todo!("Received a shape that should have been exploded already")
                        }
                        _ => {
                            unreachable!()
                        }
                    }

                    if !already_splitted {
                        // Loop over all the items of the bitmap and insert the part that fall in the shape in the queue
                        for item in bitmap.iter() {
                            // already inserted above
                            if current_item == item {
                                continue;
                            }
                            let geojson = self.item_db().get(wtxn, &Key::Item(item))?.unwrap();
                            let shape = Geometry::try_from(geojson).unwrap();
                            match shape {
                                Geometry::Point(point) => {
                                    let cell = LatLng::new(point.y(), point.x())
                                        .unwrap()
                                        .to_cell(next_res);
                                    to_insert.push_back((item, shape, cell));
                                }
                                Geometry::MultiPoint(multi_point) => {
                                    for point in multi_point.0.iter() {
                                        if cell_polygon.contains(&Coord {
                                            x: point.x(),
                                            y: point.y(),
                                        }) {
                                            let cell = LatLng::new(point.y(), point.x())
                                                .unwrap()
                                                .to_cell(next_res);
                                            to_insert.push_back((
                                                item,
                                                Geometry::Point(*point),
                                                cell,
                                            ));
                                        }
                                    }
                                }
                                Geometry::Polygon(polygon) => {
                                    let mut tiler = TilerBuilder::new(next_res)
                                        .containment_mode(ContainmentMode::Covers)
                                        .build();
                                    let intersection = polygon.intersection(cell_polygon);
                                    if intersection.is_empty() {
                                        continue;
                                    }
                                    for polygon in intersection.0 {
                                        tiler.add(polygon.clone())?;
                                    }
                                    for cell in tiler.into_coverage() {
                                        to_insert.push_back((
                                            item,
                                            Geometry::Polygon(polygon.clone()),
                                            cell,
                                        ));
                                    }
                                }
                                Geometry::MultiPolygon(multi_polygon) => {
                                    for polygon in multi_polygon.0.iter() {
                                        let mut tiler = TilerBuilder::new(next_res)
                                            .containment_mode(ContainmentMode::Covers)
                                            .build();
                                        let intersection = polygon.intersection(cell_polygon);
                                        if intersection.is_empty() {
                                            continue;
                                        }
                                        for polygon in intersection.0 {
                                            tiler.add(polygon.clone())?;
                                        }
                                        for cell in tiler.into_coverage() {
                                            to_insert.push_back((
                                                item,
                                                Geometry::Polygon(polygon.clone()),
                                                cell,
                                            ));
                                        }
                                    }
                                }
                                other => todo!("other {:?}", other),
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 1. We remove all the items by id of the items database
    /// 2. We do a scan of the whole cell database and remove the items from the bitmaps
    /// 3. We do a scan of the whole inner_shape_cell_db and remove the items from the bitmaps
    ///
    /// TODO: We could optimize 2 and 3 by diving into the cells and stopping early when one is empty
    fn remove_deleted_items(&self, wtxn: &mut RwTxn, items: RoaringBitmap) -> Result<()> {
        for item in items.iter() {
            self.item_db().delete(wtxn, &Key::Item(item))?;
        }
        let mut iter = self
            .cell_db()
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter_mut(wtxn, &KeyVariant::Cell)?
            .remap_key_type::<KeyCodec>();
        while let Some(ret) = iter.next() {
            let (key, mut bitmap) = ret?;
            bitmap -= &items;
            // safe because everything is owned

            unsafe {
                if bitmap.is_empty() {
                    iter.del_current();
                } else {
                    iter.put_current(&key, &bitmap)?;
                }
            }
        }
        drop(iter);

        let mut iter = self
            .inner_shape_cell_db()
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter_mut(wtxn, &KeyVariant::InnerShape)?
            .remap_key_type::<KeyCodec>();
        while let Some(ret) = iter.next() {
            let (key, mut bitmap) = ret?;
            bitmap -= &items;
            // safe because everything is owned

            unsafe {
                if bitmap.is_empty() {
                    iter.del_current();
                } else {
                    iter.put_current(&key, &bitmap)?;
                }
            }
        }
        Ok(())
    }

    fn insert_items_at_level_zero(&self, wtxn: &mut RwTxn, items: RoaringBitmap) -> Result<()> {
        // level 0 only have 122 cells => that fits in RAM
        let mut to_insert = HashMap::with_capacity(122);
        // TODO: Could be parallelized very easily, we just have to merge the hashmap at the end or use a shared map
        for item in items.iter() {
            let geojson = self.item_db().get(wtxn, &Key::Item(item))?.unwrap();
            let shape = Geometry::try_from(geojson).unwrap();
            let cells = self.explode_level_zero_geo(wtxn, item, shape)?;
            for cell in cells {
                to_insert
                    .entry(cell)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(item);
            }
        }
        for (cell, items) in to_insert {
            self.cell_db().put(wtxn, &Key::Cell(cell), &items)?;
        }
        Ok(())
    }

    fn explode_level_zero_geo(
        &self,
        wtxn: &mut RwTxn,
        item: ItemId,
        shape: Geometry,
    ) -> Result<Vec<CellIndex>, Error> {
        match shape {
            Geometry::Point(point) => {
                let cell = LatLng::new(point.y(), point.x())
                    .unwrap()
                    .to_cell(Resolution::Zero);
                Ok(vec![cell])
            }
            Geometry::MultiPoint(multi_point) => Ok(multi_point
                .0
                .iter()
                .map(|point| {
                    LatLng::new(point.y(), point.x())
                        .unwrap()
                        .to_cell(Resolution::Zero)
                })
                .collect()),

            Geometry::Polygon(polygon) => {
                let mut tiler = TilerBuilder::new(Resolution::Zero)
                    .containment_mode(ContainmentMode::Covers)
                    .build();
                tiler.add(polygon.clone())?;

                let mut to_insert = Vec::new();
                for cell in tiler.into_coverage() {
                    // If the cell is entirely contained in the polygon, insert directly to inner_shape_cell_db
                    let solvent = h3o::geom::SolventBuilder::new().build();
                    let cell_polygon = solvent.dissolve(Some(cell)).unwrap();
                    let cell_polygon = &cell_polygon.0[0];
                    if polygon.contains(cell_polygon) {
                        let mut bitmap = self
                            .inner_shape_cell_db()
                            .get(wtxn, &Key::InnerShape(cell))?
                            .unwrap_or_default();
                        bitmap.insert(item);
                        self.inner_shape_cell_db()
                            .put(wtxn, &Key::InnerShape(cell), &bitmap)?;
                    } else {
                        // Otherwise use insert_shape_in_cell for partial overlaps
                        to_insert.push(cell);
                    }
                }
                Ok(to_insert)
            }
            Geometry::MultiPolygon(multi_polygon) => {
                let mut to_insert = Vec::new();
                for polygon in multi_polygon.0.iter() {
                    to_insert.extend(self.explode_level_zero_geo(
                        wtxn,
                        item,
                        Geometry::Polygon(polygon.clone()),
                    )?);
                }
                Ok(to_insert)
            }
            Geometry::Rect(_rect) => todo!(),
            Geometry::Triangle(_triangle) => todo!(),

            Geometry::GeometryCollection(_geometry_collection) => todo!(),

            Geometry::Line(_) | Geometry::LineString(_) | Geometry::MultiLineString(_) => {
                panic!("Doesn't support lines")
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

    fn inner_shape_cell_db(&self) -> heed::Database<KeyCodec, RoaringBitmapCodec> {
        self.db.remap_data_type()
    }

    fn updated_db(&self) -> heed::Database<KeyCodec, Unit> {
        self.db.remap_data_type()
    }

    fn deleted_db(&self) -> heed::Database<KeyCodec, Unit> {
        self.db.remap_data_type()
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
        let mut too_large = false;

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
                    if too_large {
                        tiler.add(cell_polygon.clone())?;
                    } else {
                        tiler.add(polygon.clone())?;
                    }

                    let mut cell_number = 0;

                    for cell in tiler.into_coverage() {
                        if already_explored.insert(cell) {
                            to_explore.push_back(cell);
                        }
                        cell_number += 1;
                    }

                    if cell_number > 3 {
                        too_large = true;
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
                Geometry::MultiPoint(multi_point) => {
                    if multi_point.0.iter().any(|point| {
                        polygon.contains(&Coord {
                            x: point.x(),
                            y: point.y(),
                        })
                    }) {
                        ret.insert(item);
                    }
                }

                Geometry::Polygon(poly) => {
                    // If the polygon is contained or intersect with the query polygon, add it
                    if polygon.contains(&poly) || polygon.intersects(&poly) {
                        ret.insert(item);
                    }
                }
                Geometry::MultiPolygon(multi_polygon) => {
                    for poly in multi_polygon.0.iter() {
                        if polygon.contains(poly) || polygon.intersects(poly) {
                            ret.insert(item);
                        }
                    }
                }
                Geometry::Rect(_rect) => todo!(),
                Geometry::Triangle(_triangle) => todo!(),

                Geometry::GeometryCollection(_geometry_collection) => todo!(),

                Geometry::MultiLineString(_) | Geometry::Line(_) | Geometry::LineString(_) => {
                    unreachable!("lines not supported")
                }
            }
        }

        Ok(ret)
    }
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
