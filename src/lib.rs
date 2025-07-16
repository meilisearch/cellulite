use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use ::roaring::RoaringBitmap;
use geo::{BooleanOps, Contains, Coord, Geometry, HasDimensions, Intersects, MultiPolygon};
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
    // User errors
    #[error("Document with id `{0}` contains a {1} but only `Geometry` type is supported")]
    InvalidGeoJsonTypeFormat(ItemId, &'static str),
    #[error("Document with id `{0}` contains a {1} but only `Point`, `Polygon`, `MultiPoint` and `MultiPolygon` types are supported")]
    InvalidGeometryTypeFormat(ItemId, &'static str),

    // External errors, sometimes it's a user error and sometimes it's not
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    InvalidLatLng(#[from] InvalidLatLng),
    #[error(transparent)]
    InvalidGeometry(#[from] InvalidGeometry),
    #[error(transparent)]
    InvalidGeoJson(#[from] geojson::Error),

    // Internal errors
    #[error("Internal error: unexpected document id `{0}` missing at `{1}`")]
    InternalDocIdMissing(ItemId, String),
}

macro_rules! pos {
    () => {
        format!("{}:{}:{}", file!(), line!(), column!())
    };
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

    fn validate_geojson(&self, item: ItemId, geo: &GeoJson) -> Result<()> {
        match geo {
            GeoJson::Geometry(geometry) => match geometry.value {
                geojson::Value::Point(_)
                | geojson::Value::Polygon(_)
                | geojson::Value::MultiPoint(_)
                | geojson::Value::MultiPolygon(_) => Ok(()),
                geojson::Value::LineString(_) | geojson::Value::MultiLineString(_) => {
                    Err(Error::InvalidGeometryTypeFormat(item, "LineString"))
                }
                geojson::Value::GeometryCollection(_) => {
                    Err(Error::InvalidGeometryTypeFormat(item, "GeometryCollection"))
                }
            },
            GeoJson::Feature(_feature) => Err(Error::InvalidGeoJsonTypeFormat(item, "Feature")),
            GeoJson::FeatureCollection(_feature_collection) => {
                Err(Error::InvalidGeoJsonTypeFormat(item, "FeatureCollection"))
            }
        }
    }

    pub fn add(&self, wtxn: &mut RwTxn, item: ItemId, geo: &GeoJson) -> Result<()> {
        self.validate_geojson(item, geo)?;
        self.item_db().put(wtxn, &Key::Item(item), geo)?;
        self.updated_db().put(wtxn, &Key::Update(item), &())?;
        self.deleted_db().delete(wtxn, &Key::Remove(item))?;
        Ok(())
    }

    pub fn delete(&self, wtxn: &mut RwTxn, item: ItemId) -> Result<()> {
        self.deleted_db().put(wtxn, &Key::Remove(item), &())?;
        self.updated_db().delete(wtxn, &Key::Update(item))?;
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
            bitmap.try_push(item).unwrap();
            // safe because we own the ItemId
            unsafe {
                iter.del_current()?;
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
            bitmap.try_push(item).unwrap();
            // safe because we own the ItemId
            unsafe {
                iter.del_current()?;
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
        self.insert_items_at_level_zero(wtxn, &inserted_items)?;

        // 4. We have to iterate over all the level-zero cells and insert the new items that are in them in the database at the next level if we need to
        //    TODO: Could be parallelized
        for cell in CellIndex::base_cells() {
            let bitmap = self
                .cell_db()
                .get(wtxn, &Key::Cell(cell))?
                .unwrap_or_default();
            // Awesome, we don't care about what's in the cell, wether it have multiple levels or not
            if bitmap.len() < self.threshold || bitmap.intersection_len(&inserted_items) == 0 {
                continue;
            }
            // TODO: Unbounded RAM consumption:
            //   - We could push the items in a bumpalo and stops when we don't have enough space left
            //   - Or use an LRU cache and re-fetch them from the database when needed (but it'll be slower imo)
            let mut items = Vec::new();
            for item in bitmap.iter() {
                let geojson = self
                    .item_db()
                    .get(wtxn, &Key::Item(item))?
                    .ok_or_else(|| Error::InternalDocIdMissing(item, pos!()))?;
                let shape = Geometry::try_from(geojson)?;
                items.push((item, shape));
            }
            self.insert_chunk_of_items_recursively(wtxn, items, cell)?;
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
                    iter.del_current()?;
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
                    iter.del_current()?;
                } else {
                    iter.put_current(&key, &bitmap)?;
                }
            }
        }
        Ok(())
    }

    fn insert_items_at_level_zero(&self, wtxn: &mut RwTxn, items: &RoaringBitmap) -> Result<()> {
        // level 0 only have 122 cells => that fits in RAM
        let mut to_insert = HashMap::with_capacity(122);
        // TODO: Could be parallelized very easily, we just have to merge the hashmap at the end or use a shared map
        for item in items.iter() {
            let geojson = self
                .item_db()
                .get(wtxn, &Key::Item(item))?
                .ok_or_else(|| Error::InternalDocIdMissing(item, pos!()))?;
            let shape = Geometry::try_from(geojson)?;
            let cells = self.explode_level_zero_geo(wtxn, item, shape)?;
            for cell in cells {
                to_insert
                    .entry(cell)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(item);
            }
        }
        for (cell, items) in to_insert {
            let mut bitmap = self
                .cell_db()
                .get(wtxn, &Key::Cell(cell))?
                .unwrap_or_default();
            bitmap |= items;
            self.cell_db().put(wtxn, &Key::Cell(cell), &bitmap)?;
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

    /// To insert a bunch of items in a cell we have to:
    /// 1. Get all the possible children cells
    /// 2. See which items fits in which cells by doing an intersection between the shape and the child cell
    /// 3. Insert the cells in the database
    /// 4. For all the cells that are too large:
    ///  - If it was already too large, repeat the process with the next resolution
    ///  - If it **just became** too large. Retrieve all the items it contains and add them to the list of items to handle
    ///    Call ourselves recursively on the next resolution
    fn insert_chunk_of_items_recursively(
        &self,
        wtxn: &mut RwTxn,
        items: Vec<(ItemId, Geometry)>,
        cell: CellIndex,
    ) -> Result<()> {
        // 1. If we cannot increase the resolution, we are done
        let Some(children_cells) = get_children_cells(cell)? else {
            return Ok(());
        };
        // 2.
        let mut to_insert = HashMap::with_capacity(children_cells.len());
        let mut to_insert_in_belly = HashMap::new();

        for &cell in children_cells.iter() {
            let cell_shape = get_cell_shape(cell);
            for (item, shape) in items.iter() {
                match shape_relation_with_cell(&shape, &cell_shape) {
                    RelationToCell::ContainsCell => {
                        let entry = to_insert_in_belly
                            .entry(cell)
                            .or_insert_with(RoaringBitmap::new);
                        entry.insert(*item);
                    }
                    RelationToCell::IntersectsCell(shape) => {
                        let entry = to_insert
                            .entry(cell)
                            .or_insert_with(|| (RoaringBitmap::new(), Vec::new()));
                        entry.0.insert(*item);
                        entry.1.push((*item, shape));
                    }
                    RelationToCell::NoRelation => (),
                }
            }
        }

        // 3.
        for (cell, items) in to_insert_in_belly {
            let mut bitmap = self
                .inner_shape_cell_db()
                .get(wtxn, &Key::Cell(cell))?
                .unwrap_or_default();
            bitmap |= items;
            self.inner_shape_cell_db()
                .put(wtxn, &Key::Cell(cell), &bitmap)?;
        }

        for (cell, (items, mut items_to_insert)) in to_insert {
            let original_bitmap = self
                .cell_db()
                .get(wtxn, &Key::Cell(cell))?
                .unwrap_or_default();
            let new_bitmap = &original_bitmap | &items;
            self.cell_db().put(wtxn, &Key::Cell(cell), &new_bitmap)?;
            if original_bitmap.len() >= self.threshold {
                // if we were already too large we can immediately jump to the next resolution
                self.insert_chunk_of_items_recursively(wtxn, items_to_insert, cell)?;
            } else if new_bitmap.len() >= self.threshold {
                let cell_shape = get_cell_shape(cell);
                let mut belly_items = RoaringBitmap::new();

                // If we just became too large, we have to retrieve the items that were already in the database insert them at the next resolution
                for item_id in original_bitmap.iter() {
                    let item = self
                        .item_db()
                        .get(wtxn, &Key::Item(item_id))?
                        .ok_or_else(|| Error::InternalDocIdMissing(item_id, pos!()))?;
                    let item = Geometry::try_from(item)?;
                    match shape_relation_with_cell(&item, &cell_shape) {
                        RelationToCell::ContainsCell => {
                            belly_items.insert(item_id);
                        }
                        RelationToCell::IntersectsCell(shape) => {
                            items_to_insert.push((item_id, shape));
                        }
                        RelationToCell::NoRelation => (),
                    }
                }

                let mut inner_shape_cells = self
                    .inner_shape_cell_db()
                    .get(wtxn, &Key::Cell(cell))?
                    .unwrap_or_default();
                inner_shape_cells |= belly_items;
                self.inner_shape_cell_db()
                    .put(wtxn, &Key::Cell(cell), &inner_shape_cells)?;

                self.insert_chunk_of_items_recursively(wtxn, items_to_insert, cell)?;
            }
            // If we are not too large, we have nothing else to do yaay
        }
        Ok(())
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

enum RelationToCell {
    ContainsCell,
    IntersectsCell(Geometry),
    NoRelation,
}

/// Compute the relation between a shape and a cell shape.
/// It modifies the shape on the fly to keep the part that fits within the cell.
fn shape_relation_with_cell(shape: &Geometry, cell_shape: &MultiPolygon) -> RelationToCell {
    match shape {
        Geometry::Point(point) => {
            if cell_shape.contains(point) {
                RelationToCell::IntersectsCell(Geometry::Point(*point))
            } else {
                RelationToCell::NoRelation
            }
        }
        Geometry::MultiPoint(multi_point) => {
            let mut ret = Vec::new();
            for point in multi_point.iter() {
                if cell_shape.contains(point) {
                    ret.push(*point);
                }
            }
            if ret.is_empty() {
                RelationToCell::NoRelation
            } else {
                RelationToCell::IntersectsCell(Geometry::MultiPoint(ret.into()))
            }
        }
        Geometry::Polygon(poly) => {
            if poly.contains(cell_shape) {
                RelationToCell::ContainsCell
            } else {
                let intersection = poly.intersection(cell_shape);
                if intersection.is_empty() {
                    RelationToCell::NoRelation
                } else {
                    RelationToCell::IntersectsCell(intersection.into())
                }
            }
        }
        Geometry::MultiPolygon(multi_polygon) => {
            let mut ret = Vec::new();
            for poly in multi_polygon.iter() {
                if poly.contains(cell_shape) {
                    return RelationToCell::ContainsCell;
                }
                let mut intersection = poly.intersection(cell_shape);
                if !intersection.is_empty() {
                    ret.append(&mut intersection.0);
                }
            }
            if ret.is_empty() {
                RelationToCell::NoRelation
            } else {
                RelationToCell::IntersectsCell(Geometry::MultiPolygon(ret.into()))
            }
        }
        Geometry::Rect(rect) => shape_relation_with_cell(&rect.to_polygon().into(), cell_shape),
        Geometry::Triangle(triangle) => {
            shape_relation_with_cell(&triangle.to_polygon().into(), cell_shape)
        }
        Geometry::GeometryCollection(_geometry_collection) => todo!(),
        Geometry::Line(_) | Geometry::LineString(_) | Geometry::MultiLineString(_) => {
            unreachable!("lines not supported")
        }
    }
}

fn get_cell_shape(cell: CellIndex) -> MultiPolygon {
    let cell_polygon = h3o::geom::SolventBuilder::new()
        .build()
        .dissolve(Some(cell))
        .unwrap();
    cell_polygon
}

/// Return None if we cannot increase the resolution
/// Otherwise, return the children cells in a very non-efficient way
/// Note: We cannot use the `get_children_cells` function because it doesn't return the full coverage of our cells and leaves holes
/// TODO: Optimize this to avoid the whole dissolve + tiler thingy. We can probably do better with the grid_disk at distance 1 or 2 idk
fn get_children_cells(cell: CellIndex) -> Result<Option<Vec<CellIndex>>, Error> {
    let Some(next_res) = cell.resolution().succ() else {
        return Ok(None);
    };
    let cell_polygon = get_cell_shape(cell);
    let mut tiler = TilerBuilder::new(next_res)
        .containment_mode(ContainmentMode::Covers)
        .build();
    for polygon in cell_polygon.0.into_iter() {
        tiler.add(polygon)?;
    }
    Ok(Some(tiler.into_coverage().collect()))
}
