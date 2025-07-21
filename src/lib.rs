use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::atomic::Ordering,
};

use ::roaring::RoaringBitmap;
use ::zerometry::{Relation, RelationBetweenShapes, Zerometry};
use geo::{Coord, Densify, Haversine, MultiPolygon};
use geo_types::Polygon;
use geojson::GeoJson;
use h3o::{
    geom::{ContainmentMode, TilerBuilder},
    CellIndex, LatLng, Resolution,
};
use heed::{byteorder::BE, types::U32, Env, RoTxn, RwTxn, Unspecified};
use keys::{Key, KeyCodec, KeyPrefixVariantCodec, KeyVariant};
use steppe::Progress;

mod error;
mod keys;
pub mod roaring;
pub mod zerometry;

#[cfg(test)]
mod test;

pub use crate::error::Error;
use crate::{roaring::RoaringBitmapCodec, zerometry::ZerometryCodec};

pub type MainDb = heed::Database<KeyCodec, Unspecified>;
pub type UpdateDb = heed::Database<U32<BE>, UpdateType>;
pub type ItemId = u32;

steppe::make_enum_progress! {
    pub enum BuildSteps {
        RetrieveUpdatedItems,
        ClearUpdatedItems,
        RetrieveAndClearDeletedItems,
        RemoveDeletedItemsFromDatabase,
        InsertItemsAtLevelZero,
        InsertItemsRecursively,
    }
}
steppe::make_atomic_progress!(Item alias AtomicItemStep => "item");
steppe::make_atomic_progress!(Cell alias AtomicCellStep => "cell");

type Result<O, E = Error> = std::result::Result<O, E>;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum UpdateType {
    Insert = 0,
    Delete = 1,
}

impl<'a> heed::BytesEncode<'a> for UpdateType {
    type EItem = Self;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'a, [u8]>, heed::BoxedError> {
        Ok(Cow::Owned(vec![*item as u8]))
    }
}

impl<'a> heed::BytesDecode<'a> for UpdateType {
    type DItem = Self;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, heed::BoxedError> {
        match bytes {
            [b] if *b == UpdateType::Insert as u8 => Ok(UpdateType::Insert),
            [b] if *b == UpdateType::Delete as u8 => Ok(UpdateType::Delete),
            _ => panic!("Invalid update type {:?}", bytes),
        }
    }
}

#[derive(Clone)]
pub struct Cellulite {
    pub(crate) main: MainDb,
    pub(crate) update: UpdateDb,
    /// After how many elements should we break a cell into sub-cells
    pub threshold: u64,
}

impl Cellulite {
    pub const fn nb_dbs() -> u32 {
        2
    }

    pub const fn default_threshold() -> u64 {
        2
    }

    pub fn create_from_env(env: &Env, wtxn: &mut RwTxn) -> Result<Self> {
        let main = env.create_database(wtxn, Some("cellulite-main"))?;
        let update = env.create_database(wtxn, Some("cellulite-update"))?;
        Ok(Self {
            main,
            update,
            threshold: Self::default_threshold(),
        })
    }

    pub fn from_dbs(main: MainDb, update: UpdateDb) -> Self {
        Self {
            main,
            update,
            threshold: Self::default_threshold(),
        }
    }

    /// Return all the cells used internally in the database
    pub fn inner_db_cells<'a>(
        &self,
        rtxn: &'a RoTxn,
    ) -> Result<impl Iterator<Item = Result<(CellIndex, RoaringBitmap), heed::Error>> + 'a> {
        Ok(self
            .main
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
            .main
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
    pub fn item<'a>(&self, rtxn: &'a RoTxn, item: ItemId) -> Result<Option<Zerometry<'a>>> {
        self.item_db()
            .get(rtxn, &Key::Item(item))
            .map_err(Error::from)
    }

    /// Iterate over all the items in the database
    pub fn items<'a>(
        &self,
        rtxn: &'a RoTxn,
    ) -> Result<impl Iterator<Item = Result<(ItemId, Zerometry<'a>), heed::Error>> + 'a> {
        Ok(self
            .main
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter(rtxn, &KeyVariant::Item)?
            .remap_types::<KeyCodec, ZerometryCodec>()
            .map(|res| {
                res.map(|(key, cell)| {
                    let Key::Item(item) = key else { unreachable!() };
                    (item, cell)
                })
            }))
    }

    pub fn add(&self, wtxn: &mut RwTxn, item: ItemId, geo: &GeoJson) -> Result<()> {
        let geom = geo_types::Geometry::<f64>::try_from(geo.clone()).unwrap();
        self.item_db().put(wtxn, &Key::Item(item), &geom)?;
        self.update.put(wtxn, &item, &UpdateType::Insert)?;
        Ok(())
    }

    pub fn delete(&self, wtxn: &mut RwTxn, item: ItemId) -> Result<()> {
        self.update.put(wtxn, &item, &UpdateType::Delete)?;
        Ok(())
    }

    fn retrieve_and_clear_updated_items(
        &self,
        wtxn: &mut RwTxn,
        progress: &impl Progress,
    ) -> Result<(RoaringBitmap, RoaringBitmap)> {
        progress.update(BuildSteps::RetrieveUpdatedItems);
        let (atomic, step) = AtomicItemStep::new(self.update.len(wtxn)?);
        progress.update(step);

        let mut inserted = RoaringBitmap::new();
        let mut deleted = RoaringBitmap::new();

        let mut iter = self.update.iter(wtxn)?;
        while let Some(ret) = iter.next() {
            match ret? {
                (item, UpdateType::Insert) => inserted.try_push(item).unwrap(),
                (item, UpdateType::Delete) => deleted.try_push(item).unwrap(),
            }
            atomic.fetch_add(1, Ordering::Relaxed);
        }
        drop(iter);
        progress.update(BuildSteps::ClearUpdatedItems);
        self.update.clear(wtxn)?;

        Ok((inserted, deleted))
    }

    /// Indexing is in 4 steps:
    /// 1. We retrieve all the items that have been updated since the last indexing
    /// 2. We remove the deleted items from the database and remove the empty cells at the same time
    ///    TODO: If a cell becomes too small we cannot delete it so the database won't shrink properly but won't be corrupted either
    /// 3. We insert the new items in the database **only at the level 0**
    /// 4. We take each level-zero cell one by one and if it contains new items we insert them in the database in batch at the next level
    ///    TODO: Could be parallelized fairly easily I think
    pub fn build(&self, wtxn: &mut RwTxn, progress: &impl Progress) -> Result<()> {
        // 1.
        let (inserted_items, removed_items) =
            self.retrieve_and_clear_updated_items(wtxn, progress)?;

        // 2.
        self.remove_deleted_items(wtxn, progress, removed_items)?;

        // 3.
        self.insert_items_at_level_zero(wtxn, progress, &inserted_items)?;

        // 4. We have to iterate over all the level-zero cells and insert the new items that are in them in the database at the next level if we need to
        //    TODO: Could be parallelized
        progress.update(BuildSteps::InsertItemsRecursively); // we cannot detail more here
        for cell in CellIndex::base_cells() {
            let bitmap = self
                .cell_db()
                .get(wtxn, &Key::Cell(cell))?
                .unwrap_or_default();
            // Awesome, we don't care about what's in the cell, wether it have multiple levels or not
            if bitmap.len() < self.threshold || bitmap.intersection_len(&inserted_items) == 0 {
                continue;
            }
            self.insert_chunk_of_items_recursively(wtxn, bitmap, cell)?;
        }

        Ok(())
    }

    /// 1. We remove all the items by id of the items database
    /// 2. We do a scan of the whole cell database and remove the items from the bitmaps
    /// 3. We do a scan of the whole inner_shape_cell_db and remove the items from the bitmaps
    ///
    /// TODO: We could optimize 2 and 3 by diving into the cells and stopping early when one is empty
    fn remove_deleted_items(
        &self,
        wtxn: &mut RwTxn,
        progress: &impl Progress,
        items: RoaringBitmap,
    ) -> Result<()> {
        progress.update(BuildSteps::RemoveDeletedItemsFromDatabase);
        steppe::make_enum_progress! {
            pub enum RemoveDeletedItemsSteps {
                RemoveDeletedItemsFromItemsDatabase,
                RemoveDeletedItemsFromCellsDatabase,
                RemoveDeletedItemsFromInnerShapeCellsDatabase,
            }
        }

        progress.update(RemoveDeletedItemsSteps::RemoveDeletedItemsFromItemsDatabase);
        let (atomic, step) = AtomicItemStep::new(items.len());
        progress.update(step.clone());
        for item in items.iter() {
            self.item_db().delete(wtxn, &Key::Item(item))?;
            atomic.fetch_add(1, Ordering::Relaxed);
        }

        progress.update(RemoveDeletedItemsSteps::RemoveDeletedItemsFromCellsDatabase);
        atomic.store(0, Ordering::Relaxed);
        progress.update(step.clone());
        let mut iter = self
            .cell_db()
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter_mut(wtxn, &KeyVariant::Cell)?
            .remap_key_type::<KeyCodec>();
        while let Some(ret) = iter.next() {
            let (key, mut bitmap) = ret?;
            let len = bitmap.len();
            bitmap -= &items;
            let removed = len - bitmap.len();
            atomic.fetch_add(removed, Ordering::Relaxed);

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

        progress.update(RemoveDeletedItemsSteps::RemoveDeletedItemsFromInnerShapeCellsDatabase);
        atomic.store(0, Ordering::Relaxed);
        progress.update(step);
        let mut iter = self
            .inner_shape_cell_db()
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter_mut(wtxn, &KeyVariant::InnerShape)?
            .remap_key_type::<KeyCodec>();
        while let Some(ret) = iter.next() {
            let (key, mut bitmap) = ret?;
            let len = bitmap.len();
            bitmap -= &items;
            let removed = len - bitmap.len();
            atomic.fetch_add(removed, Ordering::Relaxed);

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

    fn insert_items_at_level_zero(
        &self,
        wtxn: &mut RwTxn,
        progress: &impl Progress,
        items: &RoaringBitmap,
    ) -> Result<()> {
        progress.update(BuildSteps::InsertItemsAtLevelZero);
        steppe::make_enum_progress! {
            pub enum InsertItemsAtLevelZeroSteps {
                InsertItemsAtLevelZero,
                WriteCellsToDatabase,
            }
        }
        let (atomic, step) = AtomicItemStep::new(items.len());
        progress.update(step);
        // level 0 only have 122 cells => that fits in RAM
        let mut to_insert = HashMap::with_capacity(122);
        let mut belly_cells = HashMap::new();
        // TODO: Could be parallelized very easily, we just have to merge the hashmap at the end or use a shared map
        for item in items.iter() {
            let shape = self
                .item_db()
                .get(wtxn, &Key::Item(item))?
                .ok_or_else(|| Error::InternalDocIdMissing(item, pos!()))?;
            let (cells, belly) = self.explode_level_zero_geo(wtxn, item, shape)?;
            for cell in cells {
                to_insert
                    .entry(cell)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(item);
            }
            for cell in belly {
                belly_cells
                    .entry(cell)
                    .or_insert_with(RoaringBitmap::new)
                    .insert(item);
            }
            atomic.fetch_add(1, Ordering::Relaxed);
        }
        progress.update(InsertItemsAtLevelZeroSteps::WriteCellsToDatabase);
        let (atomic, step) = AtomicCellStep::new(to_insert.len() as u64);
        progress.update(step);
        for (cell, items) in to_insert {
            let mut bitmap = self
                .cell_db()
                .get(wtxn, &Key::Cell(cell))?
                .unwrap_or_default();
            bitmap |= items;
            self.cell_db().put(wtxn, &Key::Cell(cell), &bitmap)?;
            atomic.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn explode_level_zero_geo(
        &self,
        rtxn: &RoTxn,
        item: ItemId,
        shape: Zerometry,
    ) -> Result<(Vec<CellIndex>, Vec<CellIndex>), Error> {
        match shape {
            Zerometry::Point(point) => {
                let cell = LatLng::new(point.lat(), point.lng())
                    .unwrap()
                    .to_cell(Resolution::Zero);
                Ok((vec![cell], vec![]))
            }
            Zerometry::MultiPoints(multi_point) => {
                let to_insert = multi_point
                    .coords()
                    .iter()
                    .map(|point| {
                        LatLng::new(point.lat(), point.lng())
                            .unwrap()
                            .to_cell(Resolution::Zero)
                    })
                    .collect();
                Ok((to_insert, vec![]))
            }

            Zerometry::Polygon(polygon) => {
                let mut tiler = TilerBuilder::new(Resolution::Zero)
                    .containment_mode(ContainmentMode::Covers)
                    .build();
                tiler.add(polygon.to_geo())?;

                let mut to_insert = Vec::new();
                let mut belly_cells = Vec::new();
                for cell in tiler.into_coverage() {
                    // If the cell is entirely contained in the polygon, insert directly to inner_shape_cell_db
                    let solvent = h3o::geom::SolventBuilder::new().build();
                    let cell_polygon = solvent.dissolve(Some(cell)).unwrap();
                    // We should use the MultiPolygon and be strict about the containment. All parts must be contained
                    let cell_polygon = &cell_polygon.0[0];
                    if polygon.contains(cell_polygon) {
                        belly_cells.push(cell);
                    } else {
                        // Otherwise use insert_shape_in_cell for partial overlaps
                        to_insert.push(cell);
                    }
                }
                Ok((to_insert, belly_cells))
            }
            Zerometry::MultiPolygon(multi_polygon) => {
                let mut to_insert = Vec::new();
                let mut belly_cells = Vec::new();
                for polygon in multi_polygon.polygons() {
                    let (cells, belly) = self.explode_level_zero_geo(rtxn, item, polygon.into())?;
                    to_insert.extend(cells);
                    belly_cells.extend(belly);
                }
                Ok((to_insert, belly_cells))
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
    fn insert_chunk_of_items_recursively<'a>(
        &self,
        wtxn: &'a mut RwTxn,
        items: RoaringBitmap,
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
            for item in items.iter() {
                let shape = self
                    .item_db()
                    .get(wtxn, &Key::Item(item))?
                    .ok_or_else(|| Error::InternalDocIdMissing(item, pos!()))?;
                match shape.relation(&cell_shape) {
                    Relation::Contains => {
                        let entry = to_insert_in_belly
                            .entry(cell)
                            .or_insert_with(RoaringBitmap::new);
                        entry.insert(item);
                    }
                    Relation::Intersects | Relation::Contained => {
                        let entry = to_insert.entry(cell).or_insert_with(RoaringBitmap::new);
                        entry.insert(item);
                    }
                    Relation::Disjoint => (),
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

        for (cell, mut items_to_insert) in to_insert {
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
                    let shape = self
                        .item_db()
                        .get(wtxn, &Key::Item(item_id))?
                        .ok_or_else(|| Error::InternalDocIdMissing(item_id, pos!()))?;

                    match shape.relation(&cell_shape) {
                        Relation::Contains => {
                            belly_items.insert(item_id);
                        }
                        Relation::Intersects | Relation::Contained => {
                            items_to_insert.insert(item_id);
                        }
                        Relation::Disjoint => (),
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

    fn item_db(&self) -> heed::Database<KeyCodec, ZerometryCodec> {
        self.main.remap_data_type()
    }

    fn cell_db(&self) -> heed::Database<KeyCodec, RoaringBitmapCodec> {
        self.main.remap_data_type()
    }

    fn inner_shape_cell_db(&self) -> heed::Database<KeyCodec, RoaringBitmapCodec> {
        self.main.remap_data_type()
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
        let polygon = Haversine.densify(polygon, 1_000.0);
        let mut tiler = TilerBuilder::new(Resolution::Zero)
            .containment_mode(ContainmentMode::Covers)
            .build();
        tiler.add(polygon.clone())?;

        let mut ret = RoaringBitmap::new();
        let mut double_check = RoaringBitmap::new();
        let mut to_explore: VecDeque<_> = tiler.into_coverage().collect();
        let mut already_explored: HashSet<CellIndex> = HashSet::with_capacity(to_explore.len());
        let mut too_large = false;

        while let Some(cell) = to_explore.pop_front() {
            if !already_explored.insert(cell) {
                continue;
            }

            let Some(items) = self.cell_db().get(rtxn, &Key::Cell(cell))? else {
                (inspector)((FilteringStep::NotPresentInDB, cell));
                continue;
            };

            let solvent = h3o::geom::SolventBuilder::new().build();
            let cell_polygon = solvent.dissolve(Some(cell)).unwrap();

            // let cell_polygon = bounding_box(cell);
            let cell_polygon = &cell_polygon.0[0];
            if geo::Contains::contains(&polygon, cell_polygon) {
                (inspector)((FilteringStep::Returned, cell));
                ret |= items;
            } else if geo::Intersects::intersects(&polygon, cell_polygon) {
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
                        if !already_explored.contains(&cell) {
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
            let shape = self.item_db().get(rtxn, &Key::Item(item))?.unwrap();
            match shape {
                Zerometry::Point(point) => {
                    if geo::Contains::contains(
                        &polygon,
                        &Coord {
                            x: point.x(),
                            y: point.y(),
                        },
                    ) {
                        ret.insert(item);
                    }
                }
                Zerometry::MultiPoints(multi_point) => {
                    if multi_point.coords().iter().any(|point| {
                        geo::Contains::contains(
                            &polygon,
                            &Coord {
                                x: point.x(),
                                y: point.y(),
                            },
                        )
                    }) {
                        ret.insert(item);
                    }
                }

                Zerometry::Polygon(poly) => {
                    // If the polygon is contained or intersect with the query polygon, add it
                    match polygon.relation(&poly) {
                        Relation::Contains | Relation::Intersects => {
                            ret.insert(item);
                        }
                        _ => (),
                    }
                }
                Zerometry::MultiPolygon(multi_polygon) => {
                    for poly in multi_polygon.polygons() {
                        match polygon.relation(&poly) {
                            Relation::Contains | Relation::Intersects => {
                                ret.insert(item);
                            }
                            _ => (),
                        }
                    }
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
