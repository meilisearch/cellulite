use std::{cell::RefCell, collections::HashMap, sync::atomic::Ordering};

use crate::{
    AtomicCellStep, AtomicItemStep, BuildSteps, ItemId, Result,
    keys::{CellKeyCodec, KeyPrefixVariantCodec, KeyVariant, UpdateType},
    metadata::Version,
    pos,
};
use geo::MultiPolygon;
use h3o::{
    CellIndex, LatLng, Resolution,
    geom::{ContainmentMode, PlotterBuilder, TilerBuilder},
};
use heed::{RoTxn, RwTxn};
use intmap::IntMap;
use rayon::iter::{ParallelBridge, ParallelIterator};
use roaring::RoaringBitmap;
use steppe::Progress;
use thread_local::ThreadLocal;
use zerometry::{InputRelation, RelationBetweenShapes, Zerometry};

use crate::{Cellulite, Error, keys::Key};

impl Cellulite {
    fn retrieve_frozen_items<'a>(
        &self,
        rtxn: &'a RoTxn,
        cancel: impl Fn() -> bool + Send + Sync,
    ) -> Result<FrozenItems<'a>> {
        let mut items = IntMap::with_capacity(self.item.len(rtxn)? as usize);
        for ret in self.item.iter(rtxn)? {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
            let (k, v) = ret?;
            items.insert(k, v);
        }
        Ok(FrozenItems { items })
    }

    fn retrieve_and_clear_updated_items(
        &self,
        wtxn: &mut RwTxn,
        cancel: impl Fn() -> bool + Send + Sync,
        progress: &impl Progress,
    ) -> Result<(RoaringBitmap, RoaringBitmap)> {
        progress.update(BuildSteps::RetrieveUpdatedItems);
        let (atomic, step) = AtomicItemStep::new(self.update.len(wtxn)?);
        progress.update(step);

        let mut inserted = RoaringBitmap::new();
        let mut deleted = RoaringBitmap::new();

        for ret in self.update.iter(wtxn)? {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
            match ret? {
                (item, UpdateType::Insert) => inserted.try_push(item).unwrap(),
                (item, UpdateType::Delete) => deleted.try_push(item).unwrap(),
            }
            atomic.fetch_add(1, Ordering::Relaxed);
        }
        progress.update(BuildSteps::ClearUpdatedItems);
        self.update.clear(wtxn)?;

        Ok((inserted, deleted))
    }

    /// Build all the internal structure required to query the database.
    // Indexing is in 4 steps:
    // 1. We retrieve all the items that have been updated since the last indexing
    // 2. We remove the deleted items from the database and remove the empty cells at the same time
    //    TODO: If a cell becomes too small we cannot delete it so the database won't shrink properly but won't be corrupted either
    // 3. We insert the new items in the database **only at the level 0**
    // 4. We take each level-zero cell one by one and if it contains new items we insert them in the database in batch at the next level
    //    TODO: Could be parallelized fairly easily I think
    pub fn build(
        &self,
        wtxn: &mut RwTxn,
        cancel: &(impl Fn() -> bool + Send + Sync),
        progress: &impl Progress,
    ) -> Result<()> {
        let db_version = self.get_version(wtxn)?;
        if db_version != Version::default() {
            return Err(Error::VersionMismatchOnBuild(db_version));
        }

        // 1.
        let (inserted_items, removed_items) =
            self.retrieve_and_clear_updated_items(wtxn, cancel, progress)?;

        // 2.
        self.remove_deleted_items(wtxn, cancel, progress, removed_items)?;

        // 3.0
        let frozen_items = self.retrieve_frozen_items(wtxn, cancel)?;
        // currently heed doesn't know that writing in a database doesn't invalidate the pointers in another
        let frozen_items: FrozenItems<'static> = unsafe { std::mem::transmute(frozen_items) };

        // 3.1
        self.insert_items_at_level_zero(wtxn, cancel, progress, &inserted_items, &frozen_items)?;

        // 4. We have to iterate over all the level-zero cells and insert the new items that are in them in the database at the next level if we need to
        //    TODO: Could be parallelized
        progress.update(BuildSteps::InsertItemsRecursively); // we cannot detail more here
        for cell in CellIndex::base_cells() {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
            let bitmap = self
                .cell_db()
                .get(wtxn, &Key::Cell(cell))?
                .unwrap_or_default();
            // Awesome, we don't care about what's in the cell, wether it have multiple levels or not
            if bitmap.len() < self.threshold || bitmap.intersection_len(&inserted_items) == 0 {
                continue;
            }
            self.insert_chunk_of_items_recursively(wtxn, cancel, bitmap, cell, &frozen_items)?;
        }

        progress.update(BuildSteps::UpdateTheMetadata);
        self.set_version(wtxn, &Version::default())?;

        Ok(())
    }

    /// 1. We remove all the items by id of the items database
    /// 2. We do a scan of the whole cell database and remove the items from the bitmaps
    /// 3. We do a scan of the whole belly_cell_db and remove the items from the bitmaps
    ///
    /// TODO: We could optimize 2 and 3 by diving into the cells and stopping early when one is empty
    fn remove_deleted_items(
        &self,
        wtxn: &mut RwTxn,
        cancel: impl Fn() -> bool + Send + Sync,
        progress: &impl Progress,
        items: RoaringBitmap,
    ) -> Result<()> {
        progress.update(BuildSteps::RemoveDeletedItemsFromDatabase);
        steppe::make_enum_progress! {
            pub enum RemoveDeletedItemsSteps {
                RemoveDeletedItemsFromItemsDatabase,
                RemoveDeletedItemsFromCellsDatabase,
                RemoveDeletedItemsFromBellyCellsDatabase,
            }
        }

        progress.update(RemoveDeletedItemsSteps::RemoveDeletedItemsFromItemsDatabase);
        let (atomic, step) = AtomicItemStep::new(items.len());
        progress.update(step.clone());
        for item in items.iter() {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
            self.item_db().delete(wtxn, &item)?;
            atomic.fetch_add(1, Ordering::Relaxed);
        }

        progress.update(RemoveDeletedItemsSteps::RemoveDeletedItemsFromCellsDatabase);
        atomic.store(0, Ordering::Relaxed);
        progress.update(step.clone());
        let mut iter = self
            .cell_db()
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter_mut(wtxn, &KeyVariant::Cell)?
            .remap_key_type::<CellKeyCodec>();
        while let Some(ret) = iter.next() {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
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

        progress.update(RemoveDeletedItemsSteps::RemoveDeletedItemsFromBellyCellsDatabase);
        atomic.store(0, Ordering::Relaxed);
        progress.update(step);
        let mut iter = self
            .cell_db()
            .remap_key_type::<KeyPrefixVariantCodec>()
            .prefix_iter_mut(wtxn, &KeyVariant::Belly)?
            .remap_key_type::<CellKeyCodec>();
        while let Some(ret) = iter.next() {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
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
        cancel: impl Fn() -> bool + Send + Sync,
        progress: &impl Progress,
        items: &RoaringBitmap,
        frozen_items: &FrozenItems<'static>,
    ) -> Result<()> {
        progress.update(BuildSteps::InsertItemsAtLevelZero);
        steppe::make_enum_progress! {
            pub enum InsertItemsAtLevelZeroSteps {
                SplitItemsToCells,
                MergeCellsMap,
                WriteCellsToDatabase,
            }
        }
        progress.update(InsertItemsAtLevelZeroSteps::SplitItemsToCells);
        let (atomic, step) = AtomicItemStep::new(items.len());
        progress.update(step);

        let tls_maps: ThreadLocal<RefCell<(HashMap<_, _>, HashMap<_, _>)>> = ThreadLocal::new();
        let tls_vecs: ThreadLocal<RefCell<(Vec<_>, Vec<_>)>> = ThreadLocal::new();

        items
            .iter()
            .par_bridge()
            .try_for_each(|item| -> Result<_> {
                if cancel() {
                    return Err(Error::BuildCanceled);
                }
                let (cells_map, belly_map) = &mut *tls_maps.get_or_default().borrow_mut();
                let (cells_vec, belly_vec) = &mut *tls_vecs.get_or_default().borrow_mut();
                cells_vec.clear();
                belly_vec.clear();

                let shape = frozen_items
                    .get(item)
                    .ok_or_else(|| Error::InternalDocIdMissing(item, pos!()))?;
                Self::explode_level_zero_geo(item, shape, cells_vec, belly_vec)?;
                for cell in cells_vec {
                    cells_map
                        .entry(*cell)
                        .or_insert_with(RoaringBitmap::new)
                        .insert(item);
                }
                for cell in belly_vec {
                    belly_map
                        .entry(*cell)
                        .or_insert_with(RoaringBitmap::new)
                        .insert(item);
                }
                atomic.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })?;
        progress.update(InsertItemsAtLevelZeroSteps::MergeCellsMap);
        let (to_insert, belly) = tls_maps
            .into_iter()
            .par_bridge()
            .map(|refcell| refcell.into_inner())
            .reduce(
                Default::default,
                |(mut l_insert, mut l_belly), (r_insert, r_belly)| {
                    for (k, v) in r_insert {
                        *l_insert.entry(k).or_default() |= v;
                    }
                    for (k, v) in r_belly {
                        *l_belly.entry(k).or_default() |= v;
                    }
                    (l_insert, l_belly)
                },
            );
        progress.update(InsertItemsAtLevelZeroSteps::WriteCellsToDatabase);
        let (atomic, step) = AtomicCellStep::new(to_insert.len() as u64 + belly.len() as u64);
        progress.update(step);
        for (cell, items) in to_insert {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
            let mut bitmap = self
                .cell_db()
                .get(wtxn, &Key::Cell(cell))?
                .unwrap_or_default();
            bitmap |= items;
            self.cell_db().put(wtxn, &Key::Cell(cell), &bitmap)?;
            atomic.fetch_add(1, Ordering::Relaxed);
        }
        for (cell, items) in belly {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
            let mut bitmap = self
                .cell_db()
                .get(wtxn, &Key::Belly(cell))?
                .unwrap_or_default();
            bitmap |= items;
            self.cell_db().put(wtxn, &Key::Belly(cell), &bitmap)?;
            atomic.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    fn explode_level_zero_geo(
        // only used for error handling
        item: ItemId,
        shape: Zerometry,
        cells: &mut Vec<CellIndex>,
        belly: &mut Vec<CellIndex>,
    ) -> Result<()> {
        match shape {
            Zerometry::Point(point) => {
                let cell = LatLng::new(point.lat(), point.lng())
                    .unwrap()
                    .to_cell(Resolution::Zero);
                cells.push(cell);
            }
            Zerometry::MultiPoints(multi_point) => {
                for point in multi_point.points() {
                    let cell = LatLng::new(point.lat(), point.lng())
                        .unwrap()
                        .to_cell(Resolution::Zero);
                    cells.push(cell);
                }
            }
            Zerometry::Polygon(polygon) => {
                let mut tiler = TilerBuilder::new(Resolution::Zero)
                    .containment_mode(ContainmentMode::Covers)
                    .build();
                tiler.add(polygon.to_geo())?;

                for cell in tiler.into_coverage() {
                    // If the cell is entirely contained in the polygon, insert directly to belly_cell_db
                    let cell_polygon = MultiPolygon::from(cell);
                    if polygon.contains(&cell_polygon) {
                        belly.push(cell);
                    } else {
                        // Otherwise use insert_shape_in_cell for partial overlaps
                        cells.push(cell);
                    }
                }
            }
            Zerometry::MultiPolygon(multi_polygon) => {
                for polygon in multi_polygon.polygons() {
                    Self::explode_level_zero_geo(item, polygon.into(), cells, belly)?;
                }
            }
            Zerometry::Line(line) => {
                let mut plotter = PlotterBuilder::new(Resolution::Zero).build();
                plotter.add_batch(line.to_geo().lines()).unwrap();

                for cell in plotter.plot() {
                    let ret_cells = cell.map_err(|err| {
                        Error::CannotConvertLineToCell(item, err, format!("{line:?}"))
                    })?;

                    cells.push(ret_cells);
                }
            }
            Zerometry::MultiLines(multi_lines) => {
                let mut plotter = PlotterBuilder::new(Resolution::Zero).build();
                for line in multi_lines.lines() {
                    plotter.add_batch(line.to_geo().lines()).unwrap();
                }

                for cell in plotter.plot() {
                    let ret_cells = cell.map_err(|err| {
                        Error::CannotConvertLineToCell(item, err, format!("{multi_lines:?}"))
                    })?;

                    cells.push(ret_cells);
                }
            }
            Zerometry::Collection(collection) => {
                Self::explode_level_zero_geo(
                    item,
                    Zerometry::MultiPoints(collection.points()),
                    cells,
                    belly,
                )?;
                Self::explode_level_zero_geo(
                    item,
                    Zerometry::MultiLines(collection.lines()),
                    cells,
                    belly,
                )?;
                Self::explode_level_zero_geo(
                    item,
                    Zerometry::MultiPolygon(collection.polygons()),
                    cells,
                    belly,
                )?;

                cells.sort_unstable();
                belly.sort_unstable();

                cells.dedup();
                belly.dedup();
            }
        };
        Ok(())
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
        cancel: &(impl Fn() -> bool + Send + Sync),
        items: RoaringBitmap,
        cell: CellIndex,
        frozen_items: &FrozenItems<'static>,
    ) -> Result<()> {
        // 1. If we cannot increase the resolution, we are done
        let Some(children_cells) = get_children_cells(cell)? else {
            return Ok(());
        };
        // 2.
        let mut to_insert = HashMap::with_capacity(children_cells.len());
        let mut to_insert_in_belly = HashMap::new();

        for &cell in children_cells.iter() {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
            let cell_shape = get_cell_shape(cell);
            for item in items.iter() {
                let shape = frozen_items
                    .get(item)
                    .ok_or_else(|| Error::InternalDocIdMissing(item, pos!()))?;
                let relation = shape.relation(
                    &cell_shape,
                    InputRelation {
                        // we don't need to know if we're being strictly contained or not
                        strict_contained: false,
                        ..InputRelation::all()
                    },
                );
                if relation.strict_contains.unwrap_or_default() {
                    let entry = to_insert_in_belly
                        .entry(cell)
                        .or_insert_with(RoaringBitmap::new);
                    entry.insert(item);
                } else if relation.any_relation() {
                    let entry = to_insert.entry(cell).or_insert_with(RoaringBitmap::new);
                    entry.insert(item);
                }
            }
        }

        // 3.
        for (cell, items) in to_insert_in_belly {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
            let mut bitmap = self
                .cell_db()
                .get(wtxn, &Key::Belly(cell))?
                .unwrap_or_default();
            bitmap |= items;
            self.cell_db().put(wtxn, &Key::Belly(cell), &bitmap)?;
        }

        for (cell, mut items_to_insert) in to_insert {
            if cancel() {
                return Err(Error::BuildCanceled);
            }
            let original_bitmap = self
                .cell_db()
                .get(wtxn, &Key::Cell(cell))?
                .unwrap_or_default();
            let new_bitmap = &original_bitmap | &items_to_insert;
            self.cell_db().put(wtxn, &Key::Cell(cell), &new_bitmap)?;
            if original_bitmap.len() >= self.threshold {
                // if we were already too large we can immediately jump to the next resolution
                self.insert_chunk_of_items_recursively(
                    wtxn,
                    cancel,
                    items_to_insert,
                    cell,
                    frozen_items,
                )?;
            } else if new_bitmap.len() >= self.threshold {
                let cell_shape = get_cell_shape(cell);
                let mut belly_items = RoaringBitmap::new();

                // If we just became too large, we have to retrieve the items that were already in the database insert them at the next resolution
                for item_id in original_bitmap.iter() {
                    let shape = frozen_items
                        .get(item_id)
                        .ok_or_else(|| Error::InternalDocIdMissing(item_id, pos!()))?;

                    let relation = shape.relation(
                        &cell_shape,
                        InputRelation {
                            // we don't need to know if we're being strictly contained or not
                            strict_contained: false,
                            ..InputRelation::all()
                        },
                    );
                    if relation.strict_contains.unwrap_or_default() {
                        belly_items.insert(item_id);
                    } else if relation.any_relation() {
                        items_to_insert.insert(item_id);
                    }
                }

                let mut belly_cells = self
                    .cell_db()
                    .get(wtxn, &Key::Belly(cell))?
                    .unwrap_or_default();
                belly_cells |= belly_items;
                self.cell_db().put(wtxn, &Key::Belly(cell), &belly_cells)?;

                self.insert_chunk_of_items_recursively(
                    wtxn,
                    cancel,
                    items_to_insert,
                    cell,
                    frozen_items,
                )?;
            }
            // If we are not too large, we have nothing else to do yaay
        }
        Ok(())
    }
}

fn get_cell_shape(cell: CellIndex) -> MultiPolygon {
    cell.into()
}

/// Return None if we cannot increase the resolution
/// Otherwise, return the children cells in a very non-efficient way
/// Note: We cannot use the `get_children_cells` function because it doesn't return the full coverage of our cells and leaves holes
fn get_children_cells(cell: CellIndex) -> Result<Option<Vec<CellIndex>>, Error> {
    let Some(next_res) = cell.resolution().succ() else {
        return Ok(None);
    };
    // safe to unwrap because we just increased the resolution
    let center_child = cell.center_child(next_res).unwrap();
    Ok(Some(center_child.grid_disk(2)))
}

struct FrozenItems<'a> {
    items: IntMap<ItemId, Zerometry<'a>>,
}

impl<'a> FrozenItems<'a> {
    pub fn get(&self, item: u32) -> Option<Zerometry<'a>> {
        self.items.get(item).copied()
    }
}
