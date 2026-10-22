#![doc = include_str!("../README.md")]

use core::f64;
use std::collections::BTreeMap;

use ::roaring::RoaringBitmap;
use ::zerometry::Zerometry;
use geo::{Densify, Geometry, Haversine};
use geojson::GeoJson;
use h3o::{CellIndex, Resolution};
use heed::{
    DatabaseOpener, DatabaseStat, OnCommit, RoTxn, RwTxn, Unspecified,
    byteorder::BE,
    types::{Bytes, U32},
};
use keys::{CellKeyCodec, ItemKeyCodec, Key, MetadataKey, UpdateType};
use metadata::{Version, VersionCodec};

mod builder;
mod error;
pub(crate) mod keys;
mod metadata;
pub mod reader;
pub mod roaring;
pub mod zerometry;

#[cfg(test)]
mod test;

pub use crate::error::Error;
use crate::{roaring::RoaringBitmapCodec, zerometry::ZerometryCodec};

pub type ItemDb<'t> = heed::Database<'t, ItemKeyCodec, ZerometryCodec>;
pub type CellDb<'t> = heed::Database<'t, CellKeyCodec, RoaringBitmapCodec>;
pub type UpdateDb<'t> = heed::Database<'t, U32<BE>, UpdateType>;
pub type MetadataDb<'t> = heed::Database<'t, MetadataKey, Unspecified>;
pub type ItemId = u32;

steppe::make_enum_progress! {
    pub enum BuildSteps {
        RetrieveUpdatedItems,
        ClearUpdatedItems,
        RetrieveAndClearDeletedItems,
        RemoveDeletedItemsFromDatabase,
        InsertItemsAtLevelZero,
        InsertItemsRecursively,
        UpdateTheMetadata,
    }
}
steppe::make_atomic_progress!(Item alias AtomicItemStep => "item");
steppe::make_atomic_progress!(Cell alias AtomicCellStep => "cell");

type Result<O, E = Error> = std::result::Result<O, E>;

/// The entry-point of the lib. It contains all the database required to write and read stuff.
#[derive(Clone)]
pub struct Cellulite<'e> {
    /// Links the item IDs with their Zerometry
    pub(crate) item: ItemDb<'e>,
    /// Links both the normal and belly `CellIndex` with a roaring bitmap of the items they contains.
    pub(crate) cell: CellDb<'e>,
    /// Temporary database holding the operation (addition or deletion) made on the items.
    /// It's in a temporary database so we can clear it quiclky.
    pub(crate) update: UpdateDb<'e>,
    /// Contains all the metadata related to the database.
    pub(crate) metadata: MetadataDb<'e>,

    /// After how many elements should we break a cell into sub-cells
    /// This is only available for the test and visualizing tools to use it.
    pub threshold: u64,
}

impl<'e> Cellulite<'e> {
    pub const fn nb_dbs() -> u32 {
        4
    }

    pub fn item_db_stats(&self, rtxn: &RoTxn) -> heed::Result<DatabaseStat> {
        self.item.stat(rtxn)
    }

    pub fn cell_db_stats(&self, rtxn: &RoTxn) -> heed::Result<DatabaseStat> {
        self.cell.stat(rtxn)
    }

    pub fn update_db_stats(&self, rtxn: &RoTxn) -> heed::Result<DatabaseStat> {
        self.update.stat(rtxn)
    }

    pub fn metadata_db_stats(&self, rtxn: &RoTxn) -> heed::Result<DatabaseStat> {
        self.metadata.stat(rtxn)
    }

    pub const fn default_threshold() -> u64 {
        200
    }

    /// Create all the databases required for cellulite to work.
    /// The prefix lets you to hold multiple cellulite database in a single environment.
    pub fn create_from_dbo<Tls>(
        dbo: &DatabaseOpener<'e, Tls>,
        wtxn: &mut RwTxn<'e>,
        prefix: &str,
    ) -> Result<Self> {
        let item = dbo.create_database(wtxn, Some(&format!("{prefix}-item")))?;
        let cell = dbo.create_database(wtxn, Some(&format!("{prefix}-cell")))?;
        let update = dbo.create_database(wtxn, Some(&format!("{prefix}-update")))?;
        let metadata = dbo.create_database(wtxn, Some(&format!("{prefix}-metadata")))?;
        Ok(Self {
            item,
            cell,
            update,
            metadata,
            threshold: Self::default_threshold(),
        })
    }

    /// Open all the databases required for cellulite to work, return an error if any of the required database doesn't exists.
    /// The prefix lets you to hold multiple cellulite database in a single environment.
    pub fn open_from_dbo<Tls>(
        env: &DatabaseOpener<'e, Tls>,
        rtxn: &RoTxn<'e>,
        prefix: &str,
    ) -> Result<Self> {
        let item = env
            .open_database(rtxn, Some(&format!("{prefix}-item")))?
            .ok_or(Error::DatabaseDoesntExists)?;
        let cell = env
            .open_database(rtxn, Some(&format!("{prefix}-cell")))?
            .ok_or(Error::DatabaseDoesntExists)?;
        let update = env
            .open_database(rtxn, Some(&format!("{prefix}-update")))?
            .ok_or(Error::DatabaseDoesntExists)?;
        let metadata = env
            .open_database(rtxn, Some(&format!("{prefix}-metadata")))?
            .ok_or(Error::DatabaseDoesntExists)?;
        Ok(Self {
            item,
            cell,
            update,
            metadata,
            threshold: Self::default_threshold(),
        })
    }

    /// Create the cellulite struct from already opened databases.
    pub fn from_dbs(
        item: ItemDb<'e>,
        cell: CellDb<'e>,
        update: UpdateDb<'e>,
        metadata: MetadataDb<'e>,
    ) -> Self {
        Self {
            item,
            cell,
            update,
            metadata,
            threshold: Self::default_threshold(),
        }
    }

    /// Clear all the databases.
    pub fn clear(&self, wtxn: &mut RwTxn) -> Result<()> {
        self.item.clear(wtxn)?;
        self.cell.clear(wtxn)?;
        self.update.clear(wtxn)?;
        self.metadata.clear(wtxn)?;
        Ok(())
    }

    #[inline]
    fn item_db(&self) -> ItemDb<'e> {
        self.item
    }

    #[inline]
    fn cell_db(&self) -> CellDb<'e> {
        self.cell
    }

    /// Return the version of the cellulite database.
    pub fn get_version(&self, rtxn: &RoTxn) -> heed::Result<Version> {
        self.metadata
            .remap_data_type::<VersionCodec>()
            .get(rtxn, &MetadataKey::Version)
            // If there is no version in the database it means we never wrote anything to the database
            // and we're at the last/current version
            .map(|opt| opt.unwrap_or_default())
    }

    fn set_version(&self, wtxn: &mut RwTxn, version: &Version) -> heed::Result<()> {
        self.metadata
            .remap_data_type::<VersionCodec>()
            .put(wtxn, &MetadataKey::Version, version)
    }

    /// Return all the cells used internally in the database
    pub fn inner_db_cells<'a>(
        &self,
        rtxn: &'a RoTxn,
    ) -> Result<impl Iterator<Item = Result<(CellIndex, RoaringBitmap), heed::Error>> + 'a> {
        Ok(self
            .cell
            .iter(rtxn)?
            .filter(|ret| {
                ret.as_ref()
                    // if there is an error we want to return it
                    .map_or(true, |(key, _v)| matches!(key, Key::Cell(_)))
            })
            .map(|res| {
                res.map(|(key, bitmap)| {
                    let Key::Cell(cell) = key else { unreachable!() };
                    (cell, bitmap)
                })
            }))
    }

    /// Return all the belly cells used internally in the database
    pub fn inner_belly_cells<'a>(
        &self,
        rtxn: &'a RoTxn,
    ) -> Result<impl Iterator<Item = Result<(CellIndex, RoaringBitmap), heed::Error>> + 'a> {
        Ok(self
            .cell
            .iter(rtxn)?
            .filter(|ret| {
                ret.as_ref()
                    // if there is an error we want to return it
                    .map_or(true, |(key, _v)| matches!(key, Key::Belly(_)))
            })
            .map(|res| {
                res.map(|(key, bitmap)| {
                    let Key::Belly(cell) = key else {
                        unreachable!()
                    };
                    (cell, bitmap)
                })
            }))
    }

    /// Return the coordinates of the items rounded down to 50cm if this id exists in the DB. Returns `None` otherwise.
    pub fn item<'a>(&self, rtxn: &'a RoTxn, item: ItemId) -> Result<Option<Zerometry<'a>>> {
        self.item_db().get(rtxn, &item).map_err(Error::from)
    }

    /// Iterate over all the items in the database
    pub fn items<'a>(
        &self,
        rtxn: &'a RoTxn,
    ) -> Result<impl Iterator<Item = Result<(ItemId, Zerometry<'a>), heed::Error>> + 'a> {
        Ok(self.item.iter(rtxn)?)
    }

    /// Insert a geojson to the database. The geojson won't be stored as-is and cannot be returned later.
    /// For the item to be searchable you must [`Self::build`] the database afterward.
    pub fn add(&self, wtxn: &mut RwTxn, item: ItemId, geo: &GeoJson) -> Result<()> {
        let geom = geo_types::Geometry::<f64>::try_from(geo.clone()).unwrap();
        self.item_db().put(wtxn, &item, &geom)?;
        self.update.put(wtxn, &item, &UpdateType::Insert)?;
        Ok(())
    }

    /// The `geo` must be a valid `Zerometry` otherwise the database will be corrupted.
    /// For the item to be searchable you must [`Self::build`] the database afterward.
    pub fn add_raw_zerometry(&self, wtxn: &mut RwTxn, item: ItemId, geo: &[u8]) -> Result<()> {
        self.item_db()
            .remap_data_type::<Bytes>()
            .put(wtxn, &item, geo)?;
        self.update.put(wtxn, &item, &UpdateType::Insert)?;
        Ok(())
    }

    /// Delete an item by its id.
    /// For the item to be removed you must [`Self::build`] the database afterward.
    pub fn delete(&self, wtxn: &mut RwTxn, item: ItemId) -> Result<()> {
        self.update.put(wtxn, &item, &UpdateType::Delete)?;
        Ok(())
    }

    /// Return stats of all the entries in the database.
    pub fn stats(&self, rtxn: &RoTxn) -> Result<Stats> {
        let total_items = self.items(rtxn)?.count();
        let mut total_cells = 0;
        let mut cells_by_resolution = BTreeMap::new();

        for entry in self.inner_db_cells(rtxn)? {
            let (cell, _) = entry?;
            total_cells += 1;
            *cells_by_resolution.entry(cell.resolution()).or_default() += 1;
        }

        let mut total_belly_cells = 0;
        let mut belly_cells_by_resolution = BTreeMap::new();

        for entry in self.inner_belly_cells(rtxn)? {
            let (cell, _) = entry?;
            total_belly_cells += 1;
            *belly_cells_by_resolution
                .entry(cell.resolution())
                .or_default() += 1;
        }

        Ok(Stats {
            total_cells,
            total_items,
            cells_by_resolution,
            total_belly_cells,
            belly_cells_by_resolution,
        })
    }
}

impl OnCommit for Cellulite<'_> {
    type Committed = Cellulite<'static>;

    fn on_commit(self, token: &heed::CommitToken) -> Self::Committed {
        let Self {
            item,
            cell,
            update,
            metadata,
            threshold,
        } = self;
        Cellulite {
            item: item.on_commit(token),
            cell: cell.on_commit(token),
            update: update.on_commit(token),
            metadata: metadata.on_commit(token),
            threshold,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub total_cells: usize,
    pub total_belly_cells: usize,
    pub total_items: usize,
    pub cells_by_resolution: BTreeMap<Resolution, usize>,
    pub belly_cells_by_resolution: BTreeMap<Resolution, usize>,
}

pub fn densify_geom(geom: &mut Geometry) {
    match geom {
        Geometry::Line(line) => {
            *geom = Geometry::LineString(
                Haversine.densify(&geo_types::LineString(vec![line.start, line.end]), 10_000.0),
            );
        }
        Geometry::LineString(line_string) => {
            *line_string = Haversine.densify(line_string, 10_000.0);
        }
        Geometry::Polygon(polygon) => {
            *polygon = Haversine.densify(polygon, 10_000.0);
        }
        Geometry::MultiLineString(multi_line_string) => {
            *multi_line_string = Haversine.densify(multi_line_string, 10_000.0);
        }
        Geometry::MultiPolygon(multi_polygon) => {
            *multi_polygon = Haversine.densify(multi_polygon, 10_000.0);
        }
        Geometry::GeometryCollection(geometry_collection) => {
            for geom in geometry_collection.0.iter_mut() {
                densify_geom(geom);
            }
        }
        Geometry::Rect(rect) => {
            *geom = Geometry::Polygon(Haversine.densify(rect, 10_000.0));
        }
        Geometry::Triangle(triangle) => {
            *geom = Geometry::Polygon(Haversine.densify(triangle, 10_000.0));
        }
        _ => (),
    };
}
