use ::roaring::RoaringBitmap;
use geo_types::Polygon;
use h3o::{
    error::{InvalidGeometry, InvalidLatLng},
    geom::{ContainmentMode, TilerBuilder},
    CellIndex, LatLng, Resolution,
};
use heed::{
    byteorder::{BigEndian, ByteOrder},
    RoTxn, RwTxn, Unspecified,
};
use ordered_float::OrderedFloat;

mod roaring;

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
    db: Database,
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
    pub fn in_shape(&self, rtxn: &RoTxn, polygon: Polygon) -> Result<RoaringBitmap> {
        let mut tiler = TilerBuilder::new(Resolution::Zero)
            .containment_mode(ContainmentMode::Covers)
            .build();
        tiler.add(polygon)?;

        let cells = tiler.into_coverage();
        let mut ret = RoaringBitmap::new();
        for cell in cells {
            let Some(items) = self.cell_db().get(rtxn, &Key::Cell(cell))? else {
                continue;
            };
            ret |= items;
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

pub struct KeyCodec;

impl<'a> heed::BytesEncode<'a> for KeyCodec {
    type EItem = Key;

    fn bytes_encode(key: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>, heed::BoxedError> {
        let mut ret;
        match key {
            Key::Item(item) => {
                ret = Vec::with_capacity(size_of::<KeyVariant>() + size_of_val(item));
                ret.push(KeyVariant::Item as u8);
                ret.extend_from_slice(&item.to_be_bytes());
            }
            Key::Cell(cell) => {
                ret = Vec::with_capacity(size_of::<KeyVariant>() + size_of_val(cell));
                ret.push(KeyVariant::Cell as u8);
                let output: u64 = (*cell).into();
                ret.extend_from_slice(&output.to_be_bytes());
            }
        }
        Ok(ret.into())
    }
}

impl heed::BytesDecode<'_> for KeyCodec {
    type DItem = Key;

    fn bytes_decode(bytes: &'_ [u8]) -> Result<Self::DItem, heed::BoxedError> {
        let variant = bytes[0];
        let bytes = &bytes[std::mem::size_of_val(&variant)..];
        let key = match variant {
            v if v == KeyVariant::Item as u8 => {
                let item = BigEndian::read_u32(bytes);
                Key::Item(item)
            }
            v if v == KeyVariant::Cell as u8 => {
                let cell = BigEndian::read_u64(bytes);
                Key::Cell(
                    // safety: the cell uses a `repr(transparent)` and only contains an `u64`. But we should make a PR to make that safe
                    unsafe { std::mem::transmute::<u64, CellIndex>(cell) },
                )
            }
            _ => unreachable!(),
        };

        Ok(key)
    }
}

pub enum Key {
    Item(ItemId),
    Cell(CellIndex),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyVariant {
    Item = 0,
    Cell = 1,
}

struct KeyPrefixVariantCodec;

impl<'a> heed::BytesEncode<'a> for KeyPrefixVariantCodec {
    type EItem = KeyVariant;

    fn bytes_encode(
        variant: &'a Self::EItem,
    ) -> Result<std::borrow::Cow<'a, [u8]>, heed::BoxedError> {
        Ok(vec![*variant as u8].into())
    }
}

pub struct CellIndexCodec;

impl<'a> heed::BytesEncode<'a> for CellIndexCodec {
    type EItem = CellIndex;

    fn bytes_encode(cell: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>, heed::BoxedError> {
        let output: u64 = (*cell).into();
        Ok(output.to_be_bytes().to_vec().into())
    }
}

impl heed::BytesDecode<'_> for CellIndexCodec {
    type DItem = CellIndex;

    fn bytes_decode(bytes: &'_ [u8]) -> Result<Self::DItem, heed::BoxedError> {
        let cell = BigEndian::read_u64(bytes);
        Ok(
            // safety: the cell uses a `repr(transparent)` and only contains an `u64`.
            // TODO: But we should make a PR to make that safe, there is no performance gain in doing it this way.
            unsafe { std::mem::transmute::<u64, CellIndex>(cell) },
        )
    }
}

#[cfg(test)]
mod test {
    use std::fmt;

    use h3o::LatLng;
    use heed::{Env, EnvOpenOptions, RoTxn, WithTls};
    use tempfile::TempDir;

    use crate::{
        roaring::RoaringBitmapCodec, CellIndexCodec, Database, ItemId, Key, KeyCodec,
        KeyPrefixVariantCodec, KeyVariant, Writer,
    };

    pub struct DatabaseHandle {
        pub env: Env<WithTls>,
        pub database: Database,
        #[allow(unused)]
        pub tempdir: TempDir,
    }

    impl DatabaseHandle {
        fn snap(&self, rtxn: &RoTxn) -> String {
            let mut s = String::new();

            s.push_str("# Items\n");
            let iter = self
                .database
                .remap_types::<KeyPrefixVariantCodec, CellIndexCodec>()
                .prefix_iter(rtxn, &KeyVariant::Item)
                .unwrap()
                .remap_key_type::<KeyCodec>();
            for ret in iter {
                let (key, value) = ret.unwrap();
                let Key::Item(item) = key else { unreachable!() };
                let lat_lng = LatLng::from(value);
                let (lat, lng) = (lat_lng.lat(), lat_lng.lng());
                s.push_str(&format!("{item}: ({lat:.4}, {lng:.4})\n"));
            }

            s.push_str("# Cells\n");
            let iter = self
                .database
                .remap_types::<KeyPrefixVariantCodec, RoaringBitmapCodec>()
                .prefix_iter(rtxn, &KeyVariant::Cell)
                .unwrap()
                .remap_key_type::<KeyCodec>();
            for ret in iter {
                let (key, value) = ret.unwrap();
                let Key::Cell(cell) = key else { unreachable!() };
                let lat_lng = LatLng::from(cell);
                let (lat, lng) = (lat_lng.lat(), lat_lng.lng());
                let res = cell.resolution();
                s.push_str(&format!(
                    "Cell {{ res: {res}, center: ({lat:.4}, {lng:.4}) }}: {value:?}\n"
                ));
            }

            s
        }
    }

    fn create_database() -> DatabaseHandle {
        let dir = tempfile::tempdir().unwrap();
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(200 * 1024 * 1024)
                .open(dir.path())
        }
        .unwrap();
        let mut wtxn = env.write_txn().unwrap();
        let database: Database = env.create_database(&mut wtxn, None).unwrap();
        wtxn.commit().unwrap();
        DatabaseHandle {
            env,
            database,
            tempdir: dir,
        }
    }

    pub struct NnRes(pub Option<Vec<(ItemId, (f64, f64))>>);

    impl fmt::Display for NnRes {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0 {
                Some(ref vec) => {
                    for (id, (lat, lng)) in vec {
                        writeln!(f, "id({id}): coord({lat:.5}, {lng:.5})")?;
                    }
                    Ok(())
                }
                None => f.write_str("No results found"),
            }
        }
    }

    #[test]
    fn basic_write() {
        let handle = create_database();
        let mut wtxn = handle.env.write_txn().unwrap();
        let mut writer = Writer::new(handle.database);
        writer.threshold = 3;
        writer.add_item(&mut wtxn, 0, (0.0, 0.0)).unwrap();

        insta::assert_snapshot!(handle.snap(&wtxn), @r###"
        # Items
        0: (0.0000, 0.0000)
        # Cells
        Cell { res: 0, center: (2.3009, -5.2454) }: RoaringBitmap<[0]>
        "###);

        writer.add_item(&mut wtxn, 1, (1.0, 0.0)).unwrap();
        writer.add_item(&mut wtxn, 2, (2.0, 0.0)).unwrap();

        insta::assert_snapshot!(handle.snap(&wtxn), @r###"
        # Items
        0: (0.0000, 0.0000)
        1: (1.0000, -0.0000)
        2: (2.0000, 0.0000)
        # Cells
        Cell { res: 0, center: (2.3009, -5.2454) }: RoaringBitmap<[0, 1, 2]>
        Cell { res: 1, center: (2.0979, 0.4995) }: RoaringBitmap<[0, 1, 2]>
        Cell { res: 2, center: (2.0979, 0.4995) }: RoaringBitmap<[1, 2]>
        Cell { res: 2, center: (-0.4597, 0.5342) }: RoaringBitmap<[0]>
        "###);

        writer.add_item(&mut wtxn, 3, (3.0, 0.0)).unwrap();

        insta::assert_snapshot!(handle.snap(&wtxn), @r###"
        # Items
        0: (0.0000, 0.0000)
        1: (1.0000, -0.0000)
        2: (2.0000, 0.0000)
        3: (3.0000, -0.0000)
        # Cells
        Cell { res: 0, center: (2.3009, -5.2454) }: RoaringBitmap<[0, 1, 2, 3]>
        Cell { res: 1, center: (2.0979, 0.4995) }: RoaringBitmap<[0, 1, 2, 3]>
        Cell { res: 2, center: (2.0979, 0.4995) }: RoaringBitmap<[1, 2, 3]>
        Cell { res: 2, center: (-0.4597, 0.5342) }: RoaringBitmap<[0]>
        Cell { res: 3, center: (2.1299, -0.3656) }: RoaringBitmap<[2]>
        Cell { res: 3, center: (1.2792, -0.0699) }: RoaringBitmap<[1]>
        Cell { res: 3, center: (2.9436, 0.1993) }: RoaringBitmap<[3]>
        "###);
    }

    #[test]
    fn basic_nearest() {
        let handle = create_database();
        let mut wtxn = handle.env.write_txn().unwrap();
        let mut writer = Writer::new(handle.database);
        writer.threshold = 10;
        // We'll draw a simple line over the y as seen below
        // (0,0) # # # # # # ...
        for i in 0..100 {
            writer.add_item(&mut wtxn, i, (i as f64, 0.0)).unwrap();
        }
        // insta::assert_snapshot!(handle.snap(&wtxn), @r###"
        // "###);
        wtxn.commit().unwrap();

        let rtxn = handle.env.read_txn().unwrap();
        let ret = writer.nearest_point(&rtxn, (0.0, 0.0), 5).unwrap();

        insta::assert_snapshot!(NnRes(Some(ret)), @r###"
        id(0): coord(0.00000, 0.00000)
        id(1): coord(1.00000, -0.00000)
        id(2): coord(2.00000, 0.00000)
        id(3): coord(3.00000, -0.00000)
        id(4): coord(4.00000, 0.00000)
        "###);

        let ret = writer.nearest_point(&rtxn, (50.0, 0.0), 5).unwrap();

        insta::assert_snapshot!(NnRes(Some(ret)), @r###"
        id(50): coord(50.00000, 0.00001)
        id(49): coord(49.00000, 0.00000)
        id(51): coord(51.00000, -0.00000)
        id(48): coord(48.00000, 0.00000)
        id(52): coord(52.00000, 0.00000)
        "###);
    }
}
