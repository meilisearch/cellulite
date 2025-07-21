use std::{fmt, ops::Deref};

use geojson::GeoJson;
use h3o::LatLng;
use heed::{Env, EnvOpenOptions, RoTxn, WithTls};
use steppe::NoProgress;
use tempfile::TempDir;

use crate::{
    roaring::RoaringBitmapCodec, zerometry::ZerometryCodec, Cellulite, ItemId, Key, KeyCodec,
    KeyPrefixVariantCodec, KeyVariant,
};

pub struct DatabaseHandle {
    pub env: Env<WithTls>,
    pub database: Cellulite,
    #[allow(unused)]
    pub tempdir: TempDir,
}

impl Deref for DatabaseHandle {
    type Target = Cellulite;

    fn deref(&self) -> &Self::Target {
        &self.database
    }
}

impl DatabaseHandle {
    fn snap(&self, rtxn: &RoTxn) -> String {
        let mut s = String::new();

        s.push_str("# Items\n");
        let iter = self
            .database
            .main
            .remap_types::<KeyPrefixVariantCodec, ZerometryCodec>()
            .prefix_iter(rtxn, &KeyVariant::Item)
            .unwrap()
            .remap_key_type::<KeyCodec>();
        for ret in iter {
            let (key, value) = ret.unwrap();
            let Key::Item(item) = key else { unreachable!() };
            s.push_str(&format!("{item}: {value:?}\n"));
        }

        s.push_str("# Cells\n");
        let iter = self
            .database
            .main
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
            .max_dbs(Cellulite::nb_dbs())
            .open(dir.path())
    }
    .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let cellulite = Cellulite::create_from_env(&env, &mut wtxn).unwrap();
    wtxn.commit().unwrap();
    DatabaseHandle {
        env,
        database: cellulite,
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
    let mut db = create_database();
    let mut wtxn = db.env.write_txn().unwrap();
    db.database.threshold = 3;
    let point = GeoJson::from(geojson::Geometry::new(geojson::Value::Point(vec![
        0.0, 0.0,
    ])));
    db.add(&mut wtxn, 0, &point).unwrap();

    insta::assert_snapshot!(db.snap(&wtxn), @r"
    # Items
    0: Point(Zoint { lng: 0.0, lat: 0.0 })
    # Cells
    ");

    db.build(&mut wtxn, &NoProgress).unwrap();
    insta::assert_snapshot!(db.snap(&wtxn), @r"
    # Items
    0: Point(Zoint { lng: 0.0, lat: 0.0 })
    # Cells
    Cell { res: 0, center: (2.3009, -5.2454) }: RoaringBitmap<[0]>
    ");

    let point = GeoJson::from(geojson::Geometry::new(geojson::Value::Point(vec![
        0.0, 1.0,
    ])));
    db.add(&mut wtxn, 1, &point).unwrap();
    let point = GeoJson::from(geojson::Geometry::new(geojson::Value::Point(vec![
        0.0, 2.0,
    ])));
    db.add(&mut wtxn, 2, &point).unwrap();

    insta::assert_snapshot!(db.snap(&wtxn), @r"
    # Items
    0: Point(Zoint { lng: 0.0, lat: 0.0 })
    1: Point(Zoint { lng: 0.0, lat: 1.0 })
    2: Point(Zoint { lng: 0.0, lat: 2.0 })
    # Cells
    Cell { res: 0, center: (2.3009, -5.2454) }: RoaringBitmap<[0]>
    ");

    db.build(&mut wtxn, &NoProgress).unwrap();

    insta::assert_snapshot!(db.snap(&wtxn), @r"
    # Items
    0: Point(Zoint { lng: 0.0, lat: 0.0 })
    1: Point(Zoint { lng: 0.0, lat: 1.0 })
    2: Point(Zoint { lng: 0.0, lat: 2.0 })
    # Cells
    Cell { res: 0, center: (2.3009, -5.2454) }: RoaringBitmap<[0, 1, 2]>
    Cell { res: 1, center: (2.0979, 0.4995) }: RoaringBitmap<[0, 1, 2]>
    Cell { res: 2, center: (2.0979, 0.4995) }: RoaringBitmap<[0, 1, 2]>
    Cell { res: 2, center: (-0.4597, 0.5342) }: RoaringBitmap<[0, 1, 2]>
    Cell { res: 3, center: (2.1299, -0.3656) }: RoaringBitmap<[1, 2]>
    Cell { res: 3, center: (1.2792, -0.0699) }: RoaringBitmap<[1, 2]>
    Cell { res: 3, center: (-0.4051, -0.3419) }: RoaringBitmap<[0]>
    Cell { res: 3, center: (0.4159, 0.2300) }: RoaringBitmap<[0]>
    ");

    let point = GeoJson::from(geojson::Geometry::new(geojson::Value::Point(vec![
        0.0, 3.0,
    ])));
    db.add(&mut wtxn, 3, &point).unwrap();
    db.build(&mut wtxn, &NoProgress).unwrap();

    insta::assert_snapshot!(db.snap(&wtxn), @r"
    # Items
    0: Point(Zoint { lng: 0.0, lat: 0.0 })
    1: Point(Zoint { lng: 0.0, lat: 1.0 })
    2: Point(Zoint { lng: 0.0, lat: 2.0 })
    3: Point(Zoint { lng: 0.0, lat: 3.0 })
    # Cells
    Cell { res: 0, center: (2.3009, -5.2454) }: RoaringBitmap<[0, 1, 2, 3]>
    Cell { res: 1, center: (2.0979, 0.4995) }: RoaringBitmap<[0, 1, 2, 3]>
    Cell { res: 2, center: (2.0979, 0.4995) }: RoaringBitmap<[0, 1, 2, 3]>
    Cell { res: 2, center: (-0.4597, 0.5342) }: RoaringBitmap<[0, 1, 2, 3]>
    Cell { res: 3, center: (2.1299, -0.3656) }: RoaringBitmap<[1, 2, 3]>
    Cell { res: 3, center: (1.2792, -0.0699) }: RoaringBitmap<[1, 2, 3]>
    Cell { res: 3, center: (2.9436, 0.1993) }: RoaringBitmap<[1, 2, 3]>
    Cell { res: 3, center: (-0.4051, -0.3419) }: RoaringBitmap<[0]>
    Cell { res: 3, center: (0.4159, 0.2300) }: RoaringBitmap<[0]>
    Cell { res: 4, center: (1.9998, -0.0776) }: RoaringBitmap<[2]>
    Cell { res: 4, center: (0.9168, -0.0660) }: RoaringBitmap<[1]>
    Cell { res: 4, center: (3.0701, -0.0891) }: RoaringBitmap<[3]>
    ");
}

#[test]
fn bug_write_points_create_useless_cells() {
    // This simple test was creating 5 cells instead of 3.
    let mut db = create_database();
    let mut wtxn = db.env.write_txn().unwrap();
    db.database.threshold = 2;
    let point = GeoJson::from(geojson::Geometry::new(geojson::Value::Point(vec![
        -11.460678226504395,
        48.213563161838714,
    ])));
    db.add(&mut wtxn, 0, &point).unwrap();
    let point = GeoJson::from(geojson::Geometry::new(geojson::Value::Point(vec![
        -1.520397001416467,
        54.586501531522245,
    ])));
    db.add(&mut wtxn, 1, &point).unwrap();
    db.build(&mut wtxn, &NoProgress).unwrap();
    insta::assert_snapshot!(db.snap(&wtxn), @r"
    # Items
    0: Point(Zoint { lng: -11.460678226504395, lat: 48.213563161838714 })
    1: Point(Zoint { lng: -1.520397001416467, lat: 54.586501531522245 })
    # Cells
    Cell { res: 0, center: (52.6758, -11.6016) }: RoaringBitmap<[0, 1]>
    Cell { res: 1, center: (45.2992, -14.2485) }: RoaringBitmap<[0]>
    Cell { res: 1, center: (53.6528, 0.2143) }: RoaringBitmap<[1]>
    ");
}

/*
#[test]
fn basic_nearest() {
    let handle = create_database();
    let mut wtxn = handle.env.write_txn().unwrap();
    let mut writer = Writer::new(handle.database);
    writer.threshold = 10;
    // We'll draw a simple line over the y as seen below
    // (0,0) # # # # # # ...
    for i in 0..100 {
        let point = GeoJson::from(geojson::Geometry::new(geojson::Value::Point(vec![i as f64, 0.0])));
        writer.add_item(&mut wtxn, i, &point).unwrap();
    }
    // insta::assert_snapshot!(handle.snap(&wtxn), @r###"
    // "###);
    wtxn.commit().unwrap();

    let rtxn = handle.env.read_txn().unwrap();
    let point = GeoJson::from(geojson::Geometry::new(geojson::Value::Point(vec![0.0, 0.0])));
    let ret = writer.nearest_point(&rtxn, &point, 5).unwrap();

    insta::assert_snapshot!(NnRes(Some(ret)), @r###"
        id(0): coord(0.00000, 0.00000)
        id(1): coord(1.00000, -0.00000)
        id(2): coord(2.00000, 0.00000)
        id(3): coord(3.00000, -0.00000)
        id(4): coord(4.00000, 0.00000)
        "###);

    let point = GeoJson::from(geojson::Geometry::new(geojson::Value::Point(vec![50.0, 0.0])));
    let ret = writer.nearest_point(&rtxn, &point, 5).unwrap();

    insta::assert_snapshot!(NnRes(Some(ret)), @r###"
        id(50): coord(50.00000, 0.00001)
        id(49): coord(49.00000, 0.00000)
        id(51): coord(51.00000, -0.00000)
        id(48): coord(48.00000, 0.00000)
        id(52): coord(52.00000, 0.00000)
        "###);
}
*/
