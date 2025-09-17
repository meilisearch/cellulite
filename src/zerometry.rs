use std::borrow::Cow;

use geo::Geometry;
use heed::BoxedError;
use zerometry::Zerometry;

pub struct ZerometryCodec;

impl<'a> heed::BytesDecode<'a> for ZerometryCodec {
    type DItem = Zerometry<'a>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        // Safe because the keys and values are aligned on 64 bits
        unsafe { Zerometry::from_bytes(bytes).map_err(Into::into) }
    }
}

impl heed::BytesEncode<'_> for ZerometryCodec {
    type EItem = Geometry;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<'_, [u8]>, BoxedError> {
        let mut bytes = Vec::new();
        Zerometry::write_from_geometry(&mut bytes, item)?;
        Ok(Cow::Owned(bytes))
    }
}
