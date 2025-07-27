use std::borrow::Cow;

use heed::BoxedError;
use roaring::RoaringBitmap;

pub struct RoaringBitmapCodec;

impl heed::BytesDecode<'_> for RoaringBitmapCodec {
    type DItem = RoaringBitmap;

    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, BoxedError> {
        RoaringBitmap::deserialize_unchecked_from(bytes).map_err(Into::into)
    }
}

impl heed::BytesEncode<'_> for RoaringBitmapCodec {
    type EItem = RoaringBitmap;

    fn bytes_encode(item: &Self::EItem) -> Result<Cow<[u8]>, BoxedError> {
        const ALIGNMENT: usize = std::mem::size_of::<f64>();
        let mut bytes = Vec::with_capacity(item.serialized_size());
        item.serialize_into(&mut bytes)?;
        let missing_to_align = ALIGNMENT - (bytes.len() % ALIGNMENT);
        bytes.extend(std::iter::repeat_n(0, missing_to_align));
        Ok(Cow::Owned(bytes))
    }
}
