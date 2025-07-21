use h3o::CellIndex;
use heed::byteorder::{BigEndian, ByteOrder};

use crate::ItemId;

pub struct KeyCodec;

impl<'a> heed::BytesEncode<'a> for KeyCodec {
    type EItem = Key;

    fn bytes_encode(key: &'a Self::EItem) -> Result<std::borrow::Cow<'a, [u8]>, heed::BoxedError> {
        const ALIGNMENT: usize = std::mem::size_of::<f64>();
        let mut ret: Vec<u8>;
        match key {
            Key::Item(item) => {
                let capacity = size_of::<KeyVariant>() + size_of_val(item);
                let missing_to_align = ALIGNMENT - (capacity % ALIGNMENT);
                ret = Vec::with_capacity(capacity + missing_to_align);
                ret.push(KeyVariant::Item as u8);
                ret.extend_from_slice(&item.to_be_bytes());
                ret.extend(std::iter::repeat(0).take(missing_to_align));
            }
            Key::Cell(cell) => {
                let capacity = size_of::<KeyVariant>() + size_of_val(cell);
                let missing_to_align = ALIGNMENT - (capacity % ALIGNMENT);
                ret = Vec::with_capacity(capacity + missing_to_align);
                ret.push(KeyVariant::Cell as u8);
                let output: u64 = (*cell).into();
                ret.extend_from_slice(&output.to_be_bytes());
                ret.extend(std::iter::repeat(0).take(missing_to_align));
            }
            Key::InnerShape(cell) => {
                let capacity = size_of::<KeyVariant>() + size_of_val(cell);
                let missing_to_align = ALIGNMENT - (capacity % ALIGNMENT);
                ret = Vec::with_capacity(capacity + missing_to_align);
                ret.push(KeyVariant::InnerShape as u8);
                let output: u64 = (*cell).into();
                ret.extend_from_slice(&output.to_be_bytes());
                ret.extend(std::iter::repeat(0).take(missing_to_align));
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
                Key::Cell(cell.try_into()?)
            }
            v if v == KeyVariant::InnerShape as u8 => {
                let cell = BigEndian::read_u64(bytes);
                Key::InnerShape(cell.try_into()?)
            }
            _ => unreachable!(),
        };
        // In any case we can skip the padding

        Ok(key)
    }
}

pub enum Key {
    Item(ItemId),
    Cell(CellIndex),
    InnerShape(CellIndex),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyVariant {
    Item = 0,
    Cell = 1,
    InnerShape = 2,
}

pub struct KeyPrefixVariantCodec;

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
        Ok(cell.try_into()?)
    }
}
