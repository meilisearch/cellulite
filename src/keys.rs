use h3o::CellIndex;
use heed::byteorder::{BigEndian, ByteOrder};

use crate::ItemId;

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
pub enum KeyVariant {
    Item = 0,
    Cell = 1,
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
        Ok(
            // safety: the cell uses a `repr(transparent)` and only contains an `u64`.
            // TODO: But we should make a PR to make that safe, there is no performance gain in doing it this way.
            unsafe { std::mem::transmute::<u64, CellIndex>(cell) },
        )
    }
}
