use anyhow::Result;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::{archived_root, Archive, Archived, Deserialize, Serialize};
use std::marker::PhantomData;

pub fn archive<T>(t: &T) -> Vec<u8>
where
    T: Serialize<AllocSerializer<256>>,
{
    let mut ser = AllocSerializer::<256>::default();
    ser.serialize_value(t).unwrap();
    ser.into_serializer().into_inner().to_vec()
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Ref<T> {
    marker: PhantomData<T>,
    bytes: sled::IVec,
}

impl<T: Archive> Ref<T> {
    pub fn new(bytes: sled::IVec) -> Self {
        Self {
            marker: PhantomData,
            bytes,
        }
    }

    pub fn to_owned(&self) -> Result<T>
    where
        Archived<T>: Deserialize<T, rkyv::Infallible>,
    {
        Ok(self.as_ref().deserialize(&mut rkyv::Infallible)?)
    }
}

impl<T: Archive> AsRef<Archived<T>> for Ref<T> {
    fn as_ref(&self) -> &Archived<T> {
        unsafe { archived_root::<T>(&self.bytes[..]) }
    }
}