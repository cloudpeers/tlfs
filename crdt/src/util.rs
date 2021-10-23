use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{archived_root, check_archived_root, Archive, Archived, Deserialize, Serialize};
use std::marker::PhantomData;

fn archive<T>(t: &T) -> Vec<u8>
where
    T: Serialize<AllocSerializer<256>>,
{
    let mut ser = AllocSerializer::<256>::default();
    ser.serialize_value(t).unwrap();
    ser.into_serializer().into_inner().to_vec()
}

/// Owned zero copy bytes encoding `T`.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Ref<T> {
    marker: PhantomData<T>,
    bytes: sled::IVec,
}

impl<T: Archive> Ref<T> {
    /// Creates a new [`Ref`] from a [`sled::IVec`]
    pub fn new(bytes: sled::IVec) -> Self {
        Self {
            marker: PhantomData,
            bytes,
        }
    }

    /// Checks the byte slice and copies it into an owned buffer.
    pub fn checked<'a>(buffer: &'a [u8]) -> Result<Self>
    where
        Archived<T>: CheckBytes<DefaultValidator<'a>> + 'static,
    {
        check_archived_root::<T>(buffer).map_err(|err| anyhow!("{}", err))?;
        Ok(Self::new(buffer.into()))
    }

    /// Serializes `T` into a zero-copy byte slice.
    pub fn archive(t: &T) -> Self
    where
        T: Serialize<AllocSerializer<256>>,
    {
        Self::new(archive(t).into())
    }

    /// Deserializes the owned bytes.
    pub fn to_owned(&self) -> Result<T>
    where
        Archived<T>: Deserialize<T, rkyv::Infallible>,
    {
        Ok(self.as_ref().deserialize(&mut rkyv::Infallible)?)
    }

    /// Returns the serialized byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl<T: Archive> AsRef<Archived<T>> for Ref<T> {
    fn as_ref(&self) -> &Archived<T> {
        unsafe { archived_root::<T>(&self.bytes[..]) }
    }
}

impl<T> From<Ref<T>> for sled::IVec {
    fn from(r: Ref<T>) -> Self {
        r.bytes
    }
}

impl<T> From<Ref<T>> for Vec<u8> {
    fn from(r: Ref<T>) -> Self {
        r.bytes.to_vec()
    }
}

impl<T: Archive> std::fmt::Debug for Ref<T>
where
    Archived<T>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}

impl<T: Archive> std::fmt::Display for Ref<T>
where
    Archived<T>: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}
