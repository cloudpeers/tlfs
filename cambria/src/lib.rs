mod crdt;
mod schema;
//mod layout;
//mod lens;
//mod precompile;
//#[cfg(test)]
//mod test;

pub use crdt::Crdt;
pub use schema::Schema;
/*pub use layout::{Bool, Number, Ptr};
pub use lens::{ArchivedSchema, Kind, Lens, Lenses, PrimitiveKind, PrimitiveValue, Schema, Value};
pub use precompile::{precompile, write_tokens};
pub use {aligned, anyhow, rkyv};*/

#[cfg(feature = "arb")]
pub mod arb {
    pub use crdt::arb::*;
}

/*use anyhow::Result;
use rkyv::archived_root;

pub trait ArchivedCambria {
    fn lenses() -> &'static [u8];

    fn schema() -> &'static ArchivedSchema;

    fn ptr(&self) -> Ptr<'_>
    where
        Self: Sized,
    {
        Ptr::from_ref(self, unsafe { &*(Self::schema() as *const _) })
    }
}

pub trait Cambria: FromValue {
    fn lenses() -> &'static [u8];

    fn schema() -> &'static ArchivedSchema;

    fn transform(lenses: &[u8], bytes: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let a = unsafe { archived_root::<Lenses>(lenses) };
        let b = unsafe { archived_root::<Lenses>(Self::lenses()) };
        let sa = a.to_schema()?;
        let sa = unsafe { archived_root::<Schema>(&sa[..]) };
        let mut value = Ptr::new(bytes, sa).to_value();
        for lens in a.transform(b) {
            lens.transform_value(&mut value);
        }
        Self::from_value(&value)
    }
}

pub trait FromValue {
    fn from_value(value: &Value) -> Result<Self>
    where
        Self: Sized;
}

impl FromValue for () {
    fn from_value(value: &Value) -> Result<Self> {
        if let Value::Null = value {
            Ok(())
        } else {
            Err(anyhow::anyhow!("expected null"))
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self> {
        if let Value::Primitive(PrimitiveValue::Boolean(b)) = value {
            Ok(*b)
        } else {
            Err(anyhow::anyhow!("expected boolean"))
        }
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self> {
        if let Value::Primitive(PrimitiveValue::Number(n)) = value {
            Ok(*n)
        } else {
            Err(anyhow::anyhow!("expected number"))
        }
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        if let Value::Primitive(PrimitiveValue::Text(s)) = value {
            Ok(s.clone())
        } else {
            Err(anyhow::anyhow!("expected text"))
        }
    }
}

impl<T: FromValue> FromValue for Vec<T> {
    fn from_value(value: &Value) -> Result<Self> {
        if let Value::Array(a) = value {
            let mut arr = Vec::with_capacity(a.len());
            for v in a {
                arr.push(T::from_value(v)?);
            }
            Ok(arr)
        } else {
            Err(anyhow::anyhow!("expected array"))
        }
    }
}*/
