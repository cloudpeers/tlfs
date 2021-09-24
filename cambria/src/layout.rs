use crate::lens::{ArchivedSchema, PrimitiveValue, Value};
use rkyv::collections::ArchivedBTreeMap;
use rkyv::string::ArchivedString;
use rkyv::vec::ArchivedVec;
use rkyv::with::{ArchiveWith, DeserializeWith, SerializeWith};
use rkyv::{Archive, Archived, Deserialize, Fallible, RawRelPtr, Serialize};
use std::collections::BTreeMap;

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Archive,
    Deserialize,
    Serialize,
)]
#[archive(as = "Number")]
#[repr(C)]
pub struct Number([u8; 8]);

impl From<Number> for i64 {
    fn from(n: Number) -> i64 {
        i64::from_le_bytes(n.0)
    }
}

impl From<i64> for Number {
    fn from(n: i64) -> Number {
        Self(n.to_le_bytes())
    }
}

impl ArchiveWith<i64> for Number {
    type Archived = rkyv::Archived<Number>;
    type Resolver = rkyv::Resolver<Number>;

    unsafe fn resolve_with(field: &i64, pos: usize, res: NumberResolver, out: *mut Self::Archived) {
        Number::from(*field).resolve(pos, res, out);
    }
}

impl<S: Fallible + ?Sized> SerializeWith<i64, S> for Number {
    fn serialize_with(field: &i64, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Number::from(*field).serialize(serializer)
    }
}

impl<D: Fallible + ?Sized> DeserializeWith<Archived<Number>, i64, D> for Number {
    fn deserialize_with(field: &Archived<Number>, deserializer: &mut D) -> Result<i64, D::Error> {
        let n: Number = field.deserialize(deserializer)?;
        Ok(i64::from(n))
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Archive,
    Deserialize,
    Serialize,
)]
#[archive(as = "Bool")]
#[repr(C)]
pub struct Bool([u8; 8]);

impl From<Bool> for bool {
    fn from(b: Bool) -> bool {
        u64::from_le_bytes(b.0) > 0
    }
}

impl From<bool> for Bool {
    fn from(b: bool) -> Bool {
        let n: u64 = if b { 1 } else { 0 };
        Self(n.to_le_bytes())
    }
}

impl ArchiveWith<bool> for Bool {
    type Archived = rkyv::Archived<Bool>;
    type Resolver = rkyv::Resolver<Bool>;

    unsafe fn resolve_with(field: &bool, pos: usize, res: BoolResolver, out: *mut Self::Archived) {
        Bool::from(*field).resolve(pos, res, out);
    }
}

impl<S: Fallible + ?Sized> SerializeWith<bool, S> for Bool {
    fn serialize_with(field: &bool, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Bool::from(*field).serialize(serializer)
    }
}

impl<D: Fallible + ?Sized> DeserializeWith<Archived<Bool>, bool, D> for Bool {
    fn deserialize_with(field: &Archived<Bool>, deserializer: &mut D) -> Result<bool, D::Error> {
        let b: Bool = field.deserialize(deserializer)?;
        Ok(bool::from(b))
    }
}

pub fn key_offset(
    key: &str,
    m: &ArchivedBTreeMap<ArchivedString, ArchivedSchema>,
) -> Option<usize> {
    if !m.contains_key(key) {
        return None;
    }
    let mut i = 0;
    for (k, _) in m.iter() {
        if k.as_str() == key {
            break;
        } else {
            i += 8;
        }
    }
    Some(i)
}

pub fn size_of(schema: &ArchivedSchema) -> usize {
    if let ArchivedSchema::Object(m) = schema {
        m.len() * 8
    } else {
        8
    }
}

pub struct Ptr<'a> {
    ptr: *const u8,
    schema: &'a ArchivedSchema,
}

impl<'a> Ptr<'a> {
    pub fn new(bytes: &'a [u8], schema: &'a ArchivedSchema) -> Self {
        let pos = bytes.len() - size_of(schema);
        let ptr = unsafe { (bytes as *const _ as *const u8).add(pos) };
        Self { ptr, schema }
    }

    pub fn from_ref<T>(ptr: &'a T, schema: &'a ArchivedSchema) -> Self {
        let ptr = ptr as *const _ as *const u8;
        Self { ptr, schema }
    }

    pub fn string(&self) -> Option<&str> {
        if let ArchivedSchema::Text = self.schema {
            let s = unsafe { &*(self.ptr as *const ArchivedString) };
            return Some(s.as_str());
        }
        None
    }

    pub fn boolean(&self) -> Option<bool> {
        if let ArchivedSchema::Boolean = self.schema {
            return Some(unsafe { *(self.ptr as *const Bool) }.into());
        }
        None
    }

    pub fn number(&self) -> Option<i64> {
        if let ArchivedSchema::Number = self.schema {
            return Some(unsafe { *(self.ptr as *const Number) }.into());
        }
        None
    }

    pub fn idx(&self, idx: usize) -> Option<Ptr<'a>> {
        if let ArchivedSchema::Array(_, schema) = self.schema {
            let rel_ptr = unsafe { &*(self.ptr as *const RawRelPtr) };
            // TODO bounds checking
            let ptr = unsafe { (rel_ptr.as_ptr() as *const u8).add(idx * 8) };
            return Some(Ptr { ptr, schema });
        }
        None
    }

    pub fn get(&self, key: &str) -> Option<Ptr<'a>> {
        if let ArchivedSchema::Object(m) = self.schema {
            let offset = key_offset(key, m)?;
            let ptr = unsafe { self.ptr.add(offset) };
            return Some(Ptr {
                ptr,
                schema: m.get(key).unwrap(),
            });
        }
        None
    }

    pub fn len(&self) -> Option<usize> {
        if let ArchivedSchema::Array(_, _) = self.schema {
            let array = unsafe { &*(self.ptr as *const ArchivedVec<u64>) };
            return Some(array.as_ref().len());
        }
        None
    }

    pub fn is_empty(&self) -> Option<bool> {
        self.len().map(|len| len == 0)
    }

    pub fn keys(&self) -> Option<impl Iterator<Item = &str>> {
        if let ArchivedSchema::Object(m) = self.schema {
            return Some(m.keys().map(|k| k.as_str()));
        }
        None
    }

    pub fn to_value(&self) -> Value {
        match self.schema {
            ArchivedSchema::Null => Value::Null,
            ArchivedSchema::Boolean => {
                Value::Primitive(PrimitiveValue::Boolean(self.boolean().unwrap()))
            }
            ArchivedSchema::Number => {
                Value::Primitive(PrimitiveValue::Number(self.number().unwrap()))
            }
            ArchivedSchema::Text => {
                Value::Primitive(PrimitiveValue::Text(self.string().unwrap().to_string()))
            }
            ArchivedSchema::Array(_, _) => {
                let len = self.len().unwrap();
                let mut arr = Vec::with_capacity(len);
                for i in 0..len {
                    arr.push(self.idx(i).unwrap().to_value());
                }
                Value::Array(arr)
            }
            ArchivedSchema::Object(_) => {
                let mut map = BTreeMap::new();
                for key in self.keys().unwrap() {
                    map.insert(key.to_string(), self.get(key).unwrap().to_value());
                }
                Value::Object(map)
            }
        }
    }
}
