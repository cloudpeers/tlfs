use anyhow::{anyhow, Result};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::string::ArchivedString;
use rkyv::{Archive, Deserialize, Infallible, Serialize};
use std::collections::BTreeMap;

pub type Prop = String;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Archive, Serialize)]
#[archive(as = "PrimitiveKind")]
#[repr(C)]
pub enum PrimitiveKind {
    Boolean,
    Number,
    Text,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Archive, Serialize)]
#[archive(as = "Kind")]
#[repr(C)]
pub enum Kind {
    Null,
    Primitive(PrimitiveKind),
    Array,
    Object,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, Hash, PartialEq))]
#[archive(compare(PartialEq))]
#[repr(C)]
pub enum PrimitiveValue {
    Boolean(bool),
    Number(i64),
    Text(String),
}

impl ArchivedPrimitiveValue {
    pub fn kind_of(&self) -> PrimitiveKind {
        match self {
            Self::Boolean(_) => PrimitiveKind::Boolean,
            Self::Number(_) => PrimitiveKind::Number,
            Self::Text(_) => PrimitiveKind::Text,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[repr(C)]
pub enum Value {
    Null,
    Primitive(PrimitiveValue),
    Array(#[omit_bounds] Vec<Value>),
    Object(#[omit_bounds] BTreeMap<Prop, Value>),
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[repr(C)]
pub enum Schema {
    Null,
    Boolean,
    Number,
    Text,
    Array(bool, #[omit_bounds] Box<Schema>),
    Object(#[omit_bounds] BTreeMap<Prop, Schema>),
}

impl ArchivedSchema {
    pub fn validate(&self, v: &Value) -> bool {
        match (self, v) {
            (Self::Null, Value::Null) => true,
            (Self::Boolean, Value::Primitive(PrimitiveValue::Boolean(_))) => true,
            (Self::Number, Value::Primitive(PrimitiveValue::Number(_))) => true,
            (Self::Text, Value::Primitive(PrimitiveValue::Text(_))) => true,
            (Self::Array(e, s), Value::Array(vs)) => {
                if vs.is_empty() {
                    *e
                } else {
                    for v in vs {
                        if !s.validate(v) {
                            return false;
                        }
                    }
                    true
                }
            }
            (Self::Object(sm), Value::Object(vm)) => {
                for k in sm.keys() {
                    if !vm.contains_key(k.as_str()) {
                        return false;
                    }
                }
                for (k, v) in vm {
                    if let Some(s) = sm.get(k.as_str()) {
                        if !s.validate(v) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[repr(C)]
pub enum Lens {
    Make(Kind),
    Destroy(Kind),
    AddProperty(Prop),
    RemoveProperty(Prop),
    RenameProperty(Prop, Prop),
    HoistProperty(Prop, Prop),
    PlungeProperty(Prop, Prop),
    Wrap,
    Head,
    LensIn(Prop, #[omit_bounds] Box<Lens>),
    LensMap(#[omit_bounds] Box<Lens>),
    Convert(
        PrimitiveKind,
        PrimitiveKind,
        Vec<(PrimitiveValue, PrimitiveValue)>,
    ),
}

impl ArchivedLens {
    pub fn to_ref(&self) -> LensRef<'_> {
        match self {
            Self::Make(k) => LensRef::Make(*k),
            Self::Destroy(k) => LensRef::Destroy(*k),
            Self::AddProperty(p) => LensRef::AddProperty(p),
            Self::RemoveProperty(p) => LensRef::RemoveProperty(p),
            Self::RenameProperty(p1, p2) => LensRef::RenameProperty(p1, p2),
            Self::HoistProperty(h, t) => LensRef::HoistProperty(h, t),
            Self::PlungeProperty(h, t) => LensRef::PlungeProperty(h, t),
            Self::Wrap => LensRef::Wrap,
            Self::Head => LensRef::Head,
            Self::LensIn(k, l) => LensRef::LensIn(false, k, l),
            Self::LensMap(l) => LensRef::LensMap(false, l),
            Self::Convert(k1, k2, m) => LensRef::Convert(false, *k1, *k2, m.as_ref()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LensRef<'a> {
    Make(Kind),
    Destroy(Kind),
    AddProperty(&'a ArchivedString),
    RemoveProperty(&'a ArchivedString),
    RenameProperty(&'a ArchivedString, &'a ArchivedString),
    HoistProperty(&'a ArchivedString, &'a ArchivedString),
    PlungeProperty(&'a ArchivedString, &'a ArchivedString),
    Wrap,
    Head,
    LensIn(bool, &'a ArchivedString, &'a ArchivedLens),
    LensMap(bool, &'a ArchivedLens),
    Convert(
        bool,
        PrimitiveKind,
        PrimitiveKind,
        &'a [(ArchivedPrimitiveValue, ArchivedPrimitiveValue)],
    ),
}

impl<'a> LensRef<'a> {
    pub fn reverse(self) -> Self {
        match self {
            Self::Make(kind) => Self::Destroy(kind),
            Self::Destroy(kind) => Self::Make(kind),
            Self::AddProperty(key) => Self::RemoveProperty(key),
            Self::RemoveProperty(key) => Self::AddProperty(key),
            Self::RenameProperty(from, to) => Self::RenameProperty(to, from),
            Self::HoistProperty(host, target) => Self::PlungeProperty(host, target),
            Self::PlungeProperty(host, target) => Self::HoistProperty(host, target),
            Self::Wrap => Self::Head,
            Self::Head => Self::Wrap,
            Self::LensIn(rev, key, lens) => Self::LensIn(!rev, key, lens),
            Self::LensMap(rev, lens) => Self::LensMap(!rev, lens),
            Self::Convert(rev, from, to, map) => Self::Convert(!rev, from, to, map),
        }
    }

    pub fn maybe_reverse(self, rev: bool) -> Self {
        if rev {
            self.reverse()
        } else {
            self
        }
    }

    pub fn transform_schema(&self, s: &mut Schema) -> Result<()> {
        match (self, s) {
            (Self::Make(k), s) => {
                if *s != Schema::Null {
                    return Err(anyhow!("cannot make schema"));
                }
                *s = match k {
                    Kind::Null => return Err(anyhow!("cannot make a null schema")),
                    Kind::Primitive(PrimitiveKind::Boolean) => Schema::Boolean,
                    Kind::Primitive(PrimitiveKind::Number) => Schema::Number,
                    Kind::Primitive(PrimitiveKind::Text) => Schema::Text,
                    Kind::Array => Schema::Array(true, Box::new(Schema::Null)),
                    Kind::Object => Schema::Object(Default::default()),
                }
            }
            (Self::Destroy(k), s) => {
                match (k, &s) {
                    (Kind::Primitive(PrimitiveKind::Boolean), Schema::Boolean) => {}
                    (Kind::Primitive(PrimitiveKind::Number), Schema::Number) => {}
                    (Kind::Primitive(PrimitiveKind::Text), Schema::Text) => {}
                    (Kind::Array, Schema::Array(true, s)) => {
                        if **s != Schema::Null {
                            return Err(anyhow!("can't destroy non empty array"));
                        }
                    }
                    (Kind::Object, Schema::Object(m)) => {
                        if !m.is_empty() {
                            return Err(anyhow!("can't destroy non empty object"));
                        }
                    }
                    (kind, schema) => {
                        return Err(anyhow!("can't apply destroy {:?} {:?}", kind, schema))
                    }
                }
                *s = Schema::Null;
            }
            (Self::AddProperty(key), Schema::Object(m)) => {
                if m.contains_key(key.as_str()) {
                    return Err(anyhow!("property {} already exists in schema", key));
                }
                m.insert(key.to_string(), Schema::Null);
            }
            (Self::RemoveProperty(key), Schema::Object(m)) => {
                match m.get(key.as_str()) {
                    Some(Schema::Null) => {}
                    Some(_) => return Err(anyhow!("property {} cannot be removed", key)),
                    None => return Err(anyhow!("property {} doesn't exist in schema", key)),
                }
                m.remove(key.as_str());
            }
            (Self::RenameProperty(from, to), Schema::Object(m)) => {
                if m.contains_key(to.as_str()) {
                    return Err(anyhow!("trying to rename to existing property: {}", to));
                }
                if let Some(s) = m.remove(from.as_str()) {
                    m.insert(to.to_string(), s);
                } else {
                    return Err(anyhow!(
                        "cannot rename property that doesn't exist: {}",
                        from
                    ));
                }
            }
            (Self::HoistProperty(host, target), Schema::Object(m)) => {
                if m.contains_key(target.as_str()) {
                    return Err(anyhow!("target property {} already exists", target));
                }
                if let Some(Schema::Object(host)) = m.get_mut(host.as_str()) {
                    if let Some(s) = host.remove(target.as_str()) {
                        m.insert(target.to_string(), s);
                    } else {
                        return Err(anyhow!("target property {} doesn't exist", target));
                    }
                } else {
                    return Err(anyhow!("host property {} doesn't exist", host));
                }
            }
            (Self::PlungeProperty(host, target), Schema::Object(m)) => {
                if host == target {
                    return Err(anyhow!("host and target property are the same"));
                }
                let s = if let Some(s) = m.remove(target.as_str()) {
                    s
                } else {
                    return Err(anyhow!("target property {} doesn't exist", target));
                };
                if let Some(Schema::Object(host)) = m.get_mut(host.as_str()) {
                    if host.contains_key(target.as_str()) {
                        return Err(anyhow!("host already contains target property {}", target));
                    }
                    host.insert(target.to_string(), s);
                } else {
                    return Err(anyhow!("host property doesn't exist"));
                }
            }
            (Self::Wrap, s) => *s = Schema::Array(false, Box::new(s.clone())),
            (Self::Head, s) => {
                if let Schema::Array(false, s2) = s {
                    *s = (**s2).clone();
                } else {
                    return Err(anyhow!("cannot apply head to {:?}", s));
                }
            }
            (Self::LensIn(rev, key, lens), Schema::Object(m)) if m.contains_key(key.as_str()) => {
                lens.to_ref()
                    .maybe_reverse(*rev)
                    .transform_schema(m.get_mut(key.as_str()).unwrap())?;
            }
            (Self::LensMap(rev, lens), Schema::Array(_, s)) => {
                lens.to_ref().maybe_reverse(*rev).transform_schema(s)?
            }
            (Self::Convert(rev, from, to, map), s) => {
                for (va, vb) in map.iter() {
                    if va.kind_of() != *from || vb.kind_of() != *to {
                        return Err(anyhow::anyhow!("invalid map"));
                    }
                }
                let (from, to) = if *rev { (to, from) } else { (from, to) };
                match (from, &s) {
                    (PrimitiveKind::Boolean, Schema::Boolean) => {}
                    (PrimitiveKind::Number, Schema::Number) => {}
                    (PrimitiveKind::Text, Schema::Text) => {}
                    _ => return Err(anyhow!("kind doesn't match schema")),
                }
                *s = match to {
                    PrimitiveKind::Boolean => Schema::Boolean,
                    PrimitiveKind::Number => Schema::Number,
                    PrimitiveKind::Text => Schema::Text,
                }
            }
            (_, s) => return Err(anyhow!("invalid lens for schema: {:?} {:?}", self, s)),
        }
        Ok(())
    }

    pub fn transform_value(&self, v: &mut Value) {
        match (self, v) {
            (Self::Make(k), v) => {
                *v = match k {
                    Kind::Null => Value::Null,
                    Kind::Primitive(PrimitiveKind::Boolean) => {
                        Value::Primitive(PrimitiveValue::Boolean(false))
                    }
                    Kind::Primitive(PrimitiveKind::Number) => {
                        Value::Primitive(PrimitiveValue::Number(0))
                    }
                    Kind::Primitive(PrimitiveKind::Text) => {
                        Value::Primitive(PrimitiveValue::Text("".into()))
                    }
                    Kind::Array => Value::Array(vec![]),
                    Kind::Object => Value::Object(Default::default()),
                };
            }
            (Self::Destroy(_), v) => {
                *v = Value::Null;
            }
            (Self::AddProperty(key), Value::Object(m)) => {
                m.insert(key.to_string(), Value::Null);
            }
            (Self::RemoveProperty(key), Value::Object(m)) => {
                m.remove(key.as_str());
            }
            (Self::RenameProperty(from, to), Value::Object(m)) => {
                if let Some(v) = m.remove(from.as_str()) {
                    m.insert(to.to_string(), v);
                }
            }
            (Self::HoistProperty(host, target), Value::Object(m)) => {
                if let Some(Value::Object(host)) = m.get_mut(host.as_str()) {
                    if let Some(v) = host.remove(target.as_str()) {
                        m.insert(target.to_string(), v);
                    }
                }
            }
            (Self::PlungeProperty(host, target), Value::Object(m)) => {
                if let Some(v) = m.remove(target.as_str()) {
                    if let Some(Value::Object(host)) = m.get_mut(host.as_str()) {
                        host.insert(target.to_string(), v);
                    } else {
                        m.insert(target.to_string(), v);
                    }
                }
            }
            (Self::Wrap, v) => {
                *v = Value::Array(vec![v.clone()]);
            }
            (Self::Head, v) => {
                if let Value::Array(vs) = &v {
                    if let Some(head) = vs.get(0) {
                        *v = head.clone();
                    }
                }
            }
            (Self::LensIn(rev, key, lens), Value::Object(m)) => {
                if let Some(v) = m.get_mut(key.as_str()) {
                    lens.to_ref().maybe_reverse(*rev).transform_value(v);
                }
            }
            (Self::LensMap(rev, lens), Value::Array(vs)) => {
                for v in vs.iter_mut() {
                    lens.to_ref().maybe_reverse(*rev).transform_value(v);
                }
            }
            (Self::Convert(rev, from, to, map), Value::Primitive(p)) => {
                for (k, v) in map.iter() {
                    if k == p {
                        *p = v.deserialize(&mut Infallible).unwrap();
                        break;
                    }
                }
                let k = if *rev { to } else { from };
                *p = match k {
                    PrimitiveKind::Boolean => PrimitiveValue::Boolean(false),
                    PrimitiveKind::Number => PrimitiveValue::Number(0),
                    PrimitiveKind::Text => PrimitiveValue::Text("".into()),
                };
            }
            _ => {}
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[repr(C)]
pub struct Lenses(Vec<Lens>);

impl Lenses {
    pub fn new(lenses: Vec<Lens>) -> Self {
        Self(lenses)
    }
}

impl ArchivedLenses {
    pub fn lenses(&self) -> &[ArchivedLens] {
        &self.0
    }

    pub fn to_schema(&self) -> Result<Vec<u8>> {
        let mut schema = Schema::Null;
        for lens in self.0.as_ref() {
            lens.to_ref().transform_schema(&mut schema)?;
        }
        let mut ser = AllocSerializer::<256>::default();
        ser.serialize_value(&schema)
            .map_err(|err| anyhow::anyhow!("{}", err))?;
        let bytes = ser.into_serializer().into_inner().to_vec();
        Ok(bytes)
    }

    pub fn transform<'a>(&'a self, b: &'a ArchivedLenses) -> Vec<LensRef<'a>> {
        let mut prefix = 0;
        for (a, b) in self.0.iter().zip(b.0.iter()) {
            if a == b {
                prefix += 1;
            } else {
                break;
            }
        }
        let mut c = Vec::with_capacity(self.0.len() + b.0.len() - 2 * prefix);
        for a in self.0[prefix..].iter().rev() {
            c.push(a.to_ref().reverse());
        }
        for b in b.0[prefix..].iter() {
            c.push(b.to_ref());
        }
        c
    }
}
