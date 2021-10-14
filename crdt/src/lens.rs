use crate::crdt::DotStore;
use crate::{Crdt, DocId, PrimitiveKind, Prop, Schema};
use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::string::ArchivedString;
use rkyv::{Archive, Serialize};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Archive, Serialize)]
#[archive_attr(derive(Clone, Copy, Debug, Eq, PartialEq, CheckBytes))]
#[repr(C)]
pub enum Kind {
    Null,
    Flag,
    Reg(PrimitiveKind),
    Table(PrimitiveKind),
    Struct,
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq, CheckBytes))]
#[archive_attr(check_bytes(
    bound = "__C: rkyv::validation::ArchiveContext, <__C as rkyv::Fallible>::Error: std::error::Error"
))]
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
    LensIn(
        Prop,
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        Box<Lens>,
    ),
    LensMapValue(
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        Box<Lens>,
    ),
}

impl Lens {
    pub fn lens_in(self, prop: &str) -> Self {
        Self::LensIn(prop.into(), Box::new(self))
    }

    pub fn lens_map_value(self) -> Self {
        Self::LensMapValue(Box::new(self))
    }
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
            Self::LensIn(k, l) => LensRef::LensIn(false, k, l),
            Self::LensMapValue(l) => LensRef::LensMapValue(false, l),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LensRef<'a> {
    Make(ArchivedKind),
    Destroy(ArchivedKind),
    AddProperty(&'a ArchivedString),
    RemoveProperty(&'a ArchivedString),
    RenameProperty(&'a ArchivedString, &'a ArchivedString),
    HoistProperty(&'a ArchivedString, &'a ArchivedString),
    PlungeProperty(&'a ArchivedString, &'a ArchivedString),
    LensIn(bool, &'a ArchivedString, &'a ArchivedLens),
    LensMapValue(bool, &'a ArchivedLens),
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
            Self::LensIn(rev, key, lens) => Self::LensIn(!rev, key, lens),
            Self::LensMapValue(rev, lens) => Self::LensMapValue(!rev, lens),
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
                    ArchivedKind::Null => return Err(anyhow!("cannot make a null schema")),
                    ArchivedKind::Flag => Schema::Flag,
                    ArchivedKind::Reg(kind) => Schema::Reg(*kind),
                    ArchivedKind::Table(kind) => Schema::Table(*kind, Box::new(Schema::Null)),
                    ArchivedKind::Struct => Schema::Struct(Default::default()),
                }
            }
            (Self::Destroy(k), s) => {
                match (k, &s) {
                    (ArchivedKind::Flag, Schema::Flag) => {}
                    (ArchivedKind::Reg(k1), Schema::Reg(k2)) => {
                        if k1 != k2 {
                            return Err(anyhow!("can't destroy different kind"));
                        }
                    }
                    (ArchivedKind::Table(k1), Schema::Table(k2, s)) => {
                        if k1 != k2 {
                            return Err(anyhow!("can't destroy different kind"));
                        }
                        if **s != Schema::Null {
                            return Err(anyhow!("can't destroy table with non null schema"));
                        }
                    }
                    (ArchivedKind::Struct, Schema::Struct(m)) => {
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
            (Self::AddProperty(key), Schema::Struct(m)) => {
                if m.contains_key(key.as_str()) {
                    return Err(anyhow!("property {} already exists in schema", key));
                }
                m.insert(key.to_string(), Schema::Null);
            }
            (Self::RemoveProperty(key), Schema::Struct(m)) => {
                match m.get(key.as_str()) {
                    Some(Schema::Null) => {}
                    Some(_) => return Err(anyhow!("property {} cannot be removed", key)),
                    None => return Err(anyhow!("property {} doesn't exist in schema", key)),
                }
                m.remove(key.as_str());
            }
            (Self::RenameProperty(from, to), Schema::Struct(m)) => {
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
            (Self::HoistProperty(host, target), Schema::Struct(m)) => {
                if m.contains_key(target.as_str()) {
                    return Err(anyhow!("target property {} already exists", target));
                }
                if let Some(Schema::Struct(host)) = m.get_mut(host.as_str()) {
                    if let Some(s) = host.remove(target.as_str()) {
                        m.insert(target.to_string(), s);
                    } else {
                        return Err(anyhow!("target property {} doesn't exist", target));
                    }
                } else {
                    return Err(anyhow!("host property {} doesn't exist", host));
                }
            }
            (Self::PlungeProperty(host, target), Schema::Struct(m)) => {
                if host == target {
                    return Err(anyhow!("host and target property are the same"));
                }
                let s = if let Some(s) = m.remove(target.as_str()) {
                    s
                } else {
                    return Err(anyhow!("target property {} doesn't exist", target));
                };
                if let Some(Schema::Struct(host)) = m.get_mut(host.as_str()) {
                    if host.contains_key(target.as_str()) {
                        return Err(anyhow!("host already contains target property {}", target));
                    }
                    host.insert(target.to_string(), s);
                } else {
                    return Err(anyhow!("host property doesn't exist"));
                }
            }
            (Self::LensIn(rev, key, lens), Schema::Struct(m)) if m.contains_key(key.as_str()) => {
                lens.to_ref()
                    .maybe_reverse(*rev)
                    .transform_schema(m.get_mut(key.as_str()).unwrap())?;
            }
            (Self::LensMapValue(rev, lens), Schema::Table(_, schema)) => {
                lens.to_ref().maybe_reverse(*rev).transform_schema(schema)?
            }
            (_, s) => return Err(anyhow!("invalid lens for schema: {:?} {:?}", self, s)),
        }
        Ok(())
    }

    pub fn transform_dotstore(&self, _store: &mut DotStore) {
        // TODO
    }

    pub fn transform_crdt(&self, _doc: &DocId, _store: &Crdt) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq, CheckBytes))]
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

    pub fn transform_dotstore(&self, store: &mut DotStore, target: &ArchivedLenses) {
        for lens in self.transform(target) {
            tracing::info!("{:?}", lens);
            lens.transform_dotstore(store);
        }
    }

    pub fn transform_crdt(&self, doc: &DocId, crdt: &Crdt, target: &ArchivedLenses) -> Result<()> {
        for lens in self.transform(target) {
            lens.transform_crdt(doc, crdt)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::props::*;
    use crate::Ref;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn reversible((lens, schema) in lens_and_schema()) {
            let lens = Ref::archive(&lens);
            let mut schema2 = schema.clone();
            prop_assume!(lens.as_ref().to_ref().transform_schema(&mut schema2).is_ok());
            lens.as_ref().to_ref().reverse().transform_schema(&mut schema2).unwrap();
            prop_assert_eq!(schema, schema2);
        }

        #[test]
        fn transform_preserves_validity((lens, mut schema, mut causal) in lens_schema_and_causal()) {
            let lens = Ref::archive(&lens);
            prop_assume!(validate(&schema, &causal));
            prop_assume!(lens.as_ref().to_ref().transform_schema(&mut schema).is_ok());
            lens.as_ref().to_ref().transform_dotstore(&mut causal.store);
            prop_assert!(validate(&schema, &causal));
        }

        #[test]
        #[ignore]
        fn crdt_transform((lens, schema, mut causal) in lens_schema_and_causal()) {
            let lens = Ref::archive(&lens);
            prop_assume!(validate(&schema, &causal));
            let crdt = causal_to_crdt(&causal);
            lens.as_ref().to_ref().transform_crdt(&causal.doc, &crdt).unwrap();
            let causal2 = crdt_to_causal(&crdt, &causal.ctx());
            lens.as_ref().to_ref().transform_dotstore(&mut causal.store);
            assert_eq!(causal, causal2);
        }
    }
}
