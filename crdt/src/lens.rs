use crate::path::{Path, PathBuf, Segment};
use crate::schema::{PrimitiveKind, Schema};
use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::string::ArchivedString;
use rkyv::{Archive, Serialize};

type Prop = String;

/// Kind of a sequence of [`Path`] [`Segment`]s.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Archive, Serialize)]
#[archive_attr(allow(missing_docs))]
#[archive_attr(derive(Clone, Copy, Debug, Eq, PartialEq, CheckBytes))]
#[repr(C)]
pub enum Kind {
    /// Empty kind.
    Null,
    /// EWFlag kind.
    Flag,
    /// MVReg with values of [`PrimitiveKind`].
    Reg(PrimitiveKind),
    /// ORMap with keys of [`PrimitiveKind`].
    Table(PrimitiveKind),
    /// Struct is a named tuple crdt.
    Struct,
    /// Observed-Remove Array (ORArray) supporting the following operations: insert, move, update,
    /// and delete based on [Rinberg et al. 2021].
    /// The precedence of concurrent operations is as follows: UPDATE > DELETE > MOVE.
    ///
    /// [Rinberg et al. 2021]: https://dl.acm.org/doi/10.1145/3447865.3457971
    Array,
}

/// A [`Lens`] is a bidirectional transform on [`Schema`]s.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize)]
#[archive_attr(allow(missing_docs))]
#[archive_attr(derive(Debug, Eq, PartialEq, CheckBytes))]
#[archive_attr(check_bytes(
    bound = "__C: rkyv::validation::ArchiveContext, <__C as rkyv::Fallible>::Error: std::error::Error"
))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[repr(C)]
pub enum Lens {
    /// Makes a crdt of [`Kind`].
    Make(Kind),
    /// Destroys a crdt of [`Kind`].
    Destroy(Kind),
    /// Adds a field to a [`Kind::Struct`].
    AddProperty(Prop),
    /// Removes a field from a [`Kind::Struct`].
    RemoveProperty(Prop),
    /// Renames a field of a [`Kind::Struct`].
    RenameProperty(Prop, Prop),
    /// Moves a field from a nested [`Kind::Struct`] to it's parent.
    HoistProperty(Prop, Prop),
    /// Moves a field from a nested [`Kind::Struct`] to it's child.
    PlungeProperty(Prop, Prop),
    /// Applies the [`Lens`] to a [`Kind::Struct`] field.
    LensIn(
        Prop,
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        Box<Lens>,
    ),
    /// Applies the [`Lens`] to all values of a [`Kind::Array`].
    LensMap(
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        Box<Lens>,
    ),
    /// Applies the [`Lens`] to all values of a [`Kind::Table`].
    LensMapValue(
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        Box<Lens>,
    ),
}

impl Lens {
    /// Wraps the [`Lens`] in a [`Lens::LensIn`].
    pub fn lens_in(self, prop: &str) -> Self {
        Self::LensIn(prop.into(), Box::new(self))
    }

    /// Wraps the [`Lens`] in a [`Lens::LensMapValue`].
    pub fn lens_map_value(self) -> Self {
        Self::LensMapValue(Box::new(self))
    }
}

impl ArchivedLens {
    /// Returns a [`LensRef`] to an [`ArchivedLens`].
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
            Self::LensMap(l) => LensRef::LensMap(false, l),
            Self::LensMapValue(l) => LensRef::LensMapValue(false, l),
        }
    }
}

/// Reversible reference to an [`ArchivedLens`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LensRef<'a> {
    /// Reference to [`Lens::Make`].
    Make(ArchivedKind),
    /// Reference to [`Lens::Destroy`].
    Destroy(ArchivedKind),
    /// Reference to [`Lens::AddProperty`].
    AddProperty(&'a ArchivedString),
    /// Reference to [`Lens::RemoveProperty`].
    RemoveProperty(&'a ArchivedString),
    /// Reference to [`Lens::RenameProperty`].
    RenameProperty(&'a ArchivedString, &'a ArchivedString),
    /// Reference to [`Lens::HoistProperty`].
    HoistProperty(&'a ArchivedString, &'a ArchivedString),
    /// Reference to [`Lens::PlungeProperty`].
    PlungeProperty(&'a ArchivedString, &'a ArchivedString),
    /// Reference to [`Lens::LensIn`].
    LensIn(bool, &'a ArchivedString, &'a ArchivedLens),
    /// Reference to [`Lens::LensMap`].
    LensMap(bool, &'a ArchivedLens),
    /// Reference to [`Lens::LensMapValue`].
    LensMapValue(bool, &'a ArchivedLens),
}

impl<'a> LensRef<'a> {
    /// Reverse the [`ArchivedLens`].
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
            Self::LensMap(rev, lens) => Self::LensMap(!rev, lens),
            Self::LensMapValue(rev, lens) => Self::LensMapValue(!rev, lens),
        }
    }

    /// Reverses the [`ArchivedLens`] if `rev` is true.
    pub fn maybe_reverse(self, rev: bool) -> Self {
        if rev {
            self.reverse()
        } else {
            self
        }
    }

    /// Applies the [`Lens`] to a [`Schema`].
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
                    ArchivedKind::Array => Schema::Array(Box::new(Schema::Null)),
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

            (Self::LensMap(rev, lens), Schema::Array(schema)) => {
                lens.to_ref().maybe_reverse(*rev).transform_schema(schema)?
            }
            (_, s) => return Err(anyhow!("invalid lens for schema: {:?} {:?}", self, s)),
        }
        Ok(())
    }

    /// Applies the [`Lens`] to a sequence of [`Path`] [`Segment`]s.
    pub fn transform_path(&self, path: &[Segment]) -> Vec<Segment> {
        match self {
            Self::Make(_) => {}
            Self::Destroy(_) => return vec![],
            Self::AddProperty(_) => {}
            Self::RemoveProperty(prop) => {
                if path[0].prim_str() == Some(prop.as_str()) {
                    return vec![];
                }
            }
            Self::RenameProperty(from, to) => {
                if path[0].prim_str() == Some(from.as_str()) {
                    let mut p2 = vec![Segment::Str(to.to_string())];
                    p2.extend(path[1..].to_vec());
                    return p2;
                }
            }
            Self::HoistProperty(host, target) => {
                if path[0].prim_str() == Some(host.as_str())
                    && path[1].prim_str() == Some(target.as_str())
                {
                    return path[1..].to_vec();
                }
            }
            Self::PlungeProperty(host, target) => {
                if path[0].prim_str() == Some(target.as_str()) {
                    let mut p2 = vec![Segment::Str(host.to_string())];
                    p2.extend(path[1..].to_vec());
                    return p2;
                }
            }
            Self::LensIn(rev, key, lens) => {
                if path[0].prim_str() == Some(key.as_str()) {
                    let path = lens.to_ref().maybe_reverse(*rev).transform_path(&path[1..]);
                    if path.is_empty() {
                        return path;
                    }
                    let mut p2 = vec![Segment::Str(key.to_string())];
                    p2.extend(path);
                    return p2;
                }
            }
            Self::LensMap(rev, lens) => {
                let path = lens.to_ref().maybe_reverse(*rev).transform_path(&path[1..]);
                if path.is_empty() {
                    return path;
                }
                let mut p2 = vec![path[0].clone()];
                p2.extend(path);
                return p2;
            }
            Self::LensMapValue(rev, lens) => {
                let path = lens.to_ref().maybe_reverse(*rev).transform_path(&path[1..]);
                if path.is_empty() {
                    return path;
                }
                let mut p2 = vec![path[0].clone()];
                p2.extend(path);
                return p2;
            }
        }
        path.to_vec()
    }
}

/// An ordered sequence of [`Lens`]es.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq, CheckBytes))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[repr(C)]
pub struct Lenses(Vec<Lens>);

impl Lenses {
    /// Creates a new [`Lenses`] wrapper from a [`Vec<Lens>`].
    pub fn new(lenses: Vec<Lens>) -> Self {
        Self(lenses)
    }
}

impl ArchivedLenses {
    /// Returns a reference to the [`ArchivedLenses`].
    pub fn lenses(&self) -> &[ArchivedLens] {
        &self.0
    }

    /// Applies the [`Lens`]es to the identity schema [`Schema::Null`] and
    /// returns the archived result.
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

    /// Given another sequence of [`Lens`]es it returns the sequence of [`Lens`]es
    /// required to transfrom from one [`Schema`] to another.
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

    /// Transforms a [`Path`] valid in the source [`Schema`] to a [`PathBuf`] valid in the
    /// target [`Schema`].
    pub fn transform_path(&self, path: Path, target: &ArchivedLenses) -> Option<PathBuf> {
        let mut segments: Vec<Segment> = path.child().unwrap().into_iter().collect();
        for lens in self.transform(target) {
            segments = lens.transform_path(&segments);
            if segments.is_empty() {
                return None;
            }
        }
        let doc = path.first().unwrap().doc().unwrap();
        let mut path = PathBuf::new();
        path.doc(&doc);
        path.extend(segments.into_iter().collect::<PathBuf>().as_path());
        Some(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::props::*;
    use crate::registry::EMPTY_LENSES;
    use crate::util::Ref;
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
        #[ignore] // props don't generate signatures
        fn transform_preserves_validity((lens, mut schema, mut causal) in lens_schema_and_causal()) {
            let from = Ref::<Lenses>::new(EMPTY_LENSES.as_ref().into());
            let to = Ref::archive(&Lenses::new(vec![lens]));
            let lens = to.as_ref().lenses()[0].to_ref();
            prop_assume!(validate(&schema, &causal));
            prop_assume!(lens.transform_schema(&mut schema).is_ok());
            causal.transform(from.as_ref(), to.as_ref());
            prop_assert!(validate(&schema, &causal));
        }
    }
}
