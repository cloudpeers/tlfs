use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeSet;
use std::iter::FromIterator;

/// Path identifier is the blake3 hash of a path.
#[derive(
    Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, CheckBytes, Deserialize, Serialize,
)]
#[archive(as = "Dot")]
#[repr(transparent)]
pub struct Dot([u8; 32]);

impl Dot {
    /// Creates a new [`Dot`] from a [`[u8; 32]`].
    pub fn new(dot: [u8; 32]) -> Self {
        Self(dot)
    }
}

impl From<Dot> for [u8; 32] {
    fn from(dot: Dot) -> Self {
        dot.0
    }
}

impl AsRef<[u8; 32]> for Dot {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for Dot {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut id = [0; 44];
        base64::encode_config_slice(&self.0, base64::URL_SAFE, &mut id);
        write!(f, "Dot({})", std::str::from_utf8(&id[..4]).expect("wtf?"))
    }
}

impl std::fmt::Display for Dot {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut dot = [0; 44];
        base64::encode_config_slice(&self.0, base64::URL_SAFE, &mut dot);
        write!(f, "{}", std::str::from_utf8(&dot).expect("wtf?"))
    }
}

impl std::str::FromStr for Dot {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 44 {
            return Err(anyhow::anyhow!("invalid dot length {}", s.len()));
        }
        let mut dot = [0; 32];
        base64::decode_config_slice(s, base64::URL_SAFE, &mut dot)?;
        Ok(Self(dot))
    }
}

/// Set of path identifiers.
#[derive(
    Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, Deserialize, Serialize,
)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub struct DotSet(BTreeSet<Dot>);

impl DotSet {
    /// Creates a new empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns an iterator of [`Dot`].
    pub fn iter(&self) -> impl Iterator<Item = &Dot> + '_ {
        self.0.iter()
    }

    /// Returns true if the [`DotSet`] contains the [`Dot`].
    pub fn contains(&self, dot: &Dot) -> bool {
        self.0.contains(dot)
    }

    /// Inserts a [`Dot`] into the [`DotSet`].
    pub fn insert(&mut self, dot: Dot) {
        self.0.insert(dot);
    }

    /// Inserts all [`Dot`]s from a [`DotSet`].
    pub fn union(&mut self, other: &Self) {
        for dot in other.iter() {
            self.insert(*dot);
        }
    }

    /// Returns a new [`DotSet`] containing all [`Dot`]s not in [`ArchivedDotSet`].
    pub fn difference(&self, other: &ArchivedDotSet) -> DotSet {
        self.iter()
            .filter(|dot| !other.0.contains_key(dot))
            .copied()
            .collect()
    }

    /// Returns a new [`DotSet`] containing all [`Dot`]s in [`DotSet`].
    pub fn intersection(&self, other: &Self) -> DotSet {
        self.iter()
            .filter(|dot| other.contains(dot))
            .copied()
            .collect()
    }
}

impl FromIterator<Dot> for DotSet {
    fn from_iter<T: IntoIterator<Item = Dot>>(iter: T) -> Self {
        let mut res = Self::new();
        for dot in iter.into_iter() {
            res.insert(dot);
        }
        res
    }
}
