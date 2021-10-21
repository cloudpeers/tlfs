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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, dot: Dot) {
        self.0.insert(dot);
    }

    pub fn union(&mut self, other: &impl AbstractDotSet) {
        for dot in other.iter() {
            self.insert(*dot);
        }
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

pub trait AbstractDotSet {
    fn iter(&self) -> Box<dyn Iterator<Item = &Dot> + '_>;
    fn contains(&self, dot: &Dot) -> bool;

    fn difference(&self, other: &impl AbstractDotSet) -> DotSet {
        self.iter()
            .filter(|dot| !other.contains(dot))
            .copied()
            .collect()
    }

    fn intersection(&self, other: &impl AbstractDotSet) -> DotSet {
        self.iter()
            .filter(|dot| other.contains(dot))
            .copied()
            .collect()
    }

    fn to_dotset(&self) -> DotSet;
}

impl AbstractDotSet for DotSet {
    fn iter(&self) -> Box<dyn Iterator<Item = &Dot> + '_> {
        Box::new(self.0.iter())
    }

    fn contains(&self, dot: &Dot) -> bool {
        self.0.contains(dot)
    }

    fn to_dotset(&self) -> DotSet {
        self.clone()
    }
}

impl AbstractDotSet for ArchivedDotSet {
    fn iter(&self) -> Box<dyn Iterator<Item = &Dot> + '_> {
        Box::new(self.0.iter())
    }

    fn contains(&self, dot: &Dot) -> bool {
        self.0.contains_key(dot)
    }

    fn to_dotset(&self) -> DotSet {
        self.0.iter().copied().collect()
    }
}
