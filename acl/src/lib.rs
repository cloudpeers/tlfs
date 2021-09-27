use anyhow::anyhow;
use crepe::crepe;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, Deserialize, Serialize)]
#[archive(as = "DocId")]
#[repr(transparent)]
pub struct DocId([u8; 32]);

impl DocId {
    pub fn new(id: [u8; 32]) -> Self {
        Self(id)
    }
}

impl std::fmt::Debug for DocId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut peer_id = [0; 44];
        base64::encode_config_slice(&self.0, base64::URL_SAFE, &mut peer_id);
        write!(f, "{}", std::str::from_utf8(&peer_id).expect("wtf?"))
    }
}

impl std::fmt::Display for DocId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::str::FromStr for DocId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 44 {
            return Err(anyhow::anyhow!("invalid peer_id length {}", s.len()));
        }
        let mut peer_id = [0; 32];
        base64::decode_config_slice(s, base64::URL_SAFE, &mut peer_id)?;
        Ok(Self(peer_id))
    }
}

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, Deserialize, Serialize)]
#[archive(as = "PeerId")]
#[repr(transparent)]
pub struct PeerId([u8; 32]);

impl PeerId {
    pub fn new(id: [u8; 32]) -> Self {
        Self(id)
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut peer_id = [0; 44];
        base64::encode_config_slice(&self.0, base64::URL_SAFE, &mut peer_id);
        write!(f, "{}", std::str::from_utf8(&peer_id).expect("wtf?"))
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::str::FromStr for PeerId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 44 {
            return Err(anyhow::anyhow!("invalid peer_id length {}", s.len()));
        }
        let mut peer_id = [0; 32];
        base64::decode_config_slice(s, base64::URL_SAFE, &mut peer_id)?;
        Ok(Self(peer_id))
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Archive, Deserialize, Serialize,
)]
#[archive(as = "Permission")]
#[repr(u8)]
pub enum Permission {
    /// Permission to read ciphertext
    Sync,
    /// Permission to read plaintext, implies sync
    Read,
    /// Permission to write, implies read
    Write,
    /// Permission to delegate sync/read/write, implies write
    Control,
    /// Permission to delegate sync/read/write/control/own, implies control
    Own,
}

impl Permission {
    fn controllable(self) -> bool {
        matches!(self, Self::Sync | Self::Read | Self::Write)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Archive, Deserialize, Serialize)]
#[archive(as = "Actor")]
#[repr(C)]
pub enum Actor {
    Doc(DocId),
    Peer(PeerId),
    Anonymous,
    Unbound,
}

impl Actor {
    fn is_local_authority(self) -> bool {
        matches!(self, Actor::Doc(_))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[repr(C)]
pub enum Label {
    Root(DocId),
    Dot(#[omit_bounds] Box<Label>, String),
}

impl Label {
    fn root(&self) -> DocId {
        match self {
            Self::Root(id) => *id,
            Self::Dot(l, _) => l.root(),
        }
    }

    fn is_ancestor(&self, other: &Label) -> bool {
        let s = self.to_string();
        let s2 = other.to_string();
        s2.starts_with(&s)
    }
}

impl std::str::FromStr for Label {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut seg = s.split('.');
        let root = seg
            .next()
            .ok_or_else(|| anyhow!("missing document identifier"))?;
        let mut l = Self::Root(root.parse()?);
        for seg in seg {
            l = Self::Dot(Box::new(l), seg.to_string());
        }
        Ok(l)
    }
}

impl std::fmt::Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Root(id) => write!(f, "{}", id),
            Self::Dot(l, s) => {
                write!(f, "{}", l)?;
                write!(f, ".{}", s)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
#[repr(C)]
pub struct Can {
    actor: Actor,
    perm: Permission,
    label: Label,
}

impl Can {
    pub fn new(actor: Actor, perm: Permission, label: Label) -> Self {
        Self { actor, perm, label }
    }

    fn as_ref(&self) -> CanRef<'_> {
        CanRef {
            actor: self.actor,
            perm: self.perm,
            label: &self.label,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CanRef<'a> {
    actor: Actor,
    perm: Permission,
    label: &'a Label,
}

impl<'a> CanRef<'a> {
    pub fn actor(self) -> Actor {
        self.actor
    }

    pub fn perm(self) -> Permission {
        self.perm
    }

    pub fn label(self) -> &'a Label {
        self.label
    }

    fn root(self) -> DocId {
        self.label().root()
    }

    fn implies(self, other: CanRef<'a>) -> bool {
        if other.actor != self.actor && other.actor != Actor::Unbound {
            return false;
        }
        other.perm <= self.perm() && self.label().is_ancestor(other.label())
    }

    fn bind(self, rule: CanRef<'a>) -> Self {
        Self {
            actor: rule.actor,
            perm: self.perm,
            label: self.label,
        }
    }
}

impl std::fmt::Display for Can {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?} can {:?} {}", self.actor, self.perm, self.label)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
#[repr(C)]
pub enum Says {
    Can(usize, Actor, Can),
    CanIf(usize, Actor, Can, Can),
    Revokes(Actor, usize),
}

impl std::fmt::Display for Says {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Can(id, actor, can) => write!(f, "{}: {:?} says {}", id, actor, can),
            Self::CanIf(id, actor, can, cond) => {
                write!(f, "{}: {:?} says {} if {}", id, actor, can, cond)
            }
            Self::Revokes(actor, id) => write!(f, "{:?} revokes {}", actor, id),
        }
    }
}

crepe! {
    @input
    struct Input<'a>(&'a Says);

    struct DerivedCanIf<'a>(usize, Actor, CanRef<'a>, CanRef<'a>);

    struct DerivedCan<'a>(usize, Actor, CanRef<'a>);

    struct DerivedRevokes<'a>(Actor, usize, Actor, CanRef<'a>);

    struct MaybeRevoked<'a>(usize, CanRef<'a>, Actor);

    @output
    struct Authorized<'a>(usize, CanRef<'a>, Actor);

    @output
    struct Revoked(usize);

    DerivedCan(*id, *actor, can.as_ref()) <-
        Input(s),
        let Says::Can(id, actor, can) = s;

    DerivedCanIf(*id, *actor, can.as_ref(), cond.as_ref()) <-
        Input(s),
        let Says::CanIf(id, actor, can, cond) = s;

    DerivedRevokes(*actor, *id, gactor, can) <-
        Input(s),
        let Says::Revokes(actor, id) = s,
        Authorized(*id, can, gactor);

    // resolve conditional
    DerivedCan(id, actor, can.bind(auth)) <-
        DerivedCanIf(id, actor, can, cond),
        Authorized(_, auth, _),
        (auth.implies(cond));

    // local authority
    Authorized(id, can, actor) <-
        DerivedCan(id, actor, can),
        let Actor::Doc(root) = actor,
        (root == can.root());

    // ownership
    Authorized(id, can, actor) <-
        DerivedCan(id, actor, can),
        Authorized(_, auth, _),
        (auth.perm() == Permission::Own),
        (actor == auth.actor() && auth.label().is_ancestor(can.label()));

    // control
    Authorized(id, can, actor) <-
        DerivedCan(id, actor, can),
        Authorized(_, auth, _),
        (auth.perm() == Permission::Control && can.perm().controllable()),
        (actor == auth.actor() && auth.label().is_ancestor(can.label()));

    // higher privileges can revoke
    Revoked(id) <-
        DerivedRevokes(actor, id, gactor, can),
        Authorized(_, auth, _),
        (auth.actor() == actor && auth.perm() >= Permission::Control || actor == Actor::Doc(can.root())),
        (
            auth.label().is_ancestor(can.label()) && auth.label() != can.label() && auth.perm() >= can.perm() ||
            auth.label() == can.label() && (auth.perm() > can.perm() || actor == gactor || actor.is_local_authority())
        );
}

#[derive(Default)]
pub struct Engine {
    claims: Vec<Says>,
}

impl Engine {
    pub fn says(&mut self, actor: Actor, can: Can) -> usize {
        let id = self.claims.len();
        self.claims.push(Says::Can(id, actor, can));
        id
    }

    pub fn says_if(&mut self, actor: Actor, can: Can, cond: Can) -> usize {
        let id = self.claims.len();
        self.claims.push(Says::CanIf(id, actor, can, cond));
        id
    }

    pub fn revokes(&mut self, actor: Actor, id: usize) {
        self.claims.push(Says::Revokes(actor, id));
    }

    pub fn rules(&self) -> impl Iterator<Item = CanRef<'_>> {
        let mut runtime = Crepe::new();
        runtime.extend(self.claims.iter().map(Input));
        let (authorized, revoked) = runtime.run();
        authorized.into_iter().filter_map(move |auth| {
            if !revoked.contains(&Revoked(auth.0)) {
                Some(auth.1)
            } else {
                None
            }
        })
    }

    pub fn can(&self, can: &Can) -> bool {
        for rule in self.rules() {
            if rule.implies(can.as_ref()) {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Label::*;
    use Permission::*;

    fn doc(i: u8) -> Actor {
        Actor::Doc(DocId::new([i; 32]))
    }

    fn peer(i: char) -> Actor {
        Actor::Peer(PeerId::new([i as u8; 32]))
    }

    fn root(i: u8) -> Label {
        Label::Root(DocId::new([i; 32]))
    }

    fn dot(l: Label, s: &str) -> Label {
        Dot(Box::new(l), s.to_string())
    }

    #[test]
    fn test_la_says_can() {
        let mut engine = Engine::default();
        engine.says(doc(9), Can::new(peer('a'), Write, root(9)));
        engine.says(doc(9), Can::new(peer('a'), Read, root(42)));
        for claim in &engine.claims {
            println!("{}", claim);
        }

        assert!(!engine.can(&Can::new(peer('b'), Read, root(9))));

        assert!(engine.can(&Can::new(peer('a'), Write, root(9))));
        assert!(engine.can(&Can::new(peer('a'), Read, root(9))));
        assert!(!engine.can(&Can::new(peer('a'), Own, root(9))));

        assert!(engine.can(&Can::new(peer('a'), Write, dot(root(9), "field"))));
        assert!(!engine.can(&Can::new(peer('a'), Read, root(42))));
    }

    #[test]
    fn test_says_if() {
        let mut engine = Engine::default();
        engine.says_if(
            doc(9),
            Can::new(peer('a'), Write, root(9)),
            Can::new(peer('a'), Read, dot(root(42), "contacts")),
        );
        assert!(!engine.can(&Can::new(peer('a'), Read, root(9))));

        engine.says(doc(42), Can::new(peer('a'), Write, root(42)));
        assert!(engine.can(&Can::new(peer('a'), Read, root(9))));
    }

    #[test]
    fn test_says_if_unbound() {
        let mut engine = Engine::default();
        engine.says_if(
            doc(9),
            Can::new(Actor::Unbound, Write, root(9)),
            Can::new(Actor::Unbound, Read, dot(root(42), "contacts")),
        );
        assert!(!engine.can(&Can::new(peer('a'), Read, root(9))));

        engine.says(doc(42), Can::new(peer('a'), Write, root(42)));
        assert!(engine.can(&Can::new(peer('a'), Read, root(9))));
    }

    #[test]
    fn test_own_and_control() {
        let mut engine = Engine::default();
        engine.says(doc(0), Can::new(peer('a'), Own, root(0)));
        engine.says(peer('a'), Can::new(peer('b'), Control, root(0)));

        engine.says(
            peer('b'),
            Can::new(peer('c'), Own, dot(root(0), "contacts")),
        );
        assert!(!engine.can(&Can::new(peer('c'), Read, dot(root(0), "contacts"))));

        engine.says(
            peer('b'),
            Can::new(peer('c'), Read, dot(root(0), "contacts")),
        );
        assert!(engine.can(&Can::new(peer('c'), Read, dot(root(0), "contacts"))));
    }

    #[test]
    fn test_revoke() {
        let mut engine = Engine::default();
        let id = engine.says(doc(0), Can::new(peer('a'), Own, root(0)));
        assert!(engine.can(&Can::new(peer('a'), Own, root(0))));
        engine.revokes(doc(0), id);
        assert!(!engine.can(&Can::new(peer('a'), Own, root(0))));
    }

    #[test]
    fn test_revoke_trans() {
        let mut engine = Engine::default();
        engine.says(doc(0), Can::new(peer('a'), Own, root(0)));
        let id = engine.says(peer('a'), Can::new(peer('b'), Own, root(0)));
        assert!(engine.can(&Can::new(peer('b'), Own, root(0))));
        engine.revokes(doc(0), id);
        assert!(!engine.can(&Can::new(peer('b'), Own, root(0))));
    }

    #[test]
    fn test_cant_revoke_inv() {
        let mut engine = Engine::default();
        let id = engine.says(doc(0), Can::new(peer('a'), Own, root(0)));
        engine.says(peer('a'), Can::new(peer('b'), Own, root(0)));
        assert!(engine.can(&Can::new(peer('b'), Own, root(0))));
        engine.revokes(peer('b'), id);
        assert!(engine.can(&Can::new(peer('a'), Own, root(0))));
    }
}
