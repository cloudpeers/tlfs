use crate::id::{DocId, PeerId};
use crate::path::{Path, PathBuf};
use crate::Dot;
use bytecheck::CheckBytes;
use crepe::crepe;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    Archive,
    CheckBytes,
    Deserialize,
    Serialize,
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
    pub fn controllable(self) -> bool {
        matches!(self, Self::Sync | Self::Read | Self::Write)
    }
}

#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Archive, Deserialize, Serialize,
)]
#[archive_attr(derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, CheckBytes))]
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

#[derive(
    Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Archive, CheckBytes, Deserialize, Serialize,
)]
#[archive_attr(derive(Debug, Eq, Hash, PartialEq, Ord, PartialOrd, CheckBytes))]
#[repr(C)]
pub struct Can {
    actor: Actor,
    perm: Permission,
    path: PathBuf,
}

impl Can {
    pub fn new(actor: Actor, perm: Permission, path: PathBuf) -> Self {
        Self { actor, perm, path }
    }

    fn as_ref(&self) -> CanRef<'_> {
        CanRef {
            actor: self.actor,
            perm: self.perm,
            path: self.path.as_path(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, Hash, PartialEq, Ord, PartialOrd, CheckBytes))]
pub enum Policy {
    Can(Actor, Permission),
    CanIf(Actor, Permission, Can),
    Revokes(Dot),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CanRef<'a> {
    actor: Actor,
    perm: Permission,
    path: Path<'a>,
}

impl<'a> CanRef<'a> {
    pub fn new(actor: Actor, perm: Permission, path: Path<'a>) -> Self {
        Self { actor, perm, path }
    }

    pub fn actor(self) -> Actor {
        self.actor
    }

    pub fn perm(self) -> Permission {
        self.perm
    }

    pub fn path(self) -> Path<'a> {
        self.path
    }

    fn root(self) -> DocId {
        self.path().root().unwrap()
    }

    fn implies(self, other: CanRef<'a>) -> bool {
        if other.actor != self.actor
            && other.actor != Actor::Unbound
            && self.actor != Actor::Anonymous
        {
            return false;
        }
        other.perm <= self.perm() && self.path().is_ancestor(other.path())
    }

    fn bind(self, rule: CanRef<'a>) -> Self {
        Self {
            actor: rule.actor,
            perm: self.perm,
            path: self.path,
        }
    }
}

impl std::fmt::Display for Can {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:?} can {:?} {}",
            self.actor,
            self.perm,
            self.path.as_path()
        )
    }
}

/*#[derive(Clone, Debug, Eq, Hash, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq, CheckBytes))]
#[repr(C)]
enum Says {
    Can(Dot, Actor, Can),
    CanIf(Dot, Actor, Can, Can),
    Revokes(Actor, Dot),
}

impl std::fmt::Display for Says {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Can(id, actor, can) => write!(f, "{:?}: {:?} says {}", id, actor, can),
            Self::CanIf(id, actor, can, cond) => {
                write!(f, "{:?}: {:?} says {} if {}", id, actor, can, cond)
            }
            Self::Revokes(actor, id) => write!(f, "{:?} revokes {:?}", actor, id),
        }
    }
}

crepe! {
    @input
    struct Input<'a>(&'a Says);

    struct DerivedCanIf<'a>(Dot, Actor, CanRef<'a>, CanRef<'a>);

    struct DerivedCan<'a>(Dot, Actor, CanRef<'a>);

    struct DerivedRevokes<'a>(Actor, Dot, Actor, CanRef<'a>);

    struct MaybeRevoked<'a>(Dot, CanRef<'a>, Actor);

    @output
    struct Authorized<'a>(Dot, CanRef<'a>, Actor);

    @output
    struct Revoked(Dot);

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
    claims: BTreeMap<Dot, Says>,
}

impl Engine {
    pub fn says(&mut self, dot: Dot, can: Can) {
        self.claims.insert(dot, Says::Can(dot, dot.id.into(), can));
    }

    pub fn says_if(&mut self, dot: Dot, can: Can, cond: Can) {
        self.claims
            .insert(dot, Says::CanIf(dot, dot.id.into(), can, cond));
    }

    pub fn revokes(&mut self, dot: Dot, claim: Dot) {
        self.claims.insert(dot, Says::Revokes(dot.id.into(), claim));
    }

    pub fn apply_policy(&mut self, dot: Dot, policy: Policy, label: LabelRef<'_>) {
        match policy {
            Policy::Can(actor, perm) => self.says(dot, Can::new(actor, perm, label.to_label())),
            Policy::CanIf(actor, perm, cond) => {
                self.says_if(dot, Can::new(actor, perm, label.to_label()), cond)
            }
            Policy::Revokes(claim) => self.revokes(dot, Dot::new(claim.id.into(), claim.counter())),
        }
    }

    pub fn rules(&self) -> impl Iterator<Item = CanRef<'_>> {
        let mut runtime = Crepe::new();
        runtime.extend(self.claims.values().map(Input));
        let (authorized, revoked) = runtime.run();
        authorized.into_iter().filter_map(move |auth| {
            if !revoked.contains(&Revoked(auth.0)) {
                Some(auth.1)
            } else {
                None
            }
        })
    }

    pub fn can(&self, peer: PeerId, perm: Permission, label: LabelCow<'_>) -> bool {
        let can = CanRef::new(Actor::Peer(peer), perm, label);
        for rule in self.rules() {
            if rule.implies(can) {
                return true;
            }
        }
        false
    }

    pub fn filter(&self, label: LabelRef<'_>, peer: PeerId, perm: Permission, crdt: &Crdt) -> Crdt {
        let data = if self.can(peer, perm, label.as_ref()) {
            crdt.data.clone()
        } else {
            match &crdt.data {
                Data::Null => Data::Null,
                Data::Flag(_) => Data::Null,
                Data::Reg(_) => Data::Null,
                Data::Table(t) => {
                    let mut delta = ORMap::default();
                    for (k, v) in &***t {
                        let v2 = self.filter(LabelRef::Key(&label, k), peer, perm, v);
                        if v2.data != Data::Null {
                            delta.insert(k.clone(), v2);
                        }
                    }
                    Data::Table(delta)
                }
                Data::Struct(fields) => {
                    let mut delta = BTreeMap::new();
                    for (k, v) in fields {
                        let v2 = self.filter(LabelRef::Field(&label, k), peer, perm, v);
                        if v2.data != Data::Null {
                            delta.insert(k.clone(), v2);
                        }
                    }
                    Data::Struct(delta)
                }
            }
        };
        Crdt {
            data,
            policy: crdt.policy.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Permission::*;

    fn dot(id: impl Into<Id>, c: u64) -> Dot {
        Dot::new(id.into(), c)
    }

    fn doc(i: u8) -> DocId {
        DocId::new([i; 32])
    }

    fn peer(i: char) -> PeerId {
        PeerId::new([i as u8; 32])
    }

    fn root(i: u8) -> Label {
        Label::Root(DocId::new([i; 32]))
    }

    fn field(l: Label, k: &str) -> Label {
        Label::Field(Box::new(l), k.to_string())
    }

    fn can(p: char, perm: Permission, l: Label) -> Can {
        Can::new(Id::Peer(peer(p)).into(), perm, l)
    }

    #[test]
    fn test_la_says_can() {
        let mut engine = Engine::default();
        engine.says(dot(doc(9), 1), can('a', Write, root(9)));
        engine.says(dot(doc(9), 2), can('a', Read, root(42)));

        assert!(!engine.can(peer('b'), Read, root(9).as_ref()));

        assert!(engine.can(peer('a'), Write, root(9).as_ref()));
        assert!(engine.can(peer('a'), Read, root(9).as_ref()));
        assert!(!engine.can(peer('a'), Own, root(9).as_ref()));

        assert!(engine.can(peer('a'), Write, field(root(9), "contacts").as_ref()));
        assert!(!engine.can(peer('a'), Read, root(42).as_ref()));
    }

    #[test]
    fn test_says_if() {
        let mut engine = Engine::default();
        engine.says_if(
            dot(doc(9), 1),
            can('a', Write, root(9)),
            can('a', Read, field(root(42), "contacts")),
        );
        assert!(!engine.can(peer('a'), Read, root(9).as_ref()));

        engine.says(dot(doc(42), 1), can('a', Write, root(42)));
        assert!(engine.can(peer('a'), Read, root(9).as_ref()));
    }

    #[test]
    fn test_says_if_unbound() {
        let mut engine = Engine::default();
        engine.says_if(
            dot(doc(9), 1),
            Can::new(Actor::Unbound, Write, root(9)),
            Can::new(Actor::Unbound, Read, field(root(42), "contacts")),
        );
        assert!(!engine.can(peer('a'), Read, root(9).as_ref()));

        engine.says(dot(doc(42), 1), can('a', Write, root(42)));
        assert!(engine.can(peer('a'), Read, root(9).as_ref()));
    }

    #[test]
    fn test_own_and_control() {
        let mut engine = Engine::default();
        engine.says(dot(doc(0), 1), can('a', Own, root(0)));
        engine.says(dot(peer('a'), 1), can('b', Control, root(0)));

        engine.says(dot(peer('b'), 1), can('c', Own, field(root(0), "contacts")));
        assert!(!engine.can(peer('c'), Read, field(root(0), "contacts").as_ref()));

        engine.says(
            dot(peer('b'), 3),
            can('c', Read, field(root(0), "contacts")),
        );
        assert!(engine.can(peer('c'), Read, field(root(0), "contacts").as_ref()));
    }

    #[test]
    fn test_revoke() {
        let mut engine = Engine::default();
        engine.says(dot(doc(0), 1), can('a', Own, root(0)));
        assert!(engine.can(peer('a'), Own, root(0).as_ref()));
        engine.revokes(dot(doc(0), 2), dot(doc(0), 1));
        assert!(!engine.can(peer('a'), Own, root(0).as_ref()));
    }

    #[test]
    fn test_revoke_trans() {
        let mut engine = Engine::default();
        engine.says(dot(doc(0), 1), can('a', Own, root(0)));
        engine.says(dot(peer('a'), 1), can('b', Own, root(0)));
        assert!(engine.can(peer('b'), Own, root(0).as_ref()));
        engine.revokes(dot(doc(0), 2), dot(peer('a'), 1));
        assert!(!engine.can(peer('b'), Own, root(0).as_ref()));
    }

    #[test]
    fn test_cant_revoke_inv() {
        let mut engine = Engine::default();
        engine.says(dot(doc(0), 1), can('a', Own, root(0)));
        engine.says(dot(peer('a'), 1), can('b', Own, root(0)));
        assert!(engine.can(peer('b'), Own, root(0).as_ref()));
        engine.revokes(dot(peer('b'), 1), dot(peer('a'), 1));
        assert!(engine.can(peer('a'), Own, root(0).as_ref()));
    }

    #[test]
    fn test_anonymous_can() {
        let mut engine = Engine::default();
        assert!(!engine.can(peer('a'), Read, root(9).as_ref()));
        engine.says(dot(doc(9), 1), Can::new(Actor::Anonymous, Read, root(9)));
        assert!(engine.can(peer('a'), Read, root(9).as_ref()));
    }
}*/
