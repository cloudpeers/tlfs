use crate::data::{Label, LabelCow};
use crate::id::{DocId, Id, PeerId};
use bytecheck::CheckBytes;
use crepe::crepe;
use rkyv::{Archive, Deserialize, Serialize};

pub type Dot = tlfs_crdt::Dot<Id>;

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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Clone, Copy, Debug, Eq, Hash, PartialEq, CheckBytes))]
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

impl From<Id> for Actor {
    fn from(id: Id) -> Self {
        match id {
            Id::Doc(doc) => Actor::Doc(doc),
            Id::Peer(peer) => Actor::Peer(peer),
        }
    }
}

impl From<Option<PeerId>> for Actor {
    fn from(actor: Option<PeerId>) -> Self {
        match actor {
            Some(peer) => Actor::Peer(peer),
            None => Actor::Anonymous,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Archive, CheckBytes, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq, CheckBytes))]
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
            label: LabelCow::Label(&self.label),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq, CheckBytes))]
pub enum Policy {
    Can(Actor, Permission),
    CanIf(Actor, Permission, Can),
    Revokes(crate::data::Dot),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CanRef<'a> {
    actor: Actor,
    perm: Permission,
    label: LabelCow<'a>,
}

impl<'a> CanRef<'a> {
    pub fn new(actor: Actor, perm: Permission, label: LabelCow<'a>) -> Self {
        Self { actor, perm, label }
    }

    pub fn actor(self) -> Actor {
        self.actor
    }

    pub fn perm(self) -> Permission {
        self.perm
    }

    pub fn label(self) -> LabelCow<'a> {
        self.label
    }

    fn root(self) -> DocId {
        self.label().root()
    }

    fn implies(self, other: CanRef<'a>) -> bool {
        if other.actor != self.actor
            && other.actor != Actor::Unbound
            && self.actor != Actor::Anonymous
        {
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
    claims: Vec<Says>,
}

impl Engine {
    pub fn says(&mut self, dot: Dot, can: Can) {
        self.claims.push(Says::Can(dot, dot.id.into(), can));
    }

    pub fn says_if(&mut self, dot: Dot, can: Can, cond: Can) {
        self.claims.push(Says::CanIf(dot, dot.id.into(), can, cond));
    }

    pub fn revokes(&mut self, dot: Dot, claim: Dot) {
        self.claims.push(Says::Revokes(dot.id.into(), claim));
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

    pub fn can(&self, can: CanRef) -> bool {
        for rule in self.rules() {
            if rule.implies(can) {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Permission::*;

    fn doc(i: u8) -> Id {
        Id::Doc(DocId::new([i; 32]))
    }

    fn peer(i: char) -> Id {
        Id::Peer(PeerId::new([i as u8; 32]))
    }

    fn root(i: u8) -> Label {
        Label::Root(DocId::new([i; 32]))
    }

    fn field(l: Label, k: &str) -> Label {
        Label::Field(Box::new(l), k.to_string())
    }

    fn can(p: char, perm: Permission, l: Label) -> Can {
        Can::new(peer(p).into(), perm, l)
    }

    #[test]
    fn test_la_says_can() {
        let mut engine = Engine::default();
        engine.says(Dot::new(doc(9), 1), can('a', Write, root(9)));
        engine.says(Dot::new(doc(9), 2), can('a', Read, root(42)));

        assert!(!engine.can(can('b', Read, root(9)).as_ref()));

        assert!(engine.can(can('a', Write, root(9)).as_ref()));
        assert!(engine.can(can('a', Read, root(9)).as_ref()));
        assert!(!engine.can(can('a', Own, root(9)).as_ref()));

        assert!(engine.can(can('a', Write, field(root(9), "contacts")).as_ref()));
        assert!(!engine.can(can('a', Read, root(42)).as_ref()));
    }

    #[test]
    fn test_says_if() {
        let mut engine = Engine::default();
        engine.says_if(
            Dot::new(doc(9), 1),
            can('a', Write, root(9)),
            can('a', Read, field(root(42), "contacts")),
        );
        assert!(!engine.can(can('a', Read, root(9)).as_ref()));

        engine.says(Dot::new(doc(42), 1), can('a', Write, root(42)));
        assert!(engine.can(can('a', Read, root(9)).as_ref()));
    }

    #[test]
    fn test_says_if_unbound() {
        let mut engine = Engine::default();
        engine.says_if(
            Dot::new(doc(9), 1),
            Can::new(Actor::Unbound, Write, root(9)),
            Can::new(Actor::Unbound, Read, field(root(42), "contacts")),
        );
        assert!(!engine.can(can('a', Read, root(9)).as_ref()));

        engine.says(Dot::new(doc(42), 1), can('a', Write, root(42)));
        assert!(engine.can(can('a', Read, root(9)).as_ref()));
    }

    #[test]
    fn test_own_and_control() {
        let mut engine = Engine::default();
        engine.says(Dot::new(doc(0), 1), can('a', Own, root(0)));
        engine.says(Dot::new(peer('a'), 1), can('b', Control, root(0)));

        engine.says(
            Dot::new(peer('b'), 1),
            can('c', Own, field(root(0), "contacts")),
        );
        assert!(!engine.can(can('c', Read, field(root(0), "contacts")).as_ref()));

        engine.says(
            Dot::new(peer('b'), 3),
            can('c', Read, field(root(0), "contacts")),
        );
        assert!(engine.can(can('c', Read, field(root(0), "contacts")).as_ref()));
    }

    #[test]
    fn test_revoke() {
        let mut engine = Engine::default();
        engine.says(Dot::new(doc(0), 1), can('a', Own, root(0)));
        assert!(engine.can(can('a', Own, root(0)).as_ref()));
        engine.revokes(Dot::new(doc(0), 2), Dot::new(doc(0), 1));
        assert!(!engine.can(can('a', Own, root(0)).as_ref()));
    }

    #[test]
    fn test_revoke_trans() {
        let mut engine = Engine::default();
        engine.says(Dot::new(doc(0), 1), can('a', Own, root(0)));
        engine.says(Dot::new(peer('a'), 1), can('b', Own, root(0)));
        assert!(engine.can(can('b', Own, root(0)).as_ref()));
        engine.revokes(Dot::new(doc(0), 2), Dot::new(peer('a'), 1));
        assert!(!engine.can(can('b', Own, root(0)).as_ref()));
    }

    #[test]
    fn test_cant_revoke_inv() {
        let mut engine = Engine::default();
        engine.says(Dot::new(doc(0), 1), can('a', Own, root(0)));
        engine.says(Dot::new(peer('a'), 1), can('b', Own, root(0)));
        assert!(engine.can(can('b', Own, root(0)).as_ref()));
        engine.revokes(Dot::new(peer('b'), 1), Dot::new(peer('a'), 1));
        assert!(engine.can(can('a', Own, root(0)).as_ref()));
    }

    #[test]
    fn test_anonymous_can() {
        let mut engine = Engine::default();
        assert!(!engine.can(can('a', Read, root(9)).as_ref()));
        engine.says(
            Dot::new(doc(9), 1),
            Can::new(Actor::Anonymous, Read, root(9)),
        );
        assert!(engine.can(can('a', Read, root(9)).as_ref()));
    }
}
