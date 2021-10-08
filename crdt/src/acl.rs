use crate::{Crdt, DocId, Dot, DotStoreType, Path, PathBuf, PeerId, Ref};
use anyhow::Result;
use bytecheck::CheckBytes;
use crepe::crepe;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

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
    Peer(PeerId),
    Anonymous,
    Unbound,
}

impl Actor {
    fn is_local_authority(&self, doc: DocId) -> bool {
        if let Actor::Peer(id) = self {
            id.as_ref() == doc.as_ref()
        } else {
            false
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq, CheckBytes))]
#[repr(C)]
enum Says {
    Can(Dot, Can),
    CanIf(Dot, Can, Can),
    Revokes(Dot, Dot),
}

impl std::fmt::Display for Says {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Can(dot, can) => write!(f, "{}: {} says {}", dot.counter(), dot.id, can),
            Self::CanIf(dot, can, cond) => {
                write!(f, "{}: {} says {} if {}", dot.counter(), dot.id, can, cond)
            }
            Self::Revokes(dot, rdot) => write!(f, "{}: {} revokes {}", dot.counter(), dot.id, rdot),
        }
    }
}

crepe! {
    @input
    struct Input<'a>(&'a Says);

    struct DerivedCanIf<'a>(Dot, CanRef<'a>, CanRef<'a>);

    struct DerivedCan<'a>(Dot, CanRef<'a>);

    struct DerivedRevokes<'a>(Dot, Dot, CanRef<'a>);

    struct MaybeRevoked<'a>(Dot, CanRef<'a>);

    @output
    struct Authorized<'a>(Dot, CanRef<'a>);

    @output
    struct Revoked(Dot);

    DerivedCan(*dot, can.as_ref()) <-
        Input(s),
        let Says::Can(dot, can) = s;

    DerivedCanIf(*dot, can.as_ref(), cond.as_ref()) <-
        Input(s),
        let Says::CanIf(dot, can, cond) = s;

    DerivedRevokes(*dot, *rdot, can) <-
        Input(s),
        let Says::Revokes(dot, rdot) = s,
        Authorized(*rdot, can);

    // resolve conditional
    DerivedCan(dot, can.bind(auth)) <-
        DerivedCanIf(dot, can, cond),
        Authorized(_, auth),
        (auth.implies(cond));

    // local authority
    Authorized(dot, can) <-
        DerivedCan(dot, can),
        (Actor::Peer(dot.id).is_local_authority(can.root()));

    // ownership
    Authorized(dot, can) <-
        DerivedCan(dot, can),
        Authorized(_, auth),
        (Actor::Peer(dot.id) == auth.actor()),
        (Permission::Own == auth.perm()),
        (auth.path().is_ancestor(can.path()));

    // control
    Authorized(dot, can) <-
        DerivedCan(dot, can),
        Authorized(_, auth),
        (Actor::Peer(dot.id) == auth.actor()),
        (auth.perm() == Permission::Control && can.perm().controllable()),
        (auth.path().is_ancestor(can.path()));

    // higher privileges can revoke
    Revoked(rdot) <-
        DerivedRevokes(dot, rdot, can),
        Authorized(_, auth),
        (
            Actor::Peer(dot.id) == auth.actor() && auth.perm() >= Permission::Control ||
            Actor::Peer(dot.id).is_local_authority(can.root())
        ),
        (
            auth.path().is_ancestor(can.path()) && auth.path() != can.path() && auth.perm() >= can.perm() ||
            auth.path() == can.path() && (
                auth.perm() > can.perm() ||
                dot.id == rdot.id ||
                Actor::Peer(dot.id).is_local_authority(can.root())
            )
        );
}

pub struct Engine {
    policy: BTreeSet<Says>,
    subscriber: sled::Subscriber,
    acl: Acl,
}

impl Engine {
    pub fn new(crdt: Crdt, acl: Acl) -> Result<Self> {
        let subscriber = crdt.watch_path(Path::new(&[]));
        let mut me = Self {
            policy: Default::default(),
            subscriber,
            acl,
        };
        for r in crdt.iter() {
            let (k, v) = r?;
            me.add_kv(&k, v);
        }
        Ok(me)
    }

    pub fn poll(&mut self, cx: &mut Context) -> Result<()> {
        while let Poll::Ready(Some(ev)) = Pin::new(&mut self.subscriber).poll(cx) {
            for (_, k, v) in ev.iter() {
                if let Some(v) = v {
                    // TODO: don't clone
                    self.add_kv(k, v.clone());
                }
            }
        }
        self.update_rules()
    }

    fn add_kv(&mut self, key: &sled::IVec, value: sled::IVec) {
        let path = Path::new(&key[..]);
        if path.ty() != Some(DotStoreType::Policy) {
            return;
        }
        let dot = path.dot();
        let path = path.parent().unwrap();
        let policies = Ref::<BTreeSet<Policy>>::new(value).to_owned().unwrap();
        for policy in policies {
            let says = match policy {
                Policy::Can(actor, perm) => Says::Can(dot, Can::new(actor, perm, path.to_owned())),
                Policy::CanIf(actor, perm, cond) => {
                    Says::CanIf(dot, Can::new(actor, perm, path.to_owned()), cond)
                }
                Policy::Revokes(claim) => Says::Revokes(dot, Dot::new(claim.id, claim.counter())),
            };
            self.policy.insert(says);
        }
    }

    fn update_rules(&self) -> Result<()> {
        let mut runtime = Crepe::new();
        runtime.extend(self.policy.iter().map(Input));
        let (authorized, revoked) = runtime.run();
        for Authorized(id, CanRef { actor, perm, path }) in authorized.into_iter() {
            self.acl.add_rule(id, actor, perm, path)?;
        }
        for Revoked(id) in revoked {
            self.acl.revoke_rule(id)?;
        }
        Ok(())
    }

    /*pub fn filter(
        &self,
        path: &mut PathBuf,
        peer: PeerId,
        perm: Permission,
        store: &DotStore,
    ) -> DotStore {
        if self.can(peer, perm, path.as_path()) {
            store.clone()
        } else {
            match store {
                DotStore::Null => DotStore::Null,
                DotStore::DotSet(_) => DotStore::Null,
                DotStore::DotFun(_) => DotStore::Null,
                DotStore::DotMap(map) => {
                    let mut delta = BTreeMap::new();
                    for (key, store) in map {
                        path.key(key);
                        let store2 = self.filter(path, peer, perm, store);
                        path.pop();
                        if store2 != DotStore::Null {
                            delta.insert(key.clone(), store2);
                        }
                    }
                    DotStore::DotMap(delta)
                }
                DotStore::Struct(fields) => {
                    let mut delta = BTreeMap::new();
                    for (k, v) in fields {
                        path.field(k);
                        let v2 = self.filter(path, peer, perm, v);
                        path.pop();
                        if v2 != DotStore::Null {
                            delta.insert(k.clone(), v2);
                        }
                    }
                    DotStore::Struct(delta)
                }
                DotStore::Policy(policy) => DotStore::Policy(policy.clone()),
            }
        }
    }*/
}

#[derive(Clone)]
pub struct Acl(sled::Tree);

impl Acl {
    pub fn new(tree: sled::Tree) -> Self {
        Self(tree)
    }

    fn add_rule(&self, id: Dot, actor: Actor, perm: Permission, path: Path) -> Result<()> {
        let peer = match actor {
            Actor::Peer(peer) => peer,
            _ => PeerId::new([0; 32]),
        };
        let mut key = peer.as_ref().to_vec();
        key.extend(path.as_ref());
        self.0.insert(key, &[perm as u8])?;
        Ok(())
    }

    fn revoke_rule(&self, id: Dot) -> Result<()> {
        todo!()
    }

    fn implies(&self, prefix: &[u8], perm: Permission, path: Path) -> Result<bool> {
        for r in self.0.scan_prefix(prefix) {
            let (k,v ) = r?;
            if Path::new(&k[32..]).is_ancestor(path) && v[0] <= perm as u8 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn can(&self, peer: PeerId, perm: Permission, path: Path) -> Result<bool> {
        let mut prefix = peer.as_ref().to_vec();
        prefix.extend(path.root().unwrap().as_ref());
        if self.implies(&prefix, perm, path)? {
            return Ok(true);
        }
        prefix[..32].copy_from_slice(&[0; 32]);
        if self.implies(&prefix, perm, path)? {
            return Ok(true);
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Permission::*;

    fn dot(peer: impl AsRef<[u8; 32]>, c: u64) -> Dot {
        Dot::new(PeerId::new(*peer.as_ref()), c)
    }

    fn doc(i: u8) -> DocId {
        DocId::new([i; 32])
    }

    fn peer(i: char) -> PeerId {
        PeerId::new([i as u8; 32])
    }

    fn root(i: u8) -> PathBuf {
        PathBuf::new(doc(i))
    }

    fn field(mut path: PathBuf, k: &str) -> PathBuf {
        path.field(k);
        path
    }

    fn can(p: char, perm: Permission, path: PathBuf) -> Can {
        Can::new(Actor::Peer(peer(p)), perm, path)
    }

    /*#[test]
    fn test_la_says_can() -> Result<()> {
        let crdt = Crdt::memory("test")?;
        let mut engine = Engine::new(crdt.clone());
        crdt.say(dot(doc(9), 1), can('a', Write, root(9)));
        crdt.say(dot(doc(9), 2), can('a', Read, root(42)));

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
    }*/
}
