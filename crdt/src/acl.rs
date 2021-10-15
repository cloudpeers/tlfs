use crate::{DocId, Path, PathBuf, PeerId, Ref};
use anyhow::Result;
use bytecheck::CheckBytes;
use crepe::crepe;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeSet;

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
    Revokes([u8; 32]),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CanRef<'a> {
    actor: Actor,
    perm: Permission,
    path: Path<'a>,
}

impl<'a> CanRef<'a> {
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
        self.path.child().unwrap().first().unwrap().doc().unwrap()
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
    Can([u8; 32], PeerId, Can),
    CanIf([u8; 32], PeerId, Can, Can),
    Revokes(PeerId, [u8; 32]),
}

impl std::fmt::Display for Says {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Can(id, peer, can) => write!(f, "{}: {} says {}", hex::encode(&id), peer, can),
            Self::CanIf(id, peer, can, cond) => {
                write!(f, "{}: {} says {} if {}", hex::encode(&id), peer, can, cond)
            }
            Self::Revokes(peer, id) => write!(f, "{} revokes {}", peer, hex::encode(&id)),
        }
    }
}

crepe! {
    @input
    struct Input<'a>(&'a Says);

    struct DerivedCan<'a>([u8; 32], PeerId, CanRef<'a>);

    struct DerivedCanIf<'a>([u8; 32], PeerId, CanRef<'a>, CanRef<'a>);

    struct DerivedRevokes<'a>(PeerId, [u8; 32], PeerId, CanRef<'a>);

    struct MaybeRevoked<'a>([u8; 32], PeerId, CanRef<'a>);

    @output
    struct Authorized<'a>([u8; 32], PeerId, CanRef<'a>);

    @output
    struct Revoked([u8; 32]);

    DerivedCan(*id, *peer, can.as_ref()) <-
        Input(s),
        let Says::Can(id, peer, can) = s;

    DerivedCanIf(*id, *peer, can.as_ref(), cond.as_ref()) <-
        Input(s),
        let Says::CanIf(id, peer, can, cond) = s;

    DerivedRevokes(*peer, *id, peer2, can) <-
        Input(s),
        let Says::Revokes(peer, id) = s,
        Authorized(*id, peer2, can);

    // resolve conditional
    DerivedCan(id, peer, can.bind(auth)) <-
        DerivedCanIf(id, peer, can, cond),
        Authorized(_, _, auth),
        (auth.implies(cond));

    // local authority
    Authorized(id, peer, can) <-
        DerivedCan(id, peer, can),
        (Actor::Peer(peer).is_local_authority(can.root()));

    // ownership
    Authorized(id, peer, can) <-
        DerivedCan(id, peer, can),
        Authorized(_, _, auth),
        (Actor::Peer(peer) == auth.actor()),
        (Permission::Own == auth.perm()),
        (auth.path().is_ancestor(can.path()));

    // control
    Authorized(id, peer, can) <-
        DerivedCan(id, peer, can),
        Authorized(_, _, auth),
        (Actor::Peer(peer) == auth.actor()),
        (auth.perm() == Permission::Control && can.perm().controllable()),
        (auth.path().is_ancestor(can.path()));

    // higher privileges can revoke
    Revoked(id) <-
        DerivedRevokes(peer, id, peer2, can),
        Authorized(_, _, auth),
        (
            Actor::Peer(peer) == auth.actor() && auth.perm() >= Permission::Control ||
            Actor::Peer(peer).is_local_authority(can.root())
        ),
        (
            auth.path().is_ancestor(can.path()) && auth.path() != can.path() && auth.perm() >= can.perm() ||
            auth.path() == can.path() && (
                auth.perm() > can.perm() ||
                peer == peer2 ||
                Actor::Peer(peer).is_local_authority(can.root())
            )
        );
}

#[derive(Archive, Serialize)]
#[archive(as = "Rule")]
#[repr(C)]
struct Rule {
    id: [u8; 32],
    perm: Permission,
}

impl Rule {
    fn new(id: [u8; 32], perm: Permission) -> Self {
        Self { id, perm }
    }
}

#[derive(Clone)]
pub struct Acl(sled::Tree);

impl Acl {
    pub fn new(tree: sled::Tree) -> Self {
        Self(tree)
    }

    pub fn memory(name: &str) -> Result<Self> {
        let db = sled::Config::new().temporary(true).open()?;
        Ok(Self(db.open_tree(name)?))
    }

    fn add_rule(&self, id: [u8; 32], actor: Actor, perm: Permission, path: Path) -> Result<()> {
        let peer = match actor {
            Actor::Peer(peer) => peer,
            _ => PeerId::new([0; 32]),
        };
        let mut key = peer.as_ref().to_vec();
        key.extend(path.as_ref());
        self.0
            .insert(key, Ref::archive(&Rule::new(id, perm)).as_bytes())?;
        Ok(())
    }

    fn revoke_rules(&self, revoked: BTreeSet<[u8; 32]>) -> Result<()> {
        for r in self.0.iter() {
            let (k, v) = r?;
            if revoked.contains(&Ref::<Rule>::new(v).as_ref().id) {
                self.0.remove(k)?;
            }
        }
        Ok(())
    }

    fn implies(&self, prefix: &[u8], perm: Permission, path: Path) -> Result<bool> {
        for r in self.0.scan_prefix(prefix) {
            let (k, v) = r?;
            if Path::new(&k[32..]).is_ancestor(path) && Ref::<Rule>::new(v).as_ref().perm >= perm {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn can(&self, peer: PeerId, perm: Permission, path: Path) -> Result<bool> {
        if peer == path.child().unwrap().first().unwrap().doc().unwrap().into() {
            return Ok(true);
        }
        let mut prefix = peer.as_ref().to_vec();
        prefix.extend(&path.as_ref()[..38]);
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

pub struct Engine {
    policy: BTreeSet<Says>,
    acl: Acl,
}

impl Engine {
    pub fn new(acl: Acl) -> Result<Self> {
        Ok(Self {
            policy: Default::default(),
            acl,
        })
    }

    fn add_policy(&mut self, path: Path) {
        self._add_policy(path);
    }

    fn _add_policy(&mut self, path: Path) -> Option<()> {
        // schema.doc.(primitive|str)*.peer.policy
        let doc = path.child()?.first()?.doc()?;
        let peer = path.parent()?.last()?.peer()?;
        let policy = path
            .last()?
            .policy()?
            .deserialize(&mut rkyv::Infallible)
            .unwrap();
        let hash = blake3::hash(path.as_ref()).into();
        let says = match policy {
            Policy::Can(actor, perm) => {
                Says::Can(hash, peer, Can::new(actor, perm, path.to_owned()))
            }
            Policy::CanIf(actor, perm, cond) => {
                Says::CanIf(hash, peer, Can::new(actor, perm, path.to_owned()), cond)
            }
            Policy::Revokes(hash) => Says::Revokes(peer, hash),
        };
        self.policy.insert(says);
        None
    }

    pub fn update_acl(&self) -> Result<()> {
        let mut runtime = Crepe::new();
        runtime.extend(self.policy.iter().map(Input));
        let (authorized, revoked) = runtime.run();
        let revoked: BTreeSet<[u8; 32]> = revoked.into_iter().map(|r| r.0).collect();
        for Authorized(id, _, CanRef { actor, perm, path }) in authorized.into_iter() {
            if !revoked.contains(&id) {
                self.acl.add_rule(id, actor, perm, path)?;
            }
        }
        self.acl.revoke_rules(revoked)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Backend, EMPTY_HASH};
    use std::pin::Pin;
    use Permission::*;

    fn peer(i: char) -> PeerId {
        PeerId::new([i as u8; 32])
    }

    #[async_std::test]
    async fn test_la_says_can() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let doc = sdk.frontend().create_doc(peer('a'), &EMPTY_HASH.into())?;
        Pin::new(&mut sdk).await?;

        assert!(!doc.cursor().can(&peer('b'), Read)?);

        assert!(doc.cursor().can(&peer('a'), Write)?);
        assert!(doc.cursor().can(&peer('a'), Read)?);
        assert!(doc.cursor().can(&peer('a'), Own)?);

        //assert!(doc.cursor().field("contacts")?.can(&peer('a'), Write)?);
        Ok(())
    }

    #[async_std::test]
    async fn test_says_if() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let doc1 = sdk.frontend().create_doc(peer('a'), &EMPTY_HASH.into())?;
        let doc2 = sdk.frontend().create_doc(peer('a'), &EMPTY_HASH.into())?;
        Pin::new(&mut sdk).await?;

        let cond = doc1.cursor().cond(Actor::Peer(peer('b')), Read);
        let op = doc2
            .cursor()
            .say_can_if(Actor::Peer(peer('b')), Write, cond)?;
        sdk.join(&peer('a'), op)?;
        assert!(!doc2.cursor().can(&peer('b'), Read)?);

        let op = doc1.cursor().say_can(Some(peer('b')), Write)?;
        sdk.join(&peer('a'), op)?;
        assert!(doc2.cursor().can(&peer('b'), Read)?);

        Ok(())
    }

    #[async_std::test]
    async fn test_says_if_unbound() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let doc1 = sdk.frontend().create_doc(peer('a'), &EMPTY_HASH.into())?;
        let doc2 = sdk.frontend().create_doc(peer('a'), &EMPTY_HASH.into())?;
        Pin::new(&mut sdk).await?;

        let cond = doc1.cursor().cond(Actor::Unbound, Read);
        let op = doc2.cursor().say_can_if(Actor::Unbound, Write, cond)?;
        sdk.join(&peer('a'), op)?;
        assert!(!doc2.cursor().can(&peer('b'), Read)?);

        let op = doc1.cursor().say_can(Some(peer('b')), Write)?;
        sdk.join(&peer('a'), op)?;
        assert!(doc2.cursor().can(&peer('b'), Read)?);

        Ok(())
    }

    #[async_std::test]
    async fn test_own_and_control() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let doc = sdk.frontend().create_doc(peer('a'), &EMPTY_HASH.into())?;
        Pin::new(&mut sdk).await?;

        let op = doc.cursor().say_can(Some(peer('b')), Control)?;
        sdk.join(&peer('a'), op)?;
        assert!(doc.cursor().can(&peer('b'), Control)?);

        //let op = doc.cursor().say_can(peer('c'), Control)?;
        //assert!(!doc.cursor().can(&peer('c'), Read)?);
        //let op = doc.cursor().say_can(peer('c'), Read)?;
        //assert!(doc.cursor().can(&peer('c'), Read));

        Ok(())
    }

    #[async_std::test]
    async fn test_anonymous_can() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let doc = sdk.frontend().create_doc(peer('a'), &EMPTY_HASH.into())?;
        Pin::new(&mut sdk).await?;

        let op = doc.cursor().say_can(None, Read)?;
        sdk.join(&peer('a'), op)?;
        assert!(doc.cursor().can(&peer('b'), Read)?);
        Ok(())
    }

    #[async_std::test]
    async fn test_revoke() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let doc = sdk.frontend().create_doc(peer('a'), &EMPTY_HASH.into())?;
        Pin::new(&mut sdk).await?;

        let op = doc.cursor().say_can(Some(peer('b')), Write)?;
        sdk.join(&peer('a'), op)?;
        assert!(doc.cursor().can(&peer('b'), Write)?);

        let op = doc.cursor().revoke(Dot::new(peer('a'), 1))?;
        sdk.join(&peer('a'), op)?;
        assert!(!doc.cursor().can(&peer('b'), Write)?);

        Ok(())
    }

    #[async_std::test]
    #[ignore]
    async fn test_revoke_trans() -> Result<()> {
        /*let (crdt, mut engine) = Crdt::memory()?;

        let op = crdt.say(root(9).as_path(), &doc(9).into(), can('a', Own))?;
        crdt.join(&doc(9).into(), &op)?;

        let op = crdt.say(root(9).as_path(), &peer('a').into(), can('b', Own))?;
        crdt.join(&doc(9).into(), &op)?;

        assert!(crdt.can(&peer('b'), Own, root(9).as_path())?);

        let op = crdt.say(
            root(9).as_path(),
            &doc(9).into(),
            Policy::Revokes(dot(peer('a'), 1)),
        )?;
        crdt.join(&doc(9).into(), &op)?;

        assert!(!crdt.can(&peer('b'), Own, root(9).as_path())?);*/

        Ok(())
    }

    #[async_std::test]
    #[ignore]
    async fn test_cant_revoke_inv() -> Result<()> {
        /*let (crdt, mut engine) = Crdt::memory()?;

        let op = crdt.say(root(9).as_path(), &doc(9).into(), can('a', Own))?;
        crdt.join(&doc(9).into(), &op)?;

        let op = crdt.say(root(9).as_path(), &peer('a'), can('b', Own))?;
        crdt.join(&doc(9).into(), &op)?;

        assert!(crdt.can(&peer('b'), Own, root(9).as_path())?);

        let op = crdt.say(
            root(9).as_path(),
            &peer('b'),
            Policy::Revokes(dot(peer('a'), 1)),
        )?;
        crdt.join(&doc(9).into(), &op)?;

        assert!(crdt.can(&peer('a'), Own, root(9).as_path())?);*/

        Ok(())
    }
}
