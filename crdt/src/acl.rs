use crate::dotset::Dot;
use crate::id::{DocId, PeerId};
use crate::path::{Path, PathBuf};
use crate::radixdb::{BlobMap, Diff, Storage};
use crate::util::Ref;
use anyhow::Result;
use bytecheck::CheckBytes;
use crepe::crepe;
use futures::stream::BoxStream;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Permission type.
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
    /// Permission can be delegated with `Control` permissions.
    pub fn controllable(self) -> bool {
        matches!(self, Self::Read | Self::Write)
    }
}

/// Actor
#[derive(Clone, Copy, Eq, Hash, PartialEq, Ord, PartialOrd, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, CheckBytes))]
#[repr(C)]
pub enum Actor {
    /// A keypair identified by it's public key.
    Peer(PeerId),
    /// Any public key.
    Anonymous,
    /// Unbound public key which will be bound by the condition
    /// in a conditional statement.
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

impl std::fmt::Debug for Actor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Peer(p) => write!(f, "{:?}", p),
            Self::Anonymous => write!(f, "Anonymous"),
            Self::Unbound => write!(f, "Unbound"),
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

/// Policy statement.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, Hash, PartialEq, Ord, PartialOrd, CheckBytes))]
pub enum Policy {
    /// Unconditional statement; An actor has permission.
    Can(Actor, Permission),
    /// Conditional statement; An actor has permission if the condition is met.
    CanIf(Actor, Permission, Can),
    /// Revocation statement.
    Revokes(Dot),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CanRef<'a> {
    actor: Actor,
    perm: Permission,
    path: Path<'a>,
}

impl<'a> CanRef<'a> {
    fn actor(self) -> Actor {
        self.actor
    }

    fn perm(self) -> Permission {
        self.perm
    }

    fn path(self) -> Path<'a> {
        self.path
    }

    fn root(self) -> DocId {
        self.path.first().unwrap().doc().unwrap()
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
    Can(Dot, PeerId, Can),
    CanIf(Dot, PeerId, Can, Can),
    Revokes(PeerId, Dot),
}

impl std::fmt::Display for Says {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Can(id, peer, can) => write!(f, "{}: {} says {}", id, peer, can),
            Self::CanIf(id, peer, can, cond) => {
                write!(f, "{}: {} says {} if {}", id, peer, can, cond)
            }
            Self::Revokes(peer, id) => write!(f, "{} revokes {}", peer, id),
        }
    }
}

crepe! {
    @input
    struct Input<'a>(&'a Says);

    struct DerivedCan<'a>(Dot, PeerId, CanRef<'a>);

    struct DerivedCanIf<'a>(Dot, PeerId, CanRef<'a>, CanRef<'a>);

    struct DerivedRevokes<'a>(PeerId, Dot, PeerId, CanRef<'a>);

    struct MaybeRevoked<'a>(Dot, PeerId, CanRef<'a>);

    @output
    struct Authorized<'a>(Dot, PeerId, CanRef<'a>);

    @output
    struct Revoked(Dot);

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

#[derive(Debug, Archive, Serialize)]
#[archive(as = "Rule")]
#[repr(C)]
pub(crate) struct Rule {
    pub id: Dot,
    pub perm: Permission,
}

impl Rule {
    fn new(id: Dot, perm: Permission) -> Self {
        Self { id, perm }
    }
}

#[derive(Clone)]
pub struct Acl(BlobMap);

impl Acl {
    pub fn new(tree: BlobMap) -> Self {
        Self(tree)
    }

    pub fn load(storage: Arc<dyn Storage>, name: &str) -> Result<Self> {
        Ok(Self(BlobMap::load(storage, name)?))
    }

    pub fn subscribe(&self, doc: &DocId) -> BoxStream<'static, Diff<u8, Arc<[u8]>>> {
        let mut path = PathBuf::new();
        path.doc(doc);
        self.0.watch_prefix(path)
    }

    fn add_rule(&self, id: Dot, actor: Actor, perm: Permission, path: Path) -> Result<()> {
        let peer = match actor {
            Actor::Peer(peer) => peer,
            _ => PeerId::new([0; 32]),
        };
        let mut prefix = PathBuf::new();
        prefix.doc(&path.first().unwrap().doc().unwrap());
        prefix.peer(&peer);
        prefix.extend(path.child().unwrap());
        self.0
            .insert_archived(prefix.as_path(), &Rule::new(id, perm));
        self.0.flush()
    }

    fn revoke_rules(&self, revoked: BTreeSet<Dot>) -> Result<()> {
        for (k, v) in self.0.iter() {
            if revoked.contains(&Ref::<Rule>::new(v.clone()).as_ref().id) {
                self.0.remove(k);
            }
        }
        self.0.flush()
    }

    fn implies(&self, peer: &PeerId, doc: &DocId, perm: Permission, path: Path) -> Result<bool> {
        let mut prefix = PathBuf::new();
        prefix.doc(doc);
        prefix.peer(peer);
        for (k, v) in self.0.scan_prefix(prefix) {
            let p = Path::new(&k);
            let rule = Ref::<Rule>::new(v.clone());
            if p.child().unwrap().child().unwrap().is_ancestor(path) && rule.as_ref().perm >= perm {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn can(&self, peer: PeerId, perm: Permission, path: Path) -> Result<bool> {
        let (doc, path) = path.split_first().unwrap();
        let doc = doc.doc().unwrap();
        if peer == doc.into() {
            return Ok(true);
        }
        if self.implies(&peer, &doc, perm, path)? {
            return Ok(true);
        }
        if self.implies(&PeerId::new([0; 32]), &doc, perm, path)? {
            return Ok(true);
        }
        Ok(false)
    }
}

struct AclDebug<'a>(&'a BlobMap);

impl<'a> std::fmt::Debug for AclDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for (k, v) in self.0.iter() {
            let path = Path::new(&k);
            let rule = Ref::<Rule>::new(v.clone());
            m.entry(&path, rule.as_ref());
        }
        m.finish()
    }
}

impl std::fmt::Debug for Acl {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        AclDebug(&self.0).fmt(f)
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

    pub fn add_policy(&mut self, path: Path) {
        self._add_policy(path);
    }

    fn _add_policy(&mut self, path: Path) -> Option<()> {
        let dot = path.dot();
        // schema.doc.(primitive|str)*.policy.peer.sig
        let (path, _) = path.split_last()?;
        let (path, peer) = path.split_last()?;
        let (path, policy) = path.split_last()?;
        let peer = peer.peer()?;
        let policy = policy.policy()?;
        let path = path.to_owned();
        let says = match policy {
            Policy::Can(actor, perm) => Says::Can(dot, peer, Can::new(actor, perm, path)),
            Policy::CanIf(actor, perm, cond) => {
                Says::CanIf(dot, peer, Can::new(actor, perm, path), cond)
            }
            Policy::Revokes(dot) => Says::Revokes(peer, dot),
        };
        self.policy.insert(says);
        None
    }

    pub fn update_acl(&self) -> Result<()> {
        let mut runtime = Crepe::new();
        runtime.extend(self.policy.iter().map(Input));
        let (authorized, revoked) = runtime.run();
        let revoked: BTreeSet<Dot> = revoked.into_iter().map(|r| r.0).collect();
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
    use crate::{Backend, Keypair, Kind, Lens, Lenses, Package};
    use std::pin::Pin;
    use Permission::*;

    #[async_std::test]
    async fn test_la_says_can() -> Result<()> {
        let lenses = Lenses::new(vec![
            Lens::Make(Kind::Struct),
            Lens::AddProperty("contacts".into()),
        ]);
        let packages = vec![Package::new("acl".into(), 2, &lenses)];
        let mut sdk = Backend::memory(&packages)?;
        let a = sdk.frontend().generate_keypair()?;
        let b = sdk.frontend().generate_keypair()?;

        let doc = sdk.frontend().create_doc(a, "acl", Keypair::generate())?;
        Pin::new(&mut sdk).await?;

        assert!(!doc.cursor().can(&b, Read)?);

        assert!(doc.cursor().can(&a, Write)?);
        assert!(doc.cursor().can(&a, Read)?);
        assert!(doc.cursor().can(&a, Own)?);

        assert!(doc.cursor().field("contacts")?.can(&a, Write)?);
        Ok(())
    }

    #[async_std::test]
    async fn test_says_if() -> Result<()> {
        let mut sdk = Backend::memory(&vec![Package::empty("acl".into())])?;
        let a = sdk.frontend().generate_keypair()?;
        let b = sdk.frontend().generate_keypair()?;
        let doc1 = sdk.frontend().create_doc(a, "acl", Keypair::generate())?;
        Pin::new(&mut sdk).await?;
        let doc2 = sdk.frontend().create_doc(a, "acl", Keypair::generate())?;
        Pin::new(&mut sdk).await?;

        let cond = doc1.cursor().cond(Actor::Peer(b), Read);
        let op = doc2.cursor().say_can_if(Actor::Peer(b), Write, cond)?;
        doc2.apply(&op)?;
        Pin::new(&mut sdk).await?;
        assert!(!doc2.cursor().can(&b, Read)?);

        let op = doc1.cursor().say_can(Some(b), Write)?;
        doc1.apply(&op)?;
        Pin::new(&mut sdk).await?;
        assert!(doc2.cursor().can(&b, Read)?);

        Ok(())
    }

    #[async_std::test]
    async fn test_says_if_unbound() -> Result<()> {
        let mut sdk = Backend::memory(&vec![Package::empty("acl".into())])?;
        let a = sdk.frontend().generate_keypair()?;
        let b = sdk.frontend().generate_keypair()?;
        let doc1 = sdk.frontend().create_doc(a, "acl", Keypair::generate())?;
        let doc2 = sdk.frontend().create_doc(a, "acl", Keypair::generate())?;
        Pin::new(&mut sdk).await?;

        let cond = doc1.cursor().cond(Actor::Unbound, Read);
        let op = doc2.cursor().say_can_if(Actor::Unbound, Write, cond)?;
        doc2.apply(&op)?;
        Pin::new(&mut sdk).await?;
        assert!(!doc2.cursor().can(&b, Read)?);

        let op = doc1.cursor().say_can(Some(b), Write)?;
        doc1.apply(&op)?;
        Pin::new(&mut sdk).await?;
        assert!(doc2.cursor().can(&b, Read)?);

        Ok(())
    }

    #[async_std::test]
    async fn test_own_and_control() -> Result<()> {
        let mut sdk = Backend::memory(&vec![Package::empty("acl".into())])?;
        let a = sdk.frontend().generate_keypair()?;
        let b = sdk.frontend().generate_keypair()?;
        let c = sdk.frontend().generate_keypair()?;
        let doc = sdk.frontend().create_doc(a, "acl", Keypair::generate())?;
        Pin::new(&mut sdk).await?;

        let op = doc.cursor().say_can(Some(b), Control)?;
        doc.apply(&op)?;
        Pin::new(&mut sdk).await?;
        assert!(doc.cursor().can(&b, Control)?);

        let bdoc = sdk.frontend().doc_as(*doc.id(), &b)?;
        assert!(bdoc.cursor().say_can(Some(c), Control).is_err());

        let op = bdoc.cursor().say_can(Some(c), Read)?;
        doc.apply(&op)?;
        Pin::new(&mut sdk).await?;
        assert!(doc.cursor().can(&c, Read)?);

        Ok(())
    }

    #[async_std::test]
    async fn test_anonymous_can() -> Result<()> {
        let mut sdk = Backend::memory(&vec![Package::empty("acl".into())])?;
        let a = sdk.frontend().generate_keypair()?;
        let b = sdk.frontend().generate_keypair()?;
        let doc = sdk.frontend().create_doc(a, "acl", Keypair::generate())?;
        Pin::new(&mut sdk).await?;

        let op = doc.cursor().say_can(None, Read)?;
        doc.apply(&op)?;
        Pin::new(&mut sdk).await?;
        assert!(doc.cursor().can(&b, Read)?);
        Ok(())
    }

    #[async_std::test]
    async fn test_revoke() -> Result<()> {
        let mut sdk = Backend::memory(&vec![Package::empty("acl".into())])?;
        let a = sdk.frontend().generate_keypair()?;
        let b = sdk.frontend().generate_keypair()?;
        let doc = sdk.frontend().create_doc(a, "acl", Keypair::generate())?;
        Pin::new(&mut sdk).await?;

        let op = doc.cursor().say_can(Some(b), Write)?;
        doc.apply(&op)?;
        Pin::new(&mut sdk).await?;
        assert!(doc.cursor().can(&b, Write)?);

        let op = doc
            .cursor()
            .revoke(op.store.iter().next().unwrap().as_path().dot())?;
        doc.apply(&op)?;
        Pin::new(&mut sdk).await?;
        assert!(!doc.cursor().can(&b, Write)?);

        Ok(())
    }

    #[async_std::test]
    async fn test_cant_revoke_inv() -> Result<()> {
        let mut sdk = Backend::memory(&vec![Package::empty("acl".into())])?;
        let a = sdk.frontend().generate_keypair()?;
        let b = sdk.frontend().generate_keypair()?;
        let doc = sdk.frontend().create_doc(a, "acl", Keypair::generate())?;
        Pin::new(&mut sdk).await?;

        let op = doc.cursor().say_can(Some(b), Own)?;
        doc.apply(&op)?;
        Pin::new(&mut sdk).await?;
        assert!(doc.cursor().can(&b, Own)?);

        let bdoc = sdk.frontend().doc_as(*doc.id(), &b)?;
        let op = bdoc
            .cursor()
            .revoke(op.store.iter().next().unwrap().as_path().dot())?;
        doc.apply(&op)?;

        assert!(doc.cursor().can(&a, Own)?);

        Ok(())
    }
}
