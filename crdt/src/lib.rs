//! # The local first sdk's crdt
//!
//! ## ORSet
//! The workhorse of this crate is an ORSet (Observed-Remove Set). An ORSet contains a store set and
//! an expired set. When an element is added it is added to the store set and moved to the
//! expired set upon deletion.
//!
//! ## Path
//! The elements stored in this ORSet are called paths. These paths are used to represent other
//! crdts like the EWFlag, MVReg, ORMap, and ORArray. The path has the following logical format:
//! ```bnf
//! prim := prim_bool | prim_u64 | prim_i64 | prim_str
//! key := prim
//! field := prim_str
//! ewflag := nonce
//! mvreg := nonce prim
//! path := doc (key | field)* (ewflag | mvreg | policy) peer sig
//! tombstone := path peer sig
//! ```
//!
//! ## Case study: Using ORSet<Path> to construct an MVReg
//! An MVReg (Multi-Value) is a set of concurrently written values. When a value is assigned all previous
//! values are cleared. To create an ORSet that performs an MVReg assign when joined with
//! another ORSet we add each value currently in the MVReg to the expired set and add the new
//! value to the store set. When this delta is joined with the previous state, or with other
//! concurrent updates the set of values will converge.
//!
//! NOTE: peer identifiers in paths are for declaring authorship and verifying signatures. they
//! are not required for convergence.  nonces are used to add some randomness to paths to make
//! them unique.
//!
//! ## Byzantine Eventual Consistency
//! In distributed systems without coordination only some properties are achievable. The strongest
//! properties that a distributed system without coordination can achive is called BEC. BEC has
//! the following properties that are guaranteed in the presence of an arbitrary number of byzantine
//! nodes assuming that correct replicas form a connected component:
//!
//! - self-update: If a correct replica generates an update, it applies that update to its own
//! state.
//! - eventual update: For any update applied by a correct replica all correct replicas will
//! eventually apply that update
//! - convergence: Any two correct replicas that have applied the same set of updates are in the
//! same state
//! - atomicity: When a correct replica applies an update, it atomically applies all the updates
//! resulting from the same transaction.
//! - authenticity: If a correct replica applies an update that is labeled as originating from
//! replica s, then that update was generated by replica s
//! - causal consistency: if a correct replica generates or applies update u1 before generating
//! update u2, then all correct replicas apply u1 before u2.
//! - invariant preservation: The state of a correct replica always satisfies all of the
//! application's declared invariants.
//!
//! ## Invariant confluence
//! A set of transactions can be executed without coordination if and only if those transactions
//! are I-confluent with regard to all of the application's invariants.
//!
//! Transaction T is I-confluent with regard to invariant I if for all Ti,Tj,S where Ti and Tj are
//! concurrent and the state S, `Si = apply(Ti, S)` and `Sj = apply(Tj, S)` satisfy I implies that
//! `apply(Tj, apply(Ti, S))` also satisfies I.
//!
//! ## Access control
//! Coordination free access control is built on the following principles:
//!
//! - Policy is encoded in a logical language that provides a means to specify permissions of
//! actors on paths.
//! - Authority flows from a single root; policy statements combine without ambiguity. Two replicas
//! with an identical set of policy claims will make identical access control decisions regardless
//! of the order in which they learned of these claims.
//! - Access control checks are performed within the operations that implement data replication.
//! These access control checks are enforced according to the local policy state present at the time
//! of enforcement.
//! - Encoded security policy is replicated as data by the existing replication framework.
//!
//! The authority root is an ephemeral keypair generated when creating a document. The public key
//! is used as the document identifier. There are three kinds of policy statements each encoded as
//! a path in the ORSet:
//!
//! - unconditional: {actor} says {actor} can {permission} {path}
//! - conditional: {actor} says {actor} can {permission} {path} if {actor} can {permission} {path}
//! - revocation: {actor} revokes {hash(path)}
//!
//! where permission is one of read/write/control/own. control allows delegating read and write
//! permission while own allows delegating read/write/control/own permissions and actor is either
//! a public key or anonymous. The `anonymous` actor can be used to for example give read
//! permissions to everyone.
//!
//! The set of all policy statements is used to deduce if a peer is authorized to perform a task.
//! There are five inference rules that can be used to determine if a peer has access:
//!
//! - resolve conditional: if there is a true statement that implies the condition, the conditional
//! is transformed into an unconditional statement.
//! - local authority: if an unconditional is signed by the ephemeral document key then the statement
//! is authorized.
//! - ownership: if an unconditional is signed by a peer and there is an authorized statement that
//! implies the peer has ownership, the statement is authorized.
//! - control: if an unconditional is signed by a peer and there is an authorized statement that
//! implies the peer has control privileges, the statement is authorized if it is delegating
//! read/write permissions.
//! - revoke: a peer can revoke a statement if one of the following conditions is met:
//!     - the revoking peer is the root authority
//!     - the revoking peer has higher permissions than the issuing peer but at least control permission
//!     - the revoking peer has permissions on the parent the issuing peer doesn't have access to
//!     - the revoking peer is the same peer as the issuing peer
//!
//! ## Schemas and transforms
//! So that applications can evolve in backwards and forwards compatible ways a system of
//! bidirectional schema transforms called lenses is used. From an ordered list of lenses a
//! schema is constructed which is used to enforce I-confluent invariants. From a source and
//! destination ordered lists of lenses data valid in one schema can be transformed into another
//! schema. This is done by finding the common prefix of those list and applying the reverse of
//! the lenses of the source schema in reverse order followed by applying the lenses of the target
//! schema.
//!
//! ## Networking
//! To ensure convergence in the presence of byzantine nodes periodic unjoins are requested from
//! peers. When requesting an unjoin a `CausalContext` is sent which includes a set of active dots
//! and a set of expired dots, where a dot is the hash of a path. The server then responds with
//! a `Causal` which includes a set of active paths not contained in the set active dots or expired
//! dots and the set of expired paths not contained in the set of expired dots.
//!
//! To ensure the correct nodes form a fully connected component we use a point to point broadcast
//! protocol. This makes the broadcast protocol sybil resistant and prevents eclipse attacks.
//!
//! ## Future improvements
//! - compromise recovery: recover from accidental or malicious modification to restore a previous
//! state.
//! - using untrusted servers: currently the ORSet converges even when the paths are encrypted.
//! However for correct operation we need to also prove that encrypted updates don't violate the
//! invariants and that the author had permission to make the change. In additon homomorphic
//! transforms which preserve the zero knowledge proofs will be necessary.
#![warn(missing_docs)]
mod acl;
mod crdt;
mod crypto;
mod cursor;
mod doc;
mod dotset;
mod fraction;
mod id;
mod lens;
mod path;
#[cfg(test)]
mod props;
mod registry;
mod schema;
mod subscriber;
mod util;

pub use crate::acl::{Actor, Permission, Policy};
pub use crate::crdt::{Causal, CausalContext};
pub use crate::crypto::Keypair;
pub use crate::cursor::Cursor;
pub use crate::doc::{Backend, Doc, Frontend};
pub use crate::dotset::{ArchivedDotSet, Dot, DotSet};
pub use crate::id::{DocId, PeerId};
pub use crate::lens::{ArchivedKind, ArchivedLens, ArchivedLenses, Kind, Lens, LensRef, Lenses};
pub use crate::path::{Path, PathBuf, Segment};
pub use crate::registry::{Expanded, Hash, Package, Registry};
pub use crate::schema::{ArchivedSchema, PrimitiveKind, Schema};
pub use crate::subscriber::{Batch, Event, Iter, Subscriber};
pub use crate::util::Ref;
