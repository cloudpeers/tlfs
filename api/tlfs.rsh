//! # The local first sdk
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

/// Main entry point for `tlfs`.
object Sdk {
    /// Creates a new persistent sdk instance.
    static fn create_persistent(path: &string, package: &[u8]) -> Future<Result<Sdk>>;
    /// Create a new in-memory sdk instance.
    static fn create_memory(package: &[u8]) -> Future<Result<Sdk>>;
    /// Returns the peer id of this sdk.
    fn get_peerid() -> string;
    /// Adds a new multiaddr for a peer id.
    fn add_address(peer_id: &string, addr: &string) -> Result<()>;
    /// Removes a multiaddr of a peer id.
    fn remove_address(peer_id: &string, addr: &string) -> Result<()>;
    // TODO /// Returns the list of multiaddr the sdk is listening on.
    // fn addresses() -> Iterator<string>;

    /// Returns an iterator of doc id's.
    fn docs(schema: string) -> Result<Iterator<string>>;
    /// Creates a new document with an initial schema.
    fn create_doc(schema: &string) -> Result<Doc>;
    /// Returns a document handle.
    fn open_doc(doc_id: &string) -> Result<Doc>;
    /// Adds a document with a schema.
    fn add_doc(doc_id: &string, schema: &string) -> Result<Doc>;
    /// Removes a document.
    fn remove_doc(doc_id: &string) -> Result<()>;
}

/// Document handle.
object Doc {
    /// Returns the id of the document.
    fn id() -> string;
    /// Returns a cursor for the document.
    fn create_cursor() -> Cursor;
    /// Applies a transaction to the document.
    fn apply_causal(causal: Causal);
}

/// A cursor into a document used to construct transactions.
object Cursor {
    /// Returns a deep copy of the cursor.
    fn clone() -> Cursor;

    /// Returns if a flag is enabled.
    fn flag_enabled() -> Result<bool>;
    /// Enables a flag.
    fn flag_enable() -> Result<Causal>;
    /// Disables a flag.
    fn flag_disable() -> Result<Causal>;

    /// Returns an iterator of bools.
    fn reg_bools() -> Result<Iterator<bool>>;
    /// Returns an iterator of u64s.
    fn reg_u64s() -> Result<Iterator<u64>>;
    /// Returns an iterator of i64s.
    fn reg_i64s() -> Result<Iterator<i64>>;
    /// Returns an iterator of strings.
    fn reg_strs() -> Result<Iterator<string>>;
    /// Assigns a value to a register.
    fn reg_assign_bool(value: bool) -> Result<Causal>;
    /// Assigns a value to a register.
    fn reg_assign_u64(value: u64) -> Result<Causal>;
    /// Assigns a value to a register.
    fn reg_assign_i64(value: i64) -> Result<Causal>;
    /// Assigns a value to a register.
    fn reg_assign_str(value: &string) -> Result<Causal>;

    /// Returns a cursor to a field in a struct.
    fn struct_field(field: &string) -> Result<()>;

    /// Returns a cursor to a value in a table.
    fn map_key_bool(key: bool) -> Result<()>;
    /// Returns a cursor to a value in a table.
    fn map_key_u64(key: u64) -> Result<()>;
    /// Returns a cursor to a value in a table.
    fn map_key_i64(key: i64) -> Result<()>;
    /// Returns a cursor to a value in a table.
    fn map_key_str(key: &string) -> Result<()>;
    /// Returns an iterator of keys.
    fn map_keys_bool() -> Result<Iterator<bool>>;
    /// Returns an iterator of keys.
    fn map_keys_u64() -> Result<Iterator<u64>>;
    /// Returns an iterator of keys.
    fn map_keys_i64() -> Result<Iterator<i64>>;
    /// Returns an iterator of keys.
    fn map_keys_str() -> Result<Iterator<string>>;
    /// Removes a value from a map.
    fn map_remove() -> Result<Causal>;

    /// Returns the length of the array.
    fn array_length() -> Result<u32>;
    /// Returns a cursor to a value in an array.
    fn array_index(idx: u32) -> Result<()>;
    /// Moves the entry inside an array.
    fn array_move(idx: u32) -> Result<Causal>;
    /// Deletes the entry from an array.
    fn array_remove() -> Result<Causal>;

    /// Checks permissions.
    fn can(peer_id: &string, perm: u8) -> Result<bool>;
    /// Creates a policy statement.
    fn say_can(actor: Option<string>, perm: u8) -> Result<Causal>;
    /// Creates a conditional.
    fn cond(actor: Actor, perm: u8) -> Result<Can>;
    /// Creates a conditional policy statement.
    fn say_can_if(actor: Actor, perm: u8, cond: Can) -> Result<Causal>;
    // TODO: revoke

    /// Subscribe to a path.
    fn subscribe() -> Stream<i32>;
}

/// Represents a state transition of a crdt. Multiple state transitions can be combined
/// together into an atomic transaction.
object Causal {
    /// Combines two transactions into a larger transaction.
    fn join(other: Causal);
}

/// Represents a tuple of actor, permission and path.
object Can {}

/// The subject of a policy.
object Actor {
    /// A peer identified by id.
    static fn peer(id: &string) -> Result<Actor>;
    /// Any peer.
    static fn anonymous() -> Actor;
    /// A variable used when specifying conditional policies.
    ///
    /// An example usage would be "unbound can read contacts if unbound can read dashboard".
    static fn unbound() -> Actor;
}
