#![allow(clippy::boxed_local)]

ffi_gen_macro::ffi_gen!("api/tlfs.rsh");

use anyhow::Result;
use futures::{Stream, StreamExt};
use tlfs::Permission;
use tlfs_crdt::ArchivedSchema;

pub struct Sdk(tlfs::Sdk);

pub async fn create_persistent(path: &str, package: &[u8]) -> Result<Sdk> {
    Ok(Sdk(tlfs::Sdk::persistent(
        std::path::Path::new(path),
        package,
    )
    .await?))
}

pub async fn create_memory(package: &[u8]) -> Result<Sdk> {
    Ok(Sdk(tlfs::Sdk::memory(package).await?))
}

impl Sdk {
    pub fn get_peer_id(&self) -> String {
        self.0.peer_id().to_string()
    }

    pub fn add_address(&self, peer_id: &str, addr: &str) -> Result<()> {
        self.0.add_address(peer_id.parse()?, addr.parse()?);
        Ok(())
    }

    pub fn remove_address(&self, peer_id: &str, addr: &str) -> Result<()> {
        self.0.remove_address(peer_id.parse()?, addr.parse()?);
        Ok(())
    }

    pub async fn addresses(&self) -> Vec<String> {
        self.0
            .addresses()
            .await
            .into_iter()
            .map(|addr| addr.to_string())
            .collect()
    }

    pub fn subscribe_addresses(&self) -> impl Stream<Item = i32> {
        self.0.subscribe_addresses().map(|_| 0)
    }

    pub async fn local_peers(&self) -> Vec<String> {
        self.0
            .local_peers()
            .await
            .into_iter()
            .map(|peer| peer.to_string())
            .collect()
    }

    pub fn subscribe_local_peers(&self) -> impl Stream<Item = i32> {
        self.0.subscribe_local_peers().map(|_| 0)
    }

    pub async fn connected_peers(&self) -> Vec<String> {
        self.0
            .connected_peers()
            .await
            .into_iter()
            .map(|peer| peer.to_string())
            .collect()
    }

    pub fn subscribe_connected_peers(&self) -> impl Stream<Item = i32> {
        self.0.subscribe_connected_peers().map(|_| 0)
    }

    pub fn docs(&self, schema: String) -> Result<Vec<String>> {
        self.0.docs(schema).map(|id| Ok(id?.to_string())).collect()
    }

    pub fn subscribe_docs(&self) -> impl Stream<Item = i32> {
        self.0.subscribe_docs().map(|_| 0)
    }

    pub async fn create_doc(&self, schema: &str) -> Result<Doc> {
        Ok(Doc(self.0.create_doc(schema).await?))
    }

    pub fn open_doc(&self, doc_id: &str) -> Result<Doc> {
        Ok(Doc(self.0.doc(doc_id.parse()?)?))
    }

    pub fn add_doc(&self, doc_id: &str, schema: &str) -> Result<Doc> {
        Ok(Doc(self.0.add_doc(doc_id.parse()?, schema)?))
    }

    pub fn remove_doc(&self, doc_id: &str) -> Result<()> {
        self.0.remove_doc(&doc_id.parse()?)
    }

    pub async fn invites(&self) -> Vec<(String, String)> {
        self.0
            .invites()
            .await
            .into_iter()
            .map(|inv| (inv.doc.to_string(), inv.schema))
            .collect()
    }

    pub fn subscribe_invites(&self) -> impl Stream<Item = i32> {
        self.0.subscribe_invites().map(|_| 0)
    }
}

pub struct Doc(tlfs::Doc);

impl Doc {
    pub fn id(&self) -> String {
        self.0.id().to_string()
    }

    pub fn create_cursor(&self) -> Cursor {
        Cursor(self.0.cursor())
    }

    pub fn apply_causal(&self, causal: Box<Causal>) -> Result<()> {
        self.0.apply(causal.0)
    }

    pub fn invite_peer(&self, peer: String) -> Result<()> {
        self.0.invite(peer.parse()?)
    }
}

#[derive(Clone)]
pub struct Cursor<'a>(tlfs::Cursor<'a>);

impl<'a> Cursor<'a> {
    pub fn points_at_value(&self) -> bool {
        matches!(
            self.0.schema(),
            ArchivedSchema::Flag | ArchivedSchema::Reg(_)
        )
    }

    pub fn value_type(&self) -> Option<String> {
        if self.points_at_value() {
            Some(match self.0.schema() {
                ArchivedSchema::Null => "null".into(),
                ArchivedSchema::Flag => "bool".into(),
                ArchivedSchema::Reg(ty) => match ty {
                    tlfs::PrimitiveKind::Bool => "Reg<bool>",
                    tlfs::PrimitiveKind::U64 => "Reg<u64>",
                    tlfs::PrimitiveKind::I64 => "Reg<i64>",
                    tlfs::PrimitiveKind::Str => "Reg<string>",
                }
                .into(),
                ArchivedSchema::Table(_, _)
                | ArchivedSchema::Array(_)
                | ArchivedSchema::Struct(_) => unreachable!(),
            })
        } else {
            None
        }
    }

    pub fn keys(&self) -> Result<Vec<String>> {
        self.0.keys()
    }

    pub fn points_at_array(&self) -> bool {
        matches!(self.0.schema(), ArchivedSchema::Array(_))
    }

    pub fn points_at_table(&self) -> bool {
        matches!(self.0.schema(), ArchivedSchema::Table(_, _))
    }

    pub fn points_at_struct(&self) -> bool {
        matches!(self.0.schema(), ArchivedSchema::Struct(_))
    }

    pub fn flag_enabled(&self) -> Result<bool> {
        self.0.enabled()
    }

    pub fn flag_enable(&self) -> Result<Causal> {
        Ok(Causal(self.0.enable()?))
    }

    pub fn flag_disable(&self) -> Result<Causal> {
        Ok(Causal(self.0.disable()?))
    }

    pub fn reg_bools(&self) -> Result<Vec<bool>> {
        self.0.bools()?.collect()
    }

    pub fn reg_u64s(&self) -> Result<Vec<u64>> {
        self.0.u64s()?.collect()
    }

    pub fn reg_i64s(&self) -> Result<Vec<i64>> {
        self.0.i64s()?.collect()
    }

    pub fn reg_strs(&self) -> Result<Vec<String>> {
        self.0.strs()?.collect()
    }

    pub fn reg_assign_bool(&self, value: bool) -> Result<Causal> {
        Ok(Causal(self.0.assign_bool(value)?))
    }

    pub fn reg_assign_u64(&self, value: u64) -> Result<Causal> {
        Ok(Causal(self.0.assign_u64(value)?))
    }

    pub fn reg_assign_i64(&self, value: i64) -> Result<Causal> {
        Ok(Causal(self.0.assign_i64(value)?))
    }

    pub fn reg_assign_str(&self, value: &str) -> Result<Causal> {
        Ok(Causal(self.0.assign_str(value)?))
    }

    pub fn struct_field(&mut self, field: &str) -> Result<()> {
        self.0.field(field)?;
        Ok(())
    }

    pub fn map_key_bool(&mut self, key: bool) -> Result<()> {
        self.0.key_bool(key)?;
        Ok(())
    }

    pub fn map_key_u64(&mut self, key: u64) -> Result<()> {
        self.0.key_u64(key)?;
        Ok(())
    }

    pub fn map_key_i64(&mut self, key: i64) -> Result<()> {
        self.0.key_i64(key)?;
        Ok(())
    }

    pub fn map_key_str(&mut self, key: &str) -> Result<()> {
        self.0.key_str(key)?;
        Ok(())
    }

    pub fn map_keys_bool(&self) -> Result<Vec<bool>> {
        Ok(self.0.keys_bool()?.collect())
    }

    pub fn map_keys_u64(&self) -> Result<Vec<u64>> {
        Ok(self.0.keys_u64()?.collect())
    }

    pub fn map_keys_i64(&self) -> Result<Vec<i64>> {
        Ok(self.0.keys_i64()?.collect())
    }

    pub fn map_keys_str(&self) -> Result<Vec<String>> {
        Ok(self.0.keys_str()?.collect())
    }

    pub fn map_remove(&self) -> Result<Causal> {
        Ok(Causal(self.0.remove()?))
    }

    pub fn array_length(&mut self) -> Result<u32> {
        self.0.len()
    }

    pub fn array_index(&mut self, index: usize) -> Result<()> {
        self.0.index(index)?;
        Ok(())
    }

    pub fn array_move(&mut self, index: usize) -> Result<Causal> {
        Ok(Causal(self.0.r#move(index)?))
    }

    pub fn array_remove(&mut self) -> Result<Causal> {
        Ok(Causal(self.0.delete()?))
    }

    pub fn can(&self, peer_id: &str, perm: u8) -> Result<bool> {
        let perm = parse_perm(perm)?;
        self.0.can(&peer_id.parse()?, perm)
    }

    pub fn say_can(&self, actor: Option<String>, perm: u8) -> Result<Causal> {
        let actor = actor.map(|s| s.parse()).transpose()?;
        let perm = parse_perm(perm)?;
        Ok(Causal(self.0.say_can(actor, perm)?))
    }

    pub fn cond(&self, actor: Box<Actor>, perm: u8) -> Result<Can> {
        let perm = parse_perm(perm)?;
        Ok(Can(self.0.cond(actor.0, perm)))
    }

    pub fn say_can_if(&self, actor: Box<Actor>, perm: u8, cond: Box<Can>) -> Result<Causal> {
        let perm = parse_perm(perm)?;
        Ok(Causal(self.0.say_can_if(actor.0, perm, cond.0)?))
    }

    // TODO: revoke

    pub fn subscribe(&self) -> impl Stream<Item = i32> {
        self.0.subscribe().map(|_batch| 0)
    }
}

pub struct Causal(tlfs::Causal);

impl Causal {
    pub fn join(&mut self, other: Box<Causal>) {
        self.0.join(&other.0);
    }
}

pub struct Can(tlfs::Can);

fn parse_perm(perm: u8) -> Result<Permission> {
    Ok(match perm {
        0 => Permission::Read,
        1 => Permission::Write,
        2 => Permission::Control,
        3 => Permission::Own,
        _ => anyhow::bail!("invalid permission"),
    })
}

pub struct Actor(tlfs::Actor);

impl Actor {
    pub fn peer(id: &str) -> Result<Self> {
        Ok(Self(tlfs::Actor::Peer(id.parse()?)))
    }

    pub fn anonymous() -> Self {
        Self(tlfs::Actor::Anonymous)
    }

    pub fn unbound() -> Self {
        Self(tlfs::Actor::Unbound)
    }
}
