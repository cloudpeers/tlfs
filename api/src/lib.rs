#![allow(clippy::boxed_local)]
#![allow(clippy::needless_question_mark)] // TODO

ffi_gen_macro::ffi_gen!("api/tlfs.rsh");

use anyhow::Result;
use futures::{Stream, StreamExt};
use std::path::Path;
use tlfs::Permission;

pub struct Sdk(tlfs::Sdk);

impl Sdk {
    pub async fn create_persistent(path: &str, package: &[u8]) -> Result<Self> {
        Ok(Self(tlfs::Sdk::persistent(Path::new(path), package).await?))
    }

    pub async fn create_memory(package: &[u8]) -> Result<Self> {
        Ok(Self(tlfs::Sdk::memory(package).await?))
    }

    pub fn get_peerid(&self) -> String {
        self.0.peer_id().to_string()
    }

    pub fn add_address(&self, peer_id: &str, addr: &str) -> Result<u8> {
        self.0.add_address(peer_id.parse()?, addr.parse()?);
        Ok(0)
    }

    pub fn remove_address(&self, peer_id: &str, addr: &str) -> Result<u8> {
        self.0.remove_address(peer_id.parse()?, addr.parse()?);
        Ok(0)
    }

    pub async fn addresses(&self) -> Vec<String> {
        self.0
            .addresses()
            .await
            .into_iter()
            .map(|addr| addr.to_string())
            .collect()
    }

    pub fn docs(&self, schema: String) -> Result<Vec<String>> {
        self.0.docs(schema).map(|id| Ok(id?.to_string())).collect()
    }

    pub fn create_doc(&self, schema: &str) -> Result<Doc> {
        Ok(Doc(self.0.create_doc(schema)?))
    }

    pub fn open_doc(&self, doc_id: &str) -> Result<Doc> {
        Ok(Doc(self.0.doc(doc_id.parse()?)?))
    }

    pub fn add_doc(&self, doc_id: &str, schema: &str) -> Result<Doc> {
        Ok(Doc(self.0.add_doc(doc_id.parse()?, schema)?))
    }

    pub fn remove_doc(&self, doc_id: &str) -> Result<u8> {
        self.0.remove_doc(&doc_id.parse()?)?;
        Ok(0)
    }
}

pub struct Doc(tlfs::Doc);

impl Doc {
    pub fn create_cursor(&self) -> Cursor {
        Cursor(self.0.cursor())
    }

    pub fn apply_causal(&self, causal: Box<Causal>) -> Result<u8> {
        self.0.apply(causal.0)?;
        Ok(0)
    }
}

#[derive(Clone)]
pub struct Cursor<'a>(tlfs::Cursor<'a>);

impl<'a> Cursor<'a> {
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

    pub fn struct_field(&mut self, field: &str) -> Result<u8> {
        self.0.field(field)?;
        Ok(0)
    }

    pub fn map_key_bool(&mut self, key: bool) -> Result<u8> {
        self.0.key_bool(key)?;
        Ok(0)
    }

    pub fn map_key_u64(&mut self, key: u64) -> Result<u8> {
        self.0.key_u64(key)?;
        Ok(0)
    }

    pub fn map_key_i64(&mut self, key: i64) -> Result<u8> {
        self.0.key_i64(key)?;
        Ok(0)
    }

    pub fn map_key_str(&mut self, key: &str) -> Result<u8> {
        self.0.key_str(key)?;
        Ok(0)
    }

    pub fn map_remove(&self) -> Result<Causal> {
        Ok(Causal(self.0.remove()?))
    }

    pub fn array_length(&mut self) -> Result<u32> {
        self.0.len()
    }

    pub fn array_index(&mut self, index: usize) -> Result<u8> {
        self.0.index(index)?;
        Ok(0)
    }

    pub fn array_move(&mut self, index: usize) -> Result<Causal> {
        Ok(Causal(self.0.r#move(index)?))
    }

    pub fn array_remove(&mut self) -> Result<Causal> {
        Ok(Causal(self.0.delete()?))
    }

    pub fn can(&self, peer_id: &str, perm: u8) -> Result<bool> {
        let perm = match perm {
            0 => Permission::Read,
            1 => Permission::Write,
            2 => Permission::Control,
            3 => Permission::Own,
            _ => anyhow::bail!("invalid permission"),
        };
        self.0.can(&peer_id.parse()?, perm)
    }

    pub fn subscribe(&self) -> impl Stream<Item = u8> {
        self.0.subscribe().map(|_batch| 0)
    }
}

pub struct Causal(tlfs::Causal);

impl Causal {
    pub fn join(&mut self, other: Box<Causal>) {
        self.0.join(&other.0);
    }
}
