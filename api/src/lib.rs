use async_trait::async_trait;
use witx_bindgen_rust::Handle;

witx_bindgen_rust::export!("./tlfs.witx");

type Result<T> = std::result::Result<T, String>;

pub struct Tlfs {}

impl tlfs::Tlfs for Tlfs {}

pub struct Sdk {
}

#[async_trait(?Send)]
impl tlfs::Sdk for Sdk {
    async fn persistent(db_path: String, package: Vec<u8>) -> Result<Handle<Self>> {
        todo!()
    }

    async fn memory(package: Vec<u8>) -> Result<Handle<Self>> {
        todo!()
    }

    fn peer_id(&self) -> String {
        todo!()
    }

    fn add_address(&self, peer_id: String, addr: String) -> Result<u8> {
        todo!()
    }

    fn remove_address(&self, peer_id: String, addr: String) -> Result<u8> {
        todo!()
    }

    fn create_doc(&self, schema: String) -> Result<Handle<Doc>> {
        todo!()
    }

    fn open_doc(&self, doc_id: String) -> Result<Handle<Doc>> {
        todo!()
    }

    fn add_doc(&self, doc_id: String, schema: String) -> Result<Handle<Doc>> {
        todo!()
    }

    fn remove_doc(&self, doc_id: String) -> Result<u8> {
        todo!()
    }
}

pub struct Doc {
}

impl tlfs::Doc for Doc {
    fn doc_id(&self) -> String {
        todo!()
    }

    fn cursor(&self) -> Handle<Cursor> {
        todo!()
    }

    fn apply(&self, causal: Handle<Causal>) -> Result<u8> {
        todo!()
    }
}

pub struct Cursor {
}

impl tlfs::Cursor for Cursor {
    fn enabled(&self) -> Result<bool> {
        todo!()
    }

    fn enable(&self) -> Result<Handle<Causal>> {
        todo!()
    }

    fn disable(&self) -> Result<Handle<Causal>> {
        todo!()
    }
}

pub struct Causal {
}

impl tlfs::Causal for Causal {
    fn join(&self, causal: Handle<Causal>) -> Result<u8> {
        todo!()
    }
}
