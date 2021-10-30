#![allow(clippy::not_unsafe_ptr_arg_deref)]
use anyhow::Result;
use ffi_helpers::*;
use std::path::Path;
use tlfs::{DocId, Multiaddr, PeerId, Permission};

#[no_mangle]
pub extern "C" fn last_error_length() -> libc::c_int {
    ffi_helpers::error_handling::last_error_length()
}

#[no_mangle]
pub extern "C" fn error_message_utf8(buf: *mut libc::c_char, length: libc::c_int) -> libc::c_int {
    unsafe { ffi_helpers::error_handling::error_message_utf8(buf, length) }
}

#[inline]
fn check_slice<T>(ptr: *const T, len: usize) -> Result<&'static [T]> {
    if ptr.is_null() {
        return Err(NullPointer.into());
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

#[inline]
fn check_str(ptr: *const u8, len: usize) -> Result<&'static str> {
    let bytes = check_slice(ptr, len)?;
    Ok(std::str::from_utf8(bytes)?)
}

#[inline]
fn check_path(ptr: *const u8, len: usize) -> Result<&'static Path> {
    let s = check_str(ptr, len)?;
    Ok(Path::new(s))
}

#[repr(C)]
pub struct Sdk;

#[repr(C)]
pub struct Doc;

#[repr(C)]
pub struct DocIter;

#[repr(C)]
pub struct Cursor;

#[repr(C)]
pub struct Causal;

#[no_mangle]
pub extern "C" fn sdk_create_persistent(
    db_path_ptr: *const u8,
    db_path_len: usize,
    package_path_ptr: *const u8,
    package_path_len: usize,
) -> *mut Sdk {
    catch_panic(|| {
        let db_path = check_path(db_path_ptr, db_path_len)?;
        let package_path = check_path(package_path_ptr, package_path_len)?;
        let sdk = async_global_executor::block_on(tlfs::Sdk::persistent(db_path, package_path))?;
        Ok(Box::into_raw(Box::new(sdk)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn sdk_create_memory(
    package_path_ptr: *const u8,
    package_path_len: usize,
) -> *mut Sdk {
    catch_panic(|| {
        let package_path = check_path(package_path_ptr, package_path_len)?;
        let package = std::fs::read(package_path)?;
        let sdk = async_global_executor::block_on(tlfs::Sdk::memory(&package))?;
        Ok(Box::into_raw(Box::new(sdk)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn sdk_destroy(sdk: *mut Sdk) -> i32 {
    catch_panic(move || {
        unsafe { Box::from_raw(sdk as *mut tlfs::Sdk) };
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn sdk_get_peerid(sdk: *mut Sdk, peer: *mut [u8; 32]) -> i32 {
    catch_panic(move || {
        let sdk = unsafe { &mut *(sdk as *mut tlfs::Sdk) };
        let peer = unsafe { &mut *peer };
        peer.copy_from_slice(sdk.peer_id().as_ref());
        Ok(32)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn sdk_add_address(
    sdk: *mut Sdk,
    peer: &[u8; 32],
    addr: *const u8,
    addr_length: usize,
) -> i32 {
    catch_panic(move || {
        let sdk = unsafe { &mut *(sdk as *mut tlfs::Sdk) };
        let peer = PeerId::new(*peer);
        let addr: Multiaddr = check_str(addr, addr_length)?.parse()?;
        sdk.add_address(peer, addr);
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn sdk_remove_address(
    sdk: *mut Sdk,
    peer: &[u8; 32],
    addr: *const u8,
    addr_length: usize,
) -> i32 {
    catch_panic(move || {
        let sdk = unsafe { &mut *(sdk as *mut tlfs::Sdk) };
        let peer = PeerId::new(*peer);
        let addr: Multiaddr = check_str(addr, addr_length)?.parse()?;
        sdk.remove_address(peer, addr);
        Ok(0)
    })
    .unwrap_or(-1)
}

// TODO: addresses

type DynDocIter = dyn Iterator<Item = Result<DocId>>;

#[no_mangle]
pub extern "C" fn sdk_create_doc_iter(sdk: *mut Sdk) -> *mut DocIter {
    catch_panic(move || {
        let sdk = unsafe { &mut *(sdk as *mut tlfs::Sdk) };
        let iter = Box::new(Box::new(sdk.docs()) as Box<DynDocIter>);
        Ok(Box::into_raw(iter) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn doc_iter_next(iter: *mut DocIter, doc: *mut [u8; 32]) -> i32 {
    catch_panic(move || {
        let iter = unsafe { &mut *(iter as *mut &mut DynDocIter) };
        let doc = unsafe { &mut *doc };
        if let Some(res) = iter.next().transpose()? {
            doc.copy_from_slice(res.as_ref());
            Ok(32)
        } else {
            Ok(0)
        }
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn doc_iter_destroy(iter: *mut DocIter) -> i32 {
    catch_panic(move || {
        drop(unsafe { Box::from_raw(iter as *mut Box<DynDocIter>) });
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn sdk_create_doc(
    sdk: *mut Sdk,
    schema_ptr: *const u8,
    schema_len: usize,
) -> *mut Doc {
    catch_panic(move || {
        let sdk = unsafe { &mut *(sdk as *mut tlfs::Sdk) };
        let schema = check_str(schema_ptr, schema_len)?;
        let doc = sdk.create_doc(schema)?;
        Ok(Box::into_raw(Box::new(doc)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn sdk_open_doc(sdk: *mut Sdk, doc: &[u8; 32]) -> *mut Doc {
    catch_panic(move || {
        let sdk = unsafe { &mut *(sdk as *mut tlfs::Sdk) };
        let doc = sdk.doc(DocId::new(*doc))?;
        Ok(Box::into_raw(Box::new(doc)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn sdk_add_doc(
    sdk: *mut Sdk,
    doc: &[u8; 32],
    schema_ptr: *const u8,
    schema_len: usize,
) -> *mut Doc {
    catch_panic(move || {
        let sdk = unsafe { &mut *(sdk as *mut tlfs::Sdk) };
        let schema = check_str(schema_ptr, schema_len)?;
        let doc = sdk.add_doc(DocId::new(*doc), schema)?;
        Ok(Box::into_raw(Box::new(doc)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn sdk_remove_doc(sdk: *mut Sdk, doc: &[u8; 32]) -> i32 {
    catch_panic(move || {
        let sdk = unsafe { &mut *(sdk as *mut tlfs::Sdk) };
        sdk.remove_doc(&DocId::new(*doc))?;
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn doc_id(doc: *mut Doc, id: *mut [u8; 32]) -> i32 {
    catch_panic(move || {
        let doc = unsafe { &mut *(doc as *mut tlfs::Doc) };
        let id = unsafe { &mut *id };
        id.copy_from_slice(doc.id().as_ref());
        Ok(32)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn doc_cursor(doc: *mut Doc) -> *mut Cursor {
    catch_panic(move || {
        let doc = unsafe { &mut *(doc as *mut tlfs::Doc) };
        let cursor = doc.cursor();
        Ok(Box::into_raw(Box::new(cursor)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn doc_apply(doc: *mut Doc, causal: *mut Causal) -> i32 {
    catch_panic(move || {
        let doc = unsafe { &mut *(doc as *mut tlfs::Doc) };
        let causal = unsafe { Box::from_raw(causal as *mut tlfs::Causal) };
        doc.apply(*causal)?;
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn doc_destroy(doc: *mut Doc) -> i32 {
    catch_panic(move || {
        unsafe { Box::from_raw(doc as *mut tlfs::Doc) };
        Ok(0)
    })
    .unwrap_or(-1)
}

// TODO: subscribe

pub extern "C" fn cursor_field(cursor: *mut Cursor, field: *const u8, field_length: usize) -> i32 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let field = check_str(field, field_length)?;
        cursor.field(field)?;
        Ok(0)
    })
    .unwrap_or(-1)
}

pub extern "C" fn cursor_can(cursor: *mut Cursor, peer: &[u8; 32], perm: Permission) -> i32 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        Ok(cursor.can(&PeerId::new(*peer), perm)? as i32)
    })
    .unwrap_or(-1)
}

// enabled, enable, disable
// field
// bools, u64s, i64s, strs, assign_bool, assign_u64, assign_i64, assign_str
// key_bool, key_u64, key_i64, key_str, remove
// index, move, delete
// can, say_can, cond, say_can_if, revoke

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api() {
        let packages = "assets/capi/include/todoapp.tlfs.rkyv";
        let package_name = "todoapp";
        let sdk = sdk_create_memory(packages.as_ptr(), packages.len());
        if sdk.is_null() {
            panic!();
        }
        let mut peer = [0; 32];
        let ret = sdk_get_peerid(sdk, &mut peer);
        if ret != 32 {
            panic!();
        }
        println!("{}", PeerId::new(peer));
        let doc = sdk_create_doc(sdk, package_name.as_ptr(), package_name.len());
        if doc.is_null() {
            panic!();
        }
        let mut id = [0; 32];
        let ret = doc_id(doc, &mut id);
        if ret != 32 {
            panic!();
        }
        println!("{}", DocId::new(id));
        let doc2 = sdk_create_doc(sdk, package_name.as_ptr(), package_name.len());
        let iter = sdk_create_doc_iter(sdk);
        if iter.is_null() {
            panic!();
        }
        println!("iterating docs");
        loop {
            let mut id = [0; 32];
            let ret = doc_iter_next(iter, &mut id);
            println!("ret {}", ret);
            if ret == 0 {
                break;
            } else if ret == 32 {
                println!("{:?}", DocId::new(id));
            } else {
                panic!()
            }
        }
        doc_destroy(doc);
        doc_destroy(doc2);
        sdk_destroy(sdk);
    }
}
