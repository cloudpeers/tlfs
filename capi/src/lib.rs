#![allow(clippy::not_unsafe_ptr_arg_deref)]
use anyhow::Result;
use ffi_helpers::*;
use std::mem::ManuallyDrop;
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

type DynIter<T> = dyn Iterator<Item = Result<T>>;

#[repr(C)]
pub struct Sdk;

#[repr(C)]
pub struct Doc;

#[repr(C)]
pub struct DocIter;

#[repr(C)]
pub struct Cursor;

#[repr(C)]
pub struct BoolIter;

#[repr(C)]
pub struct U64Iter;

#[repr(C)]
pub struct I64Iter;

#[repr(C)]
pub struct StrIter;

#[repr(C)]
pub struct Causal;

#[no_mangle]
pub extern "C" fn sdk_create_persistent(
    db_path_ptr: *const u8,
    db_path_len: usize,
    package_ptr: *const u8,
    package_len: usize,
) -> *mut Sdk {
    catch_panic(|| {
        let db_path = check_path(db_path_ptr, db_path_len)?;
        let package = check_slice(package_ptr, package_len)?;
        let sdk = async_global_executor::block_on(tlfs::Sdk::persistent(db_path, package))?;
        Ok(Box::into_raw(Box::new(sdk)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn sdk_create_memory(package_ptr: *const u8, package_len: usize) -> *mut Sdk {
    catch_panic(|| {
        let package = check_slice(package_ptr, package_len)?;
        let sdk = async_global_executor::block_on(tlfs::Sdk::memory(package))?;
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
#[no_mangle]
pub extern "C" fn sdk_create_doc_iter(
    sdk: *mut Sdk,
    schema: *const u8,
    schema_len: usize,
) -> *mut DocIter {
    catch_panic(move || {
        let sdk = unsafe { &mut *(sdk as *mut tlfs::Sdk) };
        let schema = check_str(schema, schema_len)?.to_string();
        let iter = Box::new(Box::new(sdk.docs(schema)) as Box<DynIter<DocId>>);
        Ok(Box::into_raw(iter) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn doc_iter_next(iter: *mut DocIter, doc: *mut [u8; 32]) -> i32 {
    catch_panic(move || {
        let iter = unsafe { &mut *(iter as *mut &mut DynIter<DocId>) };
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
        drop(unsafe { Box::from_raw(iter as *mut Box<DynIter<DocId>>) });
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
pub extern "C" fn doc_get_id(doc: *mut Doc, id: *mut [u8; 32]) -> i32 {
    catch_panic(move || {
        let doc = unsafe { &mut *(doc as *mut tlfs::Doc) };
        let id = unsafe { &mut *id };
        id.copy_from_slice(doc.id().as_ref());
        Ok(32)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn doc_create_cursor(doc: *mut Doc) -> *mut Cursor {
    catch_panic(move || {
        let doc = unsafe { &mut *(doc as *mut tlfs::Doc) };
        let cursor = doc.cursor();
        Ok(Box::into_raw(Box::new(cursor)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn doc_apply_causal(doc: *mut Doc, causal: *mut Causal) -> i32 {
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

#[no_mangle]
pub extern "C" fn causal_join(causal: *mut Causal, other: *mut Causal) -> i32 {
    catch_panic(move || {
        let causal = unsafe { &mut *(causal as *mut tlfs::Causal) };
        let other = unsafe { Box::from_raw(other as *mut tlfs::Causal) };
        causal.join(&other);
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_clone(cursor: *mut Cursor) -> *mut Cursor {
    catch_panic(move || {
        let cursor = unsafe { &*(cursor as *const tlfs::Cursor) };
        Ok(Box::into_raw(Box::new(cursor.clone())) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

// TODO: subscribe

#[no_mangle]
pub extern "C" fn cursor_flag_enabled(cursor: *mut Cursor) -> i32 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        Ok(cursor.enabled()? as i32)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_flag_enable(cursor: *mut Cursor) -> *mut Causal {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let causal = cursor.enable()?;
        Ok(Box::into_raw(Box::new(causal)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn cursor_flag_disable(cursor: *mut Cursor) -> *mut Causal {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let causal = cursor.disable()?;
        Ok(Box::into_raw(Box::new(causal)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn cursor_reg_bools(cursor: *mut Cursor) -> *mut BoolIter {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let iter = Box::new(Box::new(cursor.bools()?) as Box<DynIter<bool>>);
        Ok(Box::into_raw(iter) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn bool_iter_next(iter: *mut BoolIter, value: *mut bool) -> i32 {
    catch_panic(move || {
        let iter = unsafe { &mut *(iter as *mut &mut Box<DynIter<bool>>) };
        let value = unsafe { &mut *value };
        if let Some(res) = iter.next().transpose()? {
            *value = res;
            Ok(1)
        } else {
            Ok(0)
        }
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn bool_iter_destroy(iter: *mut BoolIter) -> i32 {
    catch_panic(move || {
        drop(unsafe { Box::from_raw(iter as *mut Box<DynIter<bool>>) });
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_reg_u64s(cursor: *mut Cursor) -> *mut U64Iter {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let iter = Box::new(Box::new(cursor.u64s()?) as Box<DynIter<u64>>);
        Ok(Box::into_raw(iter) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn u64_iter_next(iter: *mut U64Iter, value: *mut u64) -> i32 {
    catch_panic(move || {
        let iter = unsafe { &mut *(iter as *mut &mut Box<DynIter<u64>>) };
        let value = unsafe { &mut *value };
        if let Some(res) = iter.next().transpose()? {
            *value = res;
            Ok(1)
        } else {
            Ok(0)
        }
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn u64_iter_destroy(iter: *mut U64Iter) -> i32 {
    catch_panic(move || {
        drop(unsafe { Box::from_raw(iter as *mut Box<DynIter<u64>>) });
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_reg_i64s(cursor: *mut Cursor) -> *mut I64Iter {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let iter = Box::new(Box::new(cursor.i64s()?) as Box<DynIter<i64>>);
        Ok(Box::into_raw(iter) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn i64_iter_next(iter: *mut I64Iter, value: *mut i64) -> i32 {
    catch_panic(move || {
        let iter = unsafe { &mut *(iter as *mut &mut Box<DynIter<i64>>) };
        let value = unsafe { &mut *value };
        if let Some(res) = iter.next().transpose()? {
            *value = res;
            Ok(1)
        } else {
            Ok(0)
        }
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn i64_iter_destroy(iter: *mut I64Iter) -> i32 {
    catch_panic(move || {
        drop(unsafe { Box::from_raw(iter as *mut Box<DynIter<i64>>) });
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_reg_strs(cursor: *mut Cursor) -> *mut StrIter {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let iter = Box::new(Box::new(cursor.strs()?) as Box<DynIter<String>>);
        Ok(Box::into_raw(iter) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[repr(C)]
pub struct Buffer {
    pub data: *mut u8,
    pub len: usize,
    pub cap: usize,
}

#[no_mangle]
pub extern "C" fn str_iter_next(iter: *mut StrIter, buffer: *mut Buffer) -> i32 {
    catch_panic(move || {
        let iter = unsafe { &mut *(iter as *mut &mut DynIter<String>) };
        let buffer = unsafe { &mut *buffer };
        if let Some(res) = iter.next().transpose()? {
            let mut res = ManuallyDrop::new(res.into_bytes());
            buffer.data = res.as_mut_ptr();
            buffer.len = res.len();
            buffer.cap = res.capacity();
            Ok(1)
        } else {
            Ok(0)
        }
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn buffer_destroy(buf: Buffer) -> i32 {
    catch_panic(move || {
        unsafe { Vec::from_raw_parts(buf.data, buf.len, buf.cap) };
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn str_iter_destroy(iter: *mut StrIter) -> i32 {
    catch_panic(move || {
        drop(unsafe { Box::from_raw(iter as *mut Box<DynIter<String>>) });
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_reg_assign_bool(cursor: *mut Cursor, value: bool) -> *mut Causal {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let causal = cursor.assign_bool(value)?;
        Ok(Box::into_raw(Box::new(causal)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn cursor_reg_assign_u64(cursor: *mut Cursor, value: u64) -> *mut Causal {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let causal = cursor.assign_u64(value)?;
        Ok(Box::into_raw(Box::new(causal)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn cursor_reg_assign_i64(cursor: *mut Cursor, value: i64) -> *mut Causal {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let causal = cursor.assign_i64(value)?;
        Ok(Box::into_raw(Box::new(causal)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn cursor_reg_assign_str(
    cursor: *mut Cursor,
    value: *const u8,
    value_length: usize,
) -> *mut Causal {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let value = check_str(value, value_length)?;
        let causal = cursor.assign_str(value)?;
        Ok(Box::into_raw(Box::new(causal)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn cursor_struct_field(
    cursor: *mut Cursor,
    field: *const u8,
    field_length: usize,
) -> i32 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let field = check_str(field, field_length)?;
        cursor.field(field)?;
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_map_key_bool(cursor: *mut Cursor, key: bool) -> i32 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        cursor.key_bool(key)?;
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_map_key_u64(cursor: *mut Cursor, key: u64) -> i32 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        cursor.key_u64(key)?;
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_map_key_i64(cursor: *mut Cursor, key: i64) -> i32 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        cursor.key_i64(key)?;
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_map_key_str(
    cursor: *mut Cursor,
    key: *const u8,
    key_length: usize,
) -> i32 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let key = check_str(key, key_length)?;
        cursor.key_str(key)?;
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_map_remove(cursor: *mut Cursor) -> *mut Causal {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let causal = cursor.remove()?;
        Ok(Box::into_raw(Box::new(causal)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn cursor_array_length(cursor: *mut Cursor) -> i64 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        Ok(cursor.len()? as _)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_array_index(cursor: *mut Cursor, index: u32) -> i32 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        cursor.index(index as usize)?;
        Ok(0)
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn cursor_array_move(cursor: *mut Cursor, index: u32) -> *mut Causal {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let causal = cursor.r#move(index as usize)?;
        Ok(Box::into_raw(Box::new(causal)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn cursor_array_delete(cursor: *mut Cursor) -> *mut Causal {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        let causal = cursor.delete()?;
        Ok(Box::into_raw(Box::new(causal)) as *mut _)
    })
    .unwrap_or_else(|_| std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn cursor_acl_can(cursor: *mut Cursor, peer: &[u8; 32], perm: Permission) -> i32 {
    catch_panic(move || {
        let cursor = unsafe { &mut *(cursor as *mut tlfs::Cursor) };
        Ok(cursor.can(&PeerId::new(*peer), perm)? as i32)
    })
    .unwrap_or(-1)
}

// TODO: say_can, cond, say_can_if, revoke

#[no_mangle]
pub extern "C" fn cursor_destroy(cursor: *mut Cursor) -> i32 {
    catch_panic(move || {
        unsafe { Box::from_raw(cursor as *mut tlfs::Cursor) };
        Ok(0)
    })
    .unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api() {
        let packages = "assets/capi/include/todoapp.tlfs.rkyv";
        let packages = std::fs::read(packages).unwrap();
        let package_name = "todoapp";
        let sdk = sdk_create_memory(packages.as_ptr(), packages.len());
        assert!(!sdk.is_null());

        let mut peer = [0; 32];
        let ret = sdk_get_peerid(sdk, &mut peer);
        assert_eq!(ret, 32);

        let doc = sdk_create_doc(sdk, package_name.as_ptr(), package_name.len());
        assert!(!doc.is_null());

        let mut id = [0; 32];
        let ret = doc_get_id(doc, &mut id);
        assert_eq!(ret, 32);

        let doc2 = sdk_create_doc(sdk, package_name.as_ptr(), package_name.len());
        assert!(!doc2.is_null());

        let iter = sdk_create_doc_iter(sdk, package_name.as_ptr(), package_name.len());
        assert!(!iter.is_null());

        let mut len = 0;
        loop {
            let mut id = [0; 32];
            let ret = doc_iter_next(iter, &mut id);
            if ret == 0 {
                break;
            } else if ret == 32 {
                len += 1;
            } else {
                panic!()
            }
        }
        assert_eq!(len, 2);
        assert_eq!(doc_iter_destroy(iter), 0);

        let cursor = doc_create_cursor(doc);
        assert!(!cursor.is_null());

        let field = "tasks";
        let ret = cursor_struct_field(cursor, field.as_ptr(), field.len());
        assert_eq!(ret, 0);

        let ret = cursor_map_key_u64(cursor, 0);
        assert_eq!(ret, 0);

        let cursor2 = cursor_clone(cursor);

        let field = "complete";
        let ret = cursor_struct_field(cursor, field.as_ptr(), field.len());
        assert_eq!(ret, 0);

        // wait for acl to propagate.
        std::thread::sleep(std::time::Duration::from_millis(100));

        let causal = cursor_flag_enable(cursor);
        assert!(!causal.is_null());

        let field = "title";
        let ret = cursor_struct_field(cursor2, field.as_ptr(), field.len());
        assert_eq!(ret, 0);
        let title = "do something";
        let causal2 = cursor_reg_assign_str(cursor2, title.as_ptr(), title.len());

        let ret = causal_join(causal, causal2);
        assert_eq!(ret, 0);
        let ret = doc_apply_causal(doc, causal);
        assert_eq!(ret, 0);

        let enabled = cursor_flag_enabled(cursor);
        assert_eq!(enabled, 1);
        let iter = cursor_reg_strs(cursor2);
        assert!(!iter.is_null());
        let mut buf: Buffer = unsafe { std::mem::zeroed() };
        let ret = str_iter_next(iter, &mut buf);
        assert_eq!(ret, 1);
        let s = check_str(buf.data, buf.len).unwrap();
        println!("{}", s);
        assert_eq!(buffer_destroy(buf), 0);
        assert_eq!(str_iter_destroy(iter), 0);

        assert_eq!(cursor_destroy(cursor), 0);
        assert_eq!(cursor_destroy(cursor2), 0);

        assert_eq!(doc_destroy(doc), 0);
        assert_eq!(doc_destroy(doc2), 0);
        assert_eq!(sdk_destroy(sdk), 0);
    }
}
