library tlfs;

import './tlfs_capi.dart' as ffi;
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';
import 'package:ffi/ffi.dart';

DynamicLibrary _open() {
  if (Platform.isLinux) return DynamicLibrary.open('libtlfs.so');
  if (Platform.isAndroid) return DynamicLibrary.open('libtlfs.so');
  if (Platform.isMacOS) return DynamicLibrary.open('libtlfs.dylib');
  if (Platform.isIOS) return DynamicLibrary.executable();
  if (Platform.isWindows) return DynamicLibrary.open('libtlfs.dll');
  throw UnsupportedError('This platform is not supported.');
}

class FfiError implements Exception {
  String description = '';

  FfiError(ffi.NativeLibrary lib) {
    final length = lib.last_error_length();
    final ptr = malloc.allocate<Int8>(length);
    final ret = lib.error_message_utf8(ptr, length + 1);
    if (ret == -1) {
      throw AssertionError('Getting ffi error failed');
    }
    if (ret != length) {
      throw AssertionError('Wrong number of bytes written');
    }
    ptr[length] = 0;
    this.description = ptr.cast<Utf8>().toDartString();
    malloc.free(ptr);
  }

  @override
  String toString() => 'FfiError($description)';
}

class PeerId {
  final Uint8List bytes;

  PeerId(this.bytes);

  Pointer<Uint8> toPointer() {
    final ptr = malloc.allocate<Uint8>(32);
    for (int i = 0; i < 32; i++) {
      ptr[i] = this.bytes[i];
    }
    return ptr;
  }

  String toString() {
    return base64Url.encode(this.bytes);
  }
}

class DocId {
  final Uint8List bytes;

  DocId(this.bytes);

  Pointer<Uint8> toPointer() {
    final ptr = malloc.allocate<Uint8>(32);
    for (int i = 0; i < 32; i++) {
      ptr[i] = this.bytes[i];
    }
    return ptr;
  }

  String toString() {
    return base64Url.encode(this.bytes);
  }
}

class Sdk {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.Sdk> ptr;

  Sdk(this.lib, this.ptr);

  factory Sdk.memory(Uint8List package) {
    final lib = ffi.NativeLibrary(_open());
    final packagePtr = malloc.allocate<Uint8>(package.length);
    for (int i = 0; i < package.length; i++) {
      packagePtr[i] = package[i];
    }
    final ret =
        lib.sdk_create_memory(packagePtr.cast(), package.length);
    malloc.free(packagePtr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(lib);
    }
    return Sdk(lib, ret);
  }

  factory Sdk.persistent(String dbPath, Uint8List package) {
    final lib = ffi.NativeLibrary(_open());
    final dbPathPtr = dbPath.toNativeUtf8();
    final packagePtr = malloc.allocate<Uint8>(package.length);
    for (int i = 0; i < package.length; i++) {
      packagePtr[i] = package[i];
    }
    final ret = lib.sdk_create_persistent(dbPathPtr.cast(), dbPathPtr.length,
        packagePtr.cast(), package.length);
    malloc.free(packagePtr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(lib);
    }
    return Sdk(lib, ret);
  }

  void destroy() {
    final ret = this.lib.sdk_destroy(this.ptr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }

  PeerId peerId() {
    final peerPtr = malloc.allocate<Uint8>(32);
    final ret = this.lib.sdk_get_peerid(this.ptr, peerPtr.cast());
    if (ret != 32) {
      malloc.free(peerPtr);
      throw FfiError(this.lib);
    }
    final peer = PeerId(peerPtr.asTypedList(32));
    return peer;
  }

  // TODO: addresses

  void addAddress(PeerId peer, String addr) {
    final peerPtr = peer.toPointer();
    final addrPtr = addr.toNativeUtf8();
    final ret = this.lib.sdk_add_address(
        this.ptr, peerPtr.cast(), addrPtr.cast(), addrPtr.length);
    malloc.free(peerPtr);
    malloc.free(addrPtr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }

  void removeAddress(PeerId peer, String addr) {
    final peerPtr = peer.toPointer();
    final addrPtr = addr.toNativeUtf8();
    final ret = this.lib.sdk_remove_address(
        this.ptr, peerPtr.cast(), addrPtr.cast(), addrPtr.length);
    malloc.free(peerPtr);
    malloc.free(addrPtr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }

  DocIter docs() {
    final ret = this.lib.sdk_create_doc_iter(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return DocIter(this.lib, ret);
  }

  Doc createDoc(String schema) {
    final schemaPtr = schema.toNativeUtf8();
    final ret =
        this.lib.sdk_create_doc(this.ptr, schemaPtr.cast(), schemaPtr.length);
    malloc.free(schemaPtr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Doc(this.lib, ret);
  }

  Doc openDoc(DocId doc) {
    final docPtr = doc.toPointer();
    final ret = this.lib.sdk_open_doc(this.ptr, docPtr.cast());
    malloc.free(docPtr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Doc(this.lib, ret);
  }

  Doc addDoc(DocId doc, String schema) {
    final docPtr = doc.toPointer();
    final schemaPtr = schema.toNativeUtf8();
    final ret = this.lib.sdk_add_doc(
        this.ptr, docPtr.cast(), schemaPtr.cast(), schemaPtr.length);
    malloc.free(docPtr);
    malloc.free(schemaPtr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Doc(this.lib, ret);
  }

  void removeDoc(DocId doc) {
    final docPtr = doc.toPointer();
    final ret = this.lib.sdk_remove_doc(this.ptr, docPtr.cast());
    malloc.free(docPtr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }
}

class Doc {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.Doc> ptr;

  Doc(this.lib, this.ptr);

  DocId id() {
    final docPtr = malloc.allocate<Uint8>(32);
    final ret = this.lib.doc_get_id(this.ptr, docPtr.cast());
    if (ret != 32) {
      throw FfiError(this.lib);
    }
    return DocId(docPtr.asTypedList(32));
  }

  Cursor cursor() {
    final ret = this.lib.doc_create_cursor(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Cursor(this.lib, ret);
  }

  void apply(Causal causal) {
    final ret = this.lib.doc_apply_causal(this.ptr, causal.ptr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }

  void destroy() {
    final ret = this.lib.doc_destroy(this.ptr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }
}

class DocIter {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.DocIter> ptr;

  DocIter(this.lib, this.ptr);

  DocId? next() {
    final ptr = malloc.allocate<Uint8>(32);
    final ret = this.lib.doc_iter_next(this.ptr, ptr.cast());
    if (ret == 0) {
      return null;
    }
    if (ret == 32) {
      return DocId(ptr.asTypedList(32));
    }
    throw FfiError(this.lib);
  }

  void destroy() {
    final ret = this.lib.doc_iter_destroy(this.ptr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }
}

class Cursor {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.Cursor> ptr;

  Cursor(this.lib, this.ptr);

  // TODO: subscribe

  void destroy() {
    final ret = this.lib.cursor_destroy(this.ptr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }

  bool enabled() {
    final ret = this.lib.cursor_flag_enabled(this.ptr);
    if (ret < 0) {
      throw FfiError(this.lib);
    }
    return ret > 0;
  }

  Causal enable() {
    final ret = this.lib.cursor_flag_enable(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Causal(this.lib, ret);
  }

  Causal disable() {
    final ret = this.lib.cursor_flag_disable(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Causal(this.lib, ret);
  }

  // TODO: values

  Causal assign(dynamic value) {
    final ret;
    if (value is bool) {
      ret = this.lib.cursor_reg_assign_bool(this.ptr, value as int);
    } else if (value is int) {
      // TODO: i64 vs u64
      ret = this.lib.cursor_reg_assign_u64(this.ptr, value);
    } else if (value is String) {
      final valuePtr = value.toNativeUtf8();
      ret = this.lib.cursor_reg_assign_str(this.ptr, valuePtr.cast(), valuePtr.length);
    } else {
        throw TypeError();
    }
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Causal(this.lib, ret);
  }

  void field(String field) {
    final fieldPtr = field.toNativeUtf8();
    final ret =
        this.lib.cursor_struct_field(this.ptr, fieldPtr.cast(), fieldPtr.length);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }

  void key(dynamic key) {
    final ret;
    if (key is bool) {
      ret = this.lib.cursor_map_key_bool(this.ptr, key as int);
    } else if (key is int) {
      // TODO: i64 vs u64
      ret = this.lib.cursor_map_key_u64(this.ptr, key);
    } else if (key is String) {
      final keyPtr = key.toNativeUtf8();
      ret = this.lib.cursor_map_key_str(this.ptr, keyPtr.cast(), keyPtr.length);
    } else {
      throw TypeError();
    }
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }

  Causal remove() {
    final ret = this.lib.cursor_map_remove(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Causal(this.lib, ret);
  }

  void index(int index) {
    final ret = this.lib.cursor_array_index(this.ptr, index);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }

  Causal move(int index) {
    final ret = this.lib.cursor_array_move(this.ptr, index);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Causal(this.lib, ret);
  }

  Causal delete() {
    final ret = this.lib.cursor_array_delete(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Causal(this.lib, ret);
  }

  // TODO: can, say_can, cond, say_can_if, revoke
}

class Causal {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.Causal> ptr;

  Causal(this.lib, this.ptr);
}
