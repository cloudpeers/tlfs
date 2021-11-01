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
    final ret = lib.sdk_create_memory(packagePtr.cast(), package.length);
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
    final ret = lib.sdk_create_persistent(
        dbPathPtr.cast(), dbPathPtr.length, packagePtr.cast(), package.length);
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

  DocIter docs(String schema) {
    final schemaPtr = schema.toNativeUtf8();
    final ret = this
        .lib
        .sdk_create_doc_iter(this.ptr, schemaPtr.cast(), schemaPtr.length);
    malloc.free(schemaPtr);
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

class DocIter extends Iterable<DocId> implements Iterator<DocId> {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.DocIter> ptr;
  DocId _current = DocId(Uint8List(32));
  DocId get current => _current;

  DocIter(this.lib, this.ptr);

  @override
  Iterator<DocId> get iterator => this;

  @override
  bool moveNext() {
    final ptr = malloc.allocate<Uint8>(32);
    final ret = this.lib.doc_iter_next(this.ptr, ptr.cast());
    if (ret == 0) {
      this.destroy();
      return false;
    }
    if (ret == 32) {
      this._current = DocId(ptr.asTypedList(32));
      return true;
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

  Cursor clone() {
    final ret = this.lib.cursor_clone(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Cursor(this.lib, ret);
  }

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

  BoolIter bools() {
    final ret = this.lib.cursor_reg_bools(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return BoolIter(this.lib, ret);
  }

  Causal assignBool(bool value) {
    final ret = this.lib.cursor_reg_assign_bool(this.ptr, value);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Causal(this.lib, ret);
  }

  U64Iter u64s() {
    final ret = this.lib.cursor_reg_u64s(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return U64Iter(this.lib, ret);
  }

  Causal assignU64(int value) {
    final ret = this.lib.cursor_reg_assign_u64(this.ptr, value);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Causal(this.lib, ret);
  }

  I64Iter i64s() {
    final ret = this.lib.cursor_reg_i64s(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return I64Iter(this.lib, ret);
  }

  Causal assignI64(int value) {
    final ret = this.lib.cursor_reg_assign_u64(this.ptr, value);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Causal(this.lib, ret);
  }

  StrIter strs() {
    final ret = this.lib.cursor_reg_strs(this.ptr);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return StrIter(this.lib, ret);
  }

  Causal assignStr(String value) {
    final valuePtr = value.toNativeUtf8();
    final ret = this
        .lib
        .cursor_reg_assign_str(this.ptr, valuePtr.cast(), valuePtr.length);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(this.lib);
    }
    return Causal(this.lib, ret);
  }

  void field(String field) {
    final fieldPtr = field.toNativeUtf8();
    final ret = this
        .lib
        .cursor_struct_field(this.ptr, fieldPtr.cast(), fieldPtr.length);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }

  void key(dynamic key) {
    final ret;
    if (key is bool) {
      ret = this.lib.cursor_map_key_bool(this.ptr, key);
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

  // TODO: keys

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

  // TODO: len

  // TODO: can, say_can, cond, say_can_if, revoke
}

class Causal {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.Causal> ptr;

  Causal(this.lib, this.ptr);

  void join(Causal other) {
    final ret = this.lib.causal_join(this.ptr, other.ptr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }
}

class BoolIter extends Iterable<bool> implements Iterator<bool> {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.BoolIter> ptr;
  bool _current = false;
  bool get current => _current;

  BoolIter(this.lib, this.ptr);

  @override
  Iterator<bool> get iterator => this;

  @override
  bool moveNext() {
    final ptr = malloc.allocate<Uint8>(1);
    final ret = this.lib.bool_iter_next(this.ptr, ptr.cast());
    if (ret == 0) {
      malloc.free(ptr);
      this.destroy();
      return false;
    }
    if (ret == 1) {
      this._current = ptr as int > 0;
      malloc.free(ptr);
      return true;
    }
    throw FfiError(this.lib);
  }

  void destroy() {
    final ret = this.lib.bool_iter_destroy(this.ptr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }
}

class U64Iter extends Iterable<int> implements Iterator<int> {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.U64Iter> ptr;
  int _current = 0;
  int get current => _current;

  U64Iter(this.lib, this.ptr);

  @override
  Iterator<int> get iterator => this;

  @override
  bool moveNext() {
    final ptr = malloc.allocate<Uint64>(1);
    final ret = this.lib.u64_iter_next(this.ptr, ptr.cast());
    if (ret == 0) {
      malloc.free(ptr);
      this.destroy();
      return false;
    }
    if (ret == 1) {
      this._current = ptr as int;
      malloc.free(ptr);
      return true;
    }
    throw FfiError(this.lib);
  }

  void destroy() {
    final ret = this.lib.u64_iter_destroy(this.ptr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }
}

class I64Iter extends Iterable<int> implements Iterator<int> {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.I64Iter> ptr;
  int _current = 0;
  int get current => _current;

  I64Iter(this.lib, this.ptr);

  @override
  Iterator<int> get iterator => this;

  @override
  bool moveNext() {
    final ptr = malloc.allocate<Int64>(1);
    final ret = this.lib.i64_iter_next(this.ptr, ptr.cast());
    if (ret == 0) {
      malloc.free(ptr);
      this.destroy();
      return false;
    }
    if (ret == 1) {
      this._current = ptr as int;
      malloc.free(ptr);
      return true;
    }
    throw FfiError(this.lib);
  }

  void destroy() {
    final ret = this.lib.i64_iter_destroy(this.ptr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }
}

class StrIter extends Iterable<String> implements Iterator<String> {
  final ffi.NativeLibrary lib;
  final Pointer<ffi.StrIter> ptr;
  String _current = "";
  String get current => _current;

  StrIter(this.lib, this.ptr);
  @override
  Iterator<String> get iterator => this;

  @override
  bool moveNext() {
    final strPtr = malloc.allocate<IntPtr>(1);
    final lenPtr = malloc.allocate<IntPtr>(1);
    final ret = this.lib.str_iter_next(this.ptr, strPtr.cast(), lenPtr.cast());
    if (ret == 0) {
      malloc.free(strPtr);
      malloc.free(lenPtr);
      this.destroy();
      return false;
    }
    if (ret == 1) {
      final bytes = Uint8List(lenPtr.elementAt(0).value);
      final Pointer<Uint8> ptr = Pointer.fromAddress(strPtr.elementAt(0).value);
      for (int i = 0; i < bytes.length; i++) {
        bytes[i] = ptr.elementAt(i).value;
      }
      final str = utf8.decode(bytes);
      this.lib.str_destroy(ptr.cast());
      this._current = str;
      malloc.free(strPtr);
      malloc.free(lenPtr);
      return true;
    }
    throw FfiError(this.lib);
  }

  void destroy() {
    final ret = this.lib.str_iter_destroy(this.ptr);
    if (ret != 0) {
      throw FfiError(this.lib);
    }
  }
}
