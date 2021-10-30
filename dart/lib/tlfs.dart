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

  factory Sdk.memory(String packagePath) {
    final lib = ffi.NativeLibrary(_open());
    final packagePathPtr = packagePath.toNativeUtf8();
    final ret =
        lib.sdk_create_memory(packagePathPtr.cast(), packagePathPtr.length);
    if (ret == Pointer.fromAddress(0)) {
      throw FfiError(lib);
    }
    return Sdk(lib, ret);
  }

  factory Sdk.persistent(String dbPath, String packagePath) {
    final lib = ffi.NativeLibrary(_open());
    final dbPathPtr = dbPath.toNativeUtf8();
    final packagePathPtr = packagePath.toNativeUtf8();
    final ret = lib.sdk_create_persistent(dbPathPtr.cast(), dbPathPtr.length,
        packagePathPtr.cast(), packagePathPtr.length);
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
    final ret = this.lib.doc_id(this.ptr, docPtr.cast());
    if (ret != 32) {
      throw FfiError(this.lib);
    }
    return DocId(docPtr.asTypedList(32));
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
