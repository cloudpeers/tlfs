// AUTO GENERATED FILE, DO NOT EDIT.
//
// Generated by `package:ffigen`.
import 'dart:ffi' as ffi;

class NativeLibrary {
  /// Holds the symbol lookup function.
  final ffi.Pointer<T> Function<T extends ffi.NativeType>(String symbolName)
      _lookup;

  /// The symbols are looked up in [dynamicLibrary].
  NativeLibrary(ffi.DynamicLibrary dynamicLibrary)
      : _lookup = dynamicLibrary.lookup;

  /// The symbols are looked up with [lookup].
  NativeLibrary.fromLookup(
      ffi.Pointer<T> Function<T extends ffi.NativeType>(String symbolName)
          lookup)
      : _lookup = lookup;

  int last_error_length() {
    return _last_error_length();
  }

  late final _last_error_lengthPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function()>>('last_error_length');
  late final _last_error_length =
      _last_error_lengthPtr.asFunction<int Function()>();

  int error_message_utf8(
    ffi.Pointer<ffi.Int8> buf,
    int length,
  ) {
    return _error_message_utf8(
      buf,
      length,
    );
  }

  late final _error_message_utf8Ptr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<ffi.Int8>, ffi.Int32)>>('error_message_utf8');
  late final _error_message_utf8 = _error_message_utf8Ptr
      .asFunction<int Function(ffi.Pointer<ffi.Int8>, int)>();

  ffi.Pointer<Sdk> sdk_create_persistent(
    ffi.Pointer<ffi.Uint8> db_path_ptr,
    int db_path_len,
    ffi.Pointer<ffi.Uint8> package_ptr,
    int package_len,
  ) {
    return _sdk_create_persistent(
      db_path_ptr,
      db_path_len,
      package_ptr,
      package_len,
    );
  }

  late final _sdk_create_persistentPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Sdk> Function(ffi.Pointer<ffi.Uint8>, ffi.Uint64,
              ffi.Pointer<ffi.Uint8>, ffi.Uint64)>>('sdk_create_persistent');
  late final _sdk_create_persistent = _sdk_create_persistentPtr.asFunction<
      ffi.Pointer<Sdk> Function(
          ffi.Pointer<ffi.Uint8>, int, ffi.Pointer<ffi.Uint8>, int)>();

  ffi.Pointer<Sdk> sdk_create_memory(
    ffi.Pointer<ffi.Uint8> package_ptr,
    int package_len,
  ) {
    return _sdk_create_memory(
      package_ptr,
      package_len,
    );
  }

  late final _sdk_create_memoryPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Sdk> Function(
              ffi.Pointer<ffi.Uint8>, ffi.Uint64)>>('sdk_create_memory');
  late final _sdk_create_memory = _sdk_create_memoryPtr
      .asFunction<ffi.Pointer<Sdk> Function(ffi.Pointer<ffi.Uint8>, int)>();

  int sdk_destroy(
    ffi.Pointer<Sdk> sdk,
  ) {
    return _sdk_destroy(
      sdk,
    );
  }

  late final _sdk_destroyPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function(ffi.Pointer<Sdk>)>>(
          'sdk_destroy');
  late final _sdk_destroy =
      _sdk_destroyPtr.asFunction<int Function(ffi.Pointer<Sdk>)>();

  int sdk_get_peerid(
    ffi.Pointer<Sdk> sdk,
    ffi.Pointer<ffi.Pointer<ffi.Uint8>> peer,
  ) {
    return _sdk_get_peerid(
      sdk,
      peer,
    );
  }

  late final _sdk_get_peeridPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(ffi.Pointer<Sdk>,
              ffi.Pointer<ffi.Pointer<ffi.Uint8>>)>>('sdk_get_peerid');
  late final _sdk_get_peerid = _sdk_get_peeridPtr.asFunction<
      int Function(ffi.Pointer<Sdk>, ffi.Pointer<ffi.Pointer<ffi.Uint8>>)>();

  int sdk_add_address(
    ffi.Pointer<Sdk> sdk,
    ffi.Pointer<ffi.Pointer<ffi.Uint8>> peer,
    ffi.Pointer<ffi.Uint8> addr,
    int addr_length,
  ) {
    return _sdk_add_address(
      sdk,
      peer,
      addr,
      addr_length,
    );
  }

  late final _sdk_add_addressPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<Sdk>,
              ffi.Pointer<ffi.Pointer<ffi.Uint8>>,
              ffi.Pointer<ffi.Uint8>,
              ffi.Uint64)>>('sdk_add_address');
  late final _sdk_add_address = _sdk_add_addressPtr.asFunction<
      int Function(ffi.Pointer<Sdk>, ffi.Pointer<ffi.Pointer<ffi.Uint8>>,
          ffi.Pointer<ffi.Uint8>, int)>();

  int sdk_remove_address(
    ffi.Pointer<Sdk> sdk,
    ffi.Pointer<ffi.Pointer<ffi.Uint8>> peer,
    ffi.Pointer<ffi.Uint8> addr,
    int addr_length,
  ) {
    return _sdk_remove_address(
      sdk,
      peer,
      addr,
      addr_length,
    );
  }

  late final _sdk_remove_addressPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<Sdk>,
              ffi.Pointer<ffi.Pointer<ffi.Uint8>>,
              ffi.Pointer<ffi.Uint8>,
              ffi.Uint64)>>('sdk_remove_address');
  late final _sdk_remove_address = _sdk_remove_addressPtr.asFunction<
      int Function(ffi.Pointer<Sdk>, ffi.Pointer<ffi.Pointer<ffi.Uint8>>,
          ffi.Pointer<ffi.Uint8>, int)>();

  ffi.Pointer<DocIter> sdk_create_doc_iter(
    ffi.Pointer<Sdk> sdk,
    ffi.Pointer<ffi.Uint8> schema,
    int schema_len,
  ) {
    return _sdk_create_doc_iter(
      sdk,
      schema,
      schema_len,
    );
  }

  late final _sdk_create_doc_iterPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<DocIter> Function(ffi.Pointer<Sdk>,
              ffi.Pointer<ffi.Uint8>, ffi.Uint64)>>('sdk_create_doc_iter');
  late final _sdk_create_doc_iter = _sdk_create_doc_iterPtr.asFunction<
      ffi.Pointer<DocIter> Function(
          ffi.Pointer<Sdk>, ffi.Pointer<ffi.Uint8>, int)>();

  int doc_iter_next(
    ffi.Pointer<DocIter> iter,
    ffi.Pointer<ffi.Pointer<ffi.Uint8>> doc,
  ) {
    return _doc_iter_next(
      iter,
      doc,
    );
  }

  late final _doc_iter_nextPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(ffi.Pointer<DocIter>,
              ffi.Pointer<ffi.Pointer<ffi.Uint8>>)>>('doc_iter_next');
  late final _doc_iter_next = _doc_iter_nextPtr.asFunction<
      int Function(
          ffi.Pointer<DocIter>, ffi.Pointer<ffi.Pointer<ffi.Uint8>>)>();

  int doc_iter_destroy(
    ffi.Pointer<DocIter> iter,
  ) {
    return _doc_iter_destroy(
      iter,
    );
  }

  late final _doc_iter_destroyPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function(ffi.Pointer<DocIter>)>>(
          'doc_iter_destroy');
  late final _doc_iter_destroy =
      _doc_iter_destroyPtr.asFunction<int Function(ffi.Pointer<DocIter>)>();

  ffi.Pointer<Doc> sdk_create_doc(
    ffi.Pointer<Sdk> sdk,
    ffi.Pointer<ffi.Uint8> schema_ptr,
    int schema_len,
  ) {
    return _sdk_create_doc(
      sdk,
      schema_ptr,
      schema_len,
    );
  }

  late final _sdk_create_docPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Doc> Function(ffi.Pointer<Sdk>, ffi.Pointer<ffi.Uint8>,
              ffi.Uint64)>>('sdk_create_doc');
  late final _sdk_create_doc = _sdk_create_docPtr.asFunction<
      ffi.Pointer<Doc> Function(
          ffi.Pointer<Sdk>, ffi.Pointer<ffi.Uint8>, int)>();

  ffi.Pointer<Doc> sdk_open_doc(
    ffi.Pointer<Sdk> sdk,
    ffi.Pointer<ffi.Pointer<ffi.Uint8>> doc,
  ) {
    return _sdk_open_doc(
      sdk,
      doc,
    );
  }

  late final _sdk_open_docPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Doc> Function(ffi.Pointer<Sdk>,
              ffi.Pointer<ffi.Pointer<ffi.Uint8>>)>>('sdk_open_doc');
  late final _sdk_open_doc = _sdk_open_docPtr.asFunction<
      ffi.Pointer<Doc> Function(
          ffi.Pointer<Sdk>, ffi.Pointer<ffi.Pointer<ffi.Uint8>>)>();

  ffi.Pointer<Doc> sdk_add_doc(
    ffi.Pointer<Sdk> sdk,
    ffi.Pointer<ffi.Pointer<ffi.Uint8>> doc,
    ffi.Pointer<ffi.Uint8> schema_ptr,
    int schema_len,
  ) {
    return _sdk_add_doc(
      sdk,
      doc,
      schema_ptr,
      schema_len,
    );
  }

  late final _sdk_add_docPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Doc> Function(
              ffi.Pointer<Sdk>,
              ffi.Pointer<ffi.Pointer<ffi.Uint8>>,
              ffi.Pointer<ffi.Uint8>,
              ffi.Uint64)>>('sdk_add_doc');
  late final _sdk_add_doc = _sdk_add_docPtr.asFunction<
      ffi.Pointer<Doc> Function(ffi.Pointer<Sdk>,
          ffi.Pointer<ffi.Pointer<ffi.Uint8>>, ffi.Pointer<ffi.Uint8>, int)>();

  int sdk_remove_doc(
    ffi.Pointer<Sdk> sdk,
    ffi.Pointer<ffi.Pointer<ffi.Uint8>> doc,
  ) {
    return _sdk_remove_doc(
      sdk,
      doc,
    );
  }

  late final _sdk_remove_docPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(ffi.Pointer<Sdk>,
              ffi.Pointer<ffi.Pointer<ffi.Uint8>>)>>('sdk_remove_doc');
  late final _sdk_remove_doc = _sdk_remove_docPtr.asFunction<
      int Function(ffi.Pointer<Sdk>, ffi.Pointer<ffi.Pointer<ffi.Uint8>>)>();

  int doc_get_id(
    ffi.Pointer<Doc> doc,
    ffi.Pointer<ffi.Pointer<ffi.Uint8>> id,
  ) {
    return _doc_get_id(
      doc,
      id,
    );
  }

  late final _doc_get_idPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(ffi.Pointer<Doc>,
              ffi.Pointer<ffi.Pointer<ffi.Uint8>>)>>('doc_get_id');
  late final _doc_get_id = _doc_get_idPtr.asFunction<
      int Function(ffi.Pointer<Doc>, ffi.Pointer<ffi.Pointer<ffi.Uint8>>)>();

  ffi.Pointer<Cursor> doc_create_cursor(
    ffi.Pointer<Doc> doc,
  ) {
    return _doc_create_cursor(
      doc,
    );
  }

  late final _doc_create_cursorPtr = _lookup<
          ffi.NativeFunction<ffi.Pointer<Cursor> Function(ffi.Pointer<Doc>)>>(
      'doc_create_cursor');
  late final _doc_create_cursor = _doc_create_cursorPtr
      .asFunction<ffi.Pointer<Cursor> Function(ffi.Pointer<Doc>)>();

  int doc_apply_causal(
    ffi.Pointer<Doc> doc,
    ffi.Pointer<Causal> causal,
  ) {
    return _doc_apply_causal(
      doc,
      causal,
    );
  }

  late final _doc_apply_causalPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<Doc>, ffi.Pointer<Causal>)>>('doc_apply_causal');
  late final _doc_apply_causal = _doc_apply_causalPtr
      .asFunction<int Function(ffi.Pointer<Doc>, ffi.Pointer<Causal>)>();

  int doc_destroy(
    ffi.Pointer<Doc> doc,
  ) {
    return _doc_destroy(
      doc,
    );
  }

  late final _doc_destroyPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function(ffi.Pointer<Doc>)>>(
          'doc_destroy');
  late final _doc_destroy =
      _doc_destroyPtr.asFunction<int Function(ffi.Pointer<Doc>)>();

  int causal_join(
    ffi.Pointer<Causal> causal,
    ffi.Pointer<Causal> other,
  ) {
    return _causal_join(
      causal,
      other,
    );
  }

  late final _causal_joinPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<Causal>, ffi.Pointer<Causal>)>>('causal_join');
  late final _causal_join = _causal_joinPtr
      .asFunction<int Function(ffi.Pointer<Causal>, ffi.Pointer<Causal>)>();

  ffi.Pointer<Cursor> cursor_clone(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_clone(
      cursor,
    );
  }

  late final _cursor_clonePtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Cursor> Function(ffi.Pointer<Cursor>)>>('cursor_clone');
  late final _cursor_clone = _cursor_clonePtr
      .asFunction<ffi.Pointer<Cursor> Function(ffi.Pointer<Cursor>)>();

  int cursor_flag_enabled(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_flag_enabled(
      cursor,
    );
  }

  late final _cursor_flag_enabledPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function(ffi.Pointer<Cursor>)>>(
          'cursor_flag_enabled');
  late final _cursor_flag_enabled =
      _cursor_flag_enabledPtr.asFunction<int Function(ffi.Pointer<Cursor>)>();

  ffi.Pointer<Causal> cursor_flag_enable(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_flag_enable(
      cursor,
    );
  }

  late final _cursor_flag_enablePtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Causal> Function(
              ffi.Pointer<Cursor>)>>('cursor_flag_enable');
  late final _cursor_flag_enable = _cursor_flag_enablePtr
      .asFunction<ffi.Pointer<Causal> Function(ffi.Pointer<Cursor>)>();

  ffi.Pointer<Causal> cursor_flag_disable(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_flag_disable(
      cursor,
    );
  }

  late final _cursor_flag_disablePtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Causal> Function(
              ffi.Pointer<Cursor>)>>('cursor_flag_disable');
  late final _cursor_flag_disable = _cursor_flag_disablePtr
      .asFunction<ffi.Pointer<Causal> Function(ffi.Pointer<Cursor>)>();

  ffi.Pointer<BoolIter> cursor_reg_bools(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_reg_bools(
      cursor,
    );
  }

  late final _cursor_reg_boolsPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<BoolIter> Function(
              ffi.Pointer<Cursor>)>>('cursor_reg_bools');
  late final _cursor_reg_bools = _cursor_reg_boolsPtr
      .asFunction<ffi.Pointer<BoolIter> Function(ffi.Pointer<Cursor>)>();

  int bool_iter_next(
    ffi.Pointer<BoolIter> iter,
    ffi.Pointer<ffi.Uint8> value,
  ) {
    return _bool_iter_next(
      iter,
      value,
    );
  }

  late final _bool_iter_nextPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(ffi.Pointer<BoolIter>,
              ffi.Pointer<ffi.Uint8>)>>('bool_iter_next');
  late final _bool_iter_next = _bool_iter_nextPtr.asFunction<
      int Function(ffi.Pointer<BoolIter>, ffi.Pointer<ffi.Uint8>)>();

  int bool_iter_destroy(
    ffi.Pointer<BoolIter> iter,
  ) {
    return _bool_iter_destroy(
      iter,
    );
  }

  late final _bool_iter_destroyPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function(ffi.Pointer<BoolIter>)>>(
          'bool_iter_destroy');
  late final _bool_iter_destroy =
      _bool_iter_destroyPtr.asFunction<int Function(ffi.Pointer<BoolIter>)>();

  ffi.Pointer<U64Iter> cursor_reg_u64s(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_reg_u64s(
      cursor,
    );
  }

  late final _cursor_reg_u64sPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<U64Iter> Function(
              ffi.Pointer<Cursor>)>>('cursor_reg_u64s');
  late final _cursor_reg_u64s = _cursor_reg_u64sPtr
      .asFunction<ffi.Pointer<U64Iter> Function(ffi.Pointer<Cursor>)>();

  int u64_iter_next(
    ffi.Pointer<U64Iter> iter,
    ffi.Pointer<ffi.Uint64> value,
  ) {
    return _u64_iter_next(
      iter,
      value,
    );
  }

  late final _u64_iter_nextPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<U64Iter>, ffi.Pointer<ffi.Uint64>)>>('u64_iter_next');
  late final _u64_iter_next = _u64_iter_nextPtr.asFunction<
      int Function(ffi.Pointer<U64Iter>, ffi.Pointer<ffi.Uint64>)>();

  int u64_iter_destroy(
    ffi.Pointer<U64Iter> iter,
  ) {
    return _u64_iter_destroy(
      iter,
    );
  }

  late final _u64_iter_destroyPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function(ffi.Pointer<U64Iter>)>>(
          'u64_iter_destroy');
  late final _u64_iter_destroy =
      _u64_iter_destroyPtr.asFunction<int Function(ffi.Pointer<U64Iter>)>();

  ffi.Pointer<I64Iter> cursor_reg_i64s(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_reg_i64s(
      cursor,
    );
  }

  late final _cursor_reg_i64sPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<I64Iter> Function(
              ffi.Pointer<Cursor>)>>('cursor_reg_i64s');
  late final _cursor_reg_i64s = _cursor_reg_i64sPtr
      .asFunction<ffi.Pointer<I64Iter> Function(ffi.Pointer<Cursor>)>();

  int i64_iter_next(
    ffi.Pointer<I64Iter> iter,
    ffi.Pointer<ffi.Int64> value,
  ) {
    return _i64_iter_next(
      iter,
      value,
    );
  }

  late final _i64_iter_nextPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<I64Iter>, ffi.Pointer<ffi.Int64>)>>('i64_iter_next');
  late final _i64_iter_next = _i64_iter_nextPtr
      .asFunction<int Function(ffi.Pointer<I64Iter>, ffi.Pointer<ffi.Int64>)>();

  int i64_iter_destroy(
    ffi.Pointer<I64Iter> iter,
  ) {
    return _i64_iter_destroy(
      iter,
    );
  }

  late final _i64_iter_destroyPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function(ffi.Pointer<I64Iter>)>>(
          'i64_iter_destroy');
  late final _i64_iter_destroy =
      _i64_iter_destroyPtr.asFunction<int Function(ffi.Pointer<I64Iter>)>();

  ffi.Pointer<StrIter> cursor_reg_strs(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_reg_strs(
      cursor,
    );
  }

  late final _cursor_reg_strsPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<StrIter> Function(
              ffi.Pointer<Cursor>)>>('cursor_reg_strs');
  late final _cursor_reg_strs = _cursor_reg_strsPtr
      .asFunction<ffi.Pointer<StrIter> Function(ffi.Pointer<Cursor>)>();

  int str_iter_next(
    ffi.Pointer<StrIter> iter,
    ffi.Pointer<ffi.Pointer<ffi.Uint8>> value,
    ffi.Pointer<ffi.Uint64> value_len,
  ) {
    return _str_iter_next(
      iter,
      value,
      value_len,
    );
  }

  late final _str_iter_nextPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<StrIter>,
              ffi.Pointer<ffi.Pointer<ffi.Uint8>>,
              ffi.Pointer<ffi.Uint64>)>>('str_iter_next');
  late final _str_iter_next = _str_iter_nextPtr.asFunction<
      int Function(ffi.Pointer<StrIter>, ffi.Pointer<ffi.Pointer<ffi.Uint8>>,
          ffi.Pointer<ffi.Uint64>)>();

  int str_destroy(
    ffi.Pointer<ffi.Uint8> ptr,
  ) {
    return _str_destroy(
      ptr,
    );
  }

  late final _str_destroyPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function(ffi.Pointer<ffi.Uint8>)>>(
          'str_destroy');
  late final _str_destroy =
      _str_destroyPtr.asFunction<int Function(ffi.Pointer<ffi.Uint8>)>();

  int str_iter_destroy(
    ffi.Pointer<StrIter> iter,
  ) {
    return _str_iter_destroy(
      iter,
    );
  }

  late final _str_iter_destroyPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function(ffi.Pointer<StrIter>)>>(
          'str_iter_destroy');
  late final _str_iter_destroy =
      _str_iter_destroyPtr.asFunction<int Function(ffi.Pointer<StrIter>)>();

  ffi.Pointer<Causal> cursor_reg_assign_bool(
    ffi.Pointer<Cursor> cursor,
    bool value,
  ) {
    return _cursor_reg_assign_bool(
      cursor,
      value ? 1 : 0,
    );
  }

  late final _cursor_reg_assign_boolPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Causal> Function(
              ffi.Pointer<Cursor>, ffi.Uint8)>>('cursor_reg_assign_bool');
  late final _cursor_reg_assign_bool = _cursor_reg_assign_boolPtr
      .asFunction<ffi.Pointer<Causal> Function(ffi.Pointer<Cursor>, int)>();

  ffi.Pointer<Causal> cursor_reg_assign_u64(
    ffi.Pointer<Cursor> cursor,
    int value,
  ) {
    return _cursor_reg_assign_u64(
      cursor,
      value,
    );
  }

  late final _cursor_reg_assign_u64Ptr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Causal> Function(
              ffi.Pointer<Cursor>, ffi.Uint64)>>('cursor_reg_assign_u64');
  late final _cursor_reg_assign_u64 = _cursor_reg_assign_u64Ptr
      .asFunction<ffi.Pointer<Causal> Function(ffi.Pointer<Cursor>, int)>();

  ffi.Pointer<Causal> cursor_reg_assign_i64(
    ffi.Pointer<Cursor> cursor,
    int value,
  ) {
    return _cursor_reg_assign_i64(
      cursor,
      value,
    );
  }

  late final _cursor_reg_assign_i64Ptr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Causal> Function(
              ffi.Pointer<Cursor>, ffi.Int64)>>('cursor_reg_assign_i64');
  late final _cursor_reg_assign_i64 = _cursor_reg_assign_i64Ptr
      .asFunction<ffi.Pointer<Causal> Function(ffi.Pointer<Cursor>, int)>();

  ffi.Pointer<Causal> cursor_reg_assign_str(
    ffi.Pointer<Cursor> cursor,
    ffi.Pointer<ffi.Uint8> value,
    int value_length,
  ) {
    return _cursor_reg_assign_str(
      cursor,
      value,
      value_length,
    );
  }

  late final _cursor_reg_assign_strPtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Causal> Function(ffi.Pointer<Cursor>,
              ffi.Pointer<ffi.Uint8>, ffi.Uint64)>>('cursor_reg_assign_str');
  late final _cursor_reg_assign_str = _cursor_reg_assign_strPtr.asFunction<
      ffi.Pointer<Causal> Function(
          ffi.Pointer<Cursor>, ffi.Pointer<ffi.Uint8>, int)>();

  int cursor_struct_field(
    ffi.Pointer<Cursor> cursor,
    ffi.Pointer<ffi.Uint8> field,
    int field_length,
  ) {
    return _cursor_struct_field(
      cursor,
      field,
      field_length,
    );
  }

  late final _cursor_struct_fieldPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(ffi.Pointer<Cursor>, ffi.Pointer<ffi.Uint8>,
              ffi.Uint64)>>('cursor_struct_field');
  late final _cursor_struct_field = _cursor_struct_fieldPtr.asFunction<
      int Function(ffi.Pointer<Cursor>, ffi.Pointer<ffi.Uint8>, int)>();

  int cursor_map_key_bool(
    ffi.Pointer<Cursor> cursor,
    bool key,
  ) {
    return _cursor_map_key_bool(
      cursor,
      key ? 1 : 0,
    );
  }

  late final _cursor_map_key_boolPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<Cursor>, ffi.Uint8)>>('cursor_map_key_bool');
  late final _cursor_map_key_bool = _cursor_map_key_boolPtr
      .asFunction<int Function(ffi.Pointer<Cursor>, int)>();

  int cursor_map_key_u64(
    ffi.Pointer<Cursor> cursor,
    int key,
  ) {
    return _cursor_map_key_u64(
      cursor,
      key,
    );
  }

  late final _cursor_map_key_u64Ptr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<Cursor>, ffi.Uint64)>>('cursor_map_key_u64');
  late final _cursor_map_key_u64 = _cursor_map_key_u64Ptr
      .asFunction<int Function(ffi.Pointer<Cursor>, int)>();

  int cursor_map_key_i64(
    ffi.Pointer<Cursor> cursor,
    int key,
  ) {
    return _cursor_map_key_i64(
      cursor,
      key,
    );
  }

  late final _cursor_map_key_i64Ptr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<Cursor>, ffi.Int64)>>('cursor_map_key_i64');
  late final _cursor_map_key_i64 = _cursor_map_key_i64Ptr
      .asFunction<int Function(ffi.Pointer<Cursor>, int)>();

  int cursor_map_key_str(
    ffi.Pointer<Cursor> cursor,
    ffi.Pointer<ffi.Uint8> key,
    int key_length,
  ) {
    return _cursor_map_key_str(
      cursor,
      key,
      key_length,
    );
  }

  late final _cursor_map_key_strPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(ffi.Pointer<Cursor>, ffi.Pointer<ffi.Uint8>,
              ffi.Uint64)>>('cursor_map_key_str');
  late final _cursor_map_key_str = _cursor_map_key_strPtr.asFunction<
      int Function(ffi.Pointer<Cursor>, ffi.Pointer<ffi.Uint8>, int)>();

  ffi.Pointer<Causal> cursor_map_remove(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_map_remove(
      cursor,
    );
  }

  late final _cursor_map_removePtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Causal> Function(
              ffi.Pointer<Cursor>)>>('cursor_map_remove');
  late final _cursor_map_remove = _cursor_map_removePtr
      .asFunction<ffi.Pointer<Causal> Function(ffi.Pointer<Cursor>)>();

  int cursor_array_index(
    ffi.Pointer<Cursor> cursor,
    int index,
  ) {
    return _cursor_array_index(
      cursor,
      index,
    );
  }

  late final _cursor_array_indexPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<Cursor>, ffi.Uint32)>>('cursor_array_index');
  late final _cursor_array_index = _cursor_array_indexPtr
      .asFunction<int Function(ffi.Pointer<Cursor>, int)>();

  ffi.Pointer<Causal> cursor_array_move(
    ffi.Pointer<Cursor> cursor,
    int index,
  ) {
    return _cursor_array_move(
      cursor,
      index,
    );
  }

  late final _cursor_array_movePtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Causal> Function(
              ffi.Pointer<Cursor>, ffi.Uint32)>>('cursor_array_move');
  late final _cursor_array_move = _cursor_array_movePtr
      .asFunction<ffi.Pointer<Causal> Function(ffi.Pointer<Cursor>, int)>();

  ffi.Pointer<Causal> cursor_array_delete(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_array_delete(
      cursor,
    );
  }

  late final _cursor_array_deletePtr = _lookup<
      ffi.NativeFunction<
          ffi.Pointer<Causal> Function(
              ffi.Pointer<Cursor>)>>('cursor_array_delete');
  late final _cursor_array_delete = _cursor_array_deletePtr
      .asFunction<ffi.Pointer<Causal> Function(ffi.Pointer<Cursor>)>();

  int cursor_acl_can(
    ffi.Pointer<Cursor> cursor,
    ffi.Pointer<ffi.Pointer<ffi.Uint8>> peer,
    int perm,
  ) {
    return _cursor_acl_can(
      cursor,
      peer,
      perm,
    );
  }

  late final _cursor_acl_canPtr = _lookup<
      ffi.NativeFunction<
          ffi.Int32 Function(
              ffi.Pointer<Cursor>,
              ffi.Pointer<ffi.Pointer<ffi.Uint8>>,
              ffi.Int32)>>('cursor_acl_can');
  late final _cursor_acl_can = _cursor_acl_canPtr.asFunction<
      int Function(
          ffi.Pointer<Cursor>, ffi.Pointer<ffi.Pointer<ffi.Uint8>>, int)>();

  int cursor_destroy(
    ffi.Pointer<Cursor> cursor,
  ) {
    return _cursor_destroy(
      cursor,
    );
  }

  late final _cursor_destroyPtr =
      _lookup<ffi.NativeFunction<ffi.Int32 Function(ffi.Pointer<Cursor>)>>(
          'cursor_destroy');
  late final _cursor_destroy =
      _cursor_destroyPtr.asFunction<int Function(ffi.Pointer<Cursor>)>();
}

class Sdk extends ffi.Opaque {}

class DocIter extends ffi.Opaque {}

class Doc extends ffi.Opaque {}

class Cursor extends ffi.Opaque {}

class Causal extends ffi.Opaque {}

class BoolIter extends ffi.Opaque {}

class U64Iter extends ffi.Opaque {}

class I64Iter extends ffi.Opaque {}

class StrIter extends ffi.Opaque {}
