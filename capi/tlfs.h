#ifndef TLFS_H
#define TLFS_H

#include "stdbool.h"
#include "stdint.h"

typedef struct Sdk {

} Sdk;

typedef struct DocIter {

} DocIter;

typedef struct Doc {

} Doc;

typedef struct Cursor {

} Cursor;

typedef struct Causal {

} Causal;

typedef struct BoolIter {

} BoolIter;

typedef struct U64Iter {

} U64Iter;

typedef struct I64Iter {

} I64Iter;

typedef struct StrIter {

} StrIter;

typedef struct Buffer {
  uint8_t *data;
  uintptr_t len;
  uintptr_t cap;
} Buffer;

int last_error_length(void);

int error_message_utf8(char *buf, int length);

struct Sdk *sdk_create_persistent(const uint8_t *db_path_ptr,
                                  uintptr_t db_path_len,
                                  const uint8_t *package_ptr,
                                  uintptr_t package_len);

struct Sdk *sdk_create_memory(const uint8_t *package_ptr, uintptr_t package_len);

int32_t sdk_destroy(struct Sdk *sdk);

int32_t sdk_get_peerid(struct Sdk *sdk, uint8_t (*peer)[32]);

int32_t sdk_add_address(struct Sdk *sdk,
                        const uint8_t (*peer)[32],
                        const uint8_t *addr,
                        uintptr_t addr_length);

int32_t sdk_remove_address(struct Sdk *sdk,
                           const uint8_t (*peer)[32],
                           const uint8_t *addr,
                           uintptr_t addr_length);

struct DocIter *sdk_create_doc_iter(struct Sdk *sdk, const uint8_t *schema, uintptr_t schema_len);

int32_t doc_iter_next(struct DocIter *iter, uint8_t (*doc)[32]);

int32_t doc_iter_destroy(struct DocIter *iter);

struct Doc *sdk_create_doc(struct Sdk *sdk, const uint8_t *schema_ptr, uintptr_t schema_len);

struct Doc *sdk_open_doc(struct Sdk *sdk, const uint8_t (*doc)[32]);

struct Doc *sdk_add_doc(struct Sdk *sdk,
                        const uint8_t (*doc)[32],
                        const uint8_t *schema_ptr,
                        uintptr_t schema_len);

int32_t sdk_remove_doc(struct Sdk *sdk, const uint8_t (*doc)[32]);

int32_t doc_get_id(struct Doc *doc, uint8_t (*id)[32]);

struct Cursor *doc_create_cursor(struct Doc *doc);

int32_t doc_apply_causal(struct Doc *doc, struct Causal *causal);

int32_t doc_destroy(struct Doc *doc);

int32_t causal_join(struct Causal *causal, struct Causal *other);

struct Cursor *cursor_clone(struct Cursor *cursor);

int32_t cursor_flag_enabled(struct Cursor *cursor);

struct Causal *cursor_flag_enable(struct Cursor *cursor);

struct Causal *cursor_flag_disable(struct Cursor *cursor);

struct BoolIter *cursor_reg_bools(struct Cursor *cursor);

int32_t bool_iter_next(struct BoolIter *iter, bool *value);

int32_t bool_iter_destroy(struct BoolIter *iter);

struct U64Iter *cursor_reg_u64s(struct Cursor *cursor);

int32_t u64_iter_next(struct U64Iter *iter, uint64_t *value);

int32_t u64_iter_destroy(struct U64Iter *iter);

struct I64Iter *cursor_reg_i64s(struct Cursor *cursor);

int32_t i64_iter_next(struct I64Iter *iter, int64_t *value);

int32_t i64_iter_destroy(struct I64Iter *iter);

struct StrIter *cursor_reg_strs(struct Cursor *cursor);

int32_t str_iter_next(struct StrIter *iter, struct Buffer *buffer);

int32_t buffer_destroy(struct Buffer buf);

int32_t str_iter_destroy(struct StrIter *iter);

struct Causal *cursor_reg_assign_bool(struct Cursor *cursor, bool value);

struct Causal *cursor_reg_assign_u64(struct Cursor *cursor, uint64_t value);

struct Causal *cursor_reg_assign_i64(struct Cursor *cursor, int64_t value);

struct Causal *cursor_reg_assign_str(struct Cursor *cursor,
                                     const uint8_t *value,
                                     uintptr_t value_length);

int32_t cursor_struct_field(struct Cursor *cursor, const uint8_t *field, uintptr_t field_length);

int32_t cursor_map_key_bool(struct Cursor *cursor, bool key);

int32_t cursor_map_key_u64(struct Cursor *cursor, uint64_t key);

int32_t cursor_map_key_i64(struct Cursor *cursor, int64_t key);

int32_t cursor_map_key_str(struct Cursor *cursor, const uint8_t *key, uintptr_t key_length);

struct Causal *cursor_map_remove(struct Cursor *cursor);

int64_t cursor_array_length(struct Cursor *cursor);

int32_t cursor_array_index(struct Cursor *cursor, uint32_t index);

struct Causal *cursor_array_move(struct Cursor *cursor, uint32_t index);

struct Causal *cursor_array_delete(struct Cursor *cursor);

int32_t cursor_acl_can(struct Cursor *cursor, const uint8_t (*peer)[32], Permission perm);

int32_t cursor_destroy(struct Cursor *cursor);

#endif /* TLFS_H */
