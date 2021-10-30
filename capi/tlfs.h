#ifndef TLFS_H
#define TLFS_H

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

int last_error_length(void);

int error_message_utf8(char *buf, int length);

struct Sdk *sdk_create_persistent(const uint8_t *db_path_ptr,
                                  uintptr_t db_path_len,
                                  const uint8_t *package_path_ptr,
                                  uintptr_t package_path_len);

struct Sdk *sdk_create_memory(const uint8_t *package_path_ptr, uintptr_t package_path_len);

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

struct DocIter *sdk_create_doc_iter(struct Sdk *sdk);

int32_t doc_iter_next(struct DocIter *iter, uint8_t (*doc)[32]);

int32_t doc_iter_destroy(struct DocIter *iter);

struct Doc *sdk_create_doc(struct Sdk *sdk, const uint8_t *schema_ptr, uintptr_t schema_len);

struct Doc *sdk_open_doc(struct Sdk *sdk, const uint8_t (*doc)[32]);

struct Doc *sdk_add_doc(struct Sdk *sdk,
                        const uint8_t (*doc)[32],
                        const uint8_t *schema_ptr,
                        uintptr_t schema_len);

int32_t sdk_remove_doc(struct Sdk *sdk, const uint8_t (*doc)[32]);

int32_t doc_id(struct Doc *doc, uint8_t (*id)[32]);

struct Cursor *doc_cursor(struct Doc *doc);

int32_t doc_apply(struct Doc *doc, struct Causal *causal);

int32_t doc_destroy(struct Doc *doc);

#endif /* TLFS_H */
