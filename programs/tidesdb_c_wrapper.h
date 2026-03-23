/**
 * C-compatible wrapper for TidesDB.
 *
 * TidesDB headers use implicit void*->char* casts that are invalid in C++.
 * This wrapper provides a thin C API that the C++ benchmark can call safely.
 */
#ifndef TIDESDB_C_WRAPPER_H
#define TIDESDB_C_WRAPPER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* Opaque handles */
typedef struct tdb_wrapper      tdb_wrapper_t;
typedef struct tdb_wrapper_txn  tdb_wrapper_txn_t;
typedef struct tdb_wrapper_iter tdb_wrapper_iter_t;

#define TDB_WRAP_SUCCESS     0
#define TDB_WRAP_ERR        -1
#define TDB_WRAP_NOT_FOUND  -3

/* Sync modes — maps to TDB_SYNC_NONE / TDB_SYNC_INTERVAL / TDB_SYNC_FULL */
#define TDB_WRAP_SYNC_NONE  0
#define TDB_WRAP_SYNC_SAFE  1   /* interval-based sync (128ms) */
#define TDB_WRAP_SYNC_FULL  2

/* Database lifecycle */
tdb_wrapper_t* tdb_open(const char* path, int sync_mode);
void           tdb_close(tdb_wrapper_t* w);

/* Ensure column family exists (uses sync_mode from open) */
int tdb_ensure_cf(tdb_wrapper_t* w, const char* name);

/* Transactions */
tdb_wrapper_txn_t* tdb_txn_begin(tdb_wrapper_t* w);
tdb_wrapper_txn_t* tdb_txn_begin_read_uncommitted(tdb_wrapper_t* w);
int                tdb_txn_commit(tdb_wrapper_txn_t* txn);
void               tdb_txn_free(tdb_wrapper_txn_t* txn);

/* Key-value operations (within a transaction) */
int tdb_put(tdb_wrapper_txn_t* txn, const char* cf_name,
            const uint8_t* key, size_t key_size,
            const uint8_t* value, size_t value_size);

int tdb_get(tdb_wrapper_txn_t* txn, const char* cf_name,
            const uint8_t* key, size_t key_size,
            uint8_t** value_out, size_t* value_size_out);

int tdb_delete(tdb_wrapper_txn_t* txn, const char* cf_name,
               const uint8_t* key, size_t key_size);

/* Iterator */
tdb_wrapper_iter_t* tdb_iter_new(tdb_wrapper_txn_t* txn, const char* cf_name);
int                 tdb_iter_seek_first(tdb_wrapper_iter_t* it);
int                 tdb_iter_seek(tdb_wrapper_iter_t* it,
                                  const uint8_t* key, size_t key_size);
int                 tdb_iter_next(tdb_wrapper_iter_t* it);
int                 tdb_iter_valid(tdb_wrapper_iter_t* it);
void                tdb_iter_free(tdb_wrapper_iter_t* it);

#ifdef __cplusplus
}
#endif

#endif /* TIDESDB_C_WRAPPER_H */
