/**
 * Compiled as C — includes tidesdb.h without C++ type-checking issues.
 * Provides a thin opaque wrapper the C++ benchmark calls into.
 */
#include "tidesdb_c_wrapper.h"

#include <stdlib.h>
#include <string.h>
#include <tidesdb.h>

#include <fcntl.h>
#include <unistd.h>

/* ---------- opaque structs ---------- */

struct tdb_wrapper
{
   tidesdb_t* db;
   int        sync_mode;
   char*      path;
};

struct tdb_wrapper_txn
{
   tidesdb_txn_t* txn;
   tdb_wrapper_t* w;  /* back-pointer for cf lookup */
};

struct tdb_wrapper_iter
{
   tidesdb_iter_t* iter;
};

/* ---------- database ---------- */

static int wrap_to_tdb_sync(int wrap_mode)
{
   switch (wrap_mode)
   {
      case TDB_WRAP_SYNC_SAFE: return TDB_SYNC_INTERVAL;
      case TDB_WRAP_SYNC_FULL: return TDB_SYNC_FULL;
      default:                 return TDB_SYNC_NONE;
   }
}

tdb_wrapper_t* tdb_open(const char* path, int sync_mode)
{
   tidesdb_config_t config = tidesdb_default_config();
   config.db_path          = (char*)path;
   config.log_level        = TDB_LOG_NONE;

   tidesdb_t* db = NULL;
   if (tidesdb_open(&config, &db) != TDB_SUCCESS)
      return NULL;

   tdb_wrapper_t* w = (tdb_wrapper_t*)malloc(sizeof(*w));
   w->db            = db;
   w->sync_mode     = sync_mode;
   w->path          = strdup(path);
   return w;
}

void tdb_close(tdb_wrapper_t* w)
{
   if (!w)
      return;
   tidesdb_close(w->db);
   free(w->path);
   free(w);
}

int tdb_ensure_cf(tdb_wrapper_t* w, const char* name)
{
   if (tidesdb_get_column_family(w->db, name))
      return TDB_WRAP_SUCCESS;

   tidesdb_column_family_config_t cfg = tidesdb_default_column_family_config();
   cfg.compression_algorithm = TDB_COMPRESS_NONE;
   cfg.sync_mode = wrap_to_tdb_sync(w->sync_mode);
   return tidesdb_create_column_family(w->db, name, &cfg) == TDB_SUCCESS
              ? TDB_WRAP_SUCCESS
              : TDB_WRAP_ERR;
}

/* ---------- fullsync helper (macOS) ---------- */

static void fullsync_flush(const char* path)
{
#ifdef __APPLE__
   /* F_FULLFSYNC flushes the entire disk write cache — any fd will do */
   int fd = open(path, O_RDONLY);
   if (fd >= 0)
   {
      fcntl(fd, F_FULLFSYNC);
      close(fd);
   }
#else
   (void)path;
#endif
}

/* ---------- transactions ---------- */

tdb_wrapper_txn_t* tdb_txn_begin(tdb_wrapper_t* w)
{
   tidesdb_txn_t* txn = NULL;
   if (tidesdb_txn_begin(w->db, &txn) != TDB_SUCCESS)
      return NULL;

   tdb_wrapper_txn_t* wt = (tdb_wrapper_txn_t*)malloc(sizeof(*wt));
   wt->txn               = txn;
   wt->w                 = w;
   return wt;
}

tdb_wrapper_txn_t* tdb_txn_begin_read_uncommitted(tdb_wrapper_t* w)
{
   tidesdb_txn_t* txn = NULL;
   if (tidesdb_txn_begin_with_isolation(w->db, TDB_ISOLATION_READ_UNCOMMITTED, &txn) != TDB_SUCCESS)
      return NULL;

   tdb_wrapper_txn_t* wt = (tdb_wrapper_txn_t*)malloc(sizeof(*wt));
   wt->txn               = txn;
   wt->w                 = w;
   return wt;
}

int tdb_txn_commit(tdb_wrapper_txn_t* wt)
{
   if (tidesdb_txn_commit(wt->txn) != TDB_SUCCESS)
      return TDB_WRAP_ERR;

   if (wt->w->sync_mode == TDB_WRAP_SYNC_FULL)
      fullsync_flush(wt->w->path);

   return TDB_WRAP_SUCCESS;
}

void tdb_txn_free(tdb_wrapper_txn_t* wt)
{
   if (!wt)
      return;
   tidesdb_txn_free(wt->txn);
   free(wt);
}

/* ---------- key-value ops ---------- */

int tdb_put(tdb_wrapper_txn_t* wt, const char* cf_name,
            const uint8_t* key, size_t key_size,
            const uint8_t* value, size_t value_size)
{
   tidesdb_column_family_t* cf = tidesdb_get_column_family(wt->w->db, cf_name);
   if (!cf)
      return TDB_WRAP_ERR;
   return tidesdb_txn_put(wt->txn, cf, key, key_size, value, value_size, 0) == TDB_SUCCESS
              ? TDB_WRAP_SUCCESS
              : TDB_WRAP_ERR;
}

int tdb_get(tdb_wrapper_txn_t* wt, const char* cf_name,
            const uint8_t* key, size_t key_size,
            uint8_t** value_out, size_t* value_size_out)
{
   tidesdb_column_family_t* cf = tidesdb_get_column_family(wt->w->db, cf_name);
   if (!cf)
      return TDB_WRAP_ERR;
   int rc = tidesdb_txn_get(wt->txn, cf, key, key_size, value_out, value_size_out);
   if (rc == TDB_SUCCESS)
      return TDB_WRAP_SUCCESS;
   return TDB_WRAP_NOT_FOUND;
}

int tdb_delete(tdb_wrapper_txn_t* wt, const char* cf_name,
               const uint8_t* key, size_t key_size)
{
   tidesdb_column_family_t* cf = tidesdb_get_column_family(wt->w->db, cf_name);
   if (!cf)
      return TDB_WRAP_ERR;
   return tidesdb_txn_delete(wt->txn, cf, key, key_size) == TDB_SUCCESS
              ? TDB_WRAP_SUCCESS
              : TDB_WRAP_ERR;
}

/* ---------- iterator ---------- */

tdb_wrapper_iter_t* tdb_iter_new(tdb_wrapper_txn_t* wt, const char* cf_name)
{
   tidesdb_column_family_t* cf = tidesdb_get_column_family(wt->w->db, cf_name);
   if (!cf)
      return NULL;

   tidesdb_iter_t* iter = NULL;
   if (tidesdb_iter_new(wt->txn, cf, &iter) != TDB_SUCCESS)
      return NULL;

   tdb_wrapper_iter_t* wi = (tdb_wrapper_iter_t*)malloc(sizeof(*wi));
   wi->iter               = iter;
   return wi;
}

int tdb_iter_seek_first(tdb_wrapper_iter_t* wi)
{
   return tidesdb_iter_seek_to_first(wi->iter) == TDB_SUCCESS ? TDB_WRAP_SUCCESS : TDB_WRAP_ERR;
}

int tdb_iter_seek(tdb_wrapper_iter_t* wi, const uint8_t* key, size_t key_size)
{
   return tidesdb_iter_seek(wi->iter, key, key_size) == TDB_SUCCESS ? TDB_WRAP_SUCCESS : TDB_WRAP_ERR;
}

int tdb_iter_next(tdb_wrapper_iter_t* wi)
{
   return tidesdb_iter_next(wi->iter) == TDB_SUCCESS ? TDB_WRAP_SUCCESS : TDB_WRAP_ERR;
}

int tdb_iter_valid(tdb_wrapper_iter_t* wi)
{
   return tidesdb_iter_valid(wi->iter);
}

int tdb_iter_key(tdb_wrapper_iter_t* wi, uint8_t** key_out, size_t* key_size_out)
{
   return tidesdb_iter_key(wi->iter, key_out, key_size_out) == TDB_SUCCESS
              ? TDB_WRAP_SUCCESS
              : TDB_WRAP_ERR;
}

int tdb_iter_value(tdb_wrapper_iter_t* wi, uint8_t** value_out, size_t* value_size_out)
{
   return tidesdb_iter_value(wi->iter, value_out, value_size_out) == TDB_SUCCESS
              ? TDB_WRAP_SUCCESS
              : TDB_WRAP_ERR;
}

void tdb_iter_free(tdb_wrapper_iter_t* wi)
{
   if (!wi)
      return;
   tidesdb_iter_free(wi->iter);
   free(wi);
}
