/** @file mdbx.h
 *  @brief PsiTri-backed MDBX-compatible C API.
 *
 *  Drop-in replacement for libmdbx's C API. Applications that use the
 *  MDBX C interface can switch to psitri by linking against psitrimdbx
 *  instead of libmdbx. Not all flags/features are supported — unsupported
 *  options are silently accepted or return MDBX_ENOSYS.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque handle types ──────────────────────────────────────────── */

typedef struct MDBX_env    MDBX_env;
typedef struct MDBX_txn    MDBX_txn;
typedef struct MDBX_cursor MDBX_cursor;
typedef uint32_t           MDBX_dbi;

/* ── Key/value slice ──────────────────────────────────────────────── */

typedef struct MDBX_val {
   void*  iov_base;
   size_t iov_len;
} MDBX_val;

/* ── Error codes ──────────────────────────────────────────────────── */

typedef enum MDBX_error_t {
   MDBX_SUCCESS           =  0,
   MDBX_RESULT_FALSE      =  0,
   MDBX_RESULT_TRUE       = -1,

   /* LMDB-compatible codes */
   MDBX_KEYEXIST          = -30799,
   MDBX_NOTFOUND          = -30798,
   MDBX_PAGE_NOTFOUND     = -30797,
   MDBX_CORRUPTED         = -30796,
   MDBX_PANIC             = -30795,
   MDBX_VERSION_MISMATCH  = -30794,
   MDBX_INVALID           = -30793,
   MDBX_MAP_FULL          = -30792,
   MDBX_DBS_FULL          = -30791,
   MDBX_READERS_FULL      = -30790,
   MDBX_TXN_FULL          = -30788,
   MDBX_CURSOR_FULL       = -30787,
   MDBX_PAGE_FULL         = -30786,
   MDBX_UNABLE_EXTEND_MAPSIZE = -30785,
   MDBX_INCOMPATIBLE      = -30784,
   MDBX_BAD_RSLOT         = -30783,
   MDBX_BAD_TXN           = -30782,
   MDBX_BAD_VALSIZE       = -30781,
   MDBX_BAD_DBI           = -30780,
   MDBX_PROBLEM           = -30779,

   /* MDBX-specific codes */
   MDBX_BUSY              = -30778,
   MDBX_EMULTIVAL         = -30421,
   MDBX_EBADSIGN          = -30420,
   MDBX_WANNA_RECOVERY    = -30419,
   MDBX_EKEYMISMATCH      = -30418,
   MDBX_TOO_LARGE         = -30417,
   MDBX_THREAD_MISMATCH   = -30416,
   MDBX_TXN_OVERLAPPING   = -30415,
   MDBX_ENODATA           = -30401,
   MDBX_ENOSYS            = -30400,   /* Feature not implemented */
   MDBX_EINVAL            = -22,
   MDBX_EACCESS           = -13,
   MDBX_ENOMEM            = -12,
   MDBX_ENOFILE           = -2,
} MDBX_error_t;

/* ── Environment flags ────────────────────────────────────────────── */

typedef enum MDBX_env_flags_t {
   MDBX_ENV_DEFAULTS       = 0,
   MDBX_NOSUBDIR           = 0x4000,
   MDBX_RDONLY             = 0x20000,
   MDBX_WRITEMAP           = 0x80000,
   MDBX_NOSTICKYTHREADS    = 0x200000,
   MDBX_NOTLS              = 0x200000, /* alias */
   MDBX_NORDAHEAD          = 0x800000,
   MDBX_NOMEMINIT          = 0x1000000,
   MDBX_COALESCE           = 0x2000000,
   MDBX_LIFORECLAIM        = 0x4000000,
   MDBX_EXCLUSIVE          = 0x400000,
   MDBX_ACCEDE             = 0x40000000,

   /* Sync modes */
   MDBX_SYNC_DURABLE       = 0,
   MDBX_NOMETASYNC         = 0x40000,
   MDBX_SAFE_NOSYNC        = 0x10000,
   MDBX_MAPASYNC           = 0x10000,  /* alias */
   MDBX_UTTERLY_NOSYNC     = 0x10000 | 0x100000,
} MDBX_env_flags_t;

/* ── Transaction flags ────────────────────────────────────────────── */

typedef enum MDBX_txn_flags_t {
   MDBX_TXN_READWRITE      = 0,
   MDBX_TXN_RDONLY         = 0x20000, /* == MDBX_RDONLY */
   MDBX_TXN_RDONLY_PREPARE = 0x20000 | 0x1000000,
   MDBX_TXN_TRY            = 0x10000000,
   MDBX_TXN_NOMETASYNC     = 0x40000,
   MDBX_TXN_NOSYNC         = 0x10000,
   MDBX_TXN_USE_DWAL       = 0x20000000, /* PsiTri extension: opt into DWAL path */
} MDBX_txn_flags_t;

/* ── Database (DBI) flags ─────────────────────────────────────────── */

typedef enum MDBX_db_flags_t {
   MDBX_DB_DEFAULTS  = 0,
   MDBX_REVERSEKEY   = 0x02,
   MDBX_DUPSORT      = 0x04,
   MDBX_INTEGERKEY   = 0x08,
   MDBX_DUPFIXED     = 0x10,
   MDBX_INTEGERDUP   = 0x20,
   MDBX_REVERSEDUP   = 0x40,
   MDBX_CREATE       = 0x40000,
   MDBX_DB_ACCEDE    = 0x40000000,
} MDBX_db_flags_t;

/* ── Put flags ────────────────────────────────────────────────────── */

typedef enum MDBX_put_flags_t {
   MDBX_UPSERT       = 0,
   MDBX_NOOVERWRITE  = 0x10,
   MDBX_NODUPDATA    = 0x20,
   MDBX_CURRENT      = 0x40,
   MDBX_ALLDUPS      = 0x80,
   MDBX_RESERVE      = 0x10000,
   MDBX_APPEND       = 0x20000,
   MDBX_APPENDDUP    = 0x40000,
   MDBX_MULTIPLE     = 0x80000,
} MDBX_put_flags_t;

/* ── Cursor operations ────────────────────────────────────────────── */

typedef enum MDBX_cursor_op {
   MDBX_FIRST,
   MDBX_FIRST_DUP,
   MDBX_GET_BOTH,
   MDBX_GET_BOTH_RANGE,
   MDBX_GET_CURRENT,
   MDBX_GET_MULTIPLE,
   MDBX_LAST,
   MDBX_LAST_DUP,
   MDBX_NEXT,
   MDBX_NEXT_DUP,
   MDBX_NEXT_MULTIPLE,
   MDBX_NEXT_NODUP,
   MDBX_PREV,
   MDBX_PREV_DUP,
   MDBX_PREV_NODUP,
   MDBX_SET,
   MDBX_SET_KEY,
   MDBX_SET_RANGE,
   MDBX_PREV_MULTIPLE,
   MDBX_SET_LOWERBOUND,
   MDBX_SET_UPPERBOUND,
#ifdef __cplusplus
   first          = MDBX_FIRST,
   first_dup      = MDBX_FIRST_DUP,
   get_both       = MDBX_GET_BOTH,
   get_both_range = MDBX_GET_BOTH_RANGE,
   get_current    = MDBX_GET_CURRENT,
   get_multiple   = MDBX_GET_MULTIPLE,
   last           = MDBX_LAST,
   last_dup       = MDBX_LAST_DUP,
   next           = MDBX_NEXT,
   next_dup       = MDBX_NEXT_DUP,
   next_multiple  = MDBX_NEXT_MULTIPLE,
   next_nodup     = MDBX_NEXT_NODUP,
   previous       = MDBX_PREV,
   previous_dup   = MDBX_PREV_DUP,
   previous_nodup = MDBX_PREV_NODUP,
   key_exact      = MDBX_SET,
   key_lowerbound = MDBX_SET_RANGE,
   multi_nextkey_firstvalue    = MDBX_NEXT_NODUP,
   multi_currentkey_nextvalue  = MDBX_NEXT_DUP,
   multi_currentkey_prevvalue  = MDBX_PREV_DUP,
#endif
} MDBX_cursor_op;

/* ── Statistics ───────────────────────────────────────────────────── */

typedef struct MDBX_stat {
   uint32_t ms_psize;
   uint32_t ms_depth;
   uint64_t ms_branch_pages;
   uint64_t ms_leaf_pages;
   uint64_t ms_overflow_pages;
   uint64_t ms_entries;
   uint64_t ms_mod_txnid;
} MDBX_stat;

typedef struct MDBX_envinfo {
   struct {
      uint64_t lower;
      uint64_t upper;
      uint64_t current;
      uint64_t shrink;
      uint64_t grow;
   } mi_geo;
   uint64_t mi_mapsize;
   uint64_t mi_last_pgno;
   uint64_t mi_recent_txnid;
   uint64_t mi_latter_reader_txnid;
   uint64_t mi_self_latter_reader_txnid;
   uint32_t mi_maxreaders;
   uint32_t mi_numreaders;
   uint32_t mi_dxb_pagesize;
   uint32_t mi_sys_pagesize;
} MDBX_envinfo;

typedef struct MDBX_commit_latency {
   uint32_t preparation;
   uint32_t gc_wallclock;
   uint32_t audit;
   uint32_t write;
   uint32_t sync;
   uint32_t ending;
   uint32_t whole;
   uint32_t gc_cputime;
} MDBX_commit_latency;

typedef int mdbx_mode_t;

/* ── Environment functions ────────────────────────────────────────── */

int  mdbx_env_create(MDBX_env** penv);
int  mdbx_env_open(MDBX_env* env, const char* pathname,
                   MDBX_env_flags_t flags, mdbx_mode_t mode);
int  mdbx_env_close(MDBX_env* env);
int  mdbx_env_close_ex(MDBX_env* env, int dont_sync);
int  mdbx_env_set_geometry(MDBX_env* env,
                           intptr_t size_lower, intptr_t size_now,
                           intptr_t size_upper, intptr_t growth_step,
                           intptr_t shrink_threshold, intptr_t pagesize);
int  mdbx_env_set_maxdbs(MDBX_env* env, MDBX_dbi dbs);
int  mdbx_env_set_maxreaders(MDBX_env* env, unsigned readers);
int  mdbx_env_sync_ex(MDBX_env* env, int force, int nonblock);
int  mdbx_env_get_flags(MDBX_env* env, unsigned* flags);
int  mdbx_env_stat_ex(const MDBX_env* env, const MDBX_txn* txn,
                      MDBX_stat* stat, size_t bytes);
int  mdbx_env_info_ex(const MDBX_env* env, const MDBX_txn* txn,
                      MDBX_envinfo* info, size_t bytes);
void* mdbx_env_get_userctx(const MDBX_env* env);
int   mdbx_env_set_userctx(MDBX_env* env, void* ctx);

typedef enum MDBX_option_t {
   MDBX_opt_max_db,
   MDBX_opt_max_readers,
   MDBX_opt_sync_bytes,
   MDBX_opt_sync_period,
   MDBX_opt_rp_augment_limit,
   MDBX_opt_loose_limit,
   MDBX_opt_dp_reserve_limit,
   MDBX_opt_txn_dp_limit,
   MDBX_opt_txn_dp_initial,
   MDBX_opt_spill_max_denominator,
   MDBX_opt_spill_min_denominator,
   MDBX_opt_spill_parent4child_denominator,
   MDBX_opt_merge_threshold_16dot16_percent,
   MDBX_opt_writethrough_threshold,
   MDBX_opt_prefault_write_enable,
   MDBX_opt_psitri_cache_size_mb,
   MDBX_opt_psitri_cache_window_sec,
   MDBX_opt_psitri_write_mode,
} MDBX_option_t;

int   mdbx_env_set_option(MDBX_env* env, MDBX_option_t option, uint64_t value);
int   mdbx_env_get_option(const MDBX_env* env, MDBX_option_t option, uint64_t* value);
int   mdbx_env_copy(MDBX_env* env, const char* dest, unsigned flags);

/* ── PsiTri extension: MDBX shim write backend ───────────────────── */
/* 0=direct PsiTri transaction path, 1=DWAL buffered write path        */
#define PSITRI_WRITE_MODE_DIRECT   0
#define PSITRI_WRITE_MODE_DWAL     1
int   mdbx_env_set_write_mode(MDBX_env* env, int mode);

/* ── PsiTri extension: DWAL read mode for RO transactions ────────── */
/* 0=buffered (RO snapshot+Tri, no locks)                             */
/* 1=latest (RW+RO+Tri, shared lock on RW, sees all committed data)   */
/* 2=trie (Tri only, no DWAL layers)                                  */
#define PSITRI_READ_MODE_BUFFERED   0
#define PSITRI_READ_MODE_LATEST     1
#define PSITRI_READ_MODE_TRIE       2
int   mdbx_env_set_read_mode(MDBX_env* env, int mode);

/* ── Transaction functions ────────────────────────────────────────── */

int  mdbx_txn_begin_ex(MDBX_env* env, MDBX_txn* parent,
                       MDBX_txn_flags_t flags, MDBX_txn** txn,
                       void* context);
int  mdbx_txn_commit_ex(MDBX_txn* txn, MDBX_commit_latency* latency);
int  mdbx_txn_abort(MDBX_txn* txn);
int  mdbx_txn_reset(MDBX_txn* txn);
int  mdbx_txn_renew(MDBX_txn* txn);
MDBX_env* mdbx_txn_env(const MDBX_txn* txn);
uint64_t  mdbx_txn_id(const MDBX_txn* txn);
int       mdbx_txn_flags(const MDBX_txn* txn);

/* Convenience inline wrappers */
static inline int mdbx_txn_begin(MDBX_env* env, MDBX_txn* parent,
                                 MDBX_txn_flags_t flags, MDBX_txn** txn) {
   return mdbx_txn_begin_ex(env, parent, flags, txn, NULL);
}
static inline int mdbx_txn_commit(MDBX_txn* txn) {
   return mdbx_txn_commit_ex(txn, NULL);
}

/* ── Database (DBI) functions ─────────────────────────────────────── */

int  mdbx_dbi_open(MDBX_txn* txn, const char* name,
                   MDBX_db_flags_t flags, MDBX_dbi* dbi);
int  mdbx_dbi_close(MDBX_env* env, MDBX_dbi dbi);
int  mdbx_dbi_stat(const MDBX_txn* txn, MDBX_dbi dbi,
                   MDBX_stat* stat, size_t bytes);
int  mdbx_drop(MDBX_txn* txn, MDBX_dbi dbi, int del);
int  mdbx_dbi_flags_ex(const MDBX_txn* txn, MDBX_dbi dbi,
                       unsigned* flags, unsigned* state);

/* ── Key-value operations ─────────────────────────────────────────── */

int  mdbx_get(const MDBX_txn* txn, MDBX_dbi dbi,
              const MDBX_val* key, MDBX_val* data);
int  mdbx_get_ex(const MDBX_txn* txn, MDBX_dbi dbi,
                 MDBX_val* key, MDBX_val* data, size_t* values_count);
int  mdbx_put(MDBX_txn* txn, MDBX_dbi dbi,
              const MDBX_val* key, MDBX_val* data,
              MDBX_put_flags_t flags);
int  mdbx_del(MDBX_txn* txn, MDBX_dbi dbi,
              const MDBX_val* key, const MDBX_val* data);
int  mdbx_replace(MDBX_txn* txn, MDBX_dbi dbi,
                  const MDBX_val* key, MDBX_val* new_data,
                  MDBX_val* old_data, MDBX_put_flags_t flags);

/* ── Cursor functions ─────────────────────────────────────────────── */

int  mdbx_cursor_open(MDBX_txn* txn, MDBX_dbi dbi, MDBX_cursor** cursor);
void mdbx_cursor_close(MDBX_cursor* cursor);
int  mdbx_cursor_on_first(const MDBX_cursor* cursor);
int  mdbx_cursor_on_last(const MDBX_cursor* cursor);
int  mdbx_cursor_get(MDBX_cursor* cursor, MDBX_val* key,
                     MDBX_val* data, MDBX_cursor_op op);
int  mdbx_cursor_put(MDBX_cursor* cursor, const MDBX_val* key,
                     MDBX_val* data, MDBX_put_flags_t flags);
int  mdbx_cursor_del(MDBX_cursor* cursor, MDBX_put_flags_t flags);
int  mdbx_cursor_count(const MDBX_cursor* cursor, size_t* count);
int  mdbx_cursor_renew(MDBX_txn* txn, MDBX_cursor* cursor);
MDBX_dbi mdbx_cursor_dbi(const MDBX_cursor* cursor);
MDBX_txn* mdbx_cursor_txn(const MDBX_cursor* cursor);
MDBX_cursor* mdbx_cursor_create(void* context);
int  mdbx_cursor_copy(const MDBX_cursor* src, MDBX_cursor* dest);

/* ── Error handling ───────────────────────────────────────────────── */

const char* mdbx_strerror(int errnum);
const char* mdbx_strerror_r(int errnum, char* buf, size_t buflen);

/* ── Version ──────────────────────────────────────────────────────── */

typedef struct MDBX_version_info {
   uint8_t major;
   uint8_t minor;
   uint16_t release;
   uint32_t revision;
   struct {
      const char* datetime;
      const char* tree;
      const char* commit;
      const char* describe;
   } git;
   const char* sourcery;
} MDBX_version_info;

extern const MDBX_version_info mdbx_version;

typedef struct MDBX_build_info {
   const char* datetime;
   const char* target;
   const char* options;
   const char* compiler;
   const char* flags;
} MDBX_build_info;

extern const MDBX_build_info mdbx_build;

#ifdef __cplusplus
}  /* extern "C" */
#endif
