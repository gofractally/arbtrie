/** @file mdbx.h++
 *  @brief PsiTri-backed MDBX-compatible C++ API.
 *
 *  Drop-in replacement for libmdbx's C++ API. Provides the same class
 *  hierarchy: env/env_managed, txn/txn_managed, cursor/cursor_managed,
 *  slice, map_handle, error/exception. Backed by psitri's DWAL layer.
 */
#pragma once

#include "mdbx.h"

#include <cstddef>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>

#include <ucc/hex.hpp>
#include <string_view>
#include <utility>

namespace mdbx
{
   // ── Forward declarations ──────────────────────────────────────────

   class env;
   class env_managed;
   class txn;
   class txn_managed;
   class cursor;
   class cursor_managed;

   // ── byte type ─────────────────────────────────────────────────────

   using byte = unsigned char;

   // ── Error / Exception ─────────────────────────────────────────────

   class error
   {
      MDBX_error_t code_;

     public:
      constexpr error(MDBX_error_t code) noexcept : code_(code) {}
      error(const error&)            = default;
      error& operator=(const error&) = default;

      MDBX_error_t code() const noexcept { return code_; }
      bool         is_success() const noexcept { return code_ == MDBX_SUCCESS; }
      bool         is_failure() const noexcept { return code_ != MDBX_SUCCESS && code_ != MDBX_RESULT_TRUE; }
      const char*  what() const noexcept { return mdbx_strerror(code_); }

      void throw_on_failure() const;
      void success_or_throw() const { throw_on_failure(); }

      static void success_or_throw(int code);
      static bool boolean_or_throw(int code);
      [[noreturn]] static void throw_exception(int code);
      [[noreturn]] static void throw_exception(MDBX_error_t code) { throw_exception(static_cast<int>(code)); }

      friend bool operator==(const error& a, const error& b) noexcept { return a.code_ == b.code_; }
      friend bool operator!=(const error& a, const error& b) noexcept { return a.code_ != b.code_; }
   };

   class exception : public std::runtime_error
   {
      ::mdbx::error error_;

     public:
      exception(const ::mdbx::error& e) noexcept
          : std::runtime_error(e.what()), error_(e)
      {
      }
      const ::mdbx::error error() const noexcept { return error_; }
   };

   class not_found : public exception
   {
     public:
      not_found() : exception(::mdbx::error(MDBX_NOTFOUND)) {}
   };

   class key_exists : public exception
   {
     public:
      key_exists() : exception(::mdbx::error(MDBX_KEYEXIST)) {}
   };

   class incompatible_operation : public exception
   {
     public:
      incompatible_operation() : exception(::mdbx::error(MDBX_INCOMPATIBLE)) {}
   };

   // ── slice ─────────────────────────────────────────────────────────

   class slice
   {
      const void* data_  = nullptr;
      size_t      size_  = 0;

     public:
      constexpr slice() noexcept = default;

      slice(const void* ptr, size_t bytes) noexcept
          : data_(ptr), size_(bytes) {}

      slice(const void* begin, const void* end) noexcept
          : data_(begin),
            size_(static_cast<const byte*>(end) - static_cast<const byte*>(begin))
      {
      }

      /* implicit */ slice(const MDBX_val& val) noexcept
          : data_(val.iov_base), size_(val.iov_len) {}

      explicit slice(const char* c_str) noexcept
          : data_(c_str), size_(c_str ? std::strlen(c_str) : 0) {}

      explicit slice(const std::string& s) noexcept
          : data_(s.data()), size_(s.size()) {}

      slice(std::string_view sv) noexcept
          : data_(sv.data()), size_(sv.size()) {}

      template <size_t N>
      slice(const char (&text)[N]) noexcept
          : data_(text), size_(N - 1) {}

      // Accept any contiguous view with data()+size() (e.g. span<uint8_t>, basic_string_view<uint8_t>)
      // Excludes owning containers (basic_string<unsigned char>) to avoid overload ambiguity
      template <typename T,
                typename = std::enable_if_t<
                    !std::is_same_v<std::decay_t<T>, slice> &&
                    !std::is_same_v<std::decay_t<T>, std::string> &&
                    !std::is_same_v<std::decay_t<T>, std::string_view> &&
                    !std::is_same_v<std::decay_t<T>, std::basic_string<unsigned char>> &&
                    !std::is_same_v<std::decay_t<T>, MDBX_val>>>
      slice(const T& v) noexcept : data_(v.data()), size_(v.size()) {}

      // Accessors
      const void*  data() const noexcept { return data_; }
      const void*  end() const noexcept { return static_cast<const byte*>(data_) + size_; }
      size_t       length() const noexcept { return size_; }
      size_t       size() const noexcept { return size_; }
      bool         empty() const noexcept { return size_ == 0; }
      bool         is_null() const noexcept { return !data_; }
      operator bool() const noexcept { return data_ != nullptr; }

      byte operator[](size_t n) const noexcept
      {
         return static_cast<const byte*>(data_)[n];
      }

      const char* char_ptr() const noexcept { return static_cast<const char*>(data_); }

      // Conversion
      std::string_view string_view() const noexcept
      {
         return {static_cast<const char*>(data_), size_};
      }

      std::string as_string() const
      {
         return {static_cast<const char*>(data_), size_};
      }

      operator MDBX_val() const noexcept
      {
         return {const_cast<void*>(data_), size_};
      }

      // Sub-slices
      slice head(size_t n) const noexcept { return {data_, n < size_ ? n : size_}; }
      slice tail(size_t n) const noexcept
      {
         return {static_cast<const byte*>(data_) + (size_ > n ? size_ - n : 0),
                 n < size_ ? n : size_};
      }

      bool starts_with(const slice& prefix) const noexcept
      {
         return size_ >= prefix.size_ &&
                std::memcmp(data_, prefix.data_, prefix.size_) == 0;
      }

      bool ends_with(const slice& suffix) const noexcept
      {
         return size_ >= suffix.size_ &&
                std::memcmp(static_cast<const byte*>(data_) + size_ - suffix.size_,
                           suffix.data_, suffix.size_) == 0;
      }

      void clear() noexcept { data_ = nullptr; size_ = 0; }

      // Comparison
      static int compare_fast(const slice& a, const slice& b) noexcept
      {
         size_t min_len = a.size_ < b.size_ ? a.size_ : b.size_;
         int    cmp     = min_len ? std::memcmp(a.data_, b.data_, min_len) : 0;
         if (cmp)
            return cmp;
         return (a.size_ < b.size_) ? -1 : (a.size_ > b.size_) ? 1 : 0;
      }

      friend bool operator==(const slice& a, const slice& b) noexcept
      {
         return a.size_ == b.size_ && (a.data_ == b.data_ || !a.size_ ||
                std::memcmp(a.data_, b.data_, a.size_) == 0);
      }
      friend bool operator!=(const slice& a, const slice& b) noexcept { return !(a == b); }
      friend bool operator<(const slice& a, const slice& b) noexcept
      {
         return compare_fast(a, b) < 0;
      }
      friend bool operator<=(const slice& a, const slice& b) noexcept { return !(b < a); }
      friend bool operator>(const slice& a, const slice& b) noexcept { return b < a; }
      friend bool operator>=(const slice& a, const slice& b) noexcept { return !(a < b); }
   };

   // ── pair / result types ───────────────────────────────────────────

   struct value_result
   {
      slice value;
      bool  done;

      value_result(const slice& v, bool d) noexcept : value(v), done(d) {}
      operator bool() const noexcept { return done; }
   };

   struct pair
   {
      slice key, value;

      constexpr pair() noexcept = default;
      constexpr pair(const slice& k, const slice& v) noexcept : key(k), value(v) {}
      operator bool() const noexcept { return key; }
   };

   struct pair_result : public pair
   {
      bool done = false;

      constexpr pair_result() noexcept = default;
      constexpr pair_result(const slice& k, const slice& v, bool d) noexcept
          : pair(k, v), done(d)
      {
      }
      operator bool() const noexcept { return done; }
   };

   // ── Key/value mode enums ──────────────────────────────────────────

   enum key_mode {
      usual   = MDBX_DB_DEFAULTS,
      reverse = MDBX_REVERSEKEY,
      ordinal = MDBX_INTEGERKEY,
   };

   enum value_mode {
      single              = MDBX_DB_DEFAULTS,
      multi               = MDBX_DUPSORT,
      multi_reverse        = MDBX_DUPSORT | MDBX_REVERSEDUP,
      multi_samelength     = MDBX_DUPSORT | MDBX_DUPFIXED,
      multi_ordinal        = MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP,
   };

   // ── map_handle ────────────────────────────────────────────────────

   struct map_handle
   {
      MDBX_dbi dbi{0};

      constexpr map_handle() noexcept = default;
      constexpr map_handle(MDBX_dbi dbi) noexcept : dbi(dbi) {}

      operator bool() const noexcept { return dbi != 0; }
      operator MDBX_dbi() const noexcept { return dbi; }

      struct info
      {
         MDBX_dbi dbi;
         MDBX_db_flags_t flags;
         unsigned state;

         key_mode key_mode() const noexcept
         {
            return static_cast<enum key_mode>(flags & (MDBX_REVERSEKEY | MDBX_INTEGERKEY));
         }
         value_mode value_mode() const noexcept
         {
            return static_cast<enum value_mode>(
                flags & (MDBX_DUPSORT | MDBX_REVERSEDUP | MDBX_DUPFIXED | MDBX_INTEGERDUP));
         }
      };

      friend bool operator==(const map_handle& a, const map_handle& b) noexcept
      {
         return a.dbi == b.dbi;
      }
      friend bool operator<(const map_handle& a, const map_handle& b) noexcept
      {
         return a.dbi < b.dbi;
      }
   };

   enum put_mode {
      insert_unique = MDBX_NOOVERWRITE,
      upsert        = MDBX_UPSERT,
      update        = MDBX_CURRENT,
   };

   // ── env (unmanaged) ───────────────────────────────────────────────

   class env
   {
     public:
      constexpr env(MDBX_env* ptr) noexcept : handle_(ptr) {}

     protected:
      MDBX_env* handle_ = nullptr;

     public:
      constexpr env() noexcept = default;
      env(const env&) noexcept            = default;
      env& operator=(const env&) noexcept = default;
      ~env() noexcept                     = default;

      env(env&& o) noexcept : handle_(o.handle_) { o.handle_ = nullptr; }
      env& operator=(env&& o) noexcept
      {
         handle_   = o.handle_;
         o.handle_ = nullptr;
         return *this;
      }

      operator bool() const noexcept { return handle_ != nullptr; }
      operator const MDBX_env*() const { return handle_; }
      operator MDBX_env*() { return handle_; }

      // ── Geometry ───────────────────────────────────────────────────

      struct geometry
      {
         enum : intptr_t { default_value = -1 };

         intptr_t size_lower       = default_value;
         intptr_t size_now         = default_value;
         intptr_t size_upper       = default_value;
         intptr_t growth_step      = default_value;
         intptr_t shrink_threshold = default_value;
         intptr_t pagesize         = default_value;

         geometry& make_fixed(intptr_t size) noexcept
         {
            size_lower = size_now = size_upper = size;
            return *this;
         }

         geometry& make_dynamic(intptr_t lower = default_value,
                                intptr_t upper = default_value) noexcept
         {
            size_lower = lower;
            size_upper = upper;
            return *this;
         }
      };

      // ── Mode / durability enums ────────────────────────────────────

      enum mode {
         readonly,
         write_file_io,
         write_mapped_io,
      };

      enum durability {
         robust_synchronous,
         half_synchronous_weak_last,
         lazy_weak_tail,
         whole_fragile,
      };

      // ── operate_parameters ─────────────────────────────────────────

      struct operate_parameters
      {
         unsigned    max_maps    = 0;
         unsigned    max_readers = 0;
         env::mode       mode       = write_mapped_io;
         env::durability durability = robust_synchronous;
         unsigned    options = 0;

         MDBX_env_flags_t make_flags() const noexcept;

         static enum mode mode_from_flags(MDBX_env_flags_t flags) noexcept
         {
            if (flags & MDBX_RDONLY) return readonly;
            if (flags & MDBX_WRITEMAP) return write_mapped_io;
            return write_file_io;
         }

         static unsigned options_from_flags(MDBX_env_flags_t) noexcept { return 0; }

         static env::durability durability_from_flags(MDBX_env_flags_t flags) noexcept
         {
            if (flags & MDBX_UTTERLY_NOSYNC) return whole_fragile;
            if (flags & MDBX_SAFE_NOSYNC) return lazy_weak_tail;
            if (flags & MDBX_NOMETASYNC) return half_synchronous_weak_last;
            return robust_synchronous;
         }
      };

      // ── create_parameters (for env_managed) ────────────────────────

      struct create_parameters
      {
         geometry   geometry;
         mdbx_mode_t file_mode_bits = 0640;
         bool        use_subdirectory = false;
      };

      // ── Methods ────────────────────────────────────────────────────

      env& set_geometry(const geometry& geo);

      unsigned max_maps() const;
      unsigned max_readers() const;

      void* get_context() const noexcept { return mdbx_env_get_userctx(handle_); }
      env&  set_context(void* ctx);

      bool sync_to_disk(bool force = true, bool nonblock = false);

      void close_map(const map_handle& map);

      MDBX_stat    get_stat() const;
      MDBX_envinfo get_info() const;
      size_t       get_pagesize() const { return 4096; }
      int          check_readers() { return 0; }
      void         copy(const char* dest, bool compactify = false, bool force_dynamic = false);
      std::filesystem::path get_path() const;

      txn_managed start_read() const;
      txn_managed start_write(bool dont_wait = false);
   };

   // ── env_managed ───────────────────────────────────────────────────

   class env_managed : public env
   {
     public:
      constexpr env_managed() noexcept = default;

      env_managed(const char* pathname,
                  const create_parameters& cp,
                  const operate_parameters& op,
                  bool accede = true);

      env_managed(const std::string& pathname,
                  const create_parameters& cp,
                  const operate_parameters& op,
                  bool accede = true)
          : env_managed(pathname.c_str(), cp, op, accede) {}

      ~env_managed() noexcept;

      void close(bool dont_sync = false);

      env_managed(env_managed&& o) noexcept : env(std::move(o)) {}
      env_managed& operator=(env_managed&& o) noexcept;

      env_managed(const env_managed&)            = delete;
      env_managed& operator=(const env_managed&) = delete;
   };

   // ── txn (unmanaged) ───────────────────────────────────────────────

   class txn
   {
     public:
      constexpr txn(MDBX_txn* ptr) noexcept : handle_(ptr) {}

     protected:
      MDBX_txn* handle_ = nullptr;

     public:
      constexpr txn() noexcept = default;
      txn(const txn&) noexcept            = default;
      txn& operator=(const txn&) noexcept = default;
      ~txn() noexcept                     = default;

      txn(txn&& o) noexcept : handle_(o.handle_) { o.handle_ = nullptr; }
      txn& operator=(txn&& o) noexcept
      {
         handle_   = o.handle_;
         o.handle_ = nullptr;
         return *this;
      }

      operator bool() const noexcept { return handle_ != nullptr; }
      operator const MDBX_txn*() const { return handle_; }
      operator MDBX_txn*() { return handle_; }

      ::mdbx::env env() const noexcept;
      MDBX_txn_flags_t flags() const { return static_cast<MDBX_txn_flags_t>(mdbx_txn_flags(handle_)); }
      uint64_t id() const { return mdbx_txn_id(handle_); }
      bool is_readonly() const { return (flags() & MDBX_TXN_RDONLY) != 0; }
      bool is_readwrite() const { return !is_readonly(); }

      void* get_context() const noexcept;
      txn&  set_context(void* ctx);

      // ── Map management ─────────────────────────────────────────────

      map_handle open_map(const char* name,
                          key_mode km   = key_mode::usual,
                          value_mode vm = value_mode::single) const;

      map_handle create_map(const char* name,
                            key_mode km   = key_mode::usual,
                            value_mode vm = value_mode::single);

      map_handle open_map(const std::string& name,
                          key_mode km   = key_mode::usual,
                          value_mode vm = value_mode::single) const
      {
         return open_map(name.c_str(), km, vm);
      }

      map_handle create_map(const std::string& name,
                            key_mode km   = key_mode::usual,
                            value_mode vm = value_mode::single)
      {
         return create_map(name.c_str(), km, vm);
      }

      void drop_map(map_handle map);
      void clear_map(map_handle map);

      bool drop_map(const std::string& name, bool throw_if_absent = true);
      bool clear_map(const std::string& name, bool throw_if_absent = true);

      // ── Cursor ─────────────────────────────────────────────────────

      cursor_managed open_cursor(map_handle map) const;

      // ── Key-value operations ───────────────────────────────────────

      slice get(map_handle map, const slice& key) const;

      slice get(map_handle map, const slice& key,
                const slice& value_at_absence) const;

      void upsert(map_handle map, const slice& key, const slice& value);

      void insert(map_handle map, const slice& key, slice value);

      value_result try_insert(map_handle map, const slice& key, slice value);

      void update(map_handle map, const slice& key, const slice& value);

      bool try_update(map_handle map, const slice& key, const slice& value);

      MDBX_error_t put(map_handle map, const slice& key, slice* value,
                       MDBX_put_flags_t flags) noexcept;

      bool erase(map_handle map, const slice& key);

      bool erase(map_handle map, const slice& key, const slice& value);

      // ── Map statistics ────────────────────────────────────────────

      MDBX_stat        get_map_stat(map_handle map) const;
      map_handle::info get_handle_info(map_handle map) const;
   };

   // ── txn_managed ───────────────────────────────────────────────────

   class txn_managed : public txn
   {
     public:
      constexpr txn_managed() noexcept = default;

      ~txn_managed() noexcept;

      void abort();
      void commit();
      void commit(MDBX_commit_latency& /*latency*/) { commit(); }

      txn_managed(txn_managed&& o) noexcept : txn(std::move(o)) {}
      txn_managed& operator=(txn_managed&& o) noexcept;

      txn_managed(const txn_managed&)            = delete;
      txn_managed& operator=(const txn_managed&) = delete;

     private:
      friend class env;
      txn_managed(MDBX_txn* ptr) noexcept : txn(ptr) {}
   };

   // ── cursor (unmanaged) ────────────────────────────────────────────

   class cursor
   {
     protected:
      MDBX_cursor* handle_ = nullptr;

     public:
      constexpr cursor(MDBX_cursor* ptr) noexcept : handle_(ptr) {}
      constexpr cursor() noexcept = default;
      cursor(const cursor&) noexcept            = default;
      cursor& operator=(const cursor&) noexcept = default;
      ~cursor() noexcept                        = default;

      cursor(cursor&& o) noexcept : handle_(o.handle_) { o.handle_ = nullptr; }
      cursor& operator=(cursor&& o) noexcept
      {
         handle_   = o.handle_;
         o.handle_ = nullptr;
         return *this;
      }

      operator bool() const noexcept { return handle_ != nullptr; }
      operator const MDBX_cursor*() const { return handle_; }
      operator MDBX_cursor*() { return handle_; }

      using move_operation = MDBX_cursor_op;

      // ── move_result ────────────────────────────────────────────────

      struct move_result : public pair_result
      {
         move_result() noexcept = default;
         move_result(const slice& k, const slice& v, bool d) noexcept
             : pair_result(k, v, d) {}
      };

      // ── Navigation ─────────────────────────────────────────────────

      move_result to_first(bool throw_notfound = true);
      move_result to_last(bool throw_notfound = true);
      move_result to_next(bool throw_notfound = true);
      move_result to_previous(bool throw_notfound = true);
      move_result current(bool throw_notfound = true) const;

      move_result to_key_equal(const slice& key, bool throw_notfound = true);

      // Seek
      bool seek(const slice& key);
      move_result find(const slice& key, bool throw_notfound = true);
      move_result lower_bound(const slice& key, bool throw_notfound = false);
      move_result upper_bound(const slice& key, bool throw_notfound = false);

      // Generic move
      move_result move(move_operation op, MDBX_val* key, MDBX_val* value,
                       bool throw_notfound);

      // Overloads for move with key and key+value
      move_result move(move_operation op, bool throw_notfound);
      move_result move(move_operation op, const slice& key, bool throw_notfound);
      move_result move(move_operation op, const slice& key, const slice& value,
                       bool throw_notfound);

      // DUPSORT multi-value navigation
      move_result to_current_first_multi(bool throw_notfound = true);
      move_result to_current_last_multi(bool throw_notfound = true);
      move_result to_current_prev_multi(bool throw_notfound = true);
      move_result to_current_next_multi(bool throw_notfound = true);
      move_result to_next_dup(bool throw_notfound = true);
      move_result to_prev_dup(bool throw_notfound = true);
      move_result to_next_nodup(bool throw_notfound = true);
      move_result to_prev_nodup(bool throw_notfound = true);
      move_result to_previous_last_multi(bool throw_notfound = true);
      move_result to_next_first_multi(bool throw_notfound = true);

      move_result find_multivalue(const slice& key, const slice& value,
                                  bool throw_notfound = true);
      move_result lower_bound_multivalue(const slice& key, const slice& value,
                                         bool throw_notfound = false);

      size_t count_multivalue() const;

      // ── State ──────────────────────────────────────────────────────

      bool eof() const;
      bool on_first() const;
      bool on_last() const;

      // ── Mutations ──────────────────────────────────────────────────

      void upsert(const slice& key, const slice& value);
      void insert(const slice& key, slice value);
      value_result try_insert(const slice& key, slice value);
      void update(const slice& key, const slice& value);
      bool try_update(const slice& key, const slice& value);

      void append(const slice& key, const slice& value);

      MDBX_error_t put(const slice& key, slice* value, MDBX_put_flags_t flags) noexcept;

      bool erase(bool whole_multivalue = false);
      bool erase(const slice& key, bool whole_multivalue = true);
      bool erase(const slice& key, const slice& value);

      // ── Binding ────────────────────────────────────────────────────

      void renew(::mdbx::txn& txn);
      void bind(::mdbx::txn& txn, map_handle map);

      ::mdbx::txn txn() const;
      map_handle   map() const;

     private:
      move_result do_get(MDBX_cursor_op op, MDBX_val* key, MDBX_val* data,
                         bool throw_notfound) const;
   };

   // ── cursor_managed ────────────────────────────────────────────────

   class cursor_managed : public cursor
   {
     public:
      cursor_managed() noexcept = default;

      ~cursor_managed() noexcept;

      void close();

      cursor_managed(cursor_managed&& o) noexcept : cursor(std::move(o)) {}
      cursor_managed& operator=(cursor_managed&& o) noexcept;

      cursor_managed(const cursor_managed&)            = delete;
      cursor_managed& operator=(const cursor_managed&) = delete;

     private:
      friend class txn;
      cursor_managed(MDBX_cursor* ptr) noexcept : cursor(ptr) {}
   };

   // ── Inline implementations ────────────────────────────────────────

   inline void error::throw_on_failure() const
   {
      if (is_failure())
      {
         if (code_ == MDBX_NOTFOUND)
            throw not_found();
         if (code_ == MDBX_KEYEXIST)
            throw key_exists();
         throw exception(*this);
      }
   }

   inline void error::success_or_throw(int code)
   {
      error(static_cast<MDBX_error_t>(code)).throw_on_failure();
   }

   inline void error::throw_exception(int code)
   {
      error e(static_cast<MDBX_error_t>(code));
      if (code == MDBX_NOTFOUND)
         throw not_found();
      if (code == MDBX_KEYEXIST)
         throw key_exists();
      throw exception(e);
   }

   inline bool error::boolean_or_throw(int code)
   {
      if (code == MDBX_RESULT_TRUE)
         return true;
      if (code == MDBX_SUCCESS)
         return false;
      error(static_cast<MDBX_error_t>(code)).throw_on_failure();
      return false;
   }

   // ── Utility functions ─────────────────────────────────────────────

   struct to_hex
   {
      std::string str_;
      explicit to_hex(const slice& s) : str_(ucc::to_hex(s.data(), s.size())) {}
      std::string as_string() const { return str_; }
      operator std::string() const { return str_; }
   };

   inline const MDBX_version_info& get_version() { return mdbx_version; }
   inline const char*              get_build()   { return "psitrimdbx"; }

}  // namespace mdbx
