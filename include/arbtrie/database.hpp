#pragma once
#include <algorithm>
#include <arbtrie/arbtrie.hpp>
#include <arbtrie/binary_node.hpp>
#include <arbtrie/file_fwd.hpp>
#include <arbtrie/iterator.hpp>
#include <arbtrie/mapped_memory.hpp>
#include <arbtrie/node_handle.hpp>
#include <arbtrie/node_stats.hpp>
#include <arbtrie/seg_allocator.hpp>
#include <arbtrie/value_type.hpp>
#include <memory>
#include <optional>

namespace arbtrie
{
   using session_rlock = read_lock;
   using kv_type       = binary_node::key_index::value_type;
   using kv_index      = binary_node::key_index;

   class write_transaction;

   struct upsert_mode
   {
      enum type : uint32_t
      {
         unique             = 1,  // ref count of all parent nodes and this is 1
         insert             = 2,  // fail if key does exist
         update             = 4,  // fail if key doesn't exist
         same_region        = 8,
         remove             = 16,
         must_remove_f      = 32,
         upsert             = insert | update,
         unique_upsert      = unique | upsert,
         unique_insert      = unique | insert,
         unique_update      = unique | update,
         unique_remove      = unique | remove,
         unique_must_remove = unique | must_remove_f | remove,
         shared_upsert      = upsert,
         shared_insert      = insert,
         shared_update      = update,
         shared_remove      = remove
      };

      constexpr upsert_mode(upsert_mode::type t) : flags(t) {};

      constexpr bool        is_unique() const { return flags & unique; }
      constexpr bool        is_shared() const { return not is_unique(); }
      constexpr bool        is_same_region() const { return flags & same_region; }
      constexpr upsert_mode make_shared() const { return {flags & ~unique}; }
      constexpr upsert_mode make_unique() const { return {flags | unique}; }
      constexpr upsert_mode make_same_region() const { return {flags | same_region}; }
      constexpr bool        may_insert() const { return flags & insert; }
      constexpr bool        may_update() const { return flags & update; }
      constexpr bool        must_insert() const { return not(flags & (update | remove)); }
      constexpr bool        must_update() const { return not is_remove() and not(flags & insert); }
      constexpr bool        is_insert() const { return (flags & insert); }
      constexpr bool        is_upsert() const { return (flags & insert) and (flags & update); }
      constexpr bool        is_remove() const { return flags & remove; }
      constexpr bool        is_update() const { return flags & update; }
      constexpr bool        must_remove() const { return flags & must_remove_f; }

      // private: structural types cannot have private members,
      // but the flags field is not meant to be used directly
      constexpr upsert_mode(uint32_t f) : flags(f) {}
      uint32_t flags;
   };

   class read_session
   {
     protected:
      template <iterator_caching_mode>
      friend class iterator;
      friend class database;
      friend class root_data;
      friend class root;
      friend class node_handle;
      friend class write_session;
      friend class write_transaction;

      database* _db;
      read_session(database* db);

      int get(object_ref& root, key_view key, std::invocable<bool, value_type> auto&& callback);
      int get(object_ref&                             root,
              const auto*                             inner,
              key_view                                key,
              std::invocable<bool, value_type> auto&& callback);
      int get(object_ref& root,
              const binary_node*,
              key_view                                key,
              std::invocable<bool, value_type> auto&& callback);
      int get(object_ref& root,
              const value_node*,
              key_view                                key,
              std::invocable<bool, value_type> auto&& callback);

      /** creates a new handle for address, retains it */
      node_handle create_handle(id_address a) { return node_handle(*this, a); }
      read_lock   lock() { return _segas->lock(); }

     public:
      ~read_session();
      // Move operations can now be enabled with pointer members
      read_session(read_session&&)            = default;
      read_session& operator=(read_session&&) = default;

      std::unique_ptr<seg_alloc_session> _segas;

      uint64_t count_ids_with_refs() { return _segas->count_ids_with_refs(); }

      template <iterator_caching_mode CacheMode = caching>
      auto create_iterator(node_handle h)
      {
         return iterator<CacheMode>(*this, std::move(h));
      }

      read_transaction::ptr start_read_transaction(int top_root_node = 0)
      {
         node_handle root = (top_root_node == -1) ? create_root() : get_root(top_root_node);
         // Use make_shared with the private token
         return std::make_shared<read_transaction>(read_transaction::private_token{}, *this,
                                                   std::move(root));
      }
      caching_read_transaction::ptr start_caching_read_transaction(int top_root_node = 0)
      {
         node_handle root = (top_root_node == -1) ? create_root() : get_root(top_root_node);
         // Use make_shared with the private token
         return std::make_shared<caching_read_transaction>(
             caching_read_transaction::private_token{}, *this, std::move(root));
      }

      /**
       *  This version of get reduces the need to copy to an
       *  intermediary buffer by allowing the caller to provide
       *  a callback method with a view on the data. This callback 
       *  is called while holding the read lock, so make sure it returns
       *  quickly.
       *
       *  If the value is a subtree, this method will return as if the
       *  key is not found.
       */
      inline int get(const node_handle&                      r,
                     key_view                                key,
                     std::invocable<bool, value_view> auto&& callback);

      /**
       * If the key exists and contains a subtree then it will return a node handle
       * to the subtree. 
       *
       * If the key exists but contains data, it will return a null optional
       */
      inline std::optional<node_handle> get_subtree(const node_handle& r, key_view key);

      /**
       * resizes result to the size of the value and copies the value into result
       * @return -1 if not found, otherwise the size of the value
       */
      inline int get(const node_handle& r, key_view key, std::vector<char>* data = nullptr);

      /**
       *  Creates a new independent tree with no values on it,
       *  the tree will be deleted when the last node handle that
       *  references it goes out of scope. If node handle isn't
       *  saved via a call to set_root(), or storing it as a value
       *  associated with a key under the tree saved as set_root()
       *  it will not survivie the current process and its data will
       *  be "dead" database data until it is cleaned up.
       *
       *  Each node handle is an immutable reference to the
       *  state of the tree so it is important to keep only
       *  one copy of it if you are wanting to update effeciently
       *  in place.
       */
      node_handle create_root() { return node_handle(*this); }

      /**
       * The database supports up to 488 top root nodes that can be 
       * accessed by index. This limit is based upon the minimum disk
       * sync unit being 4kb; therefore, every it can sync up to 488 bytes
       * of root nodes without extra cost.
       *
       * It is possible to write to independent top roots in parallel because
       * there would be no conflict in updating the root; however, if two
       * threads attempt to get, modify, and set the same root then the last
       * write will win.
       * 
       */
      node_handle        get_root(int root_index = 0);
      constexpr uint32_t max_roots() const { return num_top_roots; }

      /**
       * Each node handle is associated with a specific read/write session which
       * impacts where the eventual release() is executed. Node handles can only
       * be constructed from a read_session; therefore, this allows you to construct
       * a copy in this read session from a node handle created by another session.
       */
      node_handle adopt(const node_handle& h) { return node_handle(*this, h.address()); }

      void visit_nodes(const node_handle& r, auto&& on_node);

      node_stats get_node_stats(const node_handle& r)
      {
         if (not r.address())
            return {};
         node_stats result;
         visit_nodes(r,
                     [&](int depth, const auto* node)
                     {
                        if constexpr (std::is_same_v<decltype(node), const binary_node*>)
                           result.total_keys += node->_num_branches;
                        if constexpr (std::is_same_v<decltype(node), const value_node*>)
                           result.total_keys++;
                        /*
                        if constexpr (std::is_same_v<decltype(node), const setlist_node*> or
                                      std::is_same_v<decltype(node), const full_node*> or
                                      std::is_same_v<decltype(node), const bitset_node*>)
                           result.total_keys +=
                               ((const inner_node<setlist_node>*)node)->has_eof_value();
                               */

                        if (node->type < num_types)
                        {
                           result.node_counts[node->type]++;
                           result.node_data_size[node->type] += node->_nsize;
                        }
                        else
                        {
                           ARBTRIE_WARN("unknown type! ", node->type);
                        }
                        if (result.max_depth < depth)
                           result.max_depth = depth;
                        result.total_depth += depth;
                     });
         return result;
      }
   };

   /**
    * @class write_session
    * @brief A session that provides write access to the database
    * 
    * This class uses shared ownership semantics through std::shared_ptr.
    * Transactions created from this session will hold a shared_ptr to ensure
    * the session remains alive as long as any transaction needs it.
    * 
    * @example
    * ```
    * // Create a session through the factory method
    * auto session = database::start_write_session_shared();
    * 
    * // Start a transaction - automatically keeps the session alive
    * auto tx = session->start_transaction();
    * 
    * // Store multiple sessions in containers
    * std::vector<std::shared_ptr<write_session>> sessions;
    * sessions.push_back(database::start_write_session_shared());
    * ```
    */
   class write_session : public read_session, public std::enable_shared_from_this<write_session>
   {
      friend class database;
      friend class write_transaction;

      // Private constructor - use database::start_write_session() instead
      write_session(database* db) : read_session(db) {}

      // Copy operations still deleted to prevent accidental copying
      write_session(const write_session&)            = delete;
      write_session& operator=(const write_session&) = delete;

      // Move operations are now allowed since base class is movable
      write_session(write_session&&)            = default;
      write_session& operator=(write_session&&) = default;

      id_address upsert(session_rlock& state, id_address root, key_view key, const value_type& val);

      id_address insert(session_rlock& state, id_address root, key_view key, const value_type& val);

      id_address update(session_rlock& state, id_address root, key_view key, const value_type& val);

      id_address remove(session_rlock& state, id_address root, key_view key);

      value_type _cur_val;
      // bool       _sync_lock = false;

      // when a new key is inserted this is set to 1 and the and
      // when a key is removed this is set to -1
      // innner_node::_descendants field is updated by this delta
      // as the stack unwinds after writing, then it is reset to 0
      int _delta_keys = 0;

      // when updating or removing a node, it is useful to know
      // the delta "space" without having to first query the value
      // you are about to delete.
      int _old_value_size = -1;

      std::optional<node_handle> _old_handle;
      std::optional<node_handle> _new_handle;

      // makes the passed node handle the official state of the
      // database and returns the old state so the caller can
      // choose when it gets released

      // Designed to only be called by write_transaction objects
      // which are created by the start_transaction().
      //
      // @returns the prior root so caller can choose when/where
      // to release it, if ignored it will be released immediately
      node_handle set_root(node_handle r, int index = 0);

      // Designed to only be called by write_transaction objects
      // which will take a lock on the root until they either commit
      // by calling set_root() or abort by calling abort_write().
      node_handle get_mutable_root(int index);

      void abort_write(int index);

      /**
       * Static factory method to create a shared_ptr to write_session
       * This allows us to create a shared_ptr even with a private constructor
       */
      static std::shared_ptr<write_session> create(database* db)
      {
         return std::shared_ptr<write_session>(new write_session(db));
      }

     public:
      ~write_session();

      /**
       * Start a new transaction with global root 
       * if top_root_node is provided, then the transaction will operate on that root
       * If -1 is provided as the top_root_node, then a temporary root will be created.
       * The temporary root will be lost unless it is saved as a subtree of another root.
       * 
       * The database supports up to 488 top root nodes that can be accessed by index.
       * This limit is based upon the minimum disk sync unit being 4kb; therefore,
       * it can sync up to 488 id_address values without extra cost.
       * 
       * It is possible to write to independent top roots in parallel because there
       * would be no conflict in updating the root; however, if two threads attempt 
       * to get, modify, and set the same root then the last write will win.
       * 
       * The transaction will hold a shared_ptr to this session, ensuring that
       * the session remains valid for the lifetime of the transaction.
       * 
       * @param top_root_node The index of the root node to use, or -1 for a temporary root
       * @return write_transaction A new transaction object
       */
      write_transaction::ptr start_write_transaction(int top_root_node = 0);

      void sync();

      /**
       * @return -1 on insert, otherwise the size of the old value
       */
      int upsert(node_handle& r, key_view key, value_view val);

      /**
       * throws if existing key is found
       */
      void insert(node_handle& r, key_view key, value_view val);

      /**
       * @return size of the old value, throws if key not found
       */
      int update(node_handle& r, key_view key, value_view val);

      /**
       * Inserts the subtree at key, throws if key already exists
       */
      void insert(node_handle& r, key_view key, node_handle subtree);

      /**
       * Returns a reference to the prior node handle, if it wasn't a view so that
       * the caller can control when it is released.
       */
      std::optional<node_handle> update(node_handle& r, key_view key, node_handle subtree);

      /**
       * If the existing value is a subtree, return a reference to it. Otherwise
       * insert or update the new subtree at this location.
       */
      std::optional<node_handle> upsert(node_handle& r, key_view key, node_handle subtree);

      // return the number of bytes removed, or -1 if nothing was removed
      int remove(node_handle& r, key_view key);

      // throws if no key was removed, return the number of bytes removed
      int require_remove(node_handle& r, key_view key);

      // return the number of keys removed
      // int remove(node_handle& r, key_view from, key_view to);

     private:
      template <upsert_mode mode>
      id_address upsert(object_ref& root, key_view key, const alloc_hint& parent_hint);
      template <upsert_mode mode>
      id_address upsert(object_ref&& root, key_view key, const alloc_hint& parent_hint);

      template <upsert_mode mode, typename NodeType>
      id_address upsert_inner_existing_br(object_ref&       r,
                                          key_view          key,
                                          const NodeType*   fn,
                                          key_view          cpre,
                                          branch_index_type bidx,
                                          id_address        br,
                                          const alloc_hint& parent_hint);
      template <upsert_mode mode, typename NodeType>
      id_address upsert_inner_new_br(object_ref&       r,
                                     key_view          key,
                                     const NodeType*   fn,
                                     key_view          cpre,
                                     branch_index_type bidx,
                                     id_address        br,
                                     const alloc_hint& parent_hint);
      template <upsert_mode mode, typename NodeType>
      id_address upsert_prefix(object_ref&       r,
                               key_view          key,
                               key_view          cpre,
                               const NodeType*   fn,
                               key_view          rootpre,
                               const alloc_hint& parent_hint);

      template <upsert_mode mode, typename NodeType>
      id_address upsert_eof(const alloc_hint& parent_hint, object_ref& r, const NodeType* fn);

      template <upsert_mode mode, typename NodeType>
      id_address remove_eof(const alloc_hint& parent_hint, object_ref& r, const NodeType* fn);

      template <upsert_mode mode, typename NodeType>
      id_address upsert_inner(object_ref&       r,
                              const NodeType*   fn,
                              key_view          key,
                              const alloc_hint& parent_hint);

      template <upsert_mode mode, typename NodeType>
      id_address upsert_inner(object_ref&&      r,
                              const NodeType*   fn,
                              key_view          key,
                              const alloc_hint& parent_hint)
      {
         return upsert_inner<mode>(r, fn, key, parent_hint);
      }

      template <upsert_mode mode>
      id_address upsert_eof_value(object_ref& root);

      template <upsert_mode mode>
      id_address upsert_value(object_ref&       root,
                              const value_node* vn,
                              key_view          key,
                              const alloc_hint& hint);

      //=======================
      // binary_node operations
      // ======================
      id_address make_binary(id_region         reg,
                             const alloc_hint& parent_hint,
                             session_rlock&    state,
                             key_view          key,
                             const value_type& val);

      template <upsert_mode mode>
      id_address upsert_binary(object_ref&        root,
                               const binary_node* bn,
                               key_view           key,
                               const alloc_hint&  parent_hint);

      template <upsert_mode mode>
      id_address update_binary_key(object_ref&        root,
                                   const binary_node* bn,
                                   uint16_t           lb_idx,
                                   key_view           key,
                                   const alloc_hint&  parent_hint);
      template <upsert_mode mode>
      id_address remove_binary_key(object_ref&        root,
                                   const binary_node* bn,
                                   uint16_t           lb_idx,
                                   key_view           key,
                                   const alloc_hint&  parent_hint);
   };

   /**
    * @class database
    * @brief A class for managing an Arbtrie database
    * 
    * This class provides a high-level interface for creating, opening, and managing
    * an Arbtrie database. It supports read-only and read-write access modes,
    * and provides methods for creating new databases, opening existing ones,
    * 
    * @example
    * ```
    * // Create a new database
    * database::create("my_database");
    * 
    * // Open an existing database
    * auto db = database::open("my_database");
    * 
    * // Create a read-only session
    * auto read_session = db->start_read_session();
    * 
    * // Create a read-write session
    * ```
    * 
    * ACID properties:
    *   - every time transaction::commit is called, 
    *     write_session::start_write_transaction::commit lambda
    *     will call seg_alloc_session::sync() which will call
    *     segment::sync() for every segment that has been modified since
    *     the last sync. The segment will mprotect() and msync() the regions
    *     that have been modified. 
    *   - after all segments have msynced, seg_alloc_session will fsync() 
    *     the segments file to tell the OS to push data to disk. 
    *   - then start_write_transaction::commit lambda will call
    *      write_session::set_root() and atomically update the root node
    *      pointer in database header file. 
    *   - lastly the database header file is msynced, fsynced or F_FULL_SYNC
    *     according to the sync_mode in runtime_config.
    *   - The F_FULL_SYNC flag forces everything from all processes to be
    *     pushed to the disk and for the disk to flush everything from its
    *     cache to the physical media. F_FULL_SYNC takes care of all files so
    *     that fsync() is not needed on each individual file.
    * 
    */
   class database
   {
     public:
      static constexpr auto read_write = access_mode::read_write;
      static constexpr auto read_only  = access_mode::read_only;

      database(std::filesystem::path dir, runtime_config = {}, access_mode = read_write);
      ~database();

      static std::shared_ptr<database> create(std::filesystem::path dir, runtime_config = {});

      /**
       * @brief Start a new write session for modifying the database
       * 
       * Returns a shared_ptr to a write_session, which can be stored in containers
       * and will automatically manage the session's lifetime. Transactions created 
       * from this session will hold a shared_ptr to it, ensuring that the session 
       * remains alive as long as any transaction needs it.
       * 
       * @return std::shared_ptr<write_session> A shared pointer to a new write session
       */
      std::shared_ptr<write_session> start_write_session();

      read_session start_read_session();

      void print_stats(std::ostream& os, bool detail = false);

      void recover(recover_args args = recover_args());
      bool validate();

      /**
       * @brief Gets the current runtime configuration.
       * @return A copy of the current runtime_config.
       */
      runtime_config get_runtime_config() const;

      /**
       * @brief Sets the runtime configuration.
       * @param cfg The new runtime_config to apply.
       * @note Modifying configuration on a live database might have unintended consequences
       *       depending on the specific setting changed. Use with caution.
       */
      void set_runtime_config(const runtime_config& cfg);

     private:
      friend class read_session;
      friend class write_session;
      void reset_reference_counts();

      friend class write_session;
      friend class read_session;

      struct database_memory
      {
         database_memory()
         {
            for (auto& r : top_root)
               r.store(0, std::memory_order_relaxed);
         }
         uint32_t          magic          = file_magic;
         uint32_t          flags          = file_type_database_root;
         std::atomic<bool> clean_shutdown = true;
         runtime_config    config;
         // top_root is protected by _root_change_mutex to prevent race conditions
         // which involve loading or storing top_root, bumping refcounts, decrementing
         // refcounts, cloning, and cleaning up node children when the refcount hits 0.
         // Since it's protected by a mutex, it normally wouldn't need to be atomic.
         // However, making it atomic hopefully aids SIGKILL behavior, which is impacted
         // by instruction reordering and multi-instruction non-atomic writes.
         std::atomic<uint64_t> top_root[num_top_roots];
      };

      mutable std::mutex _sync_mutex;
      mutable std::mutex _root_change_mutex[num_top_roots];
      mutable std::mutex _modify_lock[num_top_roots];

      std::mutex& modify_lock(int index) { return _modify_lock[index]; }

      seg_allocator    _sega;
      mapping          _dbfile;
      database_memory* _dbm;
   };

   template <typename NodeType>
   void retain_children(session_rlock& state, const NodeType* in);

   inline read_session::read_session(database* db)
       : _db(db), _segas(std::make_unique<seg_alloc_session>(db->_sega.start_session()))
   {
   }

   inline read_session::~read_session()
   {
      // unique_ptr will handle cleanup automatically
   }

   inline void release_node(object_ref&& r);
   inline void release_node(object_ref& r)
   {
      auto& state = r.rlock();

      auto release_id = [&](id_address b) { release_node(state.get(b)); };

      // release returns a pointer to the node if it needs to be released,
      // we must be very careful that the automatic hardware prefetching doesn't
      // try to fetch the node that we only access *if* it is actually released,
      // this was measured to have a HUGE performance impact if the pointer ever
      // materializes in a a hot cacheline the prefetcher just can't help itself
      if (auto n = r.release())
         cast_and_call_noinline(n, [&](const auto* ptr) { ptr->visit_branches(release_id); });
   }

   inline void release_node(object_ref&& r)
   {
      release_node(r);
   }

   inline int read_session::get(const node_handle&                      r,
                                key_view                                key,
                                std::invocable<bool, value_view> auto&& callback)
   {
      if (not r.address()) [[unlikely]]
      {
         callback(false, value_view());
         return -1;
      }
      int  size  = -1;
      auto state = _segas->lock();
      auto ref   = state.get(r.address());
      get(ref, key,
          [&](bool found, value_type vt)
          {
             if (found)
             {
                if (not vt.is_subtree())
                {
                   callback(true, vt.view());
                   size = vt.view().size();
                   return;
                }
             }
             callback(false, value_view());
          });
      return size;
   }

   inline std::optional<node_handle> read_session::get_subtree(const node_handle& r, key_view key)
   {
      if (not r.address()) [[unlikely]]
         return {};

      std::optional<node_handle> result;

      auto state = _segas->lock();
      auto ref   = state.get(r.address());
      get(ref, key,
          [&](bool found, value_type vt)
          {
             if (found)
             {
                if (vt.is_subtree())
                {
                   result = node_handle(*this, vt.subtree_address());
                }
             }
          });
      return result;
   }

   inline int read_session::get(const node_handle& r, key_view key, std::vector<char>* data)
   {
      int data_size = -1;
      this->get(r, key,
                [&](bool found, value_type v)
                {
                   if (found)
                   {
                      if (v.is_view())
                      {
                         data_size = v.view().size();
                         if (data)
                         {
                            data->resize(v.view().size());
                            memcpy(data->data(), v.view().data(), v.view().size());
                         }
                      }
                   }
                });
      return data_size;
   }

   int read_session::get(object_ref&                             root,
                         key_view                                key,
                         std::invocable<bool, value_type> auto&& callback)
   {
      return cast_and_call(root.header(),
                           [&](const auto* n) { return get(root, n, key, callback); });
   }

   /**
    * TODO: this method appears to be broken because key.size() == 0 
    * does not always mean eof_address when there is also a prefix
    * @bug: fix this
    */
   int read_session::get(object_ref&                             root,
                         const auto*                             inner,
                         key_view                                key,
                         std::invocable<bool, value_type> auto&& callback)
   {
      if (key.size() == 0)
      {
         if (auto val_node_id = inner->get_eof_address())
         {
            if (inner->is_eof_subtree())
            {
               callback(true, value_type::make_subtree(val_node_id));
            }
            else
            {
               auto vr = root.rlock().get(val_node_id);
               auto vn = vr.template as<value_node>();

               // a value node with a subtree value should have
               // been embedded at the inner_node::eof field
               assert(not vn->is_subtree());
               callback(true, value_type(vn->value()));
            }
            return 1;
         }
      }
      else
      {
         auto cpre = common_prefix(inner->get_prefix(), key);
         if (cpre == inner->get_prefix())
         {
            if (key.size() > cpre.size())
            {
               if (auto branch_id = inner->get_branch(char_to_branch(key[cpre.size()])))
               {
                  auto bref = root.rlock().get(branch_id);
                  return get(bref, key.substr(cpre.size() + 1), callback);
               }
            }
            else  // key is at the end of the prefix
            {
               if (auto branch_id = inner->get_eof_address())
               {
                  if (inner->is_eof_subtree())
                  {
                     callback(true, value_type::make_subtree(branch_id));
                     return 1;
                  }
                  auto bref = root.rlock().get(branch_id);
                  callback(true, bref.template as<value_node>()->value());
                  return 1;
               }
            }
         }
      }
      callback(false, value_type());
      return 0;
   }
   int read_session::get(object_ref&                             root,
                         const value_node*                       vn,
                         key_view                                key,
                         std::invocable<bool, value_type> auto&& callback)
   {
      callback(true, vn->get_value());
      return 1;
   }

   int read_session::get(object_ref&                             root,
                         const binary_node*                      bn,
                         key_view                                key,
                         std::invocable<bool, value_type> auto&& callback)
   {
      if (int idx = bn->find_key_idx(key, binary_node::key_hash(key)); idx >= 0)
      {
         // if( int idx = bn->find_key_idx( key ); idx >= 0 ) {
         auto kvp = bn->get_key_val_ptr(idx);
         switch (bn->get_value_type(idx))
         {
            case kv_index::inline_data:
               callback(true, kvp->value());
               return true;
            case kv_index::obj_id:
               callback(true, root.rlock()
                                  .get(id_address(kvp->value_id()))
                                  .template as<value_node>()
                                  ->value());
               return true;
            case kv_index::subtree:
               callback(true, value_type::make_subtree(kvp->value_id()));
               return true;
            case kv_index::remove:
               throw std::runtime_error("remove value type not supported");
         }
      }
      callback(false, value_type());
      return 0;
   }

   // NOTE This will currently recurse into subtree's which could create
   // infinite loop or over counting...
   // TODO: fix this so it doesn't recurse through subtrees
   void visit_node(object_ref&& n, int depth, auto& on_node)
   {
      assert(n.ref() > 0);
      cast_and_call(n.header(),
                    [&](const auto* no)
                    {
                       on_node(depth, no);
                       no->visit_branches([&](auto adr)
                                          { visit_node(n.rlock().get(adr), depth + 1, on_node); });
                    });
   }

   void read_session::visit_nodes(const node_handle& h, auto&& on_node)
   {
      auto state = _segas->lock();
      visit_node(state.get(h.address()), 0, on_node);
   }

   /**
    * This method should only be called by write_transaction objects.
    * 
    * @param r is passed by value, so it owns a reference,
    *        it gives this reference to the top root and the
    *        old top root's reference is taken over by r and
    *        returned so that the caller can control when it
    *        goes out of scope and resources are freed.
    */
   inline node_handle write_session::set_root(node_handle r, int index)
   {
      assert(index < num_top_roots);
      assert(index >= 0);

      uint64_t old_r;
      uint64_t new_r = r.take().to_int();
      {
         {  // lock scope
            std::unique_lock lock(_db->_root_change_mutex[index]);
            old_r = _db->_dbm->top_root[index].exchange(new_r, std::memory_order_relaxed);
         }
         if (old_r != new_r)
         {
            _db->_dbfile.sync(_db->_dbm->config.sync_mode);
         }
      }
      _db->modify_lock(index).unlock();
      return r.give(id_address(old_r));
   }

   /**
    * flushes state to disk according to sync_type,
    * only one thread may call this method at a time and it will
    * block until all msync() calls have returned. 
   inline void write_session::sync()
   {
      switch (_db->_dbm->config.sync_mode)
      {
         case sync_type::full:
         case sync_type::fsync:
            _db->_sega.fsync();
            _db->_dbfile.sync(_db->_dbm->config.sync_mode);
         default:
            return;
      }
   }
    */

   inline write_session::~write_session() {}
}  // namespace arbtrie

#include <arbtrie/iterator_impl.hpp>
