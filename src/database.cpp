#include <arbtrie/arbtrie.hpp>
#include <arbtrie/database.hpp>
#include <arbtrie/debug.hpp>
#include <arbtrie/file_fwd.hpp>
#include <arbtrie/transaction.hpp>

#include <cassert>
#include <iostream>
#include <utility>

namespace arbtrie
{
   template <typename NodeType, typename... CArgs>
   object_ref make(id_region                        reg,
                   alloc_hint                       hint,
                   session_rlock&                   state,
                   std::invocable<NodeType*> auto&& uinit,
                   CArgs&&... cargs)
   {
      auto asize     = NodeType::alloc_size(cargs...);
      auto make_init = [&](node_header* cl)
      {
         assert(cl->_nsize == asize);
         uinit(new (cl) NodeType(cl->_nsize, cl->address_seq(), cargs...));
      };
      return state.alloc(reg, hint, asize, NodeType::type, make_init);
   }
   template <typename NodeType, typename... CArgs>
   object_ref make(id_region reg, alloc_hint hint, session_rlock& state, CArgs&&... cargs)
   {
      return make<NodeType>(reg, hint, state, [](auto&&) {}, std::forward<CArgs>(cargs)...);
   }

   template <typename NodeType, typename... CArgs>
   object_ref remake(object_ref& r, std::invocable<NodeType*> auto&& uinit, CArgs&&... cargs)
   {
      auto asize     = NodeType::alloc_size(cargs...);
      auto make_init = [&](node_header* cl)
      { uinit(new (cl) NodeType(asize, cl->address_seq(), cargs...)); };

      auto modlock = r.modify();
      return r.rlock().realloc(r, asize, NodeType::type, make_init);
   }
   template <typename NodeType, typename... CArgs>
   object_ref remake(object_ref& r, CArgs&&... cargs)
   {
      return remake<NodeType>(r, [](auto&&) {}, std::forward<CArgs>(cargs)...);
   }

   template <typename NodeType>
   object_ref make(id_region           reg,
                   alloc_hint          hint,
                   session_rlock&      state,
                   const clone_config& cfg,
                   auto&&              uinit)
   {
      auto asize     = NodeType::alloc_size(cfg);
      auto make_init = [&](node_header* cl)
      {
         assert(cl->_nsize == asize);
         uinit(new (cl) NodeType(cl->_nsize, cl->address_seq(), cfg));
      };
      return state.alloc(reg, hint, asize, NodeType::type, make_init);
   }

   //===============================================
   // clone
   //  - makes a copy of r according to the config which
   //  informs the allocator how much extra space needs to
   //  be allocated.
   //  - src must equal r.as<NodeType>(), it is passed separately
   //    to avoid another atomic load.
   //  - the default configuration is a simple memcpy
   //  - if mode.is_shared() any referenced IDs will
   //    be retained and a new ID will be allocated
   //  - if mode.is_unique() then this is a effectively
   //  a realloc and the cloned node will replace the src.
   //
   //  - space will be allocated according to NodeType::alloc_size( src, cfg, CArgs...)
   //  - the constructor NodeType::NodeType( src, cfg, CArgs... ) is called
   //  - after construction, uinit(NodeType*) is called
   //
   //  uinit( new (alloc(NodeType::alloc_size(src,cfg,cargs...))) NodeType( src,cfg,cargs ) );
   // @param reg The region to allocate the node in
   // @param parent_hint The hint from the parent about what pages it would prefer for the new nodes'
   //             address to be on (so it is similar to its peers)
   //===============================================
   template <upsert_mode mode, typename NodeType, typename... CArgs>
   object_ref clone_impl(id_region                        reg,
                         const alloc_hint&                parent_hint,
                         object_ref&                      r,
                         const NodeType*                  src,
                         const clone_config&              cfg,
                         std::invocable<NodeType*> auto&& uinit,
                         CArgs&&... cargs)
   {
      if constexpr (mode.is_shared())
         retain_children(r.rlock(), src);

      auto asize = NodeType::alloc_size(src, cfg, cargs...);

      auto copy_init = [&](node_header* cl)
      {
         uinit(new (cl)
                   NodeType(asize, cl->address_seq(), src, cfg, std::forward<CArgs>(cargs)...));
      };

      if constexpr (mode.is_unique() and mode.is_same_region())
      {
         auto mod_lock = r.modify();
         return r.rlock().realloc(r /*r.address()*/, asize, src->get_type(), copy_init);
      }
      return r.rlock().alloc(reg, parent_hint, asize, src->get_type(), copy_init);
   }

   /**
    * @param parent_hint - new node should be on cacheline with sibilings from parent
    */
   template <upsert_mode mode, typename NodeType>
   object_ref clone(
       const alloc_hint&                parent_hint,
       object_ref&                      r,
       const NodeType*                  src,
       const clone_config&              cfg   = {},
       std::invocable<NodeType*> auto&& uinit = [](NodeType*) {})
   {
      return clone_impl<mode.make_same_region()>(r.address().region, parent_hint, r, src, cfg,
                                                 std::forward<decltype(uinit)>(uinit));
   }

   template <upsert_mode mode, typename NodeType>
   object_ref clone(
       id_region                        reg,
       const alloc_hint&                parent_hint,
       object_ref&                      r,
       const NodeType*                  src,
       const clone_config&              cfg   = {},
       std::invocable<NodeType*> auto&& uinit = [](NodeType*) {})
   {
      return clone_impl<mode>(reg, parent_hint, r, src, cfg, std::forward<decltype(uinit)>(uinit));
   }

   template <upsert_mode mode, typename NodeType, typename... CArgs>
   object_ref clone(object_ref&                      r,
                    const alloc_hint&                parent_hint,
                    const NodeType*                  src,
                    const clone_config&              cfg,
                    std::invocable<NodeType*> auto&& uinit,
                    CArgs&&... cargs)
   {
      return clone_impl<mode.make_same_region()>(r.address().region, parent_hint, r, src, cfg,
                                                 std::forward<decltype(uinit)>(uinit),
                                                 std::forward<CArgs>(cargs)...);
   }
   template <upsert_mode mode, typename NodeType, typename... CArgs>
   object_ref clone(id_region                        reg,
                    const alloc_hint&                parent_hint,
                    object_ref&                      r,
                    const NodeType*                  src,
                    const clone_config&              cfg,
                    std::invocable<NodeType*> auto&& uinit,
                    CArgs&&... cargs)
   {
      return clone_impl<mode>(reg, parent_hint, r, src, cfg, std::forward<decltype(uinit)>(uinit),
                              std::forward<CArgs>(cargs)...);
   }

   template <upsert_mode mode, typename NodeType, typename... CArgs>
   object_ref clone(const alloc_hint&   parent_hint,
                    object_ref&         r,
                    const NodeType*     src,
                    const clone_config& cfg,
                    CArgs&&... cargs)
   {
      return clone_impl<mode.make_same_region()>(
          r.address().region, parent_hint, r, src, cfg, [](auto) {}, std::forward<CArgs>(cargs)...);
   }

   template <upsert_mode mode, typename NodeType, typename... CArgs>
   object_ref clone(id_region           reg,
                    const alloc_hint&   parent_hint,
                    object_ref&         r,
                    const NodeType*     src,
                    const clone_config& cfg,
                    CArgs&&... cargs)
   {
      return clone_impl<mode>(
          reg, parent_hint, r, src, cfg, [](auto) {}, std::forward<CArgs>(cargs)...);
   }

   uint8_t object_header::calculate_checksum() const
   {
      return cast_and_call((const node_header*)this,
                           [&](const auto* t) { return t->calculate_checksum(); });
   }

   node_handle write_session::get_mutable_root(int index)
   {
      assert(index < num_top_roots);
      assert(index >= 0);

      // prevent other threads from modifying the root while we are
      _db->modify_lock(index).lock();

      // this lock is not needed because anyone who would modify the root
      // already has modify lock, we don't need to grab this lock until
      // we go to set the root on commit to synchronize with the readers
      // std::unique_lock lock(_db->_root_change_mutex[index]);

      return node_handle(
          *this, id_address::from_int(_db->_dbm->top_root[index].load(std::memory_order_relaxed)));
   }

   node_handle read_session::get_root(int index)
   {
      assert(index < num_top_roots);
      assert(index >= 0);

      // must take the lock to prevent a race condition around
      // retaining the current top root... otherwise we must
      //     read the current top root address,
      //     read / lookup the meta for the address
      //     attempt to retain the meta while making sure the top root
      //     hasn't changed, the lock is probably faster anyway
      std::unique_lock lock(_db->_root_change_mutex[index]);

      return node_handle(
          *this, id_address::from_int(_db->_dbm->top_root[index].load(std::memory_order_relaxed)));
   }

   database::database(std::filesystem::path dir, runtime_config cfg, access_mode mode)
       : _sega(dir, cfg), _dbfile{dir / "db", mode}
   {
      if (_dbfile.size() == 0)
      {
         _dbfile.resize(sizeof(database_memory));
         new (_dbfile.data()) database_memory();
      }

      _dbm = reinterpret_cast<database_memory*>(_dbfile.data());

      if (_dbfile.size() != sizeof(database_memory))
         throw std::runtime_error("Wrong size for file: " + (dir / "db").native());

      if (_dbm->magic != file_magic)
         throw std::runtime_error("Not a arbtrie file: " + (dir / "db").native());

      if (not _dbm->clean_shutdown)
      {
         ARBTRIE_WARN("database was not shutdown cleanly, memory may have leaked");
         ARBTRIE_DEBUG("validating database state");
         std::cerr << std::setw(5) << "root"
                   << " | " << std::setw(10) << "nodes"
                   << " | " << std::setw(10) << " size (MB) "
                   << " | " << std::setw(10) << " avg depth  "
                   << "\n";
         auto s = start_read_session();
         for (int i = 0; i < num_top_roots; ++i)
         {
            auto r = s.get_root(i);
            if (r.address())
            {
               auto stat = s.get_node_stats(r);
               std::cerr << std::setw(5) << i << " | " << stat << "\n";
            }
         }
      }
      set_runtime_config(cfg);
      _dbm->clean_shutdown = false;
   }

   database::~database()
   {
      _dbm->clean_shutdown = true;
      _dbfile.sync(sync_type::full);
   }

   std::shared_ptr<database> database::create(std::filesystem::path dir, runtime_config cfg)
   {
      if (std::filesystem::exists(std::filesystem::symlink_status(dir / "db")) ||
          std::filesystem::exists(std::filesystem::symlink_status(dir / "data")))
         throw std::runtime_error("directory already exists: " + dir.generic_string());

      std::filesystem::create_directories(dir / "data");

      return std::make_shared<database>(dir, cfg, access_mode::read_write);
   }

   void database::print_stats(std::ostream& os, bool detail)
   {
      os << _sega.dump();
   }

   std::shared_ptr<write_session> database::start_write_session()
   {
      // Use the static factory method instead of make_shared
      return write_session::create(this);
   }

   read_session database::start_read_session()
   {
      return read_session(this);
   }

   template <typename NodeType>
   void retain_children(session_rlock& state, const NodeType* in)
   {
      if constexpr (is_full_node<NodeType>)
      {
         const auto* br      = in->branches();
         using ptr           = sal::shared_ptr*;
         const int nprefetch = 8;
         ptr       ptrs[full_node::branch_count + 1];
         int       numptrs = 0;
         auto      r       = in->branch_region();

         if (in->has_eof_value())
            ptrs[numptrs++] = &state.get(in->_eof_value).meta();

         for (int i = 0; i < full_node::branch_count; i++)
         {
            if (auto b = br[i])
            {
               sal::shared_ptr* p = &state.get(id_address(r, b)).meta();
               ptrs[numptrs++]    = p;
            }
         }
         const int np = numptrs;

         // How many pointers ahead to prefetch (adjust based on profiling)
         const int prefetch_distance = 8;
         const int unroll_factor     = 4;  // Process this many per loop iteration.

         int p = 0;

         // --- Prefetch Initial Batch ---
         // Prefetch the first 'prefetch_distance' elements.
         int initial_prefetch_count = (np < prefetch_distance) ? np : prefetch_distance;
         for (int k = 0; k < initial_prefetch_count; ++k)
         {
            // Prefetch for write (retain likely modifies refcount) with high temporal locality.
            __builtin_prefetch((const void*)ptrs[k], 1 /*write*/, 3 /*high locality*/);
         }

         // --- Main Unrolled Loop ---
         // Process blocks of 'unroll_factor' (4) pointers.
         // Loop condition ensures we have enough elements for the current block (4)
         // AND the block we want to prefetch (another 4, 'prefetch_distance' ahead).
         for (; p + unroll_factor + prefetch_distance <= np;
              p += unroll_factor)  // p + 4 + prefetch_distance <= np
         {
            // Prefetch the block 'prefetch_distance' elements ahead
            __builtin_prefetch((const void*)ptrs[p + prefetch_distance + 0], 1, 3);
            __builtin_prefetch((const void*)ptrs[p + prefetch_distance + 1], 1, 3);
            __builtin_prefetch((const void*)ptrs[p + prefetch_distance + 2], 1, 3);
            __builtin_prefetch((const void*)ptrs[p + prefetch_distance + 3], 1, 3);

            // Process (retain) the current block of 'unroll_factor' (4) pointers
            ptrs[p + 0]->retain();
            ptrs[p + 1]->retain();
            ptrs[p + 2]->retain();
            ptrs[p + 3]->retain();
         }

         // --- Handle Remaining Prefetches (Near the End) ---
         // Prefetch the elements that the next loop ('Remainder Loop 1') will process.
         int remaining_prefetch_start = p + prefetch_distance;
         for (int k = 0; k < unroll_factor && remaining_prefetch_start + k < np; ++k)
         {  // k < 4
            __builtin_prefetch((const void*)ptrs[remaining_prefetch_start + k], 1, 3);
         }

         // --- Remainder Loop 1 (Unrolled Processing without Prefetching) ---
         // Process remaining full blocks of 'unroll_factor' (4) without issuing new prefetches.
         for (; p + unroll_factor <= np; p += unroll_factor)  // p + 4 <= np
         {
            ptrs[p + 0]->retain();
            ptrs[p + 1]->retain();
            ptrs[p + 2]->retain();
            ptrs[p + 3]->retain();
         }

         // --- Remainder Loop 2 (Single Element Processing) ---
         // Process any final elements that didn't form a full 'unroll_factor' (4) block.
         for (; p < np; ++p)
         {
            ptrs[p]->retain();
         }

         //for (int p = 0; p < np; ++p)
         //   ptrs[p]->retain();
      }
      else
      {
         in->visit_branches([&](auto bid) { state.get(bid).retain(); });
      }
   }

   id_address make_value(id_region         reg,
                         alloc_hint        hint,
                         session_rlock&    state,
                         const value_type& val)
   {
      return make<value_node>(reg, hint, state, [](auto) {}, val).address();
   }
   std::optional<node_handle> write_session::upsert(node_handle& r, key_view key, node_handle sub)
   {
      _delta_keys = 0;

      // old handle will be written here, if any is found, reset it to null
      _old_handle.reset();
      // retain the handle here until its address is safely stored
      _new_handle = std::move(sub);

      auto state = _segas->lock();

      r.give(upsert(state, r.take(), key, value_type::make_subtree(_new_handle->address())));

      // it has been stored without exception; therefore we can take it
      _new_handle->take();
      return std::move(_old_handle);
   }

   int write_session::upsert(node_handle& r, key_view key, value_view val)
   {
      _delta_keys     = 0;
      _old_value_size = -1;
      auto state      = _segas->lock();

      id_address adr = r.take();
      try
      {
         r.give(upsert(state, adr, key, val));
      }
      catch (...)
      {
         r.give(adr);
         throw;
      }
      return _old_value_size;
   }

   void write_session::insert(node_handle& r, key_view key, value_view val)
   {
      _delta_keys     = 0;
      _old_value_size = -1;
      auto state      = _segas->lock();

      r.give(insert(state, r.take(), key, val));
   }

   int write_session::update(node_handle& r, key_view key, value_view val)
   {
      _delta_keys     = 0;
      _old_value_size = -1;
      auto state      = _segas->lock();
      r.give(update(state, r.take(), key, val));
      return _old_value_size;
   }

   int write_session::remove(node_handle& r, key_view key)
   {
      _delta_keys     = 0;
      _old_value_size = -1;
      auto state      = _segas->lock();
      r.give(remove(state, r.take(), key));
      return _old_value_size;
   }

   id_address write_session::upsert(session_rlock&    state,
                                    id_address        root,
                                    key_view          key,
                                    const value_type& val)
   {
      if (not root) [[unlikely]]
         return make<value_node>(state.get_new_region(), alloc_hint::any(), state, key, val)
             .address();
      _cur_val = val;
      return upsert<upsert_mode::unique_upsert>(state.get(root), key, alloc_hint::any());
   }
   id_address write_session::insert(session_rlock&    state,
                                    id_address        root,
                                    key_view          key,
                                    const value_type& val)
   {
      if (not root) [[unlikely]]
         return make<value_node>(state.get_new_region(), alloc_hint::any(), state, key, val)
             .address();
      _cur_val = val;
      return upsert<upsert_mode::unique_insert>(state.get(root), key, alloc_hint::any());
   }
   id_address write_session::update(session_rlock&    state,
                                    id_address        root,
                                    key_view          key,
                                    const value_type& val)
   {
      if (not root) [[unlikely]]
         throw std::runtime_error("cannot update key that doesn't exist");
      _cur_val = val;
      return upsert<upsert_mode::unique_update>(state.get(root), key, alloc_hint::any());
   }
   id_address write_session::remove(session_rlock& state, id_address root, key_view key)
   {
      if (not root) [[unlikely]]
         return root;  //throw std::runtime_error("cannot remove key that doesn't exist");
      return upsert<upsert_mode::unique_remove>(state.get(root), key, alloc_hint::any());
   }

   template <upsert_mode mode>
   id_address write_session::upsert(object_ref&& root, key_view key, const alloc_hint& parent_hint)
   {
      return upsert<mode>(root, key, parent_hint);
   }
   /**
    *  Inserts key under root, if necessary 
    */
   template <upsert_mode mode>
   id_address write_session::upsert(object_ref& root, key_view key, const alloc_hint& parent_hint)
   {
      if constexpr (mode.is_unique())
      {
         if (root.ref() != 1)
         {
            assert(root.ref() != 0);
            return upsert<mode.make_shared()>(root, key, parent_hint);
         }
      }

      id_address         result;
      const node_header* nh = root.header();
      switch (nh->get_type())
      {
         case node_type::value:
            result = upsert_value<mode>(root, nh->as<value_node>(), key, parent_hint);
            break;
         case node_type::binary:
            result = upsert_binary<mode>(root, nh->as<binary_node>(), key, parent_hint);
            break;
         case node_type::setlist:
            result =
                upsert_inner<mode, setlist_node>(root, nh->as<setlist_node>(), key, parent_hint);
            break;
         case node_type::full:
            result = upsert_inner<mode, full_node>(root, nh->as<full_node>(), key, parent_hint);
            break;
         default:
            throw std::runtime_error("unhandled type in upsert");
      }
      if constexpr (mode.is_remove())
      {
         if (result != root.address())
            release_node(root);
      }
      else if constexpr (mode.is_shared())
      {
         assert((result != root.address()));
         release_node(root);
      }

      return result;
   }

   template <upsert_mode mode>
   id_address upsert(object_ref&& root, key_view key)
   {
      return upsert<mode>(root, key);
   }

   //================================
   //  upsert_value
   //
   //  Given a value node, if the key is the same then this will
   //  update the data, otherwise if there is a key collision it will
   //  split into a binary node containing both keys
   //================================
   template <upsert_mode mode>
   id_address write_session::upsert_value(object_ref&       root,
                                          const value_node* vn,
                                          key_view          key,
                                          const alloc_hint& hint)
   {
      auto& state        = root.rlock();
      auto  new_val_size = _cur_val.size();

      if constexpr (mode.is_remove())
      {
         //   ARBTRIE_WARN(root.address(), " remove value ", to_hex(key));
         if (vn->key() == key)
         {
            _old_value_size = vn->value_size();
            _delta_keys     = -1;
            return id_address();
         }
         if constexpr (mode.must_remove())
            throw std::runtime_error("attempt to remove key that does not exist");
         return root.address();
      }

      if (vn->key() == key)
      {
         _old_value_size = vn->value_size();
         if constexpr (mode.is_unique())
         {
            if (vn->value_capacity() >= new_val_size)
            {
               id_address old_subtree = root.modify().as<value_node>()->set_value(_cur_val);
               if (old_subtree)
               {
                  // TODO: this could be an expensive operation, say a snapshot with
                  // a days worth of modifications.  Perhaps subtrees should always be
                  // "defer-release" so the compactor can clean it up
                  //
                  // TODO: we could implement a try_fast_release via compare and swap
                  //       and if we are not the final release then we are done, else
                  //       if it looks like we would be the final release then postpone
                  release_node(state.get(old_subtree));
               }
               return root.address();
            }
            //ARBTRIE_DEBUG( "remake value node" );
            return remake<value_node>(root, key, _cur_val).address();
         }
         else  // shared
         {
            /// ideally we would use the hint from the parent here... but there isn't always a
            /// parent....
            return make<value_node>(root.address().region, hint, state, key, _cur_val).address();
         }
      }
      else  // add key
      {
         _delta_keys = 1;

         // convert to binary node
         key_view   k1      = vn->key();
         value_type v1      = vn->get_value();
         auto       t1      = vn->is_subtree() ? binary_node::key_index::subtree
                                               : binary_node::key_index::inline_data;
         key_view   k2      = key;
         value_type v2      = _cur_val;
         auto       t2      = _cur_val.is_subtree() ? binary_node::key_index::subtree
                                                    : binary_node::key_index::inline_data;
         auto       new_reg = state.get_new_region();

         if constexpr (use_binary_nodes)
         {
            if (not binary_node::can_inline(vn->value_size()))
            {
               /// we have to use any hint here because this is a new region
               v1 = value_type::make_value_node(
                   make<value_node>(new_reg, alloc_hint::any(), state, v1).address());
               t1 = binary_node::key_index::obj_id;
            }

            if (not binary_node::can_inline(v2.size()))
            {
               /// we have to use any hint here because this is a new region
               v2 = value_type::make_value_node(
                   make<value_node>(new_reg, alloc_hint::any(), state, v2).address());
               t2 = binary_node::key_index::obj_id;
            }

            // if unique, reuse id of value node
            if constexpr (mode.is_unique())
            {
               return remake<binary_node>(root, clone_config{.data_cap = binary_node_initial_size},
                                          new_reg, k1, v1, t1, k2, v2, t2)
                   .address();
            }
            else  // shared
            {
               return make<binary_node>(root.address().region, hint, state,
                                        clone_config{.data_cap = binary_node_initial_size}, new_reg,
                                        k1, v1, t1, k2, v2, t2)
                   .address();
            }
         }
         else  // don't use binary nodes
         {
            static_assert(false, "TODO: handle subtrees properly");
#if 0
            auto cpre    = common_prefix(k1,k2);
            if( k1 > k2 ) {
               std::swap( k1,k2 );
               std::swap( v1,v2 );
               std::swap( t1,t2 );
            }

            if( k1.size() > cpre.size() ) {
               v1 = make<value_node>( new_reg, state, k1.substr(cpre.size()+1), v1) .address();
            } else { // eof value
               v1 = make<value_node>( new_reg, state, v1) .address();
               assert( k2.size() > k1.size() ); // only one eof value or keys would be same
            }

            v2 = make<value_node>( new_reg, state, k2.substr(cpre.size()+1), v2) .address();

            if constexpr ( mode.is_unique() ) 
            {
               return remake<setlist_node>(root, 
                                           [&]( setlist_node* sln ) {
                                           sln->set_branch_region( new_reg );
                                              if( not k1.size() )
                                                sln->set_eof( v1.id() );
                                              else 
                                                sln->add_branch( char_to_branch(k1[cpre.size()]),
                                                                 v1.id() );
                                             sln->add_branch( char_to_branch(k2[cpre.size()]),
                                                              v2.id() );
                                           },
                                           clone_config{.branch_cap=2, .set_prefix=cpre}
                                           ).address();
            }
            else  // shared
            {
               return make<setlist_node>(root.address().region().to_int(), state, 
                                           [&]( setlist_node* sln ) {
                                           sln->set_branch_region( new_reg );
                                              if( not k1.size() )
                                                sln->set_eof( v1.id() );
                                              else 
                                                sln->add_branch( char_to_branch(k1[cpre.size()]),
                                                                 v1.id() );
                                             sln->add_branch( char_to_branch(k2[cpre.size()]),
                                                              v2.id() );
                                           },
                                         clone_config{.branch_cap=2, .set_prefix=cpre}
                                           ).address();
            }
#endif
         }
      }
   }

   /**
    * EOF values cannot be converted to binary or have other child branches, so
    * they have a unique upsert function compared to upserting when a branch of the
    * tree is a value node with a key.  
    */
   template <upsert_mode mode>
   id_address write_session::upsert_eof_value(object_ref& root)
   {
      if constexpr (mode.is_unique())
      {
         if (root.ref() > 1)
            return upsert_eof_value<mode.make_shared()>(root);
      }

      auto& state = root.rlock();

      // this should be handled at the inner node layer
      assert(not _cur_val.is_subtree());

      const node_header* nh = root.header();
      if constexpr (mode.is_shared())
      {
         auto vn         = nh->as<value_node>();
         _old_value_size = vn->value_size();
         // because it gets an entire new region, we have nothing to hint
         return make_value(state.get_new_region(), alloc_hint::any(), root.rlock(), _cur_val);
      }
      else if constexpr (mode.is_unique())
      {
         assert(root.ref() == 1);
         if (nh->get_type() == node_type::value)
         {
            auto vn         = nh->as<value_node>();
            _old_value_size = vn->value_size();

            assert(_cur_val.is_view());
            auto vv = _cur_val.view();
            if (vn->value_capacity() >= _cur_val.size())
            {
               //  ARBTRIE_DEBUG("update in place");
               root.modify().as<value_node>()->set_value(vv);
               return root.address();
            }
            //ARBTRIE_DEBUG("remake value to make more space");
            return remake<value_node>(root, key_view{}, _cur_val).address();
         }
         else  // replacing a subtree with some other kind of value
         {
            // because it gets an entire new region, we have nothing to hint
            return make_value(state.get_new_region(), alloc_hint::any(), root.rlock(), _cur_val);
         }
      }
   }

   template <upsert_mode mode>
   id_address clone_binary_range(id_region          reg,
                                 const alloc_hint&  hint,
                                 object_ref&        r,
                                 const binary_node* src,
                                 key_view           minus_prefix,
                                 int                from,
                                 int                to)
   {
      auto nbranch       = to - from;
      int  total_kv_size = 0;
      for (auto i = from; i < to; ++i)
      {
         auto kvp = src->get_key_val_ptr(i);
         total_kv_size += kvp->total_size();
      }
      assert(total_kv_size > minus_prefix.size() * nbranch);
      total_kv_size -= minus_prefix.size() * nbranch;

      auto& state       = r.rlock();
      auto  init_binary = [&](binary_node* bn)
      {
         bn->_branch_id_region = state.get_new_region();

         for (int i = from; i < to; ++i)
         {
            auto kvp  = src->get_key_val_ptr(i);
            auto nkey = kvp->key().substr(minus_prefix.size());
            auto nkh  = binary_node::key_header_hash(binary_node::key_hash(nkey));

            // if shared we need to retain any obj_ids/subtrees we migrate
            if constexpr (mode.is_shared())
            {
               if (src->is_obj_id(i))
               {
                  state.get(kvp->value_id()).retain();
               }
            }

            bn->append(kvp, minus_prefix.size(), nkh, src->value_hashes()[i],
                       src->key_offsets()[i].val_type());
         }
      };
      return make<binary_node>(reg, hint, state, {.branch_cap = nbranch, .data_cap = total_kv_size},
                               init_binary)
          .address();
   }

   //================================================
   //  refactor
   //
   //  Given a binary node, if unique, turn it into a radix node,
   //  else return a clone
   //=================================================
   template <upsert_mode mode>
   object_ref refactor(object_ref& r, const binary_node* root, const alloc_hint& parent_hint)
   {
      //ARBTRIE_WARN("REFACTOR! ", r.address());
      assert(root->num_branches() > 1);
      auto first_key     = root->get_key(0);
      auto last_key      = root->get_key(root->num_branches() - 1);
      auto cpre          = common_prefix(first_key, last_key);
      bool has_eof_value = first_key.size() == cpre.size();

      int_fast16_t nbranch = has_eof_value;
      int_fast16_t freq_table[256];

      memset(freq_table, 0, sizeof(freq_table));

      auto numb = root->num_branches();
      for (uint32_t i = has_eof_value; i < numb; ++i)
      {
         auto k = root->get_key(i);
         assert(k.size() > cpre.size());
         uint8_t f = k[cpre.size()];
         nbranch += freq_table[f] == 0;
         freq_table[f]++;
      }

      auto bregion = r.rlock().get_new_region();

      uint8_t    branches_byte[nbranch - has_eof_value];
      id_index   branches_index[nbranch - has_eof_value];
      auto*      next_branch_byte  = branches_byte;
      auto*      next_branch_index = branches_index;
      alloc_hint hint(bregion, next_branch_index, 0);

      for (int from = has_eof_value; from < numb;)
      {
         const auto    k    = root->get_key(from);
         const uint8_t byte = k[cpre.size()];
         const int     to   = from + freq_table[byte];

         // ARBTRIE_DEBUG( "branch: ", to_hex(byte), " key: ", to_hex(k) );
         id_address new_child;
         if (to - from > 1)
            new_child = clone_binary_range<mode>(bregion, hint, r, root,
                                                 k.substr(0, cpre.size() + 1), from, to);
         else
         {
            auto kvp = root->get_key_val_ptr(from);
            if (root->is_subtree(from))
            {
               new_child =
                   make<value_node>(bregion, hint, r.rlock(), kvp->key().substr(cpre.size() + 1),
                                    value_type::make_subtree(kvp->value_id()))
                       .address();
               //auto cval = r.rlock().get(kvp->value_id());
               if constexpr (mode.is_shared())
               {
                  // TODO: is this tested?
                  r.rlock().get(kvp->value_id()).retain();
               }
            }
            else if (root->is_obj_id(from))
            {
               // TODO: when mode is unique we may be able to use remake rather than make; however,
               //       we must make sure that the reference count isn't lost and that the
               //       value node also has a ref count of 1... may have to retain it in that event
               //       so that when the binary node is released it goes back to 1... for the time being
               //       we will just allocate a new value node and let the binary node clean up the old
               //       binary node at the expense of an extra ID allocation.
               auto cval = r.rlock().get(kvp->value_id());
               new_child =
                   make<value_node>(bregion, hint, r.rlock(), kvp->key().substr(cpre.size() + 1),
                                    cval.as<value_node>()->value())
                       .address();
               if constexpr (mode.is_unique())
                  release_node(cval);
            }
            else
            {
               //   ARBTRIE_WARN( ".... make new value node" );
               new_child =
                   make<value_node>(bregion, hint, r.rlock(), kvp->key().substr(cpre.size() + 1),
                                    root->get_value(from))
                       .address();
            }
         }
         from = to;

         assert(next_branch_byte < branches_byte + nbranch);
         *next_branch_byte  = byte;
         *next_branch_index = new_child.index;
         ++next_branch_byte;
         ++next_branch_index;
         hint.count++;
         //fn->add_branch(uint16_t(byte) + 1, new_child);
      }
      //ARBTRIE_WARN( "next - start: ", next_branch - branches, "  nb: ", nbranch );
      id_address eof_val;
      if (has_eof_value) [[unlikely]]
         eof_val = root->is_obj_id(0) ? root->get_key_val_ptr(0)->value_id()
                                      : make_value(bregion, alloc_hint::any(), r.rlock(),
                                                   root->get_key_val_ptr(0)->value());

      // this branch requires many small keys and small values
      if (nbranch > 128)
      {
         auto init_full = [&](full_node* fn)
         {
            fn->set_branch_region(bregion);
            fn->set_eof(eof_val);
            fn->set_descendants(root->_num_branches);
            //ARBTRIE_DEBUG( "num_br: ", root->_num_branches, " desc: ", fn->descendants() );
            for (int i = 0; i < nbranch; ++i)
               fn->add_branch(branch_index_type(branches_byte[i]) + 1,
                              id_address(bregion, branches_index[i]));
         };
         if constexpr (mode.is_unique())
         {
            return remake<full_node>(r, init_full, clone_config{.set_prefix = cpre});
         }
         else
         {
            return make<full_node>(r.address().region, parent_hint, r.rlock(), {.set_prefix = cpre},
                                   init_full);
         }
      }

      auto init_setlist = [&](setlist_node* sl)
      {
         sl->set_branch_region(bregion);
         assert(sl->_num_branches == 0);
         assert(sl->branch_capacity() >= nbranch - has_eof_value);

         sl->_num_branches = nbranch - has_eof_value;
         sl->set_eof(eof_val);
         sl->set_descendants(root->_num_branches);

         int nb = sl->_num_branches;
         for (int i = 0; i < nb; ++i)
         {
            sl->set_index(i, branches_byte[i], id_address(bregion, branches_index[i]));
         }
         assert(sl->get_setlist_size() == nb);
         assert(sl->validate());
      };

      if constexpr (mode.is_unique())
         return remake<setlist_node>(r, init_setlist,
                                     clone_config{.branch_cap = nbranch, .set_prefix = cpre});
      else
         return make<setlist_node>(r.address().region, parent_hint, r.rlock(),
                                   {.branch_cap = nbranch, .set_prefix = cpre}, init_setlist);
   }  // namespace arbtrie
   template <upsert_mode mode, typename NodeType>
   object_ref refactor_to_full(const alloc_hint& parent_hint,
                               object_ref&       r,
                               const NodeType*   src,
                               auto              init)
   {
      auto init_fn = [&](full_node* fn)
      {
         fn->set_branch_region(src->branch_region());
         fn->set_descendants(src->descendants());
         src->visit_branches_with_br(
             [&](short br, id_address oid)
             {
                // TODO: it should be possible to remove this branch
                // because it will always mispredict once
                if (br) [[likely]]
                   fn->add_branch(br, oid);
                else
                   fn->set_eof(oid);
                if constexpr (mode.is_shared())
                   r.rlock().get(oid).retain();
             });
         init(fn);
         assert(fn->num_branches() >= full_node_threshold);
      };
      if constexpr (mode.is_unique())
         return remake<full_node>(r, init_fn, clone_config{.set_prefix = src->get_prefix()});
      else
         return make<full_node>(r.address().region, parent_hint, r.rlock(),
                                {.set_prefix = src->get_prefix()}, init_fn);
   }

   // Example Transformation:
   //
   //  "prefix" ->  (in region R1 because parent needs it to be)
   //     (children in R3 because it must be different than R1)
   //     [1] ->
   //     [2] ->
   //     [3] ->
   //     [N] ->
   //
   //  insert "postfix" = "value"
   //
   //  "p" (setlist) (in R1 because it's parent needs it to be)
   //      (branches can be any region but R1 or R3)
   //     [r] -> "efix" [0-N] branches
   //           - if unique, reassign ID to R2 but don't move object
   //           - if shared, clone into R2
   //     [o] -> value_node (in R2) because cannot be same as parent)
   //           "stfix" = "value"
   //

   template <upsert_mode mode, typename NodeType>
   id_address write_session::upsert_prefix(object_ref&       r,
                                           key_view          key,
                                           key_view          cpre,
                                           const NodeType*   fn,
                                           key_view          rootpre,
                                           const alloc_hint& parent_hint)
   {
      auto& state = r.rlock();
      if constexpr (mode.is_remove())
      {
         if constexpr (mode.must_remove())
            throw std::runtime_error("attempt to remove key that does not exist");
         return r.address();
      }
      if constexpr (mode.must_update())
      {
         throw std::runtime_error("attempt to update key that does not exist");
      }
      //   ARBTRIE_DEBUG("KEY DOESN'T SHARE PREFIX  node prelen: ", rootpre.size(), "  cprelen: ", cpre.size());
      //  ARBTRIE_WARN( "root prefix: ", to_hex( rootpre ) );
      //  ARBTRIE_WARN( "insert: ", to_hex(key) );

      assert(cpre.size() < rootpre.size());

      auto new_reg = state.get_new_region();
      // ARBTRIE_DEBUG( "New Region: ", new_reg );
      // get a new region that isn't the same as the current region or the branch region
      while (new_reg == r.address().region or new_reg == fn->branch_region()) [[unlikely]]
         new_reg = state.get_new_region();

      // there is a chance that r.modify() will rewrite the data
      // pointed at by rootpre... so we should read what we need first
      char root_prebranch = rootpre[cpre.size()];

      id_address child_id;

      // the compactor needs to be smart enough to detect
      // when the node's address has changed before we can take
      // this path
      if constexpr (false and mode.is_unique())
      {
#if 0
         // because the new setlist root must have a different branch region,
         // we need to reassign the meta_address for the current root, this is
         // safe because ref count is 1
         auto allocation = state.get_new_meta_node(new_reg);
         r.modify().as<NodeType>(
             [&](auto* n)
             {
                allocation.meta.store(r.meta_data(), std::memory_order_release);
                n->set_prefix(rootpre.substr(cpre.size() + 1));
                n->set_address(allocation.address);
             });
         state.free_meta_node(r.address());
         child_id = allocation.address;
#endif
      }
      else  // shared state
      {
         //     ARBTRIE_DEBUG(" moving root to child shared ");
         auto new_prefix = rootpre.substr(cpre.size() + 1);
         auto cl         = clone<mode.make_shared()>(new_reg, alloc_hint::any(), r, fn,
                                                     {.set_prefix = new_prefix});
         child_id        = cl.address();

         // release old root if unique because it doesn't happen automatically for unique;
         if constexpr (mode.is_unique())
            release_node(r);
      }

      if (key.size() == cpre.size())
      {  // eof
         //   ARBTRIE_DEBUG("  value is on the node (EOF)");
         _delta_keys = 1;
         /// alloc_hint should be the same as child_id.index...
         auto v = make_value(child_id.region, alloc_hint(child_id.region, &child_id.index), state,
                             _cur_val);
         // must be same region as r because we can't cange the region our parent
         // put this node in.
         return make<setlist_node>(r.address().region, parent_hint, state,
                                   {.branch_cap = 2, .set_prefix = cpre},
                                   [&](setlist_node* sln)
                                   {
                                      sln->set_branch_region(child_id.region);
                                      sln->set_eof(v);
                                      sln->add_branch(char_to_branch(root_prebranch), child_id);
                                      sln->set_descendants(1 + fn->descendants());
                                   })
             .address();
      }
      else
      {  // branch node
         //       ARBTRIE_DEBUG("  two branch child, cpre: ", to_hex(cpre), "  key: ", to_hex(key),
         //                     " rpre: ", to_hex(rootpre));
         //
         // NOTE: make_binary with 1 key currently makes a value_node.. TODO: rename
         auto abx    = make_binary(child_id.region, alloc_hint(child_id.region, &child_id.index),
                                   state, key.substr(cpre.size() + 1), _cur_val);
         _delta_keys = 1;
         return make<setlist_node>(r.address().region, parent_hint, state,
                                   {.branch_cap = 2, .set_prefix = cpre},
                                   [&](setlist_node* sln)
                                   {
                                      sln->set_branch_region(new_reg);
                                      std::pair<branch_index_type, id_address> brs[2];
                                      sln->set_descendants(1 + fn->descendants());

                                      auto order  = key[cpre.size()] < root_prebranch;
                                      brs[order]  = {char_to_branch(key[cpre.size()]), abx};
                                      brs[!order] = {char_to_branch(root_prebranch), child_id};

                                      sln->add_branch(brs[0].first, brs[0].second);
                                      sln->add_branch(brs[1].first, brs[1].second);
                                   })
             .address();
      }
   }  // upsert_prefix

   /**
    * NodeType should be an inner_node and the current value should be inserted at
    * the eof position on this node.
    */
   template <upsert_mode mode, typename NodeType>
   id_address write_session::upsert_eof(const alloc_hint& parent_hint,
                                        object_ref&       r,
                                        const NodeType*   fn)
   {
      if constexpr (mode.is_remove())
         return remove_eof<mode, NodeType>(parent_hint, r, fn);

      auto& state = r.rlock();

      //  ARBTRIE_WARN("upsert value node on inner node");
      if (fn->has_eof_value())  //val_nid)
      {
         id_address val_nid = fn->get_eof_address();
         auto       old_val = state.get(val_nid);
         if constexpr (mode.is_unique())
         {
            if (_cur_val.is_subtree())
            {
               r.modify().as<NodeType>()->set_eof_subtree(_cur_val.subtree_address());
               release_node(old_val);
            }
            else
            {
               //ARBTRIE_DEBUG("... upsert_value<mode>\n");
               id_address new_id = upsert_eof_value<mode>(old_val);
               if (new_id != val_nid)
               {
                  r.modify().as<NodeType>()->set_eof(new_id);
                  release_node(old_val);
               }
            }
            return r.address();
         }
         else  // shared node
         {
            if (_cur_val.is_subtree())
            {
               auto cref = clone<mode>(parent_hint, r, fn, {.branch_cap = 16}, [&](auto cl)
                                       { cl->set_eof_subtree(_cur_val.subtree_address()); });
               release_node(old_val);
               return cref.address();
            }
            else
            {
               //  ARBTRIE_WARN("upsert value node on inner node");
               auto new_id = upsert_eof_value<mode>(old_val);
               assert(new_id != val_nid);  // because shared state
               auto cref = clone<mode>(parent_hint, r, fn, {.branch_cap = 16},
                                       [&](auto cl) { cl->set_eof(new_id); });
               release_node(old_val);
               return cref.address();
            }
         }
      }
      else  // there is no value stored here
      {
         _delta_keys = 1;
         if (_cur_val.is_subtree())
         {
            if constexpr (mode.is_unique())
            {
               r.modify().template as<NodeType>(
                   [this](auto p)
                   {
                      p->set_eof_subtree(_cur_val.subtree_address());
                      p->add_descendant(1);
                   });
               return r.address();
            }
            else
            {
               return clone<mode>(parent_hint, r, fn, {.branch_cap = 1},
                                  [&](auto cl)
                                  {
                                     cl->set_eof_subtree(_cur_val.subtree_address());
                                     cl->add_descendant(1);
                                  })
                   .address();
            }
         }
         else  // inserting data
         {
            id_address new_id =
                make_value(fn->branch_region(), fn->get_branch_alloc_hint(), state, _cur_val);
            if constexpr (mode.is_unique())
            {
               r.modify().template as<NodeType>(
                   [new_id](auto* p)
                   {
                      p->set_eof(new_id);
                      p->add_descendant(1);
                   });
               return r.address();
            }
            else
            {
               ARBTRIE_DEBUG(" clone add new value to branch 0, val =", _cur_val);
               return clone<mode>(parent_hint, r, fn, {.branch_cap = 16},
                                  [&](auto cl)
                                  {
                                     cl->set_eof(new_id);
                                     cl->add_descendant(1);
                                  })
                   .address();
            }
         }
      }
   }

   template <upsert_mode mode, typename NodeType>
   id_address write_session::remove_eof(const alloc_hint& parent_hint,
                                        object_ref&       r,
                                        const NodeType*   fn)
   {
      auto& state = r.rlock();
      //      ARBTRIE_DEBUG( "remove key ends on this node" );
      if (fn->has_eof_value())
      {
         _delta_keys = -1;
         if constexpr (mode.is_unique())
         {
            //   ARBTRIE_DEBUG( "mode is unique?" );
            release_node(state.get(fn->get_eof_address()));
            r.modify().as<NodeType>(
                [](auto* p)
                {
                   p->set_eof({});
                   p->remove_descendant(1);
                });

            if (fn->num_branches() == 0)
               return id_address();
            return r.address();
         }
         else  // mode.is_shared()
         {
            //ARBTRIE_WARN("release eof value (shared)");
            if (fn->num_branches() == 0)
            {
               //  ARBTRIE_DEBUG("  num_branch == 0, return null");
               return id_address();
            }
            return clone<mode>(parent_hint, r, fn, {},
                               [&](auto cl)
                               {
                                  //                                 ARBTRIE_DEBUG("remove eof value from clone");
                                  release_node(state.get(cl->get_eof_address()));
                                  cl->set_eof({});
                                  cl->remove_descendant(1);
                               })
                .address();
         }
      }
      if constexpr (mode.must_remove())
         throw std::runtime_error("attempt to remove key that doesn't exist");

      _delta_keys = 0;
      return r.address();
   }
   template <upsert_mode mode, typename NodeType>
   id_address write_session::upsert_inner_existing_br(object_ref&       r,
                                                      key_view          key,
                                                      const NodeType*   fn,
                                                      key_view          cpre,
                                                      branch_index_type bidx,
                                                      id_address        br,
                                                      const alloc_hint& parent_hint)
   {
      auto& state = r.rlock();

      // existing branch
      //      ARBTRIE_WARN( "upserting into existing branch: ", br, " with ref: ",
      //                    state.get(br).ref() );
      auto brn = state.get(br);
      if constexpr (mode.is_unique())
      {
         auto new_br = upsert<mode>(brn, key.substr(cpre.size() + 1), parent_hint);
         if constexpr (mode.is_remove())
         {
            if (not new_br)
            {  // then something clearly got removed
               assert(_delta_keys == -1);
               r.modify().as<NodeType>(
                   [=](auto* p)
                   {
                      p->remove_descendant(1);
                      p->remove_branch(bidx);
                   });
               if (fn->num_branches() + fn->has_eof_value() > 0)
                  return r.address();
               else
                  return id_address();
            }
            if (br != new_br)
            {
               assert(_delta_keys == -1);
               // if key was not found, then br would == new_br
               r.modify().as<NodeType>(
                   [bidx, new_br, this](auto* p)
                   {
                      p->remove_descendant(1);
                      p->set_branch(bidx, new_br);
                   });
               return r.address();
            }

            if (_delta_keys)
            {
               assert(_delta_keys == -1);
               r.modify().as<NodeType>([this](auto* p) { p->add_descendant(-1); });
            }

            return r.address();
         }
         else  //  update or insert
         {
            if (br != new_br)
               r.modify().as<NodeType>(
                   [bidx, new_br, this](auto* p)
                   {
                      p->add_descendant(_delta_keys);
                      p->set_branch(bidx, new_br);
                   });
            else if (_delta_keys)
            {
               r.modify().as<NodeType>([this](auto* p) { p->add_descendant(_delta_keys); });
            }
            return r.address();
         }
      }
      else  // shared_node
      {
         if constexpr (mode.is_remove())
         {
            //    ARBTRIE_DEBUG( "remove key ", key );
            // brn.retain();  // because upsert might release() it
            node_handle temp_retain(*this, brn.address());
            auto        new_br = upsert<mode>(brn, key.substr(cpre.size() + 1), parent_hint);
            if (not new_br)
            {
               if (fn->num_branches() + fn->has_eof_value() > 1)
               {
                  assert(_delta_keys == -1);
                  auto cl = clone<mode>(parent_hint, r, fn, {});
                  //     release_node(brn);  // because we retained before upsert(),
                  // and retained again in clone
                  cl.modify().template as<NodeType>(
                      [bidx](auto* p)
                      {
                         p->remove_descendant(1);
                         p->remove_branch(bidx);
                      });
                  return cl.address();
               }
               // because we didn't use to release it...
               temp_retain.take();

               return id_address();
            }
            else
            {
               if (br != new_br)
               {  // something was removed
                  assert(_delta_keys == -1);
                  auto cl = clone<mode>(parent_hint, r, fn, {});
                  //      release_node(brn);  // because we retained before upsert(),
                  // and retained again in clone
                  cl.modify().template as<NodeType>(
                      [bidx, new_br](auto* p)
                      {
                         p->remove_descendant(1);
                         p->set_branch(bidx, new_br);
                      });
                  return cl.address();
               }
               else
               {  // nothing was removed
                  //       release_node(brn);  // because we retained it just in case
               }
               assert(_delta_keys == 0);
               return r.address();
            }
         }
         else  // update/insert
         {
            // clone before upsert because upsert will release the branch when
            // it returns the new one
            //ARBTRIE_DEBUG( "clone: ", r.address(), " before upsert into branch: ", brn.address() );

            // cl will be returned in place of fn, so should share cachelines with parent's children
            auto cl = clone<mode>(parent_hint, r, fn, {});

            // new_br will be a child of a clone of fn, so should share cachelines with fn's children
            auto new_br =
                upsert<mode>(brn, key.substr(cpre.size() + 1), fn->get_branch_alloc_hint());
            assert(br != new_br);
            cl.modify().template as<NodeType>(
                [bidx, new_br, this](auto p)
                {
                   // upsert releases brn... which is why we
                   // don't have to retain new_brn when we modify
                   p->set_branch(bidx, new_br);
                   p->add_descendant(_delta_keys);
                });
            //    ARBTRIE_DEBUG( "returning clone: ", cl.address() );
            return cl.address();
         }
      }
   }

   template <upsert_mode mode, typename NodeType>
   id_address write_session::upsert_inner_new_br(object_ref&       r,
                                                 key_view          key,
                                                 const NodeType*   fn,
                                                 key_view          cpre,
                                                 branch_index_type bidx,
                                                 id_address        br,
                                                 const alloc_hint& parent_hint)
   {
      auto& state = r.rlock();
      if constexpr (mode.must_remove())
         throw std::runtime_error("unable to find key to remove it");
      else if constexpr (mode.is_remove())
         return r.address();
      else if constexpr (mode.must_update())
         throw std::runtime_error("unable to find key to update value");
      else
      {
         // NOTE: make_binary with one key is currently implimented
         // as making a value node
         auto new_bin = make_binary(fn->branch_region(), fn->get_branch_alloc_hint(), state,
                                    key.substr(cpre.size() + 1), _cur_val);
         _delta_keys  = 1;
         if constexpr (mode.is_unique())
         {
            if (fn->can_add_branch())
            {
               r.modify().template as<NodeType>(
                   [bidx, new_bin](auto p)
                   {
                      p->add_branch(bidx, new_bin);
                      p->add_descendant(1);
                   });
               return r.address();
            }
         }
         if constexpr (not std::is_same_v<full_node, NodeType>)
         {
            if (fn->num_branches() + 1 >= full_node_threshold)
            {
               return refactor_to_full<mode>(parent_hint, r, fn,
                                             [&](auto fptr)
                                             {
                                                fptr->add_branch(bidx, new_bin);
                                                fptr->add_descendant(1);
                                             })
                   .address();
            }
         }

         return clone<mode>(parent_hint, r, fn, {.branch_cap = fn->num_branches() + 1},
                            [&](auto cptr)
                            {
                               cptr->add_branch(bidx, new_bin);
                               cptr->add_descendant(1);
                            })
             .address();
      }
   }

   //=======================================
   // upsert
   //  - a templated upsert that works for all inner node types, but
   //  not binary_node or value_node
   //  - assumes nodes impliment the following api:
   //
   //    init( node_header* copy, clone_config )
   //    set_branch( index, id )
   //    add_branch( index, id )
   //    can_add_branch( index ) -> bool
   //    get_branch( index ) -> id
   //    get_prefix()
   //    visit_branches(auto)
   //    num_branches()
   //
   //  - returns the new ID for r if one was required, if it
   //  was able to modify in place then it will return the same
   //  id.
   //
   //  - mode tracks whether or not it is possible to modify in place
   //
   //========================================
   template <upsert_mode mode, typename NodeType>
   id_address write_session::upsert_inner(object_ref&       r,
                                          const NodeType*   fn,
                                          key_view          key,
                                          const alloc_hint& parent_hint)
   {
      auto& state = r.rlock();

      auto rootpre = fn->get_prefix();
      auto cpre    = common_prefix(rootpre, key);

      // key does not share the same prefix, insert above this node
      if (cpre.size() != rootpre.size()) [[unlikely]]
         return upsert_prefix<mode>(r, key, cpre, fn, rootpre, parent_hint);

      // else recurse into this node

      // if true, the key ends on this node, store value here
      if (cpre.size() >= key.size()) [[unlikely]]
         return upsert_eof<mode, NodeType>(parent_hint, r, fn);

      const auto bidx = char_to_branch(key[cpre.size()]);
      auto       br   = fn->get_branch(bidx);
      // on any given node there is a 256/257 chance this is false
      // on any given path this will be false for all parent nodes
      if (not br) [[unlikely]]  // for the top of the tree
         return upsert_inner_new_br<mode>(r, key, fn, cpre, bidx, br, parent_hint);

      return upsert_inner_existing_br<mode>(r, key, fn, cpre, bidx, br, parent_hint);
   }  // end upsert_inner<T>

   template <upsert_mode mode>
   id_address write_session::upsert_binary(object_ref&        root,
                                           const binary_node* bn,
                                           key_view           key,
                                           const alloc_hint&  parent_hint)
   {
      int_fast16_t lb_idx;
      uint64_t     key_hash  = binary_node::key_hash(key);
      bool         key_found = false;
      if constexpr (mode.must_insert())
      {
         _delta_keys = 1;
         // no need to test/search for existing node in hash list,
         // the caller already expects this to be an insert
         lb_idx = bn->lower_bound_idx(key);
         if (lb_idx < bn->num_branches())
            key_found = key == bn->get_key(lb_idx);

         // this is unlikey because the caller explictly told us
         // to perform an insert and not an update and we assume the
         // caller would call "upsert" if they didn't know if the key
         // existed or not.
         if (key_found)
            throw std::runtime_error("insert failed because key exists");
      }
      else if constexpr (mode.must_update())
      {
         _delta_keys = 0;
         // there must be a key key to update
         lb_idx    = bn->find_key_idx(key, key_hash);
         key_found = lb_idx != binary_node::key_not_found;
      }
      else  // we may insert or update or remove
      {
         // optimistically search for key to update
         lb_idx    = bn->find_key_idx(key, key_hash);
         key_found = lb_idx != binary_node::key_not_found;

         // but fall back to the lower bound to insert
         if (not key_found)
         {
            lb_idx = bn->lower_bound_idx(key);
            if constexpr (mode.is_remove())
               _delta_keys = 0;
            else
               _delta_keys = 1;
         }
         else  // key found
            if constexpr (mode.is_remove())
               _delta_keys = -1;
      }

      if (key_found)
      {
         if constexpr (mode.is_remove())
            return remove_binary_key<mode>(root, bn, lb_idx, key, parent_hint);
         return update_binary_key<mode>(root, bn, lb_idx, key, parent_hint);
      }
      // else key not found, insert a new value

      if constexpr (mode.must_update())
         throw std::runtime_error("update failed because key doesn't exist");

      if constexpr (mode.is_remove())
      {
         if constexpr (mode.must_remove())
            throw std::runtime_error("remove failed because key doesn't exist");
         else
            return root.address();
      }

      if constexpr (mode.is_unique())
      {
         // assuming we allocate binary nodes with space and grow them
         // every time we have to clone them anyway, then this should be
         // true, especially because we are in the unique path.
         if (bn->can_insert(key, _cur_val)) [[likely]]
         {
            if (bn->can_inline(_cur_val))
            {  // subtree or data
               kv_index kvi(lb_idx,
                            _cur_val.is_subtree() ? kv_type::subtree : kv_type::inline_data);
               root.modify().as<binary_node>()->insert(kvi, key, _cur_val);
            }
            else  // definite obj_id (aka value node)
            {
               //id_index mem[256];
               /// TODO: to use hints we must make sure there is alignment between branch_region and
               /// the indicies... if binary node doesn't honor the branch region when allocating
               /// its children, then the indicies we get back will be for he wrong region
               auto val = make_value(bn->branch_region(),
                                     alloc_hint::any() /*bn->get_branch_alloc_hint(mem, 256)*/,
                                     root.rlock(), _cur_val);
               root.modify().as<binary_node>()->insert(kv_index(lb_idx, kv_type::obj_id), key,
                                                       value_type::make_value_node(val));
            }
            return root.address();
         }
         // else we have to refactor...
      }
      // else mode is shared or unique and we cannot insert into existing
      // space.

      // worst case keys are 1kb and this misses 25% of the time
      // best case keys are small and this misses <1% of the time
      if (bn->insert_requires_refactor(key, _cur_val)) [[unlikely]]
      {
         auto rid = refactor<mode>(root, bn, parent_hint);

         assert((mode.is_unique() and (rid.address() == root.address())) or
                ((mode.is_shared()) and (rid.address() != root.address())));

         // if it wasn't unique before and the id changed then
         // it has become unique. If it was unique before, then
         // it remains unique now.
         return upsert<mode.make_unique()>(rid, key, parent_hint);
      }

      if (binary_node::can_inline(_cur_val))
         return clone<mode>(parent_hint, root, bn, {},
                            binary_node::clone_insert(
                                kv_index(lb_idx, _cur_val.is_subtree() ? kv_type::subtree
                                                                       : kv_type::inline_data),
                                key, _cur_val))
             .address();

      /*
      because binary node children don't keep all children in the same region,
      to prevent over concentration in a region and because it has room to store
      the full address without compress, we cannot use hints here.
      id_index mem[256];
      auto     hint = bn->get_branch_alloc_hint(mem, 256);
      */
      return clone<mode>(parent_hint, root, bn, {},
                         binary_node::clone_insert(
                             kv_index(lb_idx, kv_type::obj_id), key,
                             value_type::make_value_node(make_value(
                                 bn->branch_region(), alloc_hint::any(), root.rlock(), _cur_val))))
          .address();
   }  // upsert_binary

   template <upsert_mode mode>
   id_address write_session::remove_binary_key(object_ref&        root,
                                               const binary_node* bn,
                                               uint16_t           lb_idx,
                                               key_view           key,
                                               const alloc_hint&  parent_hint)
   {
      auto kvp        = bn->get_key_val_ptr(lb_idx);
      _old_value_size = kvp->value_size();

      assert(kvp->key() == key);
      if constexpr (mode.is_unique())
      {
         if (bn->is_obj_id(lb_idx))
         {
            auto vn = root.rlock().get(kvp->value_id());
            if (bn->is_subtree(lb_idx))
               _old_value_size = 4;
            else
               _old_value_size = vn.as<value_node>()->value_size();
            release_node(vn);
         }

         root.modify().as<binary_node>([&](auto* b) { b->remove_value(lb_idx); });

         if (bn->num_branches() == 0)
            return id_address();

         return root.address();
      }
      else  // not unique, must clone to update it
      {
         if (bn->num_branches() > 1)
         {
            auto cl =
                clone<mode>(parent_hint, root, bn, {}, binary_node::clone_remove(lb_idx)).address();

            if (bn->is_obj_id(lb_idx))
            {
               auto vn         = root.rlock().get(kvp->value_id());
               _old_value_size = vn.as<value_node>()->value_size();
               release_node(vn);
            }

            return cl;
         }
         return id_address();
      }
   }

   /**
    *  update_binary_key: 
    *   
    *
    *  cases:
    *    refactoring could be growing the node or splitting to radix if
    *    the binary node is already too full
    *
    *    unique & shared:
    *     inline     -> smaller inline
    *     inline     -> bigger inline (or maybe refactor)
    *     inline     -> value node (or maybe refactor)
    *     inline     -> subtree (or maybe refactor)
    *     value node -> bigger value node
    *     value node -> smaller value node
    *     value node -> smaller inline (new value less than ptr)
    *     value node -> inline (or maybe refactor)
    *     value node -> subtree
    *     subtree    -> inline (or maybe refactor)
    *     subtree    -> value node
    */
   template <upsert_mode mode>
   id_address write_session::update_binary_key(object_ref&        root,
                                               const binary_node* bn,
                                               uint16_t           lb_idx,
                                               key_view           key,
                                               const alloc_hint&  parent_hint)
   {
      const auto* kvp = bn->get_key_val_ptr(lb_idx);
      assert(kvp->key() == key);

      int inline_size = _cur_val.size();
      if (not bn->can_inline(inline_size))
         inline_size = sizeof(id_address);
      auto delta_s = inline_size - kvp->value_size();

      // this handles the case where the would be forced to grow
      // beyond the maximum size of a binary node
      if (not bn->can_update_with_compaction(delta_s))
      {
         // reclaims free space within the node...
         auto rid = refactor<mode>(root, bn, parent_hint);

         // refactor in shared mode produces a new unique node...
         return upsert<mode.make_unique()>(rid, key, parent_hint);
      }

      if constexpr (mode.is_unique())
      {
         // current value is value node or subtree
         if (bn->is_obj_id(lb_idx))
         {
            auto cval = root.rlock().get(kvp->value_id());
            if (bn->is_subtree(lb_idx))
               _old_value_size = sizeof(id_address);
            else
               _old_value_size = cval.as<value_node>()->value_size();

            uint64_t new_value_size = sizeof(id_address);
            if (not _cur_val.is_subtree())
               new_value_size = _cur_val.view().size();

            if (bn->can_inline(_cur_val))
            {
               release_node(cval);

               // value node -> smaller inline (or subtree)
               if (new_value_size <= sizeof(id_address))
               {
                  //   ARBTRIE_WARN("value node -> smaller inline" );
                  auto kvt = _cur_val.is_subtree() ? kv_type::subtree : kv_type::inline_data;
                  root.modify().as<binary_node>()->set_value(kv_index(lb_idx, kvt), _cur_val);
                  return root.address();
               }
               else  // value node -> larger inline
               {
                  assert(_cur_val.is_view());
                  // inplace
                  if (bn->can_reinsert(key, _cur_val))
                  {
                     //     ARBTRIE_WARN("value node -> larger inline" );
                     root.modify().as<binary_node>()->reinsert(kv_index(lb_idx), key,
                                                               _cur_val.view());
                     return root.address();
                  }

                  // recapture deadspace
                  return remake<binary_node>(root, bn, clone_config{},
                                             binary_node::clone_update(kv_index(lb_idx), _cur_val))
                      .address();
               }
            }
            else  // value node -> updated value node
            {
               /*
               id_index mem[256];
               auto     hint = bn->get_branch_alloc_hint(mem, 256);
               */
               auto nv = upsert_value<mode>(cval, cval.as<value_node>(), {}, parent_hint);
               if (nv != kvp->value_id())
                  root.modify().as<binary_node>()->set_value(kv_index(lb_idx, kv_type::obj_id),
                                                             value_type::make_value_node(nv));
               return root.address();
            }
         }
         else  // stored value is inline data
         {
            _old_value_size = kvp->value_size();
            if (_cur_val.is_view())  // new value is data
            {
               auto new_value_size = _cur_val.view().size();
               // inline -> eq or smaller inline
               if (_cur_val.view().size() <= _old_value_size)
               {
                  root.modify().as<binary_node>()->set_value(kv_index(lb_idx), _cur_val.view());
                  return root.address();
               }
               // inline -> larger inline
               else if (bn->can_inline(new_value_size))
               {
                  if (bn->can_reinsert(key, _cur_val))
                  {
                     root.modify().as<binary_node>()->reinsert(kv_index(lb_idx), key, _cur_val);
                     return root.address();
                  }
                  else
                     return remake<binary_node>(
                                root, bn, clone_config{},
                                binary_node::clone_update(kv_index(lb_idx), _cur_val))
                         .address();
               }
               else  // inline -> value_node
               {
                  /**
                  id_index mem[256];
                  auto     hint = bn->get_branch_alloc_hint(mem, 256);
                  */
                  auto nval =
                      make_value(bn->branch_region(), alloc_hint::any(), root.rlock(), _cur_val);
                  auto vval = value_type::make_value_node(nval);
                  // TODO kidx is irrelevant now that value_node is encoded in value_type
                  kv_index kidx(lb_idx, kv_type::obj_id);

                  if (_old_value_size >= sizeof(id_address))
                  {
                     // we can update the value in place
                     root.modify().as<binary_node>()->set_value(kidx, vval);
                     return root.address();
                  }
                  else
                  {
                     // reinsert in place in current node.
                     if (bn->can_reinsert(key, vval))
                     {
                        root.modify().as<binary_node>()->reinsert(kidx, key, vval);
                        return root.address();
                     }
                     // grow to larger binary node
                     return remake<binary_node>(root, bn, clone_config{},
                                                binary_node::clone_update(kidx, vval))
                         .address();
                  }
               }
            }
            else  // replace inline data with subtree
            {
               kv_index kidx(lb_idx, kv_type::subtree);

               if (sizeof(id_address) >= _old_value_size)
               {
                  // we can update in place
                  root.modify().as<binary_node>()->set_value(kidx, _cur_val);
                  return root.address();
               }
               else
               {
                  if (bn->can_reinsert(key, _cur_val))
                  {
                     root.modify().as<binary_node>()->reinsert(kidx, key, _cur_val);
                     return root.address();
                  }
                  else
                  {
                     // TODO: this could fail if the required size is beyond the
                     // maximum size for a binary node
                     return remake<binary_node>(root, bn, clone_config{},
                                                binary_node::clone_update(kidx, _cur_val))
                         .address();
                  }
                  // we may have to reinsert and/or refactor...
               }
            }
         }
      }
      else  // shared mode
      {
         if (bn->is_obj_id(lb_idx))  // current address (subtree/value_node)
         {
            auto cval       = root.rlock().get(kvp->value_id());
            _old_value_size = cval.as<value_node>()->value_size();
            // TODO.... what if clone fails because not enough space
            if (bn->can_inline(_cur_val.view()))
            {
               auto r = clone<mode>(parent_hint, root, bn, {},
                                    binary_node::clone_update(kv_index(lb_idx), _cur_val));
               release_node(cval);  // because clone retained a copy
               return r.address();
            }
            else
            {
               /*
               because binary node children don't keep all children in the same region,
               to prevent over concentration in a region and because it has room to store
               the full address without compress, we cannot use hints here.
               id_index mem[256];
               auto     hint = bn->get_branch_alloc_hint(mem, 256);
               */
               auto nval =
                   make_value(bn->branch_region(), alloc_hint::any(), root.rlock(), _cur_val);
               auto r = clone<mode>(parent_hint, root, bn, {},
                                    binary_node::clone_update(kv_index(lb_idx, kv_type::obj_id),
                                                              value_type::make_value_node(nval)));
               release_node(cval);  // because clone retained a copy
               return r.address();
            }
         }
         else
         {
            _old_value_size = kvp->value_size();
         }

         if (_cur_val.is_view())  // we are inserting data blob
         {
            if (bn->can_inline(_cur_val.view()))
            {
               auto r = clone<mode>(parent_hint, root, bn, {},
                                    binary_node::clone_update(kv_index(lb_idx), _cur_val));
               return r.address();
            }
            else
            {
               /**
               id_index mem[256];
               auto     hint = bn->get_branch_alloc_hint(mem, 256);
               */
               auto nval =
                   make_value(bn->branch_region(), alloc_hint::any(), root.rlock(), _cur_val);
               return clone<mode>(parent_hint, root, bn, {},
                                  binary_node::clone_update(kv_index(lb_idx, kv_type::obj_id),
                                                            value_type::make_value_node(nval)))
                   .address();
            }
         }
         else
         {
            ARBTRIE_WARN("subtree case not implimented yet");
            throw std::runtime_error("subtree not implimented");
         }
      }
   }  // update_binary_key()

   /**
    * A binary node with a single key is now just a value node until there
    * is a collision. 
    */
   id_address write_session::make_binary(id_region         reg,
                                         const alloc_hint& parent_hint,
                                         session_rlock&    state,
                                         key_view          key,
                                         const value_type& val)
   {
      return make<value_node>(reg, parent_hint, state, key, val).address();
   }

   write_transaction::ptr write_session::start_write_transaction(int top_root_node)
   {
      // Use shared_from_this() to get a shared_ptr to this session
      auto self = shared_from_this();
      return std::make_shared<write_transaction>(
          write_transaction::private_token{}, self,
          top_root_node >= 0 ? get_mutable_root(top_root_node) : create_root(),
          [this, self, top_root_node](node_handle commit, bool resume)
          {
             if (top_root_node >= 0)
             {
                // mark everything that has been written thus far as
                // read-only, this protects what has been written from being
                // corrupted by bad memory access patterns in the same
                // process.
                _segas->sync(top_root_node, commit.address());

                set_root(std::move(commit), top_root_node);
                // give other writers a chance to grab the lock
                _db->modify_lock(top_root_node).unlock();
                if (resume)
                   return get_mutable_root(top_root_node);
                return node_handle(*this, {});
             }
             if (resume)
                return commit;
             return node_handle(*this, {});
          },
          [this, top_root_node]()
          {
             if (top_root_node >= 0)
                _db->modify_lock(top_root_node).unlock();
          });
   }

   runtime_config database::get_runtime_config() const
   {
      // It's generally safer to return by value if runtime_config is small,
      // or if we want to prevent external modification of the internal state.
      return _dbm->config;
   }

   void database::set_runtime_config(const runtime_config& cfg)
   {
      // Add potential locks here if modifying config needs synchronization
      // with other operations (e.g., sync operations).
      // For now, assuming direct modification is acceptable based on current usage.

      // Update config in database's memory
      _dbm->config = cfg;

      // Propagate the config update to the seg_allocator's mapped state
      // Assert that _mapped_state is valid
      assert(_sega._mapped_state && "seg_allocator's mapped_state must be valid");
      _sega._mapped_state->_config = cfg;
      _sega._mapped_state->_segment_provider.max_mlocked_segments =
          cfg.max_pinned_cache_size_mb * 1024 * 1024 / segment_size;

      // Potentially need to signal or update other components (like seg_allocator)
      // if they depend on these config values.
      // _sega.update_config(cfg); // Example if sega needed updating
   }

}  // namespace arbtrie
