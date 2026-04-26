#pragma once
#include <psitri/count_keys.hpp>
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/node.hpp>
#include <psitri/node/value_node.hpp>
#include <psitri/upsert_mode.hpp>
#include <sal/allocator_session.hpp>
#include <sal/smart_ptr.hpp>
#include "sal/numbers.hpp"

namespace psitri
{
   using sal::alloc_header;
   using sal::smart_ptr;
   using sal::smart_ref;
   class tree_context
   {
      value_type                          _new_value;
      sal::allocator_session&             _session;
      sal::smart_ptr<alloc_header>        _root;
      int                                 _old_value_size = -1;
      int64_t                             _delta_removed_keys = 0;  // used only by range_remove to count removed keys
      uint32_t                            _collapse_threshold = 24;
      const live_range_map::snapshot*      _dead_snap = nullptr;
      uint64_t                             _current_epoch = 0;

     public:
      void set_collapse_threshold(uint32_t t) { _collapse_threshold = t; }
      void set_dead_versions(const live_range_map::snapshot* s) { _dead_snap = s; }
      void set_current_epoch(uint64_t e) { _current_epoch = e; }
      sal::smart_ptr<alloc_header> get_root() const { return _root; }
      sal::smart_ptr<alloc_header> take_root() { return std::move(_root); }

      /// Per-txn version: read the version number from the working root's
      /// ver custom CB. Returns 0 if no ver is attached (no active txn /
      /// expect_failure that hasn't ensured a version yet). Cheap — one
      /// load through the CB control block.
      uint64_t txn_version() const noexcept
      {
         auto v = _root.ver();
         if (v == sal::null_ptr_address)
            return 0;
         return _session.read_custom_cb(v);
      }
      tree_context(sal::smart_ptr<alloc_header> root)
          : _root(std::move(root)), _session(*(root.session()))
      {
         SAL_TRACE("tree_context constructor: {} {}", &_root, _root.address());
      }
      /**
       * Given a value type, if it is too large for inline converts it to a value_node,
       * otherwise returns the value type unchanged.
       */
      value_type make_value(value_type value, sal::alloc_hint hint) noexcept
      {
         if (value.is_subtree() || value.is_value_node())
            return value;  // already converted or is a subtree address
         if (value.is_view())
         {
            auto v = value.view();
            if (v.size() > 64)
               return value_type::make_value_node(_session.alloc<value_node>(hint, v));
            return value;
         }
         assert(!"only value_view, value_node, or subtree values are supported");
         abort();
      }
      uint64_t count_child_keys(ptr_address addr) noexcept
      {
         auto ref = _session.get_ref(addr);
         auto nt = node_type(ref->type());
         uint64_t result = 0;
         switch (nt)
         {
            case node_type::inner:
            {
               auto in = ref.as<inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  result += count_child_keys(in->get_branch(branch_number(i)));
               break;
            }
            case node_type::inner_prefix:
            {
               auto ipn = ref.as<inner_prefix_node>();
               for (uint16_t i = 0; i < ipn->num_branches(); ++i)
                  result += count_child_keys(ipn->get_branch(branch_number(i)));
               break;
            }
            case node_type::leaf:
               result = ref.as<leaf_node>()->num_branches();
               break;
            default:
               std::unreachable();
         }
         return result;
      }
      uint64_t count_branch_keys(const branch_set& branches) noexcept
      {
         uint64_t total = 0;
         auto     addrs = branches.addresses();
         for (int i = 0; i < addrs.size(); ++i)
            total += count_child_keys(addrs[i]);
         return total;
      }

      ptr_address make_inner(const branch_set& branches) noexcept
      {
         std::array<uint8_t, 8> out_cline_idx;
         auto                   needed_clines = find_clines(branches, out_cline_idx);
         return _session.alloc<inner_node>(branches, needed_clines, out_cline_idx,
                                           _current_epoch);
      }
      ptr_address make_inner_prefix(sal::alloc_hint   hint,
                                    key_view          prefix,
                                    const branch_set& branches) noexcept
      {
         std::array<uint8_t, 8> out_cline_idx;
         auto                   needed_clines = find_clines(branches, out_cline_idx);
         return _session.alloc<inner_prefix_node>(hint, prefix, branches, needed_clines,
                                                  out_cline_idx, _current_epoch);
      }
      template <typename NodeType>
      smart_ref<inner_prefix_node> remake_inner_prefix(const smart_ref<NodeType>& in,
                                                       key_view                   prefix,
                                                       const branch_set&          branches) noexcept
      {
         std::array<uint8_t, 8> out_cline_idx;
         auto                   needed_clines = find_clines(branches, out_cline_idx);
         return _session.realloc<inner_prefix_node>(in, prefix, branches, needed_clines,
                                                    out_cline_idx, _current_epoch);
      }

      template <typename InnerNodeType>
      uint64_t count_subrange_keys(const InnerNodeType* in, subrange range) noexcept
      {
         uint64_t total = 0;
         for (int i = *range.begin; i < *range.end; ++i)
            total += count_child_keys(in->get_branch(branch_number(i)));
         return total;
      }

      template <typename InnerNodeType>
      ptr_address make_inner_node(const sal::alloc_hint& parent_hint,
                                  const InnerNodeType*   in,
                                  subrange               range)
      {
         const branch* brs  = in->const_branches();
         auto          freq = create_cline_freq_table(brs + *range.begin, brs + *range.end);
         return _session.alloc<inner_node>(parent_hint, in, range, freq,
                                           _current_epoch);
      }
      template <typename NodeType, any_inner_node_type InnerNodeType>
      smart_ref<inner_node> remake_inner_node(const smart_ref<NodeType>& in,
                                              const InnerNodeType*       in_obj,
                                              const subrange&            range) noexcept
      {
         const branch* brs  = in->const_branches();
         auto          freq = create_cline_freq_table(brs + *range.begin, brs + *range.end);
         return _session.realloc<inner_node>(in, in_obj, range, freq,
                                             _current_epoch);
      }

      void insert(key_view key, value_type value)
      {
         _old_value_size = -1;
         auto result     = upsert<upsert_mode::unique_insert>(key, value);
         assert(result == -1);
      }

      /// @return the size of the prior value, or -1 if the value was inserted.
      template <upsert_mode mode = upsert_mode::unique_upsert>
      int upsert(key_view key, value_type value)
      {
         _old_value_size    = -1;
         sal::read_lock lock = _session.lock();
         _new_value          = std::move(value);
         if (not _root)
         {
            // First write into an empty tree: allocate a leaf and graft
            // it onto _root via give() so the txn's _ver (set at
            // start_transaction by make_unique_root) travels through
            // unchanged.
            auto leaf_addr = _session.alloc<leaf_node>(
                sal::alloc_hint(), key, make_value(_new_value, sal::alloc_hint()));
            _root.give(leaf_addr);
            return -1;
         }
         auto rref     = *_root;
         auto old_addr = _root.take();  // so it isn't released when it goes back...

         branch_set result;
         try
         {
            result = upsert<mode>({}, rref, key);
         }
         catch (...)
         {
            _root.give(old_addr);  // restore root on exception
            throw;
         }
         if (result.count() == 1)
         {
            auto new_addr = result.get_first_branch();
            _root.give(new_addr);
         }
         else
            _root.give(make_inner(result));

         return _old_value_size;
      }

      /// @return the size of the prior value, or -1 if the value was not found.
      int remove(key_view key)
      {
         if (not _root)
            return -1;
         _old_value_size = -1;
         sal::read_lock lock = _session.lock();
         auto           rref = *_root;
         auto       old_addr = _root.take();
         /// ironically, if we are in shared mode removing a key could force a split because the new
         /// branch address might not be sharable with the 16 existing clines.
         branch_set result = upsert<upsert_mode::unique_remove>({}, rref, key);
         if (result.count() == 0)
            ;  // tree is empty — node already released by upsert dispatch
         else if (result.count() == 1)
            _root.give(result.get_first_branch());
         else
            _root.give(make_inner(result));
         return _old_value_size;
      }

      /// Remove all keys in range [lower, upper).
      /// @return the number of keys removed.
      uint64_t remove_range(key_view lower, key_view upper);

      template <upsert_mode mode>
      branch_set range_remove(const sal::alloc_hint&   parent_hint,
                              smart_ref<alloc_header>& ref,
                              key_range                range);

      // ── MVCC operations ─────────────────────────────────────────
      // These modify leaf/value_node data in-place via CB location update
      // without cascading inner node changes to the root.

      /// MVCC upsert: insert or update a key at the given version.
      /// For shared trees (ref > 1), modifies only the leaf or value_node
      /// via CB relocation — no inner node cascade, root unchanged.
      /// Falls back to full COW cascade if structural changes are needed (split).
      void mvcc_upsert(key_view key, value_type value, uint64_t version);

      /// MVCC remove: append a tombstone for key at the given version.
      void mvcc_remove(key_view key, uint64_t version);

      /// Read-only traversal to find the MVCC target node for a key.
      /// Returns the node ptr_address that needs to be stripe-locked:
      /// - value_node address if the key has a value_node (Case B)
      /// - leaf address if the key has an inline value (Case A) or is new (Case C)
      /// Returns null_ptr_address only when COW fallback is unavoidable
      /// (empty tree, prefix mismatch).
      ptr_address mvcc_find_target(key_view key) const;

      /// Try MVCC upsert under stripe lock.  Returns true on success,
      /// false if the operation requires COW fallback (overflow/split).
      /// On false, no side effects — caller should retry under root mutex.
      bool try_mvcc_upsert(key_view key, value_type value, uint64_t version);

      /// Try MVCC remove under stripe lock.  Returns true on success,
      /// false if COW fallback is needed.
      bool try_mvcc_remove(key_view key, uint64_t version);

      /// MVCC helper: handle upsert at the leaf level.
      void mvcc_upsert_leaf(smart_ref<leaf_node>& leaf,
                            key_view              leaf_key,
                            key_view              full_key,
                            value_type&           value,
                            uint64_t              version);

      // ── Defrag operations ───────────────────────────────────────
      /// Background defrag: walk the tree, strip dead entries from value_nodes.
      /// Returns the number of value_nodes cleaned up.
      uint64_t defrag();

     private:
      /// Defrag helper: strip dead entries from value_nodes in a subtree.
      uint64_t defrag_subtree(ptr_address addr);
      /// Defrag helper: strip dead entries from value_nodes in a leaf.
      uint64_t defrag_leaf(smart_ref<leaf_node>& leaf);
     public:

      template <upsert_mode mode>
      branch_set range_remove_leaf(const sal::alloc_hint& parent_hint,
                                   smart_ref<leaf_node>&  leaf,
                                   key_range              range);

      template <upsert_mode mode, any_inner_node_type NodeT>
      branch_set range_remove_inner(const sal::alloc_hint& parent_hint,
                                    smart_ref<NodeT>&      node,
                                    key_range              range);

      void print()
      {
         sal::read_lock lock = _session.lock();
         print(_session.get_ref(_root.address()));
      }
      void validate()
      {
#ifdef NDEBUG
         return;  // skip expensive validation in coverage/release builds
#endif
         if (not _root)
            return;  // empty tree is valid
         sal::read_lock lock = _session.lock();
         validate(_session.get_ref(_root.address()));
      }
      void validate_unique_refs()
      {
#ifdef NDEBUG
         return;
#endif
         if (not _root)
            return;
         sal::read_lock lock = _session.lock();
         validate_unique_refs(_session.get_ref(_root.address()));
      }
      void validate(const smart_ptr<alloc_header>& r)
      {
         if (r)
            validate(*r);
      }
      void print(smart_ref<alloc_header> r, int depth = 0)
      {
         assert(_session.get_ref(r.address()).obj() == r.obj());
         switch (node_type(r->type()))
         {
            case node_type::inner:
               print(r.as<inner_node>(), depth + 1);
               break;
            case node_type::inner_prefix:
               print(r.as<inner_prefix_node>(), depth + 1);
               break;
            case node_type::leaf:
               print(r.as<leaf_node>(), depth + 1);
               break;
            case node_type::value:
            default:
               std::unreachable();
         }
      }

      struct stats
      {
         uint64_t inner_nodes           = 0;
         uint64_t inner_prefix_nodes    = 0;
         uint64_t leaf_nodes            = 0;
         uint64_t value_nodes           = 0;
         uint64_t branches              = 0;
         uint64_t clines                = 0;
         uint64_t max_depth             = 1;
         uint64_t total_inner_node_size = 0;
         uint64_t total_leaf_size       = 0;
         uint64_t total_value_size      = 0;
         uint64_t total_keys            = 0;
         uint64_t total_version_entries = 0;  ///< sum of num_versions across all value_nodes
         uint64_t single_branch_inners  = 0;
         uint64_t sparse_subtree_inners = 0;
         double   branch_per_cline      = 0;
         int      average_inner_node_size() const
         {
            return int(total_inner_node_size / (inner_nodes + inner_prefix_nodes));
         }
         double average_clines_per_inner_node() const
         {
            return double(clines) / (inner_nodes + inner_prefix_nodes);
         }
         double average_branch_per_inner_node() const
         {
            return double(branches) / (inner_nodes + inner_prefix_nodes);
         }
         uint64_t total_nodes() const
         {
            return inner_nodes + inner_prefix_nodes + leaf_nodes + value_nodes;
         }
      };
      stats get_stats()
      {
         sal::read_lock lock = _session.lock();
         stats s;
         calc_stats(s, *_root);
         return s;
      }

      // private:  // TODO: restore private after fixing friend access
      struct subtree_sizer
      {
         uint16_t count = 0;
         uint32_t key_data_size = 0;
         uint32_t value_data_size = 0;
         uint16_t addr_values = 0;
         bool     overflow = false;
         uint32_t max_entries;

         subtree_sizer(uint32_t max) : max_entries(max) {}

         bool fits_in_leaf() const
         {
            if (overflow || count == 0) return false;
            uint32_t data = key_data_size + count * 2u  // sizeof(leaf_node::key) == 2
                          + value_data_size
                          + addr_values * sizeof(ptr_address);
            uint32_t avail = leaf_node::max_leaf_size - sizeof(leaf_node) - 5u * count;
            return data <= avail;
         }
      };

      void size_subtree(ptr_address addr, uint16_t prefix_len, subtree_sizer& out)
      {
         if (out.overflow) return;
         auto ref = _session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::leaf:
            {
               auto leaf = ref.template as<leaf_node>();
               uint16_t nb = leaf->num_branches();
               if (out.count + nb > out.max_entries) { out.overflow = true; return; }
               for (uint16_t i = 0; i < nb; ++i)
               {
                  out.key_data_size += prefix_len + leaf->get_key(branch_number(i)).size();
                  auto val = leaf->get_value(branch_number(i));
                  if (val.is_view() && !val.view().empty())
                     out.value_data_size += 2u + val.view().size();  // sizeof(value_data) == 2
                  else if (val.is_subtree() || val.is_value_node())
                     ++out.addr_values;
               }
               out.count += nb;
               break;
            }
            case node_type::inner:
            {
               auto in = ref.template as<inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  size_subtree(in->get_branch(branch_number(i)), prefix_len, out);
               break;
            }
            case node_type::inner_prefix:
            {
               auto ip = ref.template as<inner_prefix_node>();
               for (uint16_t i = 0; i < ip->num_branches(); ++i)
                  size_subtree(ip->get_branch(branch_number(i)),
                              prefix_len + ip->prefix().size(), out);
               break;
            }
            default: out.overflow = true;
         }
      }

      // --- Phase 3: Subtree collapse helpers ---

      struct collapse_context
      {
         sal::allocator_session& session;
         const ptr_address*      branches;
         uint16_t                num_branches;
         key_view                root_prefix;
      };

      static void walk_subtree_insert(ptr_address addr, char* prefix_buf,
                                      uint16_t prefix_len,
                                      leaf_node::entry_inserter& ins,
                                      sal::allocator_session& session)
      {
         auto ref = session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::leaf:
            {
               auto leaf = ref.template as<leaf_node>();
               for (uint16_t i = 0; i < leaf->num_branches(); ++i)
               {
                  auto k = leaf->get_key(branch_number(i));
                  memcpy(prefix_buf + prefix_len, k.data(), k.size());
                  ins.add(key_view(prefix_buf, prefix_len + k.size()),
                          leaf->get_value(branch_number(i)));
               }
               break;
            }
            case node_type::inner:
            {
               auto in = ref.template as<inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  walk_subtree_insert(in->get_branch(branch_number(i)),
                                     prefix_buf, prefix_len, ins, session);
               break;
            }
            case node_type::inner_prefix:
            {
               auto ip = ref.template as<inner_prefix_node>();
               auto pfx = ip->prefix();
               memcpy(prefix_buf + prefix_len, pfx.data(), pfx.size());
               for (uint16_t i = 0; i < ip->num_branches(); ++i)
                  walk_subtree_insert(ip->get_branch(branch_number(i)),
                                     prefix_buf, prefix_len + pfx.size(), ins, session);
               break;
            }
            default: break;
         }
      }

      static void collapse_visitor(leaf_node::entry_inserter& ins, void* raw)
      {
         auto* ctx = static_cast<collapse_context*>(raw);
         char prefix_buf[2048];
         uint16_t pfx_len = 0;
         if (ctx->root_prefix.size() > 0)
         {
            memcpy(prefix_buf, ctx->root_prefix.data(), ctx->root_prefix.size());
            pfx_len = ctx->root_prefix.size();
         }
         for (uint16_t i = 0; i < ctx->num_branches; ++i)
            walk_subtree_insert(ctx->branches[i], prefix_buf, pfx_len,
                                ins, ctx->session);
      }

      void retain_subtree_leaf_values_by_addr(ptr_address addr)
      {
         auto ref = _session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::leaf:
               retain_children(ref.template as<leaf_node>());
               break;
            case node_type::inner:
            {
               auto in = ref.template as<inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  retain_subtree_leaf_values_by_addr(in->get_branch(branch_number(i)));
               break;
            }
            case node_type::inner_prefix:
            {
               auto ip = ref.template as<inner_prefix_node>();
               for (uint16_t i = 0; i < ip->num_branches(); ++i)
                  retain_subtree_leaf_values_by_addr(ip->get_branch(branch_number(i)));
               break;
            }
            default: break;
         }
      }

      template <typename NodeRef>
      void retain_subtree_leaf_values(NodeRef& node_ref)
      {
         for (uint16_t i = 0; i < node_ref->num_branches(); ++i)
            retain_subtree_leaf_values_by_addr(node_ref->get_branch(branch_number(i)));
      }

      // --- End Phase 3 helpers ---

      template <typename NodeType>
      void retain_children(smart_ref<NodeType>& node)
      {
         //SAL_INFO("retaining children of {} ref: {}", node.address(), node.ref());
         node->visit_branches(
             [this](ptr_address br)
             {
                //SAL_INFO("   retaining child {}", br);
                _session.retain(br);
             });
      }
      /// Prune multi-version value_nodes in a freshly-allocated leaf (ref=1).
      /// During COW-to-unique, each value_node with num_versions > 1 is replaced
      /// with a single-version copy of the latest entry.
      void prune_leaf_value_nodes(ptr_address leaf_addr);

      template <upsert_mode mode>
      branch_set split_insert(const sal::alloc_hint& parent_hint,
                              smart_ref<leaf_node>&  leaf,
                              key_view               key,
                              branch_number          lb);

      template <upsert_mode mode>
      branch_set update(const sal::alloc_hint& parent_hint,
                        smart_ref<leaf_node>&  leaf,
                        key_view               key,
                        branch_number          br);

      template <upsert_mode mode>
      branch_set remove(const sal::alloc_hint& parent_hint,
                        smart_ref<leaf_node>&  leaf,
                        key_view               key,
                        branch_number          lb);

      template <upsert_mode mode>
      branch_set insert(const sal::alloc_hint& parent_hint,
                        smart_ref<leaf_node>&  leaf,
                        key_view               key,
                        branch_number          lb);

      template <upsert_mode mode>
      branch_set upsert(const sal::alloc_hint& parent_hint,
                        smart_ref<leaf_node>&  leaf,
                        key_view               key);

      template <upsert_mode mode, any_inner_node_type InnerNodeType>
      std::pair<ptr_address, ptr_address> split(const sal::alloc_hint&    parent_hint,
                                                smart_ref<InnerNodeType>& in,
                                                branch_number             br,
                                                const branch_set&         subresult);

      template <upsert_mode mode, any_inner_node_type InnerNodeType>
      branch_set split_merge(const sal::alloc_hint&    parent_hint,
                             smart_ref<InnerNodeType>& in,
                             branch_number             br,
                             const branch_set&         sub_branches);

      template <upsert_mode mode, any_inner_node_type InnerNodeType>
      branch_set merge_branches(const sal::alloc_hint&    parent_hint,
                                smart_ref<InnerNodeType>& in,
                                branch_number             br,
                                const branch_set&         sub_branches);

      template <upsert_mode mode, any_inner_node_type InnerNodeType>
      branch_set upsert(const sal::alloc_hint&    parent_hint,
                        smart_ref<InnerNodeType>& in,
                        key_view                  key);

      /**
       * id type ref "divs"
       *   / 
       *   a/ 
              [prefix] ref "divs"
       *   b/
       *   c/ 
       *   d/ 
       *   e/ 
       *   f/ 
       *   g/ 
       *   h/ 
       */
      void print(smart_ref<inner_node> r, int depth = 0)
      {
         assert(_session.get_ref(r.address()).obj() == r.obj());
         std::string indent(4 * depth, ' ');
         std::cout << indent << "#" << r.address() << "  " << r->type() << " r:" << r.ref()
                   << " divs: '" << r->divs() << "' branches: " << r->num_branches()
                   << "  cline:" << r->num_clines() << " this: " << r.obj() << "\n";
         for (int i = 0; i < r->num_branches(); ++i)
         {
            auto br  = r->get_branch(branch_number(i));
            auto ref = _session.get_ref(br);
            std::cout << br << "->";
            print(ref, depth);
         }
      }
      void print(smart_ref<inner_prefix_node> r, int depth = 0)
      {
         std::string indent(4 * depth, ' ');
         std::cout << indent << "'" << r->prefix() << "'/  id: " << r.address()
                   << " type: " << r->type() << " ref: " << r.ref() << " prefix: '" << r->prefix()
                   << "'  divs: '" << r->divs() << "' branches: " << r->num_branches()
                   << "  cline:" << r->num_clines() << " this: " << r.obj() << "\n";
         for (int i = 0; i < r->num_branches(); ++i)
         {
            auto br  = r->get_branch(branch_number(i));
            auto ref = _session.get_ref(br);
            print(ref, depth);
         }
      }
      void print(smart_ref<leaf_node> r, int depth = 0)
      {
         std::string indent(4 * depth, ' ');
         std::cout << indent << "#" << r.address() << "  " << r->type()
                   << " branches: " << r->num_branches() << " ref: " << r.ref()
                   << " this: " << r.obj() << "\n";

         std::cout << indent << "  '" << r->get_key(branch_number(0)) << "' = '"
                   << r->get_value(branch_number(0)) << "'  ";
         std::cout << "...  \n";
         if (r->num_branches() > 1)
            std::cout << indent << "  '" << r->get_key(branch_number(r->num_branches() - 1))
                      << "' = '" << r->get_value(branch_number(r->num_branches() - 1)) << "'\n";

         // for (int i = 0; i < r->num_branches(); ++i)
         // {
         //   std::cout << indent << "  '" << r->get_key(branch_number(i)) << "' = '"
         //            << r->get_value(branch_number(i)) << "'\n";
         // }
      }
      /// validates tree structure and returns total key count for this subtree
      uint64_t validate_subtree(smart_ref<alloc_header> r, int depth = 0)
      {
         switch (node_type(r->type()))
         {
            case node_type::inner:
               return validate_inner(r.as<inner_node>(), depth + 1);
            case node_type::inner_prefix:
               return validate_inner(r.as<inner_prefix_node>(), depth + 1);
            case node_type::leaf:
               return r.as<leaf_node>()->num_branches();
            case node_type::value:
            default:
               std::unreachable();
         }
      }
      template <any_inner_node_type NodeType>
      uint64_t validate_inner(smart_ref<NodeType> r, int depth = 0)
      {
         PSITRI_ASSERT_INVARIANTS(r->validate_invariants());
         uint64_t total_keys = 0;
         // Store per-branch recursive counts for comparison
         std::vector<uint64_t> recursive_counts(r->num_branches());
         for (int i = 0; i < r->num_branches(); ++i)
         {
            auto br  = r->get_branch(branch_number(i));
            auto ref = _session.get_ref(br);
            assert(ref.ref() > 0);
            recursive_counts[i] = validate_subtree(ref, depth);
            total_keys += recursive_counts[i];
         }
         // Descendent validation removed — epoch replaced descendents
         return total_keys;
      }
      void validate(smart_ref<alloc_header> r, int depth = 0)
      {
         validate_subtree(r, depth);
      }

      /// Validates that every non-root node in the tree has refcount == 1.
      /// Only valid when there are no concurrent readers or snapshots.
      /// The root node may have ref > 1 due to the root table + caller holding a ref.
      void validate_unique_refs(smart_ref<alloc_header> r)
      {
         // Skip root ref check (root table + caller both hold refs)
         // but validate all children have ref == 1
         switch (node_type(r->type()))
         {
            case node_type::inner:
               validate_unique_refs_inner(r.as<inner_node>());
               break;
            case node_type::inner_prefix:
               validate_unique_refs_inner(r.as<inner_prefix_node>());
               break;
            case node_type::leaf:
            {
               auto leaf = r.as<leaf_node>();
               for (int i = 0; i < leaf->num_branches(); ++i)
               {
                  if (leaf->get_value_type(branch_number(i)) == leaf_node::value_type_flag::value_node)
                  {
                     auto val_addr = leaf->get_value_address(branch_number(i));
                     if (val_addr != sal::null_ptr_address)
                     {
                        auto val_ref = _session.get_ref(val_addr);
                        assert(val_ref.ref() == 1 && "value node ref must be 1");
                     }
                  }
               }
               break;
            }
            default:
               std::unreachable();
         }
      }
      void validate_unique_refs_subtree(smart_ref<alloc_header> r)
      {
         switch (node_type(r->type()))
         {
            case node_type::inner:
               validate_unique_refs_inner(r.as<inner_node>());
               break;
            case node_type::inner_prefix:
               validate_unique_refs_inner(r.as<inner_prefix_node>());
               break;
            case node_type::leaf:
            {
               auto leaf = r.as<leaf_node>();
               for (int i = 0; i < leaf->num_branches(); ++i)
               {
                  if (leaf->get_value_type(branch_number(i)) == leaf_node::value_type_flag::value_node)
                  {
                     auto val_addr = leaf->get_value_address(branch_number(i));
                     if (val_addr != sal::null_ptr_address)
                     {
                        auto val_ref = _session.get_ref(val_addr);
                        assert(val_ref.ref() == 1 && "value node ref must be 1");
                     }
                  }
               }
               break;
            }
            default:
               std::unreachable();
         }
      }
      template <any_inner_node_type NodeType>
      void validate_unique_refs_inner(smart_ref<NodeType> r)
      {
         for (int i = 0; i < r->num_branches(); ++i)
         {
            auto br  = r->get_branch(branch_number(i));
            auto ref = _session.get_ref(br);
            assert(ref.ref() == 1 && "child node ref must be 1");
            validate_unique_refs_subtree(ref);
         }
      }

      template <any_inner_node_type NodeType>
      void calc_stats(stats& s, smart_ref<NodeType> r, int depth = 0)
      {
         s.total_inner_node_size += r->size();
         s.clines += r->num_clines();
         s.branches += r->num_branches();
         s.branch_per_cline += double(r->num_branches()) / r->num_clines();
         if (r->num_branches() == 1)
            ++s.single_branch_inners;
         // sparse_subtree_inners tracking removed — epoch replaced descendents
         if (!r->validate_invariants())
         {
            SAL_ERROR("calc_stats: inner node {} failed validate_invariants at depth {},"
                      " branches={} clines={} size={}",
                      r.address(), depth, r->num_branches(), r->num_clines(), r->size());
            throw std::runtime_error("calc_stats: corrupted inner node");
         }
         for (int i = 0; i < r->num_branches(); ++i)
         {
            auto br  = r->get_branch(branch_number(i));
            auto ref = _session.get_ref(br);
            calc_stats(s, ref, depth);
         }
      }
      void calc_stats(stats& s, smart_ref<alloc_header> r, int depth = 0)
      {
         if (depth > s.max_depth)
            s.max_depth = depth;
         switch (node_type(r->type()))
         {
            case node_type::inner:
               s.inner_nodes++;
               calc_stats(s, r.as<inner_node>(), depth + 1);
               break;
            case node_type::inner_prefix:
               s.inner_prefix_nodes++;
               calc_stats(s, r.as<inner_prefix_node>(), depth + 1);
               break;
            case node_type::leaf:
            {
               s.leaf_nodes++;
               auto leaf = r.as<leaf_node>();
               s.total_leaf_size += leaf->size();
               s.total_keys += leaf->num_branches();
               for (uint32_t i = 0; i < leaf->num_branches(); ++i)
               {
                  if (leaf->get_value_type(branch_number(i)) ==
                      leaf_node::value_type_flag::value_node)
                  {
                     s.value_nodes++;
                     auto vref = _session.get_ref<value_node>(
                         leaf->get_value_address(branch_number(i)));
                     s.total_value_size += vref->size();
                     s.total_version_entries += vref->num_versions();
                  }
               }
               break;
            }
            case node_type::value:
            default:
               std::unreachable();
         }
      }

      template <upsert_mode mode>
      branch_set upsert(const sal::alloc_hint&   parent_hint,
                        smart_ref<alloc_header>& r,
                        key_view                 key)
      {
         if constexpr (mode.is_unique())
            if (r.ref() > 1)
               return upsert<mode.make_shared()>(parent_hint, r, key);

         branch_set result;
         switch (node_type(r->type()))
         {
            case node_type::inner:
               [[likely]] result = upsert<mode>(parent_hint, r.as<inner_node>(), key);
               break;
            case node_type::inner_prefix:
               [[likely]] result = upsert<mode>(parent_hint, r.as<inner_prefix_node>(), key);
               break;
            case node_type::leaf:
               [[unlikely]] result = upsert<mode>(parent_hint, r.as<leaf_node>(), key);
               break;
            case node_type::value:
               //  [[unlikely]] return upsert<mode>(parent_hint, r.as<value_node>(), key);
            default:
               std::unreachable();
         }
         if constexpr (mode.is_unique())
         {
            if constexpr (mode.is_remove())
            {
               assert(result.count() == 0 || result.contains(r.address()));
               // In unique remove, an empty result means this node became empty
               // and was not realloc'd.  Release it so we don't leak.
               if (result.count() == 0)
                  _session.release(r.address());
            }
            else
               assert(result.contains(r.address()));
         }
         else if (not result.contains(r.address())) [[likely]]
            r.release();

         return result;
      }

   };  // class tree_context

   /**
    * Splits a node into two nodes each with half the branches of the original node,
    * and returns two branches to the two halfs. It always produces two InnerNodeType's
    * even if the original node was an inner prefix node.
    */
   template <upsert_mode mode, any_inner_node_type InnerNodeType>
   std::pair<ptr_address, ptr_address> tree_context::split(const sal::alloc_hint&    parent_hint,
                                                           smart_ref<InnerNodeType>& in,
                                                           branch_number             br,
                                                           const branch_set&         subresult)
   {
      const branch_number nb(in->num_branches());
      const branch_number nb2 = branch_number(*nb / 2);

      if constexpr (is_inner_prefix_node<InnerNodeType>)
      {
         // When splitting an inner prefix node we produce two inner nodes that most likely will share
         // a cacheline. The caller will embed these two nodes under a new inner prefix node, there
         // is no difference between unique and shared mode in this case.
         ptr_address left  = make_inner_node({}, in.obj(), subrange(branch_number(0), nb2));
         ptr_address right = make_inner_node({&left, 1}, in.obj(), subrange(nb2, nb));
         return {left, right};
      }
      else if constexpr (mode.is_unique())
      {
         //auto left  = _session.realloc<inner_node>(in, in.obj(), subrange(branch_number(0), nb2));
         auto left  = remake_inner_node<inner_node>(in, in.obj(), subrange(branch_number(0), nb2));
         auto right = make_inner_node(parent_hint, in.obj(), subrange(nb2, nb));
         return std::make_pair(left.address(), right);
      }
      else if constexpr (mode.is_shared())
      {
         auto left  = make_inner_node(parent_hint, in.obj(), subrange(branch_number(0), nb2));
         auto right = make_inner_node(parent_hint, in.obj(), subrange(nb2, nb));
         return {left, right};
      }
   }

   /**
    * TODO: 
    */
   template <upsert_mode mode, any_inner_node_type InnerNodeType>
   branch_set tree_context::split_merge(const sal::alloc_hint&    parent_hint,
                                        smart_ref<InnerNodeType>& in,
                                        branch_number             br,
                                        const branch_set&         sub_branches)
   {
      auto nb = in->num_branches();

      auto [left, right] = split<mode>(parent_hint, in, br, sub_branches);

      if (br < nb / 2)
      {
         smart_ref<inner_node> left_ref = _session.get_ref<inner_node>(left);
         auto left_result =
             merge_branches<upsert_mode::unique>(parent_hint, left_ref, br, sub_branches);
         left_result.push_back(in->divs()[nb / 2 - 1], right);
         return left_result;
      }
      else
      {
         smart_ref<inner_node> right_ref = _session.get_ref<inner_node>(right);
         branch_set right_result = merge_branches<upsert_mode::unique>(
             parent_hint, right_ref, branch_number(*br - nb / 2), sub_branches);
         right_result.push_front(left, in->divs()[nb / 2 - 1]);
         return right_result;
      }
   }

   template <upsert_mode mode, any_inner_node_type InnerNodeType>
   branch_set tree_context::merge_branches(const sal::alloc_hint&    parent_hint,
                                           smart_ref<InnerNodeType>& in,
                                           branch_number             br,
                                           const branch_set&         sub_branches)
   {
      // at this point we don't care about insert vs upsert vs update vs remove so prevent code bloat
      static_assert(mode.flags <= upsert_mode::type::unique, "mode must be unique or shared");
      //SAL_INFO("merge_branches: in: {} br: {} sub: {}", in.address(), br, sub_branches);
      //print(in, 10);

      auto bref = _session.get_ref(in->get_branch(br));

      std::array<uint8_t, 8> cline_indices;
      auto needed_clines = psitri::find_clines(in->get_branch_clines(), in->get_branch(br),
                                               sub_branches.addresses(), cline_indices);
      assert(needed_clines >= in->get_branch_clines().size());

      /// this is the unlikely path because it requires a split which only happens once
      /// a node gets to 16 cachelines and thus most updates don't require a split.
      if (needed_clines == insufficient_clines) [[unlikely]]
      {
         if constexpr (is_inner_node<InnerNodeType>)
            return split_merge<mode>(parent_hint, in, br, sub_branches);
         else if constexpr (is_inner_prefix_node<InnerNodeType>)
         {
            auto new_children = split_merge<mode>(parent_hint, in, br, sub_branches);
            // inner prefix nodes cannot bubble up the new children
            if constexpr (mode.is_unique())
               return remake_inner_prefix(in, in->prefix(), new_children).address();
            else if constexpr (mode.is_shared())
               return make_inner_prefix(parent_hint, in->prefix(), new_children);
         }
      }

      if constexpr (mode.is_unique())
      {
         op::replace_branch update = {br, sub_branches, needed_clines, cline_indices};

         /// this is the likely path because realloc grows by cachelines and
         /// most updates don't force a node to grow.
         ptr_address result_addr;
         if (in->can_apply(update)) [[likely]]
         {
            in.modify()->apply(update);
            result_addr = in.address();
         }
         else
         {
            if constexpr (is_inner_node<InnerNodeType>)
               result_addr = _session.realloc<InnerNodeType>(in, in.obj(), update).address();
            else if constexpr (is_inner_prefix_node<InnerNodeType>)
               result_addr = _session.realloc<InnerNodeType>(in, in->prefix(), in.obj(), update).address();
         }

         return result_addr;
      }
      else if constexpr (mode.is_shared())
      {
         if constexpr (is_inner_node<InnerNodeType>)
            return _session.alloc<InnerNodeType>(
                parent_hint, in.obj(),
                op::replace_branch{br, sub_branches, needed_clines, cline_indices});
         else if constexpr (is_inner_prefix_node<InnerNodeType>)
         {
            return _session.alloc<InnerNodeType>(
                parent_hint, in->prefix(), in.obj(),
                op::replace_branch{br, sub_branches, needed_clines, cline_indices});
         }
      }
   }

   template <upsert_mode mode, any_inner_node_type InnerNodeType>
   branch_set tree_context::upsert(const sal::alloc_hint&    parent_hint,
                                   smart_ref<InnerNodeType>& in,
                                   key_view                  key)
   {
      //     SAL_INFO("upsert: inner {} key: {}", in.address(), key);
      //      print(in, 10);
      // all paths should use this result to return the value so RVO will work
      // this is important because result is over 32 bytes
      branch_set result;
      if constexpr (is_inner_prefix_node<InnerNodeType>)
      {
         //         SAL_INFO("        prefix {}", in->prefix());
         auto cpre = common_prefix(key, in->prefix());
         if (cpre != in->prefix()) [[unlikely]]
         {
            if constexpr (mode.is_remove())
               return in.address();

            //          SAL_ERROR("        cpre  != in->prefix() '{}' != '{}'  key: {}", cpre, in->prefix(),
            //                   key);
            assert(in->prefix().size() >= 1);  // or else it should have been an inner_node
            if constexpr (mode.must_update())
            {
               if constexpr (mode.is_shared())
                  return in.address();
               assert(!"insert/update precondition violated: key does not exist");
               std::unreachable();
            }

            // prefix mismatch — inserting a new key

            if (cpre.size() == 0)
            {
               // return 2 branches to the parent node, ordered by first byte
               ptr_address new_leaf_addr =
                   _session.alloc<leaf_node>(parent_hint, key, make_value(_new_value, parent_hint));
               uint8_t ipn_byte  = in->prefix()[0];
               uint8_t leaf_byte = key.empty() ? 0 : key[0];
               if (leaf_byte > ipn_byte)
               {
                  result.set_front(in->address());
                  result.push_back(leaf_byte, new_leaf_addr);
               }
               else
               {
                  result.set_front(new_leaf_addr);
                  result.push_back(ipn_byte, in->address());
               }
               return result;
            }
            /// any new value node is an only child of the new leaf, so has no hints
            /// the leaf node will be a child of the new inner prefix node, which
            /// (if unique) should use the same id as the existing inner prefix node
            /// the existing inner prefix node needs to be cloned with its prefix trimmed
            /// so that it can share a cacheline with the new leaf node.
            // cpre is than the full prefix, so we need to split
            //
            // cpre/
            //    postpre -> existing node
            //   [p] divider
            //    new_leaf -> new leaf node (if key[cpre.size()] > divider)
            //

            ptr_address new_leaf_addr = _session.alloc<leaf_node>(
                key.substr(cpre.size()), make_value(_new_value, sal::alloc_hint()));

            ptr_address new_ipn_addr = _session.alloc<inner_prefix_node>(
                {&new_leaf_addr, 1}, in.obj(), in->prefix().substr(cpre.size()));

            uint8_t ipn_div  = in->prefix()[cpre.size()];
            uint8_t leaf_div = key.size() > cpre.size() ? key[cpre.size()] : ipn_div;
            uint8_t divider  = ipn_div;
            if (leaf_div > ipn_div)
            {
               divider = leaf_div;
               std::swap(new_leaf_addr, new_ipn_addr);
            }

            if constexpr (mode.is_unique())
            {
               return remake_inner_prefix(in, cpre,
                                          branch_set(divider, new_leaf_addr, new_ipn_addr))
                   .address();
            }
            else if constexpr (mode.is_shared())
            {
               //               SAL_WARN("retaining children after cloning to new inner_prefix with smaller prefix");
               retain_children(in);
               return make_inner_prefix(parent_hint, cpre,
                                        branch_set(divider, new_leaf_addr, new_ipn_addr));
            }
            std::unreachable();
         }
         key = key.substr(cpre.size());
         // else fall through and traverse down tree
      }  // end of inner prefix node path

      branch_number br   = in->lower_bound(key);
      auto          badr = in->get_branch(br);

      // In sorted mode, prefetch the next sibling's node.  Sorted keys exhaust
      // the current subtree then move to the next branch — warming it early
      // hides page-fault latency for any linear scan beyond RAM.
      if constexpr (mode.is_sorted())
      {
         if (branch_number(uint32_t(*br) + 1) < in->num_branches())
            _session.prefetch(in->get_branch(branch_number(uint32_t(*br) + 1)));
      }

      auto          bref = _session.get_ref(badr);

      //SAL_INFO("in before updating branch '{}' address {} ptr: {}", br, badr, in.obj());
      //print(in, 10);

      if constexpr (mode.is_shared())
         retain_children(in);  // all children have been copied to new node, retain them

      // In unique_remove, if this is the last branch, pre-retain it so
      // the dispatcher's release (if child becomes empty) decrements from
      // 2→1 instead of 1→0.  Without this, final_release can free the
      // control block before we get a chance to retain it, corrupting memory.
      bool pre_retained_last_branch = false;
      if constexpr (mode.is_unique() && mode.is_remove())
      {
         if (in->num_branches() == 1)
         {
            _session.retain(badr);
            pre_retained_last_branch = true;
         }
      }

      // recursive upsert, give it this nodes clines as the parent hint
      branch_set sub_branches = upsert<mode>(in->get_branch_clines(), bref, key);
      //SAL_INFO("in after updating branch '{}' address {} ptr: {}", br, badr, in.obj());
      //print(in, 10);
      if constexpr (mode.is_remove())
      {
         if (sub_branches.count() == 0) [[unlikely]]
         {

            if (in->num_branches() == 1)
            {
               // badr was pre-retained above (unique mode only); in's destructor
               // will release it, balancing the dispatcher's release.
               return {};  // cascade empty to parent — dispatch releases this node
            }

            if constexpr (mode.is_unique())
            {
               in.modify(
                   [&](auto* n)
                   {
                      n->remove_branch(br);
                   });

               // Phase 2: Collapse single-branch inner node
               if (in->num_branches() == 1)
               {
                  auto child_addr = in->get_branch(branch_number(0));
                  auto child_ref  = _session.get_ref(child_addr);

                  switch (node_type(child_ref->type()))
                  {
                     case node_type::leaf:
                     {
                        auto leaf_ref = child_ref.template as<leaf_node>();
                        if constexpr (is_inner_node<InnerNodeType>)
                        {
                           retain_children(leaf_ref);
                           (void)_session.realloc<leaf_node>(in, leaf_ref.obj());
                           _session.release(child_addr);
                        }
                        else if constexpr (is_inner_prefix_node<InnerNodeType>)
                        {
                           // Skip collapse if prepending prefix would overflow leaf capacity.
                           // Account for per-branch overhead, cline slots for address-typed values,
                           // and the extra prefix bytes prepended to each key.
                           auto     pfx       = in->prefix();
                           uint16_t nb        = leaf_ref->num_branches();
                           uint32_t cline_space = leaf_ref->clines_capacity() * sizeof(ptr_address);
                           uint32_t avail = leaf_node::max_leaf_size - sizeof(leaf_node)
                                            - 5u * nb - cline_space;
                           if (leaf_ref->alloc_pos() + (uint32_t)pfx.size() * nb <= avail)
                           {
                              retain_children(leaf_ref);
                              char prefix_buf[2048];
                              memcpy(prefix_buf, pfx.data(), pfx.size());
                              (void)_session.realloc<leaf_node>(
                                  in, op::leaf_prepend_prefix{*leaf_ref.obj(),
                                                              key_view(prefix_buf, pfx.size())});
                              _session.release(child_addr);
                           }
                           // else: skip collapse, inner_prefix_node stays with 1 branch
                        }
                        break;
                     }
                     case node_type::inner:
                     {
                        auto        inner_ref = child_ref.template as<inner_node>();
                        retain_children(inner_ref);
                        const auto* child_ptr = inner_ref.obj();
                        const branch* brs =
                            child_ptr->const_branches();
                        auto freq =
                            create_cline_freq_table(brs, brs + child_ptr->num_branches());
                        auto range = subrange(branch_number(0),
                                              branch_number(child_ptr->num_branches()));
                        if constexpr (is_inner_node<InnerNodeType>)
                        {
                           (void)_session.realloc<inner_node>(in, child_ptr, range, freq,
                                                              _current_epoch);
                        }
                        else if constexpr (is_inner_prefix_node<InnerNodeType>)
                        {
                           char prefix_buf[2048];
                           auto pfx = in->prefix();
                           memcpy(prefix_buf, pfx.data(), pfx.size());
                           (void)_session.realloc<inner_prefix_node>(
                               in, child_ptr, key_view(prefix_buf, pfx.size()), range, freq,
                               _current_epoch);
                        }
                        _session.release(child_addr);
                        break;
                     }
                     case node_type::inner_prefix:
                     {
                        auto ipn_ref = child_ref.template as<inner_prefix_node>();
                        retain_children(ipn_ref);
                        if constexpr (is_inner_node<InnerNodeType>)
                        {
                           (void)_session.realloc<inner_prefix_node>(in, ipn_ref.obj(),
                                                                     ipn_ref->prefix());
                        }
                        else if constexpr (is_inner_prefix_node<InnerNodeType>)
                        {
                           char prefix_buf[2048];
                           auto p = in->prefix();
                           auto q = ipn_ref->prefix();
                           memcpy(prefix_buf, p.data(), p.size());
                           memcpy(prefix_buf + p.size(), q.data(), q.size());
                           (void)_session.realloc<inner_prefix_node>(
                               in, ipn_ref.obj(),
                               key_view(prefix_buf, p.size() + q.size()));
                        }
                        _session.release(child_addr);
                        break;
                     }
                     default:
                        std::unreachable();
                  }
               }

               // Phase 3: Collapse sparse multi-branch subtree into a single leaf
               // (size_subtree has an early-exit at _collapse_threshold, bounding cost)
               if (_collapse_threshold > 0 && in->num_branches() > 1)
               {
                  subtree_sizer sizer(_collapse_threshold);
                  uint16_t pfx_len = 0;
                  if constexpr (is_inner_prefix_node<InnerNodeType>)
                     pfx_len = in->prefix().size();
                  for (uint16_t i = 0; i < in->num_branches(); ++i)
                     size_subtree(in->get_branch(branch_number(i)), pfx_len, sizer);

                  if (sizer.fits_in_leaf())
                  {
                     retain_subtree_leaf_values(in);

                     // Save state before realloc invalidates the node
                     ptr_address branches[256];
                     uint16_t nb = in->num_branches();
                     for (uint16_t i = 0; i < nb; ++i)
                        branches[i] = in->get_branch(branch_number(i));

                     char prefix_save[2048];
                     key_view root_prefix;
                     if constexpr (is_inner_prefix_node<InnerNodeType>)
                     {
                        auto pfx = in->prefix();
                        memcpy(prefix_save, pfx.data(), pfx.size());
                        root_prefix = key_view(prefix_save, pfx.size());
                     }

                     collapse_context cctx{_session, branches, nb, root_prefix};
                     (void)_session.realloc<leaf_node>(
                         in, op::leaf_from_visitor{&collapse_visitor, &cctx, sizer.count});

                     for (uint16_t i = 0; i < nb; ++i)
                        _session.release(branches[i]);
                  }
               }
               return in.address();
            }
            else if constexpr (mode.is_shared())
            {
               // Phase 2: Collapse single-branch result in shared mode
               if (in->num_branches() == 2) [[unlikely]]
               {
                  branch_number remaining_br =
                      (*br == 0) ? branch_number(1) : branch_number(0);
                  auto remaining_addr = in->get_branch(remaining_br);

                  if constexpr (is_inner_node<InnerNodeType>)
                  {
                     // retain_children(in) already gave +1 to remaining_addr.
                     // in's destroy will give -1, transferring ownership to caller.
                     return remaining_addr;
                  }
                  else if constexpr (is_inner_prefix_node<InnerNodeType>)
                  {
                     auto child_ref = _session.get_ref(remaining_addr);
                     switch (node_type(child_ref->type()))
                     {
                        case node_type::leaf:
                        {
                           auto     leaf_ref = child_ref.template as<leaf_node>();
                           auto     pfx     = in->prefix();
                           uint16_t nb      = leaf_ref->num_branches();
                           uint32_t cline_space = leaf_ref->clines_capacity() * sizeof(ptr_address);
                           uint32_t avail   = leaf_node::max_leaf_size - sizeof(leaf_node)
                                              - 5u * nb - cline_space;
                           if (leaf_ref->alloc_pos() + (uint32_t)pfx.size() * nb <= avail)
                           {
                              retain_children(leaf_ref);
                              auto result = _session.alloc<leaf_node>(
                                  parent_hint,
                                  op::leaf_prepend_prefix{*leaf_ref.obj(), pfx});
                              _session.release(remaining_addr);
                              return result;
                           }
                           break;  // fall through to inner_remove_branch
                        }
                        case node_type::inner:
                        {
                           auto        inner_ref = child_ref.template as<inner_node>();
                           retain_children(inner_ref);
                           const auto* child_ptr = inner_ref.obj();
                           const branch* brs     = child_ptr->const_branches();
                           auto          freq =
                               create_cline_freq_table(brs, brs + child_ptr->num_branches());
                           auto range = subrange(branch_number(0),
                                                 branch_number(child_ptr->num_branches()));
                           auto result = _session.alloc<inner_prefix_node>(
                               parent_hint, child_ptr, in->prefix(), range, freq,
                               _current_epoch);
                           _session.release(remaining_addr);
                           return result;
                        }
                        case node_type::inner_prefix:
                        {
                           auto ipn_ref = child_ref.template as<inner_prefix_node>();
                           retain_children(ipn_ref);
                           char prefix_buf[2048];
                           auto p = in->prefix();
                           auto q = ipn_ref->prefix();
                           memcpy(prefix_buf, p.data(), p.size());
                           memcpy(prefix_buf + p.size(), q.data(), q.size());
                           auto result = _session.alloc<inner_prefix_node>(
                               parent_hint, ipn_ref.obj(),
                               key_view(prefix_buf, p.size() + q.size()));
                           _session.release(remaining_addr);
                           return result;
                        }
                        default:
                           std::unreachable();
                     }
                  }
               }

               // Phase 3: Collapse sparse subtree in shared mode
               {
                  if (_collapse_threshold > 0 && in->num_branches() > 2)
                  {
                     ptr_address remaining[256];
                     uint16_t rb_count = 0;
                     uint16_t pfx_len = 0;
                     if constexpr (is_inner_prefix_node<InnerNodeType>)
                        pfx_len = in->prefix().size();

                     for (uint16_t i = 0; i < in->num_branches(); ++i)
                     {
                        if (branch_number(i) == br) continue;
                        remaining[rb_count++] = in->get_branch(branch_number(i));
                     }

                     subtree_sizer sizer(_collapse_threshold);
                     for (uint16_t i = 0; i < rb_count; ++i)
                        size_subtree(remaining[i], pfx_len, sizer);

                     if (sizer.fits_in_leaf())
                     {
                        for (uint16_t i = 0; i < rb_count; ++i)
                           retain_subtree_leaf_values_by_addr(remaining[i]);

                        key_view root_prefix;
                        if constexpr (is_inner_prefix_node<InnerNodeType>)
                           root_prefix = in->prefix();  // safe: in still alive in shared mode

                        collapse_context cctx{_session, remaining, rb_count, root_prefix};
                        auto result = _session.alloc<leaf_node>(
                            parent_hint,
                            op::leaf_from_visitor{&collapse_visitor, &cctx, sizer.count});

                        // Release the remaining subtree nodes: retain_children(in) at
                        // the top of this function gave +1 to all of in's children.
                        // The collapsed leaf doesn't reference the subtree nodes
                        // (only their leaf values, which were separately retained above).
                        // Without this release, the subtree nodes end up with ref=1
                        // and no parent after the old snapshot is freed.
                        for (uint16_t i = 0; i < rb_count; ++i)
                           _session.release(remaining[i]);

                        return result;
                     }
                  }
               }

               // retain_children gave +1 to all remaining children; the removed
               // child's extra retain was already released above.
               op::inner_remove_branch rm{br};
               if constexpr (is_inner_node<InnerNodeType>)
                  return _session.alloc<InnerNodeType>(parent_hint, in.obj(), rm);
               else if constexpr (is_inner_prefix_node<InnerNodeType>)
                  return _session.alloc<InnerNodeType>(parent_hint, in->prefix(), in.obj(), rm);
            }
            std::unreachable();
         }
      }

      // Undo pre-retain: child didn't become empty, so the dispatcher
      // didn't release badr; drop the extra reference we added.
      if (pre_retained_last_branch)
         _session.release(badr);

      // the happy path where there is nothing to do: the child returned itself
      // unchanged (e.g. key not found during remove/update, or key already exists
      // during insert).
      if (sub_branches.count() == 1 && bref->address() == sub_branches.get_first_branch())
      {
         // Undo retains: no new node was created, so the retained refs
         // are unbalanced — release all children to restore balance.
         if constexpr (mode.is_shared())
            in->visit_branches([this](ptr_address br) { _session.release(br); });
         return in.address();
      }
      // integrate the sub_branches into the current node, and return the resulting branch set
      // to the parent node.
      return merge_branches<mode.make_shared_or_unique_only()>(parent_hint, in, br, sub_branches);
   }

   template <upsert_mode mode>
   branch_set tree_context::update(const sal::alloc_hint& parent_hint,
                                   smart_ref<leaf_node>&  leaf,
                                   key_view               key,
                                   branch_number          br)
   {
      // Record old value size and release old external value if needed
      auto old_value = leaf->get_value(br);
      if (old_value.is_value_node())
         _old_value_size = _session.get_ref(old_value.value_address())->size();
      else
         _old_value_size = old_value.size();

      // Per-txn version chain extension (Phase A):
      // When the working root carries a txn version (set by start_transaction
      // via make_unique_root) and the existing value is a value_node, extend
      // the chain on the existing value_node instead of releasing it and
      // allocating a fresh single-entry one. This preserves chain history
      // for snapshot readers and lets multiple writes within one txn
      // coalesce on the topmost entry.
      //
      // Limited to unique mode: in shared mode we're cloning into a new
      // leaf, so the chain semantics are different (snapshot copies must
      // not see in-flight updates).
      if constexpr (mode.is_unique())
      {
         if (uint64_t txn_ver = txn_version();
             txn_ver != 0 && old_value.is_value_node() &&
             _new_value.is_view())
         {
            auto vn_ref = _session.get_ref<value_node>(old_value.value_address());
            auto new_view = _new_value.view();
            if (!vn_ref->is_flat() &&
                new_view.size() <= value_node::max_inline_entry_size)
            {
               // Phase D three-way dispatch — same shape as
               // try_mvcc_upsert above:
               //   (a) same txn_ver + fits existing slot → in-place
               //       memcpy. modify_guard transparently COWs if the
               //       VN's page is RO from a prior sync; chain layout
               //       is preserved verbatim.
               //   (b) same txn_ver + doesn't fit → rebuild at larger
               //       size via mvcc_realloc{replace_last_tag}.
               //   (c) different version on top → append a new entry
               //       via mvcc_realloc.
               if (vn_ref->can_coalesce_in_place(txn_ver, new_view))
               {
                  vn_ref.modify()->coalesce_top_entry(new_view);
                  return leaf.address();
               }

               // Predicate failed — disambiguate (b) vs (c).
               bool coalesce = vn_ref->num_versions() > 0 &&
                               vn_ref->latest_version() == txn_ver;
               if (coalesce)
                  (void)_session.mvcc_realloc<value_node>(
                      vn_ref, vn_ref.obj(), txn_ver, new_view,
                      value_node::replace_last_tag{});
               else
                  (void)_session.mvcc_realloc<value_node>(
                      vn_ref, vn_ref.obj(), txn_ver, new_view);
               // mvcc_realloc preserves the value_node's ptr_address (only
               // moves data location); the leaf still points at it. No
               // leaf-level mutation needed.
               return leaf.address();
            }
         }
      }

      // Convert large values to value_nodes, using the leaf's clines as the
      // allocation hint so the value_node lands on an existing cline when possible.
      auto new_val = make_value(_new_value, leaf->clines());

      // Check if the leaf has enough space for the in-place update.
      // update_value() may allocate inline data (growing _alloc_pos) and/or
      // grow the cline table (growing _cline_cap). These modifications happen
      // non-atomically — if space runs out mid-update, the leaf is left corrupt.
      // can_apply() conservatively checks all space requirements up front.
      op::leaf_update update_op{.src = *leaf.obj(), .lb = br, .key = key, .value = new_val};

      if constexpr (mode.is_unique())
      {
         bool old_has_address =
             leaf->get_value_type(br) >= leaf_node::value_type_flag::value_node;

         switch (leaf->can_apply(update_op))
         {
            case leaf_node::can_apply_mode::modify:
               if (old_has_address)
                  _session.release(leaf->get_value(br).address());
               leaf.modify()->update_value(br, new_val);
               return leaf.address();
            case leaf_node::can_apply_mode::defrag:
               if (old_has_address)
                  _session.release(leaf->get_value(br).address());
               return _session.realloc<leaf_node>(leaf, update_op).address();
            case leaf_node::can_apply_mode::none:
            {
               // The leaf is at max capacity with no dead space and the new value
               // is larger than the old — it genuinely cannot fit. Treat as a
               // remove + insert which may split the leaf.
               //
               // TODO: This does two allocations (realloc to remove, then insert which
               // may split). A single-allocation approach would split the leaf around
               // the updated key, placing the updated entry in whichever half has room.
               // That mirrors what insert() does for new keys but avoids the extra copy.
               if (old_has_address)
                  _session.release(leaf->get_value(br).address());
               // Release the value_node allocated by make_value above — insert()
               // will allocate its own from _new_value.
               if (new_val.is_value_node())
                  _session.release(new_val.value_address());
               op::leaf_remove rm_op{.src = *leaf.obj(), .bn = br};
               auto removed = _session.realloc<leaf_node>(leaf, rm_op);
               // Now insert the key with the new value into the (possibly smaller) leaf.
               // Must use unique_upsert (not mode=unique_update) because the key was
               // removed — split_insert calls upsert<mode>() which must INSERT, not UPDATE.
               branch_number lb = removed->lower_bound(key);
               return insert<upsert_mode::unique_upsert>(parent_hint, removed, key, lb);
            }
         }
         std::unreachable();
      }

      // Shared mode: clone into a new max_leaf_size leaf (defragments dead space).
      {
         bool old_has_address =
             leaf->get_value_type(br) >= leaf_node::value_type_flag::value_node;

         // Check if the updated data fits in max_leaf_size. If the leaf is already
         // near capacity and the new value is larger, we must split instead.
         if (leaf->can_apply(update_op) != leaf_node::can_apply_mode::none) [[likely]]
         {
            retain_children(leaf);

            // Release old external value (retained above, so release brings it back to original)
            if (old_has_address)
               _session.release(leaf->get_value(br).address());

            auto new_leaf = _session.alloc<leaf_node>(parent_hint, update_op);
            prune_leaf_value_nodes(new_leaf);
            return new_leaf;
         }

         // Overflow: the leaf is at max capacity and the new value is larger.
         // Strategy: retain children selectively, create a removed leaf (unique,
         // ref=1), then insert the key back with the new value using unique mode.
         //
         // Ref counting:
         // - The original leaf (snapshot) holds c0..cN. Its destroy() releases them.
         // - The result node(s) hold c0..cN minus old_value plus new_value.
         //   Their destroy() releases them.
         // - So the N-1 surviving children need +1 (shared between snapshot and result).
         // - The old value at br does NOT need +1 (only snapshot holds it).
         // - retain_children(leaf) retains ALL children including old value at br,
         //   so we must un-retain the old value to keep it balanced.

         retain_children(leaf);
         // Un-retain the old value: it's only in the snapshot leaf, not the result.
         if (old_has_address)
            _session.release(leaf->get_value(br).address());

         // Release the value_node allocated by make_value — insert() will create its own.
         if (new_val.is_value_node())
            _session.release(new_val.value_address());

         // Create a new leaf without the key at br (copies N-1 child refs from source).
         // This leaf has ref=1, so we can operate on it in unique mode.
         op::leaf_remove rm_op{.src = *leaf.obj(), .bn = br};
         auto rm_addr = _session.alloc<leaf_node>(parent_hint, rm_op);
         prune_leaf_value_nodes(rm_addr);
         auto removed = _session.get_ref<leaf_node>(rm_addr);
         branch_number new_lb = removed->lower_bound(key);
         // Insert using unique mode since removed has ref=1.
         return insert<upsert_mode::unique_upsert>(parent_hint, removed, key, new_lb);
      }
   }
   template <upsert_mode mode>
   branch_set tree_context::remove(const sal::alloc_hint& parent_hint,
                                   smart_ref<leaf_node>&  leaf,
                                   key_view               key,
                                   branch_number          lb)
   {
      // make sure the key exists and is an exact match, if it isn't
      // an exact match then no changes are made, simply return itself unmodified.
      if constexpr (not mode.must_remove())
      {
         if (leaf->get_key(lb) != key) [[unlikely]]
            return leaf.address();
      }
      if constexpr (mode.must_remove())
      {
         if (leaf->get_key(lb) != key) [[unlikely]]
         {
            assert(!"must_remove precondition violated: key does not exist");
            std::unreachable();
         }
      }
      auto old_value = leaf->get_value(lb);
      if (old_value.is_value_node())
         this->_old_value_size = _session.get_ref(old_value.value_address())->size();
      else
         this->_old_value_size = leaf->get_value(lb).size();
      // at this point we know the key exists and must be removed

      if constexpr (mode.is_unique())
      {
         if (leaf->num_branches() == 1) [[unlikely]]
         {
            // Don't release the value_node here — the dispatch function will
            // release this leaf (because result.count()==0), and leaf::destroy()
            // will cascade-release any value_node/subtree addresses.
            return {};
         }

         if (leaf->get_value_type(lb) >= leaf_node::value_type_flag::value_node)
         {
            _session.release(leaf->get_value(lb).address());
         }

         leaf.modify()->remove(lb);
         return leaf.address();
      }
      else  // constexpr mode.is_shared()
      {
         if (leaf->num_branches() == 1) [[unlikely]]
         {
            //SAL_ERROR("remove shared: leaf has only one branch");
            return {};
         }

         retain_children(leaf);
         op::leaf_remove remove_op{.src = *leaf.obj(), .bn = lb};
         auto            new_leaf = _session.alloc<leaf_node>(parent_hint, remove_op);
         prune_leaf_value_nodes(new_leaf);

         // if bn is an address, we need to release the address (value_node or subtree)
         if (leaf->get_value_type(lb) >= leaf_node::value_type_flag::value_node)
         {
            _session.release(leaf->get_value(lb).address());
         }

         // if bn is the last branch, return null
         return new_leaf;
      }
   }

   template <upsert_mode mode>
   branch_set tree_context::insert(const sal::alloc_hint& parent_hint,
                                   smart_ref<leaf_node>&  leaf,
                                   key_view               key,
                                   branch_number          lb)
   {
      //     SAL_INFO("insert: leaf {} key: {} ptr: {}", leaf.address(), key, leaf.obj());
      if constexpr (mode.is_shared())
      {
         //         SAL_WARN(" insert: retaining children before inserting");
         retain_children(leaf);
      }
      uint8_t cline_idx = 0xff;

      if (_new_value.is_view())
      {
         auto v = _new_value.view();
         if (v.size() > 64)
            _new_value =
                value_type::make_value_node(_session.alloc<value_node>(leaf->clines(), _new_value));
      }
      // this check is redundant in the case we just made value node above...
      if (_new_value.is_address())
      {
         //       SAL_ERROR("insert: new_value is an address");
         cline_idx = leaf->find_cline_index(_new_value.address());
         if (cline_idx >= 16)
         {
            //         SAL_ERROR("insert: split");
            // split index will have to re-assign the ptr_address of the
            // value node after identifying what side of the split the value node
            // should go on so that it isn't sharing a cline branches from another
            // node.
            return split_insert<mode>(parent_hint, leaf, key, lb);
         }
      }

      op::leaf_insert insert_op{
          .src = *leaf.obj(), .lb = lb, .key = key, .value = _new_value, .cline_idx = cline_idx};

      if constexpr (mode.is_unique())
      {
         //     SAL_INFO("insert: unique mode: leaf {} key: {} ptr: {}", leaf.address(), key, leaf.obj());
         switch (leaf->can_apply(insert_op))
         {
            case leaf_node::can_apply_mode::modify:
               [[likely]] leaf.modify()->apply(insert_op);
               return leaf.address();
            case leaf_node::can_apply_mode::defrag:
               return _session.realloc<leaf_node>(leaf, leaf.obj(), insert_op).address();
            case leaf_node::can_apply_mode::none:
               [[unlikely]] return split_insert<mode>(parent_hint, leaf, key, lb);
            default:
               std::unreachable();
         }
      }
      else if constexpr (mode.is_shared())
      {
         switch (leaf->can_apply(insert_op))
         {
            case leaf_node::can_apply_mode::modify:
            case leaf_node::can_apply_mode::defrag:
            {
               //SAL_INFO("insert: shared mode: cloning leaf {} with insert_op leaf.obj {}",
               //         leaf.address(), leaf.obj());
               auto new_leaf = _session.alloc<leaf_node>(parent_hint, leaf.obj(), insert_op);
               prune_leaf_value_nodes(new_leaf);
               return new_leaf;
            }
            case leaf_node::can_apply_mode::none:
               //SAL_ERROR("insert: split_insert ");
               [[unlikely]] return split_insert<mode>(parent_hint, leaf, key, lb);
            default:
               std::unreachable();
         }
      }
      std::unreachable();
   }
   inline void tree_context::prune_leaf_value_nodes(ptr_address leaf_addr)
   {
      auto leaf_ref = _session.get_ref<leaf_node>(leaf_addr);
      const uint32_t n = leaf_ref->num_branches();

      // Quick scan: any multi-version value_nodes?
      bool need_prune = false;
      for (uint32_t i = 0; i < n && !need_prune; ++i)
      {
         branch_number bn(i);
         if (leaf_ref->get_value_type(bn) != leaf_node::value_type_flag::value_node)
            continue;
         auto vn_ref = _session.get_ref<value_node>(leaf_ref->get_value_address(bn));
         if (vn_ref->num_versions() > 1)
            need_prune = true;
      }
      if (!need_prune)
         return;

      // Replace each multi-version value_node with a single-version copy
      leaf_ref.modify(
          [&](leaf_node* mutable_leaf)
          {
             for (uint32_t i = 0; i < n; ++i)
             {
                branch_number bn(i);
                if (mutable_leaf->get_value_type(bn) != leaf_node::value_type_flag::value_node)
                   continue;

                auto old_addr = mutable_leaf->get_value_address(bn);
                auto vn_ref   = _session.get_ref<value_node>(old_addr);
                if (vn_ref->num_versions() <= 1)
                   continue;

                // Skip if latest entry is a tombstone or null (no data to preserve)
                int16_t latest_off = vn_ref->get_entry_offset(vn_ref->num_versions() - 1);
                if (latest_off < value_node::offset_data_start)
                   continue;

                // Allocate a new single-version value_node with the latest value
                value_view  latest_data = vn_ref->get_data();
                ptr_address new_vn_addr =
                    _session.alloc<value_node>(leaf_ref->clines(), latest_data);

                // Swap the address in the leaf
                mutable_leaf->update_value(bn, value_type::make_value_node(new_vn_addr));

                // Release the old multi-version value_node
                _session.release(old_addr);
             }
          });
   }

   inline std::string format(const sal::alloc_hint& hint)
   {
      std::string formatted_span = "[";
      for (size_t i = 0; i < hint.size(); ++i)
      {
         formatted_span += std::format("{}", *hint[i]);
         if (i < hint.size() - 1)
         {
            formatted_span += ", ";
         }
      }
      formatted_span += "]";
      return formatted_span;
   }

   template <upsert_mode mode>
   branch_set tree_context::split_insert(const sal::alloc_hint& parent_hint,
                                         smart_ref<leaf_node>&  leaf,
                                         key_view               key,
                                         branch_number          lb)
   {
      branch_set result;

      // Special case: leaf has only 1 entry and can't fit a second key.
      // get_split_pos() requires nb > 1, so we manually build the split:
      // create two single-entry leaves and an inner_prefix_node.
      if (leaf->num_branches() == 1)
      {
         key_view existing_key = leaf->get_key(branch_zero);

         // Find common prefix length
         size_t cplen = 0;
         size_t max_cp = std::min(existing_key.size(), key.size());
         while (cplen < max_cp && existing_key[cplen] == key[cplen])
            ++cplen;

         key_view cprefix = existing_key.substr(0, cplen);

         // Determine dividing byte and which key goes left vs right
         // The key with the smaller byte at cplen goes left (divider = larger byte)
         uint8_t existing_byte = (cplen < existing_key.size()) ? existing_key[cplen] : 0;
         uint8_t new_byte      = (cplen < key.size()) ? key[cplen] : 0;

         // Alloc both leaves (don't realloc leaf — we need it for remake_inner_prefix)
         ptr_address existing_leaf =
             _session.alloc<leaf_node>({}, leaf.obj(), cprefix, branch_zero, branch_number(1));

         key_view    new_key_suffix = key.substr(cplen);
         ptr_address new_leaf = _session.alloc<leaf_node>(
             {&existing_leaf, 1}, new_key_suffix, make_value(_new_value, {&existing_leaf, 1}));

         // Build inner_prefix_node: the divider is the larger of the two bytes
         ptr_address left_addr, right_addr;
         uint8_t divider;
         if (existing_byte < new_byte)
         {
            left_addr  = existing_leaf;
            right_addr = new_leaf;
            divider    = new_byte;
         }
         else
         {
            left_addr  = new_leaf;
            right_addr = existing_leaf;
            divider    = existing_byte;
         }

         branch_set branches(divider, left_addr, right_addr);
         if constexpr (mode.is_unique())
         {
            // realloc leaf into the inner_prefix_node so the address is preserved
            return remake_inner_prefix(leaf, cprefix, branches).address();
         }
         else if constexpr (mode.is_shared())
         {
            // retain_children(leaf) already called by insert() before entering split_insert
            return make_inner_prefix(parent_hint, cprefix, branches);
         }
         std::unreachable();
      }

      leaf_node::split_pos spos      = leaf->get_split_pos();
      branch_number        left_size = branch_number(spos.less_than_count);
      branch_number        right_end = branch_number(leaf->num_branches());

      if (spos.cprefix.size() > 0)
      {
         ptr_address left =
             _session.alloc<leaf_node>({}, leaf.obj(), spos.cprefix, branch_zero, left_size);
         ptr_address right =
             _session.alloc<leaf_node>({&left, 1}, leaf.obj(), spos.cprefix, left_size, right_end);

         if constexpr (mode.is_unique())
         {
            smart_ref<inner_prefix_node> remake =
                remake_inner_prefix(leaf, spos.cprefix, branch_set(spos.divider, left, right));
            return upsert<mode>(remake->hint(), remake, key);
         }
         else if constexpr (mode.is_shared())
         {
            ptr_address new_in =
                make_inner_prefix(parent_hint, spos.cprefix, branch_set(spos.divider, left, right));
            //            SAL_WARN("split_insert hint: {}", parent_hint.size());
            smart_ref<inner_prefix_node> remake = _session.get_ref<inner_prefix_node>(new_in);
            //            SAL_INFO("split_insert: new_in: {} ref: {} key: {}", remake.address(), remake.ref(),
            //                    key);
            return upsert<mode.make_unique()>(remake->hint(), remake, key);
         }
         std::unreachable();
      }
      ptr_address left;
      if constexpr (mode.is_unique())
         left = _session.realloc<leaf_node>(leaf, leaf.obj(), key_view(), branch_zero, left_size)
                    .address();
      else
      {
         left =
             _session.alloc<leaf_node>(parent_hint, leaf.obj(), key_view(), branch_zero, left_size);
      }
      ptr_address right =
          _session.alloc<leaf_node>(parent_hint, leaf.obj(), key_view(), left_size, right_end);
      //     SAL_WARN("left: {} right: {} delta: {}", left, right, right - left);

      if (key < spos.divider_key())
      {
         auto left_ref = _session.get_ref<leaf_node>(left);
         auto left_lb  = left_ref->lower_bound(key);
         //SAL_ERROR("upsert into left branch");
         // left is always a new node in shared mode, so it is unique by the time it gets here
         auto result = upsert<mode.make_unique()>(left_ref->clines(), left_ref, key);
         result.push_back(spos.divider, right);
         return result;
      }
      else
      {
         auto right_ref = _session.get_ref<leaf_node>(right);
         auto right_lb  = right_ref->lower_bound(key);
         //SAL_ERROR("upsert into right branch");
         /// right is ALWAYS a new node allocated above; therefore it is unique
         auto result = upsert<mode.make_unique()>(right_ref->hint(), right_ref, key);
         result.push_front(left, spos.divider);
         return result;
      }
      std::unreachable();
   }

   template <upsert_mode mode>
   branch_set tree_context::upsert(const sal::alloc_hint& parent_hint,
                                   smart_ref<leaf_node>&  leaf,
                                   key_view               key)
   {
      //SAL_INFO("upsert: leaf {} key: {}", leaf.address(), key);
      if constexpr (mode.must_update())
      {
         branch_number br = leaf->get(key);
         if (br == leaf->num_branches()) [[unlikely]]
         {
            // In shared mode, retain_children was called at each inner node
            // level above us — throwing would leave those refs unbalanced.
            // Return the unchanged leaf address as a no-op signal instead.
            if constexpr (mode.is_shared())
               return leaf.address();
            assert(!"update precondition violated: key does not exist");
            std::unreachable();
         }
         return update<mode>(parent_hint, leaf, key, br);
      }
      if constexpr (mode.must_remove())
      {
         branch_number br = leaf->get(key);
         if (br == leaf->num_branches()) [[unlikely]]
         {
            if constexpr (mode.is_shared())
               return leaf.address();
            assert(!"must_remove precondition violated: key does not exist");
            std::unreachable();
         }
         return remove<mode>(parent_hint, leaf, key, br);
      }
      else
      {
         branch_number lb = leaf->lower_bound(key);
         if constexpr (mode.is_upsert())
         {
            if (lb != leaf->num_branches() and leaf->get_key(lb) == key)
               return update<mode>(parent_hint, leaf, key, lb);
            else
               return insert<mode>(parent_hint, leaf, key, lb);
         }
         else if constexpr (mode.is_insert())
         {
            if (not(lb == leaf->num_branches() or leaf->get_key(lb) != key)) [[unlikely]]
            {
               if constexpr (mode.is_shared())
                  return leaf.address();
               assert(!"insert precondition violated: key already exists");
               std::unreachable();
            }
            return insert<mode>(parent_hint, leaf, key, lb);
         }
         else if constexpr (mode.is_remove())
         {
            if (lb != leaf->num_branches())
               return remove<mode>(parent_hint, leaf, key, lb);
            else
               return leaf.address();
         }
      }
      std::unreachable();
   }

   // ─── MVCC find target ─────────────────────────────────────────────
   inline ptr_address tree_context::mvcc_find_target(key_view key) const
   {
      if (!_root)
         return sal::null_ptr_address;  // empty tree → COW (allocate root)

      ptr_address addr     = _root.address();
      key_view    remaining = key;

      for (;;)
      {
         auto ref = _session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::inner_prefix:
            {
               auto ipn  = ref.as<inner_prefix_node>();
               if (ipn->epoch() < _current_epoch)
                  return sal::null_ptr_address;  // stale epoch → COW cascade for maintenance
               auto cpre = common_prefix(remaining, ipn->prefix());
               if (cpre != ipn->prefix())
                  return sal::null_ptr_address;  // prefix mismatch → COW fallback
               remaining = remaining.substr(cpre.size());
               addr      = ipn->get_branch(ipn->lower_bound(remaining));
               continue;
            }
            case node_type::inner:
            {
               auto in = ref.as<inner_node>();
               if (in->epoch() < _current_epoch)
                  return sal::null_ptr_address;  // stale epoch → COW cascade for maintenance
               addr    = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::leaf:
            {
               auto          leaf = ref.as<leaf_node>();
               branch_number lb   = leaf->lower_bound(remaining);

               if (lb != leaf->num_branches() && leaf->get_key(lb) == remaining)
               {
                  auto val = leaf->get_value(lb);
                  if (val.is_value_node())
                     return val.value_address();  // Case B: lock value_node
                  return leaf.address();           // Case A: lock leaf (inline → value_node)
               }
               return leaf.address();              // Case C: lock leaf (new key insert)
            }
            default:
               return sal::null_ptr_address;
         }
      }
   }

   // ─── try_mvcc_upsert: stripe-lock-safe, no COW fallback ─────────
   inline bool tree_context::try_mvcc_upsert(key_view key, value_type value, uint64_t version)
   {
      sal::read_lock lock = _session.lock();

      if (!_root)
         return false;

      // Flat read-only traversal to the leaf
      ptr_address addr      = _root.address();
      key_view    remaining = key;

      for (;;)
      {
         auto ref = _session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::inner_prefix:
            {
               auto ipn  = ref.as<inner_prefix_node>();
               if (ipn->epoch() < _current_epoch)
                  return false;  // stale epoch → COW cascade
               auto cpre = common_prefix(remaining, ipn->prefix());
               if (cpre != ipn->prefix())
                  return false;  // prefix mismatch → COW
               remaining = remaining.substr(cpre.size());
               addr      = ipn->get_branch(ipn->lower_bound(remaining));
               continue;
            }
            case node_type::inner:
            {
               auto in = ref.as<inner_node>();
               if (in->epoch() < _current_epoch)
                  return false;  // stale epoch → COW cascade
               addr    = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::leaf:
            {
               auto          leaf     = ref.as<leaf_node>();
               branch_number lb       = leaf->lower_bound(remaining);

               if (lb != leaf->num_branches() && leaf->get_key(lb) == remaining)
               {
                  // Key exists
                  auto old_val = leaf->get_value(lb);

                  if (old_val.is_value_node())
                  {
                     auto vref = _session.get_ref<value_node>(old_val.value_address());

                     // Flat value_nodes (large values) can't use MVCC entry system
                     if (vref->is_flat())
                        return false;  // COW fallback

                     // Case B: append to existing value_node. Three-way
                     // dispatch on the existing chain's top entry:
                     //
                     //   (a) Top is at OUR version + new value fits the
                     //       existing slot
                     //         → in-place memcpy (predicate path).
                     //         If the page is RO from a prior sync's
                     //         mprotect, modify_guard transparently
                     //         COWs the value_node first; the chain
                     //         layout is preserved so the slot offset
                     //         and size are unchanged in the new copy.
                     //         0 allocations on writable, 1 on RO.
                     //   (b) Top is at OUR version + new value too big
                     //       → REBUILD chain at larger size via
                     //         mvcc_realloc{replace_last_tag}. Chain
                     //         length unchanged. 1 allocation.
                     //   (c) Top is at a different version
                     //       → APPEND a new chain entry via
                     //         mvcc_realloc. Chain grows by 1. 1
                     //         allocation.
                     auto new_val = value.is_view() ? value.view() : value_view();
                     if (vref->can_coalesce_in_place(version, new_val))
                     {
                        // (a): predicate first (const, no side effects);
                        // modify_guard fires only after the decision so
                        // we don't pay for a COW we won't use.
                        vref.modify()->coalesce_top_entry(new_val);
                        return true;
                     }
                     // Predicate failed — disambiguate (b) vs (c).
                     bool coalesce =
                         vref->num_versions() > 0 && vref->latest_version() == version;
                     if (coalesce)
                        (void)_session.mvcc_realloc<value_node>(
                            vref, vref.obj(), version, new_val,
                            value_node::replace_last_tag{});
                     else if (_dead_snap)
                        (void)_session.mvcc_realloc<value_node>(
                            vref, vref.obj(), version, new_val, _dead_snap);
                     else
                        (void)_session.mvcc_realloc<value_node>(
                            vref, vref.obj(), version, new_val);
                     return true;
                  }

                  // Case A: inline → value_node promotion + leaf update
                  value_view old_data = old_val.view();
                  auto       new_data = value.is_view() ? value.view() : value_view();
                  uint64_t   old_ver  = 0;

                  auto vn_addr = _session.alloc<value_node>(
                      leaf->clines(), old_ver, old_data, version, new_data);

                  value_type     vn_value = value_type::make_value_node(vn_addr);
                  op::leaf_update update_op{.src = *leaf.obj(), .lb = lb,
                                            .key = remaining, .value = vn_value};

                  if (leaf->can_apply(update_op) != leaf_node::can_apply_mode::none)
                  {
                     (void)_session.mvcc_realloc<leaf_node>(leaf, update_op);
                     return true;
                  }
                  // Overflow — release allocated value_node, signal COW needed
                  _session.release(vn_addr);
                  return false;
               }

               // Case C: new key — insert into leaf via CB relocation
               auto new_val = make_value(value, leaf->clines());

               uint8_t cline_idx = 0xff;
               if (new_val.is_address())
                  cline_idx = leaf->find_cline_index(new_val.address());

               op::leaf_insert insert_op{
                   .src = *leaf.obj(), .lb = lb, .key = remaining,
                   .value = new_val, .cline_idx = cline_idx};

               if (cline_idx < 16 &&
                   leaf->can_apply(insert_op) != leaf_node::can_apply_mode::none)
               {
                  (void)_session.mvcc_realloc<leaf_node>(leaf, leaf.obj(), insert_op);
                  return true;
               }
               // Overflow or cline overflow — signal COW needed
               if (new_val.is_value_node())
                  _session.release(new_val.value_address());
               return false;
            }
            default:
               return false;
         }
      }
   }

   // ─── try_mvcc_remove: stripe-lock-safe, no COW fallback ─────────
   inline bool tree_context::try_mvcc_remove(key_view key, uint64_t version)
   {
      if (!_root)
         return true;  // nothing to remove

      sal::read_lock lock = _session.lock();

      ptr_address addr      = _root.address();
      key_view    remaining = key;

      for (;;)
      {
         auto ref = _session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::inner_prefix:
            {
               auto ipn  = ref.as<inner_prefix_node>();
               if (ipn->epoch() < _current_epoch)
                  return false;  // stale epoch → COW cascade
               auto cpre = common_prefix(remaining, ipn->prefix());
               if (cpre != ipn->prefix())
                  return true;  // key doesn't exist — no-op
               remaining = remaining.substr(cpre.size());
               addr      = ipn->get_branch(ipn->lower_bound(remaining));
               continue;
            }
            case node_type::inner:
            {
               auto in = ref.as<inner_node>();
               if (in->epoch() < _current_epoch)
                  return false;  // stale epoch → COW cascade
               addr    = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::leaf:
            {
               auto          leaf = ref.as<leaf_node>();
               branch_number lb   = leaf->lower_bound(remaining);
               if (lb == leaf->num_branches() || leaf->get_key(lb) != remaining)
                  return true;  // key doesn't exist — no-op

               auto old_val = leaf->get_value(lb);
               if (old_val.is_value_node())
               {
                  auto vref = _session.get_ref<value_node>(old_val.value_address());
                  if (vref->is_flat())
                     return false;  // COW fallback for flat nodes

                  // Append tombstone to existing value_node, with coalesce
                  // when the topmost chain entry already belongs to this
                  // version (caller updating same key twice in the same
                  // per-txn version scope).
                  bool coalesce =
                      vref->num_versions() > 0 && vref->latest_version() == version;
                  if (coalesce)
                     (void)_session.mvcc_realloc<value_node>(
                         vref, vref.obj(), version, nullptr,
                         value_node::replace_last_tag{});
                  else if (_dead_snap)
                     (void)_session.mvcc_realloc<value_node>(
                         vref, vref.obj(), version, nullptr, _dead_snap);
                  else
                     (void)_session.mvcc_realloc<value_node>(
                         vref, vref.obj(), version, nullptr);
                  return true;
               }

               // Inline value: promote to value_node + tombstone, update leaf
               value_view old_data = old_val.view();
               auto       vn_addr  = _session.alloc<value_node>(leaf->clines(), old_data);
               auto       vref     = _session.get_ref<value_node>(vn_addr);
               if (_dead_snap)
                  (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr, _dead_snap);
               else
                  (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr);

               value_type     vn_value = value_type::make_value_node(vn_addr);
               op::leaf_update update_op{.src = *leaf.obj(), .lb = lb,
                                         .key = remaining, .value = vn_value};
               if (leaf->can_apply(update_op) != leaf_node::can_apply_mode::none)
               {
                  (void)_session.mvcc_realloc<leaf_node>(leaf, update_op);
                  return true;
               }
               // Overflow — clean up and signal COW needed
               // (value_node and its tombstone entry are wasted but refcount-safe)
               _session.release(vn_addr);
               return false;
            }
            default:
               return false;
         }
      }
   }

   // ─── MVCC upsert implementation ──────────────────────────────────
   inline void tree_context::mvcc_upsert(key_view key, value_type value, uint64_t version)
   {
      sal::read_lock lock = _session.lock();

      if (!_root)
      {
         // Preserve _ver — see same pattern in insert() above.
         auto leaf_addr = _session.alloc<leaf_node>(
             sal::alloc_hint(), key, make_value(value, sal::alloc_hint()));
         _root.give(leaf_addr);
         return;
      }

      // Flat read-only traversal to the leaf using ptr_address.
      // If any inner node has a stale epoch, fall back to full COW cascade
      // which updates epochs and prunes multi-version value_nodes.
      ptr_address addr = _root.address();
      key_view remaining = key;

      for (;;)
      {
         auto ref = _session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::inner_prefix:
            {
               auto ipn = ref.as<inner_prefix_node>();
               auto cpre = common_prefix(remaining, ipn->prefix());
               if (cpre != ipn->prefix() || ipn->epoch() < _current_epoch)
               {
                  // Prefix mismatch or stale epoch — fall back to COW.
                  upsert<upsert_mode::unique_upsert>(key, std::move(value));
                  return;
               }
               remaining = remaining.substr(cpre.size());
               addr = ipn->get_branch(ipn->lower_bound(remaining));
               continue;
            }
            case node_type::inner:
            {
               auto in = ref.as<inner_node>();
               if (in->epoch() < _current_epoch)
               {
                  upsert<upsert_mode::unique_upsert>(key, std::move(value));
                  return;
               }
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::leaf:
            {
               auto leaf = ref.as<leaf_node>();
               mvcc_upsert_leaf(leaf, remaining, key, value, version);
               return;
            }
            default:
               std::unreachable();
         }
      }
   }

   inline void tree_context::mvcc_upsert_leaf(smart_ref<leaf_node>& leaf,
                                               key_view              leaf_key,
                                               key_view              full_key,
                                               value_type&           value,
                                               uint64_t              version)
   {
      branch_number lb = leaf->lower_bound(leaf_key);

      if (lb != leaf->num_branches() && leaf->get_key(lb) == leaf_key)
      {
         // Key exists — update
         auto old_val = leaf->get_value(lb);

         if (old_val.is_value_node())
         {
            auto vref = _session.get_ref<value_node>(old_val.value_address());
            if (vref->is_flat())
            {
               // Flat value_node (large value) — COW fallback
               upsert<upsert_mode::unique_upsert>(full_key, std::move(value));
               return;
            }
            // Case B: existing key with value_node — append version via CB relocation
            auto new_val = value.is_view() ? value.view() : value_view();
            if (_dead_snap)
               (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, new_val, _dead_snap);
            else
               (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, new_val);
            // ptr_address unchanged — leaf and inner nodes untouched
            return;
         }

         // Case A: existing key with inline value — promote to 2-entry value_node
         value_view old_data = old_val.view();
         auto new_data = value.is_view() ? value.view() : value_view();
         uint64_t old_ver = 0;

         auto vn_addr = _session.alloc<value_node>(
             leaf->clines(), old_ver, old_data, version, new_data);

         // COW the leaf to update the value branch from inline to value_node
         value_type vn_value = value_type::make_value_node(vn_addr);
         op::leaf_update update_op{.src = *leaf.obj(), .lb = lb,
                                   .key = leaf_key, .value = vn_value};

         if (leaf->can_apply(update_op) != leaf_node::can_apply_mode::none)
         {
            (void)_session.mvcc_realloc<leaf_node>(leaf, update_op);
         }
         else
         {
            // Overflow — fall back to full COW cascade
            _session.release(vn_addr);  // release the value_node we just allocated
            upsert<upsert_mode::unique_upsert>(full_key, std::move(value));
         }
         return;
      }

      // Case C: new key — insert into leaf via CB relocation
      auto new_val = make_value(value, leaf->clines());

      uint8_t cline_idx = 0xff;
      if (new_val.is_address())
         cline_idx = leaf->find_cline_index(new_val.address());

      op::leaf_insert insert_op{
          .src = *leaf.obj(), .lb = lb, .key = leaf_key,
          .value = new_val, .cline_idx = cline_idx};

      if (cline_idx < 16 &&
          leaf->can_apply(insert_op) != leaf_node::can_apply_mode::none)
      {
         (void)_session.mvcc_realloc<leaf_node>(leaf, leaf.obj(), insert_op);
      }
      else
      {
         // Overflow or cline overflow — fall back to full COW cascade
         if (new_val.is_value_node())
            _session.release(new_val.value_address());
         upsert<upsert_mode::unique_upsert>(full_key, std::move(value));
      }
   }

   // ─── MVCC remove implementation ──────────────────────────────────
   inline void tree_context::mvcc_remove(key_view key, uint64_t version)
   {
      if (!_root)
         return;

      sal::read_lock lock = _session.lock();

      // Flat read-only traversal to the leaf using ptr_address.
      // Stale epoch triggers COW cascade for structural maintenance.
      ptr_address addr = _root.address();
      key_view remaining = key;

      for (;;)
      {
         auto ref = _session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::inner_prefix:
            {
               auto ipn = ref.as<inner_prefix_node>();
               auto cpre = common_prefix(remaining, ipn->prefix());
               if (cpre != ipn->prefix())
                  return;  // key doesn't exist
               if (ipn->epoch() < _current_epoch)
               {
                  remove(key);  // stale epoch — COW cascade
                  return;
               }
               remaining = remaining.substr(cpre.size());
               addr = ipn->get_branch(ipn->lower_bound(remaining));
               continue;
            }
            case node_type::inner:
            {
               auto in = ref.as<inner_node>();
               if (in->epoch() < _current_epoch)
               {
                  remove(key);  // stale epoch — COW cascade
                  return;
               }
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::leaf:
            {
               auto leaf = ref.as<leaf_node>();
               branch_number lb = leaf->lower_bound(remaining);
               if (lb == leaf->num_branches() || leaf->get_key(lb) != remaining)
                  return;  // key doesn't exist

               auto old_val = leaf->get_value(lb);
               if (old_val.is_value_node())
               {
                  auto vref = _session.get_ref<value_node>(old_val.value_address());
                  if (vref->is_flat())
                  {
                     remove(key);  // COW fallback for flat nodes
                     return;
                  }
                  // Append tombstone to existing value_node
                  if (_dead_snap)
                     (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr, _dead_snap);
                  else
                     (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr);
               }
               else
               {
                  // Promote inline value to value_node with old data + tombstone.
                  // Create single-entry value_node with old data, then append tombstone.
                  value_view old_data = old_val.view();
                  auto vn_addr = _session.alloc<value_node>(leaf->clines(), old_data);
                  auto vref = _session.get_ref<value_node>(vn_addr);
                  if (_dead_snap)
                     (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr, _dead_snap);
                  else
                     (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr);

                  // Update leaf to point to value_node
                  value_type vn_value = value_type::make_value_node(vn_addr);
                  op::leaf_update update_op{.src = *leaf.obj(), .lb = lb,
                                            .key = remaining, .value = vn_value};
                  if (leaf->can_apply(update_op) != leaf_node::can_apply_mode::none)
                     (void)_session.mvcc_realloc<leaf_node>(leaf, update_op);
                  else
                     remove(key);  // fall back to COW remove
               }
               return;
            }
            default:
               std::unreachable();
         }
      }
   }

   // ─── Defrag implementation ──────────────────────────────────────
   inline uint64_t tree_context::defrag()
   {
      if (!_root || !_dead_snap)
         return 0;

      sal::read_lock lock = _session.lock();
      return defrag_subtree(_root.address());
   }

   inline uint64_t tree_context::defrag_subtree(ptr_address addr)
   {
      uint64_t cleaned = 0;
      auto ref = _session.get_ref(addr);

      switch (node_type(ref->type()))
      {
         case node_type::inner_prefix:
         {
            auto ipn = ref.as<inner_prefix_node>();
            for (uint8_t i = 0; i < ipn->num_branches(); ++i)
               cleaned += defrag_subtree(ipn->get_branch(branch_number(i)));
            break;
         }
         case node_type::inner:
         {
            auto in = ref.as<inner_node>();
            for (uint8_t i = 0; i < in->num_branches(); ++i)
               cleaned += defrag_subtree(in->get_branch(branch_number(i)));
            break;
         }
         case node_type::leaf:
         {
            auto leaf = ref.as<leaf_node>();
            cleaned += defrag_leaf(leaf);
            break;
         }
         default:
            break;
      }
      return cleaned;
   }

   inline uint64_t tree_context::defrag_leaf(smart_ref<leaf_node>& leaf)
   {
      uint64_t cleaned = 0;

      for (branch_number i{0}; i < leaf->num_branches(); ++i)
      {
         auto val = leaf->get_value(i);
         if (!val.is_value_node())
            continue;

         auto vref = _session.get_ref<value_node>(val.value_address());
         if (vref->has_dead_entries(_dead_snap))
         {
            (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), _dead_snap);
            ++cleaned;
         }
      }
      return cleaned;
   }

}  // namespace psitri

#include <psitri/range_remove.hpp>
