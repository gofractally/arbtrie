#pragma once
#include <array>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <psitri/count_keys.hpp>
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/node.hpp>
#include <psitri/node/value_node.hpp>
#include <psitri/upsert_mode.hpp>
#include <psitri/version_compare.hpp>
#include <sal/allocator_session.hpp>
#include <sal/smart_ptr.hpp>
#include "sal/numbers.hpp"

#ifndef PSITRI_ENABLE_ADAPTIVE_INNER_NODES
#define PSITRI_ENABLE_ADAPTIVE_INNER_NODES 1
#endif

namespace psitri
{
   using sal::alloc_header;
   using sal::smart_ptr;
   using sal::smart_ref;
   class tree_context
   {
      enum class tree_family : uint8_t
      {
         psitri,
         bplus
      };

      value_type                   _new_value;
      sal::allocator_session&      _session;
      sal::smart_ptr<alloc_header> _root;
      int                          _old_value_size = -1;
      int64_t  _delta_removed_keys = 0;  // negative count, or negative presence marker
      bool     _count_removed_keys = true;
      bool     _collapse_enabled   = true;
      const live_range_map::snapshot* _dead_snap     = nullptr;
      uint64_t                        _epoch_base   = 0;
      uint64_t                        _root_version = 0;
      uint64_t                        _root_value_version = 0;
      std::vector<ptr_address>        _pending_releases;
      tree_family                     _family = tree_family::psitri;

      static tree_family default_tree_family() noexcept
      {
         const char* env = std::getenv("PSITRI_TREE_FAMILY");
         if (env && (std::strcmp(env, "bplus") == 0 || std::strcmp(env, "b+tree") == 0))
            return tree_family::bplus;
         return tree_family::psitri;
      }

      static tree_family family_from_type(node_type type) noexcept
      {
         return type == node_type::bplus_inner ? tree_family::bplus : tree_family::psitri;
      }

      sal::pending_release_list make_pending_release_list(std::size_t reserve_hint)
      {
         if (_pending_releases.capacity() < reserve_hint)
            _pending_releases.reserve(reserve_hint);
         return sal::pending_release_list(_pending_releases);
      }

     public:
      void set_collapse_enabled(bool enabled) { _collapse_enabled = enabled; }
      void set_dead_versions(const live_range_map::snapshot* s) { _dead_snap = s; }
      void set_epoch_base(uint64_t e)
      {
         _epoch_base = version_token(e, last_unique_version_bits);
         if (_root_version == 0)
         {
            _root_version = _epoch_base;
            _root_value_version = version_token(e, value_version_bits);
         }
      }
      void set_root_version(uint64_t v)
      {
         _root_version = version_token(v, last_unique_version_bits);
         _root_value_version = version_token(v, value_version_bits);
      }
      bool needs_structural_refresh(uint64_t last_unique_version) const noexcept
      {
         return needs_unique_refresh(last_unique_version, _epoch_base, _root_version);
      }
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
         _family = _root ? family_from_type(node_type(_session.get_ref<node>(_root.address())->type()))
                         : default_tree_family();
         const uint64_t ver = txn_version();
         _root_version = version_token(ver, last_unique_version_bits);
         _root_value_version = version_token(ver, value_version_bits);
         SAL_TRACE("tree_context constructor: {} {}", &_root, _root.address());
      }
      /**
       * Given a value type, if it is too large for inline converts it to a value_node,
       * otherwise returns the value type unchanged.
       */
      value_type make_value(value_type value, sal::alloc_hint hint) noexcept
      {
         if (value.is_subtree() || value.is_value_node())
            return value;  // already converted
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
         auto     ref    = _session.get_ref(addr);
         auto     nt     = node_type(ref->type());
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
            case node_type::wide_inner:
            {
               auto in = ref.as<wide_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  result += count_child_keys(in->get_branch(branch_number(i)));
               break;
            }
            case node_type::direct_inner:
            {
               auto in = ref.as<direct_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  result += count_child_keys(in->get_branch(branch_number(i)));
               break;
            }
            case node_type::bplus_inner:
            {
               auto in = ref.as<bplus_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  result += count_child_keys(in->get_branch(branch_number(i)));
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

      void note_removed_keys(uint64_t count = 1) noexcept
      {
         if (count == 0)
            return;
         if (_count_removed_keys)
            _delta_removed_keys -= static_cast<int64_t>(count);
         else
            _delta_removed_keys = -1;
      }

      int64_t removed_child_delta(ptr_address addr)
      {
         if (_count_removed_keys)
            return -static_cast<int64_t>(psitri::count_child_keys(_session, addr));
         return -1;
      }

      void note_removed_child(ptr_address addr)
      {
         if (_count_removed_keys)
            note_removed_keys(psitri::count_child_keys(_session, addr));
         else
            note_removed_keys();
      }

      void release_value_ref(value_type value) noexcept
      {
         if (value.is_value_node())
            _session.release(value.value_address());
         else if (value.is_subtree())
         {
            auto tid = value.subtree_id();
            if (tid.root != sal::null_ptr_address)
               _session.release(tid.root);
            if (tid.ver != sal::null_ptr_address)
               _session.release(tid.ver);
         }
      }

      ptr_address make_inner(const branch_set& branches) noexcept
      {
         std::array<uint8_t, 8> out_cline_idx;
         auto                   needed_clines = find_clines(branches, out_cline_idx);
         return _session.alloc<inner_node>(branches, needed_clines, out_cline_idx, _root_version);
      }
      ptr_address make_inner_prefix(sal::alloc_hint   hint,
                                    key_view          prefix,
                                    const branch_set& branches) noexcept
      {
         std::array<uint8_t, 8> out_cline_idx;
         auto                   needed_clines = find_clines(branches, out_cline_idx);
         return _session.alloc<inner_prefix_node>(hint, prefix, branches, needed_clines,
                                                  out_cline_idx, _root_version);
      }
      template <typename NodeType>
      smart_ref<inner_prefix_node> remake_inner_prefix(const smart_ref<NodeType>& in,
                                                       key_view                   prefix,
                                                       const branch_set&          branches) noexcept
      {
         std::array<uint8_t, 8> out_cline_idx;
         auto                   needed_clines = find_clines(branches, out_cline_idx);
         return _session.realloc<inner_prefix_node>(in, prefix, branches, needed_clines,
                                                    out_cline_idx, _root_version);
      }

      void plan_push_child(op::inner_build_plan& plan,
                           bool&                 first,
                           uint8_t               divider,
                           ptr_address           addr) noexcept
      {
         if (first)
         {
            plan.push_first(addr);
            first = false;
         }
         else
            plan.push_back(divider, addr);
      }

      template <any_inner_node_type InnerNodeType>
      op::inner_build_plan make_replace_plan(const InnerNodeType* in,
                                             branch_number        br,
                                             const branch_set&    sub_branches,
                                             key_view             prefix) noexcept
      {
         op::inner_build_plan plan;
         plan.clear(prefix);
         bool first = true;

         for (uint16_t i = 0; i < *br; ++i)
            plan_push_child(plan, first, i == 0 ? 0 : uint8_t(in->divs()[i - 1]),
                            in->get_branch(branch_number(i)));

         auto sub_addrs = sub_branches.addresses();
         auto sub_divs  = sub_branches.dividers();
         for (uint16_t i = 0; i < sub_addrs.size(); ++i)
         {
            uint8_t div = 0;
            if (*br > 0 && i == 0)
               div = uint8_t(in->divs()[*br - 1]);
            else if (i > 0)
               div = uint8_t(sub_divs[i - 1]);
            plan_push_child(plan, first, div, sub_addrs[i]);
         }

         for (uint16_t i = *br + 1; i < in->num_branches(); ++i)
            plan_push_child(plan, first, uint8_t(in->divs()[i - 1]),
                            in->get_branch(branch_number(i)));

         assert(plan.num_branches == in->num_branches() + sub_branches.count() - 1);
         assert(std::is_sorted(plan.dividers.begin(), plan.dividers.begin() +
                                                     (plan.num_branches - 1)));
         return plan;
      }

      template <any_inner_node_type InnerNodeType>
      op::inner_build_plan make_range_plan(const InnerNodeType* in,
                                           subrange             range,
                                           key_view             prefix = {}) noexcept
      {
         op::inner_build_plan plan;
         plan.clear(prefix);
         bool first = true;
         for (uint16_t i = *range.begin; i < *range.end; ++i)
            plan_push_child(plan, first, i == *range.begin ? 0 : uint8_t(in->divs()[i - 1]),
                            in->get_branch(branch_number(i)));
         assert(plan.num_branches == *range.end - *range.begin);
         assert(std::is_sorted(plan.dividers.begin(), plan.dividers.begin() +
                                                     (plan.num_branches - 1)));
         return plan;
      }

      template <any_inner_node_type InnerNodeType>
      op::inner_build_plan make_merge_pair_plan(const InnerNodeType* in,
                                                branch_number        left,
                                                ptr_address          merged_addr,
                                                key_view             prefix = {}) noexcept
      {
         op::inner_build_plan plan;
         plan.clear(prefix);
         bool first = true;

         const branch_number right(uint32_t(*left) + 1);
         for (uint16_t i = 0; i < *left; ++i)
            plan_push_child(plan, first, i == 0 ? 0 : uint8_t(in->divs()[i - 1]),
                            in->get_branch(branch_number(i)));

         plan_push_child(plan, first, *left == 0 ? 0 : uint8_t(in->divs()[*left - 1]),
                         merged_addr);

         for (uint16_t i = uint16_t(*right) + 1; i < in->num_branches(); ++i)
            plan_push_child(plan, first, uint8_t(in->divs()[i - 1]),
                            in->get_branch(branch_number(i)));

         assert(plan.num_branches + 1 == in->num_branches());
         assert(std::is_sorted(plan.dividers.begin(), plan.dividers.begin() +
                                                     (plan.num_branches - 1)));
         return plan;
      }

      bool use_wide_inner_plan(const op::inner_build_plan& plan) const noexcept
      {
         return plan.compressed_wide_wins();
      }

      ptr_address alloc_adaptive_inner(const sal::alloc_hint&      hint,
                                       const op::inner_build_plan& plan) noexcept
      {
         ptr_address addr;
         if (use_wide_inner_plan(plan))
            addr = _session.alloc<wide_inner_node>(hint, plan);
         else
            addr = _session.alloc<direct_inner_node>(hint, plan);
         auto ref = _session.get_ref<alloc_header>(addr);
         if (node_type(ref->type()) == node_type::wide_inner)
            ref.as<wide_inner_node>().modify()->set_last_unique_version(_root_version);
         else
            ref.as<direct_inner_node>().modify()->set_last_unique_version(_root_version);
         return addr;
      }

      struct bplus_result
      {
         ptr_address  left      = sal::null_ptr_address;
         ptr_address  right     = sal::null_ptr_address;
         std::string  separator;
         bool         split     = false;
         bool         empty     = false;
      };

      op::bplus_build_plan make_bplus_leaf_split_plan(ptr_address left,
                                                      key_view    separator,
                                                      ptr_address right) const noexcept
      {
         op::bplus_build_plan plan;
         plan.push_first(left);
         plan.push_back(separator, right);
         return plan;
      }

      template <typename InnerNodeType>
      op::bplus_build_plan make_bplus_replace_plan(const InnerNodeType* in,
                                                   branch_number        br,
                                                   const bplus_result&  child) const
      {
         op::bplus_build_plan plan;
         plan.clear();

         for (uint16_t i = 0; i < in->num_branches(); ++i)
         {
            if (i == 0)
               plan.push_first(i == *br ? child.left : in->get_branch(branch_number(i)));
            else
            {
               key_view sep = in->separator(i - 1);
               plan.push_back(sep, i == *br ? child.left : in->get_branch(branch_number(i)));
            }

            if (i == *br && child.split)
               plan.push_back(child.separator, child.right);
         }
         return plan;
      }

      template <typename InnerNodeType>
      op::bplus_build_plan make_bplus_remove_plan(const InnerNodeType* in,
                                                  branch_number        br) const
      {
         op::bplus_build_plan plan;
         plan.clear();
         bool first = true;
         for (uint16_t i = 0; i < in->num_branches(); ++i)
         {
            if (i == *br)
               continue;
            if (first)
            {
               plan.push_first(in->get_branch(branch_number(i)));
               first = false;
            }
            else
            {
               uint16_t sep_index = (i > *br) ? i - 1 : i;
               if (sep_index >= in->num_divisions())
                  sep_index = in->num_divisions() - 1;
               plan.push_back(in->separator(sep_index), in->get_branch(branch_number(i)));
            }
         }
         return plan;
      }

      template <upsert_mode mode>
      bplus_result bplus_split_leaf(const sal::alloc_hint& parent_hint,
                                    smart_ref<leaf_node>&  leaf,
                                    key_view               key,
                                    branch_number          lb);
      std::string bplus_first_key(ptr_address addr) const;
      bplus_result bplus_from_branch_set(branch_set result);

      template <upsert_mode mode>
      bplus_result bplus_upsert(const sal::alloc_hint& parent_hint,
                                smart_ref<leaf_node>&  leaf,
                                key_view               key);

      template <upsert_mode mode>
      bplus_result bplus_upsert(const sal::alloc_hint&        parent_hint,
                                smart_ref<bplus_inner_node>& in,
                                key_view                     key);

      template <upsert_mode mode>
      bplus_result bplus_upsert(const sal::alloc_hint&   parent_hint,
                                smart_ref<alloc_header>& r,
                                key_view                 key);

      int bplus_upsert_root(key_view key, value_type value);
      int bplus_remove_root(key_view key);
      void bplus_collect_range_keys(ptr_address              addr,
                                    key_view                 lower,
                                    key_view                 upper,
                                    std::vector<std::string>& keys) const;
      uint64_t bplus_remove_range_counted(key_view lower, key_view upper);
      bool bplus_remove_range_any(key_view lower, key_view upper);

      template <any_inner_node_type FromNodeType>
      ptr_address realloc_adaptive_inner(smart_ref<FromNodeType>&  from,
                                         const op::inner_build_plan& plan) noexcept
      {
         if (use_wide_inner_plan(plan))
         {
            auto result = _session.realloc<wide_inner_node>(from, plan);
            result.modify()->set_last_unique_version(_root_version);
            return result.address();
         }
         auto result = _session.realloc<direct_inner_node>(from, plan);
         result.modify()->set_last_unique_version(_root_version);
         return result.address();
      }

      template <upsert_mode mode, any_inner_node_type InnerNodeType>
      branch_set rebuild_adaptive_inner(const sal::alloc_hint&      parent_hint,
                                        smart_ref<InnerNodeType>&   in,
                                        const op::inner_build_plan& plan) noexcept
      {
         if constexpr (mode.is_unique())
            return realloc_adaptive_inner(in, plan);
         else
            return alloc_adaptive_inner(parent_hint, plan);
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
         return _session.alloc<inner_node>(parent_hint, in, range, freq, _root_version);
      }
      template <typename NodeType, any_inner_node_type InnerNodeType>
      smart_ref<inner_node> remake_inner_node(const smart_ref<NodeType>& in,
                                              const InnerNodeType*       in_obj,
                                              const subrange&            range) noexcept
      {
         const branch* brs  = in->const_branches();
         auto          freq = create_cline_freq_table(brs + *range.begin, brs + *range.end);
         return _session.realloc<inner_node>(in, in_obj, range, freq, _root_version);
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
         if (_family == tree_family::bplus)
            return bplus_upsert_root(key, std::move(value));

         auto op_scope =
             _session.record_operation(sal::mapped_memory::session_operation::tree_upsert);
         _old_value_size     = -1;
         sal::read_lock lock = _session.lock();
         _new_value          = std::move(value);
         if (not _root)
         {
            // First write into an empty tree: allocate a leaf and graft
            // it onto _root via give() so the txn's _ver (set at
            // start_transaction by make_unique_root) travels through
            // unchanged.
            auto leaf_addr = _session.alloc<leaf_node>(sal::alloc_hint(), key,
                                                       make_value(_new_value, sal::alloc_hint()));
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
         if (_family == tree_family::bplus)
            return bplus_remove_root(key);

         auto op_scope =
             _session.record_operation(sal::mapped_memory::session_operation::tree_remove);
         if (not _root)
            return -1;
         _old_value_size         = -1;
         sal::read_lock lock     = _session.lock();
         auto           rref     = *_root;
         auto           old_addr = _root.take();
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
      /// @return true if at least one key was removed.
      bool remove_range_any(key_view lower, key_view upper);

      /// Remove all keys in range [lower, upper).
      /// @return the exact number of keys removed.
      uint64_t remove_range_counted(key_view lower, key_view upper);

      template <upsert_mode mode>
      branch_set range_remove(const sal::alloc_hint&   parent_hint,
                              smart_ref<alloc_header>& ref,
                              key_range                range);

      // ── Explicit-version write internals ────────────────────────
      // Public callers use upsert/remove. These helpers are the storage
      // engine's explicit-version path after the caller has allocated the
      // logical write version.

      /// Insert or update a key at the given logical write version.
      /// For shared trees (ref > 1), modifies only the leaf or value_node
      /// via CB relocation — no inner node cascade, root unchanged.
      /// Falls back to full COW cascade if structural changes are needed (split).
      void upsert_at_version(key_view key, value_type value, uint64_t version);

      /// Append a tombstone for key at the given logical write version.
      void remove_at_version(key_view key, uint64_t version);

      /// Read-only traversal to find the target node for a versioned write.
      /// Returns the node ptr_address that needs to be stripe-locked:
      /// - value_node address if the key has a value_node (Case B)
      /// - leaf address if the key has an inline value (Case A) or is new (Case C)
      /// Returns null_ptr_address only when COW fallback is unavoidable
      /// (empty tree, prefix mismatch).
      ptr_address find_versioned_write_target(key_view key) const;

      /// Try explicit-version upsert under stripe lock. Returns true on success,
      /// false if the operation requires COW fallback (overflow/split).
      /// On false, no side effects — caller should retry under root mutex.
      bool try_upsert_at_version(key_view key, value_type value, uint64_t version);

      /// Try explicit-version remove under stripe lock. Returns true on success,
      /// false if COW fallback is needed.
      bool try_remove_at_version(key_view key, uint64_t version);

      /// Leaf-level helper for explicit-version upsert.
      void upsert_leaf_at_version(smart_ref<leaf_node>& leaf,
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
            case node_type::wide_inner:
               print(r.as<wide_inner_node>(), depth + 1);
               break;
            case node_type::direct_inner:
               print(r.as<direct_inner_node>(), depth + 1);
               break;
            case node_type::bplus_inner:
               print(r.as<bplus_inner_node>(), depth + 1);
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
         uint64_t wide_inner_nodes      = 0;
         uint64_t direct_inner_nodes    = 0;
         uint64_t bplus_inner_nodes     = 0;
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
            return int(total_inner_node_size / total_inner_nodes());
         }
         double average_clines_per_inner_node() const
         {
            return double(clines) / total_inner_nodes();
         }
         double average_branch_per_inner_node() const
         {
            return double(branches) / total_inner_nodes();
         }
         uint64_t total_inner_nodes() const
         {
            return inner_nodes + inner_prefix_nodes + wide_inner_nodes + direct_inner_nodes +
                   bplus_inner_nodes;
         }
         uint64_t total_nodes() const
         {
            return total_inner_nodes() + leaf_nodes + value_nodes;
         }
      };
      stats get_stats()
      {
         sal::read_lock lock = _session.lock();
         stats          s;
         calc_stats(s, *_root);
         return s;
      }

      // private:  // TODO: restore private after fixing friend access
      struct subtree_sizer
      {
         uint16_t count           = 0;
         uint32_t key_data_size   = 0;
         uint32_t value_data_size = 0;
         uint16_t addr_clines     = 0;
         bool     overflow        = false;
         uint32_t max_entries;
         ptr_address clines[leaf_node::max_value_clines];

         subtree_sizer(uint32_t max) : max_entries(max)
         {
            for (auto& cline : clines)
               cline = sal::null_ptr_address;
         }

         void add_value_node(ptr_address value_addr)
         {
            ptr_address base_cline(*value_addr & ~0x0ful);
            for (uint16_t i = 0; i < addr_clines; ++i)
               if (clines[i] == base_cline)
                  return;

            if (addr_clines >= leaf_node::max_value_clines)
            {
               overflow = true;
               return;
            }
            clines[addr_clines++] = base_cline;
         }

         bool exceeds_leaf() const
         {
            uint32_t meta = sizeof(leaf_node) + 5u * count;
            if (meta >= leaf_node::max_leaf_size)
               return true;
            uint32_t data = key_data_size + count * 2u  // sizeof(leaf_node::key) == 2
                            + value_data_size + addr_clines * sizeof(ptr_address);
            return data > leaf_node::max_leaf_size - meta;
         }

         void add_leaf_entry(uint32_t key_size, value_type val)
         {
            if (count >= max_entries)
            {
               overflow = true;
               return;
            }

            key_data_size += key_size;
            if (val.is_view() && !val.view().empty())
               value_data_size += 2u + val.view().size();  // sizeof(value_data) == 2
            else if (val.is_subtree())
               value_data_size += sizeof(tree_id);
            else if (val.is_value_node())
               add_value_node(val.value_address());

            if (overflow)
               return;
            ++count;
            if (exceeds_leaf())
               overflow = true;
         }

         bool fits_in_leaf() const
         {
            if (overflow || count == 0)
               return false;
            return !exceeds_leaf();
         }
      };

      // Recursive subtree sizing is for explicit maintenance-style rewrites.
      // Foreground remove must use size_direct_leaf_child so deletes do not
      // walk arbitrarily deep sibling subtrees.
      void size_subtree(ptr_address addr, uint16_t prefix_len, subtree_sizer& out)
      {
         if (out.overflow)
            return;
         auto ref = _session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::leaf:
            {
               auto     leaf = ref.template as<leaf_node>();
               uint16_t nb   = leaf->num_branches();
               for (uint16_t i = 0; i < nb; ++i)
               {
                  out.add_leaf_entry(prefix_len + leaf->get_key(branch_number(i)).size(),
                                     leaf->get_value(branch_number(i)));
                  if (out.overflow)
                     return;
               }
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
                  size_subtree(ip->get_branch(branch_number(i)), prefix_len + ip->prefix().size(),
                               out);
               break;
            }
            case node_type::wide_inner:
            {
               auto in = ref.template as<wide_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  size_subtree(in->get_branch(branch_number(i)), prefix_len, out);
               break;
            }
            case node_type::direct_inner:
            {
               auto in = ref.template as<direct_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  size_subtree(in->get_branch(branch_number(i)), prefix_len, out);
               break;
            }
            case node_type::bplus_inner:
            {
               auto in = ref.template as<bplus_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  size_subtree(in->get_branch(branch_number(i)), prefix_len, out);
               break;
            }
            default:
               out.overflow = true;
         }
      }

      bool size_direct_leaf_child(ptr_address addr, uint16_t prefix_len, subtree_sizer& out)
      {
         if (out.overflow)
            return true;

         auto ref = _session.get_ref(addr);
         if (node_type(ref->type()) != node_type::leaf)
            return false;

         auto     leaf = ref.template as<leaf_node>();
         uint16_t nb   = leaf->num_branches();
         for (uint16_t i = 0; i < nb; ++i)
         {
            out.add_leaf_entry(prefix_len + leaf->get_key(branch_number(i)).size(),
                               leaf->get_value(branch_number(i)));
            if (out.overflow)
               return true;
         }
         return true;
      }

      bool size_direct_leaf_children(const ptr_address* branches,
                                     uint16_t           branch_count,
                                     uint16_t           prefix_len,
                                     subtree_sizer&     out)
      {
         for (uint16_t i = 0; i < branch_count; ++i)
            if (!size_direct_leaf_child(branches[i], prefix_len, out))
               return false;
         return true;
      }

      // --- Phase 3: Foreground collapse helpers ---
      //
      // Remove is a hot path.  Foreground collapse is intentionally limited to
      // direct leaf children so the cost is bounded by the leaf rewrite budget.
      // Deeper subtree/cousin rewrites belong in maintenance where a recursive
      // scan is explicit and can be scheduled away from transaction latency.

      struct collapse_context
      {
         sal::allocator_session& session;
         const ptr_address*      branches;
         uint16_t                num_branches;
         key_view                root_prefix;
      };

      static void walk_subtree_insert(ptr_address                addr,
                                      char*                      prefix_buf,
                                      uint16_t                   prefix_len,
                                      leaf_node::entry_inserter& ins,
                                      sal::allocator_session&    session)
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
                  walk_subtree_insert(in->get_branch(branch_number(i)), prefix_buf, prefix_len, ins,
                                      session);
               break;
            }
            case node_type::inner_prefix:
            {
               auto ip  = ref.template as<inner_prefix_node>();
               auto pfx = ip->prefix();
               memcpy(prefix_buf + prefix_len, pfx.data(), pfx.size());
               for (uint16_t i = 0; i < ip->num_branches(); ++i)
                  walk_subtree_insert(ip->get_branch(branch_number(i)), prefix_buf,
                                      prefix_len + pfx.size(), ins, session);
               break;
            }
            case node_type::wide_inner:
            {
               auto in = ref.template as<wide_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  walk_subtree_insert(in->get_branch(branch_number(i)), prefix_buf, prefix_len,
                                      ins, session);
               break;
            }
            case node_type::direct_inner:
            {
               auto in = ref.template as<direct_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  walk_subtree_insert(in->get_branch(branch_number(i)), prefix_buf, prefix_len,
                                      ins, session);
               break;
            }
            case node_type::bplus_inner:
            {
               auto in = ref.template as<bplus_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  walk_subtree_insert(in->get_branch(branch_number(i)), prefix_buf, prefix_len,
                                      ins, session);
               break;
            }
            default:
               break;
         }
      }

      static void collapse_visitor(leaf_node::entry_inserter& ins, void* raw)
      {
         auto*    ctx = static_cast<collapse_context*>(raw);
         char     prefix_buf[2048];
         uint16_t pfx_len = 0;
         if (ctx->root_prefix.size() > 0)
         {
            memcpy(prefix_buf, ctx->root_prefix.data(), ctx->root_prefix.size());
            pfx_len = ctx->root_prefix.size();
         }
         for (uint16_t i = 0; i < ctx->num_branches; ++i)
            walk_subtree_insert(ctx->branches[i], prefix_buf, pfx_len, ins, ctx->session);
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
            case node_type::wide_inner:
            {
               auto in = ref.template as<wide_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  retain_subtree_leaf_values_by_addr(in->get_branch(branch_number(i)));
               break;
            }
            case node_type::direct_inner:
            {
               auto in = ref.template as<direct_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  retain_subtree_leaf_values_by_addr(in->get_branch(branch_number(i)));
               break;
            }
            case node_type::bplus_inner:
            {
               auto in = ref.template as<bplus_inner_node>();
               for (uint16_t i = 0; i < in->num_branches(); ++i)
                  retain_subtree_leaf_values_by_addr(in->get_branch(branch_number(i)));
               break;
            }
            default:
               break;
         }
      }

      template <typename NodeRef>
      void retain_subtree_leaf_values(NodeRef& node_ref)
      {
         for (uint16_t i = 0; i < node_ref->num_branches(); ++i)
            retain_subtree_leaf_values_by_addr(node_ref->get_branch(branch_number(i)));
      }

      void retain_direct_leaf_values_by_addr(ptr_address addr)
      {
         auto ref = _session.get_ref(addr);
         assert(node_type(ref->type()) == node_type::leaf);
         auto leaf = ref.template as<leaf_node>();
         retain_children(leaf);
      }

      void retain_direct_leaf_values(const ptr_address* branches, uint16_t branch_count)
      {
         for (uint16_t i = 0; i < branch_count; ++i)
            retain_direct_leaf_values_by_addr(branches[i]);
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

      uint64_t leaf_rewrite_prune_floor(const value_node& vref) const noexcept
      {
         if (vref.is_flat() || vref.num_versions() == 0)
            return 0;
         if (_root_value_version != 0)
            return _root_value_version;
         if (_dead_snap && _dead_snap->oldest_retained_floor() != 0)
            return _dead_snap->oldest_retained_floor();
         return vref.get_entry_version(0);
      }

      bool value_node_needs_prune_floor(const value_node& vref, uint64_t floor) const noexcept
      {
         if (vref.is_flat() || vref.num_next() != 0 || vref.num_versions() == 0)
            return false;
         const uint64_t base           = vref.get_entry_version(0);
         const uint64_t floor_token    = version_token(floor, value_version_bits);
         const uint64_t floor_distance = version_distance(base, floor_token, value_version_bits);
         const uint64_t half_range     = version_mask(value_version_bits) >> 1;
         if (floor_distance > half_range)
            return false;

         uint8_t floor_idx = 0xFF;
         for (uint8_t i = 0; i < vref.num_versions(); ++i)
         {
            uint64_t entry_distance =
                version_distance(base, vref.get_entry_version(i), value_version_bits);
            if (entry_distance <= floor_distance)
               floor_idx = i;
            else
               break;
         }

         if (floor_idx == 0xFF)
            return false;
         return floor_idx != 0 || vref.get_entry_version(floor_idx) != floor_token;
      }

      static constexpr uint16_t max_leaf_rewrite_entries = leaf_node::max_leaf_branches;
      static_assert(max_leaf_rewrite_entries <= leaf_node::max_leaf_branches);
      struct leaf_rewrite_entry
      {
         branch_number bn;
         ptr_address   old_addr;
         value_type    replacement;
      };
      struct leaf_rewrite_plan
      {
         std::array<leaf_rewrite_entry, max_leaf_rewrite_entries> entries;
         uint16_t                                                 count = 0;
      };

      leaf_rewrite_plan make_leaf_rewrite_plan(const leaf_node& src,
                                               branch_number    begin,
                                               branch_number    end,
                                               branch_number    skip_begin,
                                               branch_number    skip_end)
      {
         leaf_rewrite_plan plan;
         for (uint16_t i = *begin; i < *end; ++i)
         {
            if (i >= *skip_begin && i < *skip_end)
               continue;

            branch_number bn(i);
            auto          val = src.get_value(bn);
            if (!val.is_value_node())
               continue;

            auto old_addr = val.value_address();
            auto vref     = _session.get_ref<value_node>(old_addr);
            // This plan allocates a distinct replacement value_node, so copied
            // subtree refs would need matching retains. Keep reference-owning
            // histories on same-CB rewrite paths for now.
            if (vref->is_subtree_container() || vref->is_flat() ||
                vref->num_next() != 0 || vref->num_versions() == 0)
               continue;

            uint64_t floor = leaf_rewrite_prune_floor(*vref);
            if (!value_node_needs_prune_floor(*vref, floor))
               continue;

            assert(plan.count < plan.entries.size());
            value_node::prune_floor_policy prune{floor};
            ptr_address new_addr = _session.alloc<value_node>(src.clines(), vref.obj(), prune);
            plan.entries[plan.count++] =
                leaf_rewrite_entry{bn, old_addr, value_type::make_value_node(new_addr)};
         }
         return plan;
      }

      leaf_rewrite_plan make_leaf_rewrite_plan(const leaf_node& src)
      {
         return make_leaf_rewrite_plan(src,
                                       branch_number(0),
                                       branch_number(src.num_branches()),
                                       branch_number(src.num_branches()),
                                       branch_number(src.num_branches()));
      }

      leaf_rewrite_plan make_leaf_rewrite_plan_skipping(const leaf_node& src,
                                                        branch_number    skip_begin,
                                                        branch_number    skip_end)
      {
         return make_leaf_rewrite_plan(src,
                                       branch_number(0),
                                       branch_number(src.num_branches()),
                                       skip_begin,
                                       skip_end);
      }

      void release_leaf_rewrite_sources(const leaf_rewrite_plan& plan)
      {
         for (uint16_t i = 0; i < plan.count; ++i)
            _session.release(plan.entries[i].old_addr);
      }

      void release_leaf_rewrite_replacements(const leaf_rewrite_plan& plan)
      {
         for (uint16_t i = 0; i < plan.count; ++i)
            _session.release(plan.entries[i].replacement.value_address());
      }

      static value_type rewrite_leaf_source_value(void*            raw,
                                                  const leaf_node& src,
                                                  branch_number    bn)
      {
         auto* plan = static_cast<leaf_rewrite_plan*>(raw);
         for (uint16_t i = 0; i < plan->count; ++i)
            if (plan->entries[i].bn == bn)
               return plan->entries[i].replacement;
         return src.get_value(bn);
      }

      op::leaf_value_rewrite leaf_rewrite_policy(leaf_rewrite_plan& plan) noexcept
      {
         return op::leaf_value_rewrite{&tree_context::rewrite_leaf_source_value, &plan};
      }

      /// Append/replace the latest MVCC data entry on an existing value_node.
      /// Returns false when the caller must fall back to structural COW.
      bool append_versioned_value(smart_ref<value_node>& vref,
                                     uint64_t               version,
                                     value_view             new_val);

      /// Append/replace the latest MVCC tombstone entry on an existing value_node.
      /// Returns false when the caller must fall back to structural COW.
      bool append_versioned_tombstone(smart_ref<value_node>& vref, uint64_t version);

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

      branch_set merge_branches_unique_at(const sal::alloc_hint& parent_hint,
                                          ptr_address            addr,
                                          branch_number          br,
                                          const branch_set&      sub_branches);

      template <upsert_mode mode, any_inner_node_type InnerNodeType>
      branch_set merge_branches(const sal::alloc_hint&    parent_hint,
                                smart_ref<InnerNodeType>& in,
                                branch_number             br,
                                const branch_set&         sub_branches);

      template <upsert_mode mode, any_inner_node_type InnerNodeType>
      ptr_address try_merge_adjacent_siblings_after_remove(const sal::alloc_hint&    parent_hint,
                                                           smart_ref<InnerNodeType>& in,
                                                           branch_number             br,
                                                           ptr_address child_addr);

      template <upsert_mode mode, any_inner_node_type InnerNodeType>
      ptr_address try_collapse_current_node_to_leaf(const sal::alloc_hint&    parent_hint,
                                                    smart_ref<InnerNodeType>& in);

      template <any_inner_node_type InnerNodeType>
      ptr_address try_collapse_shared_branch_list_to_leaf(const sal::alloc_hint&    parent_hint,
                                                          smart_ref<InnerNodeType>& in,
                                                          const ptr_address*        branches,
                                                          uint16_t                  branch_count);

      template <upsert_mode mode, any_inner_node_type InnerNodeType>
      ptr_address try_collapse_single_child_node(const sal::alloc_hint&    parent_hint,
                                                 smart_ref<InnerNodeType>& in,
                                                 ptr_address               child_addr);

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
      template <any_inner_node_type NodeType>
         requires(is_wide_inner_node<NodeType> || is_direct_inner_node<NodeType>)
      void print(smart_ref<NodeType> r, int depth = 0)
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
      void print(smart_ref<bplus_inner_node> r, int depth = 0)
      {
         assert(_session.get_ref(r.address()).obj() == r.obj());
         std::string indent(4 * depth, ' ');
         std::cout << indent << "#" << r.address() << "  " << r->type() << " r:" << r.ref()
                   << " branches: " << r->num_branches() << " this: " << r.obj() << "\n";
         for (int i = 0; i < r->num_branches(); ++i)
         {
            auto br  = r->get_branch(branch_number(i));
            auto ref = _session.get_ref(br);
            std::cout << br << "->";
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
            case node_type::wide_inner:
               return validate_inner(r.as<wide_inner_node>(), depth + 1);
            case node_type::direct_inner:
               return validate_inner(r.as<direct_inner_node>(), depth + 1);
            case node_type::bplus_inner:
               return validate_inner(r.as<bplus_inner_node>(), depth + 1);
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
      void validate(smart_ref<alloc_header> r, int depth = 0) { validate_subtree(r, depth); }

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
            case node_type::wide_inner:
               validate_unique_refs_inner(r.as<wide_inner_node>());
               break;
            case node_type::direct_inner:
               validate_unique_refs_inner(r.as<direct_inner_node>());
               break;
            case node_type::bplus_inner:
               validate_unique_refs_inner(r.as<bplus_inner_node>());
               break;
            case node_type::leaf:
            {
               auto leaf = r.as<leaf_node>();
               for (int i = 0; i < leaf->num_branches(); ++i)
               {
                  if (leaf->get_value_type(branch_number(i)) ==
                      leaf_node::value_type_flag::value_node)
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
            case node_type::wide_inner:
               validate_unique_refs_inner(r.as<wide_inner_node>());
               break;
            case node_type::direct_inner:
               validate_unique_refs_inner(r.as<direct_inner_node>());
               break;
            case node_type::bplus_inner:
               validate_unique_refs_inner(r.as<bplus_inner_node>());
               break;
            case node_type::leaf:
            {
               auto leaf = r.as<leaf_node>();
               for (int i = 0; i < leaf->num_branches(); ++i)
               {
                  if (leaf->get_value_type(branch_number(i)) ==
                      leaf_node::value_type_flag::value_node)
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
            SAL_ERROR(
                "calc_stats: inner node {} failed validate_invariants at depth {},"
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
            case node_type::wide_inner:
               s.wide_inner_nodes++;
               calc_stats(s, r.as<wide_inner_node>(), depth + 1);
               break;
            case node_type::direct_inner:
               s.direct_inner_nodes++;
               calc_stats(s, r.as<direct_inner_node>(), depth + 1);
               break;
            case node_type::bplus_inner:
               s.bplus_inner_nodes++;
               calc_stats(s, r.as<bplus_inner_node>(), depth + 1);
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
                     auto vref =
                         _session.get_ref<value_node>(leaf->get_value_address(branch_number(i)));
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
            case node_type::wide_inner:
               [[likely]] result = upsert<mode>(parent_hint, r.as<wide_inner_node>(), key);
               break;
            case node_type::direct_inner:
               [[likely]] result = upsert<mode>(parent_hint, r.as<direct_inner_node>(), key);
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
      else if constexpr (is_wide_inner_node<InnerNodeType> ||
                         is_direct_inner_node<InnerNodeType>)
      {
         auto left_plan  = make_range_plan(in.obj(), subrange(branch_number(0), nb2));
         auto right_plan = make_range_plan(in.obj(), subrange(nb2, nb));
         ptr_address left;
         if constexpr (mode.is_unique())
            left = realloc_adaptive_inner(in, left_plan);
         else
            left = alloc_adaptive_inner(parent_hint, left_plan);
         auto right = alloc_adaptive_inner(sal::alloc_hint{&left, 1}, right_plan);
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
   inline branch_set tree_context::merge_branches_unique_at(const sal::alloc_hint& parent_hint,
                                                            ptr_address            addr,
                                                            branch_number          br,
                                                            const branch_set&      sub_branches)
   {
      auto ref = _session.get_ref(addr);
      switch (node_type(ref->type()))
      {
         case node_type::inner:
            return merge_branches<upsert_mode::unique>(parent_hint, ref.as<inner_node>(), br,
                                                       sub_branches);
         case node_type::inner_prefix:
            return merge_branches<upsert_mode::unique>(
                parent_hint, ref.as<inner_prefix_node>(), br, sub_branches);
         case node_type::wide_inner:
            return merge_branches<upsert_mode::unique>(
                parent_hint, ref.as<wide_inner_node>(), br, sub_branches);
         case node_type::direct_inner:
            return merge_branches<upsert_mode::unique>(
                parent_hint, ref.as<direct_inner_node>(), br, sub_branches);
         default:
            std::unreachable();
      }
   }

   template <upsert_mode mode, any_inner_node_type InnerNodeType>
   branch_set tree_context::split_merge(const sal::alloc_hint&    parent_hint,
                                        smart_ref<InnerNodeType>& in,
                                        branch_number             br,
                                        const branch_set&         sub_branches)
   {
      auto nb = in->num_branches();
      auto split_divider = in->divs()[nb / 2 - 1];

      auto [left, right] = split<mode>(parent_hint, in, br, sub_branches);

      if (br < nb / 2)
      {
         auto left_result = merge_branches_unique_at(parent_hint, left, br, sub_branches);
         left_result.push_back(split_divider, right);
         return left_result;
      }
      else
      {
         branch_set right_result = merge_branches_unique_at(
             parent_hint, right, branch_number(*br - nb / 2), sub_branches);
         right_result.push_front(left, split_divider);
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

      if constexpr (is_wide_inner_node<InnerNodeType> || is_direct_inner_node<InnerNodeType>)
      {
         auto plan = make_replace_plan(in.obj(), br, sub_branches, key_view());
         if (plan.num_branches <= op::inner_build_plan::max_branches) [[likely]]
            return rebuild_adaptive_inner<mode>(parent_hint, in, plan);
         return split_merge<mode>(parent_hint, in, br, sub_branches);
      }

      else
      {
      std::array<uint8_t, 8> cline_indices;
      auto needed_clines = psitri::find_clines(in->get_branch_clines(), in->get_branch(br),
                                               sub_branches.addresses(), cline_indices);
      assert(needed_clines >= in->get_branch_clines().size());

      /// this is the unlikely path because it requires a split which only happens once
      /// a node gets to 16 cachelines and thus most updates don't require a split.
      if (needed_clines == insufficient_clines) [[unlikely]]
      {
         if constexpr (PSITRI_ENABLE_ADAPTIVE_INNER_NODES && is_inner_node<InnerNodeType>)
         {
            auto plan = make_replace_plan(in.obj(), br, sub_branches, key_view());
            if (plan.num_branches <= op::inner_build_plan::max_branches)
               return rebuild_adaptive_inner<mode>(parent_hint, in, plan);
            return split_merge<mode>(parent_hint, in, br, sub_branches);
         }
         else if constexpr (is_inner_node<InnerNodeType>)
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
            auto guard = in.modify();
            guard->apply(update);
            guard->set_last_unique_version(_root_version);
            result_addr = in.address();
         }
         else
         {
            if constexpr (!is_inner_prefix_node<InnerNodeType>)
            {
               auto result = _session.realloc<InnerNodeType>(in, in.obj(), update);
               result.modify()->set_last_unique_version(_root_version);
               result_addr = result.address();
            }
            else if constexpr (is_inner_prefix_node<InnerNodeType>)
            {
               auto result = _session.realloc<InnerNodeType>(in, in->prefix(), in.obj(), update);
               result.modify()->set_last_unique_version(_root_version);
               result_addr = result.address();
            }
         }

         return result_addr;
      }
      else if constexpr (mode.is_shared())
      {
         if constexpr (is_inner_node<InnerNodeType>)
         {
            auto result = _session.alloc<InnerNodeType>(
                parent_hint, in.obj(),
                op::replace_branch{br, sub_branches, needed_clines, cline_indices});
            auto ref = _session.get_ref<InnerNodeType>(result);
            ref.modify()->set_last_unique_version(_root_version);
            return result;
         }
         else if constexpr (is_inner_prefix_node<InnerNodeType>)
         {
            auto result = _session.alloc<InnerNodeType>(
                parent_hint, in->prefix(), in.obj(),
                op::replace_branch{br, sub_branches, needed_clines, cline_indices});
            auto ref = _session.get_ref<InnerNodeType>(result);
            ref.modify()->set_last_unique_version(_root_version);
            return result;
         }
      }
      }
   }

   template <upsert_mode mode, any_inner_node_type InnerNodeType>
   ptr_address tree_context::try_collapse_current_node_to_leaf(const sal::alloc_hint& parent_hint,
                                                               smart_ref<InnerNodeType>& in)
   {
      static_assert(mode.is_remove());
      if (!_collapse_enabled || in->num_branches() <= 1)
         return sal::null_ptr_address;

      const uint16_t               nb = in->num_branches();
      std::array<ptr_address, 256> branches;
      uint16_t                     pfx_len = 0;
      if constexpr (is_inner_prefix_node<InnerNodeType>)
         pfx_len = in->prefix().size();

      subtree_sizer sizer(max_leaf_rewrite_entries);
      for (uint16_t i = 0; i < nb; ++i)
      {
         branches[i] = in->get_branch(branch_number(i));
         if (!size_direct_leaf_child(branches[i], pfx_len, sizer))
            return sal::null_ptr_address;
      }

      if (!sizer.fits_in_leaf())
         return sal::null_ptr_address;

      char     prefix_save[2048];
      key_view root_prefix;
      if constexpr (is_inner_prefix_node<InnerNodeType>)
      {
         auto pfx = in->prefix();
         assert(pfx.size() <= sizeof(prefix_save));
         memcpy(prefix_save, pfx.data(), pfx.size());
         root_prefix = key_view(prefix_save, pfx.size());
      }

      retain_direct_leaf_values(branches.data(), nb);
      collapse_context cctx{_session, branches.data(), nb, root_prefix};

      if constexpr (mode.is_unique())
      {
         auto result = _session.realloc<leaf_node>(
             in, op::leaf_from_visitor{&collapse_visitor, &cctx, sizer.count});
         for (uint16_t i = 0; i < nb; ++i)
            _session.release(branches[i]);
         return result.address();
      }
      else if constexpr (mode.is_shared())
      {
         auto result = _session.alloc<leaf_node>(
             parent_hint, op::leaf_from_visitor{&collapse_visitor, &cctx, sizer.count});
         for (uint16_t i = 0; i < nb; ++i)
            _session.release(branches[i]);
         return result;
      }
   }

   template <any_inner_node_type InnerNodeType>
   ptr_address tree_context::try_collapse_shared_branch_list_to_leaf(
       const sal::alloc_hint&    parent_hint,
       smart_ref<InnerNodeType>& in,
       const ptr_address*        branches,
       uint16_t                  branch_count)
   {
      if (!_collapse_enabled || branch_count <= 1)
         return sal::null_ptr_address;

      uint16_t pfx_len = 0;
      if constexpr (is_inner_prefix_node<InnerNodeType>)
         pfx_len = in->prefix().size();

      subtree_sizer sizer(max_leaf_rewrite_entries);
      if (!size_direct_leaf_children(branches, branch_count, pfx_len, sizer))
         return sal::null_ptr_address;

      if (!sizer.fits_in_leaf())
         return sal::null_ptr_address;

      retain_direct_leaf_values(branches, branch_count);

      key_view root_prefix;
      if constexpr (is_inner_prefix_node<InnerNodeType>)
         root_prefix = in->prefix();  // safe: in remains alive while allocating the leaf

      collapse_context cctx{_session, branches, branch_count, root_prefix};
      auto             result = _session.alloc<leaf_node>(
          parent_hint, op::leaf_from_visitor{&collapse_visitor, &cctx, sizer.count});

      for (uint16_t i = 0; i < branch_count; ++i)
         _session.release(branches[i]);

      return result;
   }

   template <upsert_mode mode, any_inner_node_type InnerNodeType>
   ptr_address tree_context::try_collapse_single_child_node(const sal::alloc_hint&    parent_hint,
                                                            smart_ref<InnerNodeType>& in,
                                                            ptr_address               child_addr)
   {
      auto child_ref = _session.get_ref(child_addr);
      switch (node_type(child_ref->type()))
      {
         case node_type::leaf:
         {
            auto leaf_ref = child_ref.template as<leaf_node>();
            if constexpr (!is_inner_prefix_node<InnerNodeType>)
            {
               if constexpr (mode.is_unique())
               {
                  retain_children(leaf_ref);
                  auto        rewrite_plan   = make_leaf_rewrite_plan(*leaf_ref.obj());
                  auto        rewrite_policy = leaf_rewrite_policy(rewrite_plan);
                  ptr_address result_addr;
                  if (leaf_ref->rebuilt_size_fits(key_view(), branch_zero,
                                                  branch_number(leaf_ref->num_branches()),
                                                  &rewrite_policy))
                  {
                     auto result = _session.realloc<leaf_node>(in, leaf_ref.obj(), &rewrite_policy);
                     result_addr = result.address();
                     release_leaf_rewrite_sources(rewrite_plan);
                  }
                  else
                  {
                     release_leaf_rewrite_replacements(rewrite_plan);
                     auto result = _session.realloc<leaf_node>(
                         in, leaf_ref.obj(), static_cast<const op::leaf_value_rewrite*>(nullptr));
                     result_addr = result.address();
                  }
                  _session.release(child_addr);
                  return result_addr;
               }
               else
               {
                  // retain_children(in) transferred this child ref to the caller.
                  return child_addr;
               }
            }
            else
            {
               char prefix_buf[2048];
               auto pfx = in->prefix();
               assert(pfx.size() <= sizeof(prefix_buf));
               memcpy(prefix_buf, pfx.data(), pfx.size());

               auto                    rewrite_plan   = make_leaf_rewrite_plan(*leaf_ref.obj());
               auto                    rewrite_policy = leaf_rewrite_policy(rewrite_plan);
               op::leaf_prepend_prefix pp{*leaf_ref.obj(), key_view(prefix_buf, pfx.size()),
                                          &rewrite_policy};
               if (!leaf_ref->rebuilt_size_fits(pp))
               {
                  release_leaf_rewrite_replacements(rewrite_plan);
                  pp.rewrite = nullptr;
               }
               if (!leaf_ref->rebuilt_size_fits(pp))
                  return sal::null_ptr_address;

               retain_children(leaf_ref);
               ptr_address result_addr;
               if constexpr (mode.is_unique())
               {
                  auto result = _session.realloc<leaf_node>(in, pp);
                  result_addr = result.address();
               }
               else
               {
                  result_addr = _session.alloc<leaf_node>(parent_hint, pp);
               }
               if (pp.rewrite)
                  release_leaf_rewrite_sources(rewrite_plan);
               _session.release(child_addr);
               return result_addr;
            }
         }
         case node_type::inner:
         {
            auto inner_ref = child_ref.template as<inner_node>();
            retain_children(inner_ref);
            const auto*   child_ptr = inner_ref.obj();
            const branch* brs       = child_ptr->const_branches();
            auto          freq      = create_cline_freq_table(brs, brs + child_ptr->num_branches());
            auto range = subrange(branch_number(0), branch_number(child_ptr->num_branches()));

            ptr_address result_addr;
            if constexpr (!is_inner_prefix_node<InnerNodeType>)
            {
               if constexpr (mode.is_unique())
               {
                  auto result =
                      _session.realloc<inner_node>(in, child_ptr, range, freq, _root_version);
                  result_addr = result.address();
               }
               else
               {
                  result_addr = _session.alloc<inner_node>(parent_hint, child_ptr, range, freq,
                                                           _root_version);
               }
            }
            else
            {
               char prefix_buf[2048];
               auto pfx = in->prefix();
               assert(pfx.size() <= sizeof(prefix_buf));
               memcpy(prefix_buf, pfx.data(), pfx.size());
               if constexpr (mode.is_unique())
               {
                  auto result = _session.realloc<inner_prefix_node>(
                      in, child_ptr, key_view(prefix_buf, pfx.size()), range, freq, _root_version);
                  result_addr = result.address();
               }
               else
               {
                  result_addr = _session.alloc<inner_prefix_node>(parent_hint, child_ptr,
                                                                  key_view(prefix_buf, pfx.size()),
                                                                  range, freq, _root_version);
               }
            }
            _session.release(child_addr);
            return result_addr;
         }
         case node_type::inner_prefix:
         {
            auto ipn_ref = child_ref.template as<inner_prefix_node>();
            retain_children(ipn_ref);
            ptr_address result_addr;
            if constexpr (is_inner_node<InnerNodeType>)
            {
               if constexpr (mode.is_unique())
               {
                  auto result =
                      _session.realloc<inner_prefix_node>(in, ipn_ref.obj(), ipn_ref->prefix());
                  result.modify()->set_last_unique_version(_root_version);
                  result_addr = result.address();
               }
               else
               {
                  result_addr     = _session.alloc<inner_prefix_node>(parent_hint, ipn_ref.obj(),
                                                                      ipn_ref->prefix());
                  auto result_ref = _session.get_ref<inner_prefix_node>(result_addr);
                  result_ref.modify()->set_last_unique_version(_root_version);
               }
            }
            else
            {
               char prefix_buf[2048];
               auto p = in->prefix();
               auto q = ipn_ref->prefix();
               assert(p.size() + q.size() <= sizeof(prefix_buf));
               memcpy(prefix_buf, p.data(), p.size());
               memcpy(prefix_buf + p.size(), q.data(), q.size());
               auto merged_prefix = key_view(prefix_buf, p.size() + q.size());
               if constexpr (mode.is_unique())
               {
                  auto result =
                      _session.realloc<inner_prefix_node>(in, ipn_ref.obj(), merged_prefix);
                  result.modify()->set_last_unique_version(_root_version);
                  result_addr = result.address();
               }
               else
               {
                  result_addr =
                      _session.alloc<inner_prefix_node>(parent_hint, ipn_ref.obj(), merged_prefix);
                  auto ref = _session.get_ref<inner_prefix_node>(result_addr);
                  ref.modify()->set_last_unique_version(_root_version);
               }
            }
            _session.release(child_addr);
            return result_addr;
         }
         default:
            std::unreachable();
      }
   }

   template <upsert_mode mode, any_inner_node_type InnerNodeType>
   ptr_address tree_context::try_merge_adjacent_siblings_after_remove(
       const sal::alloc_hint&    parent_hint,
       smart_ref<InnerNodeType>& in,
       branch_number             br,
       ptr_address               child_addr)
   {
      static_assert(mode.is_remove());
      if (!_collapse_enabled || in->num_branches() < 2)
         return sal::null_ptr_address;

      if constexpr (is_wide_inner_node<InnerNodeType> || is_direct_inner_node<InnerNodeType>)
      {
         auto try_pair = [&](branch_number left, bool current_is_left) -> ptr_address
         {
            branch_number right(uint32_t(*left) + 1);
            ptr_address   old_left_addr  = in->get_branch(left);
            ptr_address   old_right_addr = in->get_branch(right);
            ptr_address   left_addr      = current_is_left ? child_addr : old_left_addr;
            ptr_address   right_addr     = current_is_left ? old_right_addr : child_addr;

            subtree_sizer sizer(max_leaf_rewrite_entries);
            if (!size_direct_leaf_child(left_addr, 0, sizer) ||
                !size_direct_leaf_child(right_addr, 0, sizer))
               return sal::null_ptr_address;
            if (!sizer.fits_in_leaf())
               return sal::null_ptr_address;

            retain_direct_leaf_values_by_addr(left_addr);
            retain_direct_leaf_values_by_addr(right_addr);

            ptr_address      branches[2] = {left_addr, right_addr};
            collapse_context cctx{_session, branches, 2, key_view()};
            ptr_address      merged_leaf = _session.alloc<leaf_node>(
                in->get_branch_clines(), op::leaf_from_visitor{&collapse_visitor, &cctx, sizer.count});

            auto plan = make_merge_pair_plan(in.obj(), left, merged_leaf, key_view());
            ptr_address result_addr;
            if (plan.num_branches == 1)
               result_addr = try_collapse_single_child_node<mode>(parent_hint, in, merged_leaf);
            else
               result_addr = rebuild_adaptive_inner<mode>(parent_hint, in, plan).front();

            _session.release(left_addr);
            _session.release(right_addr);
            return result_addr;
         };

         if (*br > 0)
         {
            if (auto merged = try_pair(branch_number(uint32_t(*br) - 1), false);
                merged != sal::null_ptr_address)
               return merged;
         }

         if (uint32_t(*br) + 1 < in->num_branches())
         {
            if (auto merged = try_pair(br, true); merged != sal::null_ptr_address)
               return merged;
         }

         return sal::null_ptr_address;
      }
      else
      {
      auto try_pair = [&](branch_number left, bool current_is_left) -> ptr_address
      {
         branch_number right(uint32_t(*left) + 1);
         ptr_address   old_left_addr  = in->get_branch(left);
         ptr_address   old_right_addr = in->get_branch(right);
         ptr_address   left_addr      = current_is_left ? child_addr : old_left_addr;
         ptr_address   right_addr     = current_is_left ? old_right_addr : child_addr;

         subtree_sizer sizer(max_leaf_rewrite_entries);
         if (!size_direct_leaf_child(left_addr, 0, sizer) ||
             !size_direct_leaf_child(right_addr, 0, sizer))
            return sal::null_ptr_address;
         if (!sizer.fits_in_leaf())
            return sal::null_ptr_address;

         retain_direct_leaf_values_by_addr(left_addr);
         retain_direct_leaf_values_by_addr(right_addr);

         ptr_address      branches[2] = {left_addr, right_addr};
         collapse_context cctx{_session, branches, 2, key_view()};
         ptr_address      merged_leaf = _session.alloc<leaf_node>(
             in->get_branch_clines(), op::leaf_from_visitor{&collapse_visitor, &cctx, sizer.count});

         branch_set             merged_branch(merged_leaf);
         std::array<uint8_t, 8> cline_indices;
         auto needed_clines = psitri::find_clines(in->get_branch_clines(), old_left_addr,
                                                  merged_branch.addresses(), cline_indices);

         if (needed_clines == insufficient_clines)
         {
            _session.release(merged_leaf);
            return sal::null_ptr_address;
         }

         op::replace_branch update = {left, merged_branch, needed_clines, cline_indices};

         if constexpr (mode.is_unique())
         {
            if (!in->can_apply(update))
            {
               _session.release(merged_leaf);
               return sal::null_ptr_address;
            }

            {
               auto guard = in.modify();
               guard->apply(update);
               guard->remove_branch(right);
               guard->set_last_unique_version(_root_version);
            }

            _session.release(left_addr);
            _session.release(right_addr);
            if (in->num_branches() == 1)
            {
               auto only_child = in->get_branch(branch_number(0));
               if (auto collapsed =
                       try_collapse_single_child_node<mode>(parent_hint, in, only_child);
                   collapsed != sal::null_ptr_address)
                  return collapsed;
            }
            return in.address();
         }
         else if constexpr (mode.is_shared())
         {
            ptr_address result_addr;
            if constexpr (is_inner_node<InnerNodeType>)
               result_addr = _session.alloc<InnerNodeType>(parent_hint, in.obj(), update);
            else if constexpr (is_inner_prefix_node<InnerNodeType>)
               result_addr =
                   _session.alloc<InnerNodeType>(parent_hint, in->prefix(), in.obj(), update);

            auto result_ref = _session.get_ref<InnerNodeType>(result_addr);
            {
               auto guard = result_ref.modify();
               guard->remove_branch(right);
               guard->set_last_unique_version(_root_version);
            }

            _session.release(left_addr);
            _session.release(right_addr);
            return result_addr;
         }
      };

      if (*br > 0)
      {
         if (auto merged = try_pair(branch_number(uint32_t(*br) - 1), false);
             merged != sal::null_ptr_address)
            return merged;
      }

      if (uint32_t(*br) + 1 < in->num_branches())
      {
         if (auto merged = try_pair(br, true); merged != sal::null_ptr_address)
            return merged;
      }

      return sal::null_ptr_address;
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
            auto new_ipn_ref = _session.get_ref<inner_prefix_node>(new_ipn_addr);
            new_ipn_ref.modify()->set_last_unique_version(_root_version);

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

	      const bool refresh_this_node = needs_structural_refresh(in->last_unique_version());
	      if constexpr (mode.is_unique())
	      {
	         if (refresh_this_node)
	            in.modify()->set_last_unique_version(_root_version);
	      }

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

      auto bref = _session.get_ref(badr);

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
               in.modify([&](auto* n) { n->remove_branch(br); });

	               // Phase 2: Collapse single-branch inner node
	               if (in->num_branches() == 1)
	               {
	                  auto child_addr = in->get_branch(branch_number(0));
	                  if (auto collapsed =
	                          try_collapse_single_child_node<mode>(parent_hint, in, child_addr);
	                      collapsed != sal::null_ptr_address)
	                     return collapsed;
	               }

               // Phase 3: collapse any byte-small subtree into one leaf. The
               // scratch entry cap only bounds CPU; fit is decided by the leaf
               // byte/cline budget.
               if (auto collapsed = try_collapse_current_node_to_leaf<mode>(parent_hint, in);
                   collapsed != sal::null_ptr_address)
                  return collapsed;
               return in.address();
            }
            else if constexpr (mode.is_shared())
            {
               // Phase 2: Collapse single-branch result in shared mode
               if (in->num_branches() == 2) [[unlikely]]
               {
	                  branch_number remaining_br   = (*br == 0) ? branch_number(1) : branch_number(0);
	                  auto          remaining_addr = in->get_branch(remaining_br);
	                  if (auto collapsed = try_collapse_single_child_node<mode>(
	                          parent_hint, in, remaining_addr);
	                      collapsed != sal::null_ptr_address)
	                     return collapsed;
	               }

               // Phase 3: collapse remaining branches in shared mode when their
               // complete rewritten content fits in one leaf.
               if (in->num_branches() > 2)
               {
                  ptr_address remaining[256];
                  uint16_t    rb_count = 0;
                  for (uint16_t i = 0; i < in->num_branches(); ++i)
                  {
                     if (branch_number(i) == br)
                        continue;
                     remaining[rb_count++] = in->get_branch(branch_number(i));
                  }
                  if (auto collapsed =
                          try_collapse_shared_branch_list_to_leaf(parent_hint, in, remaining, rb_count);
                      collapsed != sal::null_ptr_address)
                     return collapsed;
               }

               // retain_children gave +1 to all remaining children; the removed
               // child's extra retain was already released above.
	               op::inner_remove_branch rm{br};
	               if constexpr (is_inner_node<InnerNodeType>)
	               {
	                  auto result = _session.alloc<InnerNodeType>(parent_hint, in.obj(), rm);
	                  auto ref = _session.get_ref<InnerNodeType>(result);
	                  ref.modify()->set_last_unique_version(_root_version);
	                  return result;
	               }
		               else if constexpr (is_inner_prefix_node<InnerNodeType>)
		               {
		                  auto result = _session.alloc<InnerNodeType>(parent_hint, in->prefix(), in.obj(), rm);
		                  auto ref = _session.get_ref<InnerNodeType>(result);
		                  ref.modify()->set_last_unique_version(_root_version);
		                  return result;
		               }
		               else if constexpr (is_wide_inner_node<InnerNodeType> ||
		                                  is_direct_inner_node<InnerNodeType>)
		               {
		                  auto result = _session.alloc<InnerNodeType>(parent_hint, in.obj(), rm);
		                  auto ref = _session.get_ref<InnerNodeType>(result);
		                  ref.modify()->set_last_unique_version(_root_version);
		                  return result;
		               }
	            }
            std::unreachable();
         }
      }

      // Undo pre-retain: child didn't become empty, so the dispatcher
      // didn't release badr; drop the extra reference we added.
      if (pre_retained_last_branch)
         _session.release(badr);

	      if constexpr (mode.is_remove())
	      {
	         if (_old_value_size >= 0 && sub_branches.count() == 1)
	         {
	            ptr_address child_addr = sub_branches.get_first_branch();
	            if (auto merged = try_merge_adjacent_siblings_after_remove<mode>(
	                    parent_hint, in, br, child_addr);
	                merged != sal::null_ptr_address)
	               return merged;
	            if (child_addr == badr)
	            {
	               if (auto collapsed =
	                       try_collapse_current_node_to_leaf<mode>(parent_hint, in);
	                   collapsed != sal::null_ptr_address)
	                  return collapsed;
	            }
	         }
	      }

      // the happy path where there is nothing to do: the child returned itself
      // unchanged (e.g. key not found during remove/update, or key already exists
      // during insert).
	      if (sub_branches.count() == 1 && bref->address() == sub_branches.get_first_branch())
	      {
	         if constexpr (mode.is_unique() && mode.is_remove())
	         {
	            if (in->num_branches() == 1)
	            {
	               auto only_child = in->get_branch(branch_number(0));
	               if (auto collapsed =
	                       try_collapse_single_child_node<mode>(parent_hint, in, only_child);
	                   collapsed != sal::null_ptr_address)
	                  return collapsed;
	            }
	         }
	         if constexpr (mode.is_shared())
	         {
	            if (refresh_this_node)
	            {
	               if constexpr (is_inner_node<InnerNodeType>)
	               {
	                  return make_inner_node(parent_hint, in.obj(),
	                                         subrange(branch_number(0),
	                                                  branch_number(in->num_branches())));
	               }
		               else if constexpr (is_inner_prefix_node<InnerNodeType>)
		               {
		                  const branch* brs = in->const_branches();
		                  auto          freq =
		                      create_cline_freq_table(brs, brs + in->num_branches());
		                  return _session.alloc<inner_prefix_node>(
		                      parent_hint, in.obj(), in->prefix(),
		                      subrange(branch_number(0), branch_number(in->num_branches())), freq,
		                      _root_version);
		               }
		               else if constexpr (is_wide_inner_node<InnerNodeType> ||
		                                  is_direct_inner_node<InnerNodeType>)
		               {
		                  auto plan = make_range_plan(
		                      in.obj(), subrange(branch_number(0), branch_number(in->num_branches())));
		                  return alloc_adaptive_inner(parent_hint, plan);
		               }
		            }
	            // Undo retains: no new node was created, so the retained refs
	            // are unbalanced — release all children to restore balance.
	            in->visit_branches([this](ptr_address br) { _session.release(br); });
	         }
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

      // Unique-path updates own the route to this leaf, so older readers
      // should already be on older roots. Keep only the current value for
      // this key. When the existing value_node is uniquely owned and the new
      // value still belongs in a value_node, reuse the same control block so
      // hot large-value updates do not allocate a fresh child per write. If
      // the current value fits inline, fall through to the leaf update path
      // below so collapse also demotes value_node -> inline.
      if constexpr (mode.is_unique())
      {
         if (old_value.is_value_node() && _new_value.is_view())
         {
            auto vn_ref   = _session.get_ref<value_node>(old_value.value_address());
            auto new_view = _new_value.view();
            // Skip the in-place coalesce / mvcc_realloc fast path when
            // the value_node's ptr_address is shared (ref > 1). Both the
            // coalesce_top_entry and value_node realloc paths preserve the
            // ptr_address and mutate the data location it resolves to,
            // which would also be observed by any other holder of the
            // ptr_address (e.g. a sub-transaction frame snapshot, an
            // open read cursor, or a sibling leaf created via
            // retain_children during a recent COW). Falling through to
            // the standard leaf-update path below allocates a fresh
            // value_node ptr_address for the new value, leaving the
            // shared ptr_address untouched and snapshots intact.
            if (vn_ref.ref() == 1 && !vn_ref->is_subtree_container() && new_view.size() > 64)
            {
               if (!vn_ref->is_flat() && vn_ref->num_versions() == 1 && vn_ref->num_next() == 0 &&
                   new_view.size() <= value_node::max_inline_entry_size &&
                   vn_ref->can_coalesce_in_place(vn_ref->latest_version(), new_view))
               {
                  vn_ref.modify()->coalesce_top_entry(new_view);
                  return leaf.address();
               }

               (void)_session.realloc<value_node>(vn_ref, new_view);
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
      op::leaf_update update_op{.src     = *leaf.obj(),
                                .lb      = br,
                                .key     = key,
                                .value   = new_val};

      if constexpr (mode.is_unique())
      {
         bool old_has_address = leaf->get_value_type(br) >= leaf_node::value_type_flag::value_node;

         switch (leaf->can_apply(update_op))
         {
            case leaf_node::can_apply_mode::modify:
               if (old_has_address)
                  release_value_ref(leaf->get_value(br));
               leaf.modify()->update_value(br, new_val);
               return leaf.address();
            case leaf_node::can_apply_mode::defrag:
            {
               auto plan = make_leaf_rewrite_plan_skipping(
                   *leaf.obj(), br, branch_number(uint32_t(*br) + 1));
               auto rewrite_policy = leaf_rewrite_policy(plan);
               update_op.rewrite   = &rewrite_policy;
               if (!leaf->rebuilt_size_fits(update_op))
               {
                  release_leaf_rewrite_replacements(plan);
                  update_op.rewrite = nullptr;
               }
               if (old_has_address)
                  release_value_ref(leaf->get_value(br));
               auto result = _session.realloc<leaf_node>(leaf, update_op).address();
               if (update_op.rewrite)
                  release_leaf_rewrite_sources(plan);
               return result;
            }
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
                  release_value_ref(leaf->get_value(br));
               // Release the value_node allocated by make_value above — insert()
               // will allocate its own from _new_value.
               if (new_val.is_value_node())
                  _session.release(new_val.value_address());
               auto plan = make_leaf_rewrite_plan_skipping(
                   *leaf.obj(), br, branch_number(uint32_t(*br) + 1));
               auto rewrite_policy = leaf_rewrite_policy(plan);
               op::leaf_remove rm_op{
                   .src = *leaf.obj(), .bn = br, .rewrite = &rewrite_policy};
               if (!leaf->rebuilt_size_fits(rm_op))
               {
                  release_leaf_rewrite_replacements(plan);
                  rm_op.rewrite = nullptr;
               }
               auto            removed = _session.realloc<leaf_node>(leaf, rm_op);
               if (rm_op.rewrite)
                  release_leaf_rewrite_sources(plan);
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
         bool old_has_address = leaf->get_value_type(br) >= leaf_node::value_type_flag::value_node;

         // Check if the updated data fits in max_leaf_size. If the leaf is already
         // near capacity and the new value is larger, we must split instead.
         if (leaf->can_apply(update_op) != leaf_node::can_apply_mode::none) [[likely]]
         {
            retain_children(leaf);

            // Release old external value (retained above, so release brings it back to original)
            if (old_has_address)
               release_value_ref(leaf->get_value(br));

            auto plan = make_leaf_rewrite_plan_skipping(
                *leaf.obj(), br, branch_number(uint32_t(*br) + 1));
            auto rewrite_policy = leaf_rewrite_policy(plan);
            update_op.rewrite   = &rewrite_policy;
            if (!leaf->rebuilt_size_fits(update_op))
            {
               release_leaf_rewrite_replacements(plan);
               update_op.rewrite = nullptr;
            }
            auto new_leaf       = _session.alloc<leaf_node>(parent_hint, update_op);
            if (update_op.rewrite)
               release_leaf_rewrite_sources(plan);
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
            release_value_ref(leaf->get_value(br));

         // Release the value_node allocated by make_value — insert() will create its own.
         if (new_val.is_value_node())
            _session.release(new_val.value_address());

         // Create a new leaf without the key at br (copies N-1 child refs from source).
         // This leaf has ref=1, so we can operate on it in unique mode.
         auto plan = make_leaf_rewrite_plan_skipping(
             *leaf.obj(), br, branch_number(uint32_t(*br) + 1));
         auto rewrite_policy = leaf_rewrite_policy(plan);
         op::leaf_remove rm_op{
             .src = *leaf.obj(), .bn = br, .rewrite = &rewrite_policy};
         if (!leaf->rebuilt_size_fits(rm_op))
         {
            release_leaf_rewrite_replacements(plan);
            rm_op.rewrite = nullptr;
         }
         auto            rm_addr = _session.alloc<leaf_node>(parent_hint, rm_op);
         if (rm_op.rewrite)
            release_leaf_rewrite_sources(plan);
         auto          removed = _session.get_ref<leaf_node>(rm_addr);
         branch_number new_lb  = removed->lower_bound(key);
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
            release_value_ref(leaf->get_value(lb));
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
         auto            plan = make_leaf_rewrite_plan_skipping(
             *leaf.obj(), lb, branch_number(uint32_t(*lb) + 1));
         auto            rewrite_policy = leaf_rewrite_policy(plan);
         op::leaf_remove remove_op{
             .src = *leaf.obj(), .bn = lb, .rewrite = &rewrite_policy};
         if (!leaf->rebuilt_size_fits(remove_op))
         {
            release_leaf_rewrite_replacements(plan);
            remove_op.rewrite = nullptr;
         }
         auto            new_leaf = _session.alloc<leaf_node>(parent_hint, remove_op);
         if (remove_op.rewrite)
            release_leaf_rewrite_sources(plan);

         // if bn is an address, we need to release the address (value_node or subtree)
         if (leaf->get_value_type(lb) >= leaf_node::value_type_flag::value_node)
         {
            release_value_ref(leaf->get_value(lb));
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

      _new_value = make_value(_new_value, leaf->clines());
      // this check is redundant in the case we just made value node above...
      if (_new_value.is_value_node())
      {
         //       SAL_ERROR("insert: new_value is an address");
         cline_idx = leaf->find_cline_index(_new_value.value_address());
         if (cline_idx >= leaf_node::max_value_clines)
         {
            //         SAL_ERROR("insert: split");
            // split index will have to re-assign the ptr_address of the
            // value node after identifying what side of the split the value node
            // should go on so that it isn't sharing a cline branches from another
            // node.
            return split_insert<mode>(parent_hint, leaf, key, lb);
         }
      }

      op::leaf_insert insert_op{.src       = *leaf.obj(),
                                .lb        = lb,
                                .key       = key,
                                .value     = _new_value,
                                .cline_idx = cline_idx};

      if constexpr (mode.is_unique())
      {
         //     SAL_INFO("insert: unique mode: leaf {} key: {} ptr: {}", leaf.address(), key, leaf.obj());
         switch (leaf->can_apply(insert_op))
         {
            case leaf_node::can_apply_mode::modify:
               [[likely]] leaf.modify()->apply(insert_op);
               return leaf.address();
            case leaf_node::can_apply_mode::defrag:
            {
               auto plan           = make_leaf_rewrite_plan(*leaf.obj());
               auto rewrite_policy = leaf_rewrite_policy(plan);
               insert_op.rewrite   = &rewrite_policy;
               if (!leaf->rebuilt_size_fits(insert_op))
               {
                  release_leaf_rewrite_replacements(plan);
                  return split_insert<mode>(parent_hint, leaf, key, lb);
               }
               auto result =
                   _session.realloc<leaf_node>(leaf, leaf.obj(), insert_op).address();
               release_leaf_rewrite_sources(plan);
               return result;
            }
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
               auto plan           = make_leaf_rewrite_plan(*leaf.obj());
               auto rewrite_policy = leaf_rewrite_policy(plan);
               insert_op.rewrite   = &rewrite_policy;
               if (!leaf->rebuilt_size_fits(insert_op))
               {
                  release_leaf_rewrite_replacements(plan);
                  return split_insert<mode>(parent_hint, leaf, key, lb);
               }
               auto result = _session.alloc<leaf_node>(parent_hint, leaf.obj(), insert_op);
               release_leaf_rewrite_sources(plan);
               return result;
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
   inline bool tree_context::append_versioned_value(smart_ref<value_node>& vref,
                                                       uint64_t               version,
                                                       value_view             new_val)
   {
      if (vref->is_flat() || vref->is_subtree_container() ||
          new_val.size() > value_node::max_inline_entry_size)
         return false;

      if (vref->can_coalesce_in_place(version, new_val))
      {
         vref.modify()->coalesce_top_entry(new_val);
         return true;
      }

      const bool replace_last = vref->num_versions() > 0 && vref->latest_version() == version;
      if (replace_last)
         (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, new_val,
                                                 value_node::replace_last_tag{});
      else if (_dead_snap)
         (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, new_val, _dead_snap);
      else
         (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, new_val);

      return true;
   }

   inline bool tree_context::append_versioned_tombstone(smart_ref<value_node>& vref,
                                                           uint64_t               version)
   {
      if (vref->is_flat())
         return false;

      auto pending = make_pending_release_list(512);
      auto release_pending = [&]()
      {
         for (auto adr : pending)
            _session.release(adr);
      };

      const bool replace_last = vref->num_versions() > 0 && vref->latest_version() == version;
      if (replace_last)
      {
         if (!vref->collect_replace_last_references(pending))
            return false;
         (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr,
                                                 value_node::replace_last_tag{});
         release_pending();
      }
      else if (_dead_snap)
      {
         if (!vref->collect_dead_references(_dead_snap, pending))
            return false;
         (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr, _dead_snap);
         release_pending();
      }
      else
         (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr);

      return true;
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
      auto       rewrite_plan   = make_leaf_rewrite_plan(*leaf.obj());
      auto       rewrite_policy = leaf_rewrite_policy(rewrite_plan);
      const op::leaf_value_rewrite* split_rewrite = &rewrite_policy;

      // Special case: leaf has only 1 entry and can't fit a second key.
      // get_split_pos() requires nb > 1, so we manually build the split:
      // create two single-entry leaves and an inner_prefix_node.
      if (leaf->num_branches() == 1)
      {
         key_view existing_key = leaf->get_key(branch_zero);

         // Find common prefix length
         size_t cplen  = 0;
         size_t max_cp = std::min(existing_key.size(), key.size());
         while (cplen < max_cp && existing_key[cplen] == key[cplen])
            ++cplen;

         key_view cprefix = existing_key.substr(0, cplen);

         // Determine dividing byte and which key goes left vs right
         // The key with the smaller byte at cplen goes left (divider = larger byte)
         uint8_t existing_byte = (cplen < existing_key.size()) ? existing_key[cplen] : 0;
         uint8_t new_byte      = (cplen < key.size()) ? key[cplen] : 0;

         // Alloc both leaves (don't realloc leaf — we need it for remake_inner_prefix)
         if (!leaf->rebuilt_size_fits(cprefix, branch_zero, branch_number(1), split_rewrite))
         {
            release_leaf_rewrite_replacements(rewrite_plan);
            split_rewrite = nullptr;
         }
         ptr_address existing_leaf = _session.alloc<leaf_node>(
             parent_hint, leaf.obj(), cprefix, branch_zero, branch_number(1), split_rewrite);
         if (split_rewrite)
            release_leaf_rewrite_sources(rewrite_plan);

         key_view    new_key_suffix = key.substr(cplen);
         ptr_address new_leaf       = _session.alloc<leaf_node>(
             {&existing_leaf, 1}, new_key_suffix, make_value(_new_value, {&existing_leaf, 1}));

         // Build inner_prefix_node: the divider is the larger of the two bytes
         ptr_address left_addr, right_addr;
         uint8_t     divider;
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
      if (!leaf->rebuilt_size_fits(spos.cprefix, branch_zero, left_size, split_rewrite) ||
          !leaf->rebuilt_size_fits(spos.cprefix, left_size, right_end, split_rewrite))
      {
         release_leaf_rewrite_replacements(rewrite_plan);
         split_rewrite = nullptr;
      }

      if (spos.cprefix.size() > 0)
      {
         ptr_address left = _session.alloc<leaf_node>(
             parent_hint, leaf.obj(), spos.cprefix, branch_zero, left_size, split_rewrite);
         ptr_address right = _session.alloc<leaf_node>(
             {&left, 1}, leaf.obj(), spos.cprefix, left_size, right_end, split_rewrite);
         if (split_rewrite)
            release_leaf_rewrite_sources(rewrite_plan);

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
         left = _session
                    .realloc<leaf_node>(leaf, leaf.obj(), key_view(), branch_zero, left_size,
                                        split_rewrite)
                    .address();
      else
      {
         left = _session.alloc<leaf_node>(parent_hint, leaf.obj(), key_view(), branch_zero,
                                          left_size, split_rewrite);
      }
      ptr_address right = _session.alloc<leaf_node>(sal::alloc_hint{&left, 1}, leaf.obj(),
                                                    key_view(), left_size, right_end,
                                                    split_rewrite);
      if (split_rewrite)
         release_leaf_rewrite_sources(rewrite_plan);
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
         branch_number lb = [&] {
            if constexpr (mode.is_sorted())
               return leaf->lower_bound_append_hint(key);
            else
               return leaf->lower_bound(key);
         }();
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

   inline std::string tree_context::bplus_first_key(ptr_address addr) const
   {
      for (;;)
      {
         auto ref = _session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::bplus_inner:
            {
               auto in = ref.as<bplus_inner_node>();
               addr    = in->get_branch(branch_number(0));
               continue;
            }
            case node_type::leaf:
            {
               auto leaf = ref.as<leaf_node>();
               assert(leaf->num_branches() > 0);
               return std::string(leaf->get_key(branch_number(0)));
            }
            default:
               std::unreachable();
         }
      }
   }

   inline tree_context::bplus_result tree_context::bplus_from_branch_set(branch_set result)
   {
      if (result.count() == 0)
         return {.empty = true};
      if (result.count() == 1)
         return {.left = result.get_first_branch()};

      auto addrs = result.addresses();
      assert(addrs.size() == 2);
      return {.left      = addrs[0],
              .right     = addrs[1],
              .separator = bplus_first_key(addrs[1]),
              .split     = true};
   }

   template <upsert_mode mode>
   tree_context::bplus_result tree_context::bplus_split_leaf(const sal::alloc_hint& parent_hint,
                                                            smart_ref<leaf_node>&  leaf,
                                                            key_view               key,
                                                            branch_number          lb)
   {
      const uint16_t nb = leaf->num_branches();
      branch_number  mid(nb / 2);
      if (*lb > *mid)
         mid = branch_number(*mid + 1);
      if (mid == branch_number(0))
         mid = branch_number(1);
      if (mid == leaf->num_branches())
         mid = branch_number(leaf->num_branches() - 1);

      auto rewrite_plan   = make_leaf_rewrite_plan(*leaf.obj());
      auto rewrite_policy = leaf_rewrite_policy(rewrite_plan);
      const op::leaf_value_rewrite* split_rewrite = &rewrite_policy;
      if (!leaf->rebuilt_size_fits(key_view(), branch_zero, mid, split_rewrite) ||
          !leaf->rebuilt_size_fits(key_view(), mid, branch_number(nb), split_rewrite))
      {
         release_leaf_rewrite_replacements(rewrite_plan);
         split_rewrite = nullptr;
      }

      ptr_address left;
      if constexpr (mode.is_unique())
         left = _session
                    .realloc<leaf_node>(leaf, leaf.obj(), key_view(), branch_zero, mid,
                                        split_rewrite)
                    .address();
      else
         left = _session.alloc<leaf_node>(parent_hint, leaf.obj(), key_view(), branch_zero, mid,
                                          split_rewrite);

      ptr_address right = _session.alloc<leaf_node>(sal::alloc_hint{&left, 1}, leaf.obj(),
                                                    key_view(), mid, branch_number(nb),
                                                    split_rewrite);
      if (split_rewrite)
         release_leaf_rewrite_sources(rewrite_plan);

      ptr_address* target_addr = nullptr;
      {
         auto     right_ref   = _session.get_ref<leaf_node>(right);
         key_view right_first = right_ref->get_key(branch_number(0));
         target_addr          = key < right_first ? &left : &right;
      }

      auto          target_ref = _session.get_ref<leaf_node>(*target_addr);
      branch_number target_lb  = target_ref->lower_bound(key);
      auto inserted = insert<upsert_mode::unique_upsert>(sal::alloc_hint{target_addr, 1},
                                                         target_ref, key, target_lb);
      assert(inserted.count() == 1);
      *target_addr = inserted.get_first_branch();

      return {.left      = left,
              .right     = right,
              .separator = bplus_first_key(right),
              .split     = true};
   }

   template <upsert_mode mode>
   tree_context::bplus_result tree_context::bplus_upsert(const sal::alloc_hint& parent_hint,
                                                         smart_ref<leaf_node>&  leaf,
                                                         key_view               key)
   {
      if constexpr (mode.must_update())
      {
         branch_number br = leaf->get(key);
         if (br == leaf->num_branches())
         {
            if constexpr (mode.is_shared())
               return {.left = leaf.address()};
            assert(!"update precondition violated: key does not exist");
            std::unreachable();
         }
         return bplus_from_branch_set(update<mode>(parent_hint, leaf, key, br));
      }
      if constexpr (mode.must_remove())
      {
         branch_number br = leaf->get(key);
         if (br == leaf->num_branches())
         {
            if constexpr (mode.is_shared())
               return {.left = leaf.address()};
            assert(!"must_remove precondition violated: key does not exist");
            std::unreachable();
         }
         return bplus_from_branch_set(remove<mode>(parent_hint, leaf, key, br));
      }

      branch_number lb = [&] {
         if constexpr (mode.is_sorted())
            return leaf->lower_bound_append_hint(key);
         else
            return leaf->lower_bound(key);
      }();

      if constexpr (mode.is_remove())
      {
         if (lb != leaf->num_branches())
            return bplus_from_branch_set(remove<mode>(parent_hint, leaf, key, lb));
         return {.left = leaf.address()};
      }

      if constexpr (mode.is_upsert())
      {
         if (lb != leaf->num_branches() && leaf->get_key(lb) == key)
            return bplus_from_branch_set(update<mode>(parent_hint, leaf, key, lb));
      }
      else if constexpr (mode.is_insert())
      {
         if (!(lb == leaf->num_branches() || leaf->get_key(lb) != key))
         {
            if constexpr (mode.is_shared())
               return {.left = leaf.address()};
            assert(!"insert precondition violated: key already exists");
            std::unreachable();
         }
      }

      if constexpr (mode.is_shared())
         retain_children(leaf);

      uint8_t cline_idx = 0xff;
      _new_value = make_value(_new_value, leaf->clines());
      if (_new_value.is_value_node())
      {
         cline_idx = leaf->find_cline_index(_new_value.value_address());
         if (cline_idx >= leaf_node::max_value_clines)
            return bplus_split_leaf<mode>(parent_hint, leaf, key, lb);
      }

      op::leaf_insert insert_op{.src       = *leaf.obj(),
                                .lb        = lb,
                                .key       = key,
                                .value     = _new_value,
                                .cline_idx = cline_idx};

      if constexpr (mode.is_unique())
      {
         switch (leaf->can_apply(insert_op))
         {
            case leaf_node::can_apply_mode::modify:
               leaf.modify()->apply(insert_op);
               return {.left = leaf.address()};
            case leaf_node::can_apply_mode::defrag:
            {
               auto plan           = make_leaf_rewrite_plan(*leaf.obj());
               auto rewrite_policy = leaf_rewrite_policy(plan);
               insert_op.rewrite   = &rewrite_policy;
               if (!leaf->rebuilt_size_fits(insert_op))
               {
                  release_leaf_rewrite_replacements(plan);
                  return bplus_split_leaf<mode>(parent_hint, leaf, key, lb);
               }
               auto result =
                   _session.realloc<leaf_node>(leaf, leaf.obj(), insert_op).address();
               release_leaf_rewrite_sources(plan);
               return {.left = result};
            }
            case leaf_node::can_apply_mode::none:
               return bplus_split_leaf<mode>(parent_hint, leaf, key, lb);
         }
      }
      else
      {
         switch (leaf->can_apply(insert_op))
         {
            case leaf_node::can_apply_mode::modify:
            case leaf_node::can_apply_mode::defrag:
            {
               auto plan           = make_leaf_rewrite_plan(*leaf.obj());
               auto rewrite_policy = leaf_rewrite_policy(plan);
               insert_op.rewrite   = &rewrite_policy;
               if (!leaf->rebuilt_size_fits(insert_op))
               {
                  release_leaf_rewrite_replacements(plan);
                  return bplus_split_leaf<mode>(parent_hint, leaf, key, lb);
               }
               auto result = _session.alloc<leaf_node>(parent_hint, leaf.obj(), insert_op);
               release_leaf_rewrite_sources(plan);
               return {.left = result};
            }
            case leaf_node::can_apply_mode::none:
               return bplus_split_leaf<mode>(parent_hint, leaf, key, lb);
         }
      }
      std::unreachable();
   }

   template <upsert_mode mode>
   tree_context::bplus_result
   tree_context::bplus_upsert(const sal::alloc_hint&        parent_hint,
                              smart_ref<bplus_inner_node>& in,
                              key_view                     key)
   {
      const bool refresh_this_node = needs_structural_refresh(in->last_unique_version());
      if constexpr (mode.is_unique())
      {
         if (refresh_this_node)
            in.modify()->set_last_unique_version(_root_version);
      }

      branch_number br        = in->lower_bound(key);
      ptr_address   old_child = in->get_branch(br);
      auto          child_ref = _session.get_ref(old_child);

      if constexpr (mode.is_shared())
         retain_children(in);

      auto child = bplus_upsert<mode>(in->get_branch_clines(), child_ref, key);

      if (child.empty)
      {
         if (in->num_branches() == 1)
            return {.empty = true};
         if (in->num_branches() == 2)
         {
            branch_number keep(*br == 0 ? 1 : 0);
            return {.left = in->get_branch(keep)};
         }

         auto plan = make_bplus_remove_plan(in.obj(), br);
         if constexpr (mode.is_unique())
         {
            auto result = _session.realloc<bplus_inner_node>(in, plan);
            result.modify()->set_last_unique_version(_root_version);
            return {.left = result.address()};
         }
         else
         {
            _session.release(old_child);
            auto result = _session.alloc<bplus_inner_node>(parent_hint, plan);
            auto ref    = _session.get_ref<bplus_inner_node>(result);
            ref.modify()->set_last_unique_version(_root_version);
            return {.left = result};
         }
      }

      if (!child.split && child.left == old_child)
      {
         if constexpr (mode.is_shared())
            in->visit_branches([this](ptr_address addr) { _session.release(addr); });
         return {.left = in.address()};
      }

      auto plan      = make_bplus_replace_plan(in.obj(), br, child);
      auto plan_size = bplus_inner_node::alloc_size(plan);
      if (plan.num_branches <= op::bplus_build_plan::max_branches &&
          plan_size <= bplus_inner_node::max_inner_size)
      {
         if constexpr (mode.is_unique())
         {
            auto result = _session.realloc<bplus_inner_node>(in, plan);
            result.modify()->set_last_unique_version(_root_version);
            return {.left = result.address()};
         }
         else
         {
            if (child.left != old_child || child.split)
               _session.release(old_child);
            auto result = _session.alloc<bplus_inner_node>(parent_hint, plan);
            auto ref    = _session.get_ref<bplus_inner_node>(result);
            ref.modify()->set_last_unique_version(_root_version);
            return {.left = result};
         }
      }

      const uint16_t mid = plan.num_branches / 2;
      op::bplus_build_plan left_plan;
      op::bplus_build_plan right_plan;
      left_plan.push_first(plan.branches[0]);
      for (uint16_t i = 1; i < mid; ++i)
         left_plan.push_back(plan.separators[i - 1], plan.branches[i]);
      std::string separator(plan.separators[mid - 1]);
      right_plan.push_first(plan.branches[mid]);
      for (uint16_t i = mid + 1; i < plan.num_branches; ++i)
         right_plan.push_back(plan.separators[i - 1], plan.branches[i]);

      ptr_address left;
      if constexpr (mode.is_unique())
         left = _session.realloc<bplus_inner_node>(in, left_plan).address();
      else
      {
         if (child.left != old_child || child.split)
            _session.release(old_child);
         left = _session.alloc<bplus_inner_node>(parent_hint, left_plan);
      }
      auto right = _session.alloc<bplus_inner_node>(sal::alloc_hint{&left, 1}, right_plan);
      auto left_ref = _session.get_ref<bplus_inner_node>(left);
      left_ref.modify()->set_last_unique_version(_root_version);
      auto right_ref = _session.get_ref<bplus_inner_node>(right);
      right_ref.modify()->set_last_unique_version(_root_version);
      return {.left = left, .right = right, .separator = std::move(separator), .split = true};
   }

   template <upsert_mode mode>
   tree_context::bplus_result tree_context::bplus_upsert(const sal::alloc_hint&   parent_hint,
                                                         smart_ref<alloc_header>& r,
                                                         key_view                 key)
   {
      if constexpr (mode.is_unique())
         if (r.ref() > 1)
            return bplus_upsert<mode.make_shared()>(parent_hint, r, key);

      bplus_result result;
      switch (node_type(r->type()))
      {
         case node_type::bplus_inner:
            result = bplus_upsert<mode>(parent_hint, r.as<bplus_inner_node>(), key);
            break;
         case node_type::leaf:
            result = bplus_upsert<mode>(parent_hint, r.as<leaf_node>(), key);
            break;
         default:
            std::unreachable();
      }

      const bool contains_old = (!result.empty && result.left == r.address()) ||
                                (result.split && result.right == r.address());
      if constexpr (mode.is_unique())
      {
         if constexpr (mode.is_remove())
         {
            if (result.empty)
               r.release();
            else if (!contains_old)
               r.release();
         }
         assert(result.empty || contains_old || mode.is_remove());
      }
      else if (!contains_old)
         r.release();
      return result;
   }

   inline int tree_context::bplus_upsert_root(key_view key, value_type value)
   {
      auto op_scope =
          _session.record_operation(sal::mapped_memory::session_operation::tree_upsert);
      _old_value_size     = -1;
      sal::read_lock lock = _session.lock();
      _new_value          = std::move(value);
      if (!_root)
      {
         auto leaf_addr = _session.alloc<leaf_node>(sal::alloc_hint(), key,
                                                    make_value(_new_value, sal::alloc_hint()));
         _root.give(leaf_addr);
         return -1;
      }

      auto rref     = *_root;
      auto old_addr = _root.take();
      bplus_result result;
      try
      {
         result = bplus_upsert<upsert_mode::unique_upsert>({}, rref, key);
      }
      catch (...)
      {
         _root.give(old_addr);
         throw;
      }

      if (result.empty)
         ;
      else if (result.split)
      {
         auto plan     = make_bplus_leaf_split_plan(result.left, result.separator, result.right);
         auto new_root = _session.alloc<bplus_inner_node>({}, plan);
         auto ref      = _session.get_ref<bplus_inner_node>(new_root);
         ref.modify()->set_last_unique_version(_root_version);
         _root.give(new_root);
      }
      else
         _root.give(result.left);
      return _old_value_size;
   }

   inline int tree_context::bplus_remove_root(key_view key)
   {
      auto op_scope =
          _session.record_operation(sal::mapped_memory::session_operation::tree_remove);
      if (!_root)
         return -1;
      _old_value_size     = -1;
      sal::read_lock lock = _session.lock();
      auto rref           = *_root;
      auto old_addr       = _root.take();
      auto result         = bplus_upsert<upsert_mode::unique_remove>({}, rref, key);
      if (result.empty)
         ;
      else if (result.split)
      {
         auto plan     = make_bplus_leaf_split_plan(result.left, result.separator, result.right);
         auto new_root = _session.alloc<bplus_inner_node>({}, plan);
         auto ref      = _session.get_ref<bplus_inner_node>(new_root);
         ref.modify()->set_last_unique_version(_root_version);
         _root.give(new_root);
      }
      else
         _root.give(result.left);
      return _old_value_size;
   }

   inline void tree_context::bplus_collect_range_keys(ptr_address              addr,
                                                      key_view                 lower,
                                                      key_view                 upper,
                                                      std::vector<std::string>& keys) const
   {
      if (addr == sal::null_ptr_address)
         return;

      auto ref = _session.get_ref(addr);
      switch (node_type(ref->type()))
      {
         case node_type::bplus_inner:
         {
            auto in = ref.as<bplus_inner_node>();
            for (uint16_t i = 0; i < in->num_branches(); ++i)
            {
               if (!upper.empty() && i > 0 && in->separator(i - 1) >= upper)
                  break;
               if (!lower.empty() && i + 1 < in->num_branches() &&
                   in->separator(i) <= lower)
                  continue;
               bplus_collect_range_keys(in->get_branch(branch_number(i)), lower, upper, keys);
            }
            break;
         }
         case node_type::leaf:
         {
            auto          leaf = ref.as<leaf_node>();
            branch_number pos  = lower.empty() ? branch_number(0) : leaf->lower_bound(lower);
            for (uint16_t i = *pos; i < leaf->num_branches(); ++i)
            {
               auto key = leaf->get_key(branch_number(i));
               if (!upper.empty() && key >= upper)
                  break;
               keys.emplace_back(key.data(), key.size());
            }
            break;
         }
         default:
            std::unreachable();
      }
   }

   inline uint64_t tree_context::bplus_remove_range_counted(key_view lower, key_view upper)
   {
      auto op_scope = _session.record_operation(
          sal::mapped_memory::session_operation::tree_remove_range_counted);
      if (!_root)
         return 0;
      if (lower >= upper && upper != max_key)
         return 0;

      key_view internal_upper = upper == max_key ? key_view() : upper;
      std::vector<std::string> keys;
      {
         sal::read_lock lock = _session.lock();
         bplus_collect_range_keys(_root.address(), lower, internal_upper, keys);
      }

      uint64_t removed = 0;
      for (const auto& key : keys)
         if (bplus_remove_root(key_view(key.data(), key.size())) >= 0)
            ++removed;
      return removed;
   }

   inline bool tree_context::bplus_remove_range_any(key_view lower, key_view upper)
   {
      auto op_scope =
          _session.record_operation(sal::mapped_memory::session_operation::tree_remove_range_any);
      if (!_root)
         return false;
      if (lower >= upper && upper != max_key)
         return false;

      key_view internal_upper = upper == max_key ? key_view() : upper;
      std::vector<std::string> keys;
      {
         sal::read_lock lock = _session.lock();
         bplus_collect_range_keys(_root.address(), lower, internal_upper, keys);
      }

      bool removed = false;
      for (const auto& key : keys)
         removed = (bplus_remove_root(key_view(key.data(), key.size())) >= 0) || removed;
      return removed;
   }

   // ─── Explicit-version write target ────────────────────────────────
   inline ptr_address tree_context::find_versioned_write_target(key_view key) const
   {
      if (!_root)
         return sal::null_ptr_address;  // empty tree → COW (allocate root)

      ptr_address addr      = _root.address();
      key_view    remaining = key;

      for (;;)
      {
         auto ref = _session.get_ref(addr);
         switch (node_type(ref->type()))
         {
            case node_type::inner_prefix:
            {
               auto ipn = ref.as<inner_prefix_node>();
               if (needs_structural_refresh(ipn->last_unique_version()))
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
               if (needs_structural_refresh(in->last_unique_version()))
                  return sal::null_ptr_address;  // stale epoch → COW cascade for maintenance
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::wide_inner:
            {
               auto in = ref.as<wide_inner_node>();
               if (needs_structural_refresh(in->last_unique_version()))
                  return sal::null_ptr_address;
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::direct_inner:
            {
               auto in = ref.as<direct_inner_node>();
               if (needs_structural_refresh(in->last_unique_version()))
                  return sal::null_ptr_address;
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::bplus_inner:
            {
               auto in = ref.as<bplus_inner_node>();
               if (needs_structural_refresh(in->last_unique_version()))
                  return sal::null_ptr_address;
               addr = in->get_branch(in->lower_bound(remaining));
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
                  return leaf.address();          // Case A: lock leaf (inline → value_node)
               }
               return leaf.address();  // Case C: lock leaf (new key insert)
            }
            default:
               return sal::null_ptr_address;
         }
      }
   }

   // ─── try_upsert_at_version: stripe-lock-safe, no COW fallback ──────
   inline bool tree_context::try_upsert_at_version(key_view key, value_type value, uint64_t version)
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
               auto ipn = ref.as<inner_prefix_node>();
               if (needs_structural_refresh(ipn->last_unique_version()))
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
               if (needs_structural_refresh(in->last_unique_version()))
                  return false;  // stale epoch → COW cascade
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::wide_inner:
            {
               auto in = ref.as<wide_inner_node>();
               if (needs_structural_refresh(in->last_unique_version()))
                  return false;
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::direct_inner:
            {
               auto in = ref.as<direct_inner_node>();
               if (needs_structural_refresh(in->last_unique_version()))
                  return false;
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::bplus_inner:
            {
               auto in = ref.as<bplus_inner_node>();
               if (needs_structural_refresh(in->last_unique_version()))
                  return false;
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::leaf:
            {
               auto          leaf = ref.as<leaf_node>();
               branch_number lb   = leaf->lower_bound(remaining);

               if (lb != leaf->num_branches() && leaf->get_key(lb) == remaining)
               {
                  // Key exists
                  auto old_val = leaf->get_value(lb);

                  if (old_val.is_value_node())
                  {
                     auto vref = _session.get_ref<value_node>(old_val.value_address());
                     auto new_val = value.is_view() ? value.view() : value_view();
                     if (!append_versioned_value(vref, version, new_val))
                        return false;  // COW fallback
                     return true;
                  }

                  // Case A: inline → value_node promotion + leaf update
                  value_view old_data = old_val.view();
                  auto       new_data = value.is_view() ? value.view() : value_view();
                  uint64_t   old_ver  = leaf->get_version(lb);

                  auto vn_addr = _session.alloc<value_node>(leaf->clines(), old_ver, old_data,
                                                            version, new_data);

                  value_type      vn_value = value_type::make_value_node(vn_addr);
                  op::leaf_update update_op{
                      .src = *leaf.obj(), .lb = lb, .key = remaining, .value = vn_value};

                  if (leaf->can_apply(update_op) != leaf_node::can_apply_mode::none)
                  {
                     (void)_session.mvcc_realloc<leaf_node>(leaf, update_op);
                     return true;
                  }
                  // Overflow — release allocated value_node, signal COW needed
                  _session.release(vn_addr);
                  return false;
               }

               // Case C: new key — insert into leaf via CB relocation.
               // The branch creation version keeps older snapshots from
               // seeing the new inline branch while allowing the leaf address
               // to stay stable.
               auto new_val = make_value(value, leaf->clines());

               uint8_t cline_idx = 0xff;
               if (new_val.is_value_node())
                  cline_idx = leaf->find_cline_index(new_val.value_address());

               op::leaf_insert insert_op{.src        = *leaf.obj(),
                                         .lb         = lb,
                                         .key        = remaining,
                                         .value      = new_val,
                                         .cline_idx  = cline_idx,
                                         .created_at = version};

               if ((!new_val.is_value_node() || cline_idx < leaf_node::max_value_clines) &&
                   leaf->can_apply(insert_op) != leaf_node::can_apply_mode::none)
               {
                  (void)_session.mvcc_realloc<leaf_node>(leaf, leaf.obj(), insert_op);
                  return true;
               }
               if (new_val.is_value_node())
                  _session.release(new_val.value_address());
               return false;
            }
            default:
               return false;
         }
      }
   }

   // ─── try_remove_at_version: stripe-lock-safe, no COW fallback ──────
   inline bool tree_context::try_remove_at_version(key_view key, uint64_t version)
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
               auto ipn = ref.as<inner_prefix_node>();
               if (needs_structural_refresh(ipn->last_unique_version()))
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
               if (needs_structural_refresh(in->last_unique_version()))
                  return false;  // stale epoch → COW cascade
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::wide_inner:
            {
               auto in = ref.as<wide_inner_node>();
               if (needs_structural_refresh(in->last_unique_version()))
                  return false;
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::direct_inner:
            {
               auto in = ref.as<direct_inner_node>();
               if (needs_structural_refresh(in->last_unique_version()))
                  return false;
               addr = in->get_branch(in->lower_bound(remaining));
               continue;
            }
            case node_type::bplus_inner:
            {
               auto in = ref.as<bplus_inner_node>();
               if (needs_structural_refresh(in->last_unique_version()))
                  return false;
               addr = in->get_branch(in->lower_bound(remaining));
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
                  if (!append_versioned_tombstone(vref, version))
                     return false;  // COW fallback for flat nodes
                  return true;
               }

               // Inline value: promote to value_node + tombstone, update leaf
               value_view old_data = old_val.view();
               auto       vn_addr  = _session.alloc<value_node>(leaf->clines(), old_data);
               auto       vref     = _session.get_ref<value_node>(vn_addr);
               if (_dead_snap)
                  (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr,
                                                          _dead_snap);
               else
                  (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), version, nullptr);

               value_type      vn_value = value_type::make_value_node(vn_addr);
               op::leaf_update update_op{
                   .src = *leaf.obj(), .lb = lb, .key = remaining, .value = vn_value};
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

   inline void tree_context::upsert_at_version(key_view key, value_type value, uint64_t version)
   {
      if (!_root)
      {
         sal::read_lock lock = _session.lock();
         // Preserve _ver — see same pattern in insert() above.
         auto leaf_addr = _session.alloc<leaf_node>(sal::alloc_hint(), key,
                                                    make_value(value, sal::alloc_hint()));
         _root.give(leaf_addr);
         return;
      }

      if (try_upsert_at_version(key, value, version))
         return;

      // Prefix mismatch, stale structural freshness, overflow, or split.
      // Fall back to the normal structural writer; snapshots remain protected
      // by their older root while the new root carries this write version.
      upsert<upsert_mode::unique_upsert>(key, std::move(value));
   }

   inline void tree_context::upsert_leaf_at_version(smart_ref<leaf_node>& leaf,
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
            auto new_val = value.is_view() ? value.view() : value_view();
            if (!append_versioned_value(vref, version, new_val))
            {
               // This value_node cannot carry inline MVCC entries — COW fallback.
               upsert<upsert_mode::unique_upsert>(full_key, std::move(value));
               return;
            }
            // ptr_address unchanged — leaf and inner nodes untouched
            return;
         }

         // Case A: existing key with inline value — promote to 2-entry value_node
         value_view old_data = old_val.view();
         auto       new_data = value.is_view() ? value.view() : value_view();
         uint64_t   old_ver  = leaf->get_version(lb);

         auto vn_addr =
             _session.alloc<value_node>(leaf->clines(), old_ver, old_data, version, new_data);

         // COW the leaf to update the value branch from inline to value_node
         value_type      vn_value = value_type::make_value_node(vn_addr);
         op::leaf_update update_op{
             .src = *leaf.obj(), .lb = lb, .key = leaf_key, .value = vn_value};

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
      if (new_val.is_value_node())
         cline_idx = leaf->find_cline_index(new_val.value_address());

      op::leaf_insert insert_op{.src        = *leaf.obj(),
                                .lb         = lb,
                                .key        = leaf_key,
                                .value      = new_val,
                                .cline_idx  = cline_idx,
                                .created_at = version};

      if ((!new_val.is_value_node() || cline_idx < leaf_node::max_value_clines) &&
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

   inline void tree_context::remove_at_version(key_view key, uint64_t version)
   {
      if (try_remove_at_version(key, version))
         return;

      // Stale structural freshness or an unsupported value-node layout needs
      // the normal structural writer.
      remove(key);
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
      auto     ref     = _session.get_ref(addr);

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
         case node_type::wide_inner:
         {
            auto in = ref.as<wide_inner_node>();
            for (uint16_t i = 0; i < in->num_branches(); ++i)
               cleaned += defrag_subtree(in->get_branch(branch_number(i)));
            break;
         }
         case node_type::direct_inner:
         {
            auto in = ref.as<direct_inner_node>();
            for (uint16_t i = 0; i < in->num_branches(); ++i)
               cleaned += defrag_subtree(in->get_branch(branch_number(i)));
            break;
         }
         case node_type::bplus_inner:
         {
            auto in = ref.as<bplus_inner_node>();
            for (uint16_t i = 0; i < in->num_branches(); ++i)
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
         const uint64_t retained_floor = _dead_snap->oldest_retained_floor();
         if (retained_floor != 0 && value_node_needs_prune_floor(*vref, retained_floor))
         {
            value_node::prune_floor_policy prune{retained_floor};
            auto pending = make_pending_release_list(512);
            if (!vref->collect_pruned_references(prune, pending))
               continue;
            (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), prune);
            for (auto adr : pending)
               _session.release(adr);
            ++cleaned;
         }
         else if (vref->has_dead_entries(_dead_snap))
         {
            auto pending = make_pending_release_list(512);
            if (!vref->collect_dead_references(_dead_snap, pending))
               continue;
            (void)_session.mvcc_realloc<value_node>(vref, vref.obj(), _dead_snap);
            for (auto adr : pending)
               _session.release(adr);
            ++cleaned;
         }
      }
      return cleaned;
   }

}  // namespace psitri

#include <psitri/range_remove.hpp>
