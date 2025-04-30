#pragma once
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/node.hpp>
#include <psitri/node/value_node.hpp>
#include <psitri/upsert_mode.hpp>
#include <sal/allocator_session.hpp>
#include <sal/smart_ptr.hpp>

namespace psitri
{
   using sal::alloc_header;
   using sal::smart_ptr;
   using sal::smart_ref;
   class tree_context
   {
      value_type                   _new_value;
      sal::allocator_session&      _session;
      sal::smart_ptr<alloc_header> _root;
      int                          _old_value_size = -1;

     public:
      sal::smart_ptr<alloc_header> get_root() const { return _root; }
      tree_context(sal::smart_ptr<alloc_header> root)
          : _root(std::move(root)), _session(*(root.session()))
      {
         SAL_WARN("tree_context constructor: {} {}", &_root, _root.address());
      }
      /**
       * Given a value type, if it is too large for inline converts it to a value_node,
       * otherwise returns the value type unchanged.
       */
      value_type make_value(value_type value, sal::alloc_hint hint) noexcept
      {
         if (value.is_subtree())
            return value;
         if (value.is_view())
         {
            auto v = value.view();
            if (v.size() > 64)
               return value_type::make_value_node(_session.alloc<value_node>(hint, v));
            return value;
         }
         assert(!"only value_view or subtree values are supported");
         abort();
      }
      ptr_address make_inner(const branch_set& branches) noexcept
      {
         std::array<uint8_t, 8> out_cline_idx;
         auto                   needed_clines = find_clines(branches, out_cline_idx);
         return _session.alloc<inner_node>(branches, needed_clines, out_cline_idx);
      }
      ptr_address make_inner_prefix(sal::alloc_hint   hint,
                                    key_view          prefix,
                                    const branch_set& branches) noexcept
      {
         std::array<uint8_t, 8> out_cline_idx;
         auto                   needed_clines = find_clines(branches, out_cline_idx);
         return _session.alloc<inner_prefix_node>(hint, prefix, branches, needed_clines,
                                                  out_cline_idx);
      }
      template <typename NodeType>
      smart_ref<inner_prefix_node> remake_inner_prefix(const smart_ref<NodeType>& in,
                                                       key_view                   prefix,
                                                       const branch_set&          branches) noexcept
      {
         std::array<uint8_t, 8> out_cline_idx;
         auto                   needed_clines = find_clines(branches, out_cline_idx);
         return _session.realloc<inner_prefix_node>(in, prefix, branches, needed_clines,
                                                    out_cline_idx);
      }

      template <typename InnerNodeType>
      ptr_address make_inner_node(const sal::alloc_hint& parent_hint,
                                  const InnerNodeType*   in,
                                  subrange               range)
      {
         const branch* brs  = in->const_branches();
         auto          freq = create_cline_freq_table(brs + *range.begin, brs + *range.end);
         return _session.alloc<inner_node>(parent_hint, in, range, freq);
      }
      template <typename NodeType, any_inner_node_type InnerNodeType>
      smart_ref<inner_node> remake_inner_node(const smart_ref<NodeType>& in,
                                              const InnerNodeType*       in_obj,
                                              const subrange&            range) noexcept
      {
         const branch* brs  = in->const_branches();
         auto          freq = create_cline_freq_table(brs + *range.begin, brs + *range.end);
         return _session.realloc<inner_node>(in, in_obj, range, freq);
      }

      void insert(key_view key, value_type value)
      {
         _old_value_size = -1;
         auto result     = upsert<upsert_mode::unique_insert>(key, value);
         assert(result == -1);
      }

      /// @return the size of the prior value, or -1 if the value was not found.
      int remove(key_view key) { return upsert<upsert_mode::unique_remove>(key, value_type()); }

      /// @return the size of the prior value, or -1 if the value was inserted.
      template <upsert_mode mode = upsert_mode::unique_upsert>
      int upsert(key_view key, value_type value)
      {
         sal::read_lock lock = _session.lock();
         _new_value          = std::move(value);
         if (not _root)
         {
            _root = _session.smart_alloc<leaf_node>(key, make_value(_new_value, sal::alloc_hint()));
            return -1;
         }
         auto rref = *_root;

         _root.take();  // so it isn't released when it goes back...

         branch_set result = upsert<mode>({}, rref, key);
         if (result.count() == 1)
            _root.give(result.get_first_branch());
         else
            _root.give(make_inner(result));
         return _old_value_size;
      }
      void print() { print(_session.get_ref(_root.address())); }
      void validate() { validate(_session.get_ref(_root.address())); }
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
         uint64_t total_keys            = 0;
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
         stats s;
         calc_stats(s, *_root);
         return s;
      }

     private:
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
      template <upsert_mode mode>
      branch_set split_insert(const sal::alloc_hint& parent_hint,
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
      template <any_inner_node_type NodeType>
      void validate(smart_ref<NodeType> r, int depth = 0)
      {
         r->visit_branches([this](ptr_address br) { assert(_session.get_ref(br).ref() > 0); });
      }
      void validate(smart_ref<alloc_header> r, int depth = 0)
      {
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
      template <any_inner_node_type NodeType>
      void calc_stats(stats& s, smart_ref<NodeType> r, int depth = 0)
      {
         s.total_inner_node_size += r->size();
         s.clines += r->num_clines();
         s.branches += r->num_branches();
         s.branch_per_cline += double(r->num_branches()) / r->num_clines();
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
               s.leaf_nodes++;
               s.total_keys += r.as<leaf_node>()->num_branches();
               break;
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
         //SAL_INFO("upsert: r: {} ref: {}", r.address(), r.ref());
         if constexpr (mode.is_unique())
         {
            //SAL_INFO("  upsert: unique mode");
            if (r.ref() > 1)
            {
               return upsert<mode.make_shared()>(parent_hint, r, key);
            }
         }
         //         else if constexpr (mode.is_shared())
         //            SAL_INFO("  upsert: shared mode");
         //            ;

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
            assert(result.contains(r.address()));
         }
         else
         {
            //SAL_ERROR("upsert {} returned: {}", r.address(), result);
            // when inserting to an inner it may add a new branch without modifying the existing
            // one so we cannot "always release" here.
            //assert(not result.contains(r.address()));
            if (not result.contains(r.address())) [[likely]]
            {
               //SAL_ERROR("releasing result {}  pre ref: {}", r.address(), r.ref());
               r.release();
            }
         }

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
      // we need to split the node which will produce 2 branches
      // TODO: this split<> method assumes the parent handles inner_prefix differently...
      // but we don't do anything different with inner_prefix nodes,
      // merge_branches() has check for is_inner_prefix_node<InnerNodeType>
      auto [left, right] = split<mode>(parent_hint, in, br, sub_branches);
      // at this point both branches can be considered unique
      if (br < nb / 2)
      {
         smart_ref<inner_node> left_ref = _session.get_ref<inner_node>(left);
         // the new branch is on the left side
         auto left_result =
             merge_branches<upsert_mode::unique>(parent_hint, left_ref, br, sub_branches);
         left_result.push_back(in->divs()[nb / 2 - 1], right);
         return left_result;
      }
      else
      {
         smart_ref<inner_node> right_ref = _session.get_ref<inner_node>(right);
         // the new branch is on the right side
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
      // at this point we don't care about insert vs upsert so prevent code bloat
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
         if (in->can_apply(update)) [[likely]]
         {
            in.modify()->apply(update);
            return in.address();
         }
         else
         {
            if constexpr (is_inner_node<InnerNodeType>)
               return _session.realloc<InnerNodeType>(in, in.obj(), update).address();
            else if constexpr (is_inner_prefix_node<InnerNodeType>)
               return _session.realloc<InnerNodeType>(in, in->prefix(), in.obj(), update).address();
         }
      }
      else if constexpr (mode.is_shared())
      {
         if constexpr (is_inner_node<InnerNodeType>)
            return _session.alloc<InnerNodeType>(
                parent_hint, in.obj(),
                op::replace_branch(br, sub_branches, needed_clines, cline_indices));
         else if constexpr (is_inner_prefix_node<InnerNodeType>)
         {
            return _session.alloc<InnerNodeType>(
                parent_hint, in->prefix(), in.obj(),
                op::replace_branch(br, sub_branches, needed_clines, cline_indices));
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
            //          SAL_ERROR("        cpre  != in->prefix() '{}' != '{}'  key: {}", cpre, in->prefix(),
            //                   key);
            assert(in->prefix().size() >= 1);  // or else it should have been an inner_node
            if constexpr (mode.is_update())
               throw std::runtime_error("update: key does not exist");

            if (cpre.size() == 0)
            {
               //             SAL_WARN("adding peer without modifying this one");
               // return 2 branches to the parent node
               ptr_address new_leaf_addr =
                   _session.alloc<leaf_node>(parent_hint, key, make_value(_new_value, parent_hint));
               result.set_front(in->address());
               result.push_back(key[0], new_leaf_addr);
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
      auto          bref = _session.get_ref(badr);

      //SAL_INFO("in before updating branch '{}' address {} ptr: {}", br, badr, in.obj());
      //print(in, 10);

      if constexpr (mode.is_shared())
      {
         //         SAL_WARN("retaining children before releasing bref {}", bref.address());
         retain_children(in);  // all children have been copied to new node, retain them
         //SAL_ERROR("releasing bref {}", bref.address());
         //bref.release();  // the reference has been replaced because it was modified
      }

      // recursive upsert, give it this nodes clines as the parent hint
      branch_set sub_branches = upsert<mode>(in->get_branch_clines(), bref, key);
      //SAL_INFO("in after updating branch '{}' address {} ptr: {}", br, badr, in.obj());
      //print(in, 10);

      if constexpr (mode.is_shared())
      {
         /// TODO: fi sub_branches.count == 1, then in theory we could assert sbu_branches.contains
         // assert(not sub_branches.contains(badr));
         if (not sub_branches.contains(badr)) [[unlikely]]
         {
            //SAL_ERROR("releasing bref {}", bref.address());
            //bref.release();  // the reference has been replaced because it was modified
         }
      }

      // the happy path where there is nothing to do.
      if constexpr (mode.is_unique())
      {
         // assert(badr == bref->address());
         if (sub_branches.count() == 1) [[likely]]
         {
            if (bref->address() == sub_branches.get_first_branch())
               return in.address();
         }
         // else fall through to merge branches.
      }
      //      SAL_ERROR(" merge branches: {}", sub_branches);
      // integrate the sub_branches into the current node, and return the resulting branch set
      // to the parent node.
      return merge_branches<mode.make_shared_or_unique_only()>(parent_hint, in, br, sub_branches);
   }

   /**
   template <upsert_mode mode>
   branch_set tree_context::update(const sal::alloc_hint&      parent_hint,
                                   const smart_ref<leaf_node>& leaf,
                                   key_view                    key,
                                   branch_number               br)
   {
      throw std::runtime_error("update: not implemented");
   }
   */

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
               //SAL_INFO("insert: shared mode: new_leaf: {}", new_leaf);
               return new_leaf;
               //   [[likely]] return _session.alloc<leaf_node>(leaf.obj(), insert_op);
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
      //SAL_INFO("split_insert: leaf {} key: {}", leaf.address(), key);
      branch_set           result;
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
         //         SAL_WARN("alloc left hint: {}", format(parent_hint));
         left =
             _session.alloc<leaf_node>(parent_hint, leaf.obj(), key_view(), branch_zero, left_size);
      }

      // there is a rare case with a root node having no parent_hint where this causes
      // left and right not to share a cline.
      ptr_address right =
          _session.alloc<leaf_node>(parent_hint, leaf.obj(), key_view(), left_size, right_end);
      // _session.alloc<leaf_node>({&left, 1}, leaf.obj(), key_view(), left_size, right_end);
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
      if constexpr (mode.is_update())
      {
         branch_number br = leaf->get(key);
         if (br == leaf->num_branches()) [[unlikely]]
            throw std::runtime_error("update: key does not exist");
         return update<mode>(parent_hint, leaf, key, br);
      }
      else
      {
         branch_number lb = leaf->lower_bound(key);
         if constexpr (mode.is_insert())
         {
            if (not(lb == leaf->num_branches() or leaf->get_key(lb) != key)) [[unlikely]]
               throw std::runtime_error("insert: key already exists");
            return insert<mode>(parent_hint, leaf, key, lb);
         }
         else if constexpr (mode.is_upsert())
         {
            if (lb != leaf->num_branches())
               return update<mode>(parent_hint, leaf, key, lb);
            else
            {
               if (leaf->get_key(lb) == key)
                  return update<mode>(parent_hint, leaf, key, lb);
               else
                  return insert<mode>(parent_hint, leaf, key, lb);
            }
         }
      }
      std::unreachable();
   }

}  // namespace psitri
