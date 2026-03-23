#pragma once
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/node.hpp>
#include <psitri/node/value.hpp>

namespace psitri
{
   template <typename T>
   concept node_ptr_type = std::same_as<T, const node*> || std::same_as<T, node*>;

   template <typename T>
   concept is_leaf_node = std::same_as<std::remove_cv_t<std::remove_pointer_t<T>>, leaf>;

   template <typename T>
   concept is_inner_node = std::same_as<std::remove_cv_t<std::remove_pointer_t<T>>, inner>;

   template <typename T>
   concept is_value_node = std::same_as<std::remove_cv_t<std::remove_pointer_t<T>>, value>;

   template <typename T>
   concept any_node_type = is_inner_node<T> || is_leaf_node<T> || is_value_node<T>;

   template <node_ptr_type NodeType>
   auto cast_and_call(node_type t, NodeType n, auto&& func) noexcept(
       noexcept(func(std::declval<transcribe_const_t<NodeType, inner>*>())) &&
       noexcept(func(std::declval<transcribe_const_t<NodeType, leaf>*>())) &&
       noexcept(func(std::declval<transcribe_const_t<NodeType, value>*>())))
   {
      switch (t)
      {
         case node_type::inner:
            return func(static_cast<transcribe_const_t<NodeType, inner>*>(n));
         case node_type::leaf:
            return func(static_cast<transcribe_const_t<NodeType, leaf>*>(n));
         case node_type::value:
            return func(static_cast<transcribe_const_t<NodeType, value>*>(n));
         default:
            assert(!"unknown node type");
            abort();
      }
      std::unreachable();
   }
   template <node_ptr_type NodeType>
   auto cast_and_call(NodeType n, auto&& func) noexcept(
       noexcept(func(std::declval<transcribe_const_t<NodeType, inner>*>())) &&
       noexcept(func(std::declval<transcribe_const_t<NodeType, leaf>*>())) &&
       noexcept(func(std::declval<transcribe_const_t<NodeType, value>*>())))
   {
      return cast_and_call(n->type(), n, std::forward<decltype(func)>(func));
   }

   /**
    * Sometimes to prevent prefetching things we won't need we need to hide
    * the dereference of n until the last possible moment by ensuring this method
    * does not get inlined.
    */
   template <node_ptr_type NodeType>
   __attribute__((noinline)) auto cast_and_call_noinline(NodeType n, auto&& func) noexcept(
       noexcept(func(std::declval<transcribe_const_t<NodeType, inner>*>())) &&
       noexcept(func(std::declval<transcribe_const_t<NodeType, leaf>*>())) &&
       noexcept(func(std::declval<transcribe_const_t<NodeType, value>*>())))
   {
      return cast_and_call(n->type(), n, std::forward<decltype(func)>(func));
   }

   template <typename NodeType>
   inline auto cast_and_call(NodeType n, auto&& func) noexcept(
       noexcept(func(std::declval<transcribe_const_t<NodeType, inner>*>())) &&
       noexcept(func(std::declval<transcribe_const_t<NodeType, leaf>*>())) &&
       noexcept(func(std::declval<transcribe_const_t<NodeType, value>*>())))
   {
   }

   template <node_ptr_type NodeType>
   void retain_children(NodeType n, auto& state)
   {
      cast_and_call(n,
                    [](auto&& p) noexcept
                    {
                       auto clines = p->get_branch_clines();
                       for (auto cline : clines)
                          if (cline) [[likely]]
                             __builtin_prefetch(state.get_node(cline), 1, 3);
                       p->visit_branches([&](auto adr) { state.get_ptr(adr).retain(); });
                    });
   }

}  // namespace psitri