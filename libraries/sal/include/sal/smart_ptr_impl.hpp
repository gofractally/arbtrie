#pragma once
#include <sal/allocator_impl.hpp>
#include <sal/smart_ptr.hpp>

namespace sal
{
   inline bool smart_ptr_base::is_read_only() const noexcept
   {
      if (is_valid())
         return _asession->is_read_only(_adr);
      return true;
   }

   inline shared_smart_ptr_base::shared_smart_ptr_base(smart_ptr_base ptr) noexcept
   {
      assert(ptr.is_read_only());
      if (ptr.is_valid())
      {
         ptr.retain();
         _internal = std::make_shared<internal>(ptr.address(),
                                                ptr.session()->get_allocator().shared_from_this());
      }
   }

   inline smart_ptr_base shared_smart_ptr_base::get() const noexcept
   {
      if (_internal)
         return smart_ptr_base(_internal->_allocator->get_session(), _internal->_ptr, true);
      return {};
   }

   inline shared_smart_ptr_base& shared_smart_ptr_base::operator=(const smart_ptr_base& ptr) noexcept
   {
      assert(ptr.is_read_only());
      if (_internal)
         _internal->_allocator->release(_internal->_ptr);
      if (ptr.is_valid())
         _internal = std::make_shared<internal>(ptr.address(),
                                                ptr.session()->get_allocator().shared_from_this());
      else
         _internal.reset();
      return *this;
   }

}  // namespace sal
