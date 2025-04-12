#pragma once
#include <sal/allocator.hpp>
#include <sal/shared_ptr_alloc.hpp>

namespace sal
{
   class allocator_session;

   /**
    * a reference counted managed ptr_address that ensures the underlying, non-movable,
    * shared_ptr is retained and released as this object is copied and destroyed. Think
    * of sal::shared_ptr as the internal data of std::shared_ptr and shared_ptr_alloc is
    * the "heap" allocator for the internal state of smart_ref. 
    * 
    * This class has similar thread-safety properties as std::shared_ptr in that while the
    * internal shared_ptr state is atomic, the smart_ref itself is not thread safe. Each smart
    * ref contains a reference to an sal::allocator_session which will be used to release the
    * underlying object pointed to by the ptr_address (eg raw pointer to sal::shared_ptr). This
    * will in trun determine the context in which the object's dynamic destructor 
    * (aka alloc_header::destory) is called and any child objects can be released along with it.
    *
    * To send a smart_ref to another thread (or allocator_session) you use smart_ref::send_to(other session)
    * which must be called from the original allocator_session. In this way the origianl session 
    * can "sync" and make its state mprotect()'ed and immutable so that if and when the receiver
    * thread/session attempts to sync() it knows that all data that is referenced by it has been
    * synced. In effect, you must commit data before transfering ownership to another session/thread.
    */
   class smart_ref
   {
     public:
      // assumes responsibility for releasing adr, does not increment reference count
      smart_ref(ptr_address adr, allocator_ptr alloc) noexcept : _adr(adr), _alloc(std::move(alloc))
      {
      }
      // copies another smart_ref and increments the reference count
      smart_ref(const smart_ref& other) noexcept : _adr(other._adr), _alloc(std::move(other._alloc))
      {
         retain();
      }

      smart_ref& operator=(const smart_ref& other) noexcept
      {
         if (this == &other) [[unlikely]]
            return *this;
         return give(other.address());
      }

      smart_ref& operator=(smart_ref&& other) noexcept
      {
         if (this == &other)
            return *this;
         return give(other.take());
      }

      // return the ptr_address of the underlying shared_ptr
      ptr_address address() const noexcept { return _adr; }
      bool        is_valid() const noexcept { return _adr != ptr_address(); }
      operator bool() const noexcept { return is_valid(); }

      /**
       * Takes ownership of the underlying ptr_address and returns it,
       * the caller is responsible for releasing the ptr_address.
       */
      ptr_address take() noexcept
      {
         auto tmp = _adr;
         _adr     = ptr_address();
         return tmp;
      }
      /**
       * Give this smart_ref the responsibility of releasing the given_adr
       */
      smart_ref& give(ptr_address given_adr) noexcept
      {
         release();
         _adr = given_adr;
         return *this;
      }
      /**
       * Effectively equivalent to give( ptr_address() ) or assigning null
       */
      void reset() noexcept { release(); }

      object_ref get() noexcept { return _alloc->get(_adr); }

     private:
      void        retain() noexcept { _alloc->retain(_adr); }
      void        release() noexcept { _alloc->release(_adr); }
      ptr_address _adr;
      allocator*  _alloc;
   };

}  // namespace sal
