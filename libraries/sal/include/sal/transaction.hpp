#pragma once
#include <sal/allocator_session.hpp>
#include <sal/config.hpp>
#include <sal/smart_ptr.hpp>

namespace sal
{

   class transaction
   {
     public:
      transaction(allocator_session_ptr session, root_object_number ro) : _session(session), _ro(ro)
      {
         _adr = smart_ptr<alloc_header>(session, session->get_allocator().start_transaction(ro));
      }
      ~transaction() noexcept
      {
         if (*_ro == null_root_index)
            return;
         abort();
      }

      /**
       * Commits the transaction and returns the previous root object, 
       * the caller can ignore the return value to have it cleaned up
       * automatically.
       * 
       * @return The previous root object.
       */
      smart_ptr<alloc_header> commit(sync_type t = sync_type::default_sync_type) noexcept
      {
         assert(_ro != null_root_index);
         auto result = _session->transaction_commit(_ro, std::move(_adr), t);
         _ro         = null_root_index;
         return result;
      }

      /**
       * Aborts the transaction and releases the root object.
       */
      void abort() noexcept
      {
         if (*_ro == null_root_index)
            return;
         _session->transaction_abort(_ro);
         _adr.give(null_ptr_address);
         _ro = null_root_index;
      }

      const smart_ptr<alloc_header>& root() const noexcept
      {
         assert(_ro != null_root_index);
         return _adr;
      }
      smart_ptr<alloc_header>& root() noexcept
      {
         assert(_ro != null_root_index);
         return _adr;
      }
      void set_root(smart_ptr<alloc_header> adr) noexcept
      {
         assert(_ro != null_root_index);
         _adr = std::move(adr);
      }

     private:
      transaction(const transaction&)            = delete;
      transaction& operator=(const transaction&) = delete;
      transaction(transaction&& other) noexcept
          : _session(other._session), _ro(other._ro), _adr(std::move(other._adr))
      {
         other._ro = null_root_index;
      }
      transaction& operator=(transaction&& other) noexcept
      {
         _session  = other._session;
         _ro       = other._ro;
         _adr      = std::move(other._adr);
         other._ro = null_root_index;
         return *this;
      }

      allocator_session_ptr   _session;
      root_object_number      _ro;
      smart_ptr<alloc_header> _adr;
   };
}  // namespace sal
