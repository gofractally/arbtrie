#pragma once
#include <sal/alloc_header.hpp>
#include <sal/allocator_session.hpp>
#include <sal/control_block.hpp>
#include <sal/control_block_alloc.hpp>

namespace sal
{
   class read_lock;
   class seg_allocator;
   class seg_alloc_session;
   class alloc_header;
   class sync_header;
   class node_header;

   template <typename T>
   class modify_guard;

   template <typename T>
   class smart_ref;

   class smart_ptr_base
   {
     public:
      smart_ptr_base(allocator_session_ptr asession, ptr_address adr)
          : _asession(asession), _adr(adr)
      {
      }
      smart_ptr_base() : _asession(nullptr), _adr(null_ptr_address) {}

      ptr_address address() const noexcept { return _adr; }
      bool        is_valid() const noexcept { return _adr != null_ptr_address; }

      smart_ptr_base(const smart_ptr_base& other) : _asession(other._asession), _adr(other._adr)
      {
         retain();
      }

      smart_ptr_base& operator=(const smart_ptr_base& other)
      {
         if (this == &other)
            return *this;
         release();
         _asession = other._asession;
         _adr      = other._adr;
         retain();
         return *this;
      }

      ptr_address take()
      {
         auto tmp = _adr;
         _adr     = null_ptr_address;
         return tmp;
      }

      smart_ptr_base& give(ptr_address given_adr)
      {
         release();
         _adr = given_adr;
         return *this;
      }

      void retain()
      {
         if (_adr != null_ptr_address)
            _asession->retain(_adr);
      }

      void release()
      {
         if (_adr != null_ptr_address)
         {
            _asession->release(_adr);
            _adr = null_ptr_address;
         }
      }

     protected:
      allocator_session_ptr _asession;
      ptr_address           _adr;
   };
   template <typename T = alloc_header>
   class smart_ptr : public smart_ptr_base
   {
     public:
      smart_ptr(allocator_session_ptr asession, ptr_address adr) : smart_ptr_base(asession, adr) {}

      smart_ref<T> operator->() const noexcept { return _asession->get<T>(_adr); }
      smart_ref<T> operator*() const noexcept { return _asession->get<T>(_adr); }
   };

   class smart_ref_base
   {
      template <typename T>
      friend class modify_guard;

     public:
      ptr_address         address() const noexcept { return _obj->address(); }
      uint32_t            ref() const noexcept { return _cached.ref; }
      location            loc() const noexcept { return _cached.loc(); }
      const alloc_header* obj() const noexcept { return _obj; }

      // return false if ref count overflow
      control_block_data retain() noexcept { return _cached = _control.retain(); }
      void               release() noexcept { return _asession->release(_obj->address()); }

      template <typename Type>
      const smart_ref<Type>& as() const
      {
         assert(Type::type_id == _obj->type());
         return *reinterpret_cast<const smart_ref<Type>*>(this);
      }
      template <typename Type>
      smart_ref<Type>& as()
      {
         assert(Type::type_id == _obj->type());
         return *reinterpret_cast<smart_ref<Type>*>(this);
      }

      const allocator_session_ptr& session() const noexcept { return _asession; }

      control_block& control() const noexcept { return _control; }

     protected:
      void maybe_update_read_stats(uint32_t size) const;
      friend class read_lock;
      template <typename T>
      friend class modify_guard;

      smart_ref_base(allocator_session_ptr asession,
                     alloc_header*         obj,
                     control_block&        control,
                     control_block_data    cached)
          : _obj(obj), _control(control), _cached(cached), _asession(asession)
      {
      }

      alloc_header*         _obj;
      control_block&        _control;
      control_block_data    _cached;  // cached read of atomic _ptr
      allocator_session_ptr _asession;
   };  // smart_ref

   template <typename T>
   class smart_ref : public smart_ref_base
   {
     public:
      using smart_ref_base::smart_ref_base;
      smart_ref(allocator_session_ptr asession,
                T*                    obj,
                control_block&        control,
                control_block_data    cached)
          : smart_ref_base(asession, obj, control, cached)
      {
         assert(T::type_id == _obj->type());
      }

      const T* obj() const noexcept { return reinterpret_cast<const T*>(_obj); }
      const T* operator->() const noexcept { return obj(); }

      modify_guard<T> modify();
      void            modify(auto&& update_fn);
   };

   template <typename T>
   class modify_guard
   {
     public:
      modify_guard(smart_ref<T>& obj) : _obj(obj), _observed_ptr(nullptr) {}
      T* operator->() { return get(); }
      T* get();
      ~modify_guard();

     private:
      smart_ref<T>& _obj;
      T*            _observed_ptr = nullptr;
   };  // modify_guard

   template <typename T>
   modify_guard<T> smart_ref<T>::modify()
   {
      return modify_guard<T>(*this);
   }
   template <typename T>
   void smart_ref<T>::modify(auto&& update_fn)
   {
      auto guard = modify<T>(this);
      update_fn(guard.get());
   }

   template <typename T>
   T* modify_guard<T>::get()
   {
      if (_observed_ptr)
         return _observed_ptr;
      if (_obj._asession->can_modify(_obj._cached.loc()))
         return _observed_ptr = _obj._obj;

      return _observed_ptr = _obj._asession->copy_on_write(_obj);
   }
   template <typename T>
   modify_guard<T>::~modify_guard()
   {
      if (_observed_ptr) [[likely]]
      {
         if (_obj._asession->config_update_checksum_on_modify())
            _observed_ptr->update_checksum();
         else
            _observed_ptr->clear_checksum();
      }
      // perform post modify actions here, like update checksum
   }

}  // namespace sal
