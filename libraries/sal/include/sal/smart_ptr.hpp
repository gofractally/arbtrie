#pragma once
#include <sal/alloc_header.hpp>
#include <sal/allocator.hpp>
#include <sal/allocator_session.hpp>
#include <sal/control_block.hpp>
#include <sal/control_block_alloc.hpp>
#include <type_traits>

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
      /**
       * @brief smart_ptr_base takes ownership of the ptr_address and will release it 
       * in its destructor without incrementing the reference count. The caller is giving
       * up its reference to the ptr_address.
       */
      smart_ptr_base(allocator_session_ptr asession, ptr_address adr) noexcept
          : _asession(asession), _adr(adr), _ver(null_ptr_address)
      {
      }
      /**
       * @brief smart_ptr_base takes ownership of the ptr_address and will release it
       * in its destructor, but can optionally take a copy and increment the reference count such
       * that the caller also retains ownership of the ptr_address.
       */
      smart_ptr_base(allocator_session_ptr asession, ptr_address adr, bool inc_ref) noexcept
          : _asession(asession), _adr(adr), _ver(null_ptr_address)
      {
         if (inc_ref)
            retain();
      }
      smart_ptr_base(allocator_session_ptr asession, tree_id tid, bool inc_ref) noexcept
          : _asession(asession), _adr(tid.root), _ver(tid.ver)
      {
         if (inc_ref)
            retain();
      }
      smart_ptr_base() noexcept : _asession(nullptr), _adr(null_ptr_address), _ver(null_ptr_address) {}

      ~smart_ptr_base() noexcept
      {
         if (_adr != null_ptr_address || _ver != null_ptr_address)
         {
            release();
         }
      }

      operator bool() const noexcept { return is_valid(); }
      ptr_address address() const noexcept { return _adr; }
      bool        is_valid() const noexcept { return _adr != null_ptr_address; }
      bool        is_read_only() const noexcept;

      smart_ptr_base(const smart_ptr_base& other) noexcept
          : _asession(other._asession), _adr(other._adr), _ver(other._ver)
      {
         retain();
      }
      smart_ptr_base(smart_ptr_base&& other) noexcept
          : _asession(other._asession), _adr(other._adr), _ver(other._ver)
      {
         other._adr = null_ptr_address;
         other._ver = null_ptr_address;
      }

      smart_ptr_base& operator=(const smart_ptr_base& other) noexcept
      {
         if (this == &other)
            return *this;
         if (_adr == other._adr && _ver == other._ver)
            return *this;
         release();
         _asession = other._asession;
         _adr      = other._adr;
         _ver      = other._ver;
         retain();
         return *this;
      }
      smart_ptr_base& operator=(smart_ptr_base&& other) noexcept
      {
         if (this == &other)
            return *this;
         release();
         _asession  = std::move(other._asession);
         _adr       = other._adr;
         _ver       = other._ver;
         other._adr = null_ptr_address;
         other._ver = null_ptr_address;
         return *this;
      }

      ptr_address take() noexcept
      {
         auto tmp = _adr;
         _adr     = null_ptr_address;
         return tmp;
      }

      /// Take ownership of both root and ver, returning as tree_id.
      /// Nulls both _adr and _ver so destructor won't release them.
      tree_id take_tree_id() noexcept
      {
         tree_id tid{_adr, _ver};
         _adr = null_ptr_address;
         _ver = null_ptr_address;
         return tid;
      }

      smart_ptr_base& give(ptr_address given_adr) noexcept
      {
         release();
         _adr = given_adr;
         return *this;
      }

      void retain() noexcept
      {
         if (_adr != null_ptr_address)
            _asession->retain(_adr);
         if (_ver != null_ptr_address)
            _asession->retain(_ver);
      }

      void release() noexcept
      {
         if (_adr != null_ptr_address)
         {
            _asession->release(_adr);
            _adr = null_ptr_address;
         }
         if (_ver != null_ptr_address)
         {
            _asession->release(_ver);
            _ver = null_ptr_address;
         }
      }
      const allocator_session_ptr& session() const noexcept { return _asession; }

      ptr_address ver() const noexcept { return _ver; }
      void        set_ver(ptr_address v) noexcept { _ver = v; }
      tree_id     get_tree_id() const noexcept { return {_adr, _ver}; }

     protected:
      allocator_session_ptr _asession;
      ptr_address           _adr;
      ptr_address           _ver = null_ptr_address;
   };

   /**
    * @brief automatically tracks the reference count of a ptr_address and
    * releases the object when the reference count goes to zero. 
    * 
    * This class is similar to std::shared_ptr, but uses a custom allocator and
    * is aware of the potential for objects to be moved and write protected. When
    * it is dereferenced it returns a smart_ref with the current pointer to the
    * object, if a modification is needed the smart_ref can also return a
    * modify_guard to the object that will perform automatic copy on write if
    * needed.
    *
    * @note **Do not utilize the raw pointers beyond the lifetime of the return
    * values of this class.**
    */
   template <typename T = alloc_header>
   class smart_ptr : public smart_ptr_base
   {
     public:
      smart_ptr(allocator_session_ptr asession, ptr_address adr, bool retain = false) noexcept
          : smart_ptr_base(asession, adr, retain)
      {
      }
      smart_ptr(allocator_session_ptr asession, tree_id tid, bool inc_ref = false) noexcept
          : smart_ptr_base(asession, tid, inc_ref)
      {
      }
      smart_ptr(const smart_ptr& other) noexcept : smart_ptr_base(other) {}
      smart_ptr() noexcept : smart_ptr_base() {}
      smart_ptr(smart_ptr&& other) noexcept : smart_ptr_base(std::move(other)) {}
      smart_ptr& operator=(const smart_ptr& other) noexcept
      {
         smart_ptr_base::operator=(other);
         return *this;
      }
      smart_ptr& operator=(smart_ptr&& other) noexcept
      {
         smart_ptr_base::operator=(std::move(other));
         return *this;
      }

      template <typename U>
      smart_ptr(const smart_ptr<U>& other) noexcept : smart_ptr_base(other)
      {
         static_assert(std::is_base_of_v<T, U>, "U must be derived from T");
      }
      template <typename U>
      smart_ptr(smart_ptr<U>&& other) noexcept : smart_ptr_base(std::move(other))
      {
         static_assert(std::is_base_of_v<T, U>, "U must be derived from T");
      }
      template <typename U>
      smart_ptr& operator=(const smart_ptr<U>& other) noexcept
      {
         static_assert(std::is_base_of_v<T, U>, "U must be derived from T");
         smart_ptr_base::operator=(other);
         return *this;
      }
      template <typename U>
      smart_ptr& operator=(smart_ptr<U>&& other) noexcept
      {
         static_assert(std::is_base_of_v<T, U>, "U must be derived from T");
         smart_ptr_base::operator=(std::move(other));
         return *this;
      }

      template <typename U>
      smart_ref<U> as() const noexcept
      {
         static_assert(std::is_base_of_v<T, U>, "U must be derived from T");
         return _asession->get_ref<U>(_adr);
      }

      smart_ref<T> operator->() const noexcept { return _asession->get_ref<T>(_adr); }
      smart_ref<T> operator*() const noexcept { return _asession->get_ref<T>(_adr); }
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
         assert(int(Type::type_id) == int(alloc_header::type_id) or
                int(Type::type_id) == int(_obj->type()));
         return *reinterpret_cast<const smart_ref<Type>*>(this);
      }
      template <typename Type>
      smart_ref<Type>& as()
      {
         assert(int(Type::type_id) == int(alloc_header::type_id) or
                int(Type::type_id) == int(_obj->type()));
         return *reinterpret_cast<smart_ref<Type>*>(this);
      }

      const allocator_session_ptr& session() const noexcept { return _asession; }

      control_block& control() const noexcept { return _control; }

     protected:
      void maybe_update_read_stats(uint32_t size) const noexcept;
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

   struct shared_smart_ptr_base
   {
     public:
      shared_smart_ptr_base(smart_ptr_base ptr) noexcept;
      shared_smart_ptr_base(const shared_smart_ptr_base& other) noexcept
          : _internal(other._internal)
      {
         if (_internal)
         {
            _internal->_allocator->retain(_internal->_ptr);
            if (_internal->_ver != null_ptr_address)
               _internal->_allocator->retain(_internal->_ver);
         }
      }
      shared_smart_ptr_base(shared_smart_ptr_base&& other) noexcept
          : _internal(std::move(other._internal))
      {
      }

      ~shared_smart_ptr_base() noexcept
      {
         if (_internal)
         {
            _internal->_allocator->release(_internal->_ptr);
            if (_internal->_ver != null_ptr_address)
               _internal->_allocator->release(_internal->_ver);
         }
      }
      operator bool() const noexcept { return _internal != nullptr; }
      smart_ptr_base get() const noexcept;

      smart_ptr_base operator->() const noexcept { return get(); }
      smart_ptr_base operator*() const noexcept { return get(); }

      shared_smart_ptr_base& operator=(const smart_ptr_base& ptr) noexcept;
      shared_smart_ptr_base& operator=(const shared_smart_ptr_base& other) noexcept
      {
         if (this != &other)
         {
            if (_internal)
            {
               _internal->_allocator->release(_internal->_ptr);
               if (_internal->_ver != null_ptr_address)
                  _internal->_allocator->release(_internal->_ver);
            }
            _internal = other._internal;
            if (_internal)
            {
               _internal->_allocator->retain(_internal->_ptr);
               if (_internal->_ver != null_ptr_address)
                  _internal->_allocator->retain(_internal->_ver);
            }
         }
         return *this;
      }
      shared_smart_ptr_base& operator=(shared_smart_ptr_base&& other) noexcept
      {
         if (_internal)
         {
            _internal->_allocator->release(_internal->_ptr);
            if (_internal->_ver != null_ptr_address)
               _internal->_allocator->release(_internal->_ver);
         }
         _internal = std::move(other._internal);
         return *this;
      }

     private:
      struct internal
      {
         ptr_address                _ptr;
         ptr_address                _ver;
         std::shared_ptr<allocator> _allocator;
      };
      std::shared_ptr<internal> _internal;
   };

   /**
    * @breif shared_smart_ptr is a smart pointer that can be shared between threads.

    * Do not pass @ref smart_ptr<T> between threads because it stores a pointer to a thread
    * local allocator_session, but shared_smart_ptr stores a pointer to a shared_ptr<allocator> 
    * and dynamically looks up the proper allocator_session for the current thread.
    * 
    * Furthermore, when creating a shared_smart_ptr the source thread must commit its changes
    * and ensure that the object pointed to is read-only because no other thread is allowed to
    * see memory from a session until it is committed or it would risk undefined behavior of
    * one thread reading what another is updating.
    * 
    * Therefore, if the object pointed at is not read-only, then this will assert() in debug mode
    * as this is a programmer error. 
    */
   template <typename T>
   class shared_smart_ptr : public shared_smart_ptr_base
   {
     public:
      shared_smart_ptr(smart_ptr<T> ptr = {}) noexcept : shared_smart_ptr_base(ptr) {}
      smart_ptr<T> get() const noexcept
      {
         auto base = shared_smart_ptr_base::get();
         return smart_ptr<T>(base.session(), base.take(), false);
      }
      smart_ptr<T> operator->() const noexcept { return get(); }
   };

   template <typename T>
   class smart_ref : public smart_ref_base
   {
     public:
      using smart_ref_base::smart_ref_base;
      smart_ref(allocator_session_ptr asession,
                T*                    obj,
                control_block&        control,
                control_block_data    cached) noexcept
          : smart_ref_base(asession, obj, control, cached)
      {
         assert(uint8_t(T::type_id) == uint8_t(alloc_header::type_id) or
                uint8_t(T::type_id) == uint8_t(_obj->type()));
      }

      const T*        obj() const noexcept { return reinterpret_cast<const T*>(_obj); }
      const T*        operator->() const noexcept { return obj(); }
      const T&        operator*() const noexcept { return *obj(); }
      modify_guard<T> modify() noexcept;
      void modify(auto&& update_fn) noexcept(noexcept(update_fn(std::declval<T*>())));

      friend class allocator_session;
   };

   template <typename T>
   class modify_guard
   {
     public:
      modify_guard(smart_ref<T>& obj) noexcept : _obj(obj), _observed_ptr(nullptr) {}
      T* operator->() noexcept { return get(); }
      T* get() noexcept;
      ~modify_guard() noexcept;

     private:
      smart_ref<T>& _obj;
      T*            _observed_ptr = nullptr;
   };  // modify_guard

   template <typename T>
   modify_guard<T> smart_ref<T>::modify() noexcept
   {
      return modify_guard<T>(*this);
   }
   template <typename T>
   void smart_ref<T>::modify(auto&& update_fn)
       noexcept(noexcept(update_fn(std::declval<T*>())))
   {
      auto guard = modify();
      update_fn(guard.get());
   }

   template <typename T>
   T* modify_guard<T>::get() noexcept
   {
      if (_observed_ptr)
         return _observed_ptr;
      if (_obj._asession->can_modify(_obj._cached.loc()))
         return _observed_ptr = static_cast<T*>(_obj._obj);

      return _observed_ptr = _obj._asession->copy_on_write(_obj);
   }
   template <typename T>
   modify_guard<T>::~modify_guard() noexcept
   {
      if (_observed_ptr) [[likely]]
      {
         // if (_obj._asession->config_update_checksum_on_modify())
         //    _observed_ptr->update_checksum();
         // else
         _observed_ptr->clear_checksum();
      }
      // perform post modify actions here, like update checksum
   }

}  // namespace sal
