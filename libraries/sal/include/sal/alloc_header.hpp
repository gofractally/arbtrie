#pragma once
#include <hash/xxhash.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <sal/allocator_session.hpp>
#include <sal/time.hpp>
#include <ucc/fast_memcpy.hpp>
#include <vector>

namespace sal
{
   enum class header_type : uint8_t
   {
      undefined       = 0,
      alloc_head      = 1,
      sync_head       = 2,
      start_user_type = 3,              // user types here
      max_user_type   = ((1 << 7) - 1)  // 124 user types
   };

   /**
    * Base class for all allocatable objects.
    *
    * Every object has a ptr_address which is used to track its current location,
    * and is utilized when rebuilding the state from a system crash.
    *
    * The size of the object is measured in 64 byte cachelines, and the type is
    * used to identify the type of the object and is extended by derived types.
    *
    * The app_data field is used to store application specific metadata, and is
    * extended by derived types, e.g. psitri uses this to store the number of branches.
    */
   class alloc_header
   {
     public:
      static constexpr header_type type_id = header_type::alloc_head;
      inline constexpr header_type type() const noexcept { return (header_type)_type; };
      inline constexpr uint32_t    size() const noexcept { return _size; }
      inline constexpr uint64_t    checksum() const noexcept { return _checksum; }

      /**
       * How much space to allocate for a copy on write
       */
      uint32_t cow_size() const noexcept { return size(); }

      const alloc_header* next() const noexcept
      {
         return reinterpret_cast<const alloc_header*>(((char*)this) + size());
      }

      alloc_header(uint32_t asize, header_type t, ptr_address_seq seq) noexcept
          : _checksum(0), _address(seq), _size(asize), _type((uint8_t)t)
      {
      }
      alloc_header() noexcept : alloc_header(0, header_type::undefined, {}) {}
      constexpr ptr_address address() const noexcept { return _address.address; }

      /// the allocation sequence associated with address(), used to determine priority
      /// during recovery from a crash.
      constexpr uint16_t sequence() const noexcept { return _address.sequence; }

      void     update_checksum() noexcept { _checksum = calculate_checksum(); }
      uint16_t calculate_checksum() const noexcept
      {
         return XXH3_64bits((const char*)&_address, _size - sizeof(_checksum));
      }
      bool verify_checksum() const noexcept
      {
         return !_checksum or _checksum == calculate_checksum();
      }
      bool has_checksum() const noexcept { return _checksum != 0; }
      void clear_checksum() noexcept { _checksum = 0; }

      ptr_address_seq address_seq() const noexcept { return _address; }

      uint32_t compact_size() const noexcept { return size(); }
      void     compact_to(alloc_header* compact_dst) const noexcept
      {
         assert(compact_dst->size() == size());
         ucc::memcpy_aligned_64byte(compact_dst, this, size());
      }
      /**
       * dst->size() should be cow_size(src)
       */
      void copy_to(alloc_header* dst) const noexcept
      {
         assert(dst->size() == size());
         ucc::memcpy_aligned_64byte(dst, this, size());
      }
      void destroy(const allocator_session_ptr&) const noexcept {}

      /**
       * calls visitor for each child of this object
       */
      void visit_children(const std::function<void(ptr_address)>&) const noexcept {}

     protected:
      void set_checksum(uint16_t c) noexcept { _checksum = c; }
      void init(uint32_t asize, header_type t, ptr_address_seq seq) noexcept
      {
         _checksum = 0;
         _address  = seq;
         _size     = asize;
         _type     = (uint8_t)t;
      }

     private:
      uint16_t        _checksum;
      ptr_address_seq _address;
      uint32_t        _size : 25;  // able to store segment_size -1 (32MB)
      uint32_t        _type : 7;   // 127 types, 3 reserved for internal use
   } __attribute__((packed));
   static_assert(sizeof(alloc_header) == 12);

   class pending_release_list
   {
     public:
      explicit pending_release_list(std::vector<ptr_address>& storage) noexcept
          : _storage(&storage)
      {
         _storage->clear();
      }

      bool push(ptr_address address) noexcept
      {
         try
         {
            _storage->push_back(address);
            return true;
         }
         catch (...)
         {
            _failed = true;
            return false;
         }
      }

      std::size_t size() const noexcept { return _storage->size(); }
      bool        empty() const noexcept { return _storage->empty(); }
      bool        failed() const noexcept { return _failed; }

      ptr_address operator[](std::size_t idx) const noexcept
      {
         assert(idx < _storage->size());
         return (*_storage)[idx];
      }

      const ptr_address* begin() const noexcept
      {
         return _storage->empty() ? nullptr : _storage->data();
      }
      const ptr_address* end() const noexcept
      {
         return _storage->empty() ? nullptr : _storage->data() + _storage->size();
      }

     private:
      std::vector<ptr_address>* _storage = nullptr;
      bool                      _failed  = false;
   };

   class object_type_ops
   {
     public:
      virtual ~object_type_ops() = default;

      virtual uint32_t cow_size(const alloc_header* header) const noexcept = 0;
      virtual uint32_t compact_size(const alloc_header* header) const noexcept = 0;
      virtual bool     has_checksum(const alloc_header* header) const noexcept = 0;
      virtual bool     verify_checksum(const alloc_header* header) const noexcept = 0;
      virtual void     update_checksum(alloc_header* header) const noexcept = 0;
      virtual void     active_compact_to(const alloc_header*          src,
                                         alloc_header*                compact_dst,
                                         const allocator_session_ptr& session) const noexcept = 0;
      virtual void     passive_compact_to(const alloc_header*    src,
                                          alloc_header*          compact_dst,
                                          pending_release_list&  pending_releases) const noexcept = 0;
      virtual void     active_copy_to(const alloc_header*          src,
                                      alloc_header*                dst,
                                      const allocator_session_ptr& session) const noexcept = 0;
      virtual void     passive_copy_to(const alloc_header*   src,
                                       alloc_header*         dst,
                                       pending_release_list& pending_releases) const noexcept = 0;
      virtual void     destroy(const alloc_header*          header,
                               const allocator_session_ptr& session) const noexcept = 0;
      virtual void visit_children(
          const alloc_header*                     header,
          const std::function<void(ptr_address)>& visitor) const noexcept = 0;
   };

   /**
    * Adapts a concrete C++ alloc_header-derived type to the SAL runtime object
    * operations interface. Instances of this adapter live beside an allocator
    * or database; persisted segment data stores only type ids, never vptrs.
    */
   template <typename T>
   class static_object_type_ops : public object_type_ops
   {
     public:
      static constexpr uint8_t type_id = uint8_t(T::type_id);

      uint32_t cow_size(const alloc_header* header) const noexcept override
      {
         return static_cast<const T*>(header)->cow_size();
      }
      uint32_t compact_size(const alloc_header* header) const noexcept override
      {
         return static_cast<const T*>(header)->compact_size();
      }
      bool has_checksum(const alloc_header* header) const noexcept override
      {
         return static_cast<const T*>(header)->has_checksum();
      }
      bool verify_checksum(const alloc_header* header) const noexcept override
      {
         return static_cast<const T*>(header)->verify_checksum();
      }
      void update_checksum(alloc_header* header) const noexcept override
      {
         static_cast<T*>(header)->update_checksum();
      }
      void active_compact_to(const alloc_header*          src,
                             alloc_header*                compact_dst,
                             const allocator_session_ptr&) const noexcept override
      {
         static_cast<const T*>(src)->compact_to(compact_dst);
      }
      void passive_compact_to(const alloc_header*   src,
                              alloc_header*         compact_dst,
                              pending_release_list&) const noexcept override
      {
         static_cast<const T*>(src)->compact_to(compact_dst);
      }
      void active_copy_to(const alloc_header*          src,
                          alloc_header*                dst,
                          const allocator_session_ptr&) const noexcept override
      {
         static_cast<const T*>(src)->copy_to(dst);
      }
      void passive_copy_to(const alloc_header*   src,
                           alloc_header*         dst,
                           pending_release_list&) const noexcept override
      {
         static_cast<const T*>(src)->copy_to(dst);
      }
      void destroy(const alloc_header*          header,
                   const allocator_session_ptr& session) const noexcept override
      {
         static_cast<const T*>(header)->destroy(session);
      }
      void visit_children(
          const alloc_header*                     header,
          const std::function<void(ptr_address)>& visitor) const noexcept override
      {
         static_cast<const T*>(header)->visit_children(visitor);
      }
   };

   template <typename T>
   inline const object_type_ops& default_object_type_ops() noexcept
   {
      static const static_object_type_ops<T> ops;
      return ops;
   }

   /**
    * Stored in sync_header::user_data to record which root was updated
    * at each commit boundary. Enables root reconstruction from segments
    * during power-loss recovery.
    */
   inline constexpr uint8_t sync_header_user_data_capacity = 27;

   struct sync_root_info
   {
      uint32_t root_index;       ///< which root was updated
      uint32_t root_address;     ///< the root ptr_address it was set to
      uint32_t version_address;  ///< custom CB ptr_address for the root version
      uint64_t root_version;     ///< MVCC version stored in version_address
   };
   static_assert(sizeof(sync_root_info) <= sync_header_user_data_capacity,
                 "must fit in sync_header user_data");

   struct sync_root_info_v1
   {
      uint32_t root_index;
      uint32_t root_address;
   };
   static_assert(sizeof(sync_root_info_v1) == 8);

   /**
    * sync_header this is written every time the
    * segment is synced and documents the empty space
    * at the end of the current page along with other
    * metadata that we can store for "free" with the
    * commit because we have a full cacheline or more
    * that rounds out the OS page, which could be up to
    * 16kb. 
    */
   class sync_header : public alloc_header
   {
     public:
      sync_header(uint32_t asize) : alloc_header(asize, header_type::sync_head, {}),
         _time_stamp_usec(usec_timestamp(0)),
         _prev_aheader_pos(0),
         _start_checksum_pos(0),
         _user_data{},
         _user_data_size(0),
         _sync_checksum(0)
      {}

      template <typename UserData>
      void set_user_data(UserData user_data)
      {
         static_assert(sizeof(UserData) <= sizeof(sync_header::_user_data), "UserData too large");
         memcpy(sync_header::_user_data, &user_data, sizeof(UserData));
      }
      /**
       * Get the timestamp in microseconds when this sync occurred
       */
      usec_timestamp timestamp() const noexcept { return _time_stamp_usec; }
      void           set_timestamp(usec_timestamp ts) noexcept { _time_stamp_usec = ts; }

      /**
       * Get the position of the previous allocation header
       */
      uint32_t prev_aheader_pos() const noexcept { return _prev_aheader_pos; }
      void     set_prev_aheader_pos(uint32_t pos) noexcept { _prev_aheader_pos = pos; }

      /**
       * Get the position where checksumming starts
       */
      uint32_t start_checksum_pos() const noexcept { return _start_checksum_pos; }
      void     set_start_checksum_pos(uint32_t pos) noexcept { _start_checksum_pos = pos; }

      /**
       * Get the size of user data stored in this sync header
       */
      uint8_t user_data_size() const noexcept { return _user_data_size; }
      void    set_user_data_size(uint8_t size) noexcept { _user_data_size = size; }

      /**
       * Get the sync checksum value
       */
      uint64_t sync_checksum() const noexcept { return _sync_checksum; }
      void     set_sync_checksum(uint64_t checksum) noexcept { _sync_checksum = checksum; }

      /**
       * Get a pointer to the user data buffer
       */
      const char* user_data() const noexcept { return _user_data; }
      char*       user_data() noexcept { return _user_data; }

      /**
       * Returns the root info if user_data contains a sync_root_info, nullopt otherwise.
       */
      std::optional<sync_root_info> get_root_info() const noexcept
      {
         if (_user_data_size == sizeof(sync_root_info))
         {
            sync_root_info info;
            memcpy(&info, _user_data, sizeof(info));
            return info;
         }
         if (_user_data_size == sizeof(sync_root_info_v1))
         {
            sync_root_info_v1 legacy;
            memcpy(&legacy, _user_data, sizeof(legacy));
            return sync_root_info{
                legacy.root_index, legacy.root_address, *null_ptr_address, 0};
         }
         return std::nullopt;
      }

      uint32_t checksum_offset() const noexcept { return sizeof(*this) - sizeof(_sync_checksum); }

     private:
      usec_timestamp _time_stamp_usec;
      uint32_t       _prev_aheader_pos;
      uint32_t       _start_checksum_pos;
      char           _user_data[sync_header_user_data_capacity];
      uint8_t        _user_data_size;
      /// covers entire range from last sync to _sync_checksum
      /// disticnt from alloc_header::_checksum which only covers this object.
      /// must be the last member of the class
      uint64_t _sync_checksum;
   } __attribute__((packed));
   static_assert(sizeof(sync_header) == 64);

}  // namespace sal
