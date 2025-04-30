#pragma once
#include <hash/xxhash.h>
#include <cstdint>
#include <functional>
#include <sal/allocator_session.hpp>
#include <sal/time.hpp>
#include <ucc/fast_memcpy.hpp>

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

   /**
     * Defines a base class for user defined types to define their own 
     * optimized memory operations. The default impl is to use memcpy 
     * and full object checksumming, but there may be more efficient
     * ways to handle this for specific types, such as growing on
     * COW and compacting to a smaller size later. Or skipping certain
     * internal bytes or having a custom checksum method.
    */
   template <typename T>
   class vtable
   {
     public:
      static constexpr const uint8_t type_id = (uint8_t)T::type_id;
      static uint32_t                compact_size(const alloc_header* header) noexcept
      {
         return static_cast<const T*>(header)->compact_size();
      }
      /**
       * The size that copy_to would like to reserve in case the copy wants to
       * grow or shrink the object. Should be a multiple of 64 bytes.
       */
      static uint32_t cow_size(const alloc_header* header) noexcept
      {
         return static_cast<const T*>(header)->cow_size();
      }
      static bool has_checksum(const alloc_header* header) noexcept
      {
         return static_cast<const T*>(header)->has_checksum();
      }
      static bool verify_checksum(const alloc_header* header) noexcept
      {
         return static_cast<const T*>(header)->verify_checksum();
      }
      static void update_checksum(alloc_header* header) noexcept
      {
         return static_cast<T*>(header)->update_checksum();
      }
      /**
       * dst->size() should be compact_size(src)
       */
      static void compact_to(const alloc_header* src, alloc_header* compact_dst) noexcept
      {
         static_cast<const T*>(src)->compact_to(compact_dst);
      }
      /**
       * dst->size() should be cow_size(src)
       */
      static void copy_to(const alloc_header* src, alloc_header* dst) noexcept
      {
         static_cast<const T*>(src)->copy_to(dst);
      }
      /**
       * This is called when the object is destroyed, in which case the object
       * may have ptr_address's that need to be recursively released. An object_ref
       * provides both access to the object being destroyed, but also to the
       * allocator_session_ptr which enables getting other objects
       * and releasing them as well.
       */
      static void destroy(const alloc_header* header, const allocator_session_ptr& session) noexcept
      {
         static_cast<const T*>(header)->destroy(session);
      }
      static void visit_children(const alloc_header*                     header,
                                 const std::function<void(ptr_address)>& visitor) noexcept
      {
         static_cast<const T*>(header)->visit_children(visitor);
      }
   };

   struct vtable_pointers
   {
      template <typename T>
      static vtable_pointers create()
      {
         return {&T::update_checksum, &T::cow_size,        &T::compact_size,
                 &T::has_checksum,    &T::verify_checksum, &T::compact_to,
                 &T::copy_to,         &T::destroy,         &T::visit_children};
      }
      void (*update_checksum)(alloc_header* header) noexcept;

      uint32_t (*cow_size)(const alloc_header* header) noexcept;
      uint32_t (*compact_size)(const alloc_header* header) noexcept;
      bool (*has_checksum)(const alloc_header* header) noexcept;
      bool (*verify_checksum)(const alloc_header* header) noexcept;
      void (*compact_to)(const alloc_header* src, alloc_header* compact_dst) noexcept;
      void (*copy_to)(const alloc_header* src, alloc_header* dst) noexcept;
      void (*destroy)(const alloc_header* header, const allocator_session_ptr& session) noexcept;
      void (*visit_children)(const alloc_header*                     header,
                             const std::function<void(ptr_address)>& visitor) noexcept;
   };

   std::array<vtable_pointers, 128>& get_type_vtables();

   template <typename T>
   inline static int register_type_vtable()
   {
      static_assert(uint8_t(T::type_id) < 128, "type_id out of range");
      get_type_vtables()[uint8_t(T::type_id)] = vtable_pointers::create<vtable<T>>();
      SAL_WARN("register_type_vtable {} {} destroy ptr: {}", T::type_id, int(T::type_id),
               get_type_vtables()[uint8_t(T::type_id)].destroy);
      return int(T::type_id);
   }

   namespace vcall
   {
      inline static uint32_t cow_size(const alloc_header* header) noexcept
      {
         return get_type_vtables()[uint8_t(header->type())].cow_size(header);
      }
      inline static uint32_t compact_size(const alloc_header* header) noexcept
      {
         return get_type_vtables()[uint8_t(header->type())].compact_size(header);
      }
      inline static bool has_checksum(const alloc_header* header) noexcept
      {
         return get_type_vtables()[uint8_t(header->type())].has_checksum(header);
      }
      inline static bool verify_checksum(const alloc_header* header) noexcept
      {
         return get_type_vtables()[uint8_t(header->type())].verify_checksum(header);
      }
      inline static void update_checksum(alloc_header* header) noexcept
      {
         return get_type_vtables()[uint8_t(header->type())].update_checksum(header);
      }
      inline static void compact_to(const alloc_header* src, alloc_header* compact_dst) noexcept
      {
         return get_type_vtables()[uint8_t(src->type())].compact_to(src, compact_dst);
      }
      inline static void copy_to(const alloc_header* src, alloc_header* dst) noexcept
      {
         return get_type_vtables()[uint8_t(src->type())].copy_to(src, dst);
      }
      inline static void destroy(const alloc_header*          header,
                                 const allocator_session_ptr& session) noexcept
      {
         return get_type_vtables()[uint8_t(header->type())].destroy(header, session);
      }
   }  // namespace vcall

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
      sync_header(uint32_t asize) : alloc_header(asize, header_type::sync_head, {}) {}

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

      uint32_t checksum_offset() const noexcept { return sizeof(*this) - sizeof(_sync_checksum); }

     private:
      usec_timestamp _time_stamp_usec;
      uint32_t       _prev_aheader_pos;
      uint32_t       _start_checksum_pos;
      char           _user_data[27];
      uint8_t        _user_data_size;
      /// covers entire range from last sync to _sync_checksum
      /// disticnt from alloc_header::_checksum which only covers this object.
      /// must be the last member of the class
      uint64_t _sync_checksum;
   } __attribute__((packed));
   static_assert(sizeof(sync_header) == 64);

}  // namespace sal
