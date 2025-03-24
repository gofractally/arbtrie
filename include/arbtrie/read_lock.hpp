#pragma once
#include <arbtrie/id_alloc.hpp>
#include <arbtrie/node_meta.hpp>

namespace arbtrie
{
   class seg_alloc_session;
   class read_lock;
   class node_header;

   /**
   * @brief A lock that allows a single thread to modify a node
   * 
   * This object is returned by the modify() method on object_ref and
   * ensures that the modify lock is released when the modify lock 
   * goes out of scope.
   */
   class modify_lock
   {
     public:
      modify_lock(node_meta_type& m, read_lock& rl);
      ~modify_lock();

      // returned mutable T is only valid while modify lock is in scope
      template <typename T>
      T* as();

      template <typename T>
      void as(std::invocable<T*> auto&& call_with_tptr);
      void release();

     private:
      void         unlock();
      node_header* copy_on_write(node_meta_type::temp_type meta);

      // it starts out true because lock isn't acquired unless as() is called
      // and exposes the protected memory to the caller
      bool            _released = true;
      node_meta_type& _meta;
      read_lock&      _rlock;
      node_header*    _observed_ptr = nullptr;
   };

   class object_ref;

   /**
     * Ensures the read-lock is released so segments can be recycled
     * and ensures that all data access flows through a read_lock.
     *
     * note: this is a wait-free lock that prevents segments from
     * being reused until all reads are complete. It is cheap to
     * acquire and release, but holding it a long time will increase
     * memory usage and reduce cache performance.
     */
   class read_lock
   {
     public:
      object_ref alloc(id_region reg, uint32_t size, node_type type, auto initfunc);

      // id_address reuse,
      object_ref realloc(object_ref& r, uint32_t size, node_type type, auto initfunc);

      /**
             * @defgroup Region Alloc Helpers
             */
      /// @{
      id_region     get_new_region();
      void          free_meta_node(id_address);
      id_allocation get_new_meta_node(id_region);
      /// @}

      inline object_ref get(id_address adr);
      inline object_ref get(node_header*);

      auto call_with_node(id_address adr, auto&& call);

      ~read_lock() { _session.release_read_lock(); }

      node_header* get_node_pointer(node_location);
      //void         update_read_stats(node_location, uint32_t node_size, uint64_t time);

      bool is_read_only(node_location loc) const;
      bool can_modify(node_location loc) const;

      /**
       * Check if an object should be cached based on its size and difficulty threshold
       * @param size The size of the object in bytes
       * @return true if the object should be cached, false otherwise
       */
      bool should_cache(uint32_t size) const;

      /**
       * Records when an object has been freed to update segment metadata
       * @param segment The segment number where the object is located
       * @param obj_ptr Pointer to the object being freed
       */
      void freed_object(segment_number segment, const node_header* obj_ptr);

     private:
      friend class seg_alloc_session;
      friend class object_ref;
      friend class modify_lock;

      read_lock(seg_alloc_session& s) : _session(s) { _session.retain_read_lock(); }
      read_lock(const seg_alloc_session&) = delete;
      read_lock(seg_alloc_session&&)      = delete;

      read_lock& operator=(const read_lock&) = delete;
      read_lock& operator=(read_lock&)       = delete;
      read_lock& operator=(read_lock&&)      = delete;

      uint64_t           cache_difficulty() const;
      seg_alloc_session& _session;
   };

}  // namespace arbtrie

#include <arbtrie/object_ref.hpp>

namespace arbtrie
{
   auto read_lock::call_with_node(id_address adr, auto&& call)
   {
      auto obj_ref = get(adr);
      return cast_and_call(obj_ref.header(), std::forward<decltype(call)>(call));
   }
}  // namespace arbtrie