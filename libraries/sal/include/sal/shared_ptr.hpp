#pragma once
#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <sal/config.hpp>
#include <sal/debug.hpp>
#include <sal/location.hpp>
#include <stdexcept>

namespace sal
{
   /**
    * A shared pointer to a location in shared memory
    */
   struct shared_ptr
   {
     private:
      std::atomic<uint64_t> _data;

     public:
      // Default constructor to ensure proper initialization of the atomic
      shared_ptr() : _data(0) {}

      static constexpr uint64_t location_offset = 21;
      /**
       * The internal structure of the bits stored in the atomic _data
       */
      struct shared_ptr_data
      {
         /// reference count, up to 2M shared references
         uint64_t ref : 21;
         /// index to the cacheline of up to 128 TB of memory with 64 bytes per cacheline
         /// this is the maximum addressable by mapped memory on modern systems.
         uint64_t cacheline_offset : 41;

         /// set this bit when object is read, clearered when ref count goes to 0
         uint64_t active : 1;
         /// set this bit when object should be cached, but this gets cleared
         /// when reference count goes to 0 along with the active bit.
         uint64_t pending_cache : 1;

         shared_ptr_data() : ref(0), cacheline_offset(0), active(0), pending_cache(0) {}
         shared_ptr_data(uint64_t value) { from_int(value); }
         uint64_t to_int() const { return std::bit_cast<uint64_t>(*this); }
         void     from_int(uint64_t value) { *this = std::bit_cast<shared_ptr_data>(value); }
         location loc() const { return location::from_cacheline(cacheline_offset); }

         shared_ptr_data& set_ref(uint64_t r)
         {
            assert(r <= max_ref_count);
            ref = r;
            return *this;
         }
         shared_ptr_data& set_loc(location l)
         {
            cacheline_offset = l.cacheline();
            return *this;
         }
         shared_ptr_data& set_active(bool a)
         {
            active = a;
            return *this;
         }
         shared_ptr_data& set_pending_cache(bool p)
         {
            pending_cache = p;
            return *this;
         }
      };

      /** there are only 19 bits for the reference count, and the algorithm utilizes an
       * optimistic approach to reference counting by using fetch_add instead of 
       * the slower compare_exchange, this means that we need handle the overflow case
       * by using fetch_sub to decrement the reference count; therefore, the maximum 
       * ref count must allow for a certain number of threads to over-shoot the count
       * without causing problems.  This is why we subtract the number of threads from
       * the maximum possible ref count.
       */
      static constexpr uint64_t max_ref_count = (1ULL << 21) - sal::max_threads;

      int  use_count() const { return shared_ptr_data(_data.load(std::memory_order_relaxed)).ref; }
      bool unique() const { return use_count() == 1; }
      void reset() { _data.store(shared_ptr_data().to_int(), std::memory_order_relaxed); }
      bool retain()
      {
         shared_ptr_data prior(_data.fetch_add(1, std::memory_order_relaxed));
         if (prior.ref >= max_ref_count)
            throw std::runtime_error("reference count exceeded limits");
         assert(prior.ref > 0);
         return true;
      };

      uint32_t ref() const { return load().ref; }
      location loc() const { return load().loc(); }
      bool     active() const { return load().active; }
      bool     pending_cache() const { return load().pending_cache; }

      /// @deprecated dont use this
      uint64_t to_int(std::memory_order order = std::memory_order_relaxed) const
      {
         return _data.load(order);
      }

      shared_ptr_data load(std::memory_order order = std::memory_order_relaxed) const
      {
         return shared_ptr_data(_data.load(order));
      }
      void store(shared_ptr_data value, std::memory_order order = std::memory_order_relaxed)
      {
         _data.store(value.to_int(), order);
      }
      void reset(location loc, int ref = 1, std::memory_order order = std::memory_order_release)
      {
         store(shared_ptr_data().set_loc(loc).set_ref(ref), order);
      }
      void set_ref(int ref, std::memory_order order = std::memory_order_relaxed)
      {
         store(load(order).set_ref(ref), order);
      }

      /**
       * @return the state of the shared_ptr_data before decrementing the reference count
       */
      shared_ptr_data release()
      {
         shared_ptr_data prior(_data.fetch_sub(1, std::memory_order_release));
         assert(prior.ref > 0);
         if constexpr (debug_memory)
         {
            if (prior.ref == 0)
               abort();
         }
         if (prior.ref == 1)
            clear_pending_cache();
         return prior;
      };
      void clear_pending_cache()
      {
         uint64_t        expected = _data.load(std::memory_order_relaxed);
         shared_ptr_data updated;
         do
         {
            updated.from_int(expected);
            updated.set_active(false);
            updated.set_pending_cache(false);
         } while (
             !_data.compare_exchange_weak(expected, updated.to_int(), std::memory_order_release));
      }

      /**
       * compare and swap move,
       * updates the cacheline_offset to the desired value 
       * if the current value is equal to the expected value 
       * and the ref count is not 0, note that other changes to
       * the shared_ptr_data are alloewd 
       */
      bool cas_move(location expected_loc, location desired_loc)
      {
         uint64_t        expect_data = _data.load(std::memory_order_relaxed);
         shared_ptr_data prior;
         do
         {
            prior.from_int(expect_data);
            if (prior.loc() != expected_loc or prior.ref == 0) [[unlikely]]
               return false;
            prior.set_loc(desired_loc);
         } while (not _data.compare_exchange_weak(expect_data, prior.to_int(),
                                                  std::memory_order_release));
         return true;
      }

      /**
       * Moves the location without regard to the prior location, but
       * without disrupting any other fields that may be updated by
       * other threads.
       * 
       * @return the updated shared_ptr_data
       */
      shared_ptr_data move(location loc, std::memory_order order = std::memory_order_relaxed)
      {
         auto            expected = _data.load(order);
         shared_ptr_data updated;
         do
         {
            updated.from_int(expected);
            updated.set_loc(loc);
         } while (not _data.compare_exchange_weak(expected, updated.to_int(), order));
         return updated;
      }

      /**
       * Attempts to increment the activity counter in a non-blocking way.
       * If the object is not marked as active, tries to set the active bit.
       * If already active, tries to set the pending_cache bit.
       * May fail if there is contention, which is acceptable since this 
       * simulates random sampling behavior.
       *
       * @return true if successfully incremented activity, false if failed due to contention
       */
      bool try_inc_activity()
      {
         uint64_t        expected = _data.load(std::memory_order_relaxed);
         shared_ptr_data updated(expected);
         if (updated.pending_cache)
            return false;
         if (updated.active)
            return _data.compare_exchange_weak(expected, updated.set_pending_cache(true).to_int(),
                                               std::memory_order_relaxed);
         return _data.compare_exchange_weak(expected, updated.set_active(true).to_int(),
                                            std::memory_order_relaxed);
      }

      /**
       *  Clears the pending cache bit, returns false if it is already cleared
       * 
       * @return true if the pending cache bit was cleared, false otherwise
       */
      bool try_end_pending_cache()
      {
         uint64_t        expected = _data.load(std::memory_order_relaxed);
         shared_ptr_data updated;
         do
         {
            updated.from_int(expected);
            if (updated.pending_cache == false)
               return false;
            updated.set_pending_cache(false);
         } while (not _data.compare_exchange_weak(expected, updated.to_int(),
                                                  std::memory_order_relaxed));
         return true;
      }

      static_assert(sizeof(shared_ptr_data) == 8, "shared_ptr_data must be 8 bytes");
   };
   static_assert(sizeof(shared_ptr) == 8, "shared_ptr must be 8 bytes");

}  // namespace sal
