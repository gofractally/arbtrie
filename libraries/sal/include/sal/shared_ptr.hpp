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
      /**
       * The internal structure of the bits stored in the atomic _data
       */
      struct shared_ptr_data
      {
         /// reference count, up to 524k shared references
         uint64_t ref : 19;
         /// index to the cacheline of up to 128 TB of memory with 64 bytes per cacheline
         /// this is the maximum addressable by mapped memory on modern systems.
         uint64_t cacheline_offset : 41;

         /// indicates this object doesn't have any shared_ptr members,
         /// nor destructor calls needed
         uint64_t is_pod : 1;

         /// 0 for small objects, 1 for large objects
         uint64_t zone : 1;

         /// used to track the activity of object for caching purposes
         /// saturated integer, 0 to 3
         uint64_t activity : 2;

         shared_ptr_data() : ref(0), cacheline_offset(0), is_pod(0), zone(0), activity(0) {}
         shared_ptr_data(uint64_t value) { from_int(value); }
         uint64_t to_int() const { return std::bit_cast<uint64_t>(*this); }
         void     from_int(uint64_t value) { *this = std::bit_cast<shared_ptr_data>(value); }
         location loc() const { return location::from_cacheline(cacheline_offset); }

         shared_ptr_data& inc_activity()
         {
            activity = std::min(int(activity) + 1, 3);
            return *this;
         }
         shared_ptr_data& dec_activity()
         {
            activity = std::max(int(activity) - 1, 0);
            return *this;
         }

         shared_ptr_data& set_ref(uint64_t r)
         {
            ref = r;
            return *this;
         }
         shared_ptr_data& set_loc(location l)
         {
            cacheline_offset = l.cacheline();
            return *this;
         }
         shared_ptr_data& set_pod(bool p)
         {
            is_pod = p;
            return *this;
         }
         shared_ptr_data& set_zone(bool z)
         {
            zone = z;
            return *this;
         }
         shared_ptr_data& set_activity(uint64_t a)
         {
            activity = a;
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
      static constexpr uint64_t max_ref_count = (1ULL << 19) - sal::max_threads;

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

      shared_ptr_data load(std::memory_order order = std::memory_order_relaxed) const
      {
         return shared_ptr_data(_data.load(order));
      }
      void store(shared_ptr_data value, std::memory_order order = std::memory_order_relaxed)
      {
         _data.store(value.to_int(), order);
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
            if (prior.ref == 1)
               abort();
         }
         return prior;
      };

      /**
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
       * Activity is a "probabilistic" counter, this method will attempt to
       * increment the activity, but if there is contention it may fail, and
       * this should be acceptable as simulating the random sampling behavior
       * that we are trying to model anyway, we wont' want the caller to spin 
       * 
       * @return the value read before attempted change, says nothing about
       * whether the change was successful or not.
       */
      shared_ptr_data try_inc_activity()
      {
         uint64_t        expected = _data.load(std::memory_order_relaxed);
         shared_ptr_data updated(expected);
         updated.inc_activity();
         _data.compare_exchange_weak(expected, updated.to_int(), std::memory_order_relaxed);
         return shared_ptr_data(expected);
      }

      /**
       *  @return the value read before attempted change, says nothing about
       * whether the change was successful or not.
       */
      shared_ptr_data try_dec_activity()
      {
         uint64_t        expected = _data.load(std::memory_order_relaxed);
         shared_ptr_data updated(expected);
         updated.dec_activity();
         _data.compare_exchange_weak(expected, updated.to_int(), std::memory_order_relaxed);
         return shared_ptr_data(expected);
      }
      static_assert(sizeof(shared_ptr_data) == 8, "shared_ptr_data must be 8 bytes");
   };
   static_assert(sizeof(shared_ptr) == 8, "shared_ptr must be 8 bytes");

}  // namespace sal
