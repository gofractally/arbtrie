#pragma once
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
    * A control block for a shared pointer to a location in shared memory
    */
   struct control_block
   {
     private:
      std::atomic<uint64_t> _data;

     public:
      // Default constructor to ensure proper initialization of the atomic
      control_block() noexcept : _data(0) {}

      static constexpr uint64_t location_offset      = 21;
      static constexpr uint64_t max_cacheline_offset = (1ULL << 41) - 1;
      /**
       * The internal structure of the bits stored in the atomic _data
       */
      struct control_block_data
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

         control_block_data() noexcept : ref(0), cacheline_offset(0), active(0), pending_cache(0) {}
         control_block_data(uint64_t value) noexcept { from_int(value); }
         uint64_t to_int() const noexcept { return std::bit_cast<uint64_t>(*this); }
         void     from_int(uint64_t value) noexcept
         {
            *this = std::bit_cast<control_block_data>(value);
         }
         location loc() const noexcept { return location::from_cacheline(cacheline_offset); }

         control_block_data& set_ref(uint64_t r) noexcept
         {
            assert(r <= max_ref_count);
            ref = r;
            return *this;
         }
         control_block_data& set_loc(location l) noexcept
         {
            cacheline_offset = l.cacheline();
            return *this;
         }
         control_block_data& set_active(bool a) noexcept
         {
            active = a;
            return *this;
         }
         control_block_data& set_pending_cache(bool p) noexcept
         {
            pending_cache = p;
            return *this;
         }
         friend std::ostream& operator<<(std::ostream& os, const control_block_data& data)
         {
            os << "{ref:" << data.ref << " loc:" << data.loc() << " active:" << data.active
               << " pending_cache:" << data.pending_cache << "}";
            return os;
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

      int use_count() const noexcept
      {
         return control_block_data(_data.load(std::memory_order_relaxed)).ref;
      }
      bool unique() const noexcept { return use_count() == 1; }
      void reset() noexcept
      {
         _data.store(control_block_data().to_int(), std::memory_order_relaxed);
      }
      control_block_data retain()
      {
         control_block_data prior = _data.fetch_add(1, std::memory_order_relaxed);
         if (prior.ref >= max_ref_count)
         {
            control_block_data prior = _data.fetch_sub(1, std::memory_order_relaxed);
            throw std::runtime_error("reference count exceeded limits");
         }
         assert(prior.ref > 0);
         return prior;
      };

      uint32_t ref() const noexcept { return load(std::memory_order_relaxed).ref; }
      location loc() const noexcept { return load(std::memory_order_acquire).loc(); }
      bool     active() const noexcept { return load(std::memory_order_relaxed).active; }
      bool pending_cache() const noexcept { return load(std::memory_order_relaxed).pending_cache; }

      /// @deprecated dont use this
      uint64_t to_int(std::memory_order order = std::memory_order_relaxed) const noexcept
      {
         return _data.load(order);
      }

      control_block_data load(std::memory_order order = std::memory_order_relaxed) const noexcept
      {
         return control_block_data(_data.load(order));
      }
      void store(control_block_data value,
                 std::memory_order  order = std::memory_order_relaxed) noexcept
      {
         _data.store(value.to_int(), order);
      }
      void reset(location          loc,
                 int               ref   = 1,
                 std::memory_order order = std::memory_order_release) noexcept
      {
         store(control_block_data().set_loc(loc).set_ref(ref), order);
      }
      void set_ref(int ref, std::memory_order order = std::memory_order_relaxed) noexcept
      {
         store(load(order).set_ref(ref), order);
      }

      /**
       * @return the state of the control_block_data before decrementing the reference count
       */
      control_block_data release() noexcept
      {
         // if we are not the last reference then relaxed is best, we will
         // load with acquire before returning if we are the last reference.
         // TSAN seems happy with relaxed + acquire and this works because all
         // modifications are either done by the "unique owner" aka (ref 1),
         // or done by a thread that has just copied the data to a new location
         // and we are synchronziing with the cas_move below.
         control_block_data prior(_data.fetch_sub(1, std::memory_order_relaxed));
         assert(prior.ref > 0);
         if constexpr (debug_memory)
         {
            if (prior.ref == 0)
               abort();
         }
         if (prior.ref == 1)
         {
            if (prior.pending_cache or prior.active)
               clear_pending_cache();
            // make sure that any changes in location and the new memory being
            // pointed at are visible to the releasing thread.
            return load(std::memory_order_acquire);
         }
         return prior;
      };
      void clear_pending_cache() noexcept
      {
         uint64_t           expected = _data.load(std::memory_order_relaxed);
         control_block_data updated;
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
       * the control_block_data are alloewd 
       */
      bool cas_move(location expected_loc, location desired_loc) noexcept
      {
         assert(desired_loc.cacheline() != max_cacheline_offset);
         uint64_t           expect_data = _data.load(std::memory_order_relaxed);
         control_block_data prior;
         do
         {
            prior.from_int(expect_data);
            if (prior.loc() != expected_loc or prior.ref == 0) [[unlikely]]
               return false;
            prior.set_loc(desired_loc);
         } while (not _data.compare_exchange_weak(expect_data, prior.to_int(),
                                                  std::memory_order_seq_cst));
         return true;
      }

      // used by the allocator to claim this pointer to use, uses the
      // cacheline_offset max value to indicate the pointer alloc/free.
      /*  [[nodiscard]] bool claim() noexcept
      {
         uint64_t           expect_data = _data.load(std::memory_order_relaxed);
         control_block_data prior;
         do
         {
            prior.from_int(expect_data);
            if (uint64_t(prior.cacheline_offset) != max_cacheline_offset) [[unlikely]]
               return false;
            prior.cacheline_offset = 0;
         } while (not _data.compare_exchange_weak(expect_data, prior.to_int(),
                                                  std::memory_order_seq_cst));
         return true;
      }
      void release_claim() noexcept
      {
         control_block_data prior;
         prior.cacheline_offset = max_cacheline_offset;
         _data.store(prior.to_int(), std::memory_order_relaxed);
      }
*/
      /**
       * Moves the location without regard to the prior location, but
       * without disrupting any other fields that may be updated by
       * other threads.
       * 
       * @return the updated control_block_data
       */
      control_block_data move(location          loc,
                              std::memory_order order = std::memory_order_relaxed) noexcept
      {
         assert(loc.cacheline() != max_cacheline_offset);
         auto               expected = _data.load(order);
         control_block_data updated;
         do
         {
            updated.from_int(expected);
            assert(updated.loc() != loc);
            updated.set_loc(loc);
         } while (not _data.compare_exchange_weak(expected, updated.to_int(), order));
         updated.from_int(expected);
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
      bool try_inc_activity() noexcept
      {
         uint64_t           expected = _data.load(std::memory_order_relaxed);
         control_block_data updated(expected);
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
      bool try_end_pending_cache() noexcept
      {
         uint64_t           expected = _data.load(std::memory_order_relaxed);
         control_block_data updated;
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

      static_assert(sizeof(control_block_data) == 8, "shared_ptr_data must be 8 bytes");
   };
   static_assert(sizeof(control_block) == 8, "shared_ptr must be 8 bytes");

   using control_block_data = control_block::control_block_data;

}  // namespace sal
