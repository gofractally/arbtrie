#pragma once
#include <arbtrie/address.hpp>
#include <arbtrie/config.hpp>
#include <arbtrie/debug.hpp>
#include <arbtrie/node_location.hpp>
#include <arbtrie/util.hpp>
#include <cassert>
#include <iostream>
#include <optional>
#include <string_view>
#include <variant>

namespace arbtrie
{
   enum node_type : uint8_t
   {
      freelist  = 0,  // not initialized/invalid, must be first enum
      binary    = 1,  // binary search
      value     = 2,  // just the data, no key
      setlist   = 3,  // list of branches
      full      = 4,  // 256 full id_type
      bitset    = 5,  // 1 bit per present branch
      undefined = 6,  // no type has been defined yet
      unused    = 7

      // future, requires taking a bit, or removing undefined/freelist
      //index    = 7,  // 256 index buffer to id_type
      //bitfield = 8,
      //merge    = 9,  // delta applied to existing node
   };
   static constexpr int num_types         = 7;
   static const char*   node_type_names[] = {"freelist", "binary", "value",    "setlist",
                                             "full",     "bitset", "undefined"};

   inline std::ostream& operator<<(std::ostream& out, node_type t)
   {
      if (t < node_type::undefined) [[likely]]
         return out << node_type_names[t];
      return out << "undefined(" << int(t) << ")";
   }

   /**
    * @class node_meta
    * 
    * This class is the core of the arbtrie memory managment algorithm
    * and is responsible for a majority of the lock-free properties. It
    * manages 8 bytes of "meta" information on every node in the trie
    * including its current location, reference count, and type. 
    *
    * Because node_meta is an atomic type and we desire to minimize 
    * the number of atomic accesses, @ref node_meta is templated on
    * the storage type so the majority of the API can be used on the
    * temporary read from the atomic. See node_meta<>::temp_type
    * 
    * Primary Operations:
    *     retain() / release() - reference counting
    *     read/pending flags for cache signalling
    *     compare_exchange_location() - move the node to a new location
    * 
    * Assumptions:
    *     Only unique owners are able to modify the data
    *     pointed at by the current location, assuming that location
    *     hasn't been made write protected yet.
    */
   template <typename Storage = std::atomic<uint64_t>>
   class node_meta
   {
      /**
       *  Use the bitfield to layout the data,
       *  compute the masks. 
       */
      struct bitfield
      {
         uint64_t ref : 14          = 0;
         uint64_t type : 3          = 0;
         uint64_t read : 1          = 0;  // indicates someone read this node since last cleared
         uint64_t copy_flag : 1     = 0;
         uint64_t unused_flag : 1   = 0;  // 0 when modifying, 1 when not
         uint64_t pending_cache : 1 = 0;  // indicates this node is pending cache update
         // gives 512 TB addressable cachelines
         uint64_t location : 43 = 0;

         constexpr bitfield& from_int(uint64_t i)
         {
            memcpy(this, &i, sizeof(i));
            return *this;
         }
         explicit bitfield(uint64_t bf) { from_int(bf); }

         // doesn't work as a constexpr on all compilers
         constexpr uint64_t to_int() const { return std::bit_cast<uint64_t>(*this); }
         constexpr auto&    set_type(node_type t)
         {
            type = t;
            return *this;
         }
         constexpr auto& set_location(node_location l)
         {
            location = l.to_aligned();
            return *this;
         }
         constexpr auto& set_ref(uint16_t r)
         {
            assert(r <= max_ref_count);
            ref = r;
            return *this;
         }
      } __attribute((packed));
      static_assert(sizeof(bitfield) == sizeof(uint64_t));

     public:
      using temp_type = node_meta<uint64_t>;

      static constexpr const int      location_offset    = 21;
      static constexpr const uint64_t ref_mask           = make_mask<0, 14>();
      static constexpr const uint64_t type_mask          = make_mask<14, 3>();
      static constexpr const uint64_t read_mask          = make_mask<17, 1>();
      static constexpr const uint64_t copy_mask          = make_mask<18, 1>();
      static constexpr const uint64_t modify_mask        = make_mask<19, 1>();
      static constexpr const uint64_t pending_cache_mask = make_mask<20, 1>();
      static constexpr const uint64_t location_mask      = make_mask<location_offset, 43>();

      /**
       *  Because retain() uses fetch_add() there is a possability of
       *  overflow. For this reason retain will fail and undo once it
       *  reaches past max_ref_count. A value of 64 means 64 threads would
       *  have to fetch_add() before any thread fetch_sub() after reading
       *  the value. That is an unreasonable number of cores attempting
       *  to retain at the same time; therefore, this should be safe and
       *  still allows ref counts up to 4032. 
       */
      static constexpr const uint64_t max_ref_count = ref_mask - max_threads;

      /**
       * @defgroup Accessors
       *  These methods work on by the atomic and temp_type 
       */
      ///@{

      bitfield to_bitfield(std::memory_order order = std::memory_order_relaxed) const
      {
         return bitfield(to_int(order));
      }
      uint64_t to_int(std::memory_order order = std::memory_order_relaxed) const
      {
         if constexpr (std::is_same_v<Storage, uint64_t>)
            return _meta;
         else
            return _meta.load(order);
      }

      bool          is_read() const { return to_int() & read_mask; }
      bool          is_pending_cache() const { return to_int() & pending_cache_mask; }
      uint16_t      ref() const { return bitfield(to_int()).ref; }
      node_location loc() const { return node_location::from_aligned(bitfield(to_int()).location); }
      node_type     type() const { return node_type(bitfield(to_int()).type); }

      auto& set_pending_cache()
      {
         if constexpr (std::is_same_v<Storage, std::atomic<uint64_t>>)
            _meta.fetch_or(pending_cache_mask, std::memory_order_relaxed);
         else
            _meta |= pending_cache_mask;
         return *this;
      }

      auto& set_read()
      {
         if constexpr (std::is_same_v<Storage, std::atomic<uint64_t>>)
         {
            auto expected = _meta.load(std::memory_order_relaxed);
            while (!(expected & read_mask))
            {
               if (_meta.compare_exchange_weak(expected, expected | read_mask,
                                               std::memory_order_relaxed))
                  break;
            }
         }
         else
            _meta |= read_mask;
         return *this;
      }

      auto& clear_read_bit(std::memory_order order = std::memory_order_relaxed)
      {
         if constexpr (std::is_same_v<Storage, std::atomic<uint64_t>>)
         {
            auto expected = _meta.load(order);
            while (expected & read_mask)
            {
               if (_meta.compare_exchange_weak(expected, expected & ~read_mask,
                                               std::memory_order_relaxed))
                  break;
            }
         }
         else
            _meta &= ~read_mask;
         return *this;
      }

      auto& start_pending_cache()
      {
         if constexpr (std::is_same_v<Storage, std::atomic<uint64_t>>)
            _meta.fetch_or(pending_cache_mask, std::memory_order_relaxed);
         else
            _meta |= pending_cache_mask;
         return *this;
      }

      auto& end_pending_cache()
      {
         if constexpr (std::is_same_v<Storage, std::atomic<uint64_t>>)
            _meta.fetch_and(~(read_mask | pending_cache_mask), std::memory_order_relaxed);
         else
            _meta &= ~(read_mask | pending_cache_mask);
         return *this;
      }

      /**
       * Attempts to set the read bit atomically.
       * @return true if and only if the read bit was successfully changed from 0 to 1 
       *         via a successful compare and exchange operation.
       *         Returns false if the read bit was already set, if the pending_cache bit was set,
       *         or if the compare/exchange failed.
       */
      bool try_set_read()
      {
         if constexpr (std::is_same_v<Storage, uint64_t>)
         {
            _meta |= read_mask;
            return true;
         }
         else
         {
            auto e    = _meta.load(std::memory_order_relaxed);
            auto next = e | read_mask;
            return ((e & (read_mask | pending_cache_mask)) == 0) &&
                   _meta.compare_exchange_weak(e, next, std::memory_order_relaxed,
                                               std::memory_order_relaxed);
         }
      }
      /** 
       * The first time through the read bit gets set, if the
       * read bit is already set then the pending read bit gets set and
       * this returns true... promote the item to the cache. 
       * 
       * The read bit may be cleared at any time (by decay) but the
       * pending cache bit will be cleared when compactor proceses it.
       * 
       * We do not want to set the read bit again until after the compactor
       * has processed the pending cache bit, and we cannot set the pending
       * cache bit unless the read bit is already set.
       * 
       * @return true if the node should be promoted to the cache
      */
      bool try_set_read_or_pending_cache()
      {
         auto e = _meta.load(std::memory_order_relaxed);
         if (e & pending_cache_mask)
            return false;
         if (e & read_mask)
         {
            // try to set the pending cache bit one time
            auto next = e | pending_cache_mask;
            return _meta.compare_exchange_weak(e, next, std::memory_order_relaxed,
                                               std::memory_order_relaxed);
         }
         // try to set the pending cache bit
         _meta.compare_exchange_weak(e, e | read_mask, std::memory_order_relaxed,
                                     std::memory_order_relaxed);
         return false;  // set read bit, but didn't set pending cache bit
      }

      auto& set_ref(uint16_t ref)
      {
         assert(ref < max_ref_count);
         if constexpr (std::is_same_v<Storage, uint64_t>)
            _meta = bitfield(to_int()).set_ref(ref).to_int();
         else
         {
            bitfield bf(0);
            bf.set_ref(ref);
            uint64_t bfi = bf.to_int() & ref_mask;

            uint64_t desired;
            auto     expect = _meta.load(std::memory_order_relaxed);
            do
            {
               desired = (expect & ~(ref_mask)) | bfi;
            } while (not _meta.compare_exchange_weak(expect, desired, std::memory_order_relaxed));
         }
         return *this;
      }

      auto& set_type(node_type t)
      {
         if constexpr (std::is_same_v<Storage, uint64_t>)
            _meta = bitfield(to_int()).set_type(t).to_int();
         else
         {
            static_assert(false, "not an atomic operation");
         }
         return *this;
      }
      node_meta& set_location(node_location nl)
      {
         if constexpr (std::is_same_v<Storage, uint64_t>)
            _meta = bitfield(to_int()).set_location(nl).to_int();
         else
            static_assert(false, "not an atomic operation");
         return *this;
      }

      node_meta& set_location_and_type(node_location l, node_type t, auto memory_order)
      {
         if constexpr (std::is_same_v<Storage, uint64_t>)
            return set_location(l).set_type(t);
         else
         {
            bitfield bf(0);
            bf.set_type(t);
            bf.set_location(l);
            uint64_t bfi = bf.to_int() & (location_mask | type_mask);

            uint64_t desired;
            auto     expect = _meta.load(std::memory_order_relaxed);
            do
            {
               desired = (expect & ~(location_mask | type_mask)) | bfi;
            } while (not _meta.compare_exchange_weak(expect, desired, memory_order));
            assert(type() == t);
            assert(loc() == l);
         }
         return *this;
      }

      auto& clear_copy_flag()
      {
         _meta &= ~copy_mask;
         return *this;
      }

      void store(uint64_t v, std::memory_order memory_order = std::memory_order_relaxed)
      {
         _meta.store(v, memory_order);
      }
      auto exchange(temp_type v, std::memory_order memory_order = std::memory_order_relaxed)
      {
         return temp_type(_meta.exchange(v.to_int(), memory_order));
      }

      void store(temp_type v, std::memory_order memory_order = std::memory_order_relaxed)
      {
         _meta.store(v.to_int(), memory_order);
      }
      auto load(auto memory_order = std::memory_order_relaxed) const
      {
         if constexpr (std::is_same_v<Storage, uint64_t>)
            return temp_type(_meta);
         else
            return temp_type(_meta.load(memory_order));
      }
      ///@}

      /**
      std::mutex& mut()
      {
         static std::mutex m;
         return m;
      }
      */

      /**
       * This method updates the location of the node to @param new_loc iff 
       * the prior location is the expected location and the reference count
       * has not dropped to 0.
       */
      bool compare_exchange_location(node_location expect_loc, node_location new_loc)
      {
         uint64_t  expected = _meta.load(std::memory_order_relaxed);
         temp_type ex;
         do
         {
            ex = temp_type(expected);
            if (ex.loc() != expect_loc or ex.ref() == 0) [[unlikely]]
               return false;
            ex.set_location(new_loc);
         } while (
             not _meta.compare_exchange_weak(expected, ex.to_int(), std::memory_order_release));
         return true;
      }

      /**
       * This method is only safe to call if the caller already "owns" one
       * existing reference count. To retain from a non-owning thread requires
       * calling try_retain
       */
      bool retain()
      {
         temp_type prior(_meta.fetch_add(1, std::memory_order_relaxed));
         if (prior.ref() > node_meta::max_ref_count) [[unlikely]]
         {
            _meta.fetch_sub(1, std::memory_order_relaxed);
            throw std::runtime_error("reference count exceeded limits");
         }
         assert(prior.ref() > 0);
         return true;
      }
      temp_type release()
      {
         /**
          *  Normally reference counting requires this to be memory_order_acquire
          *  or, technically, memory_order_release followed by an acquire fence
          *  if and only if prior was 1.  This is to make sure all writes to the
          *  memory are complete before the object is theoretically handed over
          *  to someone else; however, we have a unique situation:
          *
          *  1. all data with a reference count > 1 is constant and protected by
          *     the segment sequence lock.
          *  2. The caller of release() holds the segment lock that prevents it
          *  from being reused for anyone else until after that is released.
          *
          *  In other words the "life time" of the object extends to the read
          *  lock and this decrement is not in danger given all threads
          *  synchronize their memory on the sequence numbers.
          */
         temp_type prior(_meta.fetch_sub(1, std::memory_order_relaxed));
         assert(prior.ref() != 0);
         if constexpr (debug_memory)
         {
            //  if (prior.ref() == 1 and prior.is_pending_cache())
            //     ARBTRIE_WARN("release node in pending cache");
            // no one should use meta.  Setting it to 0
            //  if (prior.ref() == 1)
            //     _meta.store(0, std::memory_order_relaxed);
            if (prior.ref() == 0)
               abort();
         }
         return prior;
      }
      ///@} k

      node_meta(const node_meta<uint64_t>& cpy) : _meta(cpy._meta) {}

      constexpr node_meta(uint64_t v = 0) : _meta(v) {}

     private:
      Storage _meta;
   };
   static_assert(sizeof(node_meta<>) == 8);

   using node_meta_type = node_meta<>;
   using temp_meta_type = node_meta<uint64_t>;
   static_assert(sizeof(node_meta_type) == sizeof(temp_meta_type));

}  // namespace arbtrie
