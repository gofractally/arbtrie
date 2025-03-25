#include <arbtrie/database.hpp>
#include <arbtrie/interprocess_mutex.hpp>

namespace arbtrie
{
   // TODO: implement this function
   bool database::validate()
   {
      // make sure all nodes are reachable
      // make sure no stuck modify bits
      return false;
   }

   void recusive_retain_all(object_ref&& r)
   {
      r.retain();
      auto retain_id = [&](id_address b) { recusive_retain_all(r.rlock().get(b)); };
      cast_and_call(r.header(), [&](const auto* ptr) { ptr->visit_branches(retain_id); });
   }

   /**
 *  Data is stored in "segments" and each segment has a
 *  data range that has been synced, once synced it is imutable.
 *  
 *  As new segments are allocated they are assigned an age, the 
 *  higher the age the more resent the writes. 
 *
 *  A node may appear on multiple segments of different ages,
 *  but only the one on the highest age is valid. Each node
 *  has the address/object id associated with it which can
 *  be used to re-establish the meta_node.
 *
 *  Start with the newest segment (highest age) and work to the oldest segment,
 *  as you come across objects, set their location in the node meta
 *  database and increment their reference count to 1. If a location
 *  has already been set then skip it because there is clearly a
 *  newer value already set, be sure to update the "free space" stats.
 *
 *  Starting with the top root, recursively retain all nodes. At this
 *  point all reachable nodes have a ref count of 2+. Scan through the
 *  node_meta and decrement reference count of all nodes while putting
 *  any node_meta with refcount of 0 or 1 into the free list.
 *
 *  Recovery Modes:
 *    1. OS / Hardware Recovery - 
 *         - Assumes Last user was using sync mode
 *         - rebuild node_meta from segments
 *         - optional checksum validation
 *    2. App Crash Recovery 
 *         - Last user was using OS sync
 *         - assumes the OS/Hardware didn't fail
 *         - only resets reference counts to what is reachable
 *         - recovers leaked memory
 *    3. User was updating the top-root in place and therefore
 *        the tree is potentially corrupt and partially written
 *         (bad,bad user....)
 *         - similar to App Crash Recovery... except..
 *         - check integrity of relevant nodes
 *         - produce report and/or sandbox subtree 
 */
   void database::recover(recover_args args)
   {
      ARBTRIE_WARN("Recovering... reset meta nodes!");

      // Stop all background threads before recovery
      bool threads_were_running = _sega.stop_background_threads();

      // all recovered nodes have a ref of 1
      _sega.reset_meta_nodes(args);

      // all refs of reachable nodes now >= 2
      {
         auto s     = start_read_session();
         auto state = s._segas->lock();
         for (int i = 0; i < num_top_roots; ++i)
         {
            auto r = s.get_root(i);
            if (r.address())
               recusive_retain_all(state.get(r.address()));
         }
      }

      // all refs that are > 0 go down by 1
      // if a ref was 1 it is added to free list
      _sega.release_unreachable();

      // Restart threads that were running before recovery
      if (threads_were_running)
         _sega.start_background_threads();
   }
   void seg_allocator::reset_reference_counts()
   {
      _id_alloc.reset_all_refs();
   }

   void database::reset_reference_counts()
   {
      // set all refs > 1 to 1
      _sega.reset_reference_counts();

      // retain all reachable nodes, sending reachable refs to 2+
      {
         auto s     = start_read_session();
         auto state = s._segas->lock();
         for (int i = 0; i < num_top_roots; ++i)
         {
            auto r = s.get_root(i);
            if (r.address())
               recusive_retain_all(state.get(r.address()));
         }
      }

      // all refs that are > 0 go down by 1
      // if a ref was 1 it is added to free list
      // free list gets reset
      _sega.release_unreachable();
   }

   void seg_allocator::reset_meta_nodes(recover_args args)
   {
      _id_alloc.clear_all();

      std::vector<int> age_index;
      age_index.resize(_block_alloc.num_blocks());
      for (int i = 0; i < age_index.size(); ++i)
         age_index[i] = i;

      std::sort(
          age_index.begin(), age_index.end(), [&](int a, int b)
          { return get_segment(a)->_provider_sequence > get_segment(b)->_provider_sequence; });

      _mapped_state->_segment_provider.free_segments.reset();

      // Reset mlock_segments bitmap
      _mapped_state->_segment_provider.mlock_segments.reset();

      // Clear all segment meta is_pinned bits to ensure consistency with the bitmap
      for (size_t i = 0; i < _block_alloc.num_blocks(); ++i)
         _mapped_state->_segment_data.set_pinned(i, false);

      _mapped_state->_segment_provider.ready_pinned_segments.clear();
      _mapped_state->_segment_provider.ready_unpinned_segments.clear();

      int next_free_seg = 0;
      for (auto i : age_index)
      {
         if (get_segment(i)->_provider_sequence < 0)
         {
            _mapped_state->_segment_provider.free_segments.set(i);
            continue;
         }
         auto seg = get_segment(i);
         auto send =
             (node_header*)((char*)seg + std::min<uint32_t>(segment_size, seg->get_alloc_pos()));

         node_header* foo = (node_header*)(seg);

         int free_space = 0;
         while (foo < send and foo->address())
         {
            // TODO:
            //   validate foo < last sync position,
            //      data written beyond sync should be unreachable from top root
            //   validate size and type are reasonable
            //   optionally validate checksum (if set), store checksum if not
            //
            node_meta_type& met = _id_alloc.get_or_alloc(foo->address());
            auto            loc = met.loc();

            //  null or in the same segment it should be updated because
            //  objects in a segment are ordered from oldest to newest
            //  and we are iterating segments from newest to oldest
            if ((not loc.cacheline()) or (get_segment_num(loc) == i))
            {
               if (get_segment_num(loc) == i)
                  free_space +=
                      ((const node_header*)((const char*)seg + get_segment_offset(loc)))->_nsize;
               met.store(temp_meta_type()
                             .set_loc(node_location::from_absolute_address(
                                 i * segment_size + ((char*)foo) - (char*)seg))
                             .set_ref(1),
                         std::memory_order_relaxed);
            }
            else
            {
               free_space += foo->_nsize;
            }

            foo = foo->next();
         }
         // TODO:
         //   make sure all segments indicate they are fully synced to the last foo position
         //   make sure _alloc_pos of each segment is in good working order
         //   make sure free space calculations are up to date for the segment
      }
      _mapped_state->clean_exit_flag.store(false);
      _mapped_state->_segment_provider._next_alloc_seq.store(
          get_segment(age_index[0])->_provider_sequence + 1);
   }

   void seg_allocator::release_unreachable()
   {
      _id_alloc.release_unreachable();
   }

}  // namespace arbtrie
