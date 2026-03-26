#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <sal/allocator.hpp>
#include <string>

using namespace psitri;

namespace
{
   std::string make_key(int i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "key_%08d", i);
      return buf;
   }

   std::string encode_u64(uint64_t v)
   {
      std::string s(8, '\0');
      memcpy(s.data(), &v, 8);
      return s;
   }

   uint64_t decode_u64(const std::string& s)
   {
      uint64_t v = 0;
      if (s.size() >= 8)
         memcpy(&v, s.data(), 8);
      return v;
   }

   /// Count live objects (ref > 0) and sum their sizes.
   std::pair<uint64_t, uint64_t> count_live(write_session& ses)
   {
      uint64_t count = 0, bytes = 0;
      ses.for_each_live_object(
          [&](sal::ptr_address, uint32_t ref, const sal::alloc_header* obj)
          {
             if (ref > 0)
             {
                ++count;
                bytes += obj->size();
             }
          });
      return {count, bytes};
   }
}  // namespace

TEST_CASE("no leaked references after many small commits", "[allocator]")
{
   const std::string dir = "freed_space_testdb";
   std::filesystem::remove_all(dir);

   {
      auto db  = database::create(dir);
      auto ses = db->start_write_session();

      // Use small values like the bank benchmark (8-byte uint64_t)
      // Use enough keys to fill multiple segments (32MB each)
      const int    num_keys   = 100000;
      const uint64_t initial_balance = 1000000;

      // Bulk load
      {
         auto tx = ses->start_transaction(0);
         for (int i = 0; i < num_keys; ++i)
         {
            auto val = encode_u64(initial_balance);
            tx.upsert(to_key_view(make_key(i)), to_value_view(val));
         }
         tx.commit();
      }

      db->wait_for_compactor();
      auto [count_after_load, bytes_after_load] = count_live(*ses);
      uint64_t reachable_after_load             = db->reachable_size();

      WARN("after load: live=" << count_after_load
           << " bytes=" << bytes_after_load
           << " reachable=" << reachable_after_load);

      // Match bank benchmark pattern: batch_size=1, each transfer is its own transaction.
      // This is the critical difference from the previous test.
      std::optional<transaction> tx_holder;

      const int num_transfers = 5000;
      int       next_log_key  = num_keys;
      int       leaked_count  = 0;
      uint64_t  pending_before = 0;
      for (int i = 0; i < num_transfers; ++i)
      {
         // begin_batch
         tx_holder.emplace(ses->start_transaction(0));

         // Single transfer: read 2, write 2, insert 1 log entry
         int src = i % num_keys;
         int dst = (src + 1) % num_keys;
         auto src_val = tx_holder->get<std::string>(to_key_view(make_key(src)));
         auto dst_val = tx_holder->get<std::string>(to_key_view(make_key(dst)));

         uint64_t amount = 1;
         if (src_val && dst_val)
         {
            uint64_t sb = decode_u64(*src_val);
            uint64_t db_val = decode_u64(*dst_val);
            if (sb >= amount)
            {
               tx_holder->upsert(to_key_view(make_key(src)), to_value_view(encode_u64(sb - amount)));
               tx_holder->upsert(to_key_view(make_key(dst)), to_value_view(encode_u64(db_val + amount)));
            }
         }
         // Log entry
         auto log_val = encode_u64(i);
         tx_holder->upsert(to_key_view(make_key(next_log_key++)), to_value_view(log_val));

         // commit_batch
         tx_holder->commit();
         tx_holder.reset();

         // Check pending releases after each commit to verify queue growth
         if (i < 5 || (i + 1) % 1000 == 0)
         {
            auto pending = ses->get_pending_release_count();
            if (i < 5)
               WARN("after commit " << i << ": pending_releases=" << pending
                    << " delta=" << (pending - pending_before));
            pending_before = pending;
         }

         // Check for leaks periodically BEFORE the compactor cleans up
         if ((i + 1) % 1000 == 0)
         {
            auto [live_count, live_bytes] = count_live(*ses);
            uint64_t reachable = db->reachable_size();
            WARN("after " << (i + 1) << " transfers: live=" << live_count
                 << " bytes=" << live_bytes << " reachable=" << reachable);

            // Check freed space audit
            auto audit = db->audit_freed_space();
            uint64_t total_estimated = 0, total_actual_dead = 0, total_actual_live = 0;
            for (auto& a : audit)
            {
               total_estimated   += a.estimated_freed;
               total_actual_dead += a.actual_dead;
               total_actual_live += a.actual_live;
            }
            if (audit.size() > 0)
            {
               WARN("  freed audit: " << audit.size() << " ro segs"
                    << " estimated=" << total_estimated
                    << " actual_dead=" << total_actual_dead
                    << " actual_live=" << total_actual_live);
            }

            if (live_bytes > reachable * 2)
            {
               ++leaked_count;
               WARN("  *** LEAK DETECTED: live_bytes/reachable = "
                    << (double)live_bytes / reachable);
            }
         }
      }

      db->wait_for_compactor();
      auto [count_final, bytes_final] = count_live(*ses);
      uint64_t reachable_final        = db->reachable_size();

      WARN("final: live=" << count_final
           << " bytes=" << bytes_final
           << " reachable=" << reachable_final);

      double leak_ratio = (double)bytes_final / reachable_final;
      WARN("final leak ratio: " << leak_ratio);

      // Live objects should not vastly exceed reachable
      CHECK(bytes_final <= reachable_final * 2);
   }

   std::filesystem::remove_all(dir);
}
