/**
 * Ethereum state workload benchmark — psitrimdbx vs native MDBX.
 *
 * Simulates Silkworm's database access patterns:
 *   - Account table (DUPSORT): 20-byte address key, variable-length value
 *   - Storage table (DUPSORT): 20-byte address + 32-byte slot key
 *   - Code table: 32-byte hash key, variable-length bytecode value
 *   - Headers table: 8-byte block number key, 32-byte hash value
 *
 * Workload mix (per "block"):
 *   70% reads, 20% updates, 8% inserts, 2% deletes
 *
 * Build:
 *   cmake -DBUILD_SILKWORM_CASE_STUDY=ON ...
 *   ninja eth-state-bench
 *
 * Usage:
 *   ./eth-state-bench [--ops N] [--blocks N] [--dir PATH]
 */

#include <mdbx.h>
#include <mdbx.h++>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static constexpr int    DEFAULT_OPS_PER_BLOCK = 2000;
static constexpr int    DEFAULT_BLOCKS        = 500;
static constexpr size_t NUM_PRELOAD_ACCOUNTS  = 100'000;

struct config
{
   int         ops_per_block = DEFAULT_OPS_PER_BLOCK;
   int         blocks        = DEFAULT_BLOCKS;
   std::string dir           = "/tmp/eth_state_bench";
};

static config parse_args(int argc, char** argv)
{
   config c;
   for (int i = 1; i < argc; i++)
   {
      std::string arg = argv[i];
      if (arg == "--ops" && i + 1 < argc)
         c.ops_per_block = std::stoi(argv[++i]);
      else if (arg == "--blocks" && i + 1 < argc)
         c.blocks = std::stoi(argv[++i]);
      else if (arg == "--dir" && i + 1 < argc)
         c.dir = argv[++i];
   }
   return c;
}

static std::string random_bytes(std::mt19937_64& rng, size_t len)
{
   std::string s(len, '\0');
   for (size_t i = 0; i < len; i += 8)
   {
      uint64_t r = rng();
      size_t   n = std::min(len - i, size_t(8));
      std::memcpy(s.data() + i, &r, n);
   }
   return s;
}

static std::string make_address(uint64_t id)
{
   std::string addr(20, '\0');
   std::memcpy(addr.data() + 12, &id, sizeof(id));
   return addr;
}

static std::string make_storage_key(uint64_t account_id, uint64_t slot)
{
   std::string key(52, '\0');  // 20 addr + 32 slot
   std::memcpy(key.data() + 12, &account_id, sizeof(account_id));
   std::memcpy(key.data() + 44, &slot, sizeof(slot));
   return key;
}

struct bench_result
{
   double       total_seconds   = 0;
   double       reads_per_sec   = 0;
   double       writes_per_sec  = 0;
   uint64_t     total_reads     = 0;
   uint64_t     total_writes    = 0;
   uint64_t     total_deletes   = 0;
};

static bench_result run_benchmark(const config& cfg)
{
   fs::remove_all(cfg.dir);
   fs::create_directories(cfg.dir);

   mdbx::env_managed::create_parameters cp;
   mdbx::env::operate_parameters        op;
   op.max_maps = 16;

   mdbx::env_managed db(cfg.dir.c_str(), cp, op);

   // Create tables matching Silkworm schema
   mdbx::map_handle accounts_map, storage_map, code_map, headers_map;
   {
      auto txn     = db.start_write();
      accounts_map = txn.create_map("Account", mdbx::key_mode::usual, mdbx::value_mode::multi);
      storage_map  = txn.create_map("Storage", mdbx::key_mode::usual, mdbx::value_mode::multi);
      code_map     = txn.create_map("Code");
      headers_map  = txn.create_map("Header");
      txn.commit();
   }

   // Preload accounts
   std::mt19937_64 rng(42);
   {
      auto txn = db.start_write();
      for (size_t i = 0; i < NUM_PRELOAD_ACCOUNTS; i++)
      {
         auto addr    = make_address(i);
         auto balance = random_bytes(rng, 32);
         txn.upsert(accounts_map, mdbx::slice(addr), mdbx::slice(balance));

         // Some accounts have storage
         if (i % 10 == 0)
         {
            for (int s = 0; s < 5; s++)
            {
               auto skey = make_storage_key(i, s);
               auto sval = random_bytes(rng, 32);
               txn.upsert(storage_map, mdbx::slice(skey), mdbx::slice(sval));
            }
         }

         // Some accounts have code
         if (i % 20 == 0)
         {
            auto hash = random_bytes(rng, 32);
            auto code = random_bytes(rng, 256 + rng() % 2048);
            txn.upsert(code_map, mdbx::slice(hash), mdbx::slice(code));
         }
      }
      txn.commit();
   }

   {
      auto txn  = db.start_read();
      auto stat = txn.get_map_stat(accounts_map);
      std::cout << "Post-preload account entries: " << stat.ms_entries << "\n";
   }
   std::cout << "Preloaded " << NUM_PRELOAD_ACCOUNTS << " accounts\n";

   // Benchmark: simulate block processing
   bench_result result;
   auto         start = std::chrono::steady_clock::now();

   for (int block = 0; block < cfg.blocks; block++)
   {
      auto txn = db.start_write();

      for (int op_i = 0; op_i < cfg.ops_per_block; op_i++)
      {
         double r       = std::uniform_real_distribution<>(0, 1)(rng);
         uint64_t acct  = rng() % NUM_PRELOAD_ACCOUNTS;
         auto     addr  = make_address(acct);

         if (r < 0.70)
         {
            // Read: account lookup
            try { txn.get(accounts_map, mdbx::slice(addr)); }
            catch (mdbx::not_found&) {}
            result.total_reads++;
         }
         else if (r < 0.90)
         {
            // Update: modify balance
            auto new_bal = random_bytes(rng, 32);
            txn.upsert(accounts_map, mdbx::slice(addr), mdbx::slice(new_bal));
            result.total_writes++;
         }
         else if (r < 0.98)
         {
            // Insert: new account (beyond preloaded range)
            auto new_addr = make_address(NUM_PRELOAD_ACCOUNTS + block * cfg.ops_per_block + op_i);
            auto balance  = random_bytes(rng, 32);
            txn.upsert(accounts_map, mdbx::slice(new_addr), mdbx::slice(balance));
            result.total_writes++;
         }
         else
         {
            // Delete: remove account
            txn.erase(accounts_map, mdbx::slice(addr));
            result.total_deletes++;
         }
      }

      // Write block header
      uint64_t    bn = block;
      std::string block_key(8, '\0');
      std::memcpy(block_key.data(), &bn, sizeof(bn));
      auto hash = random_bytes(rng, 32);
      txn.upsert(headers_map, mdbx::slice(block_key), mdbx::slice(hash));

      txn.commit();

      if (cfg.blocks <= 20)
      {
         auto rtxn = db.start_read();
         auto stat = rtxn.get_map_stat(accounts_map);
         auto sstat = rtxn.get_map_stat(storage_map);
         auto cstat = rtxn.get_map_stat(code_map);
         auto hstat = rtxn.get_map_stat(headers_map);
         std::cout << "  Block " << block << ": acct=" << stat.ms_entries
                   << " stor=" << sstat.ms_entries
                   << " code=" << cstat.ms_entries
                   << " hdr=" << hstat.ms_entries << "\n";
      }
   }

   auto end             = std::chrono::steady_clock::now();
   result.total_seconds = std::chrono::duration<double>(end - start).count();

   uint64_t total_ops    = result.total_reads + result.total_writes + result.total_deletes;
   result.reads_per_sec  = result.total_reads / result.total_seconds;
   result.writes_per_sec = (result.total_writes + result.total_deletes) / result.total_seconds;

   // Get final DB stats
   {
      auto txn  = db.start_read();
      auto stat = txn.get_map_stat(accounts_map);
      std::cout << "Final account entries: " << stat.ms_entries << "\n";
   }

   return result;
}

int main(int argc, char** argv)
{
   auto cfg = parse_args(argc, argv);

#ifndef BACKEND_NAME
#define BACKEND_NAME "psitrimdbx"
#endif
   std::cout << "=== Ethereum State Workload Benchmark (" BACKEND_NAME ") ===\n";
   std::cout << "Ops/block: " << cfg.ops_per_block << "\n";
   std::cout << "Blocks:    " << cfg.blocks << "\n";
   std::cout << "Directory: " << cfg.dir << "\n\n";

   auto result = run_benchmark(cfg);

   std::cout << "\n=== Results ===\n";
   std::cout << "Total time:    " << result.total_seconds << " s\n";
   std::cout << "Total reads:   " << result.total_reads << "\n";
   std::cout << "Total writes:  " << result.total_writes << "\n";
   std::cout << "Total deletes: " << result.total_deletes << "\n";
   std::cout << "Reads/sec:     " << static_cast<uint64_t>(result.reads_per_sec) << "\n";
   std::cout << "Writes/sec:    " << static_cast<uint64_t>(result.writes_per_sec) << "\n";

   return 0;
}
