/// snapshots.cpp — Zero-cost snapshots via copy-on-write
#include <filesystem>
#include <iostream>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>

int main()
{
   auto dir = std::filesystem::temp_directory_path() / "psitri_snapshots";
   std::filesystem::remove_all(dir);
   auto db = psitri::database::open(dir);
   auto ws = db->start_write_session();

   // Write initial data
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("balance/alice", "1000");
      tx.upsert("balance/bob", "500");
      tx.commit();
   }

   // Take a snapshot — O(1), just increments a reference count on the root
   auto rs       = db->start_read_session();
   auto snapshot = rs->create_cursor(0);

   // Mutate the live tree
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("balance/alice", "800");
      tx.upsert("balance/bob", "700");
      tx.upsert("balance/carol", "300");
      tx.commit();
   }

   // Snapshot still sees the old state (copy-on-write)
   std::cout << "=== Snapshot (before mutation) ===\n";
   snapshot.seek_begin();
   while (!snapshot.is_end())
   {
      auto val = snapshot.value<std::string>();
      std::cout << "  " << snapshot.key() << " = " << val.value_or("?") << "\n";
      snapshot.next();
   }

   // Current state sees the new values
   std::cout << "\n=== Current state (after mutation) ===\n";
   auto current = rs->create_cursor(0);
   current.seek_begin();
   while (!current.is_end())
   {
      auto val = current.value<std::string>();
      std::cout << "  " << current.key() << " = " << val.value_or("?") << "\n";
      current.next();
   }

   // Snapshot is released automatically when the cursor goes out of scope
   // — the COW tree nodes shared between snapshot and current are freed
   // only when the last reference is released

   std::filesystem::remove_all(dir);
   return 0;
}
