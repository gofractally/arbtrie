/// range_operations.cpp — O(log n) range counting and range deletion
#include <filesystem>
#include <iostream>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>

int main()
{
   auto dir = std::filesystem::temp_directory_path() / "psitri_ranges";
   std::filesystem::remove_all(dir);
   auto db = psitri::database::create(dir);
   auto ws = db->start_write_session();

   // Insert 10,000 keys: "key/00000" .. "key/09999"
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < 10000; ++i)
      {
         char key[16], val[16];
         snprintf(key, sizeof(key), "key/%05d", i);
         snprintf(val, sizeof(val), "val-%d", i);
         tx.upsert(key, val);
      }
      tx.commit();
   }

   // --- O(log n) range count ---
   // count_keys uses descendant counters in inner nodes — no leaf scanning
   auto rs     = db->start_read_session();
   auto cursor = rs->create_cursor(0);

   uint64_t total = cursor.count_keys();
   uint64_t range = cursor.count_keys("key/01000", "key/02000");

   std::cout << "Total keys:              " << total << "\n";
   std::cout << "Keys in [01000, 02000):  " << range << "\n";

   // --- O(log n) range deletion ---
   {
      auto tx = ws->start_transaction(0);

      uint64_t removed = tx.remove_range("key/05000", "key/06000");
      std::cout << "Removed in [05000, 06000): " << removed << "\n";

      tx.commit();
   }

   // Verify the count dropped
   cursor = rs->create_cursor(0);
   std::cout << "Total after removal:     " << cursor.count_keys() << "\n";

   std::filesystem::remove_all(dir);
   return 0;
}
