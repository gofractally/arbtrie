/// cursor_iteration.cpp — Iterate keys forward, backward, and by prefix
#include <filesystem>
#include <iostream>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>

int main()
{
   auto dir = std::filesystem::temp_directory_path() / "psitri_cursor";
   std::filesystem::remove_all(dir);
   auto db = psitri::database::create(dir);
   auto ws = db->start_write_session();

   // Populate some data
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("fruit/apple", "red");
      tx.upsert("fruit/banana", "yellow");
      tx.upsert("fruit/cherry", "red");
      tx.upsert("fruit/date", "brown");
      tx.upsert("veggie/carrot", "orange");
      tx.upsert("veggie/pea", "green");
      tx.commit();
   }

   // Read-only cursor from a read session
   auto rs     = db->start_read_session();
   auto cursor = rs->create_cursor(0);

   // --- Forward iteration over all keys ---
   std::cout << "=== All keys (forward) ===\n";
   cursor.seek_begin();
   while (!cursor.is_end())
   {
      auto val = cursor.value<std::string>();
      std::cout << "  " << cursor.key() << " = " << val.value_or("?") << "\n";
      cursor.next();
   }

   // --- Reverse iteration ---
   std::cout << "\n=== All keys (reverse) ===\n";
   cursor.seek_last();
   while (!cursor.is_rend())
   {
      std::cout << "  " << cursor.key() << "\n";
      cursor.prev();
   }

   // --- Prefix scan ---
   std::cout << "\n=== Keys with prefix 'fruit/' ===\n";
   cursor.lower_bound("fruit/");
   while (!cursor.is_end() && cursor.key().starts_with("fruit/"))
   {
      std::cout << "  " << cursor.key() << "\n";
      cursor.next();
   }

   // --- lower_bound seek ---
   std::cout << "\n=== lower_bound('fruit/c') ===\n";
   cursor.lower_bound("fruit/c");
   if (!cursor.is_end())
      std::cout << "  first key >= 'fruit/c': " << cursor.key() << "\n";

   std::filesystem::remove_all(dir);
   return 0;
}
