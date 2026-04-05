/// subtrees.cpp — Composable trees: store entire trees as values
#include <filesystem>
#include <iostream>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>

int main()
{
   auto dir = std::filesystem::temp_directory_path() / "psitri_subtrees";
   std::filesystem::remove_all(dir);
   auto db = psitri::database::open(dir);
   auto ws = db->start_write_session();

   // --- Build a subtree for user metadata ---
   auto meta_cursor = ws->create_write_cursor();
   meta_cursor->upsert("schema_version", "3");
   meta_cursor->upsert("created_by", "admin");
   meta_cursor->upsert("engine", "psitri");
   auto meta_root = meta_cursor->root();

   // --- Build the main tree and embed the subtree ---
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("users/alice", "engineer");
      tx.upsert("users/bob", "designer");
      tx.upsert("metadata", std::move(meta_root));  // subtree as a value
      tx.commit();
   }

   // --- Read back the subtree ---
   {
      auto tx = ws->start_transaction(0);

      if (tx.is_subtree("metadata"))
      {
         std::cout << "=== metadata subtree ===\n";
         auto sub_cursor = tx.get_subtree_cursor("metadata");
         auto rc         = sub_cursor.read_cursor();
         rc.seek_begin();
         while (!rc.is_end())
         {
            auto val = rc.value<std::string>();
            std::cout << "  " << rc.key() << " = " << val.value_or("?") << "\n";
            rc.next();
         }
      }

      // Regular keys work alongside subtree keys
      std::cout << "\n=== top-level keys ===\n";
      auto rc = tx.read_cursor();
      rc.seek_begin();
      while (!rc.is_end())
      {
         if (rc.is_subtree())
            std::cout << "  " << rc.key() << " = [subtree]\n";
         else
         {
            auto val = rc.value<std::string>();
            std::cout << "  " << rc.key() << " = " << val.value_or("?") << "\n";
         }
         rc.next();
      }

      tx.abort();
   }

   std::filesystem::remove_all(dir);
   return 0;
}
