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
   auto meta = ws->create_temporary_tree();
   auto meta_tx = ws->start_write_transaction(std::move(meta));
   meta_tx.upsert("schema_version", "3");
   meta_tx.upsert("created_by", "admin");
   meta_tx.upsert("engine", "psitri");
   meta = meta_tx.get_tree();

   // --- Build the main tree and embed the subtree ---
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("users/alice", "engineer");
      tx.upsert("users/bob", "designer");
      tx.upsert_subtree("metadata", std::move(meta));  // subtree as a value
      tx.commit();
   }

   // --- Read back the subtree ---
   {
      auto root = ws->get_root(0);

      auto metadata = root.get_subtree("metadata");
      if (metadata)
      {
         std::cout << "=== metadata subtree ===\n";
         auto rc = metadata.snapshot_cursor();
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
      auto rc = root.snapshot_cursor();
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
   }

   std::filesystem::remove_all(dir);
   return 0;
}
