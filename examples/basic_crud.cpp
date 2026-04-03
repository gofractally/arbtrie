/// basic_crud.cpp — Create a database, insert/get/update/remove keys
#include <filesystem>
#include <iostream>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>

int main()
{
   // Create (or open) a database in a temporary directory
   auto dir = std::filesystem::temp_directory_path() / "psitri_basic_crud";
   std::filesystem::remove_all(dir);
   auto db = psitri::database::create(dir);

   // All writes go through a write session (one per thread)
   auto ws = db->start_write_session();

   // --- Transactions ---
   // start_transaction acquires root 0 and commits atomically
   {
      auto tx = ws->start_transaction(0);

      tx.upsert("alice", "engineer");
      tx.upsert("bob", "designer");
      tx.upsert("carol", "manager");

      tx.commit();  // atomic — all three keys become visible at once
   }

   // --- Point lookups ---
   {
      auto tx = ws->start_transaction(0);

      if (auto val = tx.get<std::string>("alice"))
         std::cout << "alice = " << *val << "\n";

      if (auto val = tx.get<std::string>("dave"))
         std::cout << "dave = " << *val << "\n";
      else
         std::cout << "dave not found\n";

      tx.abort();  // read-only, nothing to commit
   }

   // --- Update and remove ---
   {
      auto tx = ws->start_transaction(0);

      tx.update("alice", "senior engineer");
      tx.remove("bob");

      // upsert always succeeds (insert or update)
      tx.upsert("carol", "updated value");

      tx.commit();
   }

   // --- Verify final state ---
   {
      auto tx = ws->start_transaction(0);

      auto alice = tx.get<std::string>("alice");
      auto bob   = tx.get<std::string>("bob");
      auto carol = tx.get<std::string>("carol");

      std::cout << "alice = " << alice.value_or("(gone)") << "\n";
      std::cout << "bob   = " << bob.value_or("(gone)") << "\n";
      std::cout << "carol = " << carol.value_or("(gone)") << "\n";

      tx.abort();
   }

   std::filesystem::remove_all(dir);
   return 0;
}
