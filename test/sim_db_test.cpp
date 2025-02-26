#include <catch2/catch_all.hpp>
#include <iostream>
#include <string>
#include <vector>

#include "sim_db.hpp"

TEST_CASE("sim_db basic operations", "[sim_db]")
{
   sim::database db;
   auto          ws   = db.start_write_session();
   auto          root = ws.create_root();

   SECTION("Insert and retrieve values")
   {
      ws.insert(root, "key1", "value1");
      ws.insert(root, "key2", "value2");
      ws.insert(root, "key3", "value3");

      std::vector<char> buffer;
      REQUIRE(ws.get(root, "key1", &buffer) == 6);
      REQUIRE(std::string(buffer.data(), buffer.size()) == "value1");

      REQUIRE(ws.get(root, "key2", &buffer) == 6);
      REQUIRE(std::string(buffer.data(), buffer.size()) == "value2");

      REQUIRE(ws.get(root, "key3", &buffer) == 6);
      REQUIRE(std::string(buffer.data(), buffer.size()) == "value3");

      REQUIRE(ws.get(root, "nonexistent", &buffer) == -1);
   }

   SECTION("Update values")
   {
      ws.insert(root, "key1", "value1");
      REQUIRE(ws.update(root, "key1", "newvalue1") == 6);

      std::vector<char> buffer;
      REQUIRE(ws.get(root, "key1", &buffer) == 9);
      REQUIRE(std::string(buffer.data(), buffer.size()) == "newvalue1");

      REQUIRE_THROWS(ws.update(root, "nonexistent", "value"));
   }

   SECTION("Upsert values")
   {
      REQUIRE(ws.upsert(root, "key1", "value1") == -1);    // Insert
      REQUIRE(ws.upsert(root, "key1", "newvalue1") == 6);  // Update

      std::vector<char> buffer;
      REQUIRE(ws.get(root, "key1", &buffer) == 9);
      REQUIRE(std::string(buffer.data(), buffer.size()) == "newvalue1");
   }

   SECTION("Remove values")
   {
      ws.insert(root, "key1", "value1");
      ws.insert(root, "key2", "value2");

      REQUIRE(ws.remove(root, "key1") == 6);
      REQUIRE(ws.remove(root, "nonexistent") == -1);

      std::vector<char> buffer;
      REQUIRE(ws.get(root, "key1", &buffer) == -1);
      REQUIRE(ws.get(root, "key2", &buffer) == 6);
   }

   SECTION("Count keys")
   {
      ws.insert(root, "key1", "value1");
      ws.insert(root, "key2", "value2");
      ws.insert(root, "key3", "value3");

      REQUIRE(ws.count_keys(root) == 3);
      REQUIRE(ws.count_keys(root, "key2") == 2);          // key2 and key3
      REQUIRE(ws.count_keys(root, "key2", "key3") == 1);  // Only key2
   }

   SECTION("Subtree operations")
   {
      auto subtree = ws.create_root();
      ws.insert(subtree, "subkey1", "subvalue1");
      ws.insert(subtree, "subkey2", "subvalue2");

      ws.insert(root, "key1", "value1");
      ws.insert(root, "subtree", subtree);

      // Verify subtree can be retrieved
      auto retrieved_subtree = ws.get_subtree(root, "subtree");
      REQUIRE(retrieved_subtree.has_value());

      std::vector<char> buffer;
      REQUIRE(ws.get(retrieved_subtree.value(), "subkey1", &buffer) == 9);
      REQUIRE(std::string(buffer.data(), buffer.size()) == "subvalue1");

      // Count should include subtrees as leaf nodes, but not their contents
      REQUIRE(ws.count_keys(root) == 2);  // key1, subtree (not including subtree contents)
   }
}

TEST_CASE("iterator operations", "[iterator]")
{
   sim::database db;
   auto          rs   = db.start_read_session();
   auto          ws   = db.start_write_session();
   auto          root = ws.create_root();

   // Populate the tree
   ws.insert(root, "a", "value_a");
   ws.insert(root, "b", "value_b");
   ws.insert(root, "c", "value_c");
   ws.insert(root, "d", "value_d");

   auto it = rs.start_iterator(root);

   SECTION("Basic iterator navigation")
   {
      REQUIRE(it.is_start());
      REQUIRE_FALSE(it.is_end());
      REQUIRE_FALSE(it.valid());

      // Move to first element
      REQUIRE(it.next());
      REQUIRE_FALSE(it.is_start());
      REQUIRE_FALSE(it.is_end());
      REQUIRE(it.valid());
      REQUIRE(it.key() == "a");

      // Move forward
      REQUIRE(it.next());
      REQUIRE(it.key() == "b");
      REQUIRE(it.next());
      REQUIRE(it.key() == "c");
      REQUIRE(it.next());
      REQUIRE(it.key() == "d");

      // Move to end
      REQUIRE_FALSE(it.next());
      REQUIRE(it.is_end());

      // Move backward
      REQUIRE(it.prev());
      REQUIRE(it.key() == "d");
      REQUIRE(it.prev());
      REQUIRE(it.key() == "c");
      REQUIRE(it.prev());
      REQUIRE(it.key() == "b");
      REQUIRE(it.prev());
      REQUIRE(it.key() == "a");

      // Move to start
      REQUIRE_FALSE(it.prev());
      REQUIRE(it.is_start());
   }

   SECTION("Iterator find")
   {
      REQUIRE(it.find("c"));
      REQUIRE(it.key() == "c");

      REQUIRE_FALSE(it.find("nonexistent"));
      REQUIRE(it.is_end());
   }

   SECTION("Iterator value retrieval")
   {
      REQUIRE(it.find("b"));

      std::vector<char> buffer;
      REQUIRE(it.value(buffer) == 7);
      REQUIRE(std::string(buffer.data(), buffer.size()) == "value_b");
   }
}

TEST_CASE("transaction operations", "[transaction]")
{
   sim::database db;
   auto          ws = db.start_write_session();

   SECTION("Transaction commit")
   {
      auto tx = ws.start_transaction();

      tx.insert("key1", "value1");
      tx.insert("key2", "value2");

      tx.commit();

      // Verify changes were committed
      auto rs   = db.start_read_session();
      auto root = rs.get_root();

      std::vector<char> buffer;
      REQUIRE(rs.get(root, "key1", &buffer) == 6);
      REQUIRE(std::string(buffer.data(), buffer.size()) == "value1");
   }

   SECTION("Transaction abort")
   {
      // First add some data
      {
         auto tx = ws.start_transaction();
         tx.insert("initial", "value");
         tx.commit();
      }

      // Now start a transaction and abort it
      {
         auto tx = ws.start_transaction();
         tx.insert("key1", "value1");
         tx.abort();
      }

      // Verify changes were not committed
      auto rs   = db.start_read_session();
      auto root = rs.get_root();

      std::vector<char> buffer;
      REQUIRE(rs.get(root, "initial", &buffer) == 5);
      REQUIRE(rs.get(root, "key1", &buffer) == -1);
   }

   SECTION("Transaction operations")
   {
      auto tx = ws.start_transaction();

      // Insert
      tx.insert("key1", "value1");

      // Upsert
      REQUIRE(tx.upsert("key1", "newvalue1") == 6);
      REQUIRE(tx.upsert("key2", "value2") == -1);

      // Remove
      REQUIRE(tx.remove("key1") == 9);

      tx.commit();

      // Verify final state
      auto rs   = db.start_read_session();
      auto root = rs.get_root();

      std::vector<char> buffer;
      REQUIRE(rs.get(root, "key1", &buffer) == -1);
      REQUIRE(rs.get(root, "key2", &buffer) == 6);
   }
}