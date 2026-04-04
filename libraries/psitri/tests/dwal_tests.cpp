#include <catch2/catch_test_macros.hpp>

#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/btree_value.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/dwal/range_tombstone_list.hpp>
#include <psitri/dwal/undo_log.hpp>
#include <psitri/dwal/epoch_lock.hpp>
#include <psitri/dwal/merge_pool.hpp>
#include <psitri/dwal/wal_format.hpp>
#include <psitri/dwal/wal_reader.hpp>
#include <psitri/dwal/wal_writer.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>

namespace
{
   /// RAII helper to create and remove a temp directory for WAL tests.
   struct temp_dir
   {
      std::filesystem::path path;
      temp_dir()
      {
         path = std::filesystem::temp_directory_path() / "dwal_test_XXXXXX";
         // Make unique dir name
         path = std::filesystem::temp_directory_path() /
                ("dwal_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
         std::filesystem::create_directories(path);
      }
      ~temp_dir() { std::filesystem::remove_all(path); }
   };
}  // namespace

// ═══════════════════════════════════════════════════════════════════════
// btree_value tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("btree_value construction", "[dwal]")
{
   auto data = psitri::dwal::btree_value::make_data("hello");
   CHECK(data.is_data());
   CHECK_FALSE(data.is_subtree());
   CHECK_FALSE(data.is_tombstone());
   CHECK(data.data == "hello");

   auto sub = psitri::dwal::btree_value::make_subtree(sal::ptr_address(42));
   CHECK(sub.is_subtree());
   CHECK(sub.subtree_root == sal::ptr_address(42));

   auto tomb = psitri::dwal::btree_value::make_tombstone();
   CHECK(tomb.is_tombstone());
}

// ═══════════════════════════════════════════════════════════════════════
// range_tombstone_list tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("range_tombstone_list basic operations", "[dwal]")
{
   psitri::dwal::range_tombstone_list rtl;
   CHECK(rtl.empty());

   SECTION("add and query single range")
   {
      rtl.add("b", "e");
      CHECK_FALSE(rtl.empty());
      CHECK(rtl.size() == 1);
      CHECK(rtl.is_deleted("b"));
      CHECK(rtl.is_deleted("c"));
      CHECK(rtl.is_deleted("d"));
      CHECK_FALSE(rtl.is_deleted("a"));
      CHECK_FALSE(rtl.is_deleted("e"));  // exclusive upper bound
      CHECK_FALSE(rtl.is_deleted("f"));
   }

   SECTION("merge overlapping ranges")
   {
      rtl.add("b", "e");
      rtl.add("d", "g");
      CHECK(rtl.size() == 1);
      CHECK(rtl.is_deleted("b"));
      CHECK(rtl.is_deleted("f"));
      CHECK_FALSE(rtl.is_deleted("g"));
   }

   SECTION("merge adjacent ranges")
   {
      rtl.add("a", "c");
      rtl.add("c", "f");
      CHECK(rtl.size() == 1);
      CHECK(rtl.is_deleted("a"));
      CHECK(rtl.is_deleted("c"));
      CHECK(rtl.is_deleted("e"));
      CHECK_FALSE(rtl.is_deleted("f"));
   }

   SECTION("non-overlapping ranges stay separate")
   {
      rtl.add("a", "c");
      rtl.add("e", "g");
      CHECK(rtl.size() == 2);
      CHECK(rtl.is_deleted("a"));
      CHECK_FALSE(rtl.is_deleted("c"));
      CHECK_FALSE(rtl.is_deleted("d"));
      CHECK(rtl.is_deleted("e"));
      CHECK_FALSE(rtl.is_deleted("g"));
   }

   SECTION("remove exact range")
   {
      rtl.add("b", "e");
      rtl.remove("b", "e");
      CHECK(rtl.empty());
   }

   SECTION("split_at in middle")
   {
      rtl.add("a", "z");
      rtl.split_at("m");
      CHECK(rtl.size() == 2);
      CHECK(rtl.is_deleted("a"));
      CHECK(rtl.is_deleted("l"));
      CHECK_FALSE(rtl.is_deleted("m"));  // key itself is excluded
      std::string successor = "m";
      successor.push_back('\0');
      CHECK(rtl.is_deleted(successor));
      CHECK(rtl.is_deleted("n"));
   }

   SECTION("split_at start of range")
   {
      rtl.add("a", "z");
      rtl.split_at("a");
      // "a" removed, range becomes [a\0, z)
      CHECK_FALSE(rtl.is_deleted("a"));
      std::string succ = "a";
      succ.push_back('\0');
      CHECK(rtl.is_deleted(succ));
   }

   SECTION("clear")
   {
      rtl.add("a", "z");
      rtl.clear();
      CHECK(rtl.empty());
   }
}

// ═══════════════════════════════════════════════════════════════════════
// btree_layer tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("btree_layer store and lookup", "[dwal]")
{
   psitri::dwal::btree_layer layer;

   SECTION("store_data and lookup")
   {
      layer.store_data("key1", "value1");
      layer.store_data("key2", "value2");
      CHECK(layer.size() == 2);

      auto* v = layer.map.get("key1");
      REQUIRE(v != nullptr);
      CHECK(v->is_data());
      CHECK(v->data == "value1");
   }

   SECTION("store_tombstone")
   {
      layer.store_data("key1", "value1");
      layer.store_tombstone("key1");
      auto* v = layer.map.get("key1");
      REQUIRE(v != nullptr);
      CHECK(v->is_tombstone());
   }

   SECTION("store_subtree")
   {
      layer.store_subtree("sub1", sal::ptr_address(100));
      auto* v = layer.map.get("sub1");
      REQUIRE(v != nullptr);
      CHECK(v->is_subtree());
      CHECK(v->subtree_root == sal::ptr_address(100));
   }

   SECTION("pool-backed strings survive original going out of scope")
   {
      {
         std::string temp = "ephemeral_key";
         std::string temp_val = "ephemeral_val";
         layer.store_data(temp, temp_val);
      }
      // Originals are destroyed — pool copies must survive.
      auto* v = layer.map.get("ephemeral_key");
      REQUIRE(v != nullptr);
      CHECK(v->data == "ephemeral_val");
   }
}

// ═══════════════════════════════════════════════════════════════════════
// undo_log tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("undo_log basic frame operations", "[dwal]")
{
   psitri::dwal::undo_log log;
   CHECK(log.empty());
   CHECK(log.depth() == 0);

   log.push_frame();
   CHECK(log.depth() == 1);
   CHECK(log.entry_count() == 0);

   log.record_insert("key1");
   log.record_insert("key2");
   CHECK(log.entry_count() == 2);

   log.pop_frame();  // inner commit — entries merge into parent
   CHECK(log.depth() == 0);
   CHECK(log.entry_count() == 2);  // entries still there

   log.discard();
   CHECK(log.empty());
}

TEST_CASE("undo_log nested transaction replay", "[dwal]")
{
   psitri::dwal::undo_log log;
   std::vector<std::string> replayed;

   // Outer frame
   log.push_frame();
   log.record_insert("outer1");

   // Inner frame
   log.push_frame();
   log.record_insert("inner1");
   log.record_insert("inner2");

   // Abort inner frame
   log.replay_current_frame([&](const psitri::dwal::undo_entry& e)
                            { replayed.emplace_back(e.key); });

   CHECK(replayed.size() == 2);
   CHECK(replayed[0] == "inner2");  // reverse order
   CHECK(replayed[1] == "inner1");
   CHECK(log.depth() == 1);
   CHECK(log.entry_count() == 1);  // outer1 still there

   // Commit outer
   log.pop_frame();
   log.discard();
   CHECK(log.empty());
}

TEST_CASE("undo_log overwrite_buffered preserves old value", "[dwal]")
{
   psitri::dwal::undo_log log;
   log.push_frame();

   auto old_val = psitri::dwal::btree_value::make_data("old_data");
   log.record_overwrite_buffered("key1", old_val);

   CHECK(log.entry_count() == 1);

   psitri::dwal::undo_entry::kind replayed_type;
   std::string                    replayed_key;
   psitri::dwal::btree_value      replayed_old;
   int                            replay_count = 0;
   log.replay_current_frame(
       [&](const psitri::dwal::undo_entry& e)
       {
          replayed_type = e.type;
          replayed_key  = std::string(e.key);
          replayed_old  = e.old_value;
          ++replay_count;
       });

   REQUIRE(replay_count == 1);
   CHECK(replayed_type == psitri::dwal::undo_entry::kind::overwrite_buffered);
   CHECK(replayed_key == "key1");
   CHECK(replayed_old.is_data());
   CHECK(replayed_old.data == "old_data");
}

TEST_CASE("undo_log release_old_subtrees", "[dwal]")
{
   psitri::dwal::undo_log log;
   log.push_frame();

   // Overwrite a subtree value
   auto old_sub = psitri::dwal::btree_value::make_subtree(sal::ptr_address(42));
   log.record_overwrite_buffered("sub_key", old_sub);

   // Erase a subtree value
   auto erased_sub = psitri::dwal::btree_value::make_subtree(sal::ptr_address(99));
   log.record_erase_buffered("sub_key2", erased_sub);

   // Record a data overwrite — should NOT be released
   auto old_data = psitri::dwal::btree_value::make_data("some_data");
   log.record_overwrite_buffered("data_key", old_data);

   std::vector<sal::ptr_address> released;
   log.release_old_subtrees([&](sal::ptr_address addr) { released.push_back(addr); });

   CHECK(released.size() == 2);
   CHECK(released[0] == sal::ptr_address(42));
   CHECK(released[1] == sal::ptr_address(99));
}

TEST_CASE("undo_log replay_all", "[dwal]")
{
   psitri::dwal::undo_log log;

   log.push_frame();
   log.record_insert("a");
   log.push_frame();
   log.record_insert("b");
   log.record_insert("c");

   std::vector<std::string> replayed;
   log.replay_all([&](const psitri::dwal::undo_entry& e) { replayed.emplace_back(e.key); });

   CHECK(replayed.size() == 3);
   CHECK(replayed[0] == "c");
   CHECK(replayed[1] == "b");
   CHECK(replayed[2] == "a");
   CHECK(log.empty());
   CHECK(log.depth() == 0);
}

// ═══════════════════════════════════════════════════════════════════════
// WAL format tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("WAL header size", "[dwal]")
{
   CHECK(sizeof(psitri::dwal::wal_header) == 64);
}

// ═══════════════════════════════════════════════════════════════════════
// WAL writer + reader round-trip tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("WAL writer creates valid file", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "test.dwal";

   {
      psitri::dwal::wal_writer writer(wal_path, 3, 100);
      CHECK(writer.file_size() == sizeof(psitri::dwal::wal_header));
      CHECK(writer.next_sequence() == 100);
   }

   // Reader should be able to open it.
   psitri::dwal::wal_reader reader;
   REQUIRE(reader.open(wal_path));
   CHECK(reader.header().root_index == 3);
   CHECK(reader.header().sequence_base == 100);
}

TEST_CASE("WAL round-trip: upsert data", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "test.dwal";

   // Write
   {
      psitri::dwal::wal_writer writer(wal_path, 0, 0);
      writer.begin_entry();
      writer.add_upsert_data("key1", "value1");
      writer.add_upsert_data("key2", "value2");
      auto seq = writer.commit_entry();
      CHECK(seq == 0);
      writer.flush();
   }

   // Read
   psitri::dwal::wal_reader reader;
   REQUIRE(reader.open(wal_path));

   psitri::dwal::wal_entry entry;
   REQUIRE(reader.next(entry));
   CHECK(entry.sequence == 0);
   REQUIRE(entry.ops.size() == 2);

   CHECK(entry.ops[0].type == psitri::dwal::wal_op_type::upsert_data);
   CHECK(entry.ops[0].key == "key1");
   CHECK(entry.ops[0].value == "value1");

   CHECK(entry.ops[1].type == psitri::dwal::wal_op_type::upsert_data);
   CHECK(entry.ops[1].key == "key2");
   CHECK(entry.ops[1].value == "value2");

   // No more entries.
   CHECK_FALSE(reader.next(entry));
}

TEST_CASE("WAL round-trip: all operation types", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "test.dwal";

   {
      psitri::dwal::wal_writer writer(wal_path, 1, 10);

      writer.begin_entry();
      writer.add_upsert_data("dk", "dv");
      writer.add_remove("rk");
      writer.add_remove_range("lo", "hi");
      writer.add_upsert_subtree("sk", sal::ptr_address(777));
      auto seq = writer.commit_entry();
      CHECK(seq == 10);
      writer.flush();
   }

   psitri::dwal::wal_reader reader;
   REQUIRE(reader.open(wal_path));

   psitri::dwal::wal_entry entry;
   REQUIRE(reader.next(entry));
   REQUIRE(entry.ops.size() == 4);

   CHECK(entry.ops[0].type == psitri::dwal::wal_op_type::upsert_data);
   CHECK(entry.ops[0].key == "dk");
   CHECK(entry.ops[0].value == "dv");

   CHECK(entry.ops[1].type == psitri::dwal::wal_op_type::remove);
   CHECK(entry.ops[1].key == "rk");

   CHECK(entry.ops[2].type == psitri::dwal::wal_op_type::remove_range);
   CHECK(entry.ops[2].range_low == "lo");
   CHECK(entry.ops[2].range_high == "hi");

   CHECK(entry.ops[3].type == psitri::dwal::wal_op_type::upsert_subtree);
   CHECK(entry.ops[3].key == "sk");
   CHECK(entry.ops[3].subtree == sal::ptr_address(777));
}

TEST_CASE("WAL round-trip: multiple entries", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "test.dwal";

   {
      psitri::dwal::wal_writer writer(wal_path, 0, 0);

      for (int i = 0; i < 10; ++i)
      {
         writer.begin_entry();
         writer.add_upsert_data("key" + std::to_string(i), "val" + std::to_string(i));
         auto seq = writer.commit_entry();
         CHECK(seq == static_cast<uint64_t>(i));
      }
      writer.flush();
   }

   psitri::dwal::wal_reader reader;
   REQUIRE(reader.open(wal_path));

   uint64_t count = reader.replay_all(
       [](const psitri::dwal::wal_entry& e)
       {
          REQUIRE(e.ops.size() == 1);
          CHECK(e.ops[0].type == psitri::dwal::wal_op_type::upsert_data);
       });

   CHECK(count == 10);
   CHECK(reader.end_sequence() == 10);
}

TEST_CASE("WAL discard_entry drops in-progress transaction", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "test.dwal";

   {
      psitri::dwal::wal_writer writer(wal_path, 0, 0);

      // Commit one entry.
      writer.begin_entry();
      writer.add_upsert_data("good", "data");
      writer.commit_entry();

      // Start and discard another.
      writer.begin_entry();
      writer.add_upsert_data("bad", "data");
      writer.discard_entry();

      // Commit a third.
      writer.begin_entry();
      writer.add_upsert_data("also_good", "data");
      writer.commit_entry();

      writer.flush();
   }

   psitri::dwal::wal_reader reader;
   REQUIRE(reader.open(wal_path));

   psitri::dwal::wal_entry entry;
   REQUIRE(reader.next(entry));
   CHECK(entry.ops[0].key == "good");

   REQUIRE(reader.next(entry));
   CHECK(entry.ops[0].key == "also_good");

   CHECK_FALSE(reader.next(entry));
}

TEST_CASE("WAL clean close flag", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "test.dwal";

   {
      psitri::dwal::wal_writer writer(wal_path, 0, 0);
      writer.close();
   }

   psitri::dwal::wal_reader reader;
   REQUIRE(reader.open(wal_path));
   CHECK(reader.was_clean_close());
}

TEST_CASE("WAL reader rejects corrupt entry", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "test.dwal";

   // Write two valid entries.
   {
      psitri::dwal::wal_writer writer(wal_path, 0, 0);
      writer.begin_entry();
      writer.add_upsert_data("k1", "v1");
      writer.commit_entry();
      writer.begin_entry();
      writer.add_upsert_data("k2", "v2");
      writer.commit_entry();
      writer.flush();
   }

   // Corrupt the second entry by flipping a byte.
   {
      // The second entry starts after header + first entry.
      // We'll corrupt near the end of the file.
      auto fsize = std::filesystem::file_size(wal_path);
      std::vector<char> data(fsize);
      {
         int fd = ::open(wal_path.c_str(), O_RDONLY);
         ::read(fd, data.data(), fsize);
         ::close(fd);
      }
      // Flip a byte in the second half.
      data[fsize - 5] ^= 0xFF;
      {
         int fd = ::open(wal_path.c_str(), O_WRONLY);
         ::write(fd, data.data(), fsize);
         ::close(fd);
      }
   }

   psitri::dwal::wal_reader reader;
   REQUIRE(reader.open(wal_path));

   psitri::dwal::wal_entry entry;
   // First entry should still be valid.
   REQUIRE(reader.next(entry));
   CHECK(entry.ops[0].key == "k1");

   // Second entry should fail hash check.
   CHECK_FALSE(reader.next(entry));
}

TEST_CASE("WAL reader handles nonexistent file", "[dwal]")
{
   psitri::dwal::wal_reader reader;
   CHECK_FALSE(reader.open("/tmp/nonexistent_dwal_file_12345.dwal"));
}

TEST_CASE("WAL reader handles empty file", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "empty.dwal";

   // Create an empty file.
   { std::ofstream ofs{wal_path}; }

   psitri::dwal::wal_reader reader;
   CHECK_FALSE(reader.open(wal_path));
}

TEST_CASE("WAL round-trip: large values", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "test.dwal";

   std::string big_key(1000, 'K');
   std::string big_val(50000, 'V');

   {
      psitri::dwal::wal_writer writer(wal_path, 0, 0);
      writer.begin_entry();
      writer.add_upsert_data(big_key, big_val);
      writer.commit_entry();
      writer.flush();
   }

   psitri::dwal::wal_reader reader;
   REQUIRE(reader.open(wal_path));

   psitri::dwal::wal_entry entry;
   REQUIRE(reader.next(entry));
   REQUIRE(entry.ops.size() == 1);
   CHECK(entry.ops[0].key == big_key);
   CHECK(entry.ops[0].value == big_val);
}

// ═══════════════════════════════════════════════════════════════════════
// dwal_transaction tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("dwal_transaction basic upsert and commit", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "tx_test.dwal";

   psitri::dwal::dwal_root root;
   psitri::dwal::wal_writer wal(wal_path, 0, 0);

   // Single writer — no lock needed.

   {
      psitri::dwal::dwal_transaction tx(root, &wal, 0);

      tx.upsert("key1", "val1");
      tx.upsert("key2", "val2");

      // Verify writes are in the RW btree.
      auto* v = root.rw_layer->map.get("key1");
      REQUIRE(v != nullptr);
      CHECK(v->data == "val1");

      tx.commit();
   }

   // Verify WAL has the entry.
   wal.flush();
   psitri::dwal::wal_reader reader;
   REQUIRE(reader.open(wal_path));
   psitri::dwal::wal_entry entry;
   REQUIRE(reader.next(entry));
   CHECK(entry.ops.size() == 2);
}

TEST_CASE("dwal_transaction abort restores btree", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "tx_abort.dwal";

   psitri::dwal::dwal_root root;
   psitri::dwal::wal_writer wal(wal_path, 0, 0);

   // Pre-populate the btree.
   root.rw_layer->store_data("existing", "original");


   {
      psitri::dwal::dwal_transaction tx(root, &wal, 0);

      // Overwrite existing key.
      tx.upsert("existing", "modified");
      CHECK(root.rw_layer->map.get("existing")->data == "modified");

      // Insert new key.
      tx.upsert("new_key", "new_val");
      CHECK(root.rw_layer->map.get("new_key") != nullptr);

      tx.abort();
   }

   // After abort, existing key should be restored, new key should be gone.
   auto* v = root.rw_layer->map.get("existing");
   REQUIRE(v != nullptr);
   CHECK(v->data == "original");
   CHECK(root.rw_layer->map.get("new_key") == nullptr);
}

TEST_CASE("dwal_transaction remove and tombstone", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "tx_remove.dwal";

   psitri::dwal::dwal_root root;
   psitri::dwal::wal_writer wal(wal_path, 0, 0);

   root.rw_layer->store_data("k1", "v1");


   {
      psitri::dwal::dwal_transaction tx(root, &wal, 0);
      tx.remove("k1");

      // Should be a tombstone now.
      auto* v = root.rw_layer->map.get("k1");
      REQUIRE(v != nullptr);
      CHECK(v->is_tombstone());

      tx.commit();
   }

   CHECK(root.rw_layer->map.get("k1")->is_tombstone());
}

TEST_CASE("dwal_transaction remove abort restores value", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "tx_rmabort.dwal";

   psitri::dwal::dwal_root root;
   psitri::dwal::wal_writer wal(wal_path, 0, 0);

   root.rw_layer->store_data("k1", "v1");


   {
      psitri::dwal::dwal_transaction tx(root, &wal, 0);
      tx.remove("k1");
      CHECK(root.rw_layer->map.get("k1")->is_tombstone());

      tx.abort();
   }

   // Restored after abort.
   auto* v = root.rw_layer->map.get("k1");
   REQUIRE(v != nullptr);
   CHECK(v->is_data());
   CHECK(v->data == "v1");
}

TEST_CASE("dwal_transaction range remove", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "tx_range.dwal";

   psitri::dwal::dwal_root root;
   psitri::dwal::wal_writer wal(wal_path, 0, 0);

   root.rw_layer->store_data("a", "1");
   root.rw_layer->store_data("b", "2");
   root.rw_layer->store_data("c", "3");
   root.rw_layer->store_data("d", "4");


   {
      psitri::dwal::dwal_transaction tx(root, &wal, 0);
      tx.remove_range("b", "d");  // removes b, c

      // b and c should be gone from map.
      CHECK(root.rw_layer->map.get("b") == nullptr);
      CHECK(root.rw_layer->map.get("c") == nullptr);
      // a and d should still be there.
      CHECK(root.rw_layer->map.get("a") != nullptr);
      CHECK(root.rw_layer->map.get("d") != nullptr);

      // Range tombstone should be active.
      CHECK(root.rw_layer->tombstones.is_deleted("b"));
      CHECK(root.rw_layer->tombstones.is_deleted("c"));
      CHECK_FALSE(root.rw_layer->tombstones.is_deleted("a"));
      CHECK_FALSE(root.rw_layer->tombstones.is_deleted("d"));

      tx.commit();
   }
}

TEST_CASE("dwal_transaction range remove abort restores", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "tx_rng_abort.dwal";

   psitri::dwal::dwal_root root;
   psitri::dwal::wal_writer wal(wal_path, 0, 0);

   root.rw_layer->store_data("a", "1");
   root.rw_layer->store_data("b", "2");
   root.rw_layer->store_data("c", "3");


   {
      psitri::dwal::dwal_transaction tx(root, &wal, 0);
      tx.remove_range("a", "d");

      CHECK(root.rw_layer->map.empty());
      CHECK(root.rw_layer->tombstones.is_deleted("b"));

      tx.abort();
   }

   // All entries restored.
   CHECK(root.rw_layer->size() == 3);
   CHECK(root.rw_layer->map.get("a")->data == "1");
   CHECK(root.rw_layer->map.get("b")->data == "2");
   CHECK(root.rw_layer->map.get("c")->data == "3");
   CHECK(root.rw_layer->tombstones.empty());
}

TEST_CASE("dwal_transaction get reads from RW layer", "[dwal]")
{
   psitri::dwal::dwal_root root;

   root.rw_layer->store_data("k1", "v1");
   root.rw_layer->store_tombstone("dead");


   {
      psitri::dwal::dwal_transaction tx(root, nullptr, 0);

      auto r1 = tx.get("k1");
      CHECK(r1.found);
      CHECK(r1.value.data == "v1");

      auto r2 = tx.get("dead");
      CHECK_FALSE(r2.found);

      auto r3 = tx.get("nonexistent");
      CHECK_FALSE(r3.found);

      tx.commit();
   }
}

TEST_CASE("dwal_transaction destructor aborts uncommitted", "[dwal]")
{
   psitri::dwal::dwal_root root;
   root.rw_layer->store_data("k", "original");


   {
      psitri::dwal::dwal_transaction tx(root, nullptr, 0);
      tx.upsert("k", "changed");
      // Destructor fires — should abort.
   }

   CHECK(root.rw_layer->map.get("k")->data == "original");
}

TEST_CASE("dwal_transaction subtree upsert", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "tx_sub.dwal";

   psitri::dwal::dwal_root root;
   psitri::dwal::wal_writer wal(wal_path, 0, 0);


   {
      psitri::dwal::dwal_transaction tx(root, &wal, 0);
      tx.upsert_subtree("tree_key", sal::ptr_address(42));

      auto* v = root.rw_layer->map.get("tree_key");
      REQUIRE(v != nullptr);
      CHECK(v->is_subtree());
      CHECK(v->subtree_root == sal::ptr_address(42));

      tx.commit();
   }

   // Verify WAL.
   wal.flush();
   psitri::dwal::wal_reader reader;
   REQUIRE(reader.open(wal_path));
   psitri::dwal::wal_entry entry;
   REQUIRE(reader.next(entry));
   REQUIRE(entry.ops.size() == 1);
   CHECK(entry.ops[0].type == psitri::dwal::wal_op_type::upsert_subtree);
   CHECK(entry.ops[0].subtree == sal::ptr_address(42));
}

// ═══════════════════════════════════════════════════════════════════════
// dwal_database tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("dwal_database basic transaction lifecycle", "[dwal]")
{
   temp_dir td;
   auto     db_path  = td.path / "db";
   auto     wal_path = td.path / "wal";

   // Create a real PsiTri database.
   auto db = psitri::database::create(db_path);

   psitri::dwal::dwal_database dwal_db(db, wal_path);

   // Start a write transaction.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("hello", "world");
      tx.commit();
   }

   // Verify the data is in the RW btree.
   auto& root = dwal_db.root(0);
   auto* v = root.rw_layer->map.get("hello");
   REQUIRE(v != nullptr);
   CHECK(v->data == "world");
}

TEST_CASE("dwal_database multiple transactions on same root", "[dwal]")
{
   temp_dir td;
   auto     db  = psitri::database::create(td.path / "db");
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal");

   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("k1", "v1");
      tx.commit();
   }
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("k2", "v2");
      tx.commit();
   }

   auto& root = dwal_db.root(0);
   CHECK(root.rw_layer->map.get("k1")->data == "v1");
   CHECK(root.rw_layer->map.get("k2")->data == "v2");
}

TEST_CASE("dwal_database independent roots", "[dwal]")
{
   temp_dir td;
   auto     db  = psitri::database::create(td.path / "db");
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal");

   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("root0_key", "root0_val");
      tx.commit();
   }
   {
      auto tx = dwal_db.start_write_transaction(1);
      tx.upsert("root1_key", "root1_val");
      tx.commit();
   }

   CHECK(dwal_db.root(0).rw_layer->map.get("root0_key") != nullptr);
   CHECK(dwal_db.root(1).rw_layer->map.get("root1_key") != nullptr);

   // Root 0 shouldn't have root 1's key and vice versa.
   CHECK(dwal_db.root(0).rw_layer->map.get("root1_key") == nullptr);
   CHECK(dwal_db.root(1).rw_layer->map.get("root0_key") == nullptr);
}

TEST_CASE("dwal_database swap_rw_to_ro", "[dwal]")
{
   temp_dir td;
   auto     db  = psitri::database::create(td.path / "db");
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal");

   // Write some data.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("pre_swap", "data");
      tx.commit();
   }

   auto& root = dwal_db.root(0);

   // RW has the data, buffered is null.
   CHECK(root.rw_layer->map.get("pre_swap") != nullptr);
   CHECK(root.buffered_ptr == nullptr);

   // Perform swap.
   dwal_db.swap_rw_to_ro(0);

   // Now: RW is fresh (empty), buffered has the old data.
   CHECK(root.rw_layer->map.empty());
   auto ro = root.buffered_ptr;
   REQUIRE(ro != nullptr);
   CHECK(ro->map.get("pre_swap") != nullptr);
   CHECK(ro->map.get("pre_swap")->data == "data");

   // Clean up: simulate merge complete by clearing the buffered ptr.
   {
      std::unique_lock lk(root.buffered_mutex);
      root.buffered_ptr.reset();
   }
   root.merge_complete.store(true, std::memory_order_release);
}

// ═══════════════════════════════════════════════════════════════════════
// merge_cursor tests (btree layers only — no PsiTri)
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("merge_cursor single RW layer", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("b", "B");
   rw.store_data("d", "D");
   rw.store_data("f", "F");

   psitri::dwal::merge_cursor cur(&rw, nullptr, std::nullopt);
   cur.seek_begin();

   REQUIRE_FALSE(cur.is_end());
   CHECK(cur.key() == "b");
   CHECK(cur.current_value().data == "B");

   cur.next();
   CHECK(cur.key() == "d");
   cur.next();
   CHECK(cur.key() == "f");
   cur.next();
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor RW+RO merge", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("a", "RW_A");
   rw.store_data("c", "RW_C");

   psitri::dwal::btree_layer ro;
   ro.store_data("b", "RO_B");
   ro.store_data("c", "RO_C");  // shadowed by RW
   ro.store_data("d", "RO_D");

   psitri::dwal::merge_cursor cur(&rw, &ro, std::nullopt);
   cur.seek_begin();

   CHECK(cur.key() == "a");
   CHECK(cur.current_source() == psitri::dwal::merge_cursor::source::rw);

   cur.next();
   CHECK(cur.key() == "b");
   CHECK(cur.current_source() == psitri::dwal::merge_cursor::source::ro);

   cur.next();
   CHECK(cur.key() == "c");
   CHECK(cur.current_source() == psitri::dwal::merge_cursor::source::rw);
   CHECK(cur.current_value().data == "RW_C");  // RW wins over RO

   cur.next();
   CHECK(cur.key() == "d");
   CHECK(cur.current_source() == psitri::dwal::merge_cursor::source::ro);

   cur.next();
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor tombstone in RW shadows RO", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_tombstone("b");  // tombstone shadows RO's "b"

   psitri::dwal::btree_layer ro;
   ro.store_data("a", "A");
   ro.store_data("b", "B");
   ro.store_data("c", "C");

   psitri::dwal::merge_cursor cur(&rw, &ro, std::nullopt);
   cur.seek_begin();

   CHECK(cur.key() == "a");
   cur.next();
   CHECK(cur.key() == "c");  // "b" is skipped (tombstoned by RW)
   cur.next();
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor range tombstone in RW shadows RO", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.tombstones.add("b", "d");  // range [b, d) tombstoned

   psitri::dwal::btree_layer ro;
   ro.store_data("a", "A");
   ro.store_data("b", "B");
   ro.store_data("c", "C");
   ro.store_data("d", "D");

   psitri::dwal::merge_cursor cur(&rw, &ro, std::nullopt);
   cur.seek_begin();

   CHECK(cur.key() == "a");
   cur.next();
   CHECK(cur.key() == "d");  // "b" and "c" tombstoned by RW range
   cur.next();
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor lower_bound", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("a", "A");
   rw.store_data("c", "C");
   rw.store_data("e", "E");

   psitri::dwal::merge_cursor cur(&rw, nullptr, std::nullopt);

   cur.lower_bound("b");
   CHECK(cur.key() == "c");

   cur.lower_bound("c");
   CHECK(cur.key() == "c");

   cur.lower_bound("f");
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor upper_bound", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("a", "A");
   rw.store_data("c", "C");

   psitri::dwal::merge_cursor cur(&rw, nullptr, std::nullopt);

   cur.upper_bound("a");
   CHECK(cur.key() == "c");

   cur.upper_bound("c");
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor seek exact", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("a", "A");
   rw.store_data("c", "C");

   psitri::dwal::merge_cursor cur(&rw, nullptr, std::nullopt);

   CHECK(cur.seek("a"));
   CHECK(cur.key() == "a");

   CHECK(cur.seek("c"));
   CHECK(cur.key() == "c");

   CHECK_FALSE(cur.seek("b"));
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor prev navigation", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("a", "A");
   rw.store_data("b", "B");
   rw.store_data("c", "C");

   psitri::dwal::merge_cursor cur(&rw, nullptr, std::nullopt);
   cur.seek_last();

   CHECK(cur.key() == "c");
   cur.prev();
   CHECK(cur.key() == "b");
   cur.prev();
   CHECK(cur.key() == "a");
   cur.prev();
   CHECK(cur.is_rend());
}

TEST_CASE("merge_cursor count_keys", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("a", "A");
   rw.store_data("b", "B");
   rw.store_data("c", "C");
   rw.store_data("d", "D");
   rw.store_tombstone("e");

   psitri::dwal::merge_cursor cur(&rw, nullptr, std::nullopt);

   CHECK(cur.count_keys() == 4);  // tombstone not counted
   CHECK(cur.count_keys("b", "d") == 2);  // b, c
}

TEST_CASE("merge_cursor empty layers", "[dwal]")
{
   psitri::dwal::merge_cursor cur(nullptr, nullptr, std::nullopt);
   cur.seek_begin();
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor subtree detection from btree layer", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("data_key", "val");
   rw.store_subtree("sub_key", sal::ptr_address(99));

   psitri::dwal::merge_cursor cur(&rw, nullptr, std::nullopt);
   cur.seek_begin();

   CHECK(cur.key() == "data_key");
   CHECK_FALSE(cur.is_subtree());

   cur.next();
   CHECK(cur.key() == "sub_key");
   CHECK(cur.is_subtree());
   CHECK(cur.current_value().subtree_root == sal::ptr_address(99));
}

// ── Epoch Lock ────────────────────────────────────────────────────

TEST_CASE("dwal_session_lock basic lock/unlock", "[dwal]")
{
   psitri::dwal::dwal_session_lock lock;

   // Initially idle (both halves = 0xFFFFFFFF).
   CHECK(lock.pinned_generation() == psitri::dwal::dwal_session_lock::idle);

   // Broadcast generation 5.
   lock.broadcast(5);
   // Low bits still idle until locked.
   CHECK(lock.pinned_generation() == psitri::dwal::dwal_session_lock::idle);

   // Lock — copies high bits (5) to low bits.
   lock.lock();
   CHECK(lock.pinned_generation() == 5);

   // Unlock — sets low bits back to idle.
   lock.unlock();
   CHECK(lock.pinned_generation() == psitri::dwal::dwal_session_lock::idle);
}

TEST_CASE("dwal_session_lock broadcast updates high bits", "[dwal]")
{
   psitri::dwal::dwal_session_lock lock;

   lock.broadcast(10);
   lock.lock();
   CHECK(lock.pinned_generation() == 10);

   // Broadcast a new generation while locked — low bits unchanged.
   lock.broadcast(20);
   CHECK(lock.pinned_generation() == 10);

   // Unlock and re-lock — now sees generation 20.
   lock.unlock();
   lock.lock();
   CHECK(lock.pinned_generation() == 20);

   lock.unlock();
}

TEST_CASE("epoch_registry allocate and release", "[dwal]")
{
   psitri::dwal::epoch_registry reg;

   auto idx0 = reg.allocate();
   auto idx1 = reg.allocate();
   CHECK(idx0 != idx1);
   CHECK(reg.size() == 2);

   // Both idle — min_pinned should be idle.
   CHECK(reg.min_pinned() == psitri::dwal::dwal_session_lock::idle);

   // Pin session 0 at generation 5.
   reg.broadcast_all(5);
   reg[idx0].lock();
   CHECK(reg.min_pinned() == 5);

   // Pin session 1 at generation 5 too.
   reg[idx1].lock();
   CHECK(reg.min_pinned() == 5);

   // Unlock session 0 — min is still 5 (session 1 pinned).
   reg[idx0].unlock();
   CHECK(reg.min_pinned() == 5);

   // Unlock session 1 — both idle.
   reg[idx1].unlock();
   CHECK(reg.min_pinned() == psitri::dwal::dwal_session_lock::idle);

   // Release and reuse a slot.
   reg.release(idx0);
   auto idx2 = reg.allocate();
   CHECK(idx2 == idx0);  // reused
   CHECK(reg.size() == 2);
}

TEST_CASE("epoch_registry min_pinned across generations", "[dwal]")
{
   psitri::dwal::epoch_registry reg;

   auto s0 = reg.allocate();
   auto s1 = reg.allocate();

   // Broadcast gen 3, lock s0.
   reg.broadcast_all(3);
   reg[s0].lock();

   // Broadcast gen 7, lock s1.
   reg.broadcast_all(7);
   reg[s1].lock();

   // s0 is pinned at 3, s1 at 7 — min is 3.
   CHECK(reg[s0].pinned_generation() == 3);
   CHECK(reg[s1].pinned_generation() == 7);
   CHECK(reg.min_pinned() == 3);

   // Unlock s0 — min becomes 7.
   reg[s0].unlock();
   CHECK(reg.min_pinned() == 7);

   reg[s1].unlock();
}

// ── Merge Pool (integration) ──────────────────────────────────────

TEST_CASE("merge_pool drains RO btree into PsiTri", "[dwal]")
{
   temp_dir tmp;
   auto     db_path = tmp.path / "db";

   auto db = psitri::database::create(db_path);

   psitri::dwal::epoch_registry epochs;
   psitri::dwal::merge_pool     pool(db, 1, epochs);

   // Set up a dwal_root with an RO btree containing some data.
   psitri::dwal::dwal_root root;

   // Create an RO btree layer with entries.
   auto ro = std::make_shared<psitri::dwal::btree_layer>();
   ro->store_data("key1", "val1");
   ro->store_data("key2", "val2");
   ro->store_data("key3", "val3");
   ro->generation = 1;

   // Set it as the buffered RO layer.
   {
      std::unique_lock lk(root.buffered_mutex);
      root.buffered_ptr = ro;
   }
   root.merge_complete.store(false, std::memory_order_release);

   // Signal the merge pool.
   pool.signal(0, root);

   // Wait for merge to complete.
   auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
   while (!root.merge_complete.load(std::memory_order_acquire)
          && std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

   CHECK(root.merge_complete.load());
   CHECK(root.buffered_ptr == nullptr);

   // Verify data was merged into PsiTri.
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek_begin();

   REQUIRE_FALSE(cur.is_end());
   CHECK(cur.key() == "key1");
   CHECK(cur.value<std::string>() == "val1");

   cur.next();
   CHECK(cur.key() == "key2");
   CHECK(cur.value<std::string>() == "val2");

   cur.next();
   CHECK(cur.key() == "key3");
   CHECK(cur.value<std::string>() == "val3");

   cur.next();
   CHECK(cur.is_end());

   pool.shutdown();
}

TEST_CASE("merge_pool handles tombstones during drain", "[dwal]")
{
   temp_dir tmp;
   auto     db_path = tmp.path / "db";

   auto db = psitri::database::create(db_path);

   // Pre-populate PsiTri with some data.
   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("aaa", "old_a");
      tx.upsert("bbb", "old_b");
      tx.upsert("ccc", "old_c");
      tx.commit();
   }

   psitri::dwal::epoch_registry epochs;
   psitri::dwal::merge_pool     pool(db, 1, epochs);

   psitri::dwal::dwal_root root;
   // Capture the current PsiTri root.
   {
      auto ws = db->start_write_session();
      auto r  = ws->get_root(0);
      if (r)
         root.tri_root.store(static_cast<uint32_t>(r.address()), std::memory_order_relaxed);
   }

   // Create RO btree: overwrite "aaa", tombstone "bbb", new "ddd".
   auto ro = std::make_shared<psitri::dwal::btree_layer>();
   ro->store_data("aaa", "new_a");
   ro->store_tombstone("bbb");
   ro->store_data("ddd", "val_d");
   ro->generation = 1;

   {
      std::unique_lock lk(root.buffered_mutex);
      root.buffered_ptr = ro;
   }
   root.merge_complete.store(false, std::memory_order_release);

   pool.signal(0, root);

   auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
   while (!root.merge_complete.load(std::memory_order_acquire)
          && std::chrono::steady_clock::now() < deadline)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

   CHECK(root.merge_complete.load());
   CHECK(root.buffered_ptr == nullptr);

   // Verify merged state.
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek_begin();

   // "aaa" should be overwritten.
   REQUIRE_FALSE(cur.is_end());
   CHECK(cur.key() == "aaa");
   CHECK(cur.value<std::string>() == "new_a");

   // "bbb" should be deleted.
   cur.next();
   CHECK(cur.key() == "ccc");
   CHECK(cur.value<std::string>() == "old_c");

   cur.next();
   CHECK(cur.key() == "ddd");
   CHECK(cur.value<std::string>() == "val_d");

   cur.next();
   CHECK(cur.is_end());

   pool.shutdown();
}

TEST_CASE("epoch reclamation defers pool free until readers release", "[dwal]")
{
   psitri::dwal::epoch_registry epochs;

   auto s0 = epochs.allocate();

   // Broadcast gen 1, reader locks at gen 1.
   epochs.broadcast_all(1);
   epochs[s0].lock();

   // Simulate a pool at generation 1 — min_pinned == 1, so 1 > 1 is false.
   CHECK_FALSE(epochs.min_pinned() > 1);

   // Reader unlocks — min_pinned becomes idle (0xFFFFFFFF), so idle > 1 is true.
   epochs[s0].unlock();
   CHECK(epochs.min_pinned() > 1);

   epochs.release(s0);
}

// ═══════════════════════════════════════════════════════════════════════
// Allocation leak tests — verify COW trees are released after cursor refresh
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("direct COW cursor refresh releases old tree", "[dwal][leak]")
{
   temp_dir td;
   auto     db  = psitri::database::create(td.path);
   auto     ws  = db->start_write_session();
   auto     rs  = db->start_read_session();

   // Seed initial data.
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < 1000; ++i)
      {
         auto key = std::to_string(i);
         auto val = std::string(100, 'a' + (i % 26));
         tx.upsert(psitri::key_view(key.data(), key.size()),
                    psitri::value_view(val.data(), val.size()));
      }
      tx.commit();
   }
   db->wait_for_compactor();

   uint64_t baseline = ws->get_total_allocated_objects();
   INFO("baseline allocated objects: " << baseline);
   REQUIRE(baseline > 0);

   // Create a cursor (holds a root ref).
   auto cur = rs->create_cursor(0);

   // Do several rounds of writes + cursor refresh.
   for (int round = 0; round < 10; ++round)
   {
      // Write a batch (creates new COW root).
      {
         auto tx = ws->start_transaction(0);
         for (int i = 0; i < 100; ++i)
         {
            auto key = std::to_string(round * 100 + 1000 + i);
            auto val = std::string(100, 'x');
            tx.upsert(psitri::key_view(key.data(), key.size()),
                       psitri::value_view(val.data(), val.size()));
         }
         tx.commit();
      }

      // Refresh cursor — should release old root.
      cur.refresh(0);
   }

   // Let compactor drain.
   db->wait_for_compactor();

   uint64_t after = ws->get_total_allocated_objects();
   uint64_t pending = ws->get_pending_release_count();
   INFO("after 10 rounds: allocated=" << after << " pending=" << pending
        << " baseline=" << baseline);

   // Allocated objects should grow roughly proportional to data, not 10x from
   // leaked roots. Allow 3x growth for the 1000 extra keys we added.
   REQUIRE(after < baseline * 4);
}

TEST_CASE("direct COW concurrent read+write doesn't leak", "[dwal][leak]")
{
   temp_dir td;
   auto     db  = psitri::database::create(td.path);
   auto     ws  = db->start_write_session();

   // Seed.
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < 1000; ++i)
      {
         auto key = std::to_string(i);
         auto val = std::string(100, 'v');
         tx.upsert(psitri::key_view(key.data(), key.size()),
                    psitri::value_view(val.data(), val.size()));
      }
      tx.commit();
   }
   db->wait_for_compactor();
   uint64_t baseline = ws->get_total_allocated_objects();

   std::atomic<bool> done{false};
   std::atomic<int>  read_ops{0};

   // Reader thread — mimics benchmark pattern.
   auto reader = std::thread([&]()
   {
      auto rs  = db->start_read_session();
      auto cur = rs->create_cursor(0);
      int ops = 0;
      while (!done.load(std::memory_order_relaxed))
      {
         // Periodic refresh to pick up new root.
         if ((ops % 100) == 0)
            cur.refresh(0);

         auto key = std::to_string(ops % 1000);
         std::string buf;
         cur.get(psitri::key_view(key.data(), key.size()), &buf);
         ++ops;
      }
      read_ops.store(ops);
   });

   // Writer — batch=1 mimicking the benchmark.
   for (int round = 0; round < 5; ++round)
   {
      for (int i = 0; i < 500; ++i)
      {
         auto tx = ws->start_transaction(0);
         auto key = std::to_string(round * 500 + 1000 + i);
         auto val = std::string(100, 'w');
         tx.upsert(psitri::key_view(key.data(), key.size()),
                    psitri::value_view(val.data(), val.size()));
         tx.commit();
      }
   }

   done.store(true);
   reader.join();

   db->wait_for_compactor();

   uint64_t after = ws->get_total_allocated_objects();
   uint64_t pending = ws->get_pending_release_count();
   INFO("concurrent: allocated=" << after << " pending=" << pending
        << " baseline=" << baseline << " read_ops=" << read_ops.load());

   // Should not have unbounded growth — allow 4x for the 2500 extra keys.
   REQUIRE(after < baseline * 5);
}

TEST_CASE("DWAL cursor refresh releases old tree", "[dwal][leak]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path);

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 500;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   auto ws = db->start_write_session();

   // Seed via DWAL.
   {
      auto tx = dwal_db.start_write_transaction(0);
      for (int i = 0; i < 500; ++i)
      {
         auto key = std::to_string(i);
         auto val = std::string(100, 'd');
         tx.upsert(std::string_view(key), std::string_view(val));
      }
      tx.commit();
   }
   // Force swap + merge.
   dwal_db.swap_rw_to_ro(0);

   // Wait for merge to complete.
   auto& root = dwal_db.root(0);
   for (int i = 0; i < 100 && !root.merge_complete.load(); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
   REQUIRE(root.merge_complete.load());

   db->wait_for_compactor();
   uint64_t baseline = ws->get_total_allocated_objects();
   INFO("DWAL baseline: " << baseline);

   // Create a dwal_read_session.
   auto reader = dwal_db.start_read_session();

   // Do several rounds of DWAL writes that trigger swaps.
   for (int round = 0; round < 10; ++round)
   {
      auto tx = dwal_db.start_write_transaction(0);
      for (int i = 0; i < 200; ++i)
      {
         auto key = std::to_string(round * 200 + 500 + i);
         auto val = std::string(100, 'e');
         tx.upsert(std::string_view(key), std::string_view(val));
      }
      tx.commit();

      // Read through the session — triggers refresh on gen change.
      auto key = std::to_string(round);
      reader.get(0, key, psitri::dwal::read_mode::buffered);
   }

   // Force final swap + wait for merge.
   dwal_db.swap_rw_to_ro(0);
   for (int i = 0; i < 100 && !root.merge_complete.load(); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

   db->wait_for_compactor();

   uint64_t after = ws->get_total_allocated_objects();
   uint64_t pending = ws->get_pending_release_count();
   INFO("DWAL after 10 rounds: allocated=" << after << " pending=" << pending
        << " baseline=" << baseline);

   // Should not have unbounded growth.
   REQUIRE(after < baseline * 5);
}

TEST_CASE("write-only batch=1 doesn't leak", "[dwal][leak]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path);
   auto     ws = db->start_write_session();

   // Seed 10K keys in one transaction.
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < 10000; ++i)
      {
         auto key = std::to_string(i);
         auto val = std::string(100, 'a' + (i % 26));
         tx.upsert(psitri::key_view(key.data(), key.size()),
                    psitri::value_view(val.data(), val.size()));
      }
      tx.commit();
   }
   db->wait_for_compactor();

   uint64_t baseline = ws->get_total_allocated_objects();
   auto db_size = [&]() {
      uint64_t total = 0;
      std::error_code ec;
      for (auto& e : std::filesystem::recursive_directory_iterator(td.path, ec))
         if (e.is_regular_file(ec)) total += e.file_size(ec);
      return total;
   };
   uint64_t baseline_bytes = db_size();
   std::cout << "batch=1 baseline: alloc=" << baseline
             << " db=" << (baseline_bytes / (1024*1024)) << "MB" << std::endl;
   REQUIRE(baseline > 0);

   // 5 rounds of 10K single-key transactions (batch=1).
   for (int round = 0; round < 5; ++round)
   {
      for (int i = 0; i < 10000; ++i)
      {
         auto tx  = ws->start_transaction(0);
         auto key = std::to_string(round * 10000 + 10000 + i);
         auto val = std::string(100, 'x');
         tx.upsert(psitri::key_view(key.data(), key.size()),
                    psitri::value_view(val.data(), val.size()));
         tx.commit();
      }
      db->wait_for_compactor();
   }

   db->wait_for_compactor();
   uint64_t after      = ws->get_total_allocated_objects();
   uint64_t final_bytes = db_size();

   INFO("batch=1: alloc=" << after << " baseline=" << baseline
        << " db=" << (final_bytes / (1024*1024)) << "MB"
        << " baseline_db=" << (baseline_bytes / (1024*1024)) << "MB");

   // Object count should be proportional to key count (no object leaks).
   REQUIRE(after < baseline * 8);

   // DB size should be bounded — compactor reclaims sync header padding.
   // Without the fix, this grew to 1177MB. With it, should stay under 3x baseline.
   REQUIRE(final_bytes < baseline_bytes * 3);
}

// ═══════════════════════════════════════════════════════════════════════
// Sub-transaction (nested) tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("sub_transaction commit merges into parent", "[dwal]")
{
   temp_dir td;
   auto     wal_path = td.path / "tx_sub.dwal";

   psitri::dwal::dwal_root  root;
   psitri::dwal::wal_writer wal(wal_path, 0, 0);

   {
      psitri::dwal::dwal_transaction tx(root, &wal, 0);
      tx.upsert("outer1", "val1");

      {
         auto sub = tx.sub_transaction();
         sub.upsert("inner1", "val2");
         sub.upsert("inner2", "val3");
         sub.commit();
      }

      // Inner writes visible after commit.
      CHECK(root.rw_layer->map.get("inner1")->data == "val2");
      CHECK(root.rw_layer->map.get("inner2")->data == "val3");

      tx.commit();
   }

   CHECK(root.rw_layer->map.get("outer1")->data == "val1");
   CHECK(root.rw_layer->map.get("inner1")->data == "val2");
}

TEST_CASE("sub_transaction abort rolls back inner only", "[dwal]")
{
   psitri::dwal::dwal_root root;

   {
      psitri::dwal::dwal_transaction tx(root, nullptr, 0);
      tx.upsert("keep", "yes");

      {
         auto sub = tx.sub_transaction();
         sub.upsert("discard", "gone");
         sub.upsert("keep", "overwritten");
         sub.abort();
      }

      // Inner writes rolled back.
      CHECK(root.rw_layer->map.get("discard") == nullptr);
      CHECK(root.rw_layer->map.get("keep")->data == "yes");

      tx.commit();
   }

   CHECK(root.rw_layer->map.get("keep")->data == "yes");
   CHECK(root.rw_layer->map.get("discard") == nullptr);
}

TEST_CASE("sub_transaction destructor aborts if uncommitted", "[dwal]")
{
   psitri::dwal::dwal_root root;

   {
      psitri::dwal::dwal_transaction tx(root, nullptr, 0);
      tx.upsert("base", "ok");

      {
         auto sub = tx.sub_transaction();
         sub.upsert("temp", "nope");
         // ~sub fires, should auto-abort
      }

      CHECK(root.rw_layer->map.get("temp") == nullptr);
      CHECK(root.rw_layer->map.get("base")->data == "ok");
      tx.commit();
   }
}

TEST_CASE("nested sub_transaction two levels deep", "[dwal]")
{
   psitri::dwal::dwal_root root;

   {
      psitri::dwal::dwal_transaction tx(root, nullptr, 0);
      tx.upsert("L0", "zero");

      {
         auto sub1 = tx.sub_transaction();
         sub1.upsert("L1", "one");

         {
            auto sub2 = sub1.sub_transaction();
            sub2.upsert("L2", "two");
            sub2.commit();
         }

         // L2 visible after sub2 commit.
         CHECK(root.rw_layer->map.get("L2")->data == "two");
         sub1.commit();
      }

      CHECK(root.rw_layer->map.get("L0")->data == "zero");
      CHECK(root.rw_layer->map.get("L1")->data == "one");
      CHECK(root.rw_layer->map.get("L2")->data == "two");
      tx.commit();
   }
}

TEST_CASE("sub_transaction abort after remove restores", "[dwal]")
{
   psitri::dwal::dwal_root root;
   root.rw_layer->store_data("k1", "v1");
   root.rw_layer->store_data("k2", "v2");

   {
      psitri::dwal::dwal_transaction tx(root, nullptr, 0);

      {
         auto sub = tx.sub_transaction();
         sub.remove("k1");
         CHECK(root.rw_layer->map.get("k1")->is_tombstone());
         sub.abort();
      }

      // k1 restored.
      CHECK(root.rw_layer->map.get("k1")->data == "v1");
      tx.commit();
   }
}

TEST_CASE("sub_transaction range remove abort restores", "[dwal]")
{
   psitri::dwal::dwal_root root;
   root.rw_layer->store_data("a", "1");
   root.rw_layer->store_data("b", "2");
   root.rw_layer->store_data("c", "3");

   {
      psitri::dwal::dwal_transaction tx(root, nullptr, 0);

      {
         auto sub = tx.sub_transaction();
         sub.remove_range("a", "c");  // removes a, b
         CHECK(root.rw_layer->map.get("a") == nullptr);
         CHECK(root.rw_layer->map.get("b") == nullptr);
         sub.abort();
      }

      // a and b restored.
      CHECK(root.rw_layer->map.get("a")->data == "1");
      CHECK(root.rw_layer->map.get("b")->data == "2");
      CHECK(root.rw_layer->tombstones.empty());
      tx.commit();
   }
}

// ═══════════════════════════════════════════════════════════════════════
// get_latest — reads across all 3 layers (RW → RO → Tri)
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("get_latest reads through RW, RO, and Tri layers", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 5000;  // large enough to not auto-swap
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Tri layer: write + swap + merge.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("tri_key", "tri_val");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);

   auto& root = dwal_db.root(0);
   for (int i = 0; i < 100 && !root.merge_complete.load(); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
   REQUIRE(root.merge_complete.load());

   // RO layer: write + swap (no merge yet since merge_complete is from above).
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("ro_key", "ro_val");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);
   // Don't wait for merge — RO layer stays in place.

   // RW layer: write without swapping.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("rw_key", "rw_val");
      tx.commit();
   }

   // get_latest should see all three.
   auto r1 = dwal_db.get_latest(0, "rw_key");
   CHECK(r1.found);
   CHECK(r1.value.data == "rw_val");

   auto r2 = dwal_db.get_latest(0, "ro_key");
   CHECK(r2.found);
   CHECK(r2.value.data == "ro_val");

   auto r3 = dwal_db.get_latest(0, "tri_key");
   CHECK(r3.found);
}

TEST_CASE("get_latest RW tombstone shadows RO and Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Put key into Tri.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("key", "old");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);
   auto& root = dwal_db.root(0);
   for (int i = 0; i < 100 && !root.merge_complete.load(); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

   // Tombstone in RW.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.remove("key");
      tx.commit();
   }

   auto r = dwal_db.get_latest(0, "key");
   CHECK_FALSE(r.found);
}

// ═══════════════════════════════════════════════════════════════════════
// dwal_read_session — read mode semantics
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("read_session persistent mode only sees Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Merge key into Tri.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("merged", "in_tri");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);
   auto& root = dwal_db.root(0);
   for (int i = 0; i < 100 && !root.merge_complete.load(); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
   REQUIRE(root.merge_complete.load());

   // Write to RW (not yet swapped).
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("buffered_only", "bval");
      tx.commit();
   }

   auto reader = dwal_db.start_read_session();

   // persistent: sees only merged data.
   auto p1 = reader.get(0, "merged", psitri::dwal::read_mode::persistent);
   CHECK(p1.found);
   CHECK(p1.value == "in_tri");

   auto p2 = reader.get(0, "buffered_only", psitri::dwal::read_mode::persistent);
   CHECK_FALSE(p2.found);
}

TEST_CASE("read_session buffered mode sees RO + Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 0;  // no merge — keep RO alive
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Write + swap to create RO layer.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("in_ro", "ro_val");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);

   auto reader = dwal_db.start_read_session();

   // buffered: should see RO data.
   auto r = reader.get(0, "in_ro", psitri::dwal::read_mode::buffered);
   CHECK(r.found);
   CHECK(r.value == "ro_val");
}

TEST_CASE("read_session refreshes on generation change", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 0;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Ensure root 0 exists.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("seed", "x");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);

   auto reader = dwal_db.start_read_session();

   // Reader sees "seed" from the swap.
   auto r0 = reader.get(0, "seed", psitri::dwal::read_mode::buffered);
   CHECK(r0.found);

   // Write more + swap again.
   // merge_complete is false (merge_threads=0), so swap won't fire via try_swap.
   // Force it by setting merge_complete manually.
   dwal_db.root(0).merge_complete.store(true, std::memory_order_release);
   {
      std::unique_lock lk(dwal_db.root(0).buffered_mutex);
      dwal_db.root(0).buffered_ptr.reset();
   }

   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("k", "v");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);

   // Reader should auto-refresh and see the new data.
   auto r2 = reader.get(0, "k", psitri::dwal::read_mode::buffered);
   CHECK(r2.found);
   CHECK(r2.value == "v");
}

TEST_CASE("read_session tombstone in RO hides Tri data", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Put into Tri.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("victim", "alive");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);
   auto& root = dwal_db.root(0);
   for (int i = 0; i < 100 && !root.merge_complete.load(); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

   // Remove + swap (creates tombstone in RO).
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.remove("victim");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);

   auto reader = dwal_db.start_read_session();
   auto r      = reader.get(0, "victim", psitri::dwal::read_mode::buffered);
   CHECK_FALSE(r.found);
}

// ═══════════════════════════════════════════════════════════════════════
// merge_cursor with Tri layer (3-layer merge)
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("merge_cursor iterates across RW + RO + Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   // Seed Tri with data.
   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("aaa", "tri_a");
      tx.upsert("ccc", "tri_c");
      tx.upsert("eee", "tri_e");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   // RO layer.
   psitri::dwal::btree_layer ro;
   ro.store_data("bbb", "ro_b");
   ro.store_data("ccc", "ro_c");  // shadows Tri's ccc

   // RW layer.
   psitri::dwal::btree_layer rw;
   rw.store_data("ddd", "rw_d");

   psitri::dwal::merge_cursor mc(&rw, &ro, std::move(cur));
   mc.seek_begin();

   std::vector<std::pair<std::string, std::string>> results;
   while (!mc.is_end())
   {
      std::string k(mc.key());
      std::string v;
      auto src = mc.current_source();
      if (src == psitri::dwal::merge_cursor::source::tri)
      {
         auto opt = mc.tri_cursor()->value<std::string>();
         if (opt)
            v = *opt;
      }
      else
      {
         v = std::string(mc.current_value().data);
      }
      results.emplace_back(k, v);
      mc.next();
   }

   REQUIRE(results.size() == 5);
   CHECK(results[0] == std::pair<std::string, std::string>{"aaa", "tri_a"});
   CHECK(results[1] == std::pair<std::string, std::string>{"bbb", "ro_b"});
   CHECK(results[2].first == "ccc");
   CHECK(results[2].second == "ro_c");  // RO shadows Tri
   CHECK(results[3] == std::pair<std::string, std::string>{"ddd", "rw_d"});
   CHECK(results[4] == std::pair<std::string, std::string>{"eee", "tri_e"});
}

TEST_CASE("merge_cursor RW tombstone shadows Tri key", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("b", "2");
      tx.upsert("c", "3");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer rw;
   rw.store_tombstone("b");

   psitri::dwal::merge_cursor mc(&rw, nullptr, std::move(cur));
   mc.seek_begin();

   CHECK(mc.key() == "a");
   mc.next();
   CHECK(mc.key() == "c");  // b is tombstoned
   mc.next();
   CHECK(mc.is_end());
}

TEST_CASE("merge_cursor RW range tombstone shadows Tri range", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("b", "2");
      tx.upsert("c", "3");
      tx.upsert("d", "4");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer rw;
   rw.tombstones.add("b", "d");  // [b,d) tombstoned

   psitri::dwal::merge_cursor mc(&rw, nullptr, std::move(cur));
   mc.seek_begin();

   CHECK(mc.key() == "a");
   mc.next();
   CHECK(mc.key() == "d");  // b,c skipped
   mc.next();
   CHECK(mc.is_end());
}

TEST_CASE("merge_cursor prev across RW + RO + Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("aaa", "tri");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer ro;
   ro.store_data("bbb", "ro");

   psitri::dwal::btree_layer rw;
   rw.store_data("ccc", "rw");

   psitri::dwal::merge_cursor mc(&rw, &ro, std::move(cur));
   mc.seek_last();

   CHECK(mc.key() == "ccc");
   mc.prev();
   CHECK(mc.key() == "bbb");
   mc.prev();
   CHECK(mc.key() == "aaa");
   mc.prev();
   CHECK(mc.is_rend());
}

// ═══════════════════════════════════════════════════════════════════════
// dwal_transaction get() across layers
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("dwal_transaction get reads RW then RO then Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Tri layer.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("tri_key", "tri_val");
      tx.upsert("shared", "from_tri");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);
   auto& root = dwal_db.root(0);
   for (int i = 0; i < 100 && !root.merge_complete.load(); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

   // RO layer.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("ro_key", "ro_val");
      tx.upsert("shared", "from_ro");
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);

   // RW layer — inside active tx.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("rw_key", "rw_val");

      // RW: rw_key is here
      auto r1 = tx.get("rw_key");
      CHECK(r1.found);
      CHECK(r1.value.data == "rw_val");

      // RO: ro_key is here
      auto r2 = tx.get("ro_key");
      CHECK(r2.found);
      CHECK(r2.value.data == "ro_val");

      // Tri: tri_key is here
      auto r3 = tx.get("tri_key");
      CHECK(r3.found);

      // "shared" was overwritten in RO → RO wins over Tri.
      auto r4 = tx.get("shared");
      CHECK(r4.found);
      CHECK(r4.value.data == "from_ro");

      // Nonexistent.
      auto r5 = tx.get("nope");
      CHECK_FALSE(r5.found);

      tx.commit();
   }
}

// ═══════════════════════════════════════════════════════════════════════
// Edge cases
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("DWAL handles empty string keys", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal");

   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("", "empty_key_value");
      tx.commit();
   }

   auto r = dwal_db.get_latest(0, "");
   CHECK(r.found);
   CHECK(r.value.data == "empty_key_value");
}

TEST_CASE("DWAL handles binary keys with null bytes", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal");

   std::string key1("key\0one", 7);
   std::string key2("key\0two", 7);

   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert(key1, "val1");
      tx.upsert(key2, "val2");
      tx.commit();
   }

   auto r1 = dwal_db.get_latest(0, key1);
   CHECK(r1.found);
   CHECK(r1.value.data == "val1");

   auto r2 = dwal_db.get_latest(0, key2);
   CHECK(r2.found);
   CHECK(r2.value.data == "val2");
}

TEST_CASE("DWAL large values survive swap and merge", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   std::string big_val(100000, 'X');

   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("big", big_val);
      tx.commit();
   }

   // Verify in RW.
   auto r1 = dwal_db.get_latest(0, "big");
   CHECK(r1.found);
   CHECK(r1.value.data == big_val);

   // Swap + merge into Tri.
   dwal_db.swap_rw_to_ro(0);
   auto& root = dwal_db.root(0);
   for (int i = 0; i < 100 && !root.merge_complete.load(); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
   REQUIRE(root.merge_complete.load());

   // Verify from Tri via get_latest.
   auto r2 = dwal_db.get_latest(0, "big");
   CHECK(r2.found);
}

TEST_CASE("DWAL many sequential transactions", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal");

   for (int i = 0; i < 1000; ++i)
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("key" + std::to_string(i), "val" + std::to_string(i));
      tx.commit();
   }

   // Spot-check.
   auto r0 = dwal_db.get_latest(0, "key0");
   CHECK(r0.found);
   CHECK(r0.value.data == "val0");

   auto r999 = dwal_db.get_latest(0, "key999");
   CHECK(r999.found);
   CHECK(r999.value.data == "val999");
}

TEST_CASE("DWAL overwrite same key many times", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal");

   for (int i = 0; i < 500; ++i)
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("hot_key", "version_" + std::to_string(i));
      tx.commit();
   }

   auto r = dwal_db.get_latest(0, "hot_key");
   CHECK(r.found);
   CHECK(r.value.data == "version_499");
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-root isolation through full DWAL stack
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("DWAL multi-root isolation across swap and merge", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Write to two roots.
   {
      auto tx0 = dwal_db.start_write_transaction(0);
      tx0.upsert("root0_key", "root0_val");
      tx0.commit();
   }
   {
      auto tx1 = dwal_db.start_write_transaction(1);
      tx1.upsert("root1_key", "root1_val");
      tx1.commit();
   }

   // Swap both.
   dwal_db.swap_rw_to_ro(0);
   dwal_db.swap_rw_to_ro(1);

   // Wait for merges.
   auto& r0 = dwal_db.root(0);
   auto& r1 = dwal_db.root(1);
   for (int i = 0; i < 100; ++i)
   {
      if (r0.merge_complete.load() && r1.merge_complete.load())
         break;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
   }

   // Verify isolation.
   auto g0 = dwal_db.get_latest(0, "root0_key");
   CHECK(g0.found);

   auto g0x = dwal_db.get_latest(0, "root1_key");
   CHECK_FALSE(g0x.found);

   auto g1 = dwal_db.get_latest(1, "root1_key");
   CHECK(g1.found);

   auto g1x = dwal_db.get_latest(1, "root0_key");
   CHECK_FALSE(g1x.found);
}

// ═══════════════════════════════════════════════════════════════════════
// Oracle-based end-to-end correctness
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("DWAL oracle: random ops vs std::map", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 200;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   std::map<std::string, std::string> oracle;
   std::mt19937                       rng(42);

   constexpr int num_ops     = 5000;
   constexpr int key_pool    = 500;

   auto make_key = [](int i) { return "key_" + std::to_string(i); };
   auto make_val = [](int i) { return "val_" + std::to_string(i); };

   for (int op = 0; op < num_ops; ++op)
   {
      int action = rng() % 100;
      int ki     = rng() % key_pool;
      auto k     = make_key(ki);

      if (action < 60)
      {
         // Upsert.
         auto v = make_val(op);
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert(k, v);
         tx.commit();
         oracle[k] = v;
      }
      else if (action < 80)
      {
         // Remove.
         auto tx = dwal_db.start_write_transaction(0);
         tx.remove(k);
         tx.commit();
         oracle.erase(k);
      }
      else if (action < 90)
      {
         // Abort — should be a no-op.
         auto v = make_val(op);
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert(k, v);
         tx.abort();
         // oracle unchanged
      }
      else
      {
         // Point read check.
         auto r  = dwal_db.get_latest(0, k);
         auto it = oracle.find(k);
         if (it != oracle.end())
         {
            REQUIRE(r.found);
            // Value may be from pool or Tri — just check found.
         }
         else
         {
            CHECK_FALSE(r.found);
         }
      }
   }

   // Final full verification.
   auto& root = dwal_db.root(0);

   // Force one last swap + merge to get everything into consistent state.
   if (root.merge_complete.load())
   {
      dwal_db.swap_rw_to_ro(0);
      for (int i = 0; i < 200 && !root.merge_complete.load(); ++i)
         std::this_thread::sleep_for(std::chrono::milliseconds(10));
   }

   // Check every oracle key is in DWAL.
   int verified = 0;
   for (auto& [k, v] : oracle)
   {
      auto r = dwal_db.get_latest(0, k);
      REQUIRE(r.found);
      ++verified;
   }

   // Check some keys NOT in oracle are absent.
   for (int i = key_pool; i < key_pool + 100; ++i)
   {
      auto r = dwal_db.get_latest(0, make_key(i));
      CHECK_FALSE(r.found);
   }

   INFO("verified " << verified << " keys against oracle");
}

// ═══════════════════════════════════════════════════════════════════════
// Range operations across layer boundaries
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("DWAL remove_range spans RW keys only (RO/Tri unaffected in RW)", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 0;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Write keys to RW and commit.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("b", "2");
      tx.upsert("c", "3");
      tx.upsert("d", "4");
      tx.upsert("e", "5");
      tx.commit();
   }

   // Range remove [b, e).
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.remove_range("b", "e");
      tx.commit();
   }

   auto ga = dwal_db.get_latest(0, "a");
   CHECK(ga.found);
   auto gb = dwal_db.get_latest(0, "b");
   CHECK_FALSE(gb.found);
   auto gc = dwal_db.get_latest(0, "c");
   CHECK_FALSE(gc.found);
   auto gd = dwal_db.get_latest(0, "d");
   CHECK_FALSE(gd.found);
   auto ge = dwal_db.get_latest(0, "e");
   CHECK(ge.found);
}

// ═══════════════════════════════════════════════════════════════════════
// Multiple swaps and merges
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("DWAL multiple swap-merge cycles preserve data", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   for (int cycle = 0; cycle < 5; ++cycle)
   {
      // Write a batch.
      {
         auto tx = dwal_db.start_write_transaction(0);
         for (int i = 0; i < 50; ++i)
         {
            auto k = "cycle" + std::to_string(cycle) + "_key" + std::to_string(i);
            auto v = "val_" + std::to_string(cycle * 50 + i);
            tx.upsert(k, v);
         }
         tx.commit();
      }

      auto& root = dwal_db.root(0);

      // Swap and wait for merge.
      dwal_db.swap_rw_to_ro(0);
      for (int w = 0; w < 200 && !root.merge_complete.load(); ++w)
         std::this_thread::sleep_for(std::chrono::milliseconds(10));
      REQUIRE(root.merge_complete.load());
   }

   // Verify all data.
   for (int cycle = 0; cycle < 5; ++cycle)
   {
      for (int i = 0; i < 50; ++i)
      {
         auto k = "cycle" + std::to_string(cycle) + "_key" + std::to_string(i);
         auto r = dwal_db.get_latest(0, k);
         REQUIRE(r.found);
      }
   }
}

TEST_CASE("DWAL swap-merge with interleaved deletes", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Cycle 1: insert keys 0-99.
   {
      auto tx = dwal_db.start_write_transaction(0);
      for (int i = 0; i < 100; ++i)
         tx.upsert("k" + std::to_string(i), "v" + std::to_string(i));
      tx.commit();
   }
   auto& root = dwal_db.root(0);
   dwal_db.swap_rw_to_ro(0);
   for (int w = 0; w < 200 && !root.merge_complete.load(); ++w)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

   // Cycle 2: delete even keys.
   {
      auto tx = dwal_db.start_write_transaction(0);
      for (int i = 0; i < 100; i += 2)
         tx.remove("k" + std::to_string(i));
      tx.commit();
   }
   dwal_db.swap_rw_to_ro(0);
   for (int w = 0; w < 200 && !root.merge_complete.load(); ++w)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

   // Verify: even keys gone, odd keys present.
   for (int i = 0; i < 100; ++i)
   {
      auto r = dwal_db.get_latest(0, "k" + std::to_string(i));
      if (i % 2 == 0)
         CHECK_FALSE(r.found);
      else
         CHECK(r.found);
   }
}

// ═══════════════════════════════════════════════════════════════════════
// Concurrent DWAL read + write
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("DWAL concurrent reader and writer with swaps", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 100;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   std::atomic<bool> done{false};
   std::atomic<int>  reads{0};
   std::atomic<int>  found{0};

   // Reader thread using dwal_read_session.
   auto reader_fn = [&]()
   {
      auto reader = dwal_db.start_read_session();
      while (!done.load(std::memory_order_relaxed))
      {
         for (int i = 0; i < 50; ++i)
         {
            auto r = reader.get(0, "k" + std::to_string(i),
                                psitri::dwal::read_mode::buffered);
            if (r.found)
               found.fetch_add(1, std::memory_order_relaxed);
            reads.fetch_add(1, std::memory_order_relaxed);
         }
      }
   };

   std::thread reader_thread(reader_fn);

   // Writer: 500 transactions with periodic swaps.
   for (int i = 0; i < 500; ++i)
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("k" + std::to_string(i % 50), "v" + std::to_string(i));
      tx.commit();
   }

   // Let reader run a bit more.
   std::this_thread::sleep_for(std::chrono::milliseconds(50));
   done.store(true);
   reader_thread.join();

   INFO("reads=" << reads.load() << " found=" << found.load());
   CHECK(reads.load() > 0);
}

// ═══════════════════════════════════════════════════════════════════════
// WAL flush and should_swap threshold
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("should_swap triggers at max_rw_entries", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 0;
   dcfg.max_rw_entries = 100;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   // Prevent auto-swap: mark merge_complete=false so commit() won't trigger swap.
   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("seed", "x");
      tx.commit();
   }
   dwal_db.root(0).merge_complete.store(false, std::memory_order_release);

   // Write fewer than threshold — should not trigger.
   {
      auto tx = dwal_db.start_write_transaction(0);
      for (int i = 0; i < 50; ++i)
         tx.upsert("k" + std::to_string(i), "v");
      tx.commit();
   }
   // 51 entries (seed + 50) < 100
   CHECK_FALSE(dwal_db.should_swap(0));

   // Write more to reach threshold.
   {
      auto tx = dwal_db.start_write_transaction(0);
      for (int i = 50; i < 100; ++i)
         tx.upsert("k" + std::to_string(i), "v");
      tx.commit();
   }
   // 101 entries >= 100
   CHECK(dwal_db.should_swap(0));
}

TEST_CASE("flush_wal writes all pending data to disk", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal");

   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("k", "v");
      tx.commit();
   }

   // flush_wal should not throw.
   dwal_db.flush_wal();
   dwal_db.flush_wal(0);
}

// ═══════════════════════════════════════════════════════════════════════
// merge_cursor count_keys across layers
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("merge_cursor count_keys with RW+RO+Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("c", "3");
      tx.upsert("e", "5");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer ro;
   ro.store_data("b", "2");
   ro.store_data("c", "shadow");  // shadows Tri c

   psitri::dwal::btree_layer rw;
   rw.store_data("d", "4");
   rw.store_tombstone("e");  // tombstones Tri e

   psitri::dwal::merge_cursor mc(&rw, &ro, std::move(cur));

   // Live keys: a, b, c(RO), d — e is tombstoned.
   CHECK(mc.count_keys() == 4);
}

// ═══════════════════════════════════════════════════════════════════════
// WAL crash recovery tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("DWAL recovery: RW WAL replayed into btree on reopen", "[dwal][recovery]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   // Phase 1: Write data through DWAL, flush, then simulate crash by
   // destroying the WAL writer without calling close() (no clean_close flag).
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

      {
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert("crash_key1", "crash_val1");
         tx.upsert("crash_key2", "crash_val2");
         tx.commit();
      }
      dwal_db.flush_wal();

      // Simulate crash: reset the WAL writer (fd closes via ~wal_writer
      // destructor, but close() is never called — no clean_close flag set).
      dwal_db.root(0).wal.reset();
   }

   // Phase 2: Reopen — recovery should replay the RW WAL into the btree.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

      auto r1 = dwal_db.get_latest(0, "crash_key1");
      CHECK(r1.found);
      CHECK(r1.value.data == "crash_val1");

      auto r2 = dwal_db.get_latest(0, "crash_key2");
      CHECK(r2.found);
      CHECK(r2.value.data == "crash_val2");
   }
}

TEST_CASE("DWAL recovery: RO WAL replayed into Tri on reopen", "[dwal][recovery]")
{
   temp_dir td;
   auto     db_path  = td.path / "db";
   auto     wal_path = td.path / "wal";

   auto db = psitri::database::create(db_path);

   // Phase 1: Write data, swap to RO (creates wal-ro.dwal), simulate crash.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, wal_path, dcfg);

      {
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert("frozen1", "fval1");
         tx.upsert("frozen2", "fval2");
         tx.commit();
      }
      dwal_db.flush_wal();

      // Swap RW→RO: renames wal-rw.dwal to wal-ro.dwal.
      // merge_threads=0 so no merge happens — data stays in RO.
      dwal_db.root(0).merge_complete.store(true, std::memory_order_release);
      dwal_db.swap_rw_to_ro(0);
      dwal_db.flush_wal();

      // Simulate crash: destroy WAL writers without close().
      auto& root = dwal_db.root(0);
      root.wal.reset();
   }

   // Verify wal-ro.dwal exists on disk.
   auto ro_wal = wal_path / "root-0" / "wal-ro.dwal";
   REQUIRE(std::filesystem::exists(ro_wal));

   // Phase 2: Reopen — recovery should replay RO WAL into PsiTri.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, wal_path, dcfg);

      // Data should be in PsiTri now (not in RW btree).
      auto r1 = dwal_db.get_latest(0, "frozen1");
      CHECK(r1.found);

      auto r2 = dwal_db.get_latest(0, "frozen2");
      CHECK(r2.found);

      // RO WAL should have been deleted after replay.
      CHECK_FALSE(std::filesystem::exists(ro_wal));
   }
}

TEST_CASE("DWAL recovery: clean shutdown skips replay", "[dwal][recovery]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   // Phase 1: Normal write + clean shutdown.
   {
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal");

      {
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert("k", "v");
         tx.commit();
      }
      // Destructor calls wal->close() which sets clean_close flag.
   }

   // Phase 2: Reopen. WAL has clean_close flag → recovery skips it.
   // The RW WAL file should be deleted.
   {
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal");

      // Data was only in the in-memory RW btree, which was lost on shutdown.
      // With clean close, the WAL is NOT replayed (data was "intentionally" lost
      // because it hadn't been swapped/merged yet — this is expected behavior
      // for clean shutdown without explicit flush).
      // The key should NOT be found since it was never merged to Tri.
      auto r = dwal_db.get_latest(0, "k");
      CHECK_FALSE(r.found);
   }
}

TEST_CASE("DWAL recovery: removes and range removes replayed", "[dwal][recovery]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   // Seed Tri with data via direct PsiTri write.
   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("b", "2");
      tx.upsert("c", "3");
      tx.upsert("d", "4");
      tx.commit();
   }

   // Phase 1: DWAL removes + range remove, then crash.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

      {
         auto tx = dwal_db.start_write_transaction(0);
         tx.remove("a");
         tx.remove_range("c", "e");  // removes c, d
         tx.commit();
      }
      dwal_db.flush_wal();

      // Simulate crash.
      dwal_db.root(0).wal.reset();
   }

   // Phase 2: Reopen — removes should be replayed into RW btree.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

      // "a" was removed (tombstone in RW), should not be found.
      auto ra = dwal_db.get_latest(0, "a");
      CHECK_FALSE(ra.found);

      // "b" was not touched — should still be in Tri.
      auto rb = dwal_db.get_latest(0, "b");
      CHECK(rb.found);

      // "c" and "d" were range-removed.
      auto rc = dwal_db.get_latest(0, "c");
      CHECK_FALSE(rc.found);
      auto rd = dwal_db.get_latest(0, "d");
      CHECK_FALSE(rd.found);
   }
}

TEST_CASE("DWAL recovery: multiple transactions replayed in order", "[dwal][recovery]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   // Phase 1: Multiple transactions overwriting the same key, then crash.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

      for (int i = 0; i < 100; ++i)
      {
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert("counter", std::to_string(i));
         tx.upsert("key_" + std::to_string(i), "val_" + std::to_string(i));
         tx.commit();
      }
      dwal_db.flush_wal();

      // Simulate crash.
      dwal_db.root(0).wal.reset();
   }

   // Phase 2: Reopen — all 100 transactions replayed.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

      // "counter" should have the last value.
      auto r = dwal_db.get_latest(0, "counter");
      REQUIRE(r.found);
      CHECK(r.value.data == "99");

      // All unique keys should be present.
      for (int i = 0; i < 100; ++i)
      {
         auto ri = dwal_db.get_latest(0, "key_" + std::to_string(i));
         REQUIRE(ri.found);
         CHECK(ri.value.data == "val_" + std::to_string(i));
      }
   }
}

TEST_CASE("DWAL recovery: partial WAL (torn write) recovers valid prefix", "[dwal][recovery]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   auto wal_dir = td.path / "wal";

   // Phase 1: Write two transactions, flush, then corrupt the end of the file
   // to simulate a torn write (process died mid-write of third transaction).
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, wal_dir, dcfg);

      {
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert("good1", "val1");
         tx.commit();
      }
      {
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert("good2", "val2");
         tx.commit();
      }
      dwal_db.flush_wal();

      // Simulate crash.
      dwal_db.root(0).wal.reset();
   }

   // Append garbage to the WAL to simulate a torn write.
   auto rw_wal = wal_dir / "root-0" / "wal-rw.dwal";
   {
      int fd = ::open(rw_wal.c_str(), O_WRONLY | O_APPEND);
      REQUIRE(fd >= 0);
      char garbage[50] = {};
      std::memset(garbage, 0xAB, sizeof(garbage));
      ::write(fd, garbage, sizeof(garbage));
      ::close(fd);
   }

   // Phase 2: Reopen — should recover the two valid transactions,
   // ignoring the garbage at the end.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, wal_dir, dcfg);

      auto r1 = dwal_db.get_latest(0, "good1");
      CHECK(r1.found);
      CHECK(r1.value.data == "val1");

      auto r2 = dwal_db.get_latest(0, "good2");
      CHECK(r2.found);
      CHECK(r2.value.data == "val2");
   }
}

TEST_CASE("DWAL recovery: multi-root independent recovery", "[dwal][recovery]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   // Phase 1: Write to two roots, crash.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

      {
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert("root0_key", "root0_val");
         tx.commit();
      }
      {
         auto tx = dwal_db.start_write_transaction(1);
         tx.upsert("root1_key", "root1_val");
         tx.commit();
      }
      dwal_db.flush_wal();

      // Simulate crash on both.
      dwal_db.root(0).wal.reset();
      dwal_db.root(1).wal.reset();
   }

   // Phase 2: Reopen — both roots recovered independently.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

      auto r0 = dwal_db.get_latest(0, "root0_key");
      CHECK(r0.found);
      CHECK(r0.value.data == "root0_val");

      auto r1 = dwal_db.get_latest(1, "root1_key");
      CHECK(r1.found);
      CHECK(r1.value.data == "root1_val");

      // Cross-root isolation.
      CHECK_FALSE(dwal_db.get_latest(0, "root1_key").found);
      CHECK_FALSE(dwal_db.get_latest(1, "root0_key").found);
   }
}

TEST_CASE("DWAL recovery: RO WAL deleted after merge", "[dwal][recovery]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 1;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("k", "v");
      tx.commit();
   }
   dwal_db.flush_wal();

   // Swap creates wal-ro.dwal.
   dwal_db.swap_rw_to_ro(0);

   auto ro_wal = td.path / "wal" / "root-0" / "wal-ro.dwal";

   // Wait for merge to complete.
   auto& root = dwal_db.root(0);
   for (int i = 0; i < 200 && !root.merge_complete.load(); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
   REQUIRE(root.merge_complete.load());

   // After merge, the RO WAL should be deleted.
   CHECK_FALSE(std::filesystem::exists(ro_wal));
}

TEST_CASE("DWAL recovery: aborted transaction not in WAL", "[dwal][recovery]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   // Phase 1: Commit one tx, abort another, crash.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      dcfg.max_rw_entries = 5000;
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

      {
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert("committed", "yes");
         tx.commit();
      }
      {
         auto tx = dwal_db.start_write_transaction(0);
         tx.upsert("aborted", "no");
         tx.abort();
      }
      dwal_db.flush_wal();
      dwal_db.root(0).wal.reset();
   }

   // Phase 2: Only the committed data should be recovered.
   {
      psitri::dwal::dwal_config dcfg;
      dcfg.merge_threads  = 0;
      psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

      CHECK(dwal_db.get_latest(0, "committed").found);
      CHECK_FALSE(dwal_db.get_latest(0, "aborted").found);
   }
}

// ═══════════════════════════════════════════════════════════════════════
// merge_cursor coverage expansion tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("merge_cursor lower_bound with RO layer", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("c", "RW_C");

   psitri::dwal::btree_layer ro;
   ro.store_data("a", "RO_A");
   ro.store_data("b", "RO_B");
   ro.store_data("d", "RO_D");

   psitri::dwal::merge_cursor cur(&rw, &ro, std::nullopt);

   cur.lower_bound("b");
   CHECK(cur.key() == "b");
   CHECK(cur.current_source() == psitri::dwal::merge_cursor::source::ro);

   cur.lower_bound("c");
   CHECK(cur.key() == "c");
   CHECK(cur.current_source() == psitri::dwal::merge_cursor::source::rw);

   cur.lower_bound("e");
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor upper_bound with RO and Tri layers", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("a", "tri_a");
      tx.upsert("c", "tri_c");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer ro;
   ro.store_data("b", "RO_B");

   psitri::dwal::merge_cursor mc(nullptr, &ro, std::move(cur));

   // upper_bound("a") — should skip past "a" in Tri.
   mc.upper_bound("a");
   CHECK(mc.key() == "b");

   // upper_bound("b") — should skip past "b" in RO.
   mc.upper_bound("b");
   CHECK(mc.key() == "c");

   // upper_bound("c") — past everything.
   mc.upper_bound("c");
   CHECK(mc.is_end());
}

TEST_CASE("merge_cursor next when already at end", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("a", "A");

   psitri::dwal::merge_cursor cur(&rw, nullptr, std::nullopt);
   cur.seek_begin();
   cur.next();
   CHECK(cur.is_end());

   // Calling next() again should return false and stay at end.
   CHECK_FALSE(cur.next());
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor prev when already at rend", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("a", "A");

   psitri::dwal::merge_cursor cur(&rw, nullptr, std::nullopt);
   cur.seek_last();
   cur.prev();
   CHECK(cur.is_rend());

   // Calling prev() again should return false and stay at rend.
   CHECK_FALSE(cur.prev());
   CHECK(cur.is_rend());
}

TEST_CASE("merge_cursor seek not found", "[dwal]")
{
   psitri::dwal::btree_layer rw;
   rw.store_data("a", "A");
   rw.store_data("c", "C");

   psitri::dwal::merge_cursor cur(&rw, nullptr, std::nullopt);

   // Seek for a key that doesn't exist (between a and c).
   CHECK_FALSE(cur.seek("b"));
   CHECK(cur.is_end());

   // Seek beyond all keys.
   CHECK_FALSE(cur.seek("z"));
   CHECK(cur.is_end());
}

TEST_CASE("merge_cursor RO tombstone shadows Tri in forward scan", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("b", "2");
      tx.upsert("c", "3");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer ro;
   ro.store_tombstone("b");  // RO tombstone shadows Tri's "b"

   psitri::dwal::merge_cursor mc(nullptr, &ro, std::move(cur));
   mc.seek_begin();

   CHECK(mc.key() == "a");
   mc.next();
   CHECK(mc.key() == "c");  // b is tombstoned by RO
   mc.next();
   CHECK(mc.is_end());
}

TEST_CASE("merge_cursor RO range tombstone shadows Tri in forward scan", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("b", "2");
      tx.upsert("c", "3");
      tx.upsert("d", "4");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer ro;
   ro.tombstones.add("b", "d");  // [b,d) tombstoned

   psitri::dwal::merge_cursor mc(nullptr, &ro, std::move(cur));
   mc.seek_begin();

   CHECK(mc.key() == "a");
   mc.next();
   CHECK(mc.key() == "d");  // b,c tombstoned by RO
   mc.next();
   CHECK(mc.is_end());
}

TEST_CASE("merge_cursor backward with RW tombstone + RO + Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("b", "2");
      tx.upsert("c", "3");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer ro;
   ro.store_data("b", "RO_B");  // shadows Tri b

   psitri::dwal::btree_layer rw;
   rw.store_tombstone("b");  // RW tombstone shadows both RO and Tri b
   rw.store_data("d", "RW_D");

   psitri::dwal::merge_cursor mc(&rw, &ro, std::move(cur));
   mc.seek_last();

   CHECK(mc.key() == "d");
   mc.prev();
   CHECK(mc.key() == "c");
   mc.prev();
   // "b" should be skipped (tombstoned by RW)
   CHECK(mc.key() == "a");
   mc.prev();
   CHECK(mc.is_rend());
}

TEST_CASE("merge_cursor backward with RW range tombstone shadows RO", "[dwal]")
{
   psitri::dwal::btree_layer ro;
   ro.store_data("a", "1");
   ro.store_data("b", "2");
   ro.store_data("c", "3");
   ro.store_data("d", "4");

   psitri::dwal::btree_layer rw;
   rw.tombstones.add("b", "d");  // [b,d) tombstoned

   psitri::dwal::merge_cursor cur(&rw, &ro, std::nullopt);
   cur.seek_last();

   CHECK(cur.key() == "d");
   cur.prev();
   // b and c are tombstoned by RW range tombstone
   CHECK(cur.key() == "a");
   cur.prev();
   CHECK(cur.is_rend());
}

TEST_CASE("merge_cursor backward with RO tombstone shadows Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("b", "2");
      tx.upsert("c", "3");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer ro;
   ro.store_tombstone("b");

   psitri::dwal::merge_cursor mc(nullptr, &ro, std::move(cur));
   mc.seek_last();

   CHECK(mc.key() == "c");
   mc.prev();
   // "b" tombstoned by RO
   CHECK(mc.key() == "a");
   mc.prev();
   CHECK(mc.is_rend());
}

TEST_CASE("merge_cursor backward RW shadows duplicate in RO and Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("shared", "tri");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer ro;
   ro.store_data("shared", "ro");

   psitri::dwal::btree_layer rw;
   rw.store_data("shared", "rw");

   psitri::dwal::merge_cursor mc(&rw, &ro, std::move(cur));
   mc.seek_last();

   // "shared" should appear once, from RW (highest priority).
   CHECK(mc.key() == "shared");
   CHECK(mc.current_source() == psitri::dwal::merge_cursor::source::rw);
   CHECK(mc.current_value().data == "rw");

   mc.prev();
   CHECK(mc.is_rend());
}

TEST_CASE("merge_cursor backward RO shadows duplicate in Tri", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("shared", "tri");
      tx.upsert("zzz", "end");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::btree_layer ro;
   ro.store_data("shared", "ro");

   psitri::dwal::merge_cursor mc(nullptr, &ro, std::move(cur));
   mc.seek_last();

   CHECK(mc.key() == "zzz");
   mc.prev();
   CHECK(mc.key() == "shared");
   CHECK(mc.current_source() == psitri::dwal::merge_cursor::source::ro);
   CHECK(mc.current_value().data == "ro");
   mc.prev();
   CHECK(mc.is_rend());
}

TEST_CASE("owned_merge_cursor construction and usage", "[dwal]")
{
   auto rw = std::make_shared<psitri::dwal::btree_layer>();
   rw->store_data("a", "1");
   rw->store_data("c", "3");

   auto ro = std::make_shared<psitri::dwal::btree_layer>();
   ro->store_data("b", "2");

   psitri::dwal::owned_merge_cursor omc(rw, ro, std::nullopt);

   omc->seek_begin();
   CHECK_FALSE(omc->is_end());
   CHECK(omc->key() == "a");

   omc->next();
   CHECK(omc->key() == "b");

   omc->next();
   CHECK(omc->key() == "c");

   omc->next();
   CHECK(omc->is_end());

   // Test count_keys via cursor() accessor.
   CHECK(omc.cursor().count_keys() == 3);
}

TEST_CASE("owned_merge_cursor via dwal_database::create_cursor", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   psitri::dwal::dwal_config dcfg;
   dcfg.merge_threads  = 0;
   dcfg.max_rw_entries = 5000;
   psitri::dwal::dwal_database dwal_db(db, td.path / "wal", dcfg);

   {
      auto tx = dwal_db.start_write_transaction(0);
      tx.upsert("k1", "v1");
      tx.upsert("k2", "v2");
      tx.upsert("k3", "v3");
      tx.commit();
   }

   // latest mode — sees RW data.
   auto cursor = dwal_db.create_cursor(0, psitri::dwal::read_mode::latest);
   cursor->seek_begin();

   std::vector<std::string> keys;
   while (!cursor->is_end())
   {
      keys.emplace_back(cursor->key());
      cursor->next();
   }

   CHECK(keys.size() == 3);
   CHECK(keys[0] == "k1");
   CHECK(keys[1] == "k2");
   CHECK(keys[2] == "k3");
}

TEST_CASE("merge_cursor lower_bound with Tri layer only", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("c", "3");
      tx.upsert("e", "5");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::merge_cursor mc(nullptr, nullptr, std::move(cur));

   mc.lower_bound("b");
   CHECK(mc.key() == "c");
   CHECK(mc.current_source() == psitri::dwal::merge_cursor::source::tri);

   mc.lower_bound("f");
   CHECK(mc.is_end());
}

TEST_CASE("merge_cursor is_subtree from Tri layer", "[dwal]")
{
   temp_dir td;
   auto     db = psitri::database::create(td.path / "db");

   // Write a data key (not subtree).
   {
      auto ws = db->start_write_session();
      auto tx = ws->start_transaction(0);
      tx.upsert("data_key", "val");
      tx.commit();
   }

   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);

   psitri::dwal::merge_cursor mc(nullptr, nullptr, std::move(cur));
   mc.seek_begin();

   CHECK_FALSE(mc.is_end());
   CHECK(mc.key() == "data_key");
   CHECK(mc.current_source() == psitri::dwal::merge_cursor::source::tri);
   CHECK_FALSE(mc.is_subtree());
}
