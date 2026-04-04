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
#include <mutex>
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
