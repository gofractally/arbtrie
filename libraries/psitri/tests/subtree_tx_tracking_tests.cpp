#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>

using namespace psitri;

// ── Test helpers ─────────────────────────────────────────────────────────────

namespace
{
   struct tx_track_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      tx_track_db(const std::string& name = "subtree_tx_track_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = database::open(dir);
         ses = db->start_write_session();
      }

      ~tx_track_db() { std::filesystem::remove_all(dir); }

      void wait_for_compactor(int max_ms = 5000)
      {
         if (!db->wait_for_compactor(std::chrono::milliseconds(max_ms)))
            WARN("compactor timeout");
      }

      void assert_no_leaks(const std::string& ctx = "")
      {
         ses->set_root(0, {}, sal::sync_type::none);
         wait_for_compactor();
         uint64_t n = ses->get_total_allocated_objects();
         INFO(ctx << " allocated=" << n << " (expected 0)");
         REQUIRE(n == 0);
      }

      // Build a subtree with `n` rows and commit it at `key` in root 0.
      void seed_subtree(key_view key, int n, const std::string& prefix = "row")
      {
         auto sub = ses->create_write_cursor();
         for (int i = 0; i < n; ++i)
         {
            char k[64], v[64];
            std::snprintf(k, sizeof(k), "%s%04d", prefix.c_str(), i);
            std::snprintf(v, sizeof(v), "val_%s%04d", prefix.c_str(), i);
            sub->upsert(key_view(k, std::strlen(k)), value_view(v, std::strlen(v)));
         }
         auto txn = ses->start_transaction(0);
         txn.upsert(key, sub->take_root());
         txn.commit();
      }

      uint64_t subtree_key_count(key_view key)
      {
         auto root = ses->get_root(0);
         if (!root)
            return 0;
         cursor c(root);
         auto   sub = c.seek(key) ? c.subtree() : sal::smart_ptr<sal::alloc_header>{};
         if (!sub)
            return 0;
         return cursor(sub).count_keys();
      }

      bool subtree_has(key_view subtree_key, key_view row_key)
      {
         auto root = ses->get_root(0);
         if (!root)
            return false;
         cursor c(root);
         if (!c.seek(subtree_key) || !c.is_subtree())
            return false;
         return cursor(c.subtree()).get<std::string>(row_key).has_value();
      }
   };
}  // namespace

// ═════════════════════════════════════════════════════════════════════════════
// with_subtree: basic commit
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: with_subtree changes are committed with the transaction",
          "[subtree][tx-tracking]")
{
   tx_track_db t;
   t.seed_subtree(to_key("accounts"), 10);

   {
      auto txn = t.ses->start_transaction(0);
      txn.with_subtree(to_key("accounts"), [](write_cursor& sub) {
         sub.upsert(to_key("alice"), to_value("100"));
      });
      txn.commit();
   }

   REQUIRE(t.subtree_has(to_key("accounts"), to_key("alice")));
   REQUIRE(t.subtree_key_count(to_key("accounts")) == 11);

   t.assert_no_leaks("basic with_subtree commit");
}

// ═════════════════════════════════════════════════════════════════════════════
// with_subtree: abort discards changes
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: with_subtree changes are discarded on abort",
          "[subtree][tx-tracking]")
{
   tx_track_db t;
   t.seed_subtree(to_key("accounts"), 10);

   {
      auto txn = t.ses->start_transaction(0);
      txn.with_subtree(to_key("accounts"), [](write_cursor& sub) {
         sub.upsert(to_key("alice"), to_value("100"));
      });
      txn.abort();
   }

   REQUIRE_FALSE(t.subtree_has(to_key("accounts"), to_key("alice")));
   REQUIRE(t.subtree_key_count(to_key("accounts")) == 10);

   t.assert_no_leaks("with_subtree abort");
}

// ═════════════════════════════════════════════════════════════════════════════
// with_subtree: multiple subtrees committed atomically
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: multiple with_subtree cursors committed atomically",
          "[subtree][tx-tracking]")
{
   tx_track_db t;
   t.seed_subtree(to_key("accounts"), 5);
   t.seed_subtree(to_key("index"), 5, "idx");

   {
      auto txn = t.ses->start_transaction(0);
      txn.with_subtree(to_key("accounts"), [](write_cursor& sub) {
         sub.upsert(to_key("alice"), to_value("100"));
      });
      txn.with_subtree(to_key("index"), [](write_cursor& sub) {
         sub.upsert(to_key("alice_idx"), to_value("ref"));
      });
      txn.commit();
   }

   REQUIRE(t.subtree_has(to_key("accounts"), to_key("alice")));
   REQUIRE(t.subtree_has(to_key("index"), to_key("alice_idx")));

   t.assert_no_leaks("multiple with_subtree commit");
}

// ═════════════════════════════════════════════════════════════════════════════
// with_subtree: multiple subtrees aborted atomically
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: multiple with_subtree cursors discarded on abort",
          "[subtree][tx-tracking]")
{
   tx_track_db t;
   t.seed_subtree(to_key("accounts"), 5);
   t.seed_subtree(to_key("index"), 5, "idx");

   {
      auto txn = t.ses->start_transaction(0);
      txn.with_subtree(to_key("accounts"), [](write_cursor& sub) {
         sub.upsert(to_key("alice"), to_value("100"));
      });
      txn.with_subtree(to_key("index"), [](write_cursor& sub) {
         sub.upsert(to_key("alice_idx"), to_value("ref"));
      });
      txn.abort();
   }

   REQUIRE_FALSE(t.subtree_has(to_key("accounts"), to_key("alice")));
   REQUIRE_FALSE(t.subtree_has(to_key("index"), to_key("alice_idx")));

   t.assert_no_leaks("multiple with_subtree abort");
}

// ═════════════════════════════════════════════════════════════════════════════
// with_subtree: same key called twice — second call sees first call's writes
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: same key called twice accumulates changes",
          "[subtree][tx-tracking]")
{
   tx_track_db t;
   t.seed_subtree(to_key("accounts"), 5);

   {
      auto txn = t.ses->start_transaction(0);
      txn.with_subtree(to_key("accounts"), [](write_cursor& sub) {
         sub.upsert(to_key("alice"), to_value("100"));
      });
      // Second call must see alice from the first call.
      bool alice_visible = false;
      txn.with_subtree(to_key("accounts"), [&](write_cursor& sub) {
         alice_visible = sub.get<std::string>(to_key("alice")).has_value();
         sub.upsert(to_key("bob"), to_value("200"));
      });
      REQUIRE(alice_visible);
      txn.commit();
   }

   REQUIRE(t.subtree_has(to_key("accounts"), to_key("alice")));
   REQUIRE(t.subtree_has(to_key("accounts"), to_key("bob")));

   t.assert_no_leaks("with_subtree accumulates");
}

// ═════════════════════════════════════════════════════════════════════════════
// with_subtree + flat keys: committed atomically
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: with_subtree and flat-key changes committed together",
          "[subtree][tx-tracking]")
{
   tx_track_db t;
   t.seed_subtree(to_key("accounts"), 5);

   {
      auto txn = t.ses->start_transaction(0);
      txn.upsert(to_key("meta"), to_value("v1"));
      txn.with_subtree(to_key("accounts"), [](write_cursor& sub) {
         sub.upsert(to_key("alice"), to_value("100"));
      });
      txn.commit();
   }

   REQUIRE(t.subtree_has(to_key("accounts"), to_key("alice")));
   {
      auto root = t.ses->get_root(0);
      REQUIRE(cursor(root).get<std::string>(to_key("meta")).has_value());
   }

   t.assert_no_leaks("with_subtree + flat key commit");
}

// ═════════════════════════════════════════════════════════════════════════════
// Frame: abort_frame reverts subtree edits within that frame
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: frame abort reverts subtree edits within that frame",
          "[subtree][tx-tracking][frame]")
{
   tx_track_db t;
   t.seed_subtree(to_key("accounts"), 5);

   {
      auto txn = t.ses->start_transaction(0);
      txn.with_subtree(to_key("accounts"), [](write_cursor& sub) {
         sub.upsert(to_key("alice"), to_value("100"));
      });

      {
         auto frame = txn.sub_transaction();
         frame.with_subtree(to_key("accounts"), [](write_cursor& sub) {
            sub.upsert(to_key("bob"), to_value("200"));
         });
         frame.abort();  // bob must vanish, alice must survive
      }

      bool alice_ok = false, bob_absent = false;
      txn.with_subtree(to_key("accounts"), [&](write_cursor& sub) {
         alice_ok   = sub.get<std::string>(to_key("alice")).has_value();
         bob_absent = !sub.get<std::string>(to_key("bob")).has_value();
      });
      REQUIRE(alice_ok);
      REQUIRE(bob_absent);

      txn.commit();
   }

   REQUIRE(t.subtree_has(to_key("accounts"), to_key("alice")));
   REQUIRE_FALSE(t.subtree_has(to_key("accounts"), to_key("bob")));

   t.assert_no_leaks("frame abort reverts subtree");
}

// ═════════════════════════════════════════════════════════════════════════════
// Frame: commit_frame propagates subtree edits to parent scope
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: frame commit propagates subtree edits to parent",
          "[subtree][tx-tracking][frame]")
{
   tx_track_db t;
   t.seed_subtree(to_key("accounts"), 5);

   {
      auto txn = t.ses->start_transaction(0);

      {
         auto frame = txn.sub_transaction();
         frame.with_subtree(to_key("accounts"), [](write_cursor& sub) {
            sub.upsert(to_key("alice"), to_value("100"));
         });
         frame.commit();
      }

      bool alice_visible = false;
      txn.with_subtree(to_key("accounts"), [&](write_cursor& sub) {
         alice_visible = sub.get<std::string>(to_key("alice")).has_value();
      });
      REQUIRE(alice_visible);

      txn.commit();
   }

   REQUIRE(t.subtree_has(to_key("accounts"), to_key("alice")));
   t.assert_no_leaks("frame commit propagates subtree");
}

// ═════════════════════════════════════════════════════════════════════════════
// Frame: outer abort discards frame-committed subtree edits
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: outer abort discards frame-committed subtree edits",
          "[subtree][tx-tracking][frame]")
{
   tx_track_db t;
   t.seed_subtree(to_key("accounts"), 5);

   {
      auto txn = t.ses->start_transaction(0);

      {
         auto frame = txn.sub_transaction();
         frame.with_subtree(to_key("accounts"), [](write_cursor& sub) {
            sub.upsert(to_key("alice"), to_value("100"));
         });
         frame.commit();
      }

      txn.abort();
   }

   REQUIRE_FALSE(t.subtree_has(to_key("accounts"), to_key("alice")));
   REQUIRE(t.subtree_key_count(to_key("accounts")) == 5);

   t.assert_no_leaks("outer abort discards frame-committed subtree");
}

// ═════════════════════════════════════════════════════════════════════════════
// with_subtree on a non-existent key creates empty subtree on commit
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: with_subtree on missing key creates subtree on commit",
          "[subtree][tx-tracking]")
{
   tx_track_db t;

   {
      auto txn = t.ses->start_transaction(0);
      txn.with_subtree(to_key("new_table"), [](write_cursor& sub) {
         sub.upsert(to_key("row0"), to_value("data"));
      });
      txn.commit();
   }

   REQUIRE(t.subtree_has(to_key("new_table"), to_key("row0")));
   REQUIRE(t.subtree_key_count(to_key("new_table")) == 1);

   t.assert_no_leaks("with_subtree creates new subtree");
}

// ═════════════════════════════════════════════════════════════════════════════
// Micro mode: with_subtree flushes buffer first and works correctly
// ═════════════════════════════════════════════════════════════════════════════

TEST_CASE("subtree-tx: micro mode with_subtree flushes buffer and works",
          "[subtree][tx-tracking][micro]")
{
   tx_track_db t;
   t.seed_subtree(to_key("accounts"), 5);

   {
      auto txn = t.ses->start_transaction(0, tx_mode::micro);
      txn.upsert(to_key("meta"), to_value("v1"));
      txn.with_subtree(to_key("accounts"), [](write_cursor& sub) {
         sub.upsert(to_key("alice"), to_value("100"));
      });
      txn.commit();
   }

   REQUIRE(t.subtree_has(to_key("accounts"), to_key("alice")));
   {
      auto root = t.ses->get_root(0);
      REQUIRE(cursor(root).get<std::string>(to_key("meta")).has_value());
   }

   t.assert_no_leaks("micro mode with_subtree");
}
