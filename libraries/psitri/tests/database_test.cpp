#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/value_type.hpp>
#include <sal/smart_ptr.hpp>

TEST_CASE("create_db", "[psitri][database]")
{
   using namespace psitri;
   auto db  = database::open("testdb");
   auto ses = db->start_write_session();

   // this will create a transient tree given it wasn't constructed
   // from an existing tree.
   auto cur = ses->create_write_cursor();
   cur->upsert(to_key("hello"), to_value("world"));

   std::optional<std::string> optval = cur->get<std::string>(to_key("hello"));
   REQUIRE(optval and *optval == "world");

   // on cursor destruction the transient tree is destroyed because
   // it was not saved anywhere.
}

TEST_CASE("multi_thread_subtree", "[psitri][database]")
{
   using namespace psitri;
   database_ptr db = database::open("testdb");
   // thread master
   write_cursor_ptr            cur = db->create_write_cursor(db.get_root(0));
   sal::shared_smart_ptr<node> b2  = cur->get_subtree("b2");

   sal::shared_smart_ptr<node> b3a;
   sal::shared_smart_ptr<node> b3b;

   // thread A
   std::thread t1(
       [&]()
       {
          write_cursor_ptr cur1 = db->create_write_cursor(*b2);
          cur1->upsert(to_key("block3a"), to_value("trx1"));
          cur1->sync();
          b3a = cur1->root();
       });

   // thread B
   std::thread t2(
       [&]()
       {
          write_cursor_ptr cur2 = db->create_write_cursor(*b2);
          cur2->upsert(to_key("block3b"), to_value("trx3"));
          auto subtrx = cur2->start_sub_cursor();
          cur2->upsert(to_key("block3b"), to_value("trx4"));
          subtrx->commit();  // sets root of cur2
          b3b = cur2->root();
       });

   t1.join();
   t2.join();

   // lock based prevention of multiple threads modifying the top root
   // and stomping on eachother with last write wins.
   auto trx = db->start_transaction(root(0));  // grabs write lock on top root 0
   trx->upsert(to_key("b3a"), std::move(b3a));
   trx->upsert(to_key("b3b"), std::move(b3b));
   trx->commit(sync_mode::msync_sync);  // syncs the database, releasees write lock on top root 0

   auto expected = db->get_root(0);
   auto desired  = expected;
   do
   {
      write_cursor_ptr cur = db->create_write_cursor(expected);
      cur.upsert(to_key("key"), to_value("value"));
      desired = cur.root();
   } while (not db->compare_exchange_root(expected, desired));
}
