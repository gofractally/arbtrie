#include <catch2/catch_all.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/node.hpp>
#include <sal/allocator_session_impl.hpp>

TEST_CASE("trie_tests", "[psitri][trie]")
{
   using namespace psitri;
   std::filesystem::remove_all("./test_db");

   auto alloc = std::make_shared<sal::allocator>("./test_db");
   auto ses   = alloc->get_session();

   {
      auto                      trx = ses->start_transaction(sal::root_object_number(0));
      sal::smart_ptr<leaf_node> leaf =
          ses->smart_alloc<leaf_node>(to_key("hello"), to_value("world"));
      trx->set_root(std::move(leaf));
      trx->commit();
   }
   {
      sal::transaction_ptr      trx  = ses->start_transaction(sal::root_object_number(0));
      sal::smart_ref<leaf_node> leaf = trx->root().as<leaf_node>();
      //  trx->set_root(std::move(leaf));
      //  trx->commit();
   }
}