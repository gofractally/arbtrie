#include <chrono>
#include <format>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <arbtrie/database.hpp>

namespace po = boost::program_options;
using namespace arbtrie;
std::string get_current_time_and_date()
{
   auto const time = std::chrono::system_clock::now();
   return std::format("{:%Y-%m-%d %X}", time);
}

struct benchmark_config
{
   uint32_t rounds;
   uint32_t items      = 1000000;
   uint32_t batch_size = 100;
   uint32_t value_size = 8;
};

struct result
{
   std::vector<double> items_per_second;
};

int64_t rand_from_seq(uint64_t seq)
{
   return XXH3_64bits((char*)&seq, sizeof(seq));
}

void to_key(uint64_t val, std::vector<char>& v)
{
   v.resize(sizeof(val));
   memcpy(v.data(), &val, sizeof(val));
}
void to_key(std::string val, std::vector<char>& v)
{
   v.resize(val.size());
   memcpy(v.data(), val.data(), val.size());
}

uint64_t bswap(uint64_t x)
{
   x = (x & 0x00000000FFFFFFFF) << 32 | (x & 0xFFFFFFFF00000000) >> 32;
   x = (x & 0x0000FFFF0000FFFF) << 16 | (x & 0xFFFF0000FFFF0000) >> 16;
   x = (x & 0x00FF00FF00FF00FF) << 8 | (x & 0xFF00FF00FF00FF00) >> 8;
   return x;
}
static bool addc = false;
auto        format_comma(auto arg)
{
   if (addc)
      return add_comma(arg);
   return std::to_string(arg);
}
static char sepearator = '\t';

int64_t get_test(benchmark_config   cfg,
                 arbtrie::database& db,
                 auto&              ws,
                 std::string        name,
                 auto               make_key)
{
   std::cout << "---------------------  " << name
             << "  "
                "--------------------------------------------------\n";
   std::cout << get_current_time_and_date() << "\n";
   if constexpr (debug_memory)
      std::cout << "debug memory enabled\n";
   if constexpr (update_checksum_on_modify)
      std::cout << "update checksum on modify\n";
   else if constexpr (update_checksum_on_compact)
      std::cout << "update checksum on compact\n";
   std::cout << "rounds: " << cfg.rounds << "  items: " << format_comma(cfg.items)
             << " batch: " << format_comma(cfg.batch_size) << "\n";
   std::cout << "-----------------------------------------------------------------------\n";

   std::vector<char> key;
   auto              root  = ws->get_root();
   auto              start = std::chrono::steady_clock::now();
   for (uint64_t i = 0; i < cfg.items; ++i)
   {
      make_key(i, key);
      key_view kstr(key.data(), key.size());
      ws->get(root, kstr,
              [=](bool found, const value_type& r)
              {
                 if (not found)
                 {
                    ARBTRIE_WARN("seq: ", i);
                    abort();
                 }
              });
   }
   auto   end    = std::chrono::steady_clock::now();
   auto   delta  = end - start;
   double result = cfg.items / (std::chrono::duration<double, std::milli>(delta).count() / 1000);
   std::cout << format_comma(uint64_t(result)) << " get/sec\n";
   return result;
}

template <upsert_mode mode = upsert_mode::insert>
std::vector<int> insert_test(benchmark_config   cfg,
                             arbtrie::database& db,
                             auto&&             ws,
                             std::string        name,
                             auto               make_key)
{
   std::cout << "---------------------  " << name
             << "  "
                "--------------------------------------------------\n";
   std::cout << get_current_time_and_date() << "\n";
   if constexpr (debug_memory)
      std::cout << "debug memory enabled\n";
   if constexpr (update_checksum_on_modify)
      std::cout << "update checksum on modify\n";
   else if constexpr (update_checksum_on_compact)
      std::cout << "update checksum on compact\n";
   std::cout << "rounds: " << cfg.rounds << "  items: " << format_comma(cfg.items)
             << " batch: " << format_comma(cfg.batch_size) << "\n";
   std::cout << "-----------------------------------------------------------------------\n";
   std::vector<int> result;
   result.reserve(cfg.rounds);

   auto tx = ws->start_write_transaction(0);
   if constexpr (not mode.is_update() or mode.is_insert())
      tx->set_root(ws->create_root());

   uint64_t          seq = 0;
   std::vector<char> key;
   std::vector<char> value;
   value.resize(cfg.value_size);
   value_view vv(value.data(), value.size());

   for (int r = 0; r < cfg.rounds; ++r)
   {
      auto start    = std::chrono::steady_clock::now();
      int  inserted = 0;
      while (inserted < cfg.items)
      {
         for (int i = 0; i < cfg.batch_size; ++i)
         {
            make_key(seq++, key);
            key_view kstr(key.data(), key.size());
            if constexpr (mode.is_upsert())
               tx->upsert(kstr, vv);
            else if constexpr (mode.is_insert())
               tx->insert(kstr, vv);
            else
               tx->update(kstr, vv);
            ++inserted;
         }
         tx->commit_and_continue();
      }

      auto end   = std::chrono::steady_clock::now();
      auto delta = end - start;
      result.push_back(inserted /
                       (std::chrono::duration<double, std::milli>(delta).count() / 1000));
      std::cout << std::setw(4) << std::left << r << " " << std::setw(10) << std::right
                << format_comma(seq) << sepearator << "  " << std::setw(10) << std::right
                << format_comma(result.back()) << sepearator << "  items/sec\n";
   }
   return result;
}

void print_stat(auto& ws)
{
   auto start = std::chrono::steady_clock::now();
   auto stats = ws->get_node_stats(ws->get_root());
   auto end   = std::chrono::steady_clock::now();

   std::cout << stats << "\n";
   std::cout << std::fixed << std::setprecision(3)
             << std::chrono::duration<double, std::milli>(end - start).count() / 1000 << "  sec\n";
}

int main(int argc, char** argv)
{
   arbtrie::thread_name("main");
   uint32_t    rounds;
   uint32_t    batch;
   uint32_t    items;
   uint32_t    value_size;
   uint32_t    range_n = 0;
   uint32_t    range_e = 1;
   bool        reset   = false;
   bool        stat    = false;
   std::string db_dir  = "./arbtriedb";
   std::string bench   = "all";

   po::options_description desc("Allowed options");
   auto                    opt = desc.add_options();
   opt("help,h", "print this message");
   opt("round,r", po::value<uint32_t>(&rounds)->default_value(3), "number of rounds");
   opt("batch,b", po::value<uint32_t>(&batch)->default_value(512), "batch size");
   opt("items,i", po::value<uint32_t>(&items)->default_value(1000000), "number of items");
   opt("range-n", po::value<uint32_t>(&range_n)->default_value(0), "range start");
   opt("range-e", po::value<uint32_t>(&range_e)->default_value(1), "range end");
   opt("value-size,s", po::value<uint32_t>(&value_size)->default_value(8), "value size");
   opt("db-dir,d", po::value<std::string>(&db_dir)->default_value("./arbtriedb"), "database dir");
   opt("bench", po::value<std::string>(&bench)->default_value("all"), "benchmark to run");
   opt("reset", po::bool_switch(&reset), "reset database");
   opt("stat", po::bool_switch(&stat)->default_value(false), "print database stats");

   po::variables_map vm;
   po::store(po::parse_command_line(argc, argv, desc), vm);
   po::notify(vm);

   if (vm.count("help"))
   {
      std::cout << desc << "\n";
      return 1;
   }
   if (vm.count("reset"))
   {
      ARBTRIE_WARN("resetting database");
      std::filesystem::remove_all(db_dir);
      arbtrie::database::create(db_dir);
   }
   if (stat)
   {
      database db(db_dir);
      auto     ws = db.start_write_session();
      print_stat(ws);
      std::cout << "DEBUG: Finished stat." << std::endl;
      return 0;
   }

   database db(db_dir);
   auto     ws = db.start_write_session();
   std::cout << "DEBUG: Database opened, session started." << std::endl;

   benchmark_config cfg = {rounds, items, batch, value_size};

   std::cout << "DEBUG: Calling print_stat..." << std::endl;
   print_stat(ws);
   std::cout << "DEBUG: Finished print_stat." << std::endl;
   std::cout << "DEBUG: Calling insert_test (upsert)..." << std::endl;
   insert_test<upsert_mode::upsert>(cfg, db, ws, "big endian seq upsert",
                                    [](uint64_t seq, auto& v) { to_key(bswap(seq), v); });
   print_stat(ws);

   get_test(cfg, db, ws, "big endian seq get",
            [](uint64_t seq, auto& v) { to_key(bswap(seq), v); });
   get_test(cfg, db, ws, "big endian rand get",
            [cfg](uint64_t seq, auto& v)
            {
               uint64_t k = uint64_t(rand_from_seq(seq)) % (cfg.items * cfg.rounds);
               to_key(bswap(k), v);
            });

   insert_test<upsert_mode::update>(cfg, db, ws, "big endian seq update",
                                    [](uint64_t seq, auto& v) { to_key(bswap(seq), v); });
   print_stat(ws);
   insert_test<upsert_mode::insert>(cfg, db, ws, "big endian seq insert",
                                    [](uint64_t seq, auto& v) { to_key(bswap(seq), v); });
   print_stat(ws);

   insert_test<upsert_mode::insert>(cfg, db, ws, "string number rand insert",
                                    [](uint64_t seq, auto& v)
                                    { to_key(std::to_string(rand_from_seq(seq)), v); });
   print_stat(ws);
   get_test(cfg, db, ws, "string number rand get",
            [](uint64_t seq, auto& v) { to_key(std::to_string(rand_from_seq(seq)), v); });

   insert_test<upsert_mode::update>(cfg, db, ws, "string number rand update",
                                    [](uint64_t seq, auto& v)
                                    { to_key(std::to_string(rand_from_seq(seq)), v); });
   print_stat(ws);
   insert_test<upsert_mode::upsert>(cfg, db, ws, "string number rand upsert",
                                    [](uint64_t seq, auto& v)
                                    { to_key(std::to_string(rand_from_seq(seq)), v); });
   print_stat(ws);

   insert_test<upsert_mode::insert>(cfg, db, ws, "string number seq insert",
                                    [](uint64_t seq, auto& v) { to_key(std::to_string(seq), v); });
   print_stat(ws);
   insert_test<upsert_mode::update>(cfg, db, ws, "string number seq update",
                                    [](uint64_t seq, auto& v) { to_key(std::to_string(seq), v); });
   print_stat(ws);
   insert_test<upsert_mode::upsert>(cfg, db, ws, "string number seq upsert",
                                    [](uint64_t seq, auto& v) { to_key(std::to_string(seq), v); });
   print_stat(ws);
   insert_test<upsert_mode::insert>(cfg, db, ws, "dense random insert",
                                    [](uint64_t seq, auto& v) { to_key(rand_from_seq(seq), v); });
   print_stat(ws);
   insert_test<upsert_mode::update>(cfg, db, ws, "dense random update",
                                    [](uint64_t seq, auto& v) { to_key(rand_from_seq(seq), v); });
   print_stat(ws);
   insert_test<upsert_mode::upsert>(cfg, db, ws, "dense random upsert",
                                    [](uint64_t seq, auto& v) { to_key(rand_from_seq(seq), v); });
   print_stat(ws);

   insert_test<upsert_mode::insert>(cfg, db, ws, "little endian seq insert",
                                    [](uint64_t seq, auto& v) { to_key(seq, v); });
   print_stat(ws);
   insert_test<upsert_mode::update>(cfg, db, ws, "little endian seq update",
                                    [](uint64_t seq, auto& v) { to_key(seq, v); });
   print_stat(ws);
   insert_test<upsert_mode::upsert>(cfg, db, ws, "little endian seq upsert",
                                    [](uint64_t seq, auto& v) { to_key(seq, v); });
   print_stat(ws);

   return 0;
}
