#include <mdbx.h>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/read_session_impl.hpp>

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace
{
int hex_nibble(char c)
{
   if (c >= '0' && c <= '9')
      return c - '0';
   c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
   if (c >= 'a' && c <= 'f')
      return 10 + c - 'a';
   return -1;
}

std::vector<std::uint8_t> from_hex(std::string hex)
{
   if (hex.starts_with("0x") || hex.starts_with("0X"))
      hex.erase(0, 2);
   if (hex.size() % 2)
      throw std::runtime_error("odd hex length");

   std::vector<std::uint8_t> out(hex.size() / 2);
   for (size_t i = 0; i < out.size(); ++i)
   {
      int hi = hex_nibble(hex[2 * i]);
      int lo = hex_nibble(hex[2 * i + 1]);
      if (hi < 0 || lo < 0)
         throw std::runtime_error("invalid hex digit");
      out[i] = static_cast<std::uint8_t>((hi << 4) | lo);
   }
   return out;
}

void print_hex_prefix(const MDBX_val& value, size_t max_bytes = 32)
{
   static constexpr char chars[] = "0123456789abcdef";
   auto* data = static_cast<const std::uint8_t*>(value.iov_base);
   std::cout << "prefix=0x";
   const size_t n = std::min(max_bytes, value.iov_len);
   for (size_t i = 0; i < n; ++i)
   {
      std::cout << chars[data[i] >> 4] << chars[data[i] & 0x0f];
   }
   if (value.iov_len > max_bytes)
      std::cout << "...";
   std::cout << "\n";
}

bool starts_with(std::string_view value, std::string_view prefix)
{
   return value.size() >= prefix.size() &&
          value.compare(0, prefix.size(), prefix) == 0;
}
}

int main(int argc, char** argv)
{
   if (argc != 4)
   {
      std::cerr << "usage: mdbx_probe <chaindata> <table> <hex-key>\n";
      return 2;
   }

   try
   {
      const std::string path  = argv[1];
      const std::string table = argv[2];
      auto              key   = from_hex(argv[3]);

      if (table == "scanroots")
      {
         auto db = psitri::database::open(path, psitri::open_mode::open_existing,
                                          {}, psitri::recovery_mode::deferred_cleanup);
         auto rs = db->start_read_session();
         for (std::uint32_t root_index = 0; root_index < psitri::num_top_roots; ++root_index)
         {
            auto root = rs->get_root(root_index);
            if (!root)
               continue;

            std::string value;
            const bool found = root.get(
               psitri::key_view(reinterpret_cast<const char*>(key.data()), key.size()),
               [&](psitri::value_view vv) { value.assign(vv.data(), vv.size()); });
            if (!found)
               continue;

            MDBX_val v{value.data(), value.size()};
            std::cout << "root=" << root_index << "\n";
            std::cout << "root_address=" << root.address() << "\n";
            std::cout << "root_ver=" << root.ver() << "\n";
            std::cout << "value_size=" << value.size() << "\n";
            print_hex_prefix(v);
         }
         return 0;
      }

      if (table == "scanprefix")
      {
         auto db = psitri::database::open(path, psitri::open_mode::open_existing,
                                          {}, psitri::recovery_mode::deferred_cleanup);
         auto rs = db->start_read_session();
         const std::string prefix(reinterpret_cast<const char*>(key.data()), key.size());
         for (std::uint32_t root_index = 0; root_index < psitri::num_top_roots; ++root_index)
         {
            auto root = rs->get_root(root_index);
            if (!root)
               continue;

            auto c = root.cursor();
            if (!c.lower_bound(psitri::key_view(prefix.data(), prefix.size())))
               continue;

            auto current_key = c.key();
            if (!starts_with(std::string_view(current_key.data(), current_key.size()), prefix))
               continue;

            std::string value;
            if (!c.is_subtree())
               c.get_value([&](psitri::value_view vv) { value.assign(vv.data(), vv.size()); });

            MDBX_val k{const_cast<char*>(current_key.data()), current_key.size()};
            MDBX_val v{value.data(), value.size()};
            std::cout << "root=" << root_index << "\n";
            std::cout << "root_address=" << root.address() << "\n";
            std::cout << "key_size=" << current_key.size() << "\n";
            print_hex_prefix(k);
            std::cout << "value_size=" << value.size() << "\n";
            print_hex_prefix(v);
         }
         return 0;
      }

      if (table.starts_with("root:"))
      {
         const auto root_index = static_cast<std::uint32_t>(
            std::stoul(table.substr(std::string("root:").size())));
         auto db = psitri::database::open(path, psitri::open_mode::open_existing,
                                          {}, psitri::recovery_mode::deferred_cleanup);
         auto rs = db->start_read_session();
         auto root = rs->get_root(root_index);
         std::cout << "root=" << root_index << "\n";
         std::cout << "root_address=" << root.address() << "\n";
         std::cout << "root_ver=" << root.ver() << "\n";
         std::cout << "root_tree_root=" << *root.get_tree_id().root << "\n";
         std::cout << "root_tree_ver=" << *root.get_tree_id().ver << "\n";
         std::cout << std::flush;
         if (!root)
         {
            std::cout << "found=0\n";
            return 0;
         }
         std::string value;
         const bool found = root.get(
            psitri::key_view(reinterpret_cast<const char*>(key.data()), key.size()),
            [&](psitri::value_view vv) { value.assign(vv.data(), vv.size()); });
         std::cout << "found=" << (found ? 1 : 0) << "\n";
         if (found)
         {
            MDBX_val v{value.data(), value.size()};
            std::cout << "value_size=" << value.size() << "\n";
            print_hex_prefix(v);
         }
         return 0;
      }

      MDBX_env* env = nullptr;
      int rc = mdbx_env_create(&env);
      if (rc != MDBX_SUCCESS)
      {
         std::cout << "env_create rc=" << rc << " " << mdbx_strerror(rc) << "\n";
         return 1;
      }
      mdbx_env_set_maxdbs(env, 128);
      rc = mdbx_env_open(env, path.c_str(), MDBX_RDONLY, 0644);
      if (rc != MDBX_SUCCESS)
      {
         std::cout << "env_open rc=" << rc << " " << mdbx_strerror(rc) << "\n";
         mdbx_env_close(env);
         return 1;
      }

      MDBX_txn* txn = nullptr;
      rc = mdbx_txn_begin(env, nullptr, static_cast<MDBX_txn_flags_t>(MDBX_RDONLY), &txn);
      if (rc != MDBX_SUCCESS)
      {
         std::cout << "txn_begin rc=" << rc << " " << mdbx_strerror(rc) << "\n";
         mdbx_env_close(env);
         return 1;
      }

      MDBX_dbi dbi = 0;
      rc = mdbx_dbi_open(txn, table.c_str(), MDBX_DB_DEFAULTS, &dbi);
      if (rc != MDBX_SUCCESS)
      {
         std::cout << "dbi_open rc=" << rc << " " << mdbx_strerror(rc) << "\n";
         mdbx_txn_abort(txn);
         mdbx_env_close(env);
         return 1;
      }

      MDBX_val k{key.data(), key.size()};
      MDBX_val v{};
      rc = mdbx_get(txn, dbi, &k, &v);
      std::cout << "get rc=" << rc << " " << mdbx_strerror(rc) << "\n";
      if (rc == MDBX_SUCCESS)
      {
         std::cout << "value_size=" << v.iov_len << "\n";
         print_hex_prefix(v);
      }

      mdbx_txn_abort(txn);
      mdbx_env_close(env);
      return rc == MDBX_SUCCESS || rc == MDBX_NOTFOUND ? 0 : 1;
   }
   catch (const std::exception& e)
   {
      std::cerr << "error: " << e.what() << "\n";
      return 1;
   }
}
