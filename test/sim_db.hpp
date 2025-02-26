#pragma once
#include <array>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace sim
{
   // Forward declarations
   class read_session;
   class write_session;
   class iterator;
   class transaction;

   struct recursive_map
   {
      std::map<std::string, std::variant<std::string, std::shared_ptr<recursive_map>>> data;
   };

   using node_handle = std::shared_ptr<recursive_map>;

   class database
   {
     public:
      static constexpr size_t num_top_roots = 488;

      database()
      {
         for (auto& root : top_roots)
         {
            root = std::make_shared<recursive_map>();  // Initialize each root as an empty map
         }
      }

      node_handle get_root(int root_index = 0)
      {
         if (root_index < 0 || root_index >= num_top_roots)
         {
            throw std::out_of_range("Root index out of range");
         }
         return top_roots[root_index];
      }

      void set_root(node_handle r, int index = 0)
      {
         if (index < 0 || index >= num_top_roots)
         {
            throw std::out_of_range("Root index out of range");
         }
         top_roots[index] = std::move(r);
      }

      read_session  start_read_session();
      write_session start_write_session();

     private:
      std::array<node_handle, num_top_roots> top_roots;
   };

   class read_session
   {
     public:
      read_session(database& db) : _db(db) {}

      // Get value as a string, returns size or -1 if not found
      template <typename Callback>
      int get(const node_handle& r, std::string_view key, Callback&& callback)
      {
         if (!r)
         {
            callback(false, std::string_view());
            return -1;
         }
         return traverse_get(r, key, std::forward<Callback>(callback));
      }

      // Get subtree as a node_handle
      std::optional<node_handle> get_subtree(const node_handle& r, std::string_view key)
      {
         if (!r)
            return std::nullopt;
         return traverse_get_subtree(r, key);
      }

      // Count keys in range [from, to) - including keys in subtrees
      uint32_t count_keys(const node_handle& r,
                          std::string_view   from = {},
                          std::string_view   to   = {})
      {
         if (!r)
            return 0;

         uint32_t count = 0;
         count_keys_recursive(r, from, to, "", count);
         return count;
      }

      node_handle create_root() { return std::make_shared<recursive_map>(); }

      node_handle get_root(int root_index = 0) { return _db.get_root(root_index); }

      node_handle adopt(const node_handle& h)
      {
         return h;  // In sim, adoption is a no-op since all handles are shared_ptrs
      }

      iterator start_iterator(node_handle h);

      // Add this overload for vector pointer
      int get(const node_handle& r, std::string_view key, std::vector<char>* data = nullptr)
      {
         int result = -1;
         get(r, key,
             [&](bool found, std::string_view val)
             {
                if (found)
                {
                   result = val.size();
                   if (data)
                   {
                      data->resize(val.size());
                      std::memcpy(data->data(), val.data(), val.size());
                   }
                }
             });
         return result;
      }

     private:
      database& _db;

      // Helper method to recursively count keys
      void count_keys_recursive(const node_handle& current,
                                std::string_view   from,
                                std::string_view   to,
                                std::string        prefix,
                                uint32_t&          count)
      {
         for (const auto& [k, v] : current->data)
         {
            std::string full_key = prefix + k;

            // Check if this key is in range
            if (full_key >= from && (to.empty() || full_key < to))
            {
               // Count both string values and subtrees as keys
               if (std::holds_alternative<std::string>(v) ||
                   std::holds_alternative<std::shared_ptr<recursive_map>>(v))
               {
                  ++count;
               }
            }

            // We don't recursively count keys in subtrees as they are treated as leaf nodes
         }
      }

      template <typename Callback>
      int traverse_get(const node_handle& current, std::string_view key, Callback&& callback)
      {
         if (key.empty())
         {
            auto it = current->data.find("");
            if (it != current->data.end() && std::holds_alternative<std::string>(it->second))
            {
               std::string_view val = std::get<std::string>(it->second);
               callback(true, val);
               return val.size();
            }
            callback(false, std::string_view());
            return -1;
         }

         std::string_view longest_prefix;
         for (const auto& [k, v] : current->data)
         {
            if (key.substr(0, k.size()) == k && k.size() > longest_prefix.size())
            {
               longest_prefix = k;
            }
         }

         if (longest_prefix.empty())
         {
            callback(false, std::string_view());
            return -1;
         }

         auto it = current->data.find(std::string(longest_prefix));
         if (it == current->data.end())
         {
            callback(false, std::string_view());
            return -1;
         }

         auto& value = it->second;
         if (longest_prefix == key)
         {
            if (std::holds_alternative<std::string>(value))
            {
               std::string_view val = std::get<std::string>(value);
               callback(true, val);
               return val.size();
            }
            callback(false, std::string_view());
            return -1;  // Subtree found, but get treats it as not found
         }

         if (std::holds_alternative<std::shared_ptr<recursive_map>>(value))
         {
            auto subtree = std::get<std::shared_ptr<recursive_map>>(value);
            return traverse_get(subtree, key.substr(longest_prefix.size()),
                                std::forward<Callback>(callback));
         }

         callback(false, std::string_view());
         return -1;
      }

      std::optional<node_handle> traverse_get_subtree(const node_handle& current,
                                                      std::string_view   key)
      {
         if (key.empty())
         {
            auto it = current->data.find("");
            if (it != current->data.end() &&
                std::holds_alternative<std::shared_ptr<recursive_map>>(it->second))
            {
               return std::get<std::shared_ptr<recursive_map>>(it->second);
            }
            return std::nullopt;
         }

         std::string_view longest_prefix;
         for (const auto& [k, v] : current->data)
         {
            if (key.substr(0, k.size()) == k && k.size() > longest_prefix.size())
            {
               longest_prefix = k;
            }
         }

         if (longest_prefix.empty())
            return std::nullopt;

         auto it = current->data.find(std::string(longest_prefix));
         if (it == current->data.end())
            return std::nullopt;

         auto& value = it->second;
         if (longest_prefix == key && std::holds_alternative<std::shared_ptr<recursive_map>>(value))
         {
            return std::get<std::shared_ptr<recursive_map>>(value);
         }

         if (std::holds_alternative<std::shared_ptr<recursive_map>>(value))
         {
            auto subtree = std::get<std::shared_ptr<recursive_map>>(value);
            return traverse_get_subtree(subtree, key.substr(longest_prefix.size()));
         }

         return std::nullopt;
      }
   };

   class write_session : public read_session
   {
     public:
      write_session(database& db) : read_session(db), _db(db) {}

      int upsert(node_handle& r, std::string_view key, std::string_view val)
      {
         auto new_root = upsert_impl(r, key, std::string(val));
         int  result   = r->data.find(std::string(key)) != r->data.end()
                             ? 0
                             : -1;  // Approximate: -1 for insert
         r             = std::move(new_root);
         return result;
      }

      void insert(node_handle& r, std::string_view key, std::string_view val)
      {
         if (get(r, key, [](bool found, std::string_view) { return found; }) > 0)
         {
            throw std::runtime_error("Key already exists");
         }
         r = upsert_impl(r, key, std::string(val));
      }

      int update(node_handle& r, std::string_view key, std::string_view val)
      {
         int size = get(r, key,
                        [&](bool found, std::string_view v)
                        {
                           if (!found)
                              throw std::runtime_error("Key not found");
                           return v.size();
                        });
         r        = upsert_impl(r, key, std::string(val));
         return size;
      }

      void insert(node_handle& r, std::string_view key, node_handle subtree)
      {
         if (get(r, key, [](bool found, std::string_view) { return found; }) > 0)
         {
            throw std::runtime_error("Key already exists");
         }
         r = upsert_impl(r, key, std::move(subtree));
      }

      std::optional<node_handle> update(node_handle& r, std::string_view key, node_handle subtree)
      {
         std::optional<node_handle> old_subtree = get_subtree(r, key);
         if (!old_subtree)
         {
            throw std::runtime_error("Key not found or not a subtree");
         }
         r = upsert_impl(r, key, std::move(subtree));
         return old_subtree;
      }

      std::optional<node_handle> upsert(node_handle& r, std::string_view key, node_handle subtree)
      {
         std::optional<node_handle> old_subtree = get_subtree(r, key);
         r                                      = upsert_impl(r, key, std::move(subtree));
         return old_subtree;
      }

      int remove(node_handle& r, std::string_view key)
      {
         int size =
             get(r, key, [&](bool found, std::string_view v) { return found ? v.size() : -1; });
         if (size >= 0)
         {
            r = remove_impl(r, key);
         }
         return size;
      }

      transaction start_transaction(int top_root_node = 0);

     private:
      database& _db;

      node_handle upsert_impl(const node_handle&                     current,
                              std::string_view                       key,
                              std::variant<std::string, node_handle> value)
      {
         auto new_map = std::make_shared<recursive_map>(*current);
         if (key.empty())
         {
            new_map->data[""] = std::move(value);
            return new_map;
         }

         std::string_view longest_prefix;
         for (const auto& [k, v] : current->data)
         {
            if (key.substr(0, k.size()) == k && k.size() > longest_prefix.size())
            {
               longest_prefix = k;
            }
         }

         if (longest_prefix.empty() || longest_prefix == key)
         {
            new_map->data[std::string(key)] = std::move(value);
            return new_map;
         }

         auto it = current->data.find(std::string(longest_prefix));
         if (it != current->data.end())
         {
            auto& existing = it->second;
            if (std::holds_alternative<std::shared_ptr<recursive_map>>(existing))
            {
               auto subtree = std::get<std::shared_ptr<recursive_map>>(existing);
               new_map->data[std::string(longest_prefix)] =
                   upsert_impl(subtree, key.substr(longest_prefix.size()), std::move(value));
            }
            else
            {
               new_map->data[std::string(key)] = std::move(value);
            }
         }
         else
         {
            new_map->data[std::string(key)] = std::move(value);
         }
         return new_map;
      }

      node_handle remove_impl(const node_handle& current, std::string_view key)
      {
         auto new_map = std::make_shared<recursive_map>(*current);
         if (key.empty())
         {
            new_map->data.erase("");
            return new_map;
         }

         std::string_view longest_prefix;
         for (const auto& [k, v] : current->data)
         {
            if (key.substr(0, k.size()) == k && k.size() > longest_prefix.size())
            {
               longest_prefix = k;
            }
         }

         if (longest_prefix.empty())
            return new_map;

         if (longest_prefix == key)
         {
            new_map->data.erase(std::string(key));
         }
         else
         {
            auto it = current->data.find(std::string(longest_prefix));
            if (it != current->data.end() &&
                std::holds_alternative<std::shared_ptr<recursive_map>>(it->second))
            {
               auto subtree = std::get<std::shared_ptr<recursive_map>>(it->second);
               new_map->data[std::string(longest_prefix)] =
                   remove_impl(subtree, key.substr(longest_prefix.size()));
            }
         }
         return new_map;
      }
   };

   // Iterator implementation
   class iterator
   {
     public:
      iterator(read_session& rs, node_handle root)
          : _rs(rs), _root(root), _current_position(begin_position)
      {
         if (root)
         {
            flatten_tree(root, "");
            if (!_flattened_keys.empty())
            {
               _current_position = 0;
            }
         }
      }

      bool is_start() const { return _current_position == begin_position; }
      bool is_end() const { return _current_position == end_position || _flattened_keys.empty(); }
      bool valid() const { return !is_start() && !is_end(); }

      std::string_view key() const
      {
         if (valid())
         {
            return _flattened_keys[_current_position];
         }
         return {};
      }

      bool next()
      {
         if (is_end())
            return false;

         if (is_start())
            _current_position = 0;
         else
            _current_position++;

         if (_current_position >= _flattened_keys.size())
            _current_position = end_position;

         return !is_end();
      }

      bool prev()
      {
         if (is_start())
            return false;

         if (is_end())
            _current_position = _flattened_keys.size() - 1;
         else if (_current_position > 0)
            _current_position--;
         else
            _current_position = begin_position;

         return !is_start();
      }

      bool begin()
      {
         if (_flattened_keys.empty())
         {
            _current_position = end_position;
            return false;
         }
         _current_position = 0;
         return true;
      }

      bool end()
      {
         _current_position = end_position;
         return true;
      }

      bool start()
      {
         _current_position = begin_position;
         return true;
      }

      bool find(std::string_view key)
      {
         for (size_t i = 0; i < _flattened_keys.size(); i++)
         {
            if (_flattened_keys[i] == key)
            {
               _current_position = i;
               return true;
            }
         }
         _current_position = end_position;
         return false;
      }

      template <typename Buffer>
      int value(Buffer&& buffer) const
      {
         if (!valid())
            return -1;

         std::string_view val;
         _rs.get(_root, key(), [&](bool found, std::string_view v) { val = v; });

         if (val.empty())
            return -1;

         buffer.resize(val.size());
         std::memcpy(buffer.data(), val.data(), val.size());
         return val.size();
      }

      std::optional<iterator> get_subtree(std::string_view key) const
      {
         auto subtree = _rs.get_subtree(_root, key);
         if (subtree)
         {
            return iterator(_rs, subtree.value());
         }
         return std::nullopt;
      }

      iterator subtree_iterator() const
      {
         if (!valid())
            throw std::runtime_error("Iterator not valid");

         auto subtree = _rs.get_subtree(_root, key());
         if (!subtree)
            throw std::runtime_error("Current position is not a subtree");

         return iterator(_rs, subtree.value());
      }

      node_handle root_handle() const { return _root; }

     private:
      static constexpr size_t begin_position = std::numeric_limits<size_t>::max();
      static constexpr size_t end_position   = std::numeric_limits<size_t>::max() - 1;

      read_session&            _rs;
      node_handle              _root;
      size_t                   _current_position;
      std::vector<std::string> _flattened_keys;

      void flatten_tree(const node_handle& node, const std::string& prefix)
      {
         for (const auto& [k, v] : node->data)
         {
            std::string full_key = prefix + k;
            // Both string values and subtrees are treated as keys
            _flattened_keys.push_back(full_key);

            // We don't recursively flatten subtrees as they are treated as leaf nodes
         }
      }
   };

   // Transaction implementation
   class transaction : public iterator
   {
     public:
      transaction(write_session&                   ws,
                  node_handle                      root,
                  std::function<void(node_handle)> commit_callback)
          : iterator(ws, root), _ws(ws), _commit_callback(commit_callback)
      {
      }

      ~transaction()
      {
         if (_commit_callback)
         {
            abort();
         }
      }

      void commit()
      {
         if (_commit_callback)
         {
            _commit_callback(root_handle());
            _commit_callback = nullptr;
         }
      }

      node_handle abort()
      {
         if (_commit_callback)
         {
            _commit_callback = nullptr;
         }
         return root_handle();
      }

      template <typename T>
      auto upsert(std::string_view key, T&& val)
      {
         node_handle r      = root_handle();
         auto        result = _ws.upsert(r, key, std::forward<T>(val));
         return result;
      }

      template <typename T>
      void insert(std::string_view key, T&& val)
      {
         node_handle r = root_handle();
         _ws.insert(r, key, std::forward<T>(val));
      }

      int remove(std::string_view key)
      {
         node_handle r = root_handle();
         return _ws.remove(r, key);
      }

     private:
      write_session&                   _ws;
      std::function<void(node_handle)> _commit_callback;
   };

   // Implementation of forward-declared methods
   inline read_session database::start_read_session()
   {
      return read_session(*this);
   }

   inline write_session database::start_write_session()
   {
      return write_session(*this);
   }

   inline iterator read_session::start_iterator(node_handle h)
   {
      return iterator(*this, h);
   }

   inline transaction write_session::start_transaction(int top_root_node)
   {
      auto root = get_root(top_root_node);
      return transaction(*this, root,
                         [this, top_root_node](node_handle new_root)
                         { _db.set_root(new_root, top_root_node); });
   }

}  // namespace sim