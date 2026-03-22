#pragma once
#include <functional>
#include <optional>
#include <psitri/write_cursor.hpp>

namespace psitri
{
   class write_session;

   class transaction
   {
     public:
      transaction(sal::allocator_session_ptr                            session,
                  sal::smart_ptr<sal::alloc_header>                     root,
                  std::function<void(sal::smart_ptr<sal::alloc_header>)> commit_func,
                  std::function<void()>                                  rollback_func)
          : _commit_func(std::move(commit_func)), _rollback_func(std::move(rollback_func))
      {
         if (root)
            _cursor.emplace(std::move(root));
         else
            _cursor.emplace(std::move(session));
      }

      transaction(const transaction&)            = delete;
      transaction& operator=(const transaction&) = delete;
      transaction(transaction&&) noexcept        = default;
      transaction& operator=(transaction&&)      = delete;

      ~transaction() { abort(); }

      // -- Mutations --

      bool insert(key_view key, value_view value) { return _cursor->insert(key, value); }
      bool update(key_view key, value_view value) { return _cursor->update(key, value); }
      void upsert(key_view key, value_view value) { _cursor->upsert(key, value); }
      int  remove(key_view key) { return _cursor->remove(key); }

      // -- Read access --

      cursor read_cursor() const { return _cursor->read_cursor(); }

      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const
      {
         return _cursor->get<T>(key);
      }

      int32_t get(key_view key, Buffer auto* buffer) const { return _cursor->get(key, buffer); }

      // -- Transaction control --

      void commit() noexcept
      {
         if (_commit_func)
         {
            _commit_func(std::move(_cursor->root()));
            _commit_func   = nullptr;
            _rollback_func = nullptr;
         }
      }

      void abort() noexcept
      {
         if (_rollback_func)
         {
            _rollback_func();
            _rollback_func = nullptr;
            _commit_func   = nullptr;
         }
      }

      /// Create a sub-transaction that commits back to this transaction's cursor
      [[nodiscard]] transaction sub_transaction() noexcept
      {
         auto root = _cursor->root();
         return transaction(
             root.session(), root,
             [this](sal::smart_ptr<sal::alloc_header> ptr) { _cursor.emplace(std::move(ptr)); },
             {});
      }

      // -- Diagnostics --

      tree_context::stats get_stats() { return _cursor->get_stats(); }

     private:
      std::optional<write_cursor>                              _cursor;
      std::function<void(sal::smart_ptr<sal::alloc_header>)> _commit_func;
      std::function<void()>                                    _rollback_func;
   };
}  // namespace psitri
