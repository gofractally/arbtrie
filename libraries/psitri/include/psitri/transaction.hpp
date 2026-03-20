#pragma once
#include <functional>
#include <psitri/util.hpp>
#include <psitri/value_type.hpp>
#include <sal/allocator_session.hpp>
#include <sal/smart_ptr.hpp>

namespace psitri
{
   class transaction
   {
     public:
      transaction(sal::smart_ptr<>                      root,
                  std::function<void(sal::smart_ptr<>)> commit_func,
                  std::function<void()>                 rollback_func)
          : _root(std::move(root)), _commit_func(commit_func), _rollback_func(rollback_func)
      {
      }
      transaction(const transaction&)                = delete;
      transaction& operator=(const transaction&)     = delete;
      transaction(transaction&&) noexcept            = default;
      transaction& operator=(transaction&&) noexcept = default;

      ~transaction() { abort(); }

      void commit() noexcept
      {
         _commit_func(std::move(_root));
         _rollback_func = std::function<void()>();
      }

      void abort() noexcept
      {
         if (_rollback_func)
            _rollback_func();
      }
      void set_root(sal::smart_ptr<> root) { _root = std::move(root); }

      [[nodiscard]] transaction sub_transaction() noexcept
      {
         return transaction(_root, [this](sal::smart_ptr<> ptr) { set_root(std::move(ptr)); }, {});
      }

      bool insert(key_view key, value_view value);

     private:
      sal::smart_ptr<>                      _root;
      std::function<void(sal::smart_ptr<>)> _commit_func;
      std::function<void()>                 _rollback_func;
   };
}  // namespace psitri