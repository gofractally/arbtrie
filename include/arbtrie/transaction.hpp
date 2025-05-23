#pragma once
#include <arbtrie/iterator.hpp>
#include <functional>
#include <memory>

namespace arbtrie
{

   using read_transaction         = iterator<iterator_caching_mode::noncaching>;
   using caching_read_transaction = iterator<iterator_caching_mode::caching>;
   /**
    * A write transaction is a mutable iterator that knows how
    * to commit and abort the node_handle that it manages. 
    * 
    * Transactions hold a shared_ptr to their parent session, ensuring
    * that the session remains valid for the lifetime of the transaction.
    * This means you don't need to worry about keeping sessions alive -
    * they will be automatically managed through shared ownership.
    *
    * Example of usage:
    * ```
    * // Create a session
    * auto session = db->start_write_session_shared();
    * 
    * // Create a transaction - session will be kept alive automatically
    * auto tx = session->start_transaction();
    * 
    * // Even if you reset or destroy the session variable,
    * // the transaction can still be used safely
    * session.reset();
    * 
    * // Transaction still works because it holds a reference to the session
    * tx.insert("key", "value");
    * tx.commit();
    * ```
    */
   class write_transaction : public mutable_iterator<caching>
   {
     private:
      friend class write_session;
      struct private_token
      {
      };

      write_transaction(std::shared_ptr<write_session>                ws,
                        node_handle                                   r,
                        std::function<node_handle(node_handle, bool)> commit_callback,
                        std::function<void()>                         abort_callback = {})
          : mutable_iterator<caching>(*ws, std::move(r)), _ws(std::move(ws))
      {
         assert(commit_callback);
         _abort_callback  = abort_callback;
         _commit_callback = commit_callback;
      }

     public:
      /** this method is designed to only be called by the write_session */
      write_transaction(private_token,
                        std::shared_ptr<write_session>                ws,
                        node_handle                                   r,
                        std::function<node_handle(node_handle, bool)> commit_callback,
                        std::function<void()>                         abort_callback = {})
          : mutable_iterator<caching>(*ws, std::move(r)), _ws(std::move(ws))
      {
         assert(commit_callback);
         _abort_callback  = abort_callback;
         _commit_callback = commit_callback;
      }

      using ptr = std::shared_ptr<write_transaction>;
      ~write_transaction()
      {
         if (_abort_callback)
            _abort_callback();
      }

      /**
       * Aborts the transaction and returns the node_handle
       * containing the transaction's state, which will be
       * deleted if not utilized by the caller. Can only
       * be called once.
       * 
       * @return the node_handle containing the transaction's state
       */
      node_handle abort()
      {
         if (_abort_callback)
         {
            _abort_callback();
            _abort_callback = std::function<void()>();
         }
         return std::move(_root);
      }

      /** 
       * Commits the changes back to the source of the
       * transaction, but can only be called once.
       */
      void commit()
      {
         assert(_commit_callback);
         _commit_callback(std::move(_root), false);
         _commit_callback = {};
         _abort_callback  = {};
      }

      /**
       * Commits the changes back to the source of the
       * transaction, but can be called multiple times 
       * to commit in stages. This allows for this transaction
       * object to be reused considering it contains a 
       * 3kb buffer. 
       * 
       * @note after commiting the source may block while
       * until other writers have a chance to commit. Upon
       * returning the state will contain anything that the
       * source of this transaction commited. In other words,
       * this will release and reacquire the lock on the source
       * of this transaction.
       */
      void commit_and_continue()
      {
         assert(_commit_callback);
         _root = _commit_callback(std::move(_root), true);
      }

      using mutable_iterator<caching>::get_root;
      using mutable_iterator<caching>::set_root;
      /**
       * Starts a new sub-transaction based upon the state of this
       * transaction that commits to this transaction when it is
       * done.  Any modifications to this transaction after the
       * creation of the sub-transaction will be lost if the
       * sub-transaction is committed. 
       */
      write_transaction::ptr start_transaction()
      {
         ARBTRIE_INFO("start_transaction", get_root().address().to_int(), " this: ", this);
         auto self = shared_from_this();
         return std::make_shared<write_transaction>(
             private_token{}, _ws, get_root(),
             [self](node_handle commit, bool resume)
             {
                ARBTRIE_INFO("start_transaction callback", commit.address().to_int(),
                             " this: ", self.get());
                self->set_root(resume ? commit : std::move(commit));
                return commit;
             });
      }

     private:
      std::shared_ptr<write_session>                _ws;
      std::function<void()>                         _abort_callback;
      std::function<node_handle(node_handle, bool)> _commit_callback;
   };
}  // namespace arbtrie
