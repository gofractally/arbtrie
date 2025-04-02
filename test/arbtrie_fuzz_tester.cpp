#include <arbtrie/database.hpp>
#include <arbtrie/iterator.hpp>
#include <arbtrie/transaction.hpp>
#include <catch2/catch_all.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace arbtrie;
using namespace std::literals::string_literals;

namespace
{

   // Constants for the fuzz tester
   constexpr size_t MAX_TEST_OPERATIONS      = 10000;  // Maximum number of operations per test run
   constexpr size_t DEFAULT_KEY_LENGTH       = 16;     // Default length of random keys
   constexpr size_t DEFAULT_VALUE_LENGTH     = 64;     // Default length of random values
   constexpr size_t MAX_KEY_LENGTH           = 128;    // Maximum length of random keys
   constexpr size_t MAX_VALUE_LENGTH         = 1024;   // Maximum length of random values
   constexpr size_t DEFAULT_NUM_TRANSACTIONS = 10;     // Default number of transactions

   // Operation types for the fuzzer
   enum class Operation
   {
      INSERT,
      GET,
      UPDATE,
      REMOVE,
      ITERATE_FIRST,
      ITERATE_LAST,
      ITERATE_NEXT,
      ITERATE_PREV,
      ITERATE_LOWER_BOUND,
      ITERATE_UPPER_BOUND,
      ITERATE_BEGIN,
      ABORT_TRANSACTION,
      COUNT_KEYS,
      OPERATION_COUNT
   };

   // Structure to hold the test environment
   struct FuzzTestEnvironment
   {
      std::filesystem::path               db_path;
      database*                           db = nullptr;
      std::shared_ptr<write_session>      ws;
      std::vector<write_transaction::ptr> transactions;

      // Reference map to verify results
      std::unordered_map<std::string, std::string> reference_map;

      // Track changes per transaction to update reference map only on commit
      std::vector<std::unordered_map<std::string, std::optional<std::string>>> pending_changes;

      // Track current transaction being used
      size_t current_transaction_idx = 0;

      // Random number generator
      std::mt19937_64 rng;

      FuzzTestEnvironment(uint64_t seed) : rng(seed)
      {
         // Create a unique temporary directory for the test
         auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
         db_path        = std::filesystem::temp_directory_path() /
                   ("arbtrie_fuzz_" + std::to_string(timestamp) + "_" + std::to_string(seed));

         std::cout << "Creating database at " << db_path << std::endl;
         std::filesystem::create_directories(db_path);

         runtime_config cfg;

         // Create and open the database
         database::create(db_path, cfg);
         db = new database(db_path, cfg);
         ws = db->start_write_session();

         // Initialize the first transaction
         auto tx = ws->start_write_transaction();
         transactions.push_back(std::move(tx));
         pending_changes.push_back({});
      }

      ~FuzzTestEnvironment()
      {
         // Clean up resources
         transactions.clear();
         ws.reset();  // Use reset instead of delete for shared_ptr
         delete db;

         // Clean up the temporary directory
         std::filesystem::remove_all(db_path);
      }

      // Get a reference to the current transaction
      write_transaction::ptr current_transaction() { return transactions[current_transaction_idx]; }

      // Get a reference to the current transaction's pending changes
      auto& current_pending_changes() { return pending_changes[current_transaction_idx]; }

      // Record a pending insert
      void record_pending_insert(const std::string& key, const std::string& value)
      {
         current_pending_changes()[key] = value;
      }

      // Record a pending remove
      void record_pending_remove(const std::string& key)
      {
         current_pending_changes()[key] = std::nullopt;
      }

      // Apply pending changes to the reference map
      void apply_pending_changes(size_t tx_idx)
      {
         for (const auto& [key, value_opt] : pending_changes[tx_idx])
         {
            if (value_opt)
            {
               // Insert or update
               reference_map[key] = *value_opt;
            }
            else
            {
               // Remove
               reference_map.erase(key);
            }
         }
         // Clear pending changes after applying
         pending_changes[tx_idx].clear();
      }

      // Discard pending changes (for aborted transactions)
      void discard_pending_changes(size_t tx_idx) { pending_changes[tx_idx].clear(); }

      // Generate a random string of specified length
      std::string random_string(size_t length)
      {
         static const char charset[] =
             "abcdefghijklmnopqrstuvwxyz"
             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
             "0123456789";

         std::uniform_int_distribution<size_t> dist(0, sizeof(charset) - 2);
         std::string                           result(length, 0);

         for (size_t i = 0; i < length; ++i)
         {
            result[i] = charset[dist(rng)];
         }

         return result;
      }

      // Generate a random key
      std::string random_key()
      {
         std::uniform_int_distribution<size_t> length_dist(1, DEFAULT_KEY_LENGTH);
         return random_string(length_dist(rng));
      }

      // Generate a random value
      std::string random_value()
      {
         std::uniform_int_distribution<size_t> length_dist(0, DEFAULT_VALUE_LENGTH);
         return random_string(length_dist(rng));
      }

      // Choose a random key from the reference map
      std::string random_existing_key()
      {
         if (reference_map.empty())
         {
            auto random_key_result = random_key();
            std::cout << "Reference map is empty. Using random key: " << random_key_result
                      << std::endl;
            return random_key_result;
         }

         std::uniform_int_distribution<size_t> idx_dist(0, reference_map.size() - 1);
         auto                                  it = reference_map.begin();
         std::advance(it, idx_dist(rng));
         return it->first;
      }

      // Choose a random operation to perform
      Operation random_operation()
      {
         std::uniform_int_distribution<size_t> op_dist(
             0, static_cast<size_t>(Operation::OPERATION_COUNT) - 1);
         return static_cast<Operation>(op_dist(rng));
      }
   };

   // Functions to perform the various operations

   // Insert a random key-value pair
   void perform_insert(FuzzTestEnvironment& env)
   {
      auto key   = env.random_key();
      auto value = env.random_value();
      auto tx    = env.current_transaction();

      try
      {
         tx->insert(key, value);
         // Record the pending insert instead of updating reference map directly
         env.record_pending_insert(key, value);
         std::cout << "Inserted key: " << key << " with value of length: " << value.size()
                   << std::endl;
      }
      catch (const std::exception& e)
      {
         // Key might already exist - that's okay for the fuzz test
         std::cout << "Insert failed for key: " << key << " - " << e.what() << std::endl;
      }
   }

   // Get a random key's value
   void perform_get(FuzzTestEnvironment& env)
   {
      auto key = env.random_existing_key();
      auto tx  = env.current_transaction();

      std::vector<char> buffer;
      int32_t           result = tx->get(key, &buffer);

      bool exists_in_ref = env.reference_map.find(key) != env.reference_map.end();

      if (result >= 0)
      {
         std::string value(buffer.data(), buffer.size());
         std::cout << "Got key: " << key << " with value of length: " << value.size() << std::endl;

         // Verify against reference map
         if (exists_in_ref)
         {
            REQUIRE(value == env.reference_map[key]);
         }
         else
         {
            // This could happen in multi-transaction scenarios where one transaction
            // hasn't seen updates from another yet
            std::cout << "WARNING: Key found in DB but not in reference map: " << key << std::endl;
         }
      }
      else if (result == iterator<>::value_subtree)
      {
         std::cout << "Key is a subtree: " << key << std::endl;
      }
      else
      {
         std::cout << "Key not found: " << key << std::endl;
         // If the key is not found in the database but exists in the reference map,
         // this is a bug - the database and reference map are out of sync
         if (exists_in_ref)
         {
            std::cout << "ERROR: Key exists in reference map but not in database. This is a bug!"
                      << std::endl;
            REQUIRE_FALSE(exists_in_ref);  // This will fail the test, indicating a bug
         }
         else
         {
            REQUIRE(!exists_in_ref);
         }
      }
   }

   // Update a random key's value
   void perform_update(FuzzTestEnvironment& env)
   {
      auto key   = env.random_existing_key();
      auto value = env.random_value();
      auto tx    = env.current_transaction();

      try
      {
         int result = tx->update(key, value);
         if (result >= 0)
         {
            // Record the pending update instead of updating reference map directly
            env.record_pending_insert(key, value);
            std::cout << "Updated key: " << key << " with new value of length: " << value.size()
                      << std::endl;
         }
         else
         {
            std::cout << "Key not found for update: " << key << std::endl;
            // If the key is not found in the database but exists in the reference map,
            // this is a bug - the database and reference map are out of sync
            bool exists_in_ref = env.reference_map.find(key) != env.reference_map.end();
            if (exists_in_ref)
            {
               std::cout << "ERROR: Key exists in reference map but not in database. This is a bug!"
                         << std::endl;
               REQUIRE_FALSE(exists_in_ref);  // This will fail the test, indicating a bug
            }
         }
      }
      catch (const std::exception& e)
      {
         std::cout << "Update failed for key: " << key << " - " << e.what() << std::endl;
      }
   }

   // Remove a random key
   void perform_remove(FuzzTestEnvironment& env)
   {
      auto key = env.random_existing_key();
      auto tx  = env.current_transaction();

      try
      {
         int result = tx->remove(key);
         if (result >= 0)
         {
            // Record the pending remove instead of updating reference map directly
            env.record_pending_remove(key);
            std::cout << "Removed key: " << key << std::endl;
         }
         else
         {
            std::cout << "Key not found for removal: " << key << std::endl;
            // If the key is not found in the database but exists in the reference map,
            // this is a bug - the database and reference map are out of sync
            bool exists_in_ref = env.reference_map.find(key) != env.reference_map.end();
            if (exists_in_ref)
            {
               std::cout << "ERROR: Key exists in reference map but not in database. This is a bug!"
                         << std::endl;
               REQUIRE_FALSE(exists_in_ref);  // This will fail the test, indicating a bug
            }
         }
      }
      catch (const std::exception& e)
      {
         std::cout << "Remove failed for key: " << key << " - " << e.what() << std::endl;
      }
   }

   // Iterate to the next key
   void perform_iterate_next(FuzzTestEnvironment& env)
   {
      auto tx = env.current_transaction();

      try
      {
         // Check if we have any keys in the database by trying to insert a test key
         std::string test_key   = "test_key_" + std::to_string(env.rng());
         std::string test_value = "test_value";
         tx->insert(test_key, test_value);

         // Now we know we have at least one key
         tx->start();

         // Check if we have at least one key
         if (!tx->begin())
         {
            std::cout << "No keys in database" << std::endl;
            return;
         }

         // Start at a random position
         size_t steps = env.rng() % 5;
         for (size_t i = 0; i < steps && !tx->is_end(); ++i)
         {
            tx->next();
         }

         if (!tx->is_end())
         {
            // Convert key_view to std::string
            std::string key(tx->key().data(), tx->key().size());
            std::cout << "Iterator at key: " << key << std::endl;

            // Try to move next
            if (tx->next())
            {
               std::string next_key(tx->key().data(), tx->key().size());
               std::cout << "  Moved to next: " << next_key << std::endl;
            }
            else
            {
               std::cout << "  Moved to end" << std::endl;
            }
         }
         else
         {
            std::cout << "Iterator is at end" << std::endl;
         }
      }
      catch (const std::exception& e)
      {
         std::cout << "Iterator operation failed: " << e.what() << std::endl;
      }
   }

   // Iterate to the previous key
   void perform_iterate_prev(FuzzTestEnvironment& env)
   {
      auto tx = env.current_transaction();

      try
      {
         // Check if we have any keys in the database by trying to insert a test key
         std::string test_key   = "test_key_" + std::to_string(env.rng());
         std::string test_value = "test_value";
         tx->insert(test_key, test_value);

         // Now we know we have at least one key
         tx->start();

         // Start at end and move to last element
         if (!tx->end() || !tx->prev())
         {
            std::cout << "No keys in database" << std::endl;
            return;
         }

         // Now we're at the last key
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Iterator at last key: " << key << std::endl;

         // Move to a random previous position
         size_t steps = env.rng() % 5;
         for (size_t i = 0; i < steps && !tx->is_start(); ++i)
         {
            if (!tx->prev())
            {
               break;
            }
         }

         if (!tx->is_start())
         {
            std::string prev_key(tx->key().data(), tx->key().size());
            std::cout << "  Moved to previous: " << prev_key << std::endl;
         }
         else
         {
            std::cout << "  Moved to start" << std::endl;
         }
      }
      catch (const std::exception& e)
      {
         std::cout << "Iterator operation failed: " << e.what() << std::endl;
      }
   }

   // Move iterator to first key
   void perform_iterate_first(FuzzTestEnvironment& env)
   {
      auto tx = env.current_transaction();

      try
      {
         // Insert a test key to ensure we have at least one key in the database
         std::string test_key   = "test_key_" + std::to_string(env.rng());
         std::string test_value = "test_value";
         tx->insert(test_key, test_value);

         // Start iteration on the transaction
         std::cout << "  Starting transaction for iterate_first" << std::endl;
         tx->start();

         std::cout << "  Calling tx.first()" << std::endl;
         if (tx->first())
         {
            std::string key(tx->key().data(), tx->key().size());
            std::cout << "Iterator at first key: " << key << std::endl;
         }
         else
         {
            std::cout << "No keys in database" << std::endl;
         }
      }
      catch (const std::exception& e)
      {
         std::cout << "Iterator operation failed: " << e.what() << std::endl;
      }
   }

   // Move iterator to last key
   void perform_iterate_last(FuzzTestEnvironment& env)
   {
      auto tx     = env.current_transaction();
      auto tx_idx = env.current_transaction_idx;  // Store index

      try
      {
         // Insert a test key to ensure we have at least one key in the database
         std::string test_key   = "test_key_" + std::to_string(env.rng());
         std::string test_value = "test_value";
         tx->insert(test_key, test_value);

         // Commit the insert to ensure the transaction is valid
         // USE COMMIT INSTEAD OF COMMIT_AND_CONTINUE
         std::cout << "  Committing transaction before iterate_last" << std::endl;
         tx->commit();

         // *** START NEW LOGIC TO HANDLE COMMITTED TRANSACTION ***
         // Apply pending changes (although maybe none from the simple insert above)
         //   env.apply_pending_changes(tx_idx);
         std::cout << "Committed transaction (idx=" << tx_idx << ") within iterate_last"
                   << std::endl;

         // Remove the committed transaction, unless it's the last one
         if (env.transactions.size() > 1)
         {
            env.transactions.erase(env.transactions.begin() + tx_idx);
            env.pending_changes.erase(env.pending_changes.begin() + tx_idx);

            // Update current_transaction_idx if necessary
            if (env.current_transaction_idx >= env.transactions.size() && !env.transactions.empty())
            {
               env.current_transaction_idx = env.transactions.size() - 1;
            }
            // Now get the potentially new current transaction for the rest of the function
            tx = env.current_transaction();
            std::cout << "  Switched to new current transaction (idx="
                      << env.current_transaction_idx << ")" << std::endl;
         }
         else
         {
            // If it was the last transaction, we can't remove it.
            // Start a new one for the iteration part? Or maybe skip iteration?
            // For now, let's just log and potentially skip iteration.
            std::cout << "  Cannot remove the last transaction. Restarting it for iteration."
                      << std::endl;
            // We need a valid transaction to iterate. Since we committed the only one,
            // we must start a new one.
            env.transactions[0] = env.ws->start_write_transaction();
            env.pending_changes[0].clear();
            tx = env.transactions[0];
         }
         // *** END NEW LOGIC ***

         // Start iteration on the transaction
         std::cout << "  Starting transaction for iterate_last" << std::endl;

         // Instead of using start(), use begin() to position at the first key
         // This ensures we have a valid iterator state before calling last()
         if (tx->begin())
         {
            std::cout << "  Iterator positioned at first key, now calling tx.last()" << std::endl;
            if (tx->last())
            {
               std::string key(tx->key().data(), tx->key().size());
               std::cout << "Iterator at last key: " << key << std::endl;
            }
            else
            {
               std::cout << "Failed to move to last key" << std::endl;
            }
         }
         else
         {
            std::cout << "No keys in database for last()" << std::endl;
         }
      }
      catch (const std::exception& e)
      {
         std::cout << "Iterator operation failed: " << e.what() << std::endl;
         // If an exception occurred during commit, the transaction might still be in the list
         // but unusable. We should probably try to remove it defensively.
         // This part is complex and depends on desired error handling.
      }
   }

   // Move iterator to lower bound of key
   void perform_iterate_lower_bound(FuzzTestEnvironment& env)
   {
      auto tx  = env.current_transaction();
      auto key = env.random_key();

      try
      {
         // Insert a test key to ensure we have at least one key in the database
         std::string test_key   = "test_key_" + std::to_string(env.rng());
         std::string test_value = "test_value";
         tx->insert(test_key, test_value);

         // Start iteration on the transaction
         std::cout << "  Starting transaction for lower_bound" << std::endl;
         tx->start();

         std::cout << "  Calling tx.lower_bound('" << key << "')" << std::endl;
         if (tx->lower_bound(key))
         {
            std::string found_key(tx->key().data(), tx->key().size());
            std::cout << "Lower bound of '" << key << "': " << found_key << std::endl;
         }
         else
         {
            std::cout << "No lower bound for '" << key << "'" << std::endl;
         }
      }
      catch (const std::exception& e)
      {
         std::cout << "Iterator operation failed: " << e.what() << std::endl;
      }
   }

   // Find upper bound of a random key
   void perform_iterate_upper_bound(FuzzTestEnvironment& env)
   {
      auto tx  = env.current_transaction();
      auto key = env.random_key();

      try
      {
         // Insert a test key to ensure we have at least one key in the database
         std::string test_key   = "test_key_" + std::to_string(env.rng());
         std::string test_value = "test_value";
         tx->insert(test_key, test_value);

         // Start iteration on the transaction
         std::cout << "  Starting transaction for upper_bound" << std::endl;
         tx->start();

         std::cout << "  Calling tx.upper_bound('" << key << "')" << std::endl;
         if (tx->upper_bound(key))
         {
            std::string found_key(tx->key().data(), tx->key().size());
            std::cout << "Upper bound of '" << key << "': " << found_key << std::endl;
         }
         else
         {
            std::cout << "No upper bound found for key: " << key << std::endl;
         }
      }
      catch (const std::exception& e)
      {
         std::cout << "Iterator operation failed: " << e.what() << std::endl;
      }
   }

   // Start a new transaction
   void perform_start_transaction(FuzzTestEnvironment& env)
   {
      // First commit the current one to avoid accumulating too many
      if (env.transactions.size() > DEFAULT_NUM_TRANSACTIONS)
      {
         // Apply pending changes before committing
         env.apply_pending_changes(env.current_transaction_idx);

         env.current_transaction()->commit();
         env.transactions.erase(env.transactions.begin() + env.current_transaction_idx);
         env.pending_changes.erase(env.pending_changes.begin() + env.current_transaction_idx);

         if (env.current_transaction_idx >= env.transactions.size() && !env.transactions.empty())
         {
            env.current_transaction_idx = env.transactions.size() - 1;
         }
      }

      auto tx = env.ws->start_write_transaction();
      env.transactions.push_back(std::move(tx));
      env.pending_changes.push_back({});  // Initialize pending changes for the new transaction
      env.current_transaction_idx = env.transactions.size() - 1;

      std::cout << "Started new transaction (idx=" << env.current_transaction_idx << ")"
                << std::endl;
   }

   // Commit the current transaction
   void perform_commit_transaction(FuzzTestEnvironment& env)
   {
      if (env.transactions.size() <= 1)
      {
         // Don't commit the last transaction
         std::cout << "Not committing the only transaction" << std::endl;
         return;
      }

      // Apply pending changes to the reference map before committing
      env.apply_pending_changes(env.current_transaction_idx);

      env.current_transaction()->commit();
      std::cout << "Committed transaction (idx=" << env.current_transaction_idx << ")" << std::endl;

      env.transactions.erase(env.transactions.begin() + env.current_transaction_idx);
      env.pending_changes.erase(env.pending_changes.begin() + env.current_transaction_idx);

      if (env.current_transaction_idx >= env.transactions.size())
      {
         env.current_transaction_idx = env.transactions.size() - 1;
      }
   }

   // Abort the current transaction
   void perform_abort_transaction(FuzzTestEnvironment& env)
   {
      if (env.transactions.size() <= 1)
      {
         // Don't abort the last transaction
         std::cout << "Not aborting the only transaction" << std::endl;
         return;
      }

      // Discard pending changes for this transaction
      env.discard_pending_changes(env.current_transaction_idx);

      env.current_transaction()->abort();
      std::cout << "Aborted transaction (idx=" << env.current_transaction_idx << ")" << std::endl;

      env.transactions.erase(env.transactions.begin() + env.current_transaction_idx);
      env.pending_changes.erase(env.pending_changes.begin() + env.current_transaction_idx);

      if (env.current_transaction_idx >= env.transactions.size())
      {
         env.current_transaction_idx = env.transactions.size() - 1;
      }
   }

   // Count keys in a range
   void perform_count_keys(FuzzTestEnvironment& env)
   {
      auto tx = env.current_transaction();

      // Generate two random keys
      auto key1 = env.random_key();
      auto key2 = env.random_key();

      // Ensure key1 <= key2
      if (key1 > key2)
      {
         std::swap(key1, key2);
      }

      uint32_t count = tx->count_keys(key1, key2);
      std::cout << "Count keys in range [" << key1 << ", " << key2 << "): " << count << std::endl;
   }

   // Iterate from the beginning of the database
   void perform_iterate_begin(FuzzTestEnvironment& env)
   {
      auto tx = env.current_transaction();

      try
      {
         // Insert a test key to ensure we have at least one key in the database
         std::string test_key   = "test_key_" + std::to_string(env.rng());
         std::string test_value = "test_value";
         tx->insert(test_key, test_value);

         // Start iteration on the transaction
         std::cout << "  Starting transaction for begin" << std::endl;
         tx->start();

         std::cout << "  Calling tx.begin()" << std::endl;
         if (tx->begin())
         {
            std::string found_key(tx->key().data(), tx->key().size());
            std::cout << "First key: " << found_key << std::endl;
         }
         else
         {
            std::cout << "No keys in database" << std::endl;
         }
      }
      catch (const std::exception& e)
      {
         std::cout << "Iterator operation failed: " << e.what() << std::endl;
      }
   }

   // Select a random operation to perform
   Operation select_random_operation(FuzzTestEnvironment& env)
   {
      return env.random_operation();
   }

   // Perform a random operation on the database
   void perform_random_operation(FuzzTestEnvironment& env)
   {
      auto op = select_random_operation(env);

      switch (op)
      {
         case Operation::INSERT:
            std::cout << "INSERT" << std::endl;
            perform_insert(env);
            break;

         case Operation::GET:
            std::cout << "GET" << std::endl;
            perform_get(env);
            break;

         case Operation::UPDATE:
            std::cout << "UPDATE" << std::endl;
            perform_update(env);
            break;

         case Operation::REMOVE:
            std::cout << "REMOVE" << std::endl;
            perform_remove(env);
            break;

         case Operation::ITERATE_FIRST:
            std::cout << "ITERATE_FIRST" << std::endl;
            perform_iterate_first(env);
            break;

         case Operation::ITERATE_LAST:
            std::cout << "ITERATE_LAST" << std::endl;
            perform_iterate_last(env);
            break;

         case Operation::ITERATE_NEXT:
            std::cout << "ITERATE_NEXT" << std::endl;
            perform_iterate_next(env);
            break;

         case Operation::ITERATE_PREV:
            std::cout << "ITERATE_PREV" << std::endl;
            perform_iterate_prev(env);
            break;

         case Operation::ITERATE_LOWER_BOUND:
            std::cout << "ITERATE_LOWER_BOUND" << std::endl;
            perform_iterate_lower_bound(env);
            break;

         case Operation::ITERATE_UPPER_BOUND:
            std::cout << "ITERATE_UPPER_BOUND" << std::endl;
            perform_iterate_upper_bound(env);
            break;

         case Operation::ITERATE_BEGIN:
            std::cout << "ITERATE_BEGIN" << std::endl;
            perform_iterate_begin(env);
            break;

         case Operation::ABORT_TRANSACTION:
            std::cout << "ABORT_TRANSACTION" << std::endl;
            perform_abort_transaction(env);
            break;

         case Operation::COUNT_KEYS:
            std::cout << "COUNT_KEYS" << std::endl;
            perform_count_keys(env);
            break;

         default:
            std::cout << "UNKNOWN" << std::endl;
            break;
      }
   }

   // Verify database contents after all operations
   void verify_database_contents(FuzzTestEnvironment& env)
   {
      std::cout << "\nVerifying database contents..." << std::endl;

      // Commit any pending transactions
      if (!env.transactions.empty())
      {
         try
         {
            // Apply pending changes for the last transaction before committing
            env.apply_pending_changes(env.transactions.size() - 1);

            env.transactions.back()->commit();
            std::cout << "Committed pending transaction" << std::endl;
         }
         catch (const std::exception& e)
         {
            std::cout << "Failed to commit final transaction: " << e.what() << std::endl;
         }
      }

      // Start a new read-only transaction for verification
      auto tx = env.ws->start_read_transaction();
      std::cout << "Started verification transaction" << std::endl;

      try
      {
         // Verify each key in our reference map exists with correct value
         size_t verified_keys = 0;
         for (const auto& [key, expected_value] : env.reference_map)
         {
            std::vector<char> buffer;
            try
            {
               int32_t result = tx->get(key, &buffer);

               if (result >= 0)
               {
                  std::string actual_value(buffer.data(), buffer.size());
                  if (actual_value == expected_value)
                  {
                     verified_keys++;
                  }
                  else
                  {
                     std::cout << "Value mismatch for key '" << key << "': expected '"
                               << expected_value << "', got '" << actual_value << "'" << std::endl;
                  }
               }
               else
               {
                  std::cout << "Key missing from database: " << key << std::endl;
               }
            }
            catch (const std::exception& e)
            {
               std::cout << "Error getting key '" << key << "': " << e.what() << std::endl;
            }
         }

         std::cout << "Verified " << verified_keys << " of " << env.reference_map.size()
                   << " keys from reference map" << std::endl;

         // Count total keys in database to compare with reference map
         size_t total_keys = 0;
         try
         {
            // Start at first key and iterate through all
            if (tx->begin())
            {
               do
               {
                  total_keys++;
               } while (tx->next());
            }

            std::cout << "Total keys in database: " << total_keys << std::endl;

            if (total_keys != verified_keys)
            {
               std::cout << "Warning: Reference map size (" << env.reference_map.size()
                         << ") differs from database key count (" << total_keys << ")" << std::endl;
            }
         }
         catch (const std::exception& e)
         {
            std::cout << "Error counting keys: " << e.what() << std::endl;
         }
      }
      catch (const std::exception& e)
      {
         std::cout << "Error during verification: " << e.what() << std::endl;
      }

      std::cout << "Verification completed" << std::endl;
   }

}  // anonymous namespace

// Run the fuzz test
TEST_CASE("Fuzz test for arbtrie Database API", "[fuzz]")
{
   // Create a test environment with random seed
   std::random_device rd;
   uint32_t           seed = rd();
   std::cout << "Randomness seeded to: " << seed << std::endl;

   // Allow overriding seed with environment variable for reproducing
   char* env_seed = std::getenv("ARBTRIE_FUZZ_SEED");
   if (env_seed)
   {
      try
      {
         seed = std::stoul(env_seed);
      }
      catch (...)
      {
         // Keep the random seed if conversion fails
      }
   }

   std::cout << "Fuzz test using seed: " << seed << std::endl;
   FuzzTestEnvironment env(seed);

   // Run the test operations
   size_t num_operations =
       std::min(static_cast<size_t>(GENERATE(100, 1000, MAX_TEST_OPERATIONS)), MAX_TEST_OPERATIONS);

   std::cout << "Running " << num_operations << " operations..." << std::endl;

   // Increment operation number
   size_t current_op = 0;

   // Run operations
   try
   {
      while (current_op < num_operations)
      {
         std::cout << "Operation " << current_op << ": ";
         perform_random_operation(env);
         current_op++;
      }
   }
   catch (const std::exception& e)
   {
      std::cout << "Exception during operation " << current_op << ": " << e.what() << std::endl;
   }

   // Verify database at the end
   verify_database_contents(env);
}

// Replace edge cases test with simplified version
TEST_CASE("Edge cases for arbtrie Database API", "[fuzz][edge_cases]")
{
   // Create a test environment with fixed seed for reproducibility
   FuzzTestEnvironment env(42);

   SECTION("Empty key handling")
   {
      // Test empty key
      auto        tx        = env.current_transaction();
      std::string empty_key = "";
      std::string value     = "value for empty key";

      tx->insert(empty_key, value);

      std::vector<char> buffer;
      int32_t           result = tx->get(empty_key, &buffer);
      REQUIRE(result >= 0);
      REQUIRE(std::string(buffer.data(), buffer.size()) == value);
   }

   SECTION("Very long keys and values")
   {
      auto tx = env.current_transaction();

      // Create a very long key
      std::string long_key   = env.random_string(MAX_KEY_LENGTH);
      std::string long_value = env.random_string(MAX_VALUE_LENGTH);

      tx->insert(long_key, long_value);

      std::vector<char> buffer;
      int32_t           result = tx->get(long_key, &buffer);
      REQUIRE(result >= 0);
      REQUIRE(std::string(buffer.data(), buffer.size()) == long_value);
   }

   SECTION("Keys with special characters")
   {
      auto tx = env.current_transaction();

      // Create keys with various special characters
      std::vector<std::string> special_keys = {"key\nwith\nnewlines", "key\twith\ttabs",
                                               "key with spaces", "!@#$%^&*()"};

      for (const auto& key : special_keys)
      {
         std::string value = "value for " + key;

         try
         {
            tx->insert(key, value);

            std::vector<char> buffer;
            int32_t           result = tx->get(key, &buffer);
            REQUIRE(result >= 0);
            REQUIRE(std::string(buffer.data(), buffer.size()) == value);
         }
         catch (const std::exception& e)
         {
            // Some special characters might not be allowed, that's okay
            std::cout << "Failed with special key: " << e.what() << std::endl;
         }
      }
   }
}

// Add a stress test with concurrent transactions
TEST_CASE("Stress test transaction isolation", "[stress]")
{
   FuzzTestEnvironment env(42);

   // Commit the initial transaction created by the environment constructor
   std::cout << "Committing initial transaction from FuzzTestEnvironment..." << std::endl;
   env.current_transaction()->commit();
   env.transactions.clear();  // Clear the environment's transaction list
   env.pending_changes.clear();
   env.current_transaction_idx = 0;  // Reset index
   std::cout << "Initial transaction committed." << std::endl;

   // Create multiple write transactions
   std::vector<std::shared_ptr<write_session>> sessions;  // Store sessions as shared pointers
   std::vector<write_transaction::ptr>         txs;
   std::vector<std::set<std::string>>          committed_keys;
   const int                                   num_transactions     = 3;
   const int                                   keys_per_transaction = 5;

   // First create and store sessions
   for (int i = 0; i < num_transactions; i++)
   {
      // Use the start_write_session method
      sessions.emplace_back(env.db->start_write_session());
   }

   // Start multiple transactions with DIFFERENT ROOT INDICES
   for (int i = 0; i < num_transactions; i++)
   {
      ARBTRIE_WARN("Starting transaction ", i, " with root index ", i);
      // Use a different root index for each transaction
      txs.emplace_back(sessions[i]->start_write_transaction(i));
      committed_keys.emplace_back();

      std::cout << "Started transaction " << i << " with root index " << i << std::endl;

      // Insert some keys in each transaction
      for (int j = 0; j < keys_per_transaction; j++)
      {
         std::string key   = "tx" + std::to_string(i) + "_key" + std::to_string(j);
         std::string value = "value_" + key;

         txs[i]->insert(key, value);
         committed_keys[i].insert(key);

         std::cout << "Inserted key " << key << " in transaction " << i << std::endl;

         // Verify the key was inserted correctly
         std::vector<char> check_buffer;
         int32_t           check_result = txs[i]->get(key, &check_buffer);
         if (check_result < 0)
         {
            std::cout << "ERROR: Inserted key " << key << " but get returned " << check_result
                      << std::endl;
         }
      }
   }

   // Verify that each transaction can see its own keys but not others' keys
   for (int tx_idx = 0; tx_idx < txs.size(); tx_idx++)
   {
      auto tx = txs[tx_idx];
      std::cout << "Verifying visibility in transaction " << tx_idx << " before commit"
                << std::endl;

      // Should see its own keys
      for (const auto& key : committed_keys[tx_idx])
      {
         std::vector<char> buffer;
         int32_t           result = tx->get(key, &buffer);
         if (result < 0)
         {
            std::cout << "ERROR: Transaction " << tx_idx << " cannot see its own key: " << key
                      << std::endl;
         }
         REQUIRE(result >= 0);
      }

      // Should not see other transactions' keys
      for (int other_tx = 0; other_tx < txs.size(); other_tx++)
      {
         if (other_tx == tx_idx)
            continue;

         for (const auto& key : committed_keys[other_tx])
         {
            std::vector<char> buffer;
            int32_t           result = tx->get(key, &buffer);
            if (result >= 0)
            {
               std::cout << "ERROR: Transaction " << tx_idx << " can see transaction " << other_tx
                         << "'s key: " << key << std::endl;
            }
            REQUIRE(result < 0);
         }
      }
   }

   // Commit the transactions one by one
   for (int tx_idx = 0; tx_idx < txs.size(); tx_idx++)
   {
      std::cout << "Committing transaction " << tx_idx << std::endl;
      txs[tx_idx]->commit();
      std::cout << "Transaction " << tx_idx << " committed" << std::endl;

      // Start a new read transaction to see the updated state
      auto rs = env.db->start_read_session();
      // Use the same root index as the transaction we just committed
      auto read_tx = rs.start_read_transaction(tx_idx);
      std::cout << "Started read transaction with root index " << tx_idx << " to verify commit "
                << tx_idx << std::endl;

      // Verify all keys in this transaction are visible
      for (const auto& key : committed_keys[tx_idx])
      {
         std::cout << "Checking key: " << key << " from transaction " << tx_idx << std::endl;
         std::vector<char> buffer;
         int32_t           result = read_tx->get(key, &buffer);
         if (result < 0)
         {
            std::cout << "ERROR: Key " << key << " should be visible but result is " << result
                      << std::endl;
         }
         REQUIRE(result >= 0);
      }
   }
}

// Add a specific test to identify which operation is causing the bug
TEST_CASE("Identify which operation in the sequence causes the bug", "[fuzz][bug][isolation]")
{
   // Create a test environment with a fixed seed
   uint32_t seed = 4187684981;
   std::cout << "Isolation test using seed: " << seed << std::endl;

   // Test different combinations to isolate the issue
   SECTION("Test 1: Just insert and call last()")
   {
      FuzzTestEnvironment env(seed);

      // Insert some keys
      std::cout << "Inserting test keys" << std::endl;
      for (int i = 0; i < 5; i++)
      {
         std::string test_key   = "test_key_" + std::to_string(i);
         std::string test_value = "test_value_" + std::to_string(i);
         env.current_transaction()->insert(test_key, test_value);
      }

      auto tx = env.current_transaction();
      tx->start();

      // Call last() directly
      std::cout << "Calling last() without other operations" << std::endl;
      if (tx->last())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Last key: " << key << std::endl;
      }
   }

   SECTION("Test 2: Insert, upper_bound, and last()")
   {
      FuzzTestEnvironment env(seed);

      // Insert some keys
      std::cout << "Inserting test keys" << std::endl;
      for (int i = 0; i < 5; i++)
      {
         std::string test_key   = "test_key_" + std::to_string(i);
         std::string test_value = "test_value_" + std::to_string(i);
         env.current_transaction()->insert(test_key, test_value);
      }

      auto tx = env.current_transaction();
      tx->start();

      // Call upper_bound
      std::cout << "Calling upper_bound" << std::endl;
      if (tx->upper_bound("m"))
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Upper bound result: " << key << std::endl;
      }

      // Call last()
      std::cout << "Calling last() after upper_bound" << std::endl;
      if (tx->last())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Last key: " << key << std::endl;
      }
   }

   SECTION("Test 3: Insert, upper_bound, next, and last()")
   {
      FuzzTestEnvironment env(seed);

      // Insert some keys
      std::cout << "Inserting test keys" << std::endl;
      for (int i = 0; i < 5; i++)
      {
         std::string test_key   = "test_key_" + std::to_string(i);
         std::string test_value = "test_value_" + std::to_string(i);
         env.current_transaction()->insert(test_key, test_value);
      }

      auto tx = env.current_transaction();
      tx->start();

      // Call upper_bound
      std::cout << "Calling upper_bound" << std::endl;
      if (tx->upper_bound("m"))
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Upper bound result: " << key << std::endl;
      }

      // Call next
      std::cout << "Calling next" << std::endl;
      if (tx->next())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Next result: " << key << std::endl;
      }

      // Call last()
      std::cout << "Calling last() after upper_bound and next" << std::endl;
      if (tx->last())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Last key: " << key << std::endl;
      }
   }

   SECTION("Test 4: The full sequence that causes the crash")
   {
      FuzzTestEnvironment env(seed);

      // Insert some keys
      std::cout << "Inserting test keys" << std::endl;
      for (int i = 0; i < 5; i++)
      {
         std::string test_key   = "test_key_" + std::to_string(i);
         std::string test_value = "test_value_" + std::to_string(i);
         env.current_transaction()->insert(test_key, test_value);
      }

      auto tx = env.current_transaction();
      tx->start();

      // Call upper_bound
      std::cout << "Calling upper_bound" << std::endl;
      if (tx->upper_bound("m"))
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Upper bound result: " << key << std::endl;
      }

      // Call next
      std::cout << "Calling next" << std::endl;
      if (tx->next())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Next result: " << key << std::endl;
      }

      // Insert and remove a key
      std::string temp_key = "temp_key";
      std::cout << "Insert temp key" << std::endl;
      tx->insert(temp_key, "temp_value");

      std::cout << "Remove temp key" << std::endl;
      tx->remove(temp_key);

      // Call last()
      std::cout << "Calling last() after all operations" << std::endl;
      if (tx->last())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Last key: " << key << std::endl;
      }
   }
}

// Add a simplified test to check basic iterator operations
TEST_CASE("Basic iterator operations test", "[fuzz][bug][basic]")
{
   // Create a test environment once for all sections
   FuzzTestEnvironment env(42);

   // Insert some keys if not already done
   std::cout << "Inserting test keys for Basic iterator operations test" << std::endl;
   {
      auto initial_tx = env.current_transaction();
      for (int i = 0; i < 5; i++)
      {
         std::string test_key   = "test_key_" + std::to_string(i);
         std::string test_value = "test_value_" + std::to_string(i);
         initial_tx->insert(test_key, test_value);
      }
      initial_tx->commit();  // Commit the initial inserts
   }

   // Get a fresh transaction for each test section
   auto tx = env.ws->start_write_transaction();  // Start fresh tx here

   SECTION("Test first()")
   {
      std::cout << "Starting transaction and calling first()" << std::endl;

      // Check if transaction is valid
      std::cout << "Transaction valid: " << (tx->valid() ? "yes" : "no") << std::endl;

      // Insert a test key to ensure we have at least one key in the database
      std::string test_key   = "test_key_first";
      std::string test_value = "test_value";
      tx->insert(test_key, test_value);

      // Start the transaction
      tx->start();

      std::cout << "After start(), transaction valid: " << (tx->valid() ? "yes" : "no")
                << std::endl;

      if (tx->first())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "First key: " << key << std::endl;
      }
      else
      {
         std::cout << "Failed to move to first key" << std::endl;
      }
   }

   SECTION("Test begin()")
   {
      std::cout << "Starting transaction and calling begin()" << std::endl;

      // Check if transaction is valid
      std::cout << "Transaction valid: " << (tx->valid() ? "yes" : "no") << std::endl;

      // Insert a test key to ensure we have at least one key in the database
      std::string test_key   = "test_key_begin";
      std::string test_value = "test_value";
      tx->insert(test_key, test_value);

      // Start the transaction
      tx->start();

      std::cout << "After start(), transaction valid: " << (tx->valid() ? "yes" : "no")
                << std::endl;

      if (tx->begin())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Begin key: " << key << std::endl;
      }
      else
      {
         std::cout << "Failed to move to begin key" << std::endl;
      }
   }

   SECTION("Test last() with restart")
   {
      std::cout << "Starting transaction with restart before calling last()" << std::endl;

      // Check if transaction is valid
      std::cout << "Transaction valid: " << (tx->valid() ? "yes" : "no") << std::endl;

      // Insert a test key to ensure we have at least one key in the database
      std::string test_key   = "test_key_last";
      std::string test_value = "test_value";
      tx->insert(test_key, test_value);

      // Commit the insert to ensure the transaction is valid
      // tx->commit(); // COMMIT IS ALREADY CALLED IN THE SECTION

      std::cout << "After commit(), transaction valid: "  // ADJUST LOG MESSAGE
                << (tx->valid() ? "yes" : "no") << std::endl;

      // Call last() directly without using begin() first
      std::cout << "Calling last() directly" << std::endl;
      if (tx->last())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Last key: " << key << std::endl;
      }
      else
      {
         std::cout << "Failed to move to last key" << std::endl;
      }
   }
}

// Test environment setup
struct TestEnv
{
   std::filesystem::path          db_path;
   database*                      db = nullptr;
   std::shared_ptr<write_session> ws = nullptr;

   TestEnv()
   {
      // Create a unique temporary directory for the test
      auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
      db_path =
          std::filesystem::temp_directory_path() / ("arbtrie_test_" + std::to_string(timestamp));

      std::cout << "Creating database at " << db_path << std::endl;
      std::filesystem::create_directories(db_path);

      // Configure and open the database
      runtime_config cfg;

      database::create(db_path, cfg);
      db = new database(db_path, cfg);
      ws = db->start_write_session();
   }

   ~TestEnv()
   {
      ws.reset();  // Use reset instead of delete for shared_ptr
      delete db;
      std::filesystem::remove_all(db_path);
   }

   write_transaction::ptr start_transaction() { return ws->start_write_transaction(); }
};

// Tests for the arbtrie database
TEST_CASE("Iterator operations bug test", "[bug]")
{
   TestEnv env;
   auto    tx = env.start_transaction();

   // Insert some test keys
   std::cout << "Inserting test keys..." << std::endl;
   for (int i = 0; i < 5; i++)
   {
      std::string key   = "test_key_" + std::to_string(i);
      std::string value = "test_value_" + std::to_string(i);
      tx->insert(key, value);
   }

   SECTION("Basic first() operation")
   {
      tx->start();
      std::cout << "Testing first()..." << std::endl;
      if (tx->first())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "First key: " << key << std::endl;
      }
   }

   SECTION("Basic last() operation")
   {
      tx->start();
      std::cout << "Testing last()..." << std::endl;
      if (tx->last())
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Last key: " << key << std::endl;
      }
   }

   SECTION("Operations that might cause the last() bug")
   {
      // Start transaction
      tx->start();

      // Call upper_bound
      std::cout << "Calling upper_bound..." << std::endl;
      if (tx->upper_bound("m"))
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Upper bound result: " << key << std::endl;
      }

      // Insert and remove a key
      std::string temp_key = "temp_key";
      std::cout << "Inserting and removing a temporary key..." << std::endl;
      tx->insert(temp_key, "temp_value");
      tx->remove(temp_key);

      // Call last() which might crash
      std::cout << "Calling last() after operations..." << std::endl;
      bool result = tx->last();
      std::cout << "last() returned: " << (result ? "true" : "false") << std::endl;

      if (result)
      {
         std::string key(tx->key().data(), tx->key().size());
         std::cout << "Last key: " << key << std::endl;
      }
   }
}

// Add a simplified test to isolate the transaction commit issue
TEST_CASE("Simplified transaction commit test", "[fuzz][commit]")
{
   // Create a test environment with fixed seed for reproducibility
   FuzzTestEnvironment env(42);

   // Commit the initial transaction created by the environment constructor
   std::cout << "Committing initial transaction from FuzzTestEnvironment..." << std::endl;
   env.current_transaction()->commit();
   env.transactions.clear();  // Clear the environment's transaction list
   env.pending_changes.clear();
   env.current_transaction_idx = 0;  // Reset index
   std::cout << "Initial transaction committed." << std::endl;

   std::cout << "Starting simplified transaction commit test" << std::endl;

   // Start a transaction with explicit index 0
   auto        tx    = env.ws->start_write_transaction(0);
   std::string key   = "test_key";
   std::string value = "test_value";

   std::cout << "Inserting key: " << key << std::endl;
   tx->insert(key, value);

   // Verify the key is visible in the transaction
   std::vector<char> buffer;
   int32_t           result = tx->get(key, &buffer);
   std::cout << "Before commit, key visibility: " << (result >= 0 ? "visible" : "not visible")
             << std::endl;
   REQUIRE(result >= 0);

   // Commit the transaction
   std::cout << "Committing transaction" << std::endl;
   tx->commit();

   // Start a new read transaction with the same index
   auto rs      = env.db->start_read_session();
   auto read_tx = rs.start_read_transaction(0);

   // Verify the key is visible in the read transaction
   std::vector<char> read_buffer;
   int32_t           read_result = read_tx->get(key, &read_buffer);
   std::cout << "After commit, key visibility: " << (read_result >= 0 ? "visible" : "not visible")
             << std::endl;
   REQUIRE(read_result >= 0);

   // Try with a different index to see if that's the issue
   if (read_result < 0)
   {
      std::cout << "Testing with different index..." << std::endl;
      for (int i = 0; i < 10; i++)
      {
         auto              alt_read_tx = rs.start_read_transaction(i);
         std::vector<char> buffer;
         int32_t           alt_result = alt_read_tx->get(key, &buffer);
         std::cout << "Index " << i
                   << " key visibility: " << (alt_result >= 0 ? "visible" : "not visible")
                   << std::endl;
      }
   }
}

// Add a new test case to verify that transactions on the same root index properly block each other
TEST_CASE("Transactions on same root index should block", "[transaction]")
{
   FuzzTestEnvironment env(42);

   // Commit the initial transaction created by the environment constructor
   std::cout << "Committing initial transaction from FuzzTestEnvironment..." << std::endl;
   env.current_transaction()->commit();
   env.transactions.clear();  // Clear the environment's transaction list
   env.pending_changes.clear();
   env.current_transaction_idx = 0;  // Reset index
   std::cout << "Initial transaction committed." << std::endl;

   std::cout << "Starting transaction test..." << std::endl;

   // Start a transaction on root index 0
   auto ws1 = env.db->start_write_session();
   auto tx1 = ws1->start_write_transaction(0);

   // Insert a key
   std::string key   = "test_key";
   std::string value = "test_value";
   tx1->insert(key, value);

   // Verify the key was inserted
   std::vector<char> buffer;
   int32_t           result = tx1->get(key, &buffer);
   REQUIRE(result >= 0);

   std::cout << "First transaction started and key inserted successfully" << std::endl;

   // Commit the first transaction
   std::cout << "Main thread: Committing first transaction..." << std::endl;
   tx1->commit();
   std::cout << "Main thread: First transaction committed" << std::endl;

   // Create a second write session
   auto ws2 = env.db->start_write_session();

   // Now try to start a second transaction - this should work now
   std::cout << "Main thread: Starting second transaction after commit..." << std::endl;
   auto tx2 = ws2->start_write_transaction(0);
   std::cout << "Main thread: Second transaction started successfully" << std::endl;

   // Insert another key in the second transaction
   std::string key2   = "another_key";
   std::string value2 = "another_value";
   tx2->insert(key2, value2);

   // Verify the key was inserted
   std::vector<char> buffer2;
   int32_t           result2 = tx2->get(key2, &buffer2);
   REQUIRE(result2 >= 0);

   // Commit the second transaction
   std::cout << "Main thread: Committing second transaction..." << std::endl;
   tx2->commit();
   std::cout << "Main thread: Second transaction committed" << std::endl;

   std::cout << "Test completed successfully" << std::endl;
}

// anonymous namespace