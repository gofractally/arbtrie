#pragma once
#include <atomic>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>

//#include <syncstream>
// #undef NDEBUG
#include <cassert>

namespace arbtrie
{
   /**
    * Controls whether caching operations should log debug information.
    * Used primarily in read operations and cache management to track:
    * - Cache hit/miss patterns
    * - Cache update timing
    * - Cache state changes
    */
   static constexpr bool debug_cache = false;

   /**
    * Enables validation of structural invariants throughout the codebase.
    * Used in:
    * - binary_node operations - Verifies node structure and key ordering
    * - tree operations - Ensures tree balance and connectivity
    * - data structure validation - Checks internal consistency
    * This is a fundamental debugging flag that helps maintain data structure integrity.
    */
   static constexpr bool debug_invariant = false;

   /**
    * Enables debug logging for root node operations.
    * Used in database operations to track:
    * - Root node modifications
    * - Tree structure changes
    * - Database state transitions
    * Particularly useful for debugging database consistency issues.
    */
   static constexpr bool debug_roots = false;

   /**
    * Enables comprehensive memory operation validation and tracking.
    * Used extensively throughout the codebase:
    * - read_lock::alloc() - Validates allocation state and prevents double allocation
    * - object_ref operations - Verifies checksums and validates memory moves
    * - seg_allocator - Tracks segment compaction and memory management
    * - node operations - Ensures proper memory boundaries and layout
    * - binary_node operations - Validates memory operations during node modifications
    * 
    * This is a critical debugging flag for catching memory-related issues and 
    * ensuring proper memory management throughout the system.
    */
   static constexpr bool debug_memory = false;

   struct scope
   {
      scope(const scope&) = delete;
      scope() { ++indent(); }
      ~scope() { --indent(); }
      static std::atomic<int>& indent()
      {
         static std::atomic<int> i = 0;
         return i;
      }
   };

   inline const char* thread_name(const char* n = "unset-thread-name")
   {
      static thread_local const char* thread_name = n;
      if (n)
         thread_name = n;
      return thread_name;
   }

   namespace detail
   {
      static inline std::mutex& debug_mutex()
      {
         static std::mutex m;
         return m;
      }
   }  // namespace detail

   template <typename... Ts>
   void debug(const char* func, int line, Ts... args)
   {
      std::lock_guard<std::mutex> lock(detail::debug_mutex());
      auto pre = std::string(thread_name()) + " " + std::string(func) + ":" + std::to_string(line);
      std::cerr << std::setw(20) << std::left << pre << " ";
      for (int x = 0; x < 4 * scope::indent(); ++x)
         std::cerr << " ";
      ((std::cerr << std::forward<Ts>(args)), ...);
      std::cerr << "\n";
   }

   inline auto set_current_thread_name(const char* name)
   {
      thread_name(name);
#ifdef __APPLE__
      return pthread_setname_np(name);
#else
      return pthread_setname_np(pthread_self(), name);
#endif
   }

// Log Levels with Distinct Colors
// --------------------------------
// ARBTRIE_WARN - Orange text (33m) - For warnings that require attention but aren't fatal
#define ARBTRIE_WARN(...) arbtrie::debug(__func__, __LINE__, "\033[33m", __VA_ARGS__, "\033[0m")

// ARBTRIE_ERROR - Bold Red text (1;31m) - For errors and exceptions
#define ARBTRIE_ERROR(...) arbtrie::debug(__func__, __LINE__, "\033[1;31m", __VA_ARGS__, "\033[0m")

// ARBTRIE_INFO - Cyan text (36m) - For informational messages about normal operation
#define ARBTRIE_INFO(...) arbtrie::debug(__func__, __LINE__, "\033[36m", __VA_ARGS__, "\033[0m")

// ARBTRIE_DEBUG - No color - For detailed debugging information (only in debug builds)
#define ARBTRIE_DEBUG(...) arbtrie::debug(__func__, __LINE__, __VA_ARGS__)

// Debug-only macros
#ifndef NDEBUG
#define ARBTRIE_SCOPE arbtrie::scope __sco__##__LINE__;
#else
   //#define ARBTRIE_DEBUG(...)
#define ARBTRIE_SCOPE
#endif

}  // namespace arbtrie
