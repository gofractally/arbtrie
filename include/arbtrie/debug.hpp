#pragma once
#include <atomic>
#include <cstdlib>  // For getenv
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

//#include <syncstream>
// #undef NDEBUG
#include <cassert>

namespace arbtrie
{
   /**
    * Log Level Enumeration
    * Used to control which messages are displayed based on their severity.
    * Can be set via the ARBTRIE_LOG_LEVEL environment variable.
    * 
    * Values:
    * 0 = TRACE - Most detailed logging, includes all messages
    * 1 = DEBUG - Detailed information for debugging
    * 2 = INFO  - General information about normal operation
    * 3 = WARN  - Warnings that need attention but aren't fatal
    * 4 = ERROR - Error conditions
    * 5 = FATAL - Critical errors causing termination
    * 6 = NONE  - No output (silent operation)
    */
   enum class log_level : uint8_t
   {
      trace = 0,
      debug = 1,
      info  = 2,
      warn  = 3,
      error = 4,
      fatal = 5,
      none  = 6
   };

   /**
    * Get the current log level from the environment variable ARBTRIE_LOG_LEVEL
    * 
    * Environment variable options:
    * ARBTRIE_LOG_LEVEL=TRACE (or 0) - Show all messages
    * ARBTRIE_LOG_LEVEL=DEBUG (or 1) - Show debug and above
    * ARBTRIE_LOG_LEVEL=INFO  (or 2) - Show info and above
    * ARBTRIE_LOG_LEVEL=WARN  (or 3) - Show warnings and errors only
    * ARBTRIE_LOG_LEVEL=ERROR (or 4) - Show only errors
    * ARBTRIE_LOG_LEVEL=FATAL (or 5) - Show only fatal errors
    * ARBTRIE_LOG_LEVEL=NONE  (or 6) - Silent operation
    * 
    * If not set, defaults to INFO in debug builds and WARN in release builds.
    */
   inline log_level get_log_level()
   {
      static log_level level = []
      {
         const char* env_level = std::getenv("ARBTRIE_LOG_LEVEL");
         if (!env_level)
         {
// Default log level if not specified
#ifdef NDEBUG
            return log_level::warn;  // Default to WARN in release builds
#else
            return log_level::info;  // Default to INFO in debug builds
#endif
         }

         // Check for string values
         std::string level_str(env_level);
         if (level_str == "TRACE" || level_str == "trace")
            return log_level::trace;
         if (level_str == "DEBUG" || level_str == "debug")
            return log_level::debug;
         if (level_str == "INFO" || level_str == "info")
            return log_level::info;
         if (level_str == "WARN" || level_str == "warn")
            return log_level::warn;
         if (level_str == "ERROR" || level_str == "error")
            return log_level::error;
         if (level_str == "FATAL" || level_str == "fatal")
            return log_level::fatal;
         if (level_str == "NONE" || level_str == "none")
            return log_level::none;

         // Try to parse as integer
         char* end;
         int   num = std::strtol(env_level, &end, 10);
         if (*end == '\0' && num >= 0 && num <= 6)
         {
            return static_cast<log_level>(num);
         }

// Invalid value defaults to INFO or WARN
#ifdef NDEBUG
         return log_level::warn;
#else
         return log_level::info;
#endif
      }();

      return level;
   }

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

   inline const char* thread_name(const char* n = nullptr)
   {
      static thread_local const char* thread_name = "unset-thread-name";
      if (n)
      {
         // Set thread name without truncation
         thread_name = n;
      }
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
   void debug(const char* file, const char* func, int line, log_level level, Ts... args)
   {
      // Skip if the message's log level is below the current threshold
      if (level < get_log_level())
      {
         return;
      }

      // Do all string preparation outside the lock

      // Extract filename from path
      const char* filename = file;
      const char* slash    = strrchr(file, '/');
      if (slash)
      {
         filename = slash + 1;
      }

      // Format as filename:line
      std::string location = std::string(filename) + ":" + std::to_string(line);

      // Get thread name - print nothing if it's the default "unset-thread-name"
      const char* tname = thread_name();
      std::string thread_str;
      if (strcmp(tname, "unset-thread-name") != 0)
      {
         // Truncate thread name to 8 chars only for display purposes
         thread_str = std::string(tname).substr(0, 8);
      }

      // Truncate function name to 20 chars if longer
      std::string func_str = std::string(func);
      if (func_str.length() > 20)
      {
         func_str = func_str.substr(0, 20);
      }

      // Calculate indentation
      int         indent_spaces = 4 * scope::indent();
      std::string indent_str(indent_spaces, ' ');

      // Build output stream outside the lock
      std::ostringstream output;
      output << std::setw(25) << std::left << location << "  "   // Add 2 spaces after location
             << std::setw(9) << std::left << thread_str << "  "  // Add 2 spaces after thread name
             << std::setw(20) << std::left << func_str << "  "   // Add 2 spaces after function name
             << indent_str;

      // Append the actual message
      ((output << std::forward<Ts>(args)), ...);
      output << "\n";

      // Only lock for the actual write to std::cerr
      std::lock_guard<std::mutex> lock(detail::debug_mutex());
      std::cerr << output.str();
   }

// Update the debug methods to include the log level

// ARBTRIE_TRACE - Gray text (37m) - For most detailed tracing information
#define ARBTRIE_TRACE(...)                                                             \
   arbtrie::debug(__FILE__, __func__, __LINE__, arbtrie::log_level::trace, "\033[37m", \
                  __VA_ARGS__, "\033[0m")

// ARBTRIE_DEBUG - No color - For detailed debugging information
#define ARBTRIE_DEBUG(...) \
   arbtrie::debug(__FILE__, __func__, __LINE__, arbtrie::log_level::debug, __VA_ARGS__)

// ARBTRIE_INFO - Cyan text (36m) - For informational messages about normal operation
#define ARBTRIE_INFO(...)                                                                          \
   arbtrie::debug(__FILE__, __func__, __LINE__, arbtrie::log_level::info, "\033[36m", __VA_ARGS__, \
                  "\033[0m")

// ARBTRIE_WARN - Orange text (33m) - For warnings that require attention but aren't fatal
#define ARBTRIE_WARN(...)                                                                          \
   arbtrie::debug(__FILE__, __func__, __LINE__, arbtrie::log_level::warn, "\033[33m", __VA_ARGS__, \
                  "\033[0m")

// ARBTRIE_ERROR - Bold Red text (1;31m) - For errors and exceptions
#define ARBTRIE_ERROR(...)                                                               \
   arbtrie::debug(__FILE__, __func__, __LINE__, arbtrie::log_level::error, "\033[1;31m", \
                  __VA_ARGS__, "\033[0m")

// ARBTRIE_FATAL - Bold Magenta text (1;35m) - For fatal errors causing termination
#define ARBTRIE_FATAL(...)                                                               \
   arbtrie::debug(__FILE__, __func__, __LINE__, arbtrie::log_level::fatal, "\033[1;35m", \
                  __VA_ARGS__, "\033[0m")

// Debug-only macros
#ifndef NDEBUG
#define ARBTRIE_SCOPE arbtrie::scope __sco__##__LINE__;
#else
#define ARBTRIE_SCOPE
#endif

   inline auto set_current_thread_name(const char* name)
   {
      thread_name(name);
#ifdef __APPLE__
      return pthread_setname_np(name);
#else
      return pthread_setname_np(pthread_self(), name);
#endif
   }

   /**
    * Log Level Environment Variable
    * ==============================
    * 
    * The ARBTRIE_LOG_LEVEL environment variable controls which log messages are displayed.
    * 
    * Setting the log level:
    * ```bash
    * # Show only warnings and errors (default in release builds)
    * export ARBTRIE_LOG_LEVEL=WARN
    * 
    * # Show all debug messages (verbose)
    * export ARBTRIE_LOG_LEVEL=TRACE
    * 
    * # Completely silent operation
    * export ARBTRIE_LOG_LEVEL=NONE
    * ```
    * 
    * Log Levels (from most to least verbose):
    * - TRACE (0): Most detailed information, helpful for tracing program execution
    * - DEBUG (1): Detailed information useful for debugging
    * - INFO (2): General information about normal operation (default in debug builds)
    * - WARN (3): Warnings that need attention but aren't fatal (default in release builds)
    * - ERROR (4): Error conditions that might allow the program to continue
    * - FATAL (5): Critical errors causing termination
    * - NONE (6): No output (silent operation)
    * 
    * You can use either the string name (e.g., "WARN") or the numeric value (e.g., 3).
    * 
    * Example usage in code:
    * ```cpp
    * ARBTRIE_TRACE("Entering function with value:", x);  // Most detailed
    * ARBTRIE_DEBUG("Calculated hash:", hash_value);      // Debugging details
    * ARBTRIE_INFO("Cache initialized with size:", size); // General information
    * ARBTRIE_WARN("Disk space below 10%");               // Warning
    * ARBTRIE_ERROR("Failed to open file:", filename);    // Error
    * ARBTRIE_FATAL("Critical memory corruption detected"); // Fatal error
    * ```
    */

}  // namespace arbtrie
