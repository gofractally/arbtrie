#pragma once
#include <array>  // For std::array
#include <atomic>
#include <cstdlib>  // For getenv
#include <format>   // C++20 formatting library
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string_view>  // C++20 string_view
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
         std::string_view level_str(env_level);
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

   // Global default thread name for efficient pointer comparison
   static inline const char* const DEFAULT_THREAD_NAME = "unset-thread-name";

   inline const char* thread_name(const char* n = nullptr)
   {
      static thread_local const char* thread_name = DEFAULT_THREAD_NAME;
      if (n)
      {
         // Set thread name without truncation
         thread_name = n;
      }
      return thread_name;
   }

   namespace detail
   {
      // RESTORED MUTEX
      static inline std::mutex& debug_mutex()
      {
         static std::mutex m;
         return m;
      }

      // Helper to extract filename from path
      inline std::string_view extract_filename(const char* path)
      {
         const char* slash = strrchr(path, '/');
         return slash ? std::string_view(slash + 1) : std::string_view(path);
      }

      // Helper to create truncated string_view
      inline std::string_view truncate(std::string_view str, size_t max_len)
      {
         return str.length() > max_len ? str.substr(0, max_len) : str;
      }

      // Track the maximum location width seen so far
      inline size_t& max_location_width()
      {
         static size_t width = 25;  // Start with default width of 25
         return width;
      }

      // Pre-computed spaces for indentation (avoids allocations)
      constexpr int MAX_INDENT = 32;  // Max reasonable indentation level
      static const std::array<std::string_view, MAX_INDENT> SPACES = []()
      {
         std::array<std::string_view, MAX_INDENT> result = {};
         static const char                        spaces[MAX_INDENT * 4 + 1] =
             "                                                                                     "
             "                                           ";
         for (int i = 0; i < MAX_INDENT; i++)
         {
            result[i] = std::string_view(spaces, i * 4);
         }
         return result;
      }();
   }  // namespace detail

   template <typename... Ts>
   void debug(const char* file, const char* func, int line, log_level level, Ts... args)
   {
      // Skip if the message's log level is below the current threshold
      if (level < get_log_level())
      {
         return;
      }

      // Extract filename as string_view (no allocation)
      std::string_view filename = detail::extract_filename(file);

      // Format location directly without allocation
      char             location_buf[64];  // Large enough for any reasonable filename:line
      int              location_len = std::snprintf(location_buf, sizeof(location_buf), "%.*s:%d",
                                                    static_cast<int>(filename.length()), filename.data(), line);
      std::string_view location(location_buf, location_len);

      // Update max width if this location is longer
      size_t& max_width = detail::max_location_width();
      if (location_len > max_width)
      {
         max_width = location_len + 1;  // Add 1 for a bit of padding
      }

      // Get thread name as string_view (no allocation)
      const char*      tname = thread_name();
      std::string_view thread_str;
      if (tname != DEFAULT_THREAD_NAME)  // Fast pointer comparison
      {
         thread_str = detail::truncate(std::string_view(tname), 8);
      }

      // Get function name as string_view (no allocation)
      std::string_view func_str = detail::truncate(std::string_view(func), 20);

      // Get indentation using pre-computed spaces
      int              indent_level = scope::indent();
      std::string_view indent = detail::SPACES[std::min(indent_level, detail::MAX_INDENT - 1)];

      // Format the header using dynamic width
      std::string header = std::format("{:<{}}  {:<9}  {:<20}  {}", location, max_width, thread_str,
                                       func_str, indent);

      // Use ostringstream again
      std::ostringstream output;
      output << header;
      ((output << std::forward<Ts>(args)), ...);
      output << "\n";

      // Use lock_guard again
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
