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

namespace sal
{
   /**
    * Log Level Enumeration
    * Used to control which messages are displayed based on their severity.
    * Can be set via the SAL_LOG_LEVEL environment variable.
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
    * Get the current log level from the environment variable SAL_LOG_LEVEL
    * 
    * Environment variable options:
    * SAL_LOG_LEVEL=TRACE (or 0) - Show all messages
    * SAL_LOG_LEVEL=DEBUG (or 1) - Show debug and above
    * SAL_LOG_LEVEL=INFO  (or 2) - Show info and above
    * SAL_LOG_LEVEL=WARN  (or 3) - Show warnings and errors only
    * SAL_LOG_LEVEL=ERROR (or 4) - Show only errors
    * SAL_LOG_LEVEL=FATAL (or 5) - Show only fatal errors
    * SAL_LOG_LEVEL=NONE  (or 6) - Silent operation
    * 
    * If not set, defaults to INFO in debug builds and WARN in release builds.
    */
   inline log_level get_log_level()
   {
      static log_level level = []
      {
         const char* env_level = std::getenv("SAL_LOG_LEVEL");
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
    */
   static constexpr bool debug_cache = false;

   /**
    * Enables debug logging for memory operations like mmap/mlock.
    */
   static constexpr bool debug_memory = true;

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

      // Color codes for different log levels
      inline constexpr const char* COLOR_TRACE = "\033[37m";    // Gray
      inline constexpr const char* COLOR_INFO  = "\033[36m";    // Cyan
      inline constexpr const char* COLOR_WARN  = "\033[33m";    // Orange
      inline constexpr const char* COLOR_ERROR = "\033[1;31m";  // Bold Red
      inline constexpr const char* COLOR_FATAL = "\033[1;35m";  // Bold Magenta
      inline constexpr const char* COLOR_RESET = "\033[0m";     // Reset to default
   }                                                            // namespace detail

   /**
    * Format and output a log message with std::format-style formatting.
    *
    * @param file The source file generating the log message
    * @param func The function generating the log message
    * @param line The line number in the source file
    * @param level The severity level of the message
    * @param fmt Format string compatible with std::format
    * @param args Arguments to be formatted according to the format string
    */
   template <typename... Args>
   void debug_fmt(const char*      file,
                  const char*      func,
                  int              line,
                  log_level        level,
                  std::string_view fmt,
                  Args&&... args)
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

      // Format header with snprintf
      char header_buf[128];
      int  header_len = std::snprintf(header_buf, sizeof(header_buf), "%-25s  %-9s  %-20s  %.*s",
                                      location.data(), thread_str.data(), func_str.data(),
                                      static_cast<int>(indent.length()), indent.data());
      std::string_view header(header_buf, header_len);

      // Add color based on log level
      const char* color_prefix = "";
      const char* color_suffix = "";

      if (level == log_level::trace)
      {
         color_prefix = detail::COLOR_TRACE;
         color_suffix = detail::COLOR_RESET;
      }
      else if (level == log_level::info)
      {
         color_prefix = detail::COLOR_INFO;
         color_suffix = detail::COLOR_RESET;
      }
      else if (level == log_level::warn)
      {
         color_prefix = detail::COLOR_WARN;
         color_suffix = detail::COLOR_RESET;
      }
      else if (level == log_level::error)
      {
         color_prefix = detail::COLOR_ERROR;
         color_suffix = detail::COLOR_RESET;
      }
      else if (level == log_level::fatal)
      {
         color_prefix = detail::COLOR_FATAL;
         color_suffix = detail::COLOR_RESET;
      }

      // Format the message with ostringstream
      std::ostringstream oss;

      // Simple string replacement for format placeholders
      if constexpr (sizeof...(args) > 0)
      {
         size_t pos         = 0;
         size_t count       = 0;
         auto   process_arg = [&](auto&& arg)
         {
            size_t next_brace = fmt.find('{', pos);
            if (next_brace != std::string_view::npos)
            {
               // Output text before the brace
               oss.write(fmt.data() + pos, next_brace - pos);

               // Skip the format specifier
               size_t close_brace = fmt.find('}', next_brace);
               if (close_brace != std::string_view::npos)
               {
                  // Output the argument
                  oss << arg;
                  // Update position past this format specifier
                  pos = close_brace + 1;
               }
               else
               {
                  // No closing brace, just output the opening brace and continue
                  oss << '{';
                  pos = next_brace + 1;
               }
            }
         };

         ((process_arg(std::forward<Args>(args)), count++), ...);

         // Output any remaining text after the last placeholder
         if (pos < fmt.size())
         {
            oss.write(fmt.data() + pos, fmt.size() - pos);
         }

         // If there were more args than placeholders, append them
         if (count < sizeof...(args))
         {
            oss << " ";
            // Function to append remaining args - fix the ternary operator issue
            size_t i                = 0;
            auto   append_remaining = [&](auto&& arg)
            {
               if (i >= count)
               {
                  oss << " " << arg;
               }
               ++i;
            };
            (append_remaining(std::forward<Args>(args)), ...);
         }
      }
      else
      {
         oss << fmt;
      }

      // Finalize the output
      std::lock_guard<std::mutex> lock(detail::debug_mutex());
      std::cerr << header << color_prefix << oss.str() << color_suffix << std::endl;
   }

   // For backward compatibility
   template <typename... Ts>
   void debug(const char* file, const char* func, int line, log_level level, Ts... args)
   {
      // Convert the variadic arguments to a string
      std::ostringstream output;
      ((output << std::forward<Ts>(args)), ...);
      debug_fmt(file, func, line, level, output.str());
   }

// Define the log macros - adjusted to avoid redefinition problems
#ifndef SAL_TRACE_DEFINED
#define SAL_TRACE_DEFINED
#define SAL_TRACE(fmt, ...) \
   sal::debug_fmt(__FILE__, __func__, __LINE__, sal::log_level::trace, fmt, ##__VA_ARGS__)
#endif

#ifndef SAL_DEBUG_DEFINED
#define SAL_DEBUG_DEFINED
#define sal_debug(fmt, ...) \
   sal::debug_fmt(__FILE__, __func__, __LINE__, sal::log_level::debug, fmt, ##__VA_ARGS__)
#endif

#ifndef SAL_INFO_DEFINED
#define SAL_INFO_DEFINED
#define SAL_INFO(fmt, ...) \
   sal::debug_fmt(__FILE__, __func__, __LINE__, sal::log_level::info, fmt, ##__VA_ARGS__)
#endif

#ifndef SAL_WARN_DEFINED
#define SAL_WARN_DEFINED
#define SAL_WARN(fmt, ...) \
   sal::debug_fmt(__FILE__, __func__, __LINE__, sal::log_level::warn, fmt, ##__VA_ARGS__)
#endif

#ifndef SAL_ERROR_DEFINED
#define SAL_ERROR_DEFINED
#define SAL_ERROR(fmt, ...) \
   sal::debug_fmt(__FILE__, __func__, __LINE__, sal::log_level::error, fmt, ##__VA_ARGS__)
#endif

#ifndef SAL_FATAL_DEFINED
#define SAL_FATAL_DEFINED
#define SAL_FATAL(fmt, ...) \
   sal::debug_fmt(__FILE__, __func__, __LINE__, sal::log_level::fatal, fmt, ##__VA_ARGS__)
#endif

// Debug-only macros
#ifndef NDEBUG
#define SAL_SCOPE sal::scope __sco__##__LINE__;
#else
#define SAL_SCOPE
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
    * The SAL_LOG_LEVEL environment variable controls which log messages are displayed.
    * 
    * Setting the log level:
    * ```bash
    * # Show only warnings and errors (default in release builds)
    * export SAL_LOG_LEVEL=WARN
    * 
    * # Show all debug messages (verbose)
    * export SAL_LOG_LEVEL=TRACE
    * 
    * # Completely silent operation
    * export SAL_LOG_LEVEL=NONE
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
    * The new debug macros use std::format style for more powerful formatting:
    * 
    * ```cpp
    * SAL_TRACE("Entering function with value: {}", x);
    * SAL_DEBUG("Calculated hash: {:x}", hash_value);  // Format as hex
    * SAL_INFO("Cache initialized with size: {} KB", size / 1024);
    * SAL_WARN("Disk space below {}%", percent);
    * SAL_ERROR("Failed to open file: {}", filename);
    * SAL_FATAL("Critical memory corruption detected in block {:#x}", block_addr);
    * ```
    */

}  // namespace sal