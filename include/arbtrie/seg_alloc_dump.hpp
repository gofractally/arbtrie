#pragma once
#include <arbtrie/config.hpp>  // Include for segment_size constant
#include <cassert>             // Add include for assert
#include <chrono>              // Add include for std::chrono
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>  // Add include for std::ostringstream
#include <string>
#include <vector>

namespace arbtrie
{

   struct seg_alloc_dump
   {
      struct segment_info
      {
         uint32_t segment_num   = 0;
         uint32_t freed_percent = 0;
         uint64_t freed_bytes   = 0;
         uint64_t freed_objects = 0;
         int64_t  alloc_pos     = 0;
         bool     is_alloc      = false;
         bool     is_pinned     = false;  // From segment metadata
         bool     bitmap_pinned = false;  // From mlock_segments bitmap
         int64_t  age           = 0;
         uint32_t read_nodes    = 0;  // Count of valid objects in segment
         uint64_t read_bytes    = 0;  // Total size of valid objects
         uint64_t vage          = 0;  // Virtual age of the segment
         uint32_t total_objects = 0;  // Total count of all objects in segment
      };

      struct session_info
      {
         uint32_t session_num = 0;
         uint32_t read_ptr    = 0;
         bool     is_locked   = true;
      };

      struct pending_segment
      {
         uint64_t index       = 0;
         uint32_t segment_num = 0;
      };

      // Overall stats
      uint64_t total_segments         = 0;
      uint64_t total_retained         = 0;
      uint64_t total_free_space       = 0;
      uint64_t total_read_bytes       = 0;  // Total bytes of valid objects across all segments
      uint32_t total_read_nodes       = 0;  // Total count of valid objects across all segments
      uint32_t mlocked_segments_count = 0;  // Count of segments in the mlock_segments bitmap

      // Segment queue state
      uint64_t alloc_ptr       = 0;
      uint64_t end_ptr         = 0;
      uint32_t active_sessions = 0;

      // Free release count
      int64_t free_release_count = 0;

      // Detailed info per component
      std::vector<segment_info>    segments;
      std::vector<session_info>    sessions;
      std::vector<pending_segment> pending_segments;

      // ANSI terminal color codes
      static constexpr const char* COLOR_RESET    = "\033[0m";
      static constexpr const char* COLOR_GREEN    = "\033[32m";
      static constexpr const char* COLOR_YELLOW   = "\033[33m";
      static constexpr const char* COLOR_RED      = "\033[31m";
      static constexpr const char* COLOR_BLUE     = "\033[34m";
      static constexpr const char* COLOR_DARK_RED = "\033[31;2m";  // Dark red for unallocated space

      // Bold versions of green and yellow for pinned segments
      static constexpr const char* COLOR_DARK_GREEN =
          "\033[32;1m";  // Bold green (more vibrant) for pinned segments
      static constexpr const char* COLOR_DARK_YELLOW =
          "\033[33;1m";  // Bold yellow (more vibrant) for pinned segments

      // Keeping these for backward compatibility
      static constexpr const char* COLOR_PINNED_GREEN  = "\033[36m";  // Cyan - not used anymore
      static constexpr const char* COLOR_PINNED_YELLOW = "\033[35m";  // Magenta - not used anymore
      static constexpr const char* COLOR_BOLD_GREEN    = "\033[32;1m";
      static constexpr const char* COLOR_BOLD_YELLOW   = "\033[33;1m";
      static constexpr const char* COLOR_BRIGHT_GREEN  = "\033[92m";
      static constexpr const char* COLOR_BRIGHT_YELLOW = "\033[93m";

      // Helper method to create a colored progress bar with Unicode block characters
      static std::string create_colored_progress_bar(uint32_t           freed_percent,
                                                     uint64_t           alloc_pos,
                                                     uint64_t           freed_bytes,
                                                     const std::string& color,
                                                     int                width = 15)
      {
         // Define the block characters as string constants
         static const std::string FULL_BLOCK  = "█";  // Full block for used space
         static const std::string MED_BLOCK   = "▒";  // Medium shade for freed space
         static const std::string LIGHT_BLOCK = "░";  // Light shade for unallocated space

         // Handle special cases for alloc_pos
         uint64_t actual_used;
         uint32_t used_percent;
         bool     is_special_case = false;

         if (alloc_pos == 0)
         {
            // Empty segment
            actual_used     = 0;
            used_percent    = 0;
            is_special_case = true;
         }
         else if (alloc_pos == 4294967295 || alloc_pos == (uint64_t)-1)
         {
            // Interpret this special value as segment_size with freed bytes subtracted
            actual_used     = segment_size - freed_bytes;
            used_percent    = static_cast<uint32_t>((actual_used * 100) / segment_size);
            is_special_case = true;
         }
         else if (alloc_pos == 64)
         {
            // PEND status (64 is the size of the segment header)
            actual_used     = 0;
            used_percent    = 0;
            is_special_case = true;
         }
         else
         {
            // Normal case - calculate actual used space
            // Avoid underflow if freed_bytes > alloc_pos
            actual_used  = (alloc_pos > freed_bytes) ? (alloc_pos - freed_bytes) : 0;
            used_percent = static_cast<uint32_t>((actual_used * 100) / segment_size);
         }

         // Calculate the alloc position percentage (how far the allocator has advanced)
         uint32_t alloc_percent = 0;
         if (!is_special_case)
         {
            alloc_percent = static_cast<uint32_t>((alloc_pos * 100) / segment_size);
         }
         else if (alloc_pos == 4294967295 || alloc_pos == (uint64_t)-1)
         {
            alloc_percent = 100;  // Full allocation
         }

         // Determine if this is a pinned segment (blue color indicates pinned)
         bool is_pinned = (color == COLOR_BLUE);

         // Define colors based on pinned status - staying within the green/yellow scale
         const char* used_color  = is_pinned ? COLOR_DARK_GREEN : COLOR_GREEN;
         const char* freed_color = is_pinned ? COLOR_DARK_YELLOW : COLOR_YELLOW;

         // Precisely calculate how many characters of each type to show
         int used_chars  = (used_percent * width + 50) / 100;   // Round to nearest
         int alloc_chars = (alloc_percent * width + 50) / 100;  // Round to nearest

         // Ensure values are in valid range
         used_chars  = std::min(std::max(used_chars, 0), width);
         alloc_chars = std::min(std::max(alloc_chars, 0), width);

         // Ensure used doesn't exceed allocated
         used_chars = std::min(used_chars, alloc_chars);

         // Calculate freed space (space that was allocated but is now freed)
         int freed_chars = alloc_chars - used_chars;

         // Calculate unallocated space
         int unalloc_chars = width - alloc_chars;

         // Assemble the progress bar with exact character counts
         std::string result;

         // 1. Add used space (green blocks)
         if (used_chars > 0)
         {
            std::string used_part;
            for (int i = 0; i < used_chars; i++)
            {
               used_part += FULL_BLOCK;
            }
            result += used_color + used_part + COLOR_RESET;
         }

         // 2. Add allocated but freed space (yellow blocks)
         if (freed_chars > 0)
         {
            std::string freed_part;
            for (int i = 0; i < freed_chars; i++)
            {
               freed_part += MED_BLOCK;
            }
            result += freed_color + freed_part + COLOR_RESET;
         }

         // 3. Add unallocated space (dark red blocks)
         if (unalloc_chars > 0)
         {
            std::string unalloc_part;
            for (int i = 0; i < unalloc_chars; i++)
            {
               unalloc_part += LIGHT_BLOCK;
            }
            result += COLOR_DARK_RED + unalloc_part + COLOR_RESET;
         }

         // Sanity check: ensure the visible length is exactly the requested width
         assert(used_chars + freed_chars + unalloc_chars == width);

         return result;
      }

      /**
       * REMOVED: object statistics collection methods
       * This functionality should be implemented in seg_allocator::dump()
       * using the compactor's session object to get the read lock.
       * 
       * The implementation should:
       * 1. Use compactor session's read_lock to access segments
       * 2. For each segment, iterate through all objects as in compact_segment
       * 3. For each object, get obj_ref using read_lock::get(obj->id())
       * 4. Check if object is valid and read (obj_ref.valid() && obj_ref.is_read())
       * 5. If valid, increment read_nodes and read_bytes for that segment
       * 6. Accumulate the total counts across all segments
       */

      // Print to any ostream (defaults to cout if called without arguments)
      void print(std::ostream& os = std::cout) const
      {
         // Get current time in milliseconds since epoch using arbtrie::get_current_time_ms()
         auto current_time_ms = arbtrie::get_current_time_ms();

         os << "\n--- segment allocator state ---\n";

         // Print header
         // clang-format off
         os << std::left
            << std::setw(5)  << "Seg#" << " "   // Add space after segment number
            << std::setw(12) << "% Used" << " " // Add space after progress bar
            << std::setw(2)  << "S" << " "      // Status dot column with space
            << std::setw(2)  << "P" << " "      // Pin column with space
            << std::setw(10) << "% Alloc" << " " // Percentage allocation column
            << std::setw(8)  << "Age" << " "
            << std::setw(10) << "Seconds" << " " // Time since vage column
            << std::setw(12) << "Read Nodes" << " " // Valid objects count
            << std::setw(8) << "Total Obj" << " " // Total objects count
            << std::setw(12) << "Read Bytes" << " " // Valid objects total size
            << std::setw(10) << "% Free" << " "     // Free percentage column
            << std::setw(12) << "Alloc Pos" << " "  // Raw alloc_pos value
            << std::setw(10) << "% Used" << "\n";   // New calculated used percentage column
         // clang-format on

         // Count segments pinned according to different sources
         int meta_pinned_count   = 0;
         int bitmap_pinned_count = 0;

         // Track total used space for summary
         uint64_t total_used_space = 0;

         // Track pinned segment usage statistics
         uint64_t pinned_total_space = 0;
         uint64_t pinned_used_space  = 0;

         // Track age statistics for pinned and unpinned segments
         uint64_t pinned_segments_count      = 0;
         uint64_t unpinned_segments_count    = 0;
         double   pinned_total_age_seconds   = 0.0;
         double   unpinned_total_age_seconds = 0.0;

         // Print segment info
         for (const auto& seg : segments)
         {
            std::string status_text        = "";   // Status text (percentage)
            std::string status_dot         = " ";  // Status indicator dot
            std::string pin_emoji          = " ";  // For the thin pin column
            std::string progress_bar_color = "";
            std::string status_text_color  = "";
            std::string status_dot_color   = "";

            // Determine status and color
            if (seg.alloc_pos == 0)
            {
               status_text = "EMPTY";  // Text for empty/unused segments
               // No colored dot for this special case
            }
            else if (seg.alloc_pos == 4294967295 || seg.alloc_pos == (uint64_t)-1)
            {
               // Calculate the actual used percentage
               uint64_t actual_used  = segment_size - seg.freed_bytes;
               int      used_percent = static_cast<int>((actual_used * 100) / segment_size);

               // Show actual used percentage instead of static "FULL" text
               status_text       = std::to_string(used_percent) + "%";
               status_text_color = COLOR_GREEN;
               // No colored dot for this special case
            }
            else if (seg.alloc_pos == 64)
            {
               status_text       = "PEND";  // Text for pending/recycled
               status_text_color = COLOR_YELLOW;
               status_dot        = "🟡";  // Yellow dot for pending/recycled
               status_dot_color  = COLOR_YELLOW;
            }
            else if (seg.is_alloc && seg.alloc_pos < segment_size)
            {
               // Calculate percentage of segment used (alloc_pos relative to segment size)
               int used_percent  = static_cast<int>((seg.alloc_pos * 100) / segment_size);
               status_text       = std::to_string(used_percent) + "%";  // Just percentage
               status_text_color = COLOR_GREEN;  // Green text for active allocations
               status_dot        = "🟢";         // Green dot for active
               status_dot_color  = COLOR_GREEN;
            }

            // Check for age 4294967295 - this takes precedence for status color
            if (seg.age == 4294967295)
            {
               status_text       = "FREE";  // Text for free/inactive segments
               status_text_color = COLOR_RED;
               status_dot        = "🔴";  // Red dot for freed
               status_dot_color  = COLOR_RED;
            }

            // Add pin emoji to dedicated pin column if segment is pinned (in bitmap)
            if (seg.bitmap_pinned)
            {
               pin_emoji = "📌";  // Pin emoji in dedicated column
               bitmap_pinned_count++;

               // Use blue as marker that this is a pinned segment (will be translated to bright colors)
               progress_bar_color = COLOR_BLUE;
            }

            // Calculate correct used space and percentage
            uint64_t actual_used;
            int      used_percent;

            if (seg.alloc_pos == 0)
            {
               actual_used  = 0;
               used_percent = 0;
            }
            else if (seg.alloc_pos == 4294967295 || seg.alloc_pos == (uint64_t)-1)
            {
               // For completed allocation, used = segment_size - freed_bytes
               actual_used  = segment_size - seg.freed_bytes;
               used_percent = static_cast<int>((actual_used * 100) / segment_size);
            }
            else if (seg.alloc_pos == 64)
            {
               actual_used  = 0;
               used_percent = 0;
            }
            else
            {
               // Calculate used space as: alloc_pos - freed_bytes (avoiding underflow)
               uint64_t unused =
                   (seg.alloc_pos > seg.freed_bytes) ? (seg.alloc_pos - seg.freed_bytes) : 0;
               actual_used  = segment_size - unused;
               used_percent = static_cast<int>((actual_used * 100) / segment_size);
            }

            // Add to total used space for summary
            total_used_space += actual_used;

            // Update pinned segment statistics if applicable
            if (seg.bitmap_pinned || seg.is_pinned)
            {
               pinned_total_space += segment_size;
               pinned_used_space += actual_used;

               // Track age for pinned segments
               if (seg.vage > 0)
               {
                  double age_seconds = (current_time_ms - seg.vage) / 1000.0;
                  pinned_total_age_seconds += age_seconds;
                  pinned_segments_count++;
               }
            }
            else
            {
               // Track age for unpinned segments
               if (seg.vage > 0)
               {
                  double age_seconds = (current_time_ms - seg.vage) / 1000.0;
                  unpinned_total_age_seconds += age_seconds;
                  unpinned_segments_count++;
               }
            }

            // Count pinned segments for stats
            if (seg.bitmap_pinned && seg.is_pinned)
            {
               meta_pinned_count++;
            }
            else if (seg.is_pinned)
            {
               meta_pinned_count++;
            }
            else if (seg.bitmap_pinned)
            {
               // Only count in bitmap_pinned_count
            }

            // Create progress bar with color applied only to the used portion, using the USED percentage for display
            std::string progress_bar = create_colored_progress_bar(
                seg.freed_percent, seg.alloc_pos, seg.freed_bytes, progress_bar_color);

            // Calculate time difference in seconds with 1 decimal place precision
            double time_diff_seconds = 0.0;
            if (seg.vage > 0)
            {
               time_diff_seconds = (current_time_ms - seg.vage) / 1000.0;
            }

            // Format time difference with 1 decimal place
            std::ostringstream time_diff_str;
            time_diff_str << std::fixed << std::setprecision(1) << time_diff_seconds;

            // Print segment number
            os << std::left << std::setw(5) << seg.segment_num << " ";

            // Print progress bar (already colored)
            os << std::setw(12) << progress_bar << " ";

            // Print status dot with color
            os << status_dot_color << std::setw(2) << status_dot << COLOR_RESET << " ";

            // Print pin emoji in thin column
            os << std::setw(2) << pin_emoji << " ";

            // Print percentage/status with color
            os << status_text_color << std::setw(10) << status_text << COLOR_RESET << " ";

            // Print the rest of the row
            os << std::setw(8) << (seg.age == 4294967295 ? "NONE" : std::to_string(seg.age)) << " "
               << std::setw(10) << time_diff_str.str() << " "  // Time difference column
               << std::setw(12) << seg.read_nodes << " " << std::setw(8) << seg.total_objects
               << " "  // Total objects count
               << std::setw(12) << seg.read_bytes << " " << std::setw(10) << seg.freed_percent
               << "%"
               << " "  // Add freed percentage column
               << std::setw(12)
               << (seg.alloc_pos == 4294967295 || seg.alloc_pos == (uint64_t)-1
                       ? "END"
                       : std::to_string(seg.alloc_pos))
               << " " << std::setw(10) << used_percent << "%";  // Add calculated used percentage

            os << "\n";
         }

         // Print totals
         os << "\ntotal free: " << total_free_space / 1024 / 1024. << "Mb  "
            << (100 * total_free_space / double(total_segments * (1ull << 30))) << "%\n";
         os << "total retained: " << total_retained << " objects\n";
         os << "total read nodes: " << total_read_nodes << "\n";
         os << "total read bytes: " << total_read_bytes / 1024 / 1024. << "Mb  "
            << (100 * total_read_bytes / double(total_segments * (1ull << 30))) << "%\n";
         os << "bitmap mlocked segments: " << mlocked_segments_count
            << "  (displayed: " << bitmap_pinned_count << ")\n";
         os << "metadata pinned segments: " << meta_pinned_count << "\n\n";

         // Calculate total space and unused space
         uint64_t total_space  = total_segments * segment_size;
         uint64_t unused_space = total_space - total_used_space;
         double   used_percent = (total_used_space * 100.0) / total_space;

         // Calculate pinned space usage
         uint64_t pinned_unused_space = pinned_total_space - pinned_used_space;
         double   pinned_used_percent =
             pinned_total_space > 0 ? (pinned_used_space * 100.0) / pinned_total_space : 0.0;

         // Calculate unpinned space usage
         uint64_t unpinned_total_space  = total_space - pinned_total_space;
         uint64_t unpinned_used_space   = total_used_space - pinned_used_space;
         uint64_t unpinned_unused_space = unpinned_total_space - unpinned_used_space;
         double   unpinned_used_percent =
             unpinned_total_space > 0 ? (unpinned_used_space * 100.0) / unpinned_total_space : 0.0;

         // Calculate average age for pinned and unpinned segments
         double avg_pinned_age_seconds =
             pinned_segments_count > 0 ? pinned_total_age_seconds / pinned_segments_count : 0.0;
         double avg_unpinned_age_seconds =
             unpinned_segments_count > 0 ? unpinned_total_age_seconds / unpinned_segments_count
                                         : 0.0;

         // Print space usage summary
         os << "---------------------- SPACE USAGE SUMMARY ----------------------\n";
         os << "Total space:     " << total_space / 1024 / 1024. << " MB (" << total_segments
            << " segments × " << segment_size / 1024 / 1024. << " MB)\n";
         os << "Total used:      " << total_used_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << used_percent << "% of total)\n";
         os << "Total unused:    " << unused_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << (100.0 - used_percent) << "% of total)\n";

         // Add pinned segment usage information
         os << "\nPinned space:    " << pinned_total_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2)
            << (pinned_total_space > 0 ? (pinned_total_space * 100.0) / total_space : 0.0)
            << "% of total)\n";
         os << "Pinned used:     " << pinned_used_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << pinned_used_percent << "% of pinned)\n";
         os << "Pinned unused:   " << pinned_unused_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << (100.0 - pinned_used_percent) << "% of pinned)\n";

         // Add unpinned segment usage information
         os << "\nUnpinned space:  " << unpinned_total_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2)
            << (unpinned_total_space > 0 ? (unpinned_total_space * 100.0) / total_space : 0.0)
            << "% of total)\n";
         os << "Unpinned used:   " << unpinned_used_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << unpinned_used_percent << "% of unpinned)\n";
         os << "Unpinned unused: " << unpinned_unused_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << (100.0 - unpinned_used_percent) << "% of unpinned)\n";

         // Add average age information
         os << "\nAvg age pinned:   " << std::fixed << std::setprecision(2)
            << avg_pinned_age_seconds << " seconds (" << pinned_segments_count << " segments)\n";
         os << "Avg age unpinned: " << std::fixed << std::setprecision(2)
            << avg_unpinned_age_seconds << " seconds (" << unpinned_segments_count
            << " segments)\n";

         // Add valid object statistics
         os << "\nValid objects:   " << total_read_nodes << " objects ("
            << total_read_bytes / 1024 / 1024. << " MB, " << std::fixed << std::setprecision(2)
            << (total_read_bytes * 100.0) / total_space << "% of total space)\n";

         os << "----------------------------------------------------------------\n\n";

         // Print segment queue state
         os << "---- free segment Q ------\n";
         os << "[---A---R*---E------]\n";
         os << "A - alloc idx: " << alloc_ptr << "\n";

         // Print session info
         for (const auto& session : sessions)
         {
            if (session.is_locked)
            {
               os << "R" << session.session_num << ": " << session.read_ptr << "\n";
            }
         }

         os << "E - end idx: " << end_ptr << "\n\n";
         os << "active sessions: " << active_sessions << "\n";

         // Print unlocked sessions
         for (const auto& session : sessions)
         {
            if (!session.is_locked)
            {
               os << "R" << session.session_num << ": UNLOCKED\n";
            }
         }

         // Print pending segments
         os << "\n------- pending free segments -----------\n";
         for (const auto& pending : pending_segments)
         {
            os << pending.index << "] " << pending.segment_num << "\n";
         }
         os << "--------------------------\n";
         os << "free release +/- = " << free_release_count << "\n";
      }

      // ostream operator now just calls print()
      friend std::ostream& operator<<(std::ostream& os, const seg_alloc_dump& dump)
      {
         dump.print(os);
         return os;
      }
   };

}  // namespace arbtrie