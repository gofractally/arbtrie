#pragma once
#include <arbtrie/config.hpp>  // Include for segment_size constant
#include <arbtrie/util.hpp>
#include <cassert>  // Add include for assert
#include <chrono>   // Add include for std::chrono
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>   // Add include for std::numeric_limits
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
      uint32_t total_non_value_nodes = 0;  // Total count of non-value nodes for average calculation
      uint32_t index_cline_counts[257] = {0};  // Histogram of cacheline hits [0-256+]
      uint32_t cline_delta_counts[257] = {
          0};  // Histogram of delta between actual and ideal cachelines

      // Cache related stats
      uint32_t cache_difficulty     = 0;  // Current cache difficulty setting
      uint64_t total_promoted_bytes = 0;  // Total bytes promoted through the cache

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

      // Helper function to format time with appropriate units (s, m, h, d)
      static std::string format_time_with_units(double seconds)
      {
         std::ostringstream result;
         result << std::fixed << std::setprecision(1);

         if (seconds < 60.0)
         {
            // Display as seconds
            result << seconds << " s";
         }
         else if (seconds < 3600.0)
         {
            // Display as minutes
            result << (seconds / 60.0) << " m";
         }
         else if (seconds < 86400.0)
         {
            // Display as hours
            result << (seconds / 3600.0) << " h";
         }
         else
         {
            // Display as days
            result << (seconds / 86400.0) << " d";
         }

         return result.str();
      }

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

      // Helper method to create a histogram using Unicode block characters
      static std::string create_histogram(const uint32_t data[257],
                                          int            width  = 257,
                                          int            height = 20)
      {
         static const std::string blocks[] = {" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};

         // Transpose the histogram to show one row per cacheline count
         // with a maximum width of 80 characters
         const int display_width     = 80;
         const int count_field_width = 10;  // Width for displaying the count at start of row
         const int graph_width = display_width - count_field_width - 3;  // Width for the actual bar

         // Find the maximum value to scale the histogram
         uint32_t max_value    = 0;
         uint64_t total_nodes  = 0;
         uint64_t weighted_sum = 0;

         for (int i = 0; i < width; ++i)
         {
            if (data[i] > max_value)
               max_value = data[i];
            total_nodes += data[i];
            weighted_sum += static_cast<uint64_t>(i) * data[i];
         }

         if (max_value == 0)
            return "No data available for histogram";

         // Calculate average cachelines per node
         double avg_cachelines =
             total_nodes > 0 ? static_cast<double>(weighted_sum) / total_nodes : 0.0;

         std::ostringstream result;
         result << "Cacheline Hits Histogram (Row = # of unique cachelines, Bar = frequency)\n";
         result << "Total non-value nodes: " << total_nodes
                << ", Average cachelines per node: " << std::fixed << std::setprecision(2)
                << avg_cachelines << "\n";
         result << std::string(display_width, '-') << "\n";

         // Draw header
         result << std::setw(count_field_width) << std::left << "Cachelines" << " │ "
                << "Count (max: " << max_value << ")\n";
         result << std::string(count_field_width, '-') << "┬"
                << std::string(display_width - count_field_width - 1, '-') << "\n";

         // Calculate scaling factor
         double scale = static_cast<double>(graph_width) / max_value;

         // Draw one row for each non-zero cacheline count
         for (int i = 0; i < width; ++i)
         {
            // Skip rows with zero count
            if (data[i] == 0)
               continue;

            // Draw the row number (cacheline count)
            result << std::setw(count_field_width) << std::right << i << " │ ";

            // Calculate bar length
            int bar_length = static_cast<int>(data[i] * scale);

            // Create bar with proper color based on value
            if (data[i] >= max_value * 0.75)
            {
               result << COLOR_GREEN;
            }
            else if (data[i] >= max_value * 0.4)
            {
               result << COLOR_YELLOW;
            }
            else
            {
               result << COLOR_RED;
            }

            // Draw the bar
            for (int j = 0; j < bar_length; ++j)
            {
               result << "█";
            }

            // Add the count at the end of the bar
            result << COLOR_RESET << " " << data[i] << "\n";
         }

         return result.str();
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

         // Define column widths for consistency
         // clang-format off
         const int seg_width = 5;        // Segment number (5 digits)
         const int pin_width = 3;        // Pin emoji
         const int prog_width = 15;      // Progress bar
         const int dot_width = 3;        // Status dot
         const int used_pct_width = 4;   // Used % - narrowed to 4 chars
         const int free_pct_width = 4;   // Free % - narrowed to 4 chars
         const int unalloc_pct_width = 4; // Unallocated % (TBA) - narrowed to 4 chars
         const int seconds_width = 7;    // Age in seconds - now 7 chars wide to fit "999.9 h"
         const int age_width = 8;        // Seq number (formerly age)
         const int total_obj_width = 10; // #Nodes
         const int read_nodes_width = 10; // Read nodes
         const int read_bytes_width = 12; // Read bytes

         // Function to create a separator row with the specified column widths
         auto create_separator = [&]() {
            return std::string(
               seg_width + 4 + prog_width + dot_width + // 4 spaces for pin column instead of pin_width 
               used_pct_width + free_pct_width + unalloc_pct_width + // Percentage columns
               seconds_width + age_width + total_obj_width + read_nodes_width + read_bytes_width + 
               // Add spaces between columns (12 columns = 11 spaces)
               11, '-');
         };
         
         // Two-row header
         os << std::left
            << std::setw(seg_width) << "Seg#"
            << "    " // Match the 4 spaces used for pin column
            << std::setw(prog_width) << "Segment" << " "
            << std::setw(dot_width) << "S" << " "
            << std::right
            << std::setw(used_pct_width) << "Used" << " " // Renamed from Used%
            << std::setw(free_pct_width) << "Free" << " " // Renamed from Free%
            << std::setw(unalloc_pct_width) << "TBA" << " " // Renamed from Unall%
            << std::setw(seconds_width) << "Age" << " " // Renamed from Seconds
            << std::setw(age_width) << "Seq" << " " // Renamed from Age
            << std::setw(total_obj_width) << "#Nodes" << " " // Renamed from NumNodes
            << std::setw(read_nodes_width) << "ReadNodes" << " "
            << std::setw(read_bytes_width) << "ReadBytes" << "\n";

         // Add horizontal rule between header and data
         os << create_separator() << "\n";
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
         double   pinned_min_age_seconds =
             std::numeric_limits<double>::max();  // Initialize to max value
         double pinned_max_age_seconds = 0.0;
         double unpinned_min_age_seconds =
             std::numeric_limits<double>::max();  // Initialize to max value
         double unpinned_max_age_seconds = 0.0;

         // Track max sequence number (age)
         uint64_t max_seq = 0;

         // Print segment info
         for (const auto& seg : segments)
         {
            std::string status_dot         = " ";  // Status indicator dot
            std::string pin_emoji          = " ";  // For the pin column
            std::string progress_bar_color = "";

            // Calculate percentages as requested
            int used_percent    = 0;  // (alloc_pos - freed_bytes) / segment_size
            int free_percent    = 0;  // freed_bytes / segment_size
            int unalloc_percent = 0;  // (segment_size - alloc_pos) / segment_size

            // Calculate the values based on alloc_pos
            uint64_t actual_used = 0;
            if (seg.alloc_pos == 0)
            {
               // Empty segment
               used_percent    = 0;
               free_percent    = 0;
               unalloc_percent = 100;
            }
            else if (seg.alloc_pos == 4294967295 || seg.alloc_pos == (uint64_t)-1)
            {
               // Full allocation, used = segment_size - freed_bytes
               actual_used     = segment_size - seg.freed_bytes;
               used_percent    = static_cast<int>((actual_used * 100) / segment_size);
               free_percent    = static_cast<int>((seg.freed_bytes * 100) / segment_size);
               unalloc_percent = 0;
            }
            else if (seg.alloc_pos == 64)
            {
               // PEND status (64 is the size of the segment header)
               used_percent    = 0;
               free_percent    = 0;
               unalloc_percent = 100;
            }
            else
            {
               // Normal case
               // Calculate used space
               actual_used =
                   (seg.alloc_pos > seg.freed_bytes) ? (seg.alloc_pos - seg.freed_bytes) : 0;
               used_percent = static_cast<int>((actual_used * 100) / segment_size);

               // Calculate free space (allocated but freed)
               free_percent = static_cast<int>((seg.freed_bytes * 100) / segment_size);

               // Calculate unallocated space
               uint64_t alloc_percent = (seg.alloc_pos * 100) / segment_size;
               unalloc_percent        = 100 - alloc_percent;
            }

            // Determine status dot based on segment state
            if (seg.alloc_pos == 0)
            {
               // Empty segment
               status_dot = " ";
            }
            else if (seg.age == 4294967295)
            {
               // Free segment - always use red dot
               status_dot = "🔴";  // Red dot for freed
            }
            else if (seg.alloc_pos == 64)
            {
               // PEND segment (already set above)
               status_dot = "🟡";  // Yellow dot for pending/recycled
            }
            else if (seg.is_alloc && seg.alloc_pos < segment_size)
            {
               // Active allocation
               status_dot = "🟢";  // Green dot for active
            }

            // Special case: handle FREE age for any segment that has this specific age,
            // even if it doesn't meet the other criteria
            if (seg.age == 4294967295)
            {
               status_dot = "🔴";  // Always use red dot for FREE age
            }

            // Add to total used space for summary
            total_used_space += actual_used;

            // Add pin emoji if segment is pinned
            if (seg.bitmap_pinned)
            {
               pin_emoji = "📌";  // Pin emoji
               bitmap_pinned_count++;
               // Use blue to indicate a pinned segment
               progress_bar_color = COLOR_BLUE;
            }

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

                  // Track min/max age for pinned segments
                  pinned_min_age_seconds = std::min(pinned_min_age_seconds, age_seconds);
                  pinned_max_age_seconds = std::max(pinned_max_age_seconds, age_seconds);
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

                  // Track min/max age for unpinned segments
                  unpinned_min_age_seconds = std::min(unpinned_min_age_seconds, age_seconds);
                  unpinned_max_age_seconds = std::max(unpinned_max_age_seconds, age_seconds);
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

            // Track max sequence number (excluding FREE age which is the max uint value)
            if (seg.age != 4294967295 && seg.age > max_seq)
               max_seq = seg.age;

            // Create progress bar with proper coloring (using the original function unchanged)
            std::string progress_bar = create_colored_progress_bar(
                seg.freed_percent, seg.alloc_pos, seg.freed_bytes, progress_bar_color);

            // Calculate time difference in seconds with 1 decimal place
            double time_diff_seconds = 0.0;
            if (seg.vage > 0)
            {
               time_diff_seconds = (current_time_ms - seg.vage) / 1000.0;
            }

            // Format time difference with 1 decimal place
            std::ostringstream time_diff_str;
            time_diff_str << std::fixed << std::setprecision(1) << time_diff_seconds;

            // Format time with appropriate units for display
            std::string formatted_time = format_time_with_units(time_diff_seconds);

            // 1. Print segment number (left-justified)
            os << std::left << std::setw(seg_width) << seg.segment_num;

            // 2. Print pin column (left-aligned fixed width)
            // Unicode emojis can cause alignment issues - use a fixed column width
            // and ensure proper padding with spaces
            if (seg.bitmap_pinned)
            {
               os << " 📌 ";  // Add extra space after for proper spacing
            }
            else
            {
               os << "    ";  // 4 spaces to match the width of the emoji plus spaces
            }

            // 3. Print progress bar (already colored with fixed width)
            os << std::setw(prog_width) << progress_bar << " ";

            // 4. Print status dot with consistent spacing
            // Unicode emojis often take up more space than expected visually
            if (status_dot == " ")
            {
               // No dot - use plain spaces
               os << "   ";  // 3 spaces for no dot
            }
            else
            {
               // Has a dot - add the dot and ensure proper alignment with extra space
               os << status_dot << " ";  // Extra space after the dot to ensure alignment
            }
            os << " ";  // Always add one space after dot position for all rows

            // Switch to right-justified for numeric columns
            os << std::right;

            // Print the percentage columns with appropriate colors
            os << COLOR_GREEN << std::setw(used_pct_width) << used_percent << COLOR_RESET << " "
               << COLOR_YELLOW << std::setw(free_pct_width) << free_percent << COLOR_RESET << " "
               << COLOR_DARK_RED << std::setw(unalloc_pct_width) << unalloc_percent << COLOR_RESET
               << " "

               // Skip printing alloc_percent since it's been removed

               // Print the remaining columns with updated widths
               << std::setw(seconds_width) << formatted_time << " " << std::setw(age_width)
               << (seg.age == 4294967295 ? "NONE" : std::to_string(seg.age)) << " "
               << std::setw(total_obj_width) << seg.total_objects << " "
               << std::setw(read_nodes_width) << seg.read_nodes << " "
               << std::setw(read_bytes_width) << seg.read_bytes;

            os << "\n";
         }

         // Print totals section with right justification for numbers
         os << std::right;
         os << "\ntotal free: " << total_free_space / 1024 / 1024. << "Mb  "
            << (100 * total_free_space / double(total_segments * (1ull << 30))) << "%\n";
         os << "total retained: " << total_retained << " objects\n";
         os << "total read nodes: " << total_read_nodes << "\n";
         os << "total read bytes: " << total_read_bytes / 1024 / 1024. << "Mb  "
            << (100 * total_read_bytes / double(total_segments * (1ull << 30))) << "%\n";
         os << "bitmap mlocked segments: " << mlocked_segments_count
            << "  (displayed: " << bitmap_pinned_count << ")\n";
         os << "metadata pinned segments: " << meta_pinned_count << "\n\n";

         // Print cacheline histogram
         os << "\n--- cacheline hits histogram ---\n";

         // Calculate total weighted sum for average
         uint64_t weighted_sum = 0;
         for (int i = 0; i < 257; ++i)
         {
            weighted_sum += static_cast<uint64_t>(i) * index_cline_counts[i];
         }

         // Calculate average cachelines per node
         double avg_cachelines = total_non_value_nodes > 0
                                     ? static_cast<double>(weighted_sum) / total_non_value_nodes
                                     : 0.0;

         os << "Total non-value nodes: " << total_non_value_nodes
            << ", Average cachelines per node: " << std::fixed << std::setprecision(2)
            << avg_cachelines << "\n";

         os << create_histogram(index_cline_counts) << "\n";

         // Calculate average delta for the delta histogram
         weighted_sum = 0;
         for (int i = 0; i < 257; ++i)
         {
            weighted_sum += static_cast<uint64_t>(i) * cline_delta_counts[i];
         }

         double avg_delta = total_non_value_nodes > 0
                                ? static_cast<double>(weighted_sum) / total_non_value_nodes
                                : 0.0;

         os << "\n--- cacheline delta from ideal histogram ---\n";
         os << "Average delta from ideal: " << std::fixed << std::setprecision(2) << avg_delta
            << " cachelines\n";

         os << create_histogram(cline_delta_counts) << "\n";

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

         // Print space usage summary with right justification for numbers
         os << "---------------------- SPACE USAGE SUMMARY ----------------------\n";

         // Use consistent column widths for better alignment
         const int label_width = 17;  // Width for labels
         const int value_width = 10;  // Width for numeric values
         const int pct_width   = 7;   // Width for percentage values

         os << std::left << std::setw(label_width) << "Total space:" << std::right
            << std::setw(value_width) << total_space / 1024 / 1024. << " MB (" << total_segments
            << " segments × " << segment_size / 1024 / 1024. << " MB)\n";

         os << std::left << std::setw(label_width) << "Total used:" << std::right
            << std::setw(value_width) << total_used_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << std::setw(pct_width) << used_percent << "% of total)\n";

         os << std::left << std::setw(label_width) << "Total unused:" << std::right
            << std::setw(value_width) << unused_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << std::setw(pct_width) << (100.0 - used_percent)
            << "% of total)\n";

         // Add pinned segment usage information
         os << "\n"
            << std::left << std::setw(label_width) << "Pinned space:" << std::right
            << std::setw(value_width) << pinned_total_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << std::setw(pct_width)
            << (pinned_total_space > 0 ? (pinned_total_space * 100.0) / total_space : 0.0)
            << "% of total)\n";

         os << std::left << std::setw(label_width) << "Pinned used:" << std::right
            << std::setw(value_width) << pinned_used_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << std::setw(pct_width) << pinned_used_percent
            << "% of pinned)\n";

         os << std::left << std::setw(label_width) << "Pinned unused:" << std::right
            << std::setw(value_width) << pinned_unused_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << std::setw(pct_width) << (100.0 - pinned_used_percent)
            << "% of pinned)\n";

         // Add unpinned segment usage information
         os << "\n"
            << std::left << std::setw(label_width) << "Unpinned space:" << std::right
            << std::setw(value_width) << unpinned_total_space / 1024 / 1024. << " MB ("
            << std::fixed << std::setprecision(2) << std::setw(pct_width)
            << (unpinned_total_space > 0 ? (unpinned_total_space * 100.0) / total_space : 0.0)
            << "% of total)\n";

         os << std::left << std::setw(label_width) << "Unpinned used:" << std::right
            << std::setw(value_width) << unpinned_used_space / 1024 / 1024. << " MB (" << std::fixed
            << std::setprecision(2) << std::setw(pct_width) << unpinned_used_percent
            << "% of unpinned)\n";

         os << std::left << std::setw(label_width) << "Unpinned unused:" << std::right
            << std::setw(value_width) << unpinned_unused_space / 1024 / 1024. << " MB ("
            << std::fixed << std::setprecision(2) << std::setw(pct_width)
            << (100.0 - unpinned_used_percent) << "% of unpinned)\n";

         // Add average age information
         os << "\n"
            << std::left << std::setw(label_width) << "Avg age pinned:" << std::right << std::fixed
            << std::setprecision(2) << std::setw(value_width) << avg_pinned_age_seconds
            << " seconds (" << pinned_segments_count << " segments)\n";

         os << std::left << std::setw(label_width) << "Avg age unpinned:" << std::right
            << std::fixed << std::setprecision(2) << std::setw(value_width)
            << avg_unpinned_age_seconds << " seconds (" << unpinned_segments_count
            << " segments)\n";

         // Add min/max age information for pinned segments (only if there are any)
         if (pinned_segments_count > 0)
         {
            os << std::left << std::setw(label_width) << "Min age pinned:" << std::right
               << std::fixed << std::setprecision(2) << std::setw(value_width)
               << pinned_min_age_seconds << " seconds ("
               << format_time_with_units(pinned_min_age_seconds) << ")\n";

            os << std::left << std::setw(label_width) << "Max age pinned:" << std::right
               << std::fixed << std::setprecision(2) << std::setw(value_width)
               << pinned_max_age_seconds << " seconds ("
               << format_time_with_units(pinned_max_age_seconds) << ")\n";
         }

         // Add min/max age information for unpinned segments (only if there are any)
         if (unpinned_segments_count > 0)
         {
            os << std::left << std::setw(label_width) << "Min age unpinned:" << std::right
               << std::fixed << std::setprecision(2) << std::setw(value_width)
               << unpinned_min_age_seconds << " seconds ("
               << format_time_with_units(unpinned_min_age_seconds) << ")\n";

            os << std::left << std::setw(label_width) << "Max age unpinned:" << std::right
               << std::fixed << std::setprecision(2) << std::setw(value_width)
               << unpinned_max_age_seconds << " seconds ("
               << format_time_with_units(unpinned_max_age_seconds) << ")\n";
         }

         // After printing average age information, add the maximum Seq info:
         os << std::left << std::setw(label_width) << "Max Seq:" << std::right << std::fixed
            << std::setprecision(0) << std::setw(value_width) << max_seq
            << " (highest sequence number)\n";

         // Add valid object statistics
         os << "\n"
            << std::left << std::setw(label_width) << "Valid objects:" << std::right
            << std::setw(value_width) << total_read_nodes << " objects ("
            << total_read_bytes / 1024 / 1024. << " MB, " << std::fixed << std::setprecision(2)
            << (total_read_bytes * 100.0) / total_space << "% of total space)\n";

         // Add cache stats
         if (cache_difficulty > 0)
         {
            // Calculate cache probability as "1 in N attempts"
            uint64_t max_uint32  = 0xFFFFFFFFULL;
            double   probability = 1.0 - (static_cast<double>(cache_difficulty) / max_uint32);
            uint64_t attempts_per_hit =
                probability > 0 ? std::round(1.0 / probability) : max_uint32;

            os << "\n"
               << std::left << std::setw(label_width) << "Cache difficulty:" << std::right
               << std::setw(value_width) << cache_difficulty << " (1 in " << attempts_per_hit
               << " attempts)\n";

            os << std::left << std::setw(label_width) << "Promoted bytes:" << std::right
               << std::setw(value_width) << total_promoted_bytes / 1024 / 1024.
               << " MB (total since startup)\n";
         }

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