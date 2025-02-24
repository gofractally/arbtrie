#pragma once
#include <cstdint>
#include <iomanip>
#include <iostream>
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
         bool     is_pinned     = false;
         int64_t  age           = 0;
         uint32_t read_nodes    = 0;
         uint64_t read_bytes    = 0;
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
      uint64_t total_segments   = 0;
      uint64_t total_retained   = 0;
      uint64_t total_free_space = 0;
      uint64_t total_read_bytes = 0;
      uint32_t total_read_nodes = 0;

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

      // Print to any ostream (defaults to cout if called without arguments)
      void print(std::ostream& os = std::cout) const
      {
         os << "\n--- segment allocator state ---\n";

         // Print header
         // clang-format off
         os << std::setw(6)  << "#"      << " | "
            << std::setw(8)  << "freed %" << " | "
            << std::setw(12) << "freed bytes" << " | "
            << std::setw(12) << "freed obj"   << " | "
            << std::setw(12) << "alloc pos"   << " | "
            << std::setw(12) << "is alloc"    << " | "
            << std::setw(12) << "is pinned"   << " | "
            << std::setw(8)  << "age"         << " | "
            << std::setw(12) << "read nodes"  << " | "
            << std::setw(12) << "read bytes"  << "\n";
         // clang-format on

         // Print segment info
         for (const auto& seg : segments)
         {
            // clang-format off
            os << std::setw(6)  << seg.segment_num << " | "
               << std::setw(8)  << seg.freed_percent << " | "
               << std::setw(12) << seg.freed_bytes << " | "
               << std::setw(12) << seg.freed_objects << " | "
               << std::setw(12) << (seg.alloc_pos == -1 ? "END" : std::to_string(seg.alloc_pos)) << " | "
               << std::setw(12) << (seg.is_alloc ? "alloc" : "") << " | "
               << std::setw(12) << (seg.is_pinned ? "pin" : "") << " | "
               << std::setw(8)  << seg.age << " | "
               << std::setw(12) << seg.read_nodes << " | "
               << std::setw(12) << seg.read_bytes << "\n";
            // clang-format on
         }

         // Print totals
         os << "\ntotal free: " << total_free_space / 1024 / 1024. << "Mb  "
            << (100 * total_free_space / double(total_segments * (1ull << 30))) << "%\n";
         os << "total retained: " << total_retained << " objects\n";
         os << "total read nodes: " << total_read_nodes << "\n";
         os << "total read bytes: " << total_read_bytes / 1024 / 1024. << "Mb  "
            << (100 * total_read_bytes / double(total_segments * (1ull << 30))) << "%\n\n";

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