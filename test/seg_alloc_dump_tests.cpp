#include <arbtrie/seg_alloc_dump.hpp>
#include <arbtrie/util.hpp>
#include <catch2/catch_test_macros.hpp>
#include <sstream>

namespace arbtrie::test
{

   TEST_CASE("seg_alloc_dump formatting", "[seg_alloc]")
   {
      seg_alloc_dump dump;

      // Assume segment_size is 32MB (33554432 bytes)
      const uint64_t test_segment_size = 33554432;

      // Setup basic stats
      dump.total_segments         = 10;
      dump.total_retained         = 100;
      dump.total_free_space       = test_segment_size * 5;  // 50% of total space is free
      dump.total_read_bytes       = test_segment_size * 2;  // 20% of space has valid objects
      dump.total_read_nodes       = 50000;
      dump.mlocked_segments_count = 3;

      // Set segment queue state
      dump.alloc_ptr          = 5;
      dump.end_ptr            = 8;
      dump.active_sessions    = 2;
      dump.free_release_count = 12;

      // Create 10 segments with various states
      for (int i = 0; i < 10; i++)
      {
         seg_alloc_dump::segment_info seg;
         seg.segment_num = i;

         // Add different segment states
         switch (i)
         {
            case 0:                    // Empty segment - no dot, no pin
               seg.alloc_pos     = 0;  // Not allocated at all
               seg.freed_bytes   = 0;
               seg.freed_percent = 0;
               seg.is_alloc      = false;
               seg.age           = 10;
               break;

            case 1:                             // Full segment (FULL) - no dot, no pin
               seg.alloc_pos     = 4294967295;  // uint32_t(-1) signals fully allocated
               seg.freed_bytes   = 6710886;     // 20% of segment freed
               seg.freed_percent = 20;
               seg.is_alloc      = true;
               seg.age           = 20;
               seg.read_nodes    = 10000;
               seg.read_bytes    = 26843545;  // 80% of segment size
               seg.total_objects = 12000;
               break;

            case 2:                     // Pending segment (PEND) - yellow dot, no pin
               seg.alloc_pos     = 64;  // PEND status (header only)
               seg.freed_bytes   = 0;
               seg.freed_percent = 0;
               seg.is_alloc      = false;
               seg.age           = 5;
               break;

            case 3:                    // Free segment - red dot, no pin
               seg.alloc_pos     = 0;  // Not allocated
               seg.freed_bytes   = 0;
               seg.freed_percent = 0;
               seg.is_alloc      = false;
               seg.age           = 4294967295;  // FREE age
               break;

            case 4:  // Active segment - green dot, no pin (25% allocated)
               seg.alloc_pos     = test_segment_size / 4;   // 25% of segment allocated
               seg.freed_bytes   = test_segment_size / 20;  // 5% of segment freed
               seg.freed_percent = 5;
               seg.is_alloc      = true;  // Active segment
               seg.age           = 15;
               seg.read_nodes    = 5000;
               seg.read_bytes    = test_segment_size / 5;  // 20% of segment has valid objects
               seg.total_objects = 6000;
               // Add virtual age (30 seconds ago)
               seg.vage = arbtrie::get_current_time_ms() - 30000;
               break;

            case 5:  // Pinned segment with green dot - both bitmap and metadata (75% allocated)
               seg.alloc_pos     = test_segment_size * 3 / 4;  // 75% allocated
               seg.freed_bytes   = test_segment_size / 5;      // 20% of segment freed
               seg.freed_percent = 20;
               seg.is_alloc      = true;  // Active segment
               seg.bitmap_pinned = true;  // Should get pin
               seg.is_pinned     = true;  // Both types of pins
               seg.age           = 8;
               seg.read_nodes    = 15000;
               seg.read_bytes = test_segment_size * 55 / 100;  // 55% of segment has valid objects
               seg.total_objects = 18000;
               // Set a virtual age (100 seconds ago)
               seg.vage = arbtrie::get_current_time_ms() - 100000;
               break;

            case 6:                    // Pinned segment with no dot (0% allocation)
               seg.alloc_pos     = 0;  // Empty
               seg.freed_bytes   = 0;
               seg.freed_percent = 0;
               seg.is_alloc      = false;  // No dot
               seg.bitmap_pinned = true;   // Get pin
               seg.age           = 30;
               // Set this as the minimum age for pinned segments (50 seconds ago)
               seg.vage = arbtrie::get_current_time_ms() - 50000;
               break;

            case 7:                     // Pinned segment with yellow dot (PEND)
               seg.alloc_pos     = 64;  // PEND status
               seg.freed_bytes   = 0;
               seg.freed_percent = 0;
               seg.is_alloc      = false;
               seg.bitmap_pinned = true;  // Get pin
               seg.age           = 25;
               // Set a virtual age (75 seconds ago)
               seg.vage = arbtrie::get_current_time_ms() - 75000;
               break;

            case 8:                    // Pinned segment with red dot (FREE)
               seg.alloc_pos     = 0;  // Not allocated (freed)
               seg.freed_bytes   = 0;
               seg.freed_percent = 0;
               seg.is_alloc      = false;
               seg.is_pinned     = true;        // Metadata pinned
               seg.age           = 4294967295;  // FREE age - should get red dot
               seg.read_nodes    = 0;
               seg.read_bytes    = 0;
               seg.total_objects = 0;
               // Set a virtual age (125 seconds ago)
               seg.vage = arbtrie::get_current_time_ms() - 125000;
               break;

            case 9:  // Metadata only pinned with green dot (90% allocated)
               seg.alloc_pos     = test_segment_size * 9 / 10;  // 90% allocated
               seg.freed_bytes   = test_segment_size / 10;      // 10% freed
               seg.freed_percent = 10;
               seg.is_alloc      = true;   // Active segment
               seg.is_pinned     = true;   // Only metadata pin
               seg.bitmap_pinned = false;  // No bitmap pin
               seg.age           = 12;
               seg.read_nodes    = 7000;
               seg.read_bytes = test_segment_size * 8 / 10;  // 80% is valid data (changed from 70%)
               seg.total_objects = 8000;
               // Set this as the maximum age for pinned segments (200 seconds ago)
               seg.vage = arbtrie::get_current_time_ms() - 200000;
               break;
         }

         // Add segment to the dump data
         dump.segments.push_back(seg);
      }

      // Add session info
      seg_alloc_dump::session_info session1;
      session1.session_num = 1;
      session1.read_ptr    = 3;
      session1.is_locked   = true;
      dump.sessions.push_back(session1);

      seg_alloc_dump::session_info session2;
      session2.session_num = 2;
      session2.read_ptr    = 4;
      session2.is_locked   = true;
      dump.sessions.push_back(session2);

      seg_alloc_dump::session_info session3;
      session3.session_num = 3;
      session3.is_locked   = false;
      dump.sessions.push_back(session3);

      // Add pending segments
      seg_alloc_dump::pending_segment pending1;
      pending1.index       = 0;
      pending1.segment_num = 11;
      dump.pending_segments.push_back(pending1);

      seg_alloc_dump::pending_segment pending2;
      pending2.index       = 1;
      pending2.segment_num = 12;
      dump.pending_segments.push_back(pending2);

      // Capture the output to a string stream
      std::stringstream ss;
      dump.print(ss);

      // Instead of verifying exact output, just check that it contains critical headers/info
      // This avoids fragile tests that break with minor formatting changes
      std::string output = ss.str();

      // Check existence of key headers and sections
      REQUIRE(output.find("segment allocator state") != std::string::npos);
      REQUIRE(output.find("Seg#") != std::string::npos);
      REQUIRE(output.find("Segment") != std::string::npos);
      REQUIRE(output.find("Used") != std::string::npos);
      REQUIRE(output.find("Free") != std::string::npos);
      REQUIRE(output.find("TBA") != std::string::npos);
      REQUIRE(output.find("Age") != std::string::npos);
      REQUIRE(output.find("Seq") != std::string::npos);
      REQUIRE(output.find("#Nodes") != std::string::npos);
      REQUIRE(output.find("ReadNodes") != std::string::npos);

      // Check segment-specific output
      REQUIRE(output.find("0 ") != std::string::npos);  // Segment 0
      REQUIRE(output.find("1 ") != std::string::npos);  // Segment 1
      REQUIRE(output.find("SPACE USAGE SUMMARY") != std::string::npos);
      REQUIRE(output.find("free segment Q") != std::string::npos);
      REQUIRE(output.find("pending free segments") != std::string::npos);

      // Verify pin emojis appear
      REQUIRE(output.find("📌") != std::string::npos);

      // Print the output to console for visual inspection
      std::cout << output << std::endl;
   }

}  // namespace arbtrie::test