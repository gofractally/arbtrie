#include <arbtrie/node_header.hpp>
#include <catch2/catch_test_macros.hpp>
#include <cstring>

using namespace arbtrie;

TEST_CASE("Header type bit location is consistent", "[mapped_memory]")
{
   // Create both header types
   allocator_header alloc;

   // For object_header, we need minimal valid parameters
   id_address     addr = id_address::from_int(1);  // Create a valid id_address
   id_address_seq dummy_id(addr, 1);
   object_header  obj(sizeof(object_header), dummy_id);

   // Test the default values first (should be 1 for allocator and 0 for object)
   REQUIRE(alloc._header_type == 1);
   REQUIRE(obj._header_type == 0);

   // Flip the values
   alloc._header_type = 0;
   obj._header_type   = 1;

   // Check they were flipped successfully
   REQUIRE(alloc._header_type == 0);
   REQUIRE(obj._header_type == 1);

   // Verify object_header with _header_type=1 can be correctly interpreted as an allocator_header
   allocator_header* obj_as_alloc = reinterpret_cast<allocator_header*>(&obj);
   REQUIRE(obj_as_alloc->_header_type == 1);

   // Verify allocator_header with _header_type=0 can be correctly interpreted as an object_header
   object_header* alloc_as_obj = reinterpret_cast<object_header*>(&alloc);
   REQUIRE(alloc_as_obj->_header_type == 0);
}