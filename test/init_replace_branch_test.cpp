/**
 * Replay test for init(clone, replace_branch) crash dumps.
 *
 * Reads /tmp/node_crash_dump.bin (written by the instrumented inner_base.hpp
 * when validate_invariants fails after init(replace_branch)) and replays
 * the operation deterministically to reproduce the bug without threads.
 *
 * Dump format (little-endian):
 *   uint32_t  clone_size
 *   uint32_t  dest_size
 *   uint32_t  br           (branch number being replaced)
 *   uint32_t  sub_count    (number of sub-branches)
 *   uint16_t  needed_clines
 *   uint8_t   cline_indices[8]
 *   ptr_address sub_addrs[sub_count]
 *   uint32_t  ndivs
 *   uint8_t   dividers[ndivs]
 *   uint8_t   clone_bytes[clone_size]
 *   uint8_t   dest_bytes[dest_size]   (the corrupted result — for comparison)
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <vector>
#include <iostream>

#include <psitri/node/inner.hpp>
#include <psitri/node/node.hpp>

using namespace psitri;

int main(int argc, char* argv[])
{
   const char* path = "/tmp/node_crash_dump.bin";
   if (argc > 1)
      path = argv[1];

   FILE* f = fopen(path, "rb");
   if (!f)
   {
      fprintf(stderr, "Cannot open %s\n", path);
      return 1;
   }

   // Read header
   uint32_t clone_size, dest_size, br_val, sub_count;
   uint16_t needed_clines;
   std::array<uint8_t, 8> cline_indices{};

   fread(&clone_size, 4, 1, f);
   fread(&dest_size, 4, 1, f);
   fread(&br_val, 4, 1, f);
   fread(&sub_count, 4, 1, f);
   fread(&needed_clines, 2, 1, f);
   fread(cline_indices.data(), 1, 8, f);

   printf("clone_size=%u  dest_size=%u  br=%u  sub_count=%u  needed_clines=%u\n",
          clone_size, dest_size, br_val, sub_count, needed_clines);
   printf("cline_indices: ");
   for (uint32_t i = 0; i < sub_count; ++i)
      printf("%u ", cline_indices[i]);
   printf("\n");

   // Read sub-branch addresses
   std::vector<sal::ptr_address> sub_addrs(sub_count);
   for (uint32_t i = 0; i < sub_count; ++i)
      fread(&sub_addrs[i], sizeof(sal::ptr_address), 1, f);

   printf("sub_addrs: ");
   for (auto a : sub_addrs)
      printf("%u ", *a);
   printf("\n");

   // Read dividers
   uint32_t ndivs;
   fread(&ndivs, 4, 1, f);
   std::vector<uint8_t> dividers(ndivs);
   if (ndivs > 0)
      fread(dividers.data(), 1, ndivs, f);

   // Read raw clone bytes
   std::vector<uint8_t> clone_bytes(clone_size);
   fread(clone_bytes.data(), 1, clone_size, f);

   // Read raw dest bytes (the corrupted result)
   std::vector<uint8_t> dest_bytes(dest_size);
   fread(dest_bytes.data(), 1, dest_size, f);

   fclose(f);

   // Print clone info
   auto* clone = reinterpret_cast<const inner_prefix_node*>(clone_bytes.data());
   printf("\nClone: nb=%u  ncl=%u  prefix='%.*s'  size=%u\n",
          clone->num_branches(), clone->num_clines(),
          (int)clone->prefix().size(), clone->prefix().data(),
          clone->size());

   printf("Clone passes validate_invariants: %s\n",
          clone->validate_invariants() ? "YES" : "NO");

   // Print clone branches
   auto c_cl = reinterpret_cast<const cline_data*>(clone->clines());
   for (uint32_t i = 0; i < clone->num_branches(); ++i)
   {
      auto br = clone->const_branches()[i];
      auto addr = sal::ptr_address(*c_cl[br.line()].base() + br.index());
      printf("  clone br[%2u] line=%u idx=%2u -> addr=%u\n",
             i, br.line(), br.index(), *addr);
   }

   // Print clone clines
   printf("Clone clines:\n");
   for (uint32_t i = 0; i < clone->num_clines(); ++i)
   {
      printf("  cline[%u] base=%u ref=%u %s\n",
             i, *c_cl[i].base(), c_cl[i].ref(),
             c_cl[i].is_null() ? "(null)" : "");
   }

   // Now replay: construct branch_set from sub-branches
   branch_set sub_branches;
   for (uint32_t i = 0; i < sub_count; ++i)
   {
      if (i == 0)
         sub_branches = branch_set(sub_addrs[0]);
      else
         sub_branches.push_back(dividers[i - 1], sub_addrs[i]);
   }

   // Verify find_clines produces the same result
   std::array<uint8_t, 8> replay_cline_indices{};
   auto replay_needed = psitri::find_clines(
       clone->get_branch_clines(),
       clone->get_branch(branch_number(br_val)),
       sub_branches.addresses(),
       replay_cline_indices);

   printf("\nfind_clines replay: needed=%u (original=%u)\n", replay_needed, needed_clines);
   printf("cline_indices replay: ");
   for (uint32_t i = 0; i < sub_count; ++i)
      printf("%u ", replay_cline_indices[i]);
   printf("  (original: ");
   for (uint32_t i = 0; i < sub_count; ++i)
      printf("%u ", cline_indices[i]);
   printf(")\n");

   // Build the replace_branch op
   op::replace_branch update{
       branch_number(br_val),
       sub_branches,
       needed_clines,
       cline_indices,
       0  // delta_descendents (doesn't matter for this test)
   };

   // Allocate dest buffer and use placement new with the public constructor
   // which calls init(clone, update) + set_prefix() internally
   std::vector<uint8_t> replay_dest(dest_size, 0);

   printf("\nReplaying via placement new inner_prefix_node(...)...\n");
   // The constructor is: inner_prefix_node(asize, seq, prefix, clone, update)
   auto* dest = new (replay_dest.data()) inner_prefix_node(
       dest_size,
       clone->address_seq(),
       clone->prefix(),
       clone,
       update);

   printf("Replay dest passes validate_invariants: %s\n",
          dest->validate_invariants() ? "YES" : "NO");

   // Print replay dest branches
   auto d_cl = reinterpret_cast<const cline_data*>(dest->clines());
   for (uint32_t i = 0; i < dest->num_branches(); ++i)
   {
      auto br = dest->const_branches()[i];
      auto addr = sal::ptr_address(*d_cl[br.line()].base() + br.index());
      printf("  replay br[%2u] line=%u idx=%2u -> addr=%u\n",
             i, br.line(), br.index(), *addr);
   }

   // Also check the captured (corrupted) dest
   printf("\nCaptured dest (corrupted):\n");
   auto* cap_dest = reinterpret_cast<const inner_prefix_node*>(dest_bytes.data());
   auto cap_cl = reinterpret_cast<const cline_data*>(cap_dest->clines());
   for (uint32_t i = 0; i < cap_dest->num_branches(); ++i)
   {
      auto br = cap_dest->const_branches()[i];
      auto addr = sal::ptr_address(*cap_cl[br.line()].base() + br.index());
      printf("  cap   br[%2u] line=%u idx=%2u -> addr=%u\n",
             i, br.line(), br.index(), *addr);
   }

   printf("\nDone.\n");
   return dest->validate_invariants() ? 0 : 1;
}
