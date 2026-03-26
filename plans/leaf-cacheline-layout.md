# Leaf Node Compact/Expand

## Implemented

Compactor shrinks idle leaves to actual used size. COW expands back to `max_leaf_size`.

### compact_size()
- Returns tight size when no dead space: `round_up_64(header+arrays) + round_up_64(alloc_pos)`
- Returns current `size()` when dead space exists (can't shrink without rebuild)
- The next COW triggers `clone_from` rebuild, eliminating dead space, then compaction can shrink

### compact_to()
- Same-size: fast `memcpy_aligned_64byte` (unchanged)
- Different sizes: copies header+arrays forward, alloc area backward from each `tail()`
- Saves/restores `alloc_header` fields that the allocator pre-initialized

### clone_from() (COW expand)
- Already handles different sizes correctly — copies `head_size` + `_alloc_pos` bytes
- Independent of total `size()`, so expanding from compact→max works with no changes

## Future: Cacheline-Aligned Header Layout
- Move dynamic arrays to byte 64 so both data regions are 64-byte aligned
- Add `_capacity` field to decouple array slot count from `_num_branches`
- Enables `memcpy_aligned_64byte` for both compact_to and clone_from
- Simplifies insert (3 independent memmoves instead of 3 cascading ones)
