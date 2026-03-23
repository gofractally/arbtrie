# Adaptive Range Count (`count_keys`) for psitri

## Context

psitri inner nodes maintain a `_descendents` field (39-bit) tracking total keys in each subtree. This invariant enables O(log n) range counting without iterating every key. The deprecated arbtrie library (`src/iterator_count_keys.cpp`) had a sophisticated adaptive algorithm that chose between counting by **inclusion** (sum in-range branches) or **exclusion** (total - out-of-range branches) depending on which minimizes work. This needs to be migrated to psitri.

## Key Simplification in psitri

arbtrie had sparse `full_node` (256 slots with gaps) requiring `next_index()` iteration. psitri inner nodes have **contiguous branches** (0 to `num_branches-1`), so `in_range_count = end - start` is trivial. This eliminates `count_branches_in_range()` entirely.

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `libraries/psitri/include/psitri/count_keys.hpp` | CREATE | `key_range` struct + adaptive counting algorithm |
| `libraries/psitri/include/psitri/node/leaf.hpp` | MODIFY | Add `upper_bound(key_view)` to `leaf_node` |
| `libraries/psitri/include/psitri/cursor.hpp` | MODIFY | Add `count_keys(key_view, key_view)` method |
| `libraries/psitri/include/psitri/write_cursor.hpp` | MODIFY | Add convenience `count_keys` |
| `libraries/psitri/tests/count_keys_tests.cpp` | CREATE | Tests |
| `libraries/psitri/CMakeLists.txt` | MODIFY | Add test file |

## Implementation Steps

### Step 1: Add `upper_bound` to `leaf_node`

In `leaf.hpp`, mirror the existing `lower_bound(key_view)` (line 304) but use `>` instead of `>=`:

```cpp
branch_number upper_bound(key_view key) const noexcept
{
   int pos[2];
   pos[0] = -1;               // left
   pos[1] = num_branches();   // right
   while (pos[1] - pos[0] > 1)
   {
      int  middle = (pos[0] + pos[1]) >> 1;
      bool gt     = get_key(branch_number(middle)) > key;
      pos[gt]     = middle;
   }
   return branch_number(pos[1]);
}
```

### Step 2: Create `count_keys.hpp`

#### 2a: Port `key_range` from `include/arbtrie/concepts.hpp:282-421`

Near-direct copy into `psitri` namespace. Replace `arbtrie::common_prefix` with `psitri::common_prefix` (from `psitri/util.hpp:20`). Key methods:
- `get_begin_byte()`, `get_end_byte()` - first byte of bounds
- `is_unbounded()`, `is_empty_range()` - range checks
- `try_narrow_with_prefix(key_view*)` - consume prefix from range
- `with_advanced_from()`, `with_advanced_to()` - narrow range by one byte for recursion
- `contains_key(key_view)` - point-in-range check

#### 2b: `count_child_keys(session, addr)` - O(1) per node

Reuse pattern from `tree_ops.hpp:52-66`. Returns `descendents()` for inner nodes, `num_branches()` for leaf, `1` for value_node. Avoids dereferencing value_nodes (cache optimization from arbtrie).

#### 2c: Dispatch function

```cpp
uint64_t count_keys(sal::allocator_session& session, ptr_address addr, key_range range);
```

Dispatches by `node_type` to typed overloads.

#### 2d: Leaf specialization

```cpp
uint64_t count_keys_leaf(const leaf_node& node, key_range range)
{
   branch_number lo = range.lower_bound.empty() ? branch_number(0) : node.lower_bound(range.lower_bound);
   branch_number hi = range.upper_bound.empty() ? branch_number(node.num_branches()) : node.upper_bound(range.upper_bound);
   return *hi - *lo;
}
```

#### 2e: Inner node algorithm (core logic)

```
count_keys_inner(session, node, range):
   // Handle prefix (inner_prefix_node only)
   if constexpr (is_inner_prefix_node):
      prefix = node.prefix()
      if not range.try_narrow_with_prefix(&prefix): return 0

   if range.is_unbounded(): return node.descendents()

   start = range.lower_bound.empty() ? 0 : node.lower_bound(get_begin_byte())
   if start == num_branches(): return 0

   // Find boundary branch at upper bound
   if range.upper_bound.empty():
      end = num_branches(); boundary = num_branches()
   else:
      boundary = node.lower_bound(get_end_byte())
      end = boundary

   // Determine if boundary needs special handling
   has_boundary = boundary < num_branches() && !range.is_last_byte_of_end()

   // Adaptive decision: inclusion vs exclusion
   in_range = end - start
   out_of_range = num_branches() - in_range - (has_boundary ? 1 : 0)

   if in_range > out_of_range:
      return count_by_exclusion(...)
   else:
      count = count_keys_in_branches(session, node, start, end, range)
      if has_boundary:
         count += count_keys(session, node.get_branch(boundary), range.with_advanced_to())
      return count
```

#### 2f: Inclusion path (`count_keys_in_branches`)

- First branch: if lower_bound non-empty and routes to this branch, recurse with `{lb.substr(1), maybe_ub.substr(1)}`
- Middle branches: fully contained, use `count_child_keys()` - O(1) each
- Boundary branch handled separately by caller

#### 2g: Exclusion path (`count_by_exclusion`)

```
total = node.descendents()
// Subtract keys before lower_bound
before = sum count_child_keys for branches [0, start)
       + count_keys(start_branch, {"", lb.substr(1)})  // partial
// Subtract keys at/after upper_bound
after  = count_keys(boundary_branch, {ub.substr(1), ""})  // partial
       + sum count_child_keys for branches [boundary+1, num_branches)
return total - before - after
```

### Step 3: Add `count_keys` to `cursor`

```cpp
// cursor.hpp - public method
uint64_t count_keys(key_view lower = {}, key_view upper = {}) const noexcept;

// inline implementation
inline uint64_t cursor::count_keys(key_view lower, key_view upper) const noexcept {
   if (_node.address() == sal::null_ptr_address) return 0;
   auto read_lock = _node.session()->lock();
   return psitri::count_keys(*_node.session(), _node.address(), {lower, upper});
}
```

### Step 4: Add `count_keys` to `write_cursor`

```cpp
uint64_t count_keys(key_view lo = {}, key_view hi = {}) const {
   return cursor(_ctx.get_root()).count_keys(lo, hi);
}
```

### Step 5: Tests

**Test file:** `libraries/psitri/tests/count_keys_tests.cpp`

1. **Empty tree** - returns 0
2. **Single leaf** (3-5 keys) - exact range checks
3. **Unbounded range** `("","")` - equals total key count
4. **Lower-bound only** `("m","")` - half the alphabet
5. **Upper-bound only** `("","m")` - half the alphabet
6. **Both bounds** `("d","m")` - middle slice
7. **Empty range** `("z","a")` - returns 0
8. **Large tree** (1000+ keys) - forces multi-level inner nodes
   - Random range validation: compare `count_keys(lo,hi)` against iterating `lower_bound(lo)` -> `next()` until `key() >= hi`
9. **Prefix coverage** - keys sharing common prefixes to trigger `inner_prefix_node`
10. **Inclusion vs exclusion paths** - narrow range (few branches) vs wide range (most branches)
11. **Edge cases** - single-char keys, empty-string key, prefix-of-each-other keys

## Verification

```bash
cd build/debug && cmake ../.. && make -j psitri_tests && ./libraries/psitri/psitri_tests "[count_keys]"
```

Brute-force validation test (random ranges against iteration) is the gold standard.
