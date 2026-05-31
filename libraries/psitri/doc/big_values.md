# Big values (out-of-line `big`)

Values too large for the leaf's inline `value_region` (`value_len` is a
`uint8_t`, so inline tops out at **255 B**) are stored out-of-line and referenced
by a single `ptr_address` from the leaf. This is the `big` value type (see
`wtree_mvcc.md` §1). Subtrees (`tree`) use the same single-ptr shape and the
same lifecycle.

This doc covers the **current scope** (immutable contiguous blob, ≤ 2 MB) and the
**deferred future** (filesystem-style offset+length read/update + chunking),
including *why* the future path changes the versioning model.

---

## SAL constraints (measured)

- `alloc_header._size` is **25 bits** → a node can represent up to 32 MB.
- `max_object_size = segment_size / 2 = 16 MB` → hard ceiling on any single SAL
  allocation. **2 MB is a policy cap, not the limit.**
- `max_cacheable_object_size = 4096` → the read-cache never promotes objects
  > 4 KB, so big values are automatically exempt from promotion copies.

---

## Size ladder

```
0 .. 255 B      inline in leaf value_region        zero-copy (in the 4 KB leaf page)
256 B .. 2 MB   big_blob: one out-of-line node      zero-copy (span into the node)
> 2 MB          big_index: node of ptr_address[]    FUTURE — scatter read
```

A `big` `value_region` stores a 4-byte `ptr_address` inline (`value_len == 4`,
`type == big`). `big_blob` vs `big_index` is a tag in the **big node's own
header**, invisible to the leaf: the leaf holds one ptr and
retains/releases/visits it; the node's own `destroy()` / `visit_children()`
recurse into chunk ptrs when it is an index.

---

## Current scope: immutable contiguous blob, ≤ 2 MB

**Versioning lives only in the leaf. The blob is immutable.** This is the right
model in a COW + refcount + MVCC engine and needs *no* new lifecycle concepts:

1. **The leaf owns the version horizon.** The alt-chain + 15-slot version table
   maps `version → ptr_address`. A big node tracking its own versions would
   duplicate that, against a horizon (`read_lock`) that is leaf-rooted anyway.
2. **Refcounting gives sharing for free.** An update allocs a fresh blob; the
   leaf's *current* slot points at the new ptr, the *old* ptr slides into the
   alt-chain, kept alive by older roots' refs, and `release`d once it drops
   below the prune floor (the min live snapshot).
3. **Immutable ⇒ zero-copy reads + free relocation.** A read returns a `span`
   straight into the mmap'd blob; the compactor moves it like any node.

**Update = whole-value replace.** Alloc a new blob, repoint the slot, old → alt
chain. The cost is that a small edit rewrites the whole blob — acceptable at
≤ 2 MB; chunking (future) is what buys this back.

**Lifecycle** (already specified in `wtree_mvcc.md` §6): leaf
`retain_branches` / `destroy` / `visit_children` retain / release / visit the
`big` ptr. Single ptr → one op each. Every walk must cover **current AND the
alt/history chain** — an old version can itself be `big`.

**Read API:** `find()` returns a zero-copy `value_type` span over the blob.

---

## Future (documented, deferred): offset+length + chunking

Goal: a filesystem-style API on big values —

- `read(key, offset, len) -> span-list` (pread)
- `update(key, offset, len, bytes)` (pwrite) — **update only, not insert**
  (insert remains whole-value)

and chunking for values **> 2 MB**: the big node becomes a `big_index`,
`[total_len][chunk_ptr ...]`, each chunk a contiguous blob. Reads return a
**span-list** (zero-copy scatter for streaming / hashing / copy-out) plus an
`assemble(buf)` convenience that materializes one contiguous span (one copy).
Chunk-granular COW then makes a partial write rewrite *one* chunk + copy the
small index + retain the rest; appends add a tail chunk and retain all.

### Why partial writes change the versioning model

Partial **reads** at offset are free against immutable storage — read any
sub-range of a blob or chunk-list with zero coordination. The forcing function
is the partial **write**:

> A `pwrite` to a sub-range either (a) rewrites the whole value — defeating the
> point of chunking — or (b) mutates a single chunk independently. Independent
> chunk mutation means a chunk now changes *without* the leaf re-pointing the
> whole value, so **each chunk / the index must carry its own version** to keep
> snapshots consistent. That pushes MVCC state down out of the leaf and into the
> big-value structure.

So the clean "immutable blob, version-only-in-the-leaf" model holds **exactly as
long as updates are whole-value replace.** Supporting offset+length writes is a
deliberate step into per-chunk version tracking, deferred until the filesystem
API is actually needed. For now: whole-value replace, ≤ 2 MB, immutable.
