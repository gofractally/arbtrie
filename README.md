## Adaptive Radix Binary Trie

- High performance, copy-on-write, persistent database, key/value sorted database
- 500k writes/sec while sustaining 20M reads across 15 threads on M4 Max
- Ideal for Blockchain due to instant snapshot support
- Under active development, not yet stable
- Mostly lock-free and wait-free (except when growing DB file)
- Supports multiple tables, multiple writers (1 per table).

## Advanced Memory Managment
- Relocatable Memory via effecient smart pointers
- Trie nodes are adaptive in size, utilize 2 bytes per child
- Trie nodes optimized to align on cacheline boundaries 
- Reference counting smart-pointers are allocated with locality in mind
- Probablistic sampling to support Most Frequently Used Cache

## Scales beyond RAM
- all writes are sequential and contigious
- "binary search nodes" group leaf nodes into 4kb blocks to minimize page-misses at tail of Trie
- "smart" hash-based point lookups

## Safety 
- All commits place data in READ ONLY memory, safe from program errors
- Updates are atomic, a atomic operation updates the root
- Infinitately nested transactional updates

  
