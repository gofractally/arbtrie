// LevelDB-compatible FilterPolicy for psitrileveldb.
// Same as Cache: Core constructs a bloom policy, stashes it, and deletes it.
// Never invokes its methods. We provide the minimum lifecycle.
#pragma once

namespace leveldb {

class FilterPolicy {
 public:
    FilterPolicy() = default;
    FilterPolicy(const FilterPolicy&) = delete;
    FilterPolicy& operator=(const FilterPolicy&) = delete;
    virtual ~FilterPolicy() = default;
};

const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

}  // namespace leveldb
