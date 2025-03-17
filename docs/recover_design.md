## Design Thoughts on Recovery of Database

- these ideas are not fully implimented and may never be
implimented, and are only here for future reference in
case they might be useful.

# Goal
  - ensure that the database can be reliably recovered just
    from the segments and that everything in the allocator_state
    and id_alloc can be blown away, because these will be high
    churn and non msync 
  - given each node's id_address can be assigned to multiple nodes
    across multiple threads and moved multiple times, only the
    most recent copy associated with the id_address is valid. 
  - given two segments with a copy of a node with the same id_address,
    we must reliably and unambigiously determine which copy is 
    the latest.

# Worst Case Test
  1. thread A uses ID1 then in Segment 1
  2. thread C compactor moves it to Segment 2
  3. thread B re-allocs ID1 after A releases it Segment 3 

  There are now three copies of a node with ID1 in three different
  segments and the recovery algorithm must correctly identify
  which one is the most recent use of ID1. 

  We know that each thread (aka session) can maintain a sequential order of
  segments because the time stamps on segment open / finalize will give
  the ordering and the compactor thread maintains this timestamp info
  as it merges data from one segment to another.

  When a ID1 moves from a segment owned by thread A to one owned by
  thread B we know that B is the latest owner. However, ID1 can bounce
  back and forth multiple times between thread A and B in the same segment.

  Storing a global allocation sequence number in the node header would
  let us know which one is the newest allocation, this number need not update
  on every modify or copy, but only on initial allocation of a node with
  that id. We could have many allocation counters to reduce thread contention
  on a single counter. As long as the counter's don't wrap within the potential
  over-lap window of segments then we have a winner. 

  If each segment is 32 MB, and we have two threads creating and releasing
  a single value node of 64 byte size fighting over the same 2 node ids...
  then an ID could change hands 1 million times before the segments became
  full. In practice each new alloc would be in a different region so the
  two threads would have to be creating and releasing an ID int he same
  region, e.g. a child of the an inner node in a particular region, which
  would happen if they cloned a tree, then started adding and removing
  keys under the same branch. 

  I steal 16 bits from the checksum to have an allocation sequence number,
  associated with each node.  This would require 128kb to have one for
  each region and could live in the region header which already has
  a mutex protecting allocations. Utilizing an allocation sequence number
  and putting it in the node header clearly and unambigiously establishes
  a total order of all nodes with a given ID... provided the sequence
  number doesn't wrap in the time it takes two over lapping segments
  to fill up with the same id.

  64 possible sessions, each with 32 MB of data, worst case they
  round-robin allocate and free the same ID in the same region which
  would produce 33 million copies of the same object... however,
  each segment only has "one" latest copy because they are ordered within
  a segment. You would think this means we only have 64 possible values to
  consider, but we still need to detect the sequence number wrapping.. 

  If the checksum is split across the unused bits of the inner node, plus 8
  bits from the node header, then we would maintan a 30 bit checksum on
  inner nodes, a 19 bit checksum binary/value nodes and a 24 bit allocation
  sequence number which is large enough to easily detect wrapping even
  when many threads are writing... not quite large enough to handle the
  worst possible case of 64 threads fighting over the same tiny 64 byte
  object with the same ID.. but anyone operating in such a manner 
  on a system that has a hardware failure (the only time this is even
                                           close to relevant) should
  probably accept the fact that their usage pattern is suspect.

# Knowns
  1. within a given segment, the last copy in the file is most recent
  2. each segment tracks the session (aka thread) that allocated it,
     combined with time stamps on the segments gives us a total-ordering
     of all uses of the ID by an individual thread.
  3. the "timestamp" on a node is only known to be between the segment's
     creation and finalization time. 
  4. segments can be sorted by [start,end] time where any overlap in the
     interval is considred equality and < means a.end <= b.start 
  5. on every sync() a "timestamp" node is inserted into all active segments
      - this creates new ranges with [start,end] times 
      - on every sync() all threads are known to be idle. 
      - this produces many time slices that share a common end,
        but they may still overlap with their start times. 
      - each inner range can have its own size-weighted timestamp which 
        helps the compactor keep more accurate times on nodes.

      - if a node id is found in a segment, it means that session took ownership
        of that node id sometime after the start of the segment. 
  

  3. the compactor records the segment numbers and time stamp it moves from,
     in combination with the timestamp on each segment header we can
     establish a total ordering between a single session and the
     compactor thread. 
      - because the compactor is moving from multiple segments into one
        and each source can be from a different session and timestamp,
        the compactor puts "marker nodes" that contain information about
        the segment the data is coming from. 
      - when re-compacting this enables us to preserve the accurate time
        of the original data and minimize "time skew" caused by weighted
        average combining. 
      - note it is possible the compactor's segment could be created
        before the sessions segment is allocated, the session's segment
        could then be filled and drained, and be compacted before
        the compactor's segment is full. By recording the source
        segment number and time stamp we establilsh everything in
        that segment is after everything in 

## Read Decay Thread
  - updates a shared variable with the current time every ms
       * this is faster to read than getting the current system time
       * every alloc uses this to update the size-weighted average of 
         the segment as it is being filled.

## Segment Header 
  - stores the time the segment was popped from the spmc_buffer
  - stores the time the segment was finalized after being filled
  - stores the size-weighted average time of the data within 
  - stores the session number that popped it from the spmc buffer
  - stores the [session number, segment number] of each source segment
    used by the compactor to compact data into the segment
