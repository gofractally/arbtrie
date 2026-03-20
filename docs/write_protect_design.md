session thread
    appends nodes to 1+segments
    calls sync() when transaction is complete
        - advances to end of current page
        - marks all pages modified by any thread as read-only
            - TODO: only mark segments modified by your session as read-only
            -       only modify segments "owned" by your session
            -         if you come across a unique ownership of a node that is not
                      owned by your session, then copy on write.. this shouldn't
                      ever happen in practice

compactor thread
    - must ensure that no one is modifing a segment that it is compacting
    - you can only modify a segment if you are the thread that alloced the segment
      and it has not been marked read only
          - it gets marked read-only on commit
          - is there any reason to compact during a large commit? No
          - is there any reason to promote in cache during large commit? No
          - is there any reason we need the mutate lock... ? No.


start transaction
    modify_lock(index).lock()  - start write trans, blocks other writers
        get the root (no need for root change mutex
commit transaction
        set_root() - grabs root change mutex to sync with writers
            modify_lock(index).unlock()
        advance the last_writable_page on all segments utilized in transaction
            - enables caching and compacting of nodes / segments in read-only memory
abort transaction
    modify_lock(index).unlock()
           
Races 
    - compactor could move/cache node while session is COW node...
        It doesn't matter who starts the write, only difference is 
        what happens on commit... 
          Session Wins (then compactor yields)
           session starts copy
           compactor starts copy
           session writes pos, takes notice of old location (not location copy started from)
              - marks free space in old segment
           compactor tries and abandonds write
              - unalloc space in its own session segment
          Compactor Wins (then session overwrites)
           session starts copy
           compactor starts copy
           compactor updates pos
              - marks free space in prior segement (where it moved from)
           session writes pos, takes notice of old location
              - marks free space in old segment, aka compactor
        

    scoped lock root-change-mutex[index] - 

segment
   node->node->node->[?sync]->node, node, node... [footer]

   Each [sync] marks the end of a transaction... and hashes everything
   from the last sync or beginning of segment, the sync becomes part of
   the write protected memory

   On sync, all segments can be processed in parallel which could make
   sense if it involves calculating a 32MB checksum... 

   If the "sync" node includes the root transaction... 
     - can we have two open transactions on different roots 
       in the same session... one would think yes. 
     - the sync node contains the root IDs as of that sync.

The compactor needs to securely move nodes while maintaining the property 
   that the data that is moved is write protected and unmodified
      - except that the compactor may want to "optimize" memory layout
      of binary nodes and/or update checksums if they were not updated
      on the original commit. 


struct sync
  size  (bytes to start of last sync or segment start)
  root node being synced
  time it was synced 
  checksum


Types of Failure
1. Program Crash
   a. everything in read-only memory should be safe
   b. id db and other memory kept in RW mode may be in question
       - did the crash result from writes 
       - memory leaks become possible...

Recovery
   - scan all segments... 
   -   set ids based upon the newest segment

2. Power Failture
   b. everything sent to the SSD should be safe
3. VM Rug Pull 
   c. similar to Power Failure... 
4. OS Crash 
   d. similar to power failure. 
5. Data Corruption in RAM
    - checksums validated at compaction...
6. Data Corruption at Rest on Disk
    - detected via checksums validated at compaction








