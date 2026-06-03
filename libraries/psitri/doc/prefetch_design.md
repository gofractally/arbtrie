We start off with a ptr_address, and must first look up the control block..
   then we derive the ptr to the object in get_ref, we prefetch it and 1 extra line
   if a bit says to prefetch 2 cachelines... you only ever prefetch the 2nd cacheline,
   not the first cacheline... we access it too quickly, the second cachline latency
   is masked by the cold hit on the first one... 

   Then, while we wait, we check the random chance we might need to
   then the caller to get ref gets a node_header ptr and
   switches on the type.. blocking on the prefetch load above,
   then casts to the proper type... 

   Then once we have the 1 or 2 cachelines of the header, we have to
   quickly determine which ptr_address to take and then navigate to the next node.

   This normally requires: 
         1. estimating or calculting via header index which cacheline has
            has the right stem and which cacheline has the right ptr address
         2. scanning the cacheline with the stem to narrow down the exact
            ptr address
         3. loading the prt address starting at the top.


    Inner16 node at scale with 8k branch capacity,
            requires 16kb of stem data across 128 cachelines 
            requires 32kb of ptr address w/out compression across 256 cachelines
            with max sharing of ptr_address 512 cachelines referenced, or 8k cachelines worst case.

    A perfect index of the cachelines requiest 256 bytes, with header means Inner16 would
            want to prefetch the first 3 cachelines, with SIMD 16 way index, simd 8 way index two step
            to identify the exact cacheline... 1 cold miss.. on header thus far. 
    A adaptive piece wise linear approximation of u16 should be more precice when
            all values are already narrowed to 12 to 16 bits... thus we can fit
            2x the resolution in the same 96 bytes, allowing us to pin the
            exact half of the cacheline to do the final stem search, which 
            is important because the ptr_address cachelines need the higher
            resolution to predict correctly.


    Once stem 1/2 cacheline is identified, prefetch it, and prefetch the 1 ptr_address fan out. 
            - this is the second cold miss... 

    A cacheline can hold 32 branches, which point to 2 cachelines of control_blocks
    Now, ideally all branches in a particular cacheline point to 2, but realisticaly with
    snapshots and other randomness, at least 4 and up to 32 different cachelines are
    potential candidiates. Thus a ptr address cacheline could be encoded as:
    24 u32 + 32 u8:4 

            


