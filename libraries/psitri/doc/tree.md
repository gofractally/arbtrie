# Tree Logic Design

Node Types:

1. Value - data or subtree, no key or prefix
2. Leaf - binary search with arbitrary keys, up to 16 clines of children, 4096 data max size
3. InnerPrefix - common prefix with 1 byte branch dividers, up to 16 clines of children 590+prefix.size is max size
4. Inner - inner node without prefix


Rules:
    Leaf splits without a common prefix, attempt to add to Parent's children
    Leaf splits with a common prefix, it produces InnerPrefix that takes over Address of old Leaf

    Leaf splits, but both halfs still require 16 clines... split again... 
       so long as split does not produce a common prefix we queue them to pass to the parent.

    Once a split produces a common prefix, we produce a new InnerPrefix for the two halfs
    and recurse down.

  Leaf node splits when it grows more than 4096 bytes or needs more than 16 clines
    - it produces either a InnerPrefix, if a common prefix or Inner if no common prefix. 


Largest possible split needs... 16 clines with 16 children each...
      16 with 16
      16 with 8
      16 with 4
      16 with 2
      16 with 1
      less than 16... 
        8     4     2     1    <1   <1
  1 =>  1 + ( 1 + ( 1 + ( 1 + (1  + 1) )) )

    So worst case a Leaf Node gets split into 6 parts that need to be passed to the parent.
    Each of these parts should attempt to be allocated in the parent's existing cline 
    

    // this is either a common prefix + exactly 1 child
    // or no common prefix and up to 6 children..
    upsert_result {
        uint8_t num_children()const { return dividers[0]; }
        ptr_address children[6]
        uint8_t     dividers[6]; // div 0 is used for num_children because it 
    };

   struct can_insert_result {
       int cline_index; // 0 to 15 if found, 16 if not required, -1 if required but no space
   }



   result upsert( parent, leaf, key ) 
       if( big value or subtree )
            if big value
                val_adr =  make value_node with leaf.hint
            else 
                val_adr = subtree
            auto cline_idx = cline_idx = leaf.find_cline_index(val);
            if( cline_idx < 16 )
            {
                auto req_space = leaf.calc_required_space( key, val, cline_idx );
                if( unique ) 
                    if( leaf.free_space() >= req_space )
                        leaf.modify.insert( key, val, cline_idx );
                        return leaf
                    if( leaf.free_space() + dead_space >= req_space )
                        new_leaf = ralloc( leaf, 4096, )
                        new_leaf.modify.insert( key, val, cline_idx );
                        return new_leaf
                 else shared
                    if( leaf.free_space() + dead_space >= req_space )
                        new_leaf = alloc( leaf, 4096, parent.hint )
                        new_leaf.modify.insert( key, val, cline_idx );
                        return new_leaf
            }
        else inline value
                auto req_space = leaf.calc_required_space( key, val );
                if( unique ) 
                    if( leaf.free_space() >= req_space )
                        leaf.modify.insert( key, val);
                        return leaf
                    if( leaf.free_space() + dead_space >= req_space )
                        new_leaf = ralloc( leaf, 4096, )
                        new_leaf.modify.insert( key, val );
                        return new_leaf
                else shared
                    if( leaf.free_space() + dead_space >= req_space )
                        new_leaf = alloc( leaf, 4096, parent.hint )
                        new_leaf.modify.insert( key, val );
                        return new_leaf

        split = leaf.calc_split()
        if split.cpre 
            result.insert( 0, 0, make leaf split.left, anyhint );
            result.insert( 1, split.div, make leaf split.right, left is sole hint )
            auto ip = remake_leaf_to_inner_pre( using leaf.address, result ); // or parent.hint if shared mode
            return upsert( parent, ip, key - cpre );
        else
            result.insert( 0, 0, make leaf split.left, parent.hint);
            result.insert( 1, split.div, make leaf split.right, parent.hint + branch 0 hint )
            if( key in 0 branch ) {
                child_result = upsert( parent, result.children[0], key );
                child_result.push_back( result.children[1], result.dividers[1] );
                return child_result;
            }
            else key in 1 branch {
                child_result = upsert( parent, result.children[1], key );
                result replace branch 1 to 1 + child_result.num_branch
                return result;
            }

result upsert( parent, inner, key ) 
{
    auto branch = inner.lower_bound(key);
    auto subresult = upsert( inner, inner.get_branch(branch), key );

    // beyond this point the key is no longer relevant, all we are doing
    // is updating the branch and/or splitting if necessary
    // in the fast path, aka unique all we have to do is update the
    // descendents count... in shared mode we have address that changed
    // which means we need a fast way to determine if this change of
    // address produces a free slot

    if( subresult.num_children == 1 ) {
        int new_cline = inner.can_swap_cline( inner.address, subresult.children[0] );
        if( new_cline < 16 ) {
            if unique 
                inner.update( branch, subresult.children[0], new_cline );
                return inner;
            else shared
                new_in = make_inner( inner, parent.hint, update{branch,subresult.children[0]))
                return new_in;
        }
        else  split  
            // split as much as needed and return all the split nodes to parent
            result r;
            r.insert( 0, 0, make_inner( parent.hint, inner, 0, n/2 ) )
            r.insert( 0, inner.divider(n/2), make_inner( parent.hint, inner, n/2, n ) )
            if( key < inner.divider(n/2) )
                auto subresult = upsert( parent, r.children[0], key );
                subresult.push_back( r.children[1] )
                return subresult;
            else
                auto subresult = upsert( parent, r.children[1], key );
                result replace branch 1 to 1 + child_result.num_branch
                return result;

    }

    // 2 requirements to edit in place, space and clines 
    uint8_t cidx[6];
    if( inner.find_clines( subresult, cidx ) ) {
        if( inner.can_insert( subresult ) ) {
            inner.insert( subresult );
            return inner;
        }
        grow_inner = remake<inner>( inner, size += 1 cacheline...)
        grow_inner.insert(cidx, subresult);
        return grow_inner;
    }
    else split // inner couldn't find enough clines for all of the results
    {
        result r;
        r.insert( 0, 0, make_inner( parent.hint, inner, 0, n/2 ) )
        r.insert( 0, inner.divider(n/2), make_inner( parent.hint, inner, n/2, n ) )
        if( branch < n/2 )
            auto subresult = upsert_subresult( parent, r.children[0], subresult );
            subresult.push_back( r.children[1] )
            return subresult;
        else branch >= n/2
            auto subresult = upsert_subresult( parent, r.children[1], subresult );
            result replace branch 1 to 1 + child_result.num_branch
            return result;
       
    }
}

result upsert( parent, InnerPrefix in, key ) {
    auto cpre = commonPrefix(key, in.prefix) )
    // the common case of traversing down the tree
    if( cpre == in.prefix ) {
        auto branch = lower_bound( key.substr(cpre.size()) )
        ... all the same logic from upsert( parent, inner, key...)
    }

    // if the cpre size is 0, it differs on the first byte we return two
    // branches to the parent, and the parent can attempt to insert or split
    if( cpre.size == 0 ) {
        new_leaf = { key, value }
            result.left  = new_leaf
            result.right = in
        if( key > in.prefix )
            swap(result.left, result.right)

        return result;
    }

    // if there is at least 1 byte of common prefix, then a new inner_prefix node is
    // required... and we return a single address to the parent
    if( cpre.size < in.prefix.size )
    {
        new_leaf   = {key.substr(cpre.size), val }
        // left/right depends upon whether key.substr(cpre.size()) <= in.prefix.substr(cpre.size))
        new_in_pre = { cpre, left = new_leaf, right = in.modify()->set_prefix(size-cpre.size) }
        return result;
    }
        
}
