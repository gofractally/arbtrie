#include <arbtrie/id_alloc.hpp>

namespace arbtrie
{

   id_alloc::id_alloc(std::filesystem::path id_file)
       : _data_dir(id_file),
         _block_alloc(id_file, id_block_size, 1024 /* 256 GB*/),
         _ids_state_file(id_file.native() + ".state", access_mode::read_write)
   {
      if (_ids_state_file.size() == 0)
      {
         ARBTRIE_WARN("initializing new node_meta index");
         _ids_state_file.resize(round_to_page(sizeof(id_alloc_state)));
         auto idh = new (_ids_state_file.data()) id_alloc_state();
         for (auto& r : idh->regions)
         {
            r.first_free.store(temp_meta_type().set_loc(end_of_freelist).to_int());
            // skip the first 8... reserved
            // must skip 0 because it is reserved for NULL ids
            r.use_count.store(8);
            r.next_alloc.store(8);
         }
         ARBTRIE_DEBUG("eofl: ", end_of_freelist.cacheline());
         idh->clean_shutdown = true;
      }
      _state = reinterpret_cast<id_alloc_state*>(_ids_state_file.data());
      if (not _state->clean_shutdown)
      {
         ARBTRIE_WARN("checking node_meta index state...");
         /// TODO: validate the tree
      }
   }

   id_alloc::~id_alloc()
   {
      if (_state)
         _state->clean_shutdown = 1;
   }

}  // namespace arbtrie
