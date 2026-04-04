add_library( psitri
             src/database.cpp
             src/node/leaf.cpp
             src/node/inner.cpp
             src/dwal/wal_writer.cpp
             src/dwal/wal_reader.cpp
             src/dwal/dwal_transaction.cpp
             src/dwal/dwal_database.cpp
             src/dwal/dwal_read_session.cpp
             src/dwal/merge_cursor.cpp
             src/dwal/merge_pool.cpp
         )

target_include_directories( psitri
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        )

# Define XXH_INLINE_ALL to use xxhash as header-only
target_compile_definitions(psitri PUBLIC XXH_INLINE_ALL)

target_link_libraries( psitri PUBLIC sal hash ucc
    PRIVATE $<BUILD_INTERFACE:absl::btree>)
