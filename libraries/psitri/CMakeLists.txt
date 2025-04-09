add_library( psitri
             src/database.cpp
             src/node/leaf.cpp 
             src/node/inner.cpp 
         )

target_include_directories( psitri
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        )

# Define XXH_INLINE_ALL to use xxhash as header-only
target_compile_definitions(psitri PUBLIC XXH_INLINE_ALL)

target_link_libraries( psitri sal hash ucc)
                            
