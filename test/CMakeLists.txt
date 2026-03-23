# ============================================================
# Legacy arbtrie tests (disabled — arbtrie library target removed)
# Kept as reference; re-enable when psitri replaces arbtrie.
# ============================================================
# add_executable(arbtrie-tests
#     arbtrie-tests.cpp
#     hierarchical_bitmap_tests.cpp
#     padded_atomic_tests.cpp
#     segment_meta_tests.cpp
#     interprocess_mutex_tests.cpp
#     seg_alloc_dump_tests.cpp
#     cache_difficulty_state_tests.cpp
#     mapped_memory_tests.cpp
#     poly_buffer_test.cpp
#     find_byte_tests.cpp
# )
# target_link_libraries(arbtrie-tests PUBLIC arbtrie Catch2::Catch2WithMain)
# target_include_directories(arbtrie-tests PUBLIC
#     ${Boost_INCLUDE_DIRS}
#     $<TARGET_PROPERTY:Catch2::Catch2WithMain,INTERFACE_INCLUDE_DIRECTORIES>
# )
# set_target_properties(arbtrie-tests PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
# add_test(NAME arbtrie-tests COMMAND arbtrie-tests)

# add_executable(arbtrie-fuzz-tester arbtrie_fuzz_tester.cpp)
# target_link_libraries(arbtrie-fuzz-tester PRIVATE arbtrie Catch2::Catch2WithMain)
# target_include_directories(arbtrie-fuzz-tester PUBLIC
#     ${Boost_INCLUDE_DIRS}
#     $<TARGET_PROPERTY:Catch2::Catch2WithMain,INTERFACE_INCLUDE_DIRECTORIES>
# )
# target_compile_options(arbtrie-fuzz-tester PRIVATE $<$<CONFIG:Debug>:-O0 -g -UNDEBUG -fno-omit-frame-pointer>)
# set_target_properties(arbtrie-fuzz-tester PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
# add_test(NAME arbtrie-fuzz-tester COMMAND arbtrie-fuzz-tester)

# add_executable(arbtrie-last-test arbtrie_last_test.cpp)
# target_link_libraries(arbtrie-last-test PRIVATE arbtrie Catch2::Catch2WithMain)

# add_executable(arbtrie-last-debug arbtrie_last_debug.cpp)
# target_link_libraries(arbtrie-last-debug PRIVATE arbtrie Catch2::Catch2WithMain)

# add_executable(min_index_test min_index_test.cpp)
# target_link_libraries(min_index_test PUBLIC arbtrie Catch2::Catch2WithMain)
# set_target_properties(min_index_test PROPERTIES
#     RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
#     OUTPUT_NAME "min_index_test"
# )
# add_test(NAME min_index_test COMMAND min_index_test)

# ============================================================
# Active tests
# ============================================================

# sim_db test (no arbtrie dependency)
add_executable(sim-db-test sim_db_test.cpp)
target_link_libraries(sim-db-test PRIVATE Catch2::Catch2WithMain)
target_include_directories(sim-db-test
    PUBLIC
        ${Boost_INCLUDE_DIRS}
        $<TARGET_PROPERTY:Catch2::Catch2WithMain,INTERFACE_INCLUDE_DIRECTORIES>
)
set_target_properties(sim-db-test PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
add_test(NAME sim-db-test COMMAND sim-db-test)

# psitri tests
add_executable(psitri-tests
    ../libraries/psitri/tests/leaf_node_test.cpp
    ../libraries/psitri/tests/inner_node_tests.cpp
    ../libraries/psitri/tests/inner_node_util_tests.cpp
    ../libraries/psitri/tests/leaf_node_split_test.cpp
    ../libraries/psitri/tests/trie_tests.cpp
    ../libraries/psitri/tests/tree_context_tests.cpp
    ../libraries/psitri/tests/public_api_tests.cpp
    ../libraries/psitri/tests/count_keys_tests.cpp
)
target_link_libraries(psitri-tests
    PRIVATE
        psitri # psitri library depends on sal
        Catch2::Catch2WithMain
)
target_include_directories(psitri-tests
    PUBLIC
        $<TARGET_PROPERTY:psitri,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:Catch2::Catch2WithMain,INTERFACE_INCLUDE_DIRECTORIES>
)
target_compile_definitions(psitri-tests PUBLIC XXH_INLINE_ALL)
set_target_properties(psitri-tests PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
add_test(NAME psitri-tests COMMAND psitri-tests)

# ============================================================
# Sanitizer / coverage settings for active targets
# ============================================================
if(ENABLE_SANITIZER)
    target_compile_options(psitri-tests PUBLIC -fsanitize=thread -g)
    target_link_options(psitri-tests PUBLIC -fsanitize=thread -g)

    target_compile_options(sim-db-test PUBLIC -fsanitize=thread -g)
    target_link_options(sim-db-test PUBLIC -fsanitize=thread -g)
endif()

if(ENABLE_COVERAGE)
    target_compile_options(psitri-tests PUBLIC --coverage)
    target_link_options(psitri-tests PUBLIC --coverage)

    target_compile_options(sim-db-test PUBLIC --coverage)
    target_link_options(sim-db-test PUBLIC --coverage)
endif()

# ============================================================
# Benchmarks (no arbtrie dependency)
# ============================================================
add_executable(min_index_benchmark min_index_benchmark.cpp)
add_executable(min_index_32_benchmark min_index_32_benchmark.cpp)
add_executable(min_index_combined_benchmark min_index_combined_benchmark.cpp)

set_target_properties(min_index_benchmark PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    OUTPUT_NAME "min_index_benchmark"
)
set_target_properties(min_index_32_benchmark PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    OUTPUT_NAME "min_index_32_benchmark"
)
set_target_properties(min_index_combined_benchmark PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    OUTPUT_NAME "min_index_combined_benchmark"
)
target_compile_options(min_index_benchmark PRIVATE -O3 -march=native)
target_compile_options(min_index_32_benchmark PRIVATE -O3 -march=native)
target_compile_options(min_index_combined_benchmark PRIVATE -O3 -march=native)

add_executable(hint_benchmark hint_benchmark.cpp)
set_target_properties(hint_benchmark PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
target_compile_options(hint_benchmark PRIVATE -O3 -march=native)

add_subdirectory(min_index)

# SQLite benchmark test (no arbtrie dependency)
add_executable(sqlite-arb-test sqlite_arb_test.cpp)
target_link_libraries(sqlite-arb-test
    PRIVATE
        Boost::program_options
        SQLite3::SQLite3
)
set_target_properties(sqlite-arb-test PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
