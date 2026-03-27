# sim_db test
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
    ../libraries/psitri/tests/range_remove_tests.cpp
    ../libraries/psitri/tests/subtree_tests.cpp
    ../libraries/psitri/tests/multi_writer_tests.cpp
    ../libraries/psitri/tests/recovery_tests.cpp
    ../libraries/psitri/tests/fuzz_tests.cpp
    ../libraries/psitri/tests/cursor_tests.cpp
    ../libraries/psitri/tests/integrity_tests.cpp
    ../libraries/psitri/tests/edge_case_tests.cpp
    ../libraries/psitri/tests/update_value_tests.cpp
    ../libraries/psitri/tests/zip_tests.cpp
    ../libraries/psitri/tests/deep_recovery_tests.cpp
    ../libraries/psitri/tests/coverage_gap_tests.cpp
    ../libraries/psitri/tests/tree_ops_tests.cpp
    ../libraries/psitri/tests/truncate_tests.cpp
    ../libraries/psitri/tests/smart_ptr_tests.cpp
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

# init_replace_branch crash dump replay test
add_executable(init-replace-branch-test init_replace_branch_test.cpp)
target_link_libraries(init-replace-branch-test PRIVATE psitri)
target_include_directories(init-replace-branch-test
    PUBLIC
        $<TARGET_PROPERTY:psitri,INTERFACE_INCLUDE_DIRECTORIES>
)
target_compile_definitions(init-replace-branch-test PUBLIC XXH_INLINE_ALL)
set_target_properties(init-replace-branch-test PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# Sanitizer / coverage settings
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

# Benchmarks
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

if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm|aarch64|ARM|AARCH64")
    add_executable(hint_benchmark hint_benchmark.cpp)
    set_target_properties(hint_benchmark PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
    target_compile_options(hint_benchmark PRIVATE -O3 -march=native)
endif()

add_subdirectory(min_index)

# x86 SIMD correctness + performance benchmark
add_executable(simd-x86-benchmark simd_x86_benchmark.cpp)
target_link_libraries(simd-x86-benchmark PRIVATE psitri)
target_include_directories(simd-x86-benchmark
    PUBLIC
        $<TARGET_PROPERTY:psitri,INTERFACE_INCLUDE_DIRECTORIES>
)
target_compile_definitions(simd-x86-benchmark PUBLIC XXH_INLINE_ALL)
set_target_properties(simd-x86-benchmark PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# SQLite benchmark test (optional -- requires libsqlite3-dev)
if(SQLite3_FOUND)
    add_executable(sqlite-arb-test sqlite_arb_test.cpp)
    target_link_libraries(sqlite-arb-test
        PRIVATE
            Boost::program_options
            SQLite3::SQLite3
    )
    set_target_properties(sqlite-arb-test PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()
