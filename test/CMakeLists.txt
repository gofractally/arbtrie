# Add test executable
add_executable(arbtrie-tests
    arbtrie-tests.cpp
    hierarchical_bitmap_tests.cpp
    padded_atomic_tests.cpp
    segment_meta_tests.cpp
    interprocess_mutex_tests.cpp
    seg_alloc_dump_tests.cpp
    cache_difficulty_state_tests.cpp
    mapped_memory_tests.cpp
    poly_buffer_test.cpp
    find_byte_tests.cpp
)

target_link_libraries(arbtrie-tests 
    PUBLIC 
        arbtrie
        Catch2::Catch2WithMain
)

target_include_directories(arbtrie-tests 
    PUBLIC 
        ${Boost_INCLUDE_DIRS}
        $<TARGET_PROPERTY:Catch2::Catch2WithMain,INTERFACE_INCLUDE_DIRECTORIES>
)

set_target_properties(arbtrie-tests
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Add test to CTest
add_test(
    NAME arbtrie-tests
    COMMAND arbtrie-tests
)

# Add sim_db test executable
add_executable(sim-db-test sim_db_test.cpp)
target_link_libraries(sim-db-test PRIVATE Catch2::Catch2WithMain)
target_include_directories(sim-db-test
    PUBLIC
        ${Boost_INCLUDE_DIRS}
        $<TARGET_PROPERTY:Catch2::Catch2WithMain,INTERFACE_INCLUDE_DIRECTORIES>
)

set_target_properties(sim-db-test
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Add sim_db test to CTest
add_test(
    NAME sim-db-test
    COMMAND sim-db-test
)

# Add fuzz tester executable
add_executable(arbtrie-fuzz-tester arbtrie_fuzz_tester.cpp)
target_link_libraries(arbtrie-fuzz-tester PRIVATE arbtrie Catch2::Catch2WithMain)

target_include_directories(arbtrie-fuzz-tester
    PUBLIC
        ${Boost_INCLUDE_DIRS}
        $<TARGET_PROPERTY:Catch2::Catch2WithMain,INTERFACE_INCLUDE_DIRECTORIES>
)

# Add explicit debug flags
target_compile_options(arbtrie-fuzz-tester 
    PRIVATE 
        $<$<CONFIG:Debug>:-O0 -g -UNDEBUG -fno-omit-frame-pointer>
)

set_target_properties(arbtrie-fuzz-tester
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Add fuzz test to CTest
add_test(
    NAME arbtrie-fuzz-tester
    COMMAND arbtrie-fuzz-tester
)

# Add last test executable
add_executable(arbtrie-last-test arbtrie_last_test.cpp)
target_link_libraries(arbtrie-last-test PRIVATE arbtrie Catch2::Catch2WithMain)

# Add last debug test executable
add_executable(arbtrie-last-debug arbtrie_last_debug.cpp)
target_link_libraries(arbtrie-last-debug PRIVATE arbtrie Catch2::Catch2WithMain)

# Add psitri test executable
add_executable(psitri-tests
    ../libraries/psitri/tests/leaf_node_test.cpp
    ../libraries/psitri/tests/leaf_node_split_test.cpp
)

target_link_libraries(psitri-tests
    PRIVATE
        psitri # psitri library depends on sal
        Catch2::Catch2WithMain
)

target_include_directories(psitri-tests
    PUBLIC
        # Include directories from psitri and Catch2
        $<TARGET_PROPERTY:psitri,INTERFACE_INCLUDE_DIRECTORIES>
        $<TARGET_PROPERTY:Catch2::Catch2WithMain,INTERFACE_INCLUDE_DIRECTORIES>
)

# Define XXH_INLINE_ALL for the tests as well
target_compile_definitions(psitri-tests PUBLIC XXH_INLINE_ALL)

set_target_properties(psitri-tests
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Add psitri-tests to CTest
add_test(
    NAME psitri-tests
    COMMAND psitri-tests
)

# Apply sanitizer and coverage settings if enabled
if(ENABLE_SANITIZER)
    target_compile_options(arbtrie-tests PUBLIC -fsanitize=thread -g)
    target_link_options(arbtrie-tests PUBLIC -fsanitize=thread -g)
    
    target_compile_options(psitri-tests PUBLIC -fsanitize=thread -g) # Added for psitri-tests
    target_link_options(psitri-tests PUBLIC -fsanitize=thread -g)   # Added for psitri-tests
    
    target_compile_options(arbtrie-fuzz-tester PUBLIC -fsanitize=thread -g)
    target_link_options(arbtrie-fuzz-tester PUBLIC -fsanitize=thread -g)
    
    target_compile_options(sim-db-test PUBLIC -fsanitize=thread -g)
    target_link_options(sim-db-test PUBLIC -fsanitize=thread -g)
endif()

if(ENABLE_COVERAGE)
    target_compile_options(arbtrie-tests PUBLIC --coverage)
    target_link_options(arbtrie-tests PUBLIC --coverage)
    
    target_compile_options(psitri-tests PUBLIC --coverage) # Added for psitri-tests
    target_link_options(psitri-tests PUBLIC --coverage)   # Added for psitri-tests
    
    target_compile_options(arbtrie-fuzz-tester PUBLIC --coverage)
    target_link_options(arbtrie-fuzz-tester PUBLIC --coverage)
    
    target_compile_options(sim-db-test PUBLIC --coverage)
    target_link_options(sim-db-test PUBLIC --coverage)
endif()

#add_executable(arbtrie-tests-bigdb big.cpp)
#target_link_libraries(arbtrie-tests-bigdb PUBLIC Boost::program_options arbtrie)
#target_include_directories(arbtrie-tests-bigdb PUBLIC ${Boost_INCLUDE_DIRS})
#set_target_properties(arbtrie-tests-bigdb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${ROOT_BINARY_DIR})

# Add benchmark for min_index implementations
add_executable(min_index_benchmark min_index_benchmark.cpp)
add_executable(min_index_32_benchmark min_index_32_benchmark.cpp)
add_executable(min_index_combined_benchmark min_index_combined_benchmark.cpp)
add_executable(min_index_test min_index_test.cpp)

# Link with Catch2 for unit tests
target_link_libraries(min_index_test 
    PUBLIC 
        arbtrie
        Catch2::Catch2WithMain
)

# Set properties for benchmark executables
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

set_target_properties(min_index_test PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    OUTPUT_NAME "min_index_test"
)

# Ensure we compile with optimization and native CPU instructions for benchmarking
target_compile_options(min_index_benchmark PRIVATE -O3 -march=native)
target_compile_options(min_index_32_benchmark PRIVATE -O3 -march=native)
target_compile_options(min_index_combined_benchmark PRIVATE -O3 -march=native)

# Add test to CTest
add_test(
    NAME min_index_test
    COMMAND min_index_test
)

# Add benchmark for hint implementations
add_executable(hint_benchmark hint_benchmark.cpp)
set_target_properties(hint_benchmark
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Ensure we compile with optimization and native CPU instructions for benchmarking
target_compile_options(hint_benchmark PRIVATE -O3 -march=native)

# Add the min_index subdirectory with its tests
add_subdirectory(min_index)

# Add SQLite benchmark test executable
add_executable(sqlite-arb-test sqlite_arb_test.cpp)

# Link libraries for sqlite-arb-test
target_link_libraries(sqlite-arb-test
    PRIVATE
        Boost::program_options
        SQLite::SQLite3
)

# Set output directory for sqlite-arb-test
set_target_properties(sqlite-arb-test
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Add test to CTest (optional, uncomment if you want 'ctest' to run it)
# add_test(
#     NAME sqlite-arb-test
#     COMMAND sqlite-arb-test
# )


