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

# Apply sanitizer and coverage settings if enabled
if(ENABLE_SANITIZER)
    target_compile_options(arbtrie-tests PUBLIC -fsanitize=thread -g)
    target_link_options(arbtrie-tests PUBLIC -fsanitize=thread -g)
    
    target_compile_options(arbtrie-fuzz-tester PUBLIC -fsanitize=thread -g)
    target_link_options(arbtrie-fuzz-tester PUBLIC -fsanitize=thread -g)
    
    target_compile_options(sim-db-test PUBLIC -fsanitize=thread -g)
    target_link_options(sim-db-test PUBLIC -fsanitize=thread -g)
endif()

if(ENABLE_COVERAGE)
    target_compile_options(arbtrie-tests PUBLIC --coverage)
    target_link_options(arbtrie-tests PUBLIC --coverage)
    
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

# Ensure we compile with optimization and native CPU instructions for benchmarking
target_compile_options(min_index_benchmark PRIVATE -O3 -march=native)
target_compile_options(min_index_32_benchmark PRIVATE -O3 -march=native)
target_compile_options(min_index_combined_benchmark PRIVATE -O3 -march=native)

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


