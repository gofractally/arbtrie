add_executable(arbt arb.cpp)
target_link_libraries(arbt PUBLIC Boost::program_options arbtrie)
target_include_directories(arbt PUBLIC ${Boost_INCLUDE_DIRS})
set_target_properties(arbt 
    PROPERTIES 
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

if(ENABLE_SANITIZER)
    target_compile_options(arbt PUBLIC -fsanitize=thread -g)
    target_link_options(arbt PUBLIC -fsanitize=thread -g)
endif()

if(ENABLE_COVERAGE)
    target_compile_options(arbt PUBLIC --coverage)
    target_link_options(arbt PUBLIC --coverage)
endif()

add_executable(arbtrie-benchmark bench.cpp)
target_link_libraries(arbtrie-benchmark PUBLIC Boost::program_options arbtrie)
target_include_directories(arbtrie-benchmark PUBLIC ${Boost_INCLUDE_DIRS})
set_target_properties(arbtrie-benchmark
    PROPERTIES 
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

if(ENABLE_SANITIZER)
    target_compile_options(arbtrie-benchmark PUBLIC -fsanitize=thread -g)
    target_link_options(arbtrie-benchmark PUBLIC -fsanitize=thread -g)
endif()

if(ENABLE_COVERAGE)
    target_compile_options(arbtrie-benchmark PUBLIC --coverage)
    target_link_options(arbtrie-benchmark PUBLIC --coverage)
endif()

# Add the arbtrie_sql program subdirectory
if(BUILD_SQLITE_TOOLS)
    add_subdirectory(arbtrie_sql)
endif()
