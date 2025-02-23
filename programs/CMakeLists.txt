#add_executable(triedentdb tdb.cpp)
#target_link_libraries(triedentdb PUBLIC Boost::program_options triedent)
#target_include_directories(triedentdb PUBLIC ${Boost_INCLUDE_DIRS})
#set_target_properties(triedentdb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${ROOT_BINARY_DIR})

add_executable(arbt arb.cpp)
target_link_libraries(arbt PUBLIC Boost::program_options arbtrie)
target_include_directories(arbt PUBLIC ${Boost_INCLUDE_DIRS})
set_target_properties(arbt 
    PROPERTIES 
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

add_executable(arbtrie-benchmark bench.cpp)
target_link_libraries(arbtrie-benchmark PUBLIC Boost::program_options arbtrie)
target_include_directories(arbtrie-benchmark PUBLIC ${Boost_INCLUDE_DIRS})
set_target_properties(arbtrie-benchmark
    PROPERTIES 
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

#add_executable(arbtrie_test test.cpp)

#target_link_libraries(arbtrie_bench PRIVATE arbtrie)
#target_link_libraries(arbtrie_test PRIVATE arbtrie)

#set_target_properties(arbtrie_bench arbtrie_test
#    PROPERTIES
#    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
#)

#if(ENABLE_SANITIZER)
#    target_compile_options(arbtrie_bench PUBLIC -fsanitize=thread -g)
#    target_link_options(arbtrie_bench PUBLIC -fsanitize=thread -g)

#    target_compile_options(arbtrie_test PUBLIC -fsanitize=thread -g)
#    target_link_options(arbtrie_test PUBLIC -fsanitize=thread -g)
#endif()

#if(ENABLE_COVERAGE)
#    target_compile_options(arbtrie_bench PUBLIC --coverage)
#    target_link_options(arbtrie_bench PUBLIC --coverage)

#    target_compile_options(arbtrie_test PUBLIC --coverage)
#    target_link_options(arbtrie_test PUBLIC --coverage)
#endif()

#add_executable(mermaid mermaid.cpp)
#target_link_libraries(mermaid PUBLIC Boost::program_options triedent)
#target_include_directories(mermaid PUBLIC ${Boost_INCLUDE_DIRS} )
#set_target_properties(mermaid PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${ROOT_BINARY_DIR})
