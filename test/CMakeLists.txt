# Add test executable
add_executable(arbtrie-tests
    arbtrie-tests.cpp
    hierarchical_bitmap.cpp
    spmc_circular_buffer.cpp
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

# Apply sanitizer and coverage settings if enabled
if(ENABLE_SANITIZER)
    target_compile_options(arbtrie-tests PUBLIC -fsanitize=thread -g)
    target_link_options(arbtrie-tests PUBLIC -fsanitize=thread -g)
endif()

if(ENABLE_COVERAGE)
    target_compile_options(arbtrie-tests PUBLIC --coverage)
    target_link_options(arbtrie-tests PUBLIC --coverage)
endif()

#add_executable(arbtrie-tests-bigdb big.cpp)
#target_link_libraries(arbtrie-tests-bigdb PUBLIC Boost::program_options arbtrie)
#target_include_directories(arbtrie-tests-bigdb PUBLIC ${Boost_INCLUDE_DIRS})
#set_target_properties(arbtrie-tests-bigdb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${ROOT_BINARY_DIR})


