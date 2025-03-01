cmake_minimum_required(VERSION 3.24)

# Set CMake policies
if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

# Set the C++ compiler to LLVM from Homebrew on macOS
if(APPLE)
    set(CMAKE_CXX_COMPILER "/opt/homebrew/opt/llvm/bin/clang++")
endif()

# Enable compile commands for IDE support
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Create symlink to compile_commands.json in project root, for IDE support
if(CMAKE_EXPORT_COMPILE_COMMANDS)
    execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_BINARY_DIR}/compile_commands.json
        ${CMAKE_SOURCE_DIR}/compile_commands.json)
endif()

project(arbtrie VERSION 0.1.0 LANGUAGES CXX)

# Define build options
option(ENABLE_SANITIZER "Enable Thread Sanitizer" OFF)
option(ENABLE_COVERAGE "Enable coverage reporting" OFF)
option(BUILD_TESTING "Build tests" OFF)
option(BUILD_VISUALIZER "Build segment visualizer" )

# Find required packages
find_package(Threads REQUIRED)

# Find Boost
set(Boost_NO_WARN_NEW_VERSIONS ON)
find_package(Boost REQUIRED COMPONENTS program_options)

# Only find Catch2 if we're building tests
if(BUILD_TESTING)
    find_package(Catch2 3 REQUIRED)
    enable_testing()
endif()

# Only find Qt if building visualizer
if(BUILD_VISUALIZER)
    add_subdirectory(visualizer)
endif()

# Main library
add_library(arbtrie
    src/node_handle.cpp
    src/database.cpp
    src/mapping.cpp
    src/seg_allocator.cpp
    src/seg_allocator_session.cpp
    src/id_alloc.cpp
    src/recover.cpp
    src/iterator_count_keys.cpp
)

# Library configuration
target_include_directories(arbtrie 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${Boost_INCLUDE_DIRS}
)

target_link_libraries(arbtrie PUBLIC Threads::Threads)
target_compile_features(arbtrie PUBLIC cxx_std_23)

# Compiler options
target_compile_options(arbtrie 
    PUBLIC 
        -std=c++23
        -Wno-vla-extension
        $<$<CONFIG:Debug>:-O0 -g -UNDEBUG -fno-omit-frame-pointer>
)

# Sanitizer configuration
if(ENABLE_SANITIZER)
    message(STATUS "Enabling Thread Sanitizer")
    target_compile_options(arbtrie PUBLIC -fsanitize=thread -g)
    target_link_options(arbtrie PUBLIC -fsanitize=thread -g)
endif()

# Coverage configuration
if(ENABLE_COVERAGE)
    message(STATUS "Enabling coverage reporting")
    
    # Add coverage flags - using more specific flags
    target_compile_options(arbtrie PUBLIC 
        $<$<CONFIG:Debug>:-fprofile-arcs -ftest-coverage -g -O0>)
    target_link_options(arbtrie PUBLIC 
        $<$<CONFIG:Debug>:-fprofile-arcs -ftest-coverage>)
    
    # Find required tools
    find_program(LCOV lcov REQUIRED)
    find_program(GENHTML genhtml REQUIRED)
    
    # Define common arguments
    set(LCOV_ARGS
        --rc branch_coverage=1
        --rc function_coverage=1
        --ignore-errors graph,empty,corrupt,inconsistent,category,deprecated,format,unused
        --verbose
    )
    
    set(GENHTML_ARGS
        --ignore-errors corrupt,inconsistent,category
        --branch-coverage
        --function-coverage
        --verbose
    )

    # Define unwanted files list
    set(COVERAGE_EXCLUDES
        '/opt/homebrew/*'
        '/usr/include/*'
        '/usr/local/*'
        '/v1/*'
        '/Library/*'
        '*/catch2/*'
        '*/test/*'
    )
    
    # Add custom target for generating coverage report
    add_custom_target(coverage
        # Clear counters
        COMMAND ${CMAKE_COMMAND} -E make_directory coverage
        COMMAND ${LCOV} --directory ${CMAKE_BINARY_DIR} --zerocounters
        
        # Run tests (continue even if they fail)
        COMMAND ${CMAKE_BINARY_DIR}/bin/arbtrie-tests || true
        COMMAND ${CMAKE_BINARY_DIR}/bin/arbt || true
        COMMAND ${CMAKE_BINARY_DIR}/bin/arbtrie-benchmark || true
        
        # Capture coverage data
        COMMAND ${LCOV} 
            --directory ${CMAKE_BINARY_DIR}
            --capture 
            --output-file coverage/coverage.info 
            ${LCOV_ARGS}

        # Remove unwanted files from coverage
        COMMAND ${LCOV} 
            --remove coverage/coverage.info 
            ${COVERAGE_EXCLUDES}
            --output-file coverage/coverage.info
            ${LCOV_ARGS}
        
        # Generate HTML report
        COMMAND ${GENHTML} 
            coverage/coverage.info 
            --output-directory coverage/html 
            ${GENHTML_ARGS}
        
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating coverage report..."
    )

    # Add target that only generates report from existing data
    add_custom_target(coverage-report
        # Capture coverage data
        COMMAND ${LCOV} 
            --directory ${CMAKE_BINARY_DIR}
            --capture 
            --output-file coverage/coverage.info 
            ${LCOV_ARGS}

        # Remove unwanted files from coverage
        COMMAND ${LCOV} 
            --remove coverage/coverage.info 
            ${COVERAGE_EXCLUDES}
            --output-file coverage/coverage.info
            ${LCOV_ARGS}
        
        # Generate HTML report
        COMMAND ${GENHTML} 
            coverage/coverage.info 
            --output-directory coverage/html 
            ${GENHTML_ARGS}
        
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating coverage report from existing data..."
    )

    # Rename the full coverage target to be more descriptive
    add_custom_target(coverage-all
        COMMAND ${CMAKE_COMMAND} -E env TEST_PATTERN="" ${CMAKE_MAKE_PROGRAM} coverage
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating coverage report for all tests..."
    )

    # Add target to just reset coverage data
    add_custom_target(coverage-reset
        COMMAND ${CMAKE_COMMAND} -E make_directory coverage
        COMMAND ${LCOV} --directory ${CMAKE_BINARY_DIR} --zerocounters
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Resetting coverage counters..."
    )
endif()

# Platform-specific optimizations
if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86_64)|(X86_64)|(amd64)|(AMD64)")
    if(NOT APPLE)
        target_compile_options(arbtrie PUBLIC $<$<CONFIG:Release>:-march=haswell -flto>)
        target_link_options(arbtrie PUBLIC $<$<CONFIG:Release>:-march=haswell -flto>)
    else()
        target_compile_options(arbtrie PUBLIC $<$<CONFIG:Release>:-flto>)
        target_link_options(arbtrie PUBLIC $<$<CONFIG:Release>:-flto>)
    endif()
endif()

# Add subdirectories
add_subdirectory(programs)

# Only add test directory if testing is enabled
if(BUILD_TESTING)
    add_subdirectory(test)
endif()

# Install rules (optional)
install(TARGETS arbtrie
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY include/
    DESTINATION include
)
