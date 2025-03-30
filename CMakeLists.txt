cmake_minimum_required(VERSION 3.16)

# Set CMake policies
if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

# Enforce build type if not specified - place before project()
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Choose the type of build" FORCE)
endif()

# Valid build types
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")

# Print build type for debugging
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Set debug flags explicitly - must be before project()
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -UNDEBUG -fno-omit-frame-pointer" CACHE STRING "Flags used by the compiler in Debug builds" FORCE)
    message(STATUS "Setting Debug flags: ${CMAKE_CXX_FLAGS_DEBUG}")
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

# Set global properties
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Define build options
option(ENABLE_SANITIZER "Enable Thread Sanitizer" OFF)
option(ENABLE_COVERAGE "Enable coverage reporting" OFF)
option(BUILD_TESTS "Build tests" ON)
option(BUILD_VISUALIZER "Build segment visualizer" )

# Find required packages
find_package(Threads REQUIRED)
find_package(SQLite3 REQUIRED)

# Find Boost
set(Boost_NO_WARN_NEW_VERSIONS ON)
find_package(Boost REQUIRED COMPONENTS program_options)

# Only find Catch2 if we're building tests
if(BUILD_TESTS)
    find_package(Catch2 3 REQUIRED)
    enable_testing()
endif()

# Only find Qt if building visualizer
if(BUILD_VISUALIZER)
    add_subdirectory(visualizer)
endif()

# SAL library setup - required by arbtrie
option(SAL_DEBUG "Enable debug logging for SAL library" ON)
option(SAL_BUILD_TESTS "Build SAL test suite" ${BUILD_TESTS})

# Add the SAL library
add_subdirectory(libraries/sal)
message(STATUS "SAL library added to build")

# Main library
add_library(arbtrie
    src/node_handle.cpp
    src/database.cpp
    src/mapping.cpp
    src/seg_allocator.cpp
    src/seg_allocator_session.cpp
    src/recover.cpp
    src/iterator_count_keys.cpp
    src/segment_thread.cpp
    src/mapped_memory/session_data.cpp
)

# Library configuration
target_include_directories(arbtrie 
    PUBLIC 
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${Boost_INCLUDE_DIRS}
)

target_link_libraries(arbtrie 
    PUBLIC 
        Threads::Threads
        sal
)
target_compile_features(arbtrie PUBLIC cxx_std_23)

# Set up explicit compile definitions
add_compile_definitions($<$<CONFIG:Debug>:_DEBUG>)

# Common compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-vla-extension -fdiagnostics-color=always")

# Debug specific settings at the global level
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O0 -g -fno-omit-frame-pointer)
endif()

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

# MDBX benchmark test (optional)
option(BUILD_MDBX_TEST "Build the lmdbx benchmark test" OFF)
if(BUILD_MDBX_TEST)
    message(STATUS "Building MDBX benchmark test")
    # Look for header directly in external/libmdbx
    find_path(MDBX_INCLUDE_DIR mdbx.h PATHS external/libmdbx HINTS "${CMAKE_SOURCE_DIR}/external/libmdbx" NO_CACHE)
    
    # Look for library directly in external/libmdbx, prefer static
    find_library(MDBX_LIBRARY NAMES libmdbx-static.a libmdbx.a mdbx 
                 PATHS external/libmdbx 
                 HINTS "${CMAKE_SOURCE_DIR}/external/libmdbx" NO_DEFAULT_PATH NO_CACHE)

    # If static wasn't found, maybe dynamic exists (less preferred but okay for testing)
    if(NOT MDBX_LIBRARY)
         find_library(MDBX_LIBRARY NAMES mdbx 
                      PATHS external/libmdbx 
                      HINTS "${CMAKE_SOURCE_DIR}/external/libmdbx" NO_DEFAULT_PATH NO_CACHE)
    endif()


    if(MDBX_INCLUDE_DIR AND MDBX_LIBRARY)
        message(STATUS "Found MDBX include: ${MDBX_INCLUDE_DIR}")
        message(STATUS "Found MDBX library: ${MDBX_LIBRARY}")
        add_executable(lmdbx_arb_test test/lmdbx_arb_test.cpp)
        target_include_directories(lmdbx_arb_test PUBLIC ${MDBX_INCLUDE_DIR} ${Boost_INCLUDE_DIRS})
        target_link_libraries(lmdbx_arb_test PRIVATE ${MDBX_LIBRARY} ${Boost_LIBRARIES} Threads::Threads)
        if(WIN32)
            # Add necessary libraries for Windows if mdbx requires them
        elseif(APPLE)
            # Add necessary libraries for macOS if mdbx requires them
        else()
            # Add necessary libraries for Linux (like pthread, rt if needed)
             target_link_libraries(lmdbx_arb_test PRIVATE rt) # For clock_gettime if needed by boost/chrono?
        endif()

         # Install the test executable if needed
         install(TARGETS lmdbx_arb_test RUNTIME DESTINATION bin)

    else()
        message(WARNING "MDBX library or includes not found. Skipping lmdbx_arb_test build. Looked in external/libmdbx")
        if(NOT MDBX_INCLUDE_DIR)
            message(WARNING "MDBX_INCLUDE_DIR not found.")
        endif()
        if(NOT MDBX_LIBRARY)
            message(WARNING "MDBX_LIBRARY not found.")
        endif()
    endif()
endif()

# Only add test directory if testing is enabled
if(BUILD_TESTS)
    add_subdirectory(test)
endif()

# Installation rules (optional)
install(TARGETS arbtrie
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY include/
    DESTINATION include
)

# End of file - removing duplicate SAL options and includes
