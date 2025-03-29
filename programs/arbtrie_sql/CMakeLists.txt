cmake_minimum_required(VERSION 3.16)
project(arbtrie_sql)

# Find the necessary libraries
find_package(SQLite3 REQUIRED)

# Assuming arbtrie is built as a library in the parent project
# The arbtrie target is already known from the parent CMakeLists.txt

# --- Virtual Table Library ---
# Define a STATIC library for the virtual table implementation
add_library(arbtrie_vtab STATIC arbtrie_vtab.cpp)

# Set include directories for the vtab library
target_include_directories(arbtrie_vtab PUBLIC # PUBLIC allows consumers to find these headers if needed
    ../../include # Include dir from project root
    ../../libraries/sal/include # Include dir for sal library
    ${SQLITE3_INCLUDE_DIRS}
)

# Link dependencies for the vtab library
target_link_libraries(arbtrie_vtab PUBLIC # PUBLIC propagates link dependency & includes
    arbtrie
    SQLite::SQLite3
)

# --- Main Executable ---
add_executable(arbtrie_sql main.cpp)

# Link main executable against the vtab library and other direct dependencies
target_link_libraries(arbtrie_sql PRIVATE
    arbtrie_vtab # Link the vtab library
    # arbtrie and SQLite::SQLite3 are inherited via PUBLIC from arbtrie_vtab
    # but main.cpp might use them directly, so keep if needed.
    arbtrie 
    SQLite::SQLite3
)

# Include directories for the main executable (if main.cpp needs specific headers)
# Often inherited via PUBLIC from arbtrie_vtab, but explicit can be clearer.
target_include_directories(arbtrie_sql PRIVATE
    # ../../include 
    # ../../libraries/sal/include
    # ${SQLITE3_INCLUDE_DIRS}
)

# Set runtime output directory property
set_target_properties(arbtrie_sql PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# Optional: Add install rule if needed
# install(TARGETS arbtrie_sql DESTINATION bin)
# install(TARGETS arbtrie_vtab ARCHIVE DESTINATION lib)

# ---- Testing ----
if(BUILD_TESTS) # Only configure tests if BUILD_TESTS is ON
    # Add test executable
    add_executable(arbtrie_sql_tests tests/test_sql_interface.cpp)

    # Link test dependencies (including the vtab library)
    target_link_libraries(arbtrie_sql_tests PRIVATE
        arbtrie_vtab # Link the vtab library
        # arbtrie and SQLite::SQLite3 are inherited via PUBLIC from arbtrie_vtab
        arbtrie 
        SQLite::SQLite3
        Catch2::Catch2WithMain # Assumes Catch2 target is Catch2::Catch2WithMain
    )

    # Include directories for tests
    # Often inherited via PUBLIC from arbtrie_vtab, but explicit can be clearer.
    target_include_directories(arbtrie_sql_tests PRIVATE
        # ../../include
        # ../../libraries/sal/include
        # ${SQLITE3_INCLUDE_DIRS}
        ${Catch2_INCLUDE_DIRS} # Add Catch2 include dirs if needed
    )

    # Set runtime output directory property for tests
    set_target_properties(arbtrie_sql_tests PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

    # Add tests to CTest
    include(CTest)
    enable_testing()
    add_test(NAME ArbtrieSQLTests COMMAND arbtrie_sql_tests)
endif() # End BUILD_TESTS 