# MdbxRocks: RocksDB-compatible API backed by libmdbx
add_library(mdbxrocks
    src/db_impl.cpp
)

target_include_directories(mdbxrocks
    PUBLIC
        # Reuse the rocksdb headers from psitrirocks
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../psitrirocks/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(mdbxrocks PUBLIC mdbx-static)

target_compile_features(mdbxrocks PUBLIC cxx_std_20)
