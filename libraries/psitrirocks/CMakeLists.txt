# PsiTriRocks: RocksDB-compatible API backed by psitri
add_library(psitrirocks
    src/db_impl.cpp
)

target_include_directories(psitrirocks
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(psitrirocks PUBLIC psitri)

target_compile_features(psitrirocks PUBLIC cxx_std_20)
