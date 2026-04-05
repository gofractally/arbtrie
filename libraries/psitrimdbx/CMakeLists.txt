# PsiTriMDBX: MDBX-compatible API backed by psitri
add_library(psitrimdbx
    src/mdbx_impl.cpp
)

target_include_directories(psitrimdbx
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(psitrimdbx PUBLIC psitri)

target_compile_features(psitrimdbx PUBLIC cxx_std_20)
