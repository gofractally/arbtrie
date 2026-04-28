# PsiTriLevelDB: LevelDB-compatible API backed by psitri direct API
add_library(psitrileveldb
    src/db_impl.cpp
)

target_include_directories(psitrileveldb
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(psitrileveldb PUBLIC psitri)

target_compile_features(psitrileveldb PUBLIC cxx_std_20)
