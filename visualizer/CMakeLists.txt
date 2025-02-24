cmake_minimum_required(VERSION 3.24)

# Find Qt packages
find_package(Qt6 COMPONENTS Core Widgets OpenGL OpenGLWidgets REQUIRED)

# Enable Qt's meta-object compiler
set(CMAKE_AUTOMOC ON)

# Add segment visualizer executable
add_executable(segment_visualizer 
    segment_visualizer.cpp
)

target_include_directories(segment_visualizer 
    PRIVATE 
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(segment_visualizer
    PRIVATE
        arbtrie
        Qt6::Core
        Qt6::Widgets
        Qt6::OpenGL
        Qt6::OpenGLWidgets
) 