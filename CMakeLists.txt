cmake_minimum_required(VERSION 3.23)
project(graph)

include_directories(include)
set(CMAKE_CXX_STANDARD 11)

link_directories(${PROJECT_SOURCE_DIR}/lib)
add_executable(graph ${PROJECT_SOURCE_DIR}/src/main.cpp ${PROJECT_SOURCE_DIR}/src/json11.cpp  ${PROJECT_SOURCE_DIR}/src/util.cpp)
target_link_libraries(graph leveldb gflags)

