cmake_minimum_required(VERSION 3.20)

project(pdgapi-diff CXX)

find_package(unofficial-sqlite3 REQUIRED)
find_package(cxxopts REQUIRED)
find_package(range-v3 REQUIRED)

set(CMAKE_CXX_STANDARD 20)

add_executable(pdgapi-diff main.cxx)

target_link_libraries(pdgapi-diff
  PUBLIC "-static"
  PRIVATE
    unofficial::sqlite3::sqlite3
    cxxopts::cxxopts
    range-v3::range-v3)
