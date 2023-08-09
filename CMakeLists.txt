cmake_minimum_required(VERSION 3.23)

project(pdgapi_diff_pp CXX)

find_package(unofficial-sqlite3 REQUIRED)
# find_package(fmt REQUIRED)
find_package(cxxopts REQUIRED)

set(CMAKE_CXX_STANDARD 20)

add_executable(pdgapi_diff_pp main.cxx)

target_link_libraries(pdgapi_diff_pp
  PRIVATE
    unofficial::sqlite3::sqlite3
    cxxopts::cxxopts)
