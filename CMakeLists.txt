cmake_minimum_required (VERSION 3.1...3.27)
project(ZombieHashTable)

option(NEW_BLOCKOFFSET "Use new blockoffset" ON)
set(VARIANT "RHM" CACHE STRING "")

if (NEW_BLOCKOFFSET)
  add_compile_definitions(-D_BLOCKOFFSET_4_NUM_RUNENDS)
endif()

if(VARIANT STREQUAL "RHM")
  add_compile_definitions(-DUSE_RHM)
elseif(VARIANT STREQUAL "TRHM")
  add_compile_definitions(-DUSE_TRHM -DQF_TOMBSTONE)
elseif (VARIANT STREQUAL "GRHM")
  add_compile_definitions(-DUSE_GRHM -DQF_TOMBSTONE -DAMORTIZED_REBUILD)
elseif (VARIANT STREQUAL "GRHM_NO_INSERT")
  add_compile_definitions(-DUSE_GRHM_NO_INSERT -DQF_TOMBSTONE -DREBUILD_NO_INSERT -DAMORTIZED_REBUILD)
elseif (VARIANT STREQUAL "GZHM")
  add_compile_definitions(-DUSE_GZHM -DQF_TOMBSTONE -DREBUILD_DEAMORTIZED_GRAVEYARD)
elseif (VARIANT STREQUAL "GZHM_NO_INSERT")
  add_compile_definitions(-DUSE_GZHM_NO_INSERT -DQF_TOMBSTONE -DREBUILD_NO_INSERT -DREBUILD_DEAMORTIZED_GRAVEYARD)
elseif (VARIANT STREQUAL "GZHM_INSERT")
  add_compile_definitions(-DUSE_GZHM_INSERT -DQF_TOMBSTONE -DREBUILD_AT_INSERT)
elseif (VARIANT STREQUAL "GZHM_DELETE")
  add_compile_definitions(-DUSE_GZHM_DELETE -DQF_TOMBSTONE -DDELETE_AND_PUSH)
elseif (VARIANT STREQUAL "ABSL")
  add_compile_definitions(-DUSE_ABSL)
else()
  message("Unkonwn Variant")
endif()


set(CMAKE_CXX_STANDARD 23)
set_source_files_properties(src/gqf.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(src/hm.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(src/hashutil.c PROPERTIES LANGUAGE CXX )

include_directories(
  include/
  tests/
)

add_library(hashutil src/hashutil.c)
add_library(gqf src/gqf.c)
add_library(hm src/hm.c)
add_library(pc src/partitioned_counter.c)
add_executable(hm_churn bench/hm_churn.cc)
target_link_libraries(hm_churn ssl crypto hm pc gqf hashutil)

if (VARIANT STREQUAL "ABSL")
  add_subdirectory(abseil-cpp)
  target_link_libraries(hm_churn ssl crypto absl::flat_hash_map)
else()
  target_link_libraries(hm_churn ssl crypto hm pc gqf hashutil)
endif()
