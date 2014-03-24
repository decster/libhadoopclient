cmake_minimum_required (VERSION 2.6)

project (hadoopclient)

set(CMAKE_BUILD_TYPE Release)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(CMAKE_CXX_COMPILER "clang++")
  set(CMAKE_C_COMPILER "clang")
  set(CMAKE_CXX_COMPILER_ARG1 "-std=c++11 -stdlib=libc++ ")
else (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(CMAKE_CXX_COMPILER "g++")
  set(CMAKE_C_COMPILER "gcc")
  set(CMAKE_CXX_COMPILER_ARG1 "-std=c++0x ")
endif (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

set(CMAKE_CXX_FLAGS "-g ")

find_library(PROTOBUF_LIBRARY NAMES protobuf)
if (NOT PROTOBUF_LIBRARY)
  message(FATAL_ERROR "Required protobuf library")
endif (NOT PROTOBUF_LIBRARY)

find_library(JSONC_LIBRARY NAMES "json-c")
if (NOT JSONC_LIBRARY)
  message(FATAL_ERROR "Required json-c library")
endif (NOT JSONC_LIBRARY)

include_directories(depends protos src test)

add_definitions(-DHADOOP_CLIENT_INTERNAL)

#file(GLOB ProtoFiles "protos/*.proto")
#PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})

file(GLOB_RECURSE LIB_SOURCES protos/*.cc src/*.cc)
add_library(hadoopclient SHARED ${LIB_SOURCES})

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries(hadoopclient ${PROTOBUF_LIBRARY} ${JSONC_LIBRARY})
else (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  target_link_libraries(hadoopclient ${PROTOBUF_LIBRARY} ${JSONC_LIBRARY} -lrt)
endif (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")


file(GLOB_RECURSE TEST_SOURCES test/*.cc depends/gtest/gtest-all.cc)
add_executable(testhadoopclient ${TEST_SOURCES})
target_link_libraries(testhadoopclient hadoopclient)
