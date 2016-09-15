cmake_minimum_required (VERSION 3.0)

project (
    cpp_consumer
    VERSION 0.5.0
    LANGUAGES CXX C
)
add_executable(cpp_consumer main.cpp cpp_consumer.cpp pg_pb3.pb.cc)

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pg_pb3.proto
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMAND head -2 ../pg_pb3.proto > pg_pb3.proto
    COMMAND echo 'package pg_pb3\;' >> pg_pb3.proto
    COMMAND tail -n +2 ../pg_pb3.proto >> pg_pb3.proto
    MAIN_DEPENDENCY ${CMAKE_CURRENT_BINARY_DIR}/../pg_pb3.proto
)

add_custom_command (
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pg_pb3.pb.cc
    COMMAND protoc -I=${CMAKE_CURRENT_BINARY_DIR}/ --cpp_out=${CMAKE_CURRENT_BINARY_DIR}/ ${CMAKE_CURRENT_BINARY_DIR}/pg_pb3.proto
    MAIN_DEPENDENCY pg_pb3.proto
)

find_package(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIRS})
target_link_libraries(cpp_consumer ${PROTOBUF_LIBRARIES})

# The PostgreSQL package requires a bunch of server-side stuff we won't need
# (and doesn't even appear to work very well for that).  This code sucks, but I
# don't know how to make it better.  Patches welcome.

find_program (
    PG_CONFIG
    pg_config
)
if (PG_CONFIG STREQUAL "PG_CONFIG-NOTFOUND")
    message(FATAL_ERROR "pg_config not found!")
endif()
execute_process(COMMAND ${PG_CONFIG} --includedir OUTPUT_VARIABLE LIBPQ_INCLUDE_DIRS)
execute_process(COMMAND ${PG_CONFIG} --cppflags OUTPUT_VARIABLE LIBPQ_CPPFLAGS_RAW)
string(REPLACE "\n" "" LIBPQ_CPPFLAGS ${LIBPQ_CPPFLAGS_RAW})
execute_process(COMMAND ${PG_CONFIG} --ldflags OUTPUT_VARIABLE LIBPQ_LDFLAGS_RAW)
string(REPLACE "\n" "" LIBPQ_LDFLAGS ${LIBPQ_LDFLAGS_RAW})

include_directories(${LIBPQ_INCLUDE_DIRS})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${LIBPQ_CPPFLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${LIBPQ_LDFLAGS} -lpq")

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if(COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
endif()
