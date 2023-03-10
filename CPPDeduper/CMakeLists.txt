# CMakeList.txt : CMake project for CPPDeduper, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

project ("CPPDeduper")

list(APPEND CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/")
list(APPEND CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/cmake")
list(APPEND CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/cmake/Snappy")
find_package(Arrow REQUIRED)

#option(BUILD_SHARED_LIBS "Build shared libs" OFF) #optional
set(XXHASH_BUILD_ENABLE_INLINE_API ON) #optional
#set(XXHASH_BUILD_XXHSUM OFF) #optional

set(CMAKE_VERBOSE_MAKEFILE 1)

set(CMAKE_CXX_STANDARD C++20)

set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)

if ( CMAKE_COMPILER_IS_GNUCC )
    # -Werror
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=gnu++2a -Wall -Wextra -msse4.2 -DXXH_INLINE_ALL" ) #-fsanitize=undefined -fno-sanitize-recover 
endif()
if ( MSVC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} /W4 /EHsc") #/WX
endif()

ADD_CUSTOM_TARGET(debug
  COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Debug ${CMAKE_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target all
  COMMENT "Creating the executable in the debug mode.")

ADD_CUSTOM_TARGET(release
  COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release ${CMAKE_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target all
  COMMENT "Creating the executable in the release mode.")


# Add source to this project's executable.
# Local header files here ONLY
SET(TARGET_H
    ArrowLoaderThread.h 
    ComparerThread.h
    DupeResolverThread.h
    HasherThread.h
    Hashing.h
    isalphanum.h
    Jaccard.h
    LockableQueue.h
    CLI11.hpp
    Shingler32.hpp
    Shingler64.hpp
    xxh_x86dispatch.h
    xxh3.h
    xxhash.h
   )

# Local source files here
SET(TARGET_SRC
    CPPDeduper.cpp
    isalphanum.cc
    xxhash.c
    xxh_x86dispatch.c
    )

add_executable (CPPDeduper ${TARGET_SRC} ${TARGET_H}) 

target_link_libraries(CPPDeduper PRIVATE Arrow::arrow_static)

set_property(TARGET CPPDeduper PROPERTY CXX_STANDARD 20)

# TODO: Add tests and install targets if needed.
