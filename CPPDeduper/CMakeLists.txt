# CMakeList.txt : CMake project for CPPDeduper, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

project ("CPPDeduper")

list(APPEND CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/")
list(APPEND CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/cmake")
list(APPEND CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/cmake/Snappy")
find_package(Arrow REQUIRED)

set(CMAKE_VERBOSE_MAKEFILE 1)

set(CMAKE_CXX_STANDARD C++20)
set(CMAKE_CXX_FLAGS "-std=gnu++2a") 

if ( CMAKE_COMPILER_IS_GNUCC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror")
endif()
if ( MSVC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} /W4 /WX /EHsc")
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
    CPPDeduper.h
    DupeResolverThread.h
    HasherThread.h
    Hashing.h
    isalphanum.h
    Jaccard.h
    LockableQueue.h
    
   )

# Local source files here
SET(TARGET_SRC
    CPPDeduper.cpp
    isalphanum.cc
    )

add_executable (CPPDeduper ${TARGET_SRC} ${TARGET_H}) 

target_link_libraries(CPPDeduper PRIVATE Arrow::arrow_shared)

set_property(TARGET CPPDeduper PROPERTY CXX_STANDARD 20)

# TODO: Add tests and install targets if needed.
