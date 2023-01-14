# CMakeList.txt : CMake project for CPPDeduper, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

project ("CPPDeduper")

find_package(Arrow REQUIRED)

set(CMAKE_CXX_STANDARD 20)

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

add_executable (CPPDeduper ${TARGET_SRC} ${TARGET_H}) #"CPPDeduper.cpp" "CPPDeduper.h")

target_link_libraries(CPPDeduper PRIVATE Arrow::arrow_shared)

set_property(TARGET CPPDeduper PROPERTY CXX_STANDARD 20)

# TODO: Add tests and install targets if needed.
