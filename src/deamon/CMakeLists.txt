# include(./CMake/FindSyscall_intercept.cmake)

set(PRELOAD_SRC
    main.cpp
    server.cpp
    )
# set(PRELOAD_HEADERS
#     include/preload.hpp
#     include/intercept.hpp
#     )

include_directories(../../include /usr/local/include/fuse3)

set(PRELOAD_LINK_LIBRARIES
    fuse3
    leveldb
    pthread
    )

find_package(PkgConfig)
find_package(leveldb REQUIRED)
pkg_search_module(fuse3 REQUIRED fuse3)

# add_compile_options("`pkg-config fuse3 --cflags --libs`")

#add_compile_options(-no-pie)

#find_package( CURL REQUIRED )
    
# find_package(fmt REQUIRED)

add_executable(fuse_deamon ${PRELOAD_SRC})

# set(CLIENT_LOG_MESSAGE_SIZE 1024 CACHE STRING "Maximum size of a log message in the client library")

# add_definitions(-DLIBGKFS_LOG_MESSAGE_SIZE=${CLIENT_LOG_MESSAGE_SIZE})

target_link_libraries(fuse_deamon ${PRELOAD_LINK_LIBRARIES})

# option(ENABLE_CLIENT_LOG "Enable logging messages" ON)
# if(ENABLE_CLIENT_LOG)
#     add_definitions(-DGKFS_ENABLE_LOGGING)
#     add_definitions(-DGKFS_DEBUG_BUILD)
# endif()