
set(PRELOAD_SRC
    main.cpp
    server.cpp
    logging.cpp
    engine.cpp
    util.cpp
    )

include_directories(../../include /usr/local/include/fuse3)

set(PRELOAD_LINK_LIBRARIES
    fuse3
    leveldb
    pthread
    )

find_package(PkgConfig)
find_package(leveldb REQUIRED)
pkg_search_module(fuse3 REQUIRED fuse3)

add_executable(fuse_deamon ${PRELOAD_SRC})

target_link_libraries(fuse_deamon ${PRELOAD_LINK_LIBRARIES})
