# include(./CMake/FindSyscall_intercept.cmake)
# set(helloworld_PROTOBUF_PROTOC_EXECUTABLE "/usr/local/bin/protoc" CACHE STRING "Protoc binary on host")
# set(helloworld_GRPC_CPP_PLUGIN_EXECUTABLE "/usr/local/bin/grpc_cpp_plugin" CACHE STRING "gRPC CPP plugin binary on host")
# set(GRPC_PROTO_GENS_DIR "../protos" CACHE STRING "Directory where to put generated files")
# set(PROTOBUF_INCLUDE_PATH "../" CACHE STRING "Directory where to put protobuf files")
# set(REL_FIL "lst_fuse_client.proto" CACHE STRING "Relative path to proto file")
# set(RELFIL_WE "lst_fuse_client" CACHE STRING "Relative path to proto file without extension")

# set(hw_proto_srcs "${GRPC_PROTO_GENS_DIR}/${RELFIL_WE}.pb.cc")
# set(hw_grpc_srcs "${GRPC_PROTO_GENS_DIR}/${RELFIL_WE}.grpc.pb.cc")

# add_custom_command(
#     OUTPUT "${GRPC_PROTO_GENS_DIR}/${RELFIL_WE}.grpc.pb.cc"
#            "${GRPC_PROTO_GENS_DIR}/${RELFIL_WE}.grpc.pb.h"
#            "${GRPC_PROTO_GENS_DIR}/${RELFIL_WE}.pb.cc"
#            "${GRPC_PROTO_GENS_DIR}/${RELFIL_WE}.pb.h"
#     COMMAND ${helloworld_PROTOBUF_PROTOC_EXECUTABLE}
#     ARGS --grpc_out=${GRPC_PROTO_GENS_DIR}
#       --cpp_out=${GRPC_PROTO_GENS_DIR}
#       --plugin=protoc-gen-grpc=${helloworld_GRPC_CPP_PLUGIN_EXECUTABLE}
#       ${PROTOBUF_INCLUDE_PATH}
#       ${REL_FIL}
#     WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#     DEPENDS ${helloworld_PROTOBUF_PROTOC_EXECUTABLE} ${helloworld_GRPC_CPP_PLUGIN_EXECUTABLE} ${PROTOBUF_INCLUDE_PATH} )

set(PRELOAD_SRC
    client.cpp
    connection.cpp
    logging.cpp
    main.cpp
    # ${hw_proto_srcs}
    # ${hw_grpc_srcs}
    )
# set(PRELOAD_HEADERS
#     include/preload.hpp
#     include/intercept.hpp
#     )

include_directories(../../include /usr/local/include/fuse3)

set(PRELOAD_LINK_LIBRARIES
    fuse3
    # grpc++
    )

find_package(PkgConfig)
pkg_search_module(fuse3 REQUIRED fuse3)

# add_compile_options("`pkg-config fuse3 --cflags --libs`")

#add_compile_options(-no-pie)

#find_package( CURL REQUIRED )
    
# find_package(fmt REQUIRED)

add_executable(fuse_client ${PRELOAD_SRC})

# set(CLIENT_LOG_MESSAGE_SIZE 1024 CACHE STRING "Maximum size of a log message in the client library")

# add_definitions(-DLIBGKFS_LOG_MESSAGE_SIZE=${CLIENT_LOG_MESSAGE_SIZE})

target_link_libraries(fuse_client ${PRELOAD_LINK_LIBRARIES})

# option(ENABLE_CLIENT_LOG "Enable logging messages" ON)
# if(ENABLE_CLIENT_LOG)
#     add_definitions(-DGKFS_ENABLE_LOGGING)
#     add_definitions(-DGKFS_DEBUG_BUILD)
# endif()