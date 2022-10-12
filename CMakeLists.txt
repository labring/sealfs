cmake_minimum_required(VERSION 3.10)
project (test)
SET(CMAKE_SYSTEM_NAME Linux)

include_directories(./include /usr/local/include/fuse3)

include_directories (${PROJECT_SOURCE_DIR}/include)

ADD_SUBDIRECTORY(${PROJECT_SOURCE_DIR}/src/client)	
ADD_SUBDIRECTORY(${PROJECT_SOURCE_DIR}/src/deamon)	
