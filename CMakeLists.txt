cmake_minimum_required(VERSION 3.10)
project (test)
SET(CMAKE_SYSTEM_NAME Linux)

include_directories(./include /usr/local/include/fuse3)
# SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)			# 设置了Debug版本和Release版本可执行文件的输出目录
# SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)			# 设置可执行文件的输出目录
# SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)			# 设置库文件的输出目录

include_directories (${PROJECT_SOURCE_DIR}/include)				# 附加头文件目录

# ADD_SUBDIRECTORY(${PROJECT_SOURCE_DIR}/libsrc)				# 先执行这个目录下的cmake
ADD_SUBDIRECTORY(${PROJECT_SOURCE_DIR}/src/client)	
ADD_SUBDIRECTORY(${PROJECT_SOURCE_DIR}/src/deamon)	
