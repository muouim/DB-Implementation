##
# @file CMakeLists.txt
# @brief
# src目录的cmake文件
#
# @author niexw
# @email xiaowen.nie.cn@gmail.com
#
include_directories(${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/src)

set(LIB_DB_IMPL integer.cc file.cc schema.cc block.cc record.cc datatype.cc timestamp.cc test.cc table.cc)
add_library(dbimpl STATIC ${LIB_DB_IMPL})

add_executable(udb ${LIB_DB_IMPL})

target_link_libraries(udb dbimpl)
# set(CMAKE_C_FLAGS "/D EXPORT ${CMAKE_C_FLAGS}")
# set(CMAKE_CXX_FLAGS "/D EXPORT ${CMAKE_CXX_FLAGS}")
