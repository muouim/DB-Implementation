////
// @file file.h
// @brief
// 文件操作
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_FILE_H__
#define __DB_FILE_H__

#include "./config.h"
#include <Windows.h>
#include <iostream>
#include <db/block.h>
#include <db/record.h>
namespace db {

class File
{
  public:
    HANDLE handle_; // 文件描述符句柄

  public:
    File()
        : handle_(INVALID_HANDLE_VALUE)
    {}
    ~File() { close(); }

    // 打开文件
    int open(const char *path);
    // 关闭文件
    void close();
    // 读文件
    int read(unsigned long long offset, char *buffer, size_t length);
    // 写文件
    int write(unsigned long long offset, const char *buffer, size_t length);
    // 文件长度
    int length(unsigned long long &len);
    // 删除文件
    static int remove(const char *path);
};

} // namespace db

#endif // __DB_FILE_H__
