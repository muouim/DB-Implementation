////
// @file file.cc
// @brief
// 实现文件功能
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/file.h>

namespace db {

int File::open(const char *path)
{
    // https://docs.microsoft.com/zh-cn/windows/win32/api/fileapi/nf-fileapi-createfilea
    // TODO:
    // 1. path修改成unicode，CreateFile
    // 2. buffer、overlap？
    //
    handle_ = ::CreateFileA(
        path,                               // 路径
        GENERIC_READ | GENERIC_WRITE,       // 访问权限
        FILE_SHARE_READ | FILE_SHARE_WRITE, // 与其它进程共享读写
        NULL,                               // 安全属性
        OPEN_ALWAYS,           // 打开已有文件，不存在文件则创建
        FILE_ATTRIBUTE_NORMAL, // 普通文件
        NULL);    
    if(handle_==INVALID_HANDLE_VALUE) {
       //std::cout<<"CreateFile error"<<std::endl;
        return 1;
    }

    return S_OK;
}

void File::close()
{
    if (handle_ != INVALID_HANDLE_VALUE) {
        ::CloseHandle(handle_);
        handle_ = INVALID_HANDLE_VALUE;
    }
}

int File::read(unsigned long long offset, char *buffer, size_t length)
{
    // https://docs.microsoft.com/zh-cn/windows/win32/api/fileapi/nf-fileapi-readfile
    DWORD len = 0; // 读长度
    OVERLAPPED over = {};
    over.Offset = (DWORD) offset;
    over.OffsetHigh = offset >> 32;
    bool ret = ::ReadFile(
        handle_,         // 文件句柄
        (LPVOID) buffer, // 读buffer
        (DWORD) length,  // buffer大小
        &len,            // 读长度
        &over);          // 偏移量
    return ret ? S_OK : ::GetLastError();
    // TODO: len == length??
}

int File::write(unsigned long long offset, const char *buffer, size_t length)
{
    // https://docs.microsoft.com/zh-cn/windows/win32/api/fileapi/nf-fileapi-writefile
    DWORD len = 0; // 写长度
    OVERLAPPED over = {};
    over.Offset = (DWORD) offset;
    over.OffsetHigh = offset >> 32;
    bool ret = ::WriteFile(
        handle_,        // 文件句柄
        buffer,         // 写buffer
        (DWORD) length, // buffer长度
        &len,           // 写长度返回值
        &over);         // 设定偏移量
    return ret ? S_OK : ::GetLastError();
    // TODO: len == length??
}

int File::remove(const char *path)
{
    // TODO: DeleteFile
    // https://docs.microsoft.com/zh-cn/windows/win32/api/fileapi/nf-fileapi-deletefilea
    bool ret = ::DeleteFileA(path);
    return ret ? S_OK : ::GetLastError();
}

int File::length(unsigned long long &len)
{
    // https://docs.microsoft.com/zh-cn/windows/win32/api/fileapi/nf-fileapi-getfilesizeex
    LARGE_INTEGER size;
    bool ret = ::GetFileSizeEx(handle_, &size);
    if (!ret)
        return ::GetLastError();
    else {
        len = size.QuadPart;
        return S_OK;
    }
}

} // namespace db