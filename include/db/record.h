////
// @file record.h
// @brief
// 定义数据库记录
// 采用类似MySQL的记录方案，一条记录分为四个部分：
// 记录总长度+字段长度数组+Header+字段
// 1. 记录总长度从最开始算；
// 2. 字段长度是一个逆序变长数组，记录每条记录从Header开始的头部偏移量位置；
// 3. Header存放一些相关信息，1B；
// 4. 然后是各字段顺序摆放；
//
// | T | M | x | x | x | x | x | x |
//   ^   ^
//   |   |
//   |   +-- 最小记录
//   +-- tombstone
//
// 记录的分配按照4B对齐，同时要求block头部至少按照4B对齐
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_RECORD_H__
#define __DB_RECORD_H__

#include <utility>
#include "./config.h"
#include "./integer.h"

namespace db {

// 物理记录
// TODO: 长度超越一个block？
class Record
{
  public:
    static const int HEADER_SIZE = 1; // 头部1B
    static const int ALIGN_SIZE = 8;  // 按8B对齐

    static const int BYTE_TOMBSTONE = 1; // tombstone在header的第1字节
    static const unsigned char MASK_TOMBSTONE = 0x80; // tombstone掩码
    static const int BYTE_MIMIMUM = 1; // 最小记录标记在header的第1字节
    static const unsigned char MASK_MINIMUM = 0x40; // 最小记录掩码

  private:
    unsigned char *buffer_; // 记录buffer
    unsigned short length_; // buffer长度

  public:
    Record()
        : buffer_(NULL)
        , length_(0)
    {}

    // 关联buffer
    inline void attach(unsigned char *buffer, unsigned short length)
    {
        buffer_ = buffer;
        length_ = length;
    }
    // 整个记录长度+header偏移量
    static std::pair<size_t, size_t> size(const iovec *iov, int iovcnt);

    // 向buffer里写各个域，返回按照对齐后的长度
    size_t set(const iovec *iov, int iovcnt, const unsigned char *header);
    // 从buffer获取各字段
    bool get(iovec *iov, int iovcnt, unsigned char *header);
    // 从buffer引用各字段
    bool ref(iovec *iov, int iovcnt, unsigned char *header);
    // TODO:
    void dump(char *buf, size_t len);

    // 获得记录总长度，包含头部+变长偏移数组+长度+记录
    size_t length();
    // 获取记录字段个数
    size_t fields();
};

} // namespace db

#endif // __DB_RECORD_H__
