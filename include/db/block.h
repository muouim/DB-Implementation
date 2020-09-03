////
// @file block.h
// @brief
// 定义block
// block是记录、索引的存储单元。在MySQL和HBase中，存储单元与分配单位是分开的，一般来说，
// 最小分配单元要比block大得多。
// block的布局如下，每个slot占用2B，这要求block最大为64KB。由于记录和索引要求按照4B对
// 齐，BLOCK_DATA、BLOCK_TRAILER也要求4B对齐。
//
// +--------------------+
// |   common header    |
// +--------------------+
// |  data/index header |
// +--------------------+ <--- BLOCK_DATA
// |                    |
// |     data/index     |
// |                    |
// +--------------------+ <--- BLOCK_FREE
// |     free space     |
// +--------------------+
// |       slots        |
// +--------------------+ <--- BLOCK_TRAILER
// |      trailer       |
// +--------------------+
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_BLOCK_H__
#define __DB_BLOCK_H__
#include <algorithm>
#include "./checksum.h"
#include "./endian.h"
#include "./timestamp.h"
#include "./file.h"
#include <iostream>
#include <vector>
#include <math.h>
namespace db {

const short BLOCK_TYPE_DATA = 0;  // 数据
const short BLOCK_TYPE_INDEX = 1; // 索引
const short BLOCK_TYPE_META = 2;  // 元数据
const short BLOCK_TYPE_LOG = 3;   // wal日志

////
// @brief
// 根block
//
class Root
{
  public:
    static const int ROOT_SIZE = 1024 * 4; // root大小为4KB

#if BYTE_ORDER == LITTLE_ENDIAN
    static const int ROOT_MAGIC_NUMBER = 0x1ef0c6c1; // magic number
#else
    static const int ROOT_MAGIC_NUMBER = 0xc1c6f01e;  // magic number
#endif
    static const int ROOT_MAGIC_OFFSET = 0; // magic number偏移量
    static const int ROOT_MAGIC_SIZE = 4;   // magic number大小4B

    static const int ROOT_TYPE_OFFSET =
        ROOT_MAGIC_OFFSET + ROOT_MAGIC_SIZE; // 类型偏移量
    static const int ROOT_TYPE_SIZE = 2;     // 类型大小

    static const int ROOT_TIMESTAMP_OFFSET =
        ROOT_TYPE_OFFSET + ROOT_TYPE_SIZE;    // 时戳偏移量
    static const int ROOT_TIMESTAMP_SIZE = 8; // 时戳大小

    static const int ROOT_HEAD_OFFSET =
        ROOT_TIMESTAMP_OFFSET + ROOT_TIMESTAMP_SIZE; // 链头偏移量
    static const int ROOT_HEAD_SIZE = 4;             // 链头大小

    static const int ROOT_GARBAGE_OFFSET =
        ROOT_HEAD_OFFSET + ROOT_HEAD_SIZE;  // 空闲block链头偏移量
    static const int ROOT_GARBAGE_SIZE = 4; // 空闲block链头大小

    static const int ROOT_TRAILER_SIZE = 4; // checksum大小
    static const int ROOT_TRAILER_OFFSET =  // checksum偏移量
        ROOT_SIZE - ROOT_TRAILER_SIZE;

  protected:
    unsigned char *buffer_; // block对应的buffer

  public:
    Root()
        : buffer_(NULL)
    {}

    // 关联buffer
    inline void attach(unsigned char *buffer) { buffer_ = buffer; }
    // 清root
    void clear(unsigned short type);

    // 获取类型
    inline unsigned short getType()
    {
        unsigned short type;
        ::memcpy(&type, buffer_ + ROOT_TYPE_OFFSET, ROOT_TYPE_SIZE);
        return be16toh(type);
    }
    // 设定类型
    inline void setType(unsigned short type)
    {
        type = htobe16(type);
        ::memcpy(buffer_ + ROOT_TYPE_OFFSET, &type, ROOT_TYPE_SIZE);
    }

    // 获取时戳
    inline TimeStamp getTimeStamp()
    {
        TimeStamp ts;
        ::memcpy(&ts, buffer_ + ROOT_TIMESTAMP_OFFSET, ROOT_TIMESTAMP_SIZE);
        *((long long *) &ts) = be64toh(*((long long *) &ts));
        return ts;
    }
    // 设定时戳
    inline void setTimeStamp(TimeStamp ts)
    {
        *((long long *) &ts) = htobe64(*((long long *) &ts));
        ::memcpy(buffer_ + ROOT_TIMESTAMP_OFFSET, &ts, ROOT_TIMESTAMP_SIZE);
    }

    // 获取block链头
    inline unsigned int getHead()
    {
        unsigned int head;
        ::memcpy(&head, buffer_ + ROOT_HEAD_OFFSET, ROOT_HEAD_SIZE);
        head = be32toh(head);
        return head;
    }
    // 设定block链头
    inline void setHead(unsigned int head)
    {
        head = htobe32(head);
        ::memcpy(buffer_ + ROOT_HEAD_OFFSET, &head, ROOT_HEAD_SIZE);
    }

    // 获取空闲链头
    inline int getGarbage()
    {
        int garbage;
        ::memcpy(&garbage, buffer_ + ROOT_GARBAGE_OFFSET, ROOT_GARBAGE_SIZE);
        garbage = be32toh(garbage);
        return garbage;
    }
    // 设定空闲链头
    inline void setGarbage(int garbage)
    {
        garbage = htobe32(garbage);
        ::memcpy(buffer_ + ROOT_GARBAGE_OFFSET, &garbage, ROOT_GARBAGE_SIZE);
    }

    // 设定checksum
    inline void setChecksum()
    {
        unsigned int check = 0;
        ::memset(buffer_ + ROOT_TRAILER_OFFSET, 0, ROOT_TRAILER_SIZE);
        check = checksum32(buffer_, ROOT_SIZE);
        ::memcpy(buffer_ + ROOT_TRAILER_OFFSET, &check, ROOT_TRAILER_SIZE);
    }
    // 获取checksum
    inline unsigned int getChecksum()
    {
        unsigned int check = 0;
        ::memcpy(&check, buffer_ + ROOT_TRAILER_OFFSET, ROOT_TRAILER_SIZE);
        return check;
    }
    // 检验checksum
    inline bool checksum()
    {
        unsigned int sum = 0;
        sum = checksum32(buffer_, ROOT_SIZE);
        return !sum;
    }
};

// 通用block头部
class Block
{
  public:
    // 公共头部字段偏移量
        int  BLOCK_NEXT;

    static const int BLOCK_SIZE = 1024 * 16; // block大小为16KB

#if BYTE_ORDER == LITTLE_ENDIAN
    static const int BLOCK_MAGIC_NUMBER = 0x1ef0c6c1; // magic number
#else
    static const int BLOCK_MAGIC_NUMBER = 0xc1c6f01e; // magic number
#endif
    static const int BLOCK_MAGIC_OFFSET = 0; // magic number偏移量
    static const int BLOCK_MAGIC_SIZE = 4;   // magic number大小4B

    static const int BLOCK_SPACEID_OFFSET =
        BLOCK_MAGIC_OFFSET + BLOCK_MAGIC_SIZE; // 表空间id偏移量
    static const int BLOCK_SPACEID_SIZE = 4;   // 表空间id长度4B

    static const int BLOCK_NUMBER_OFFSET =
        BLOCK_SPACEID_OFFSET + BLOCK_SPACEID_SIZE; // blockid偏移量
    static const int BLOCK_NUMBER_SIZE = 4;        // blockid大小4B

    static const int BLOCK_NEXTID_OFFSET =
        BLOCK_NUMBER_OFFSET + BLOCK_NUMBER_SIZE; // 下一个blockid偏移量
    static const int BLOCK_NEXTID_SIZE = 4;      // 下一个blockid大小4B

    static const int BLOCK_TYPE_OFFSET =
        BLOCK_NEXTID_OFFSET + BLOCK_NEXTID_SIZE; // block类型偏移量
    static const int BLOCK_TYPE_SIZE = 2;        // block类型大小2B

    static const int BLOCK_SLOTS_OFFSET =
        BLOCK_TYPE_OFFSET + BLOCK_TYPE_SIZE; // slots[]数目偏移量
    static const int BLOCK_SLOTS_SIZE = 2;   // slosts[]数目大小2B

    static const int BLOCK_GARBAGE_OFFSET =
        BLOCK_SLOTS_OFFSET + BLOCK_SLOTS_SIZE; // 空闲链表偏移量
    static const int BLOCK_GARBAGE_SIZE = 2;   // 空闲链表大小2B

    static const int BLOCK_FREESPACE_OFFSET =
        BLOCK_GARBAGE_OFFSET + BLOCK_GARBAGE_SIZE; // 空闲空间偏移量
    static const int BLOCK_FREESPACE_SIZE = 2;     // 空闲空间大小2B

    static const int BLOCK_CHECKSUM_SIZE = 4; // checksum大小4B
    static const int BLOCK_CHECKSUM_OFFSET =
        BLOCK_SIZE - BLOCK_CHECKSUM_SIZE; // trailer偏移量

    static const short BLOCK_DEFAULT_FREESPACE =
        BLOCK_FREESPACE_OFFSET + BLOCK_FREESPACE_SIZE; // 空闲空间缺省偏移量
    static const int BLOCK_DEFAULT_CHECKSUM = 0xc70f393e;

  protected:
    unsigned char *buffer_; // block对应的buffer

  public:
    Block()
        : buffer_(NULL)
    {}
    Block(unsigned char *b)
        : buffer_(b)
    {}

    // 关联buffer
    inline void attach(unsigned char *buffer) { buffer_ = buffer; }
    // 清buffer
    void clear(int spaceid, int blockid);

    // 获取spaceid
    inline int spaceid()
    {
        int id;
        ::memcpy(&id, buffer_ + BLOCK_SPACEID_OFFSET, BLOCK_SPACEID_SIZE);
        return be32toh(id);
    }
    // 获取blockid
    inline int blockid()
    {
        int id;
        ::memcpy(&id, buffer_ + BLOCK_NUMBER_OFFSET, BLOCK_NUMBER_SIZE);
        return be32toh(id);
    }

    // 设置garbage
    inline void setGarbage(unsigned short garbage)
    {
        garbage = htobe16(garbage);
        ::memcpy(buffer_ + BLOCK_GARBAGE_OFFSET, &garbage, BLOCK_GARBAGE_SIZE);
    }
    // 获得garbage
    inline unsigned short getGarbage()
    {
        short garbage;
        ::memcpy(&garbage, buffer_ + BLOCK_GARBAGE_OFFSET, BLOCK_GARBAGE_SIZE);
        return be16toh(garbage);
    }

    // 设定nextid
    inline void setNextid(int id)
    {
        id = htobe32(id);
        ::memcpy(buffer_ + BLOCK_NEXTID_OFFSET, &id, BLOCK_NEXTID_SIZE);
    }
    // 获取nextid
    inline int getNextid()
    {
        int id;
        ::memcpy(&id, buffer_ + BLOCK_NEXTID_OFFSET, BLOCK_NEXTID_SIZE);
        BLOCK_NEXT=be32toh(id);
        return be32toh(id);
    }

    // 设定checksum
    inline void setChecksum()
    {
        unsigned int check = 0;
        ::memset(buffer_ + BLOCK_CHECKSUM_OFFSET, 0, BLOCK_CHECKSUM_SIZE);
        check = checksum32(buffer_, BLOCK_SIZE);
        ::memcpy(buffer_ + BLOCK_CHECKSUM_OFFSET, &check, BLOCK_CHECKSUM_SIZE);
    }
    // 获取checksum
    inline unsigned int getChecksum()
    {
        unsigned int check = 0;
        ::memcpy(&check, buffer_ + BLOCK_CHECKSUM_OFFSET, BLOCK_CHECKSUM_SIZE);
        return check;
    }
    // 检验checksum
    inline bool checksum()
    {
        unsigned int sum = 0;
        sum = checksum32(buffer_, BLOCK_SIZE);
        return !sum;
    }

    // 获取类型
    inline unsigned short getType()
    {
        unsigned short type;
        ::memcpy(&type, buffer_ + BLOCK_TYPE_OFFSET, BLOCK_TYPE_SIZE);
        return be16toh(type);
    }
    // 设定类型
    inline void setType(unsigned short type)
    {
        type = htobe16(type);
        ::memcpy(buffer_ + BLOCK_TYPE_OFFSET, &type, BLOCK_TYPE_SIZE);
    }

    // 设定slots[]数目
    inline void setSlotsNum(unsigned short count)
    {
        count = htobe16(count);
        ::memcpy(buffer_ + BLOCK_SLOTS_OFFSET, &count, BLOCK_SLOTS_SIZE);
    }
    // 获取slots[]数目
    inline unsigned short getSlotsNum()
    {
        unsigned short count;
        ::memcpy(&count, buffer_ + BLOCK_SLOTS_OFFSET, BLOCK_SLOTS_SIZE);
        return be16toh(count);
    }

    // 设定freespace偏移量
    inline void setFreespace(unsigned short freespace)
    {
        freespace = htobe16(freespace);
        ::memcpy(
            buffer_ + BLOCK_FREESPACE_OFFSET, &freespace, BLOCK_FREESPACE_SIZE);
    }
    // 获得freespace偏移量
    inline unsigned short getFreespace()
    {
        unsigned short freespace;
        ::memcpy(
            &freespace, buffer_ + BLOCK_FREESPACE_OFFSET, BLOCK_FREESPACE_SIZE);
        return be16toh(freespace);
    }
    // 获取freespace大小
    inline unsigned short getFreeLength()
    {
        unsigned short slots = getSlotsNum();
        unsigned short offset = getFreespace();
        unsigned short slots2 = // slots[]起始位置
            BLOCK_SIZE - slots * sizeof(unsigned short) - BLOCK_CHECKSUM_SIZE;
        if (offset >= slots2)
            return 0;
        else
            return BLOCK_SIZE - slots * sizeof(short) - BLOCK_CHECKSUM_SIZE -
                   offset;
    }

    // 设置slot存储的偏移量，从下向上开始，0....
    inline void setSlot(unsigned short index, unsigned short off)
    {
        unsigned short offset = BLOCK_SIZE - BLOCK_CHECKSUM_SIZE -
                                (index + 1) * sizeof(unsigned short);
        *((unsigned short *) (buffer_ + offset)) = off;
    }
    // 获取slot存储的偏移量
    inline unsigned short getSlot(unsigned short index)
    {
        unsigned short offset = BLOCK_SIZE - BLOCK_CHECKSUM_SIZE -
                                (index + 1) * sizeof(unsigned short);
        return *((unsigned short *) (buffer_ + offset));
    }

    // 分配记录及slots，返回false表示失败
    bool allocate(const unsigned char *header, struct iovec *iov, int iovcnt);
    //bool sortSlots();
        /*Table table;

        std::sort(table.begin(),table.end(),cmp1);
        return S_OK;
    }
    bool cmp1(Table::iterator a,Table::iterator b)
    {
        Record record_a = a.*();
        Record record_b = b.*();
        return 1;

    }*/
};

class MetaBlock : public Block
{
  public:
    static const int META_TABLES_OFFSET =
        BLOCK_FREESPACE_OFFSET + BLOCK_FREESPACE_SIZE; // 表个数偏移量
    static const int META_TABLES_SIZE = 4;             // 表个数大小4B

    static const short META_DEFAULT_FREESPACE =
        META_TABLES_OFFSET + META_TABLES_SIZE; // 空闲空间缺省偏移量

  public:
    void clear(unsigned int blockid);

    // 获得表个数
    inline unsigned int getTableCount()
    {
        unsigned int count;
        ::memcpy(&count, buffer_ + META_TABLES_OFFSET, META_TABLES_SIZE);
        return be32toh(count);
    }
    // 设定表个数
    inline void setTableCount(unsigned int count)
    {
        count = htobe32(count);
        ::memcpy(buffer_ + META_TABLES_OFFSET, &count, META_TABLES_SIZE);
    }
};

#if 0
// 数据block
class DataBlock : public Block
{
  public:
    // 数据block各字段偏移量
    static const int BLOCK_DATA_ROWS = BLOCK_NEXT + 8; // 记录个数，2B
    static const int BLOCK_DATA_PADDING =
        BLOCK_DATA_ROWS + 2; // data头部填充，2B
                             // TODO: 指向btree内部节点？
    static const int BLOCK_DATA_START = BLOCK_DATA_PADDING + 2; // 记录开始位置

  public:
    // 各字段位置
    inline unsigned short *rows()
    {
        return (unsigned short *) (this->buffer_ + BLOCK_DATA_ROWS);
    }
    inline char *data() { return (char *) (this->buffer_ + BLOCK_DATA_START); }
    inline unsigned short *slots()
    {
        return (
            unsigned short *) (this->buffer_ + getChecksum() - 2 * (*rows()));
    }
    // 空闲空间大小
    inline int freesize()
    {
        size_t size = (char *) slots() - data();
        return size / 4 * 4; // slots按2B对齐，可能需要在slots前填充2B
    }
};
#endif

} // namespace db

#endif // __DB_BLOCK_H__
