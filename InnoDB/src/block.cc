////
// @file block.cc
// @brief
// 实现block
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/block.h>
#include <db/record.h>

namespace db {

void Block::clear(int spaceid, int blockid)
{
    spaceid = htobe32(spaceid);
    blockid = htobe32(blockid);
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + BLOCK_MAGIC_OFFSET, &BLOCK_MAGIC_NUMBER, BLOCK_MAGIC_SIZE);
    // 设置spaceid
    ::memcpy(buffer_ + BLOCK_SPACEID_OFFSET, &spaceid, BLOCK_SPACEID_SIZE);
    // 设置blockid
    ::memcpy(buffer_ + BLOCK_NUMBER_OFFSET, &blockid, BLOCK_NUMBER_SIZE);
    // 设置freespace
    unsigned short data = htobe16(BLOCK_DEFAULT_FREESPACE);
    ::memcpy(buffer_ + BLOCK_FREESPACE_OFFSET, &data, BLOCK_FREESPACE_SIZE);
    // 设置checksum
    int checksum = BLOCK_DEFAULT_CHECKSUM;
    ::memcpy(buffer_ + BLOCK_CHECKSUM_OFFSET, &checksum, BLOCK_CHECKSUM_SIZE);
}

void Root::clear(unsigned short type)
{
    // 清buffer
    ::memset(buffer_, 0, ROOT_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + Block::BLOCK_MAGIC_OFFSET,
        &Block::BLOCK_MAGIC_NUMBER,
        Block::BLOCK_MAGIC_SIZE);
    // 设定类型
    setType(type);
    // 设定时戳
    TimeStamp ts;
    ts.now();
    setTimeStamp(ts);
    // 设置checksum
    setChecksum();
}

void MetaBlock::clear(unsigned int blockid)
{
    unsigned int spaceid = 0xffffffff; // -1表示meta
    blockid = htobe32(blockid);
    // 清buffer
    ::memset(buffer_, 0, BLOCK_SIZE);
    // 设置magic number
    ::memcpy(
        buffer_ + BLOCK_MAGIC_OFFSET, &BLOCK_MAGIC_NUMBER, BLOCK_MAGIC_SIZE);
    // 设置spaceid
    ::memcpy(buffer_ + BLOCK_SPACEID_OFFSET, &spaceid, BLOCK_SPACEID_SIZE);
    // 设置blockid
    ::memcpy(buffer_ + BLOCK_NUMBER_OFFSET, &blockid, BLOCK_NUMBER_SIZE);
    // 设置freespace
    unsigned short data = htobe16(META_DEFAULT_FREESPACE);
    ::memcpy(buffer_ + BLOCK_FREESPACE_OFFSET, &data, BLOCK_FREESPACE_SIZE);
    // 设定类型
    setType(BLOCK_TYPE_META);
    // 设置checksum
    setChecksum();
}

bool Block::allocate(const unsigned char *header, struct iovec *iov, int iovcnt)
{
    // 判断是否有空间
    unsigned short length = getFreeLength();
    if (length == 0) return false;

    // 判断能否分配
    std::pair<size_t, size_t> ret = Record::size(iov, iovcnt);
    length -= 2; // 一个slot占2字节
    if (ret.first > length) return false;

    // 写入记录
    Record record;
    unsigned short oldf = getFreespace();
    record.attach(buffer_ + oldf, length);
    unsigned short pos = (unsigned short) record.set(iov, iovcnt, header);

    // 调整freespace
    setFreespace(pos + oldf);
    // 写slot
    unsigned short slots = getSlotsNum();
    setSlotsNum(slots + 1); // 增加slots数目
    setSlot(slots, oldf);   // 第slots个
    //sortSlots();
    // TODO:slots未排序 std::sort，同时需要setChecksum
    return true;
}