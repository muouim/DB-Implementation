////
// @file index.h
// @brief
// B+树索引
//
// @author 魏国立
// @email 2018120703018@uestc.edu.cn
//

#ifndef __DB_INDEX_H__
#define __DB_INDEX_H__

#include "block.h"
#include "schema.h"
#include <stack>

namespace db {

class BplusTree
{
  protected:
    unsigned char *buffer_;   // block缓冲模块
    unsigned char *rb;        // root缓冲模块
    std::vector<int> mystack; //路径栈

    //主键字段信息
    RelationInfo *relationinfo;
    DataType *dtype;
    db::RelationInfo getinfo;
    std::pair<Schema::TableSpace::iterator, bool> indexret_;

    int findToLeaf(
        struct iovec *record,
        size_t iovcnt,
        std::vector<int> &mystack); //找到索引Block的叶子节点
    int sortSlots(
        Block &block,
        size_t iovcnt,
        unsigned char *sortbuffer,
        unsigned char header); // Block内主键排序

    int getRecord(
        struct iovec *iov,
        size_t offset,
        size_t iovcnt,
        unsigned char *tmpbuffer,
        unsigned char *recordbuffer,
        unsigned char *header);                          //取用记录字段
    int remove(Block &block, int slotid, size_t iovcnt); //类内调用，删除记录
    int update(
        int blockid,
        int slotid,
        struct iovec *record,
        size_t iovcnt); //类内调用，更新记录

    int getBrother(
        int slotid,
        int fatherid,
        size_t iovcnt); //获取索引Block的兄弟节点
    int countBlockSize(
        Block &block,
        unsigned char *tmpbuffer,
        size_t iovcnt); //计算索引Block记录总长度
    int merge(
        Block &block1,
        unsigned char *buffer1,
        Block &block2,
        unsigned char *buffer2,
        Block &newblock,
        unsigned char *newbuffer,
        size_t iovcnt); //平衡/合并索引Block
    std::pair<int, int>
    findWay(struct iovec *record, size_t iovcnt); //查找记录及其父节点

  public:
    File indexfile_; //索引文件操作

    BplusTree()
    {
        buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
        rb = (unsigned char *) malloc(Block::BLOCK_SIZE);
    }
    BplusTree(const BplusTree &o)
    {
        buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
        this->indexfile_ = o.indexfile_;
        this->buffer_ = o.buffer_;
        std::cout << "____________" << std::endl;
    }
    ~BplusTree();

    int create(char *name, RelationInfo &info); //创建索引表
    int open(const char *name);                 //打开索引表
    std::pair<int, int> search(
        struct iovec *record,
        size_t iovcnt); // Table内调用，找到对应记录的数据Block
    int insert(
        struct iovec *record,
        size_t iovcnt,
        int change,
        int rightid); // Table内调用，插入对应索引记录
    int remove(
        struct iovec *record,
        size_t iovcnt); // Table内调用，删除对应索引记录
    int update(
        struct iovec *record,
        size_t iovcnt,
        struct iovec *newrecord); // Table内调用，更新对应索引记录
    int gettableBrother(
        struct iovec *record,
        size_t iovcnt,
        int slotid); // Table内调用，通过B+树索引找到数据Block兄弟节点
};

} // namespace db

#endif // __DB_INDEX_H__
