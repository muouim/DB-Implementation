////
// @file table.h
// @brief
// 存储管理
//
// @author 魏国立
// @email 2018120703018@uestc.edu.cn
//
#ifndef __DB_TABLE_H__
#define __DB_TABLE_H__
#include <db/file.h>
#include <db/block.h>
#include <db/schema.h>
#include "./config.h"

namespace db {

////
// @brief
// 表操作接口
//

class Table
{
  public:
    // 表的迭代器
    struct iterator
    {
        // friend class Table;
      private:
        unsigned int blockid; // block位置
        unsigned short sloti; // slots[]索引
        Table *table;

      public:
        int unsigned pointer;
        Record record;
        Block block_;

        iterator(unsigned int bid, unsigned short si)
            : blockid(bid)
            , sloti{si}
        {}
        iterator(const iterator &o)
            : blockid(o.blockid)
            , sloti(o.sloti)
            , table(o.table)
            , block_(o.block_)
        {}

        iterator &operator++()
        { //前缀
            if (this->blockid == 0) return *this;
            size_t blockoff_ =
                (this->blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            int slotid_ = ++this->sloti;

            if (slotid_ > this->block_.getSlotsNum() - 1) {
                this->blockid = block_.getNextid();
                if (this->blockid == 0) this->sloti = 0;
                this->sloti = 0;
                Table *tetable = new Table(); //怎么删除
                tetable->open("test.dat");
                blockoff_ =
                    (this->blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
                tetable->datafile_.read(
                    blockoff_, (char *) tetable->buffer_, Block::BLOCK_SIZE);
                setTable(tetable);
                this->block_.attach(this->table->buffer_);
            }
            return *this;
        }
        iterator operator++(int)
        { // 后缀
            iterator tmp(*this);
            operator++(); // 当前的迭代器做+1
            return tmp;   // 返回的是+1之前的迭代器
        }
        void setid(int blockid_, int slotid_)
        {
            this->blockid = blockid_;
            this->sloti = slotid_;
        }
        int getblockid() { return blockid; }
        int getslotid() { return sloti; }

        void readTable() {}
        void openTable() { table->open("test.dat"); }
        void setTable(Table *a) { table = a; }

        bool operator==(const iterator &rhs) const
        {
            if (this->blockid == 0 && rhs.blockid == 0)
                return true;
            else if (
                this->table == rhs.table && this->blockid == rhs.blockid &&
                this->sloti == rhs.sloti)
                return true;
            else
                return false;
        }
        bool operator!=(const iterator &rhs) const
        {
            if (this->blockid == 0 && rhs.blockid == 0)
                return false;
            else if (this->blockid != rhs.blockid || this->sloti != rhs.sloti)
                return true;
            else
                return false;
        }
        Record &operator*()
        {
            // 得到记录
            size_t offset_ = Root::ROOT_SIZE +
                             (this->blockid - 1) * Block::BLOCK_SIZE +
                             block_.getSlot(this->sloti);
            unsigned char *rb = (unsigned char *) malloc(Block::BLOCK_SIZE);
            this->table->datafile_.read(offset_, (char *) rb, 2);
            size_t length = 0;
            Integer it;
            int ret = it.decode((char *) rb, 2);
            length = it.get();
            this->table->datafile_.read(offset_, (char *) rb, length);
            record.attach(rb, unsigned short(length));
            return record;
        }
    };
    friend struct iterator;

  private:
    unsigned char *buffer_; // block缓冲模块
    unsigned char *rb;      // root缓冲模块

    int getRecord(
        struct iovec *iov,
        size_t offset,
        size_t iovcnt,
        unsigned char *recordbuffer); //获取记录字段

    int sortSlots(Block &block, int iovcnt); // Block内主键排序
  public:
    // 打开一张表
    File datafile_;
    Table()
    {
        buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
        rb = (unsigned char *) malloc(Root::ROOT_SIZE);
    }
    Table(const Table &o)
    {
        buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
        this->datafile_ = o.datafile_;
        this->buffer_ = o.buffer_;
        std::cout << "____________" << std::endl;
    }
    ~Table()
    {
        free(buffer_);
        free(rb);
    }
    int create(char *name, RelationInfo &info); //创建表
    int open(const char *name);                 //打开表

    int insert(struct iovec *record, size_t iovcnt); //插入记录
    int update(
        int blockid,
        int slotid,
        struct iovec *record,
        size_t iovcnt);                  //更新记录
    int remove(int blockid, int slotid); //删除记录

    std::pair<int, int> findkey(struct iovec *key, int iovcnt); // 主键查找

    //全局迭代器begin, end
    iterator begin()
    {
        Root root_;
        unsigned char *rootbuffer = new unsigned char[Root::ROOT_SIZE];
        this->datafile_.read(0, (char *) rootbuffer, Root::ROOT_SIZE);
        root_.attach(rootbuffer);

        Block block_;

        int slotid_ = 0;
        int blockid_ = root_.getHead();
        this->datafile_.read(
            Root::ROOT_SIZE + (blockid_ - 1) * Block::BLOCK_SIZE,
            (char *) this->buffer_,
            Block::BLOCK_SIZE);
        block_.attach(this->buffer_);

        iterator it_(blockid_, slotid_);
        it_.setTable(this);
        it_.block_.attach(this->buffer_);

        return it_;
    }
    iterator end()
    {
        iterator it_(0, 0);
        it_.setTable(this);
        return it_;
    }

    //指定block
    iterator begin(int Iblockid)
    {
        Root root_;
        unsigned char *rootbuffer = new unsigned char[Root::ROOT_SIZE];
        this->datafile_.read(0, (char *) rootbuffer, Root::ROOT_SIZE);
        root_.attach(rootbuffer);

        Block block_;
        this->datafile_.read(
            Root::ROOT_SIZE + (Iblockid - 1) * Block::BLOCK_SIZE,
            (char *) this->buffer_,
            Block::BLOCK_SIZE);
        block_.attach(this->buffer_);
        int blockid_ = Iblockid;
        int slotid_ = 0;

        iterator it_(blockid_, slotid_);
        it_.setTable(this);
        it_.block_.attach(this->buffer_);
        return it_;
    }

    iterator end(int Iblockid)
    {
        Root root_;
        unsigned char *rootbuffer = new unsigned char[Root::ROOT_SIZE];
        this->datafile_.read(0, (char *) rootbuffer, Root::ROOT_SIZE);
        root_.attach(rootbuffer);

        Block block_;
        this->datafile_.read(
            Root::ROOT_SIZE + (Iblockid) *Block::BLOCK_SIZE,
            (char *) this->buffer_,
            Block::BLOCK_SIZE);
        block_.attach(this->buffer_);
        int slotid_ = 0;
        int blockid_ = block_.getNextid();
        iterator it_(blockid_, 0);
        it_.setTable(this);
        return it_;
    }
};
} // namespace db

#endif // __DB_TABLE_H__
