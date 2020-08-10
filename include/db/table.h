////
// @file table.h
// @brief
// 存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_TABLE_H__
#define __DB_TABLE_H__
#include <db/file.h>
#include <db/block.h>
#include <db/schema.h>
#include <db/index.h>

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
      //friend class Table;
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

        iterator &operator++() {//前缀
          if(this->blockid==0)return *this;
          size_t blockoff_=(this->blockid-1)*Block::BLOCK_SIZE + Root::ROOT_SIZE;
          int slotid_=++this->sloti;

          //this->table->open("test.dat");
          if(slotid_>this->block_.getSlotsNum()-1) {
            this->blockid=block_.getNextid();
            if(this->blockid==0) {
              this->sloti=0;return *this;
            }
            this->sloti=0;
            Table *tetable=new Table();//怎么删除
            tetable->open("test.dat");
            blockoff_=(this->blockid-1)*Block::BLOCK_SIZE + Root::ROOT_SIZE;
            tetable->datafile_.read(blockoff_, (char *)tetable->buffer_ ,Block::BLOCK_SIZE);            
            setTable(tetable);
            this->block_.attach(this->table->buffer_);
            std::cout<<"new number "<<this->blockid<<" "<<this->block_.getNextid()<<std::endl;

          }return *this;          
        }
        iterator operator++(int) { // 后缀
          iterator tmp(*this);
          operator++(); // 当前的迭代器做+1
          return tmp;   // 返回的是+1之前的迭代器
        }
        void setid(int blockid_,int slotid_) {
          this->blockid=blockid_;
          this->sloti=slotid_;
        }
        int getblockid() {return blockid;}
        int getslotid() {return sloti;}

        void readTable() {}
        void openTable() {table->open("test.dat");}
        void setTable(Table *a) {table=a; }

        bool operator==(const iterator &rhs) const { 
          if(this->blockid==0&&rhs.blockid==0) return true;
          else if(this->table==rhs.table&&this->blockid==rhs.blockid&&this->sloti==rhs.sloti) return true;
          else return false;
        }
        bool operator!=(const iterator &rhs) const {
          if(this->blockid==0&&rhs.blockid==0) return false;
          else if(this->blockid!=rhs.blockid||this->sloti!=rhs.sloti) return true;
          else return false;
        }
        Record &operator*() {
          // 得到记录
          size_t offset_=Root::ROOT_SIZE+(this->blockid-1)* Block::BLOCK_SIZE+ block_.getSlot(this->sloti);
          unsigned char *rb = (unsigned char *) malloc(Block::BLOCK_SIZE);
          this->table->datafile_.read(offset_,(char *)rb,2);
          size_t length = 0;
          Integer it;
          int ret = it.decode((char *)rb, 2);
          length = it.get();
          this->table->datafile_.read(offset_,(char *)rb,length);
          record.attach(rb, unsigned short(length));
          return record;
        }
    };
    friend struct iterator;
  private:
    unsigned char *buffer_; // block，TODO: 缓冲模块

  public:
    // 打开一张表
    File datafile_;
    db::BplusTree index;

    Table() {
      buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
    }
    Table(const Table &o) {
      buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
      this->datafile_ = o.datafile_;
      this->buffer_ =o.buffer_;
      std::cout<<"____________"<<std::endl;
    }
    ~Table() {
      /*Block block;
      block.attach(buffer_);
      block.setChecksum();
      std::cout<<"block id: "<<block.blockid()<<std::endl;
      std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup("test.dat");
      db::RelationInfo getinfo;
      getinfo=ret_.first->second;
      if(block.blockid()>0)sortSlots(block,(int)getinfo.count);
 
      datafile_.write(Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);*/
      free(buffer_); 
    }
    int create(char *name,RelationInfo &info);
    int open(const char *name);

    // 插入一条记录
    int insert(struct iovec *record, size_t iovcnt);
    int update(int blockid,int slotid,struct iovec *record, size_t iovcnt);
    int remove(int blockid,int slotid);


    int getRecord(struct iovec *iov, size_t offset,size_t iovcnt, unsigned char *recordbuffer);

    int sortSlots(Block &block,int iovcnt) ;
    std::pair<int,int> findkey(struct iovec *key,int iovcnt);
    // begin, end
    iterator begin() {

      Root root_;
      unsigned char *rootbuffer= new unsigned char[Root::ROOT_SIZE];
      this->datafile_.read(0, (char *)rootbuffer,Root::ROOT_SIZE);
      root_.attach(rootbuffer);
      
      Block block_;

      int slotid_=0;
      int blockid_=root_.getHead();
      this->datafile_.read(Root::ROOT_SIZE+(blockid_-1)* Block::BLOCK_SIZE, (char *)this->buffer_,Block::BLOCK_SIZE);
      block_.attach(this->buffer_);

      iterator it_(blockid_,slotid_);
      it_.setTable(this);
      it_.block_.attach(this->buffer_);

      return it_;
    }
    iterator end() {

      iterator it_(0,0);
      it_.setTable(this);
      return it_;
    }

    //指定block
    iterator begin(int Iblockid) {
      Root root_;
      unsigned char *rootbuffer= new unsigned char[Root::ROOT_SIZE];
      this->datafile_.read(0, (char *)rootbuffer,Root::ROOT_SIZE);
      root_.attach(rootbuffer);
      
      Block block_;
      this->datafile_.read(Root::ROOT_SIZE+(Iblockid-1)*Block::BLOCK_SIZE, (char *)this->buffer_,Block::BLOCK_SIZE);
      block_.attach(this->buffer_);
      int blockid_=Iblockid;
      int slotid_=0;

      iterator it_(blockid_,slotid_);
      it_.setTable(this);
      it_.block_.attach(this->buffer_);
      return it_;
    }


    iterator end(int Iblockid) {
      Root root_;
      unsigned char *rootbuffer= new unsigned char[Root::ROOT_SIZE];
      this->datafile_.read(0, (char *)rootbuffer,Root::ROOT_SIZE);
      root_.attach(rootbuffer);
      
      Block block_;
      this->datafile_.read(Root::ROOT_SIZE+(Iblockid)*Block::BLOCK_SIZE, (char *)this->buffer_,Block::BLOCK_SIZE);
      block_.attach(this->buffer_);
      int slotid_=0;
      int blockid_=block_.getNextid();
      iterator it_(blockid_,0);
      it_.setTable(this);
      return it_;
    }
};

/*struct Compare
    {
        FieldInfo *fin;

        Compare(FieldInfo *f)
            : fin(f)
        {}
        bool operator()(unsigned short x, unsigned short y,Table *table) const
        {
            // 根据x,y偏移量，引用两条记录；
            const Record *rx = ...;
            const Record *ry = ...;
            size_t lx = ;
            size_t ly = ;
            fin->compare(rx, ry, lx, ly);
            return fasle;
        }
    };*/
} // namespace db

#endif // __DB_TABLE_H__
      
      /*File tempfile;
      Root root;
      unsigned char *rb= new unsigned char[Root::ROOT_SIZE];
      
      tempfile.open("test.dat");
      tempfile.read(0, (char *)rb,Root::ROOT_SIZE);
      root.attach(rb);
      int blockoff_=root.getHead();

   
      delete[]rb;
      rb=new unsigned char[Block::BLOCK_SIZE];
      tempfile.read(blockoff_, (char *)rb,Block::BLOCK_SIZE);

      Block tempblock;

      tempblock.attach(rb);
      
      
                //if(*this==end())return *this;
          iterator it_(0,0);
          Block block_;
          int blockoff_=this->blockid*Block::BLOCK_SIZE + Root::ROOT_SIZE;
          int blockid_=this->blockid;
          int slotid_=this->sloti++;
          this->datafile_.read(blockoff_, (char *)it_.table->buffer_,Block::BLOCK_SIZE);
          block_.attach(tempbuffer);


          if(slotid_>block_.getSlotsNum()) {
            blockid_=this->blockid++;
            slotid_=0;
          }

          it(blockoff_,slotid_);

          //std::cout<<block_.getSlot(0)<<std::endl;
          return it;   */


                //载入root
     /* Root root;
      unsigned char *rootbuffer= new unsigned char[Root::ROOT_SIZE];
      this->datafile_.read(0, (char *)rootbuffer,Root::ROOT_SIZE);
      root.attach(rootbuffer);
      int blockid_=1;

      //载入最后block
      Block block_;
      int blockoff_=Root::ROOT_SIZE+(blockid_-1)*Block::BLOCK_SIZE-Block::BLOCK_SIZE;
      this->datafile_.read(blockoff_,(char *)this->buffer_,Block::BLOCK_SIZE);
      block_.attach(this->buffer_);
      while(block_.getNextid()!=0) {
        blockid_++;
        blockoff_=Root::ROOT_SIZE+(blockid_-1)*Block::BLOCK_SIZE-Block::BLOCK_SIZE;
        this->datafile_.read(blockoff_,(char *)this->buffer_,Block::BLOCK_SIZE);
        block_.attach(this->buffer_);
      }

      int slotid_=block_.getSlotsNum();*/

              /*void setTable(){ 

        }
        void openTable() {

          table->open("test.dat");
        }
        void readTable() {
          
        }
                  Block block_;

          Table table;
          table.open("test.dat");
          this->table=&table;
          this->table->datafile_.read(Root::ROOT_SIZE+(this->blockid-1)*Block::BLOCK_SIZE, (char *)this->table->buffer_ ,Block::BLOCK_SIZE);
          block_.attach(this->table->buffer_);

        
        */