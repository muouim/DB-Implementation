#ifndef __DB_INDEX_H__
#define __DB_INDEX_H__

#include "block.h"
#include "schema.h"
#include<stack>

namespace db {

class BplusTree
{

  protected:
    unsigned char *buffer_; // block对应的buffer
    unsigned char *rb;

    RelationInfo *relationinfo;
    int root_;
    size_t IndexBlockCnt;
    size_t level;
    int findToLeaf(struct iovec *record, size_t iovcnt,std::vector <int> &mystack);//find
    int sortSlots(Block &block,int iovcnt,unsigned char *sortbuffer,unsigned char header);
    int getRecord(struct iovec *iov, size_t offset, size_t iovcnt,unsigned char *tmpbuffer,
                  unsigned char *recordbuffer,unsigned char *header);
    int getBrother(int fatherid,int cunrrentid);

  public:
    bool isleaf;

    File indexfile_;//索引

    BplusTree() {
      buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
      rb = (unsigned char *) malloc(Block::BLOCK_SIZE);
    }
    BplusTree(const BplusTree &o) {
      buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
      this->indexfile_ = o.indexfile_;
      this->buffer_ =o.buffer_;
      std::cout<<"____________"<<std::endl;
    }
    ~BplusTree();

      
    int create(char *name,RelationInfo &info);
    int open(const char *name);
    std::pair<int,int> search(struct iovec *record, size_t iovcnt);
    int insert(struct iovec *record, size_t iovcnt,int change ,int rightid);
    int remove(struct iovec *record, size_t iovcnt);
    int remove(Block &block,int slotid);
    int update(struct iovec *record, size_t iovcnt,struct iovec *newrecord);
    int update(int blockid,int slotid,struct iovec *record, size_t iovcnt);

};

}

#endif // __DB_INDEX_H__
