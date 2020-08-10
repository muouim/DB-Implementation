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
    RelationInfo *relationinfo;
    int root_;
    size_t IndexBlockCnt;
    size_t level;

    bool init();// init the tree
    bool adjustafterinsert();//adjust
    bool adbjustafterdelete();
    int FindToLeaf(struct iovec *record, size_t iovcnt,std::vector <int> &mystack);//find

  public:
    bool isleaf;

    File indexfile_;//索引

    BplusTree() {
      buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
    }
    BplusTree(const BplusTree &o) {
      buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
      this->indexfile_ = o.indexfile_;
      this->buffer_ =o.buffer_;
      std::cout<<"____________"<<std::endl;
    }
    ~BplusTree();

    bool isRoot(Block &block);
      
    int create(char *name,RelationInfo &info);
    int open(const char *name);
    int destroy(const char *name){
        return S_OK;
    }
    std::pair<int,int> search(struct iovec *record, size_t iovcnt);
    int insert(struct iovec *record, size_t iovcnt);
    int remove(struct iovec *record, size_t iovcnt);
    int remove(Block &block,int slotid);

    int sortSlots(Block &block,int iovcnt);
    int getRecord(struct iovec *iov, size_t offset, size_t iovcnt,unsigned char *recordbuffer,unsigned char *header);

};

}

#endif // __DB_INDEX_H__
