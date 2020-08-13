#include <db/index.h>
std::string indexname("index.dat");

namespace db {

BplusTree::~BplusTree() {

}

int BplusTree::create(char *name,RelationInfo &info) {
    
    info.file=indexfile_;
    return gschema.create(name,info);
}
int BplusTree::open(const char *name) {

    // 查找schema
    gschema.open();
    std::pair<Schema::TableSpace::iterator, bool> ret = gschema.lookup(name);
    gschema.load(ret.first);
    if (!ret.second) return EINVAL;
    int ret_ = indexfile_.open(name);
    unsigned long long length;
    ret_= indexfile_.length(length);   
   
    // 如果meta.db长度为0，则写一个block
    if (ret_) return ret_;
    if (length) {
        // 加载ROOT
        indexfile_.read(0, (char *) buffer_, Root::ROOT_SIZE);
        // 获取第1个block
        Root root;
        root.attach(buffer_);
        unsigned int first = root.getHead();
        size_t offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    } else {
        // 创建root
        Root root;
        root.attach(rb);
        root.clear(BLOCK_TYPE_META);
        root.setHead(1);
        root.setGarbage(2);
        // 创建第1个block
        Block block;
        block.attach(buffer_);
        block.clear(1,1);
        block.setNextid(-1);
        // 写root和block
        indexfile_.write(0, (const char *) rb, Root::ROOT_SIZE);
        indexfile_.write(Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    return S_OK;
    indexfile_.read(0, (char *) rb, Root::ROOT_SIZE);

    //schema.load()// 找到后，加载meta信息
}

bool BplusTree::init() {
    return S_OK;

}// init the tree

bool indexcompare(std::pair<unsigned short,struct iovec> x,  std::pair<unsigned short,struct iovec> y) {

    //载入数据类型
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(indexname.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//findDataType("INT");
    //比较
    return dtype->compare(x.second.iov_base,y.second.iov_base,x.second.iov_len,y.second.iov_len);
}
int BplusTree::sortSlots(Block &block,int iovcnt) {

    //载入relationinfo    
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(indexname.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;

    std::vector<std::pair<unsigned short,struct iovec>>keys;
    unsigned char header=0;
    std::cout <<"____________"<<std::endl;
    char  **base=new char*[block.getSlotsNum()];
    //取用所有主键字段
    std::vector<iovec*>deiov;
    for(int i = 0; i <block.getSlotsNum(); ++i) {
        
        unsigned char recordbuffer[Block::BLOCK_SIZE]={0};
        memcpy(recordbuffer,buffer_+block.getSlot(i),2);
        size_t length = 0;
        Integer it;Record record_;
        struct iovec *iov_=new struct iovec[iovcnt];
        int ret = it.decode((char *)recordbuffer, 2);
        if (!ret) {
            std::cout<<"error"<<std::endl;   
            return false;
        }
        length = it.get();
        unsigned char tempheader=1;
        memcpy(recordbuffer,buffer_+block.getSlot(i),length);
        memcpy(&header,recordbuffer, 1);

        record_.attach(recordbuffer, unsigned short(length));
        record_.ref(iov_,(int)iovcnt, &header);
        deiov.push_back(iov_);
        
        base[i]=new char[iov_[getinfo.key].iov_len];
        memcpy(base[i],iov_[getinfo.key].iov_base,iov_[getinfo.key].iov_len);
        //std::cout <<"base:"<<base[i]<<std::endl;

        std::pair<unsigned short,struct iovec>p(block.getSlot(i),iov_[getinfo.key]);
        //std::cout <<*(int *)p.second.iov_base<<" "<<p.second.iov_len<<std::endl;
        keys.push_back(p);//这里没有问题
    }
    std::cout <<"____________"<<std::endl;
    for(int i = 0; i <block.getSlotsNum(); ++i) 
        keys[i].second.iov_base=base[i];

    std::sort(keys.begin(), keys.end(),indexcompare);
    int tempslotid=0;
    for(int i=0; i<keys.size(); i++) {
        block.setSlot(tempslotid,keys[i].first);
        std::cout <<"Indexkey:"<<*(int *)keys[i].second.iov_base<<" "<<keys[i].first<<std::endl;
        tempslotid++;
    }
    for(int i = 0; i <deiov.size(); i++) delete[] deiov[i];
    for(int i = 0; i <block.getSlotsNum(); i++) delete[] base[i];
    delete[] base;

    /*block.setChecksum();
    datafile_.write(Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);*/
    return S_OK;
}
int BplusTree::getRecord(struct iovec *iov, size_t offset, size_t iovcnt,unsigned char *recordbuffer,unsigned char *header) {

    memcpy(recordbuffer,buffer_+offset,2);
    size_t length = 0;Integer it;Record record_;
    bool tempret = it.decode((char *) recordbuffer, 2);
    //if (!tempret) return false;
    length = it.get();
    memcpy(recordbuffer,buffer_+offset,length);
    memcpy(header,recordbuffer, 1);
    record_.attach(recordbuffer, unsigned short(length));
    record_.ref(iov,(int)iovcnt, header);
    return S_OK;
}

bool BplusTree::adjustafterinsert() {
    return S_OK;
}
bool BplusTree::adbjustafterdelete() {
    return S_OK;
}

bool BplusTree::isRoot(Block &block) {
    return S_OK;
}
      
std::pair<int,int> BplusTree::search(struct iovec *record, size_t iovcnt) {//header=1代表叶节点 header=0代表非叶节点，遍历时通过栈记录路径

    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(indexname.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//getinfo.fields[getinfo.key].type;
    
    std::vector <int> mystack;
    int blockid=FindToLeaf(record,iovcnt,mystack);

    int nextid=0;
    unsigned char header=1;
    size_t reoffset = 0;
    unsigned char recordbuffer[Block::BLOCK_SIZE];

    size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    db::Block block;block.attach(buffer_);
    struct iovec *iov_=new struct iovec[iovcnt];
    if(block.getSlotsNum()==0) {
        nextid=-1;
        blockid=-block.getNextid();
    }
    else for(int i = 0; i <block.getSlotsNum();i++) {//找到刚好比key大的，然后往下迭代找，直到找到叶子节
        reoffset = block.getSlot(i);
        getRecord(iov_,reoffset,iovcnt,recordbuffer,&header); 
        if(dtype->compare(record[getinfo.key].iov_base,iov_[getinfo.key].iov_base
                    ,record[getinfo.key].iov_len,iov_[getinfo.key].iov_len)) {//找到第一个比记录大的,它的左指针就是我们要找的blockid
            blockid=*(int*)iov_[1].iov_base;
            nextid=i;break;
        }
        if(i==block.getSlotsNum()-1) { //右指针,正数代表非叶节点，负数代表叶节点
            blockid=-block.getNextid();
            nextid=-1;
        }
    }
    //返回两个，如果右边有就返回右边的，没有就回溯mystack，往回找，因为是左指针，所以是后继，
    //不用管nextidid，如果使用nextid，可以直接使用nextid，因为是左指针
    std::pair<int ,int> result(blockid,nextid);
    delete [] iov_;
    return result;
}
int BplusTree::FindToLeaf(struct iovec *record, size_t iovcnt,std::vector <int> &mystack) {//找到要插入的叶子节点

    size_t offset=0;    
    int ret=0;unsigned char header=0;

    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(indexname.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//getinfo.fields[getinfo.key].type;

    Root root;
    root.attach(rb);
    unsigned int first = root.getHead();

    offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    db::Block block;block.attach(buffer_);
    mystack.push_back(block.blockid());

    if(block.getSlotsNum()==0)return block.blockid();

    size_t reoffset=0;    
    unsigned char recordbuffer[Block::BLOCK_SIZE];
    struct iovec *iov_=new struct iovec[iovcnt];
    int blockid=first;

    while(header==0) {
        for(int i = 0; i <block.getSlotsNum(); ++i) {
            reoffset = block.getSlot(i);
            getRecord(iov_,reoffset,iovcnt,recordbuffer,&header);
            
            if(dtype->compare(record[getinfo.key].iov_base,iov_[getinfo.key].iov_base
                    ,record[getinfo.key].iov_len,iov_[getinfo.key].iov_len)) {//找到第一个比记录大的
                
                if(header==0)blockid=*(int*)iov_[1].iov_base;//非叶节点
                else blockid=block.blockid();
                break;
            }
            if(i==block.getSlotsNum()-1) {//右指针,正数代表非叶节点，负数代表叶节点
                blockid=block.getNextid();
                if(blockid<0) {blockid=block.blockid();header=1;break;}//说明当前的是叶节点
                else header=0;
            }   
        }
        if(header==0) {
            mystack.push_back(blockid);
            offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
            block.attach(buffer_);
        }
    }
    delete [] iov_;
    return blockid;
}

int BplusTree::insert(struct iovec *record, size_t iovcnt, int change ,int rightid) {//header=1代表叶节点 header=0代表非叶节点；右指针nextid,正数代表非叶节点，负数代表叶节点
    //b+树采取的左指针，指向比当前主键小的block，最后一个block放在nextid里面
    size_t offset=0; 
    size_t reoffset=0;size_t newoffset=0;    

    int ret=0;unsigned char header=1;

    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(indexname.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//getinfo.fields[getinfo.key].type;

    Root root;
    root.attach(rb);
    struct iovec *iov_=new struct iovec[iovcnt];
    struct iovec *tempiov_=new struct iovec[iovcnt];

    std::vector <int> mystack;
    int blockid=FindToLeaf(record,iovcnt,mystack);
    std::cout<<"Insert Block id: "<<blockid<<std::endl;

    unsigned char newbuffer[Block::BLOCK_SIZE];
    unsigned char tmpbuffer[Block::BLOCK_SIZE];
    unsigned char recordbuffer[Block::BLOCK_SIZE];
    offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    db::Block block;block.attach(buffer_);

    if(change==-1) {//更新
        std::cout<<"right"<<std::endl;
        block.setNextid(-rightid);
    }else  {
        reoffset = block.getSlot(change);
        getRecord(iov_,reoffset,iovcnt,recordbuffer,&header);
        iov_[1].iov_base=&rightid;
        update(blockid,change,iov_,iovcnt);
        //block.attach(buffer_);
    }
    memcpy(tmpbuffer,buffer_,Block::BLOCK_SIZE);
    db::Block newblock;newblock.attach(newbuffer);

    int flag=0;//0插入左边，1插入右边
    ret=block.allocate(&header,record,(int)iovcnt);//满了要分裂？怎么分裂？创建两个新的block？因为要保证两个新的block都可以插入
    if(!ret) {//分裂，插入父节点 //判断是否是root
        int count=0;
        while(!ret) {
            int garbage=root.getGarbage();
            int left=block.getSlotsNum()/2;

            reoffset = block.getSlot(left);//获取中间左指针
            getRecord(tempiov_,reoffset,iovcnt,recordbuffer,&header);

            if(dtype->compare(record[getinfo.key].iov_base,tempiov_[getinfo.key].iov_base
                    ,record[getinfo.key].iov_len,tempiov_[getinfo.key].iov_len)) //判断是插入左block还是右block
                flag=0;
            else flag=1;

            int nextid=*(int*)tempiov_[1].iov_base;

            newblock.clear(1,root.getGarbage());//新分裂左block
            if(count==0)nextid=-nextid;//叶节点
            newblock.setNextid(nextid);//右指针
            for(int i=0;i<left;i++) {
                reoffset = block.getSlot(i);
                getRecord(iov_,reoffset,iovcnt,recordbuffer,&header);
                ret=newblock.allocate(&header,iov_,(int)iovcnt);
            }
            newoffset = (root.getGarbage()-1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            if(flag==0) {
                newblock.allocate(&header,record,(int)iovcnt);//是否比当前最小的小
                memcpy(buffer_,newbuffer,Block::BLOCK_SIZE);
                sortSlots(newblock,2);
                indexfile_.write(newoffset, (const char *) buffer_,Block::BLOCK_SIZE);
                memcpy(buffer_,tmpbuffer,Block::BLOCK_SIZE);
            }else {
                indexfile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);
            }
            root.setGarbage(root.getGarbage()+1);

            nextid=block.getNextid();
            newblock.clear(1,block.blockid());//右block不新开，覆盖原来的block
            if(count==0)nextid=-nextid;//叶节点
            newblock.setNextid(nextid);//右指针
            for(int i=left+1;i<block.getSlotsNum();i++) {
                reoffset = block.getSlot(i);
                getRecord(iov_,reoffset,iovcnt,recordbuffer,&header);
                ret=newblock.allocate(&header,iov_,(int)iovcnt);
            }
            newoffset = (block.blockid()-1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            if(flag==1){
                newblock.allocate(&header,record,(int)iovcnt);
                memcpy(buffer_,newbuffer,Block::BLOCK_SIZE);
                sortSlots(newblock,2);
                indexfile_.write(newoffset, (const char *) buffer_, Block::BLOCK_SIZE);
                memcpy(buffer_,tmpbuffer,Block::BLOCK_SIZE);
            }else {
                indexfile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);
            }

            tempiov_[1].iov_base=&garbage;
            tempiov_[1].iov_len=sizeof(int);
            header=0;

            if(block.blockid()==root.getHead()){//父节点,如果当前是根节点，新开一个block作为新的根节点
                newblock.clear(1,root.getGarbage());//新开父节点
                newblock.setNextid(block.blockid());//右指针
                ret=newblock.allocate(&header,tempiov_,(int)iovcnt);
                newoffset = (root.getGarbage()-1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
                indexfile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);
                root.setGarbage(root.getGarbage()+1);
                indexfile_.write(0, (const char *) rb, Root::ROOT_SIZE);
                break;
            }
            indexfile_.write(0, (const char *) rb, Root::ROOT_SIZE);

            blockid=mystack[mystack.size()-count-1];//父节点
            offset=(blockid-1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
            block.attach(buffer_);
            ret=block.allocate(&header,tempiov_,(int)iovcnt);
            if(ret){
                sortSlots(block,2);
                block.setChecksum();
                indexfile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
            }
            else {
                count++;//中间的往上插的记录给record
                /*for(int i=0;i<iovcnt; ++i) {
                    memcpy(record[i].iov_base,tempiov_.iov_base,)
                }*/
                header=0;record=tempiov_;
            }
        }
    }
    else  {
        sortSlots(block,2);
        indexfile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
        std::cout<<"next indexid "<<block.getNextid()<<std::endl;
    }
    delete [] tempiov_;
    delete [] iov_;
    return S_OK;
}

int BplusTree::update(struct iovec *record, size_t iovcnt,struct iovec *newrecord) {//更新索引主键

    return S_OK;
}
int BplusTree::update(int blockid,int slotid,struct iovec *record, size_t iovcnt) {//更新索引主键
    
    size_t offset = Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE;
    //indexfile_.read(offset, (char *)buffer_,Block::BLOCK_SIZE);
    Block block;block.attach(buffer_);

    if(slotid>block.getSlotsNum()-1) {
        std::cout<<"Num Error"<<std::endl;
        return -1;
    }
    size_t length = 0;Integer it;
    
    unsigned char recordbuffer[Block::BLOCK_SIZE]={0};
    size_t reoffset=block.getSlot(slotid);
    memcpy(recordbuffer,buffer_+reoffset,2);
    bool ret= it.decode((char *)recordbuffer, 2);
    length = it.get();
    if (!ret)  {
        std::cout<<" error "<<length<<std::endl;
        return -1;
    }
    Record updaterecord;
    const unsigned char header=0;
    unsigned char uprecordbuffer[Block::BLOCK_SIZE]={0};
    updaterecord.attach(uprecordbuffer, unsigned short(length));
    updaterecord.set(record,(int)iovcnt, &header);
    int newlength=(int)updaterecord.length();

    //判断是不是能插入，能插入就更新
    if(newlength<=length) {
        /*indexfile_.write(offset+reoffset, (char *)uprecordbuffer,length);
        indexfile_.read(offset, (char *)buffer_,Block::BLOCK_SIZE);*/
        memcpy(buffer_+reoffset, (char *)uprecordbuffer,length);
    }
    //不能删除原来的，调用insert重新插入
    /*else  {
        unsigned char header=0;
        remove(block,slotid);
        insert(record,iovcnt,0,0);
    }*/
    return S_OK;

}
int BplusTree::remove(struct iovec *record, size_t iovcnt) {


    //删除时要合并
    return S_OK;
}
int BplusTree::remove(Block &block,int slotid) {
    
    if(slotid>block.getSlotsNum()-1)return -1;
    unsigned short offset = block.getSlot(slotid);
    unsigned short temp= block.getGarbage();
    memcpy(buffer_ + offset + 2, &temp, sizeof(temp));
    block.setGarbage(slotid);

    for(int i=slotid-1;i<block.getSlotsNum()-1;i++) {
        block.setSlot(i,block.getSlot(i+1));
    }
    block.setSlot(block.getSlotsNum()-1,0);
    block.setSlotsNum(block.getSlotsNum()-1);
    indexfile_.write(Root::ROOT_SIZE+(block.blockid()-1)*Block::BLOCK_SIZE, (char *) buffer_, Block::BLOCK_SIZE);
    //删除时要合并
    return S_OK;
}
}

/*

            int garoffset = (garbage - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            indexfile_.read(garoffset, (char *) prebuffer, Block::BLOCK_SIZE);
            db::Block garblock;garblock.attach(prebuffer);

            newblock.clear(1,garbage);//新分裂左block

            if(garblock.getNextid()==0)root.setGarbage(garbage+1);
            else root.setGarbage(garblock.getNextid());

            root.setGarbage(block.blockid());
            block.setNextid(garbage);
*/