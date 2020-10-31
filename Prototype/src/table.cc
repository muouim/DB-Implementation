////
// @file table.cc
// @brief
// 实现存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <db/table.h>
#include <string>

std::string tablename("test.dat");
std::string indexname_("index.dat");

namespace db {
int Table::create(char *name,RelationInfo &info,char *indexname,RelationInfo &indexinfo) {
    //创建schema,创建数据表的同时创建索引表
    info.file=datafile_;
    gschema.create(indexname,indexinfo);
    return gschema.create(name,info);
}
int Table::open(const char *name) {
    // 查找schema
    gschema.open();
    std::pair<Schema::TableSpace::iterator, bool> ret = gschema.lookup(name);
    gschema.load(ret.first);
    if (!ret.second) return EINVAL;
    int ret_ = datafile_.open(name);
    unsigned long long length;
    ret_= datafile_.length(length);   
   
    // 如果meta.db长度为0，则写一个block
    if (ret_) return ret_;
    if (length) {
        // 加载ROOT
        datafile_.read(0, (char *) buffer_, Root::ROOT_SIZE);
        // 获取第1个block
        Root root;
        root.attach(buffer_);
        unsigned int first = root.getHead();
        size_t offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
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
        // 写root和block
        datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);
        datafile_.write(Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    datafile_.read(0, (char *) rb, Root::ROOT_SIZE);
    
    index.open(indexname_.c_str());
    index.indexfile_.open(indexname_.c_str());
    index.indexfile_.read(0, (char *)indexrb, Root::ROOT_SIZE);
    return S_OK;
    //schema.load()// 找到后，加载meta信息
}
//template<class T>
bool compare(std::pair<unsigned short,struct iovec> x,  std::pair<unsigned short,struct iovec> y) {

    //载入数据类型
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//findDataType("INT")//比较
    return dtype->compare(x.second.iov_base,y.second.iov_base,x.second.iov_len,y.second.iov_len);
}
int Table::sortSlots(Block &block,int iovcnt,unsigned char *sortbuffer) {

    //载入relationinfo    
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;
    
    size_t reoffset=0; 
    std::vector<std::pair<unsigned short,struct iovec>>keys;
    unsigned char header=0;
    char  **base=new char*[block.getSlotsNum()];
    struct iovec *iov_=new struct iovec[iovcnt];
    unsigned char recordbuffer[Block::BLOCK_SIZE];

    //取用所有主键字段
    for(int i = 0; i <block.getSlotsNum(); i++) {
        reoffset = block.getSlot(i);
        getRecord(iov_,reoffset,iovcnt,sortbuffer,recordbuffer);
        
        base[i]=new char[iov_[getinfo.key].iov_len];
        memcpy(base[i],iov_[getinfo.key].iov_base,iov_[getinfo.key].iov_len);
        std::pair<unsigned short,struct iovec>p(block.getSlot(i),iov_[getinfo.key]);
        keys.push_back(p);
    }
    //std::cout <<"____________"<<std::endl;
    for(int i = 0; i <block.getSlotsNum(); ++i) 
        keys[i].second.iov_base=base[i];

    std::sort(keys.begin(), keys.end(),compare);
    int tempslotid=0;
    for(int i=0; i<block.getSlotsNum(); i++) {
        block.setSlot(i,keys[i].first);
        //std::cout <<"key:"<<*(int *)keys[i].second.iov_base<<" "<<keys[i].first<<std::endl;
        tempslotid++;
    }
    for(int i = 0; i <block.getSlotsNum(); i++) delete[] base[i];
    delete []iov_;
    delete[] base;

    return S_OK;
}


int Table::getRecord(struct iovec *iov, size_t offset, size_t iovcnt,unsigned char *tmpbuffer,unsigned char *recordbuffer) {

    memcpy(recordbuffer,tmpbuffer+offset,2);
    size_t length = 0;Integer it;Record record_;
    bool tempret = it.decode((char *) recordbuffer, 2);
        //if (!tempret) return false;
    length = it.get();
    unsigned char header=0;
    memcpy(recordbuffer,tmpbuffer+offset,length);

    record_.attach(recordbuffer, unsigned short(length));
    record_.ref(iov,(int)iovcnt, &header);
    return S_OK;
}

int Table::insert(struct iovec *record, size_t iovcnt) {//调整索引block主键
    //TODO::实验二，不用遍历，使用b树找到对应的block，插入后，如果范围发生改变就改变实验二b树的block
    size_t offset=0;
    size_t newoffset=0;size_t reoffset=0;    
    
    int ret=0;unsigned char header=0;
    //读取root
    db::Root root;root.attach(rb);
    
    unsigned char newbuffer[Block::BLOCK_SIZE];

    //TODO:怎么载入想要插入record的BLOCK？
    db::Block block;
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;getinfo=ret_.first->second;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//findDataType("INT");
    
    struct iovec *iov_=new struct iovec[iovcnt];
    unsigned char recordbuffer[Block::BLOCK_SIZE];

    //插入索引
    struct iovec *indexiov_=new struct iovec[2];
    int a=*(int *)record[getinfo.key].iov_base;
    indexiov_[0].iov_base =&a;        
    indexiov_[0].iov_len = record[getinfo.key].iov_len;
    
    Root indexroot;
    indexroot.attach(indexrb);
    
    //找到block所在的位置
    std::pair<int, int>tmpair=index.search(indexiov_,2);
    int blockid=tmpair.first;
    int change=tmpair.second;//是否改变索引右指针

    //std::cout<<"Insert "<<a<<" "<<blockid<<std::endl;

    offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);

    block.attach(buffer_);
    db::Block newblock;newblock.attach(newbuffer);

    ret=block.allocate(&header,record,(int)iovcnt);//怎么更新索引叶节点对应的主键？？？？不用更新，只有一个数据block时，没有索引记录，
    offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;

    struct iovec *tmpiov_=new struct iovec[iovcnt];
    int flag=0;//0插入左边，1插入右边

    if(!ret) {//无法插入：直接分裂 从中间劈开，保留左边的，右边新建一个block
        int left=block.getSlotsNum()/2;
        reoffset = block.getSlot(left);//获取中间左指针
        getRecord(tmpiov_,reoffset,iovcnt,buffer_,recordbuffer);

        if(dtype->compare(record[getinfo.key].iov_base,tmpiov_[getinfo.key].iov_base
        ,record[getinfo.key].iov_len,tmpiov_[getinfo.key].iov_len)) //判断是插入左block还是右block
            flag=0;
        else flag=1;

        int nextid=block.getNextid();
        int currentid=block.blockid();
        int garbage=root.getGarbage();
        std::cout<<"New Block id: "<<garbage<<std::endl;

        newblock.clear(1,root.getGarbage());//新分裂右block
        newblock.setNextid(nextid);//
        for(int i=left+1;i<block.getSlotsNum();i++) {
            reoffset = block.getSlot(i);
            getRecord(iov_,reoffset,iovcnt,buffer_,recordbuffer);
            ret=newblock.allocate(&header,iov_,(int)iovcnt);
        }
        newoffset = (garbage-1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        if(flag==1) {
            newblock.allocate(&header,record,(int)iovcnt);
            sortSlots(newblock,(int)iovcnt,newbuffer);
        }
        datafile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);
        root.setGarbage(root.getGarbage()+1);

        newblock.clear(1,block.blockid());//左block不新开，覆盖原来的block
        newblock.setNextid(garbage);//
        for(int i=0;i<left+1;i++) {
            reoffset = block.getSlot(i);
            getRecord(iov_,reoffset,iovcnt,buffer_,recordbuffer);
            ret=newblock.allocate(&header,iov_,(int)iovcnt);
        }
        newoffset = (currentid-1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        if(flag==0) {
            newblock.allocate(&header,record,(int)iovcnt);
            sortSlots(newblock,(int)iovcnt,newbuffer);
        }
        datafile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);
        datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);

        a=*(int *)tmpiov_[getinfo.key].iov_base;
        indexiov_[0].iov_base =&a;        
        indexiov_[0].iov_len = tmpiov_[getinfo.key].iov_len;
        int tmpid=-currentid;
        indexiov_[1].iov_base=&tmpid;//左指针指向当前block
        indexiov_[1].iov_len=sizeof(int);

        index.insert(indexiov_,2,change,-garbage);//如果是右指针改变nextid change=-1 //change>=0,需要改变原来的指针指向garbage
    }
    else {//能插入直接写入
        sortSlots(block,(int)iovcnt,buffer_);
        block.setChecksum();
        datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    delete [] iov_;
    delete [] indexiov_;
    delete [] tmpiov_;
    //排序
    
    //读record
    //std::cout <<"Head "<<block.getSlot(0)<<" "<<std::endl;
    return S_OK;
}

int Table::update(int blockid,int slotid,struct iovec *record, size_t iovcnt) {
    //TODO:定位到对应record
    size_t offset = Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE;
    datafile_.read(offset, (char *)buffer_,Block::BLOCK_SIZE);
    Block block;block.attach(buffer_);

    if(slotid>block.getSlotsNum()-1) {
        std::cout<<"num error"<<std::endl;
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
    updaterecord.attach(uprecordbuffer, Block::BLOCK_SIZE);
    updaterecord.set(record,(int)iovcnt, &header);
    int newlength = (int) updaterecord.length();
    
    //判断是不是能插入，能插入就更新
    if(newlength<=length) {
        block.setChecksum();
        datafile_.write(offset+reoffset, (char *)uprecordbuffer,newlength);
        std::cout <<"can insert "<<newlength<<" "<<length<<std::endl;
    }
    //不能删除原来的，调用insert重新插入
    else  {
        std::cout <<"can not insert "<<newlength<<" "<<length<<std::endl;
        remove(blockid,slotid);
        insert(record,iovcnt);
    }
    return S_OK;
}
int Table::getBrother(int fatherid,int cunrrentid) {
    //向兄弟节点借一个节点然后更新兄弟节点的索引节点
    //如果非最右节点，找next，如果是最右节点，找前一个
    return S_OK;
}
int Table::remove(int blockid,int slotid) {//调整索引block主键
        
    //写到garbage，替换record记录长度前2B，形成链表，新删除的写入头部，替换原来头部record记录长度前2B
    datafile_.read(Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE, (char *)buffer_,Block::BLOCK_SIZE);

    Block block;
    block.attach(buffer_);
    if(slotid>block.getSlotsNum()-1)return -1;
    //如果删除是Block内的第一个记录需要更新对于的索引记录
    
    //if(slotid==block.getSlotsNum()-1)update()
    unsigned short offset = block.getSlot(slotid);
    unsigned short temp= block.getGarbage();
    memcpy(buffer_ + offset + 2, &temp, sizeof(temp));
    block.setGarbage(slotid);
    
    //TODO:slots 重新排序，向前挤压
    for(int i=slotid;i<block.getSlotsNum()-1;i++) {
        block.setSlot(i,block.getSlot(i+1));
    }
    block.setSlot(block.getSlotsNum()-1,0);
    block.setSlotsNum(block.getSlotsNum()-1);
    datafile_.write(Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE, (char *) buffer_, Block::BLOCK_SIZE);

    //删除完需要判断是否<50%,如果小于，那么需要向兄弟节点借一个节点然后更新兄弟节点的索引节点，并更新兄弟节点对应的索引
    //如果两个都 <50%，就合并两个，删除当前Block对应的索引记录
    if(block.getSlotsNum()==0) {

    }   
    return S_OK;
}

std::pair<int,int> Table::findkey(struct iovec *key,int iovcnt){

    size_t offset=0;  size_t reoffset=0;  
    int ret=0;unsigned char header=1;
    int slotid_=0;

    //读取root
    Root root;
    root.attach(rb);
    
    size_t length = 0;
    Integer it;Record record_;
    struct iovec *iov_=new struct iovec[iovcnt];
    unsigned char recordbuffer[Block::BLOCK_SIZE]={0};

    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//getinfo.fields[getinfo.key].type;

    //TODO:使用b树找到对应block
    struct iovec *indexiov_=new struct iovec[2];
    int a=*(int *)key[getinfo.key].iov_base;
    indexiov_[0].iov_base =&a;        
    indexiov_[0].iov_len = key[getinfo.key].iov_len;
    
    Root indexroot;
    indexroot.attach(indexrb);
    
    //找到block所在的位置
    std::pair<int, int>tmpair=index.search(indexiov_,2);
    offset = (tmpair.first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    Block block;block.attach(buffer_);

    //遍历record
    int flag=0;
    int lower=0,big=block.getSlotsNum()-1;
    
    while(big>=lower) {
        reoffset=block.getSlot((lower+big)/2);
        getRecord(iov_,reoffset,iovcnt,buffer_,recordbuffer);

        std::cout <<*(int *)key[getinfo.key].iov_base<<" "<<*(int *)iov_[getinfo.key].iov_base<<std::endl;
        if(dtype->equal(key[getinfo.key].iov_base,iov_[getinfo.key].iov_base,
        key[getinfo.key].iov_len,iov_[getinfo.key].iov_len)) {
            slotid_=(lower+big)/2;
            flag=1;break;
        }
        if(dtype->compare(key[getinfo.key].iov_base,iov_[getinfo.key].iov_base,
        key[getinfo.key].iov_len,iov_[getinfo.key].iov_len)){
            big=(lower+big)/2-1;
            continue;
        }
        else lower=(lower+big)/2+1;
    }

    if(flag==0) {
        std::pair<int,int> no(0,0);
        return no;
    }
    std::pair<int,int> p(block.blockid(),slotid_);
    delete []iov_;
    delete []indexiov_;

    return p;
}
    
} // namespace db

    /*block.clear(spaceid_,blockid);
    size_t offset=Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;    

    datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
        int size_= atoi(size);
    datafile_.read(offset, size,2);
    */    


       /*int a=2018;
    keys[0].second.iov_base=(int *)&a;
    std::cout <<*(int *)keys[0].second.iov_base<<" "<<std::endl;
    int b=2019;
    if(block.getSlotsNum()>1)    keys[1].second.iov_base=(int *)&b;
    int c=2020;
    if(block.getSlotsNum()>2)    keys[2].second.iov_base=(int *)&c;*/

        /*int temp;
        memcpy(&temp,keys[i].second.iov_base,keys[i].second.iov_len);
        std::cout <<temp<<" "<<keys[i].second.iov_len<<" "<<keys[i].first<<std::endl;*/


        /*int offset_=Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE+block.getSlot(i);//
        unsigned char recordbuffer[Block::BLOCK_SIZE]={0};
        datafile_.read(offset_,(char *)recordbuffer,2);
        size_t length = 0;
        Integer it;Record record_;
        struct iovec *iov_=new struct iovec[iovcnt];
        int ret = it.decode((char *)recordbuffer, 2);
        if (!ret) return false;
        length = it.get();
        unsigned char tempheader=1;
        datafile_.read(offset_,(char *)recordbuffer,length);

        record_.attach(recordbuffer, unsigned short(length));
        record_.ref(iov_,(int)iovcnt, &header);
        deiov.push_back(iov_);
        
        base[i]=new char[iov_[getinfo.key].iov_len];
        memcpy(base[i],iov_[getinfo.key].iov_base,iov_[getinfo.key].iov_len);
        //std::cout <<"base:"<<base[i]<<std::endl;

        std::pair<unsigned short,struct iovec>p(block.getSlot(i),iov_[getinfo.key]);
        //std::cout <<*(int *)p.second.iov_base<<" "<<p.second.iov_len<<std::endl;
        keys.push_back(p);//这里没有问题*/

            /*for(int i = 0; i <block.getSlotsNum(); ++i) {
        offset_=Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE+block.getSlot(i);
        datafile_.read(offset_,(char *)recordbuffer,2);

        ret = it.decode((char *)recordbuffer, 2);
        length = it.get();
        datafile_.read(offset_,(char *)recordbuffer,length);
        record_.attach(recordbuffer, unsigned short(length));
        record_.ref(iov_,(int)iovcnt, &header);
        //std::cout <<*(int *)key[getinfo.key].iov_base<<" "<<*(int *)iov_[getinfo.key].iov_base<<std::endl;
        if(edtype->compare(key[getinfo.key].iov_base,iov_[getinfo.key].iov_base,key[getinfo.key].iov_len,iov_[getinfo.key].iov_len)) {
            slotid_=i;break;
        }
    }*/