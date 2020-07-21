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

namespace db {
int Table::create(char *name,RelationInfo &info) {
    //创建schema
    info.file=datafile_;
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
        unsigned char rb[Root::ROOT_SIZE];
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
    return S_OK;
    //schema.load()// 找到后，加载meta信息
}
//template<class T>
bool compare(std::pair<unsigned short,struct iovec> x,  std::pair<unsigned short,struct iovec> y) {

    //载入数据类型
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//findDataType("INT");
    //比较
    return dtype->compare(x.second.iov_base,y.second.iov_base,x.second.iov_len,y.second.iov_len);
}
int Table::sortSlots(Block &block,int iovcnt) {

    //载入relationinfo    
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;

    std::vector<std::pair<unsigned short,struct iovec>>keys;
    unsigned char header=0;
    std::cout <<"____________"<<std::endl;
    char  **base=new char*[block.getSlotsNum()];
    //取用所有主键字段
    std::vector<iovec*>deiov;
    for(int i = 0; i <block.getSlotsNum(); ++i) {
        /*int offset_=Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE+block.getSlot(i);//
        unsigned char tempbuffer[Block::BLOCK_SIZE]={0};
        datafile_.read(offset_,(char *)tempbuffer,2);
        size_t length = 0;
        Integer it;Record record_;
        struct iovec *iov_=new struct iovec[iovcnt];
        int ret = it.decode((char *)tempbuffer, 2);
        if (!ret) return false;
        length = it.get();
        unsigned char tempheader=1;
        datafile_.read(offset_,(char *)tempbuffer,length);

        record_.attach(tempbuffer, unsigned short(length));
        record_.ref(iov_,(int)iovcnt, &header);
        deiov.push_back(iov_);
        
        base[i]=new char[iov_[getinfo.key].iov_len];
        memcpy(base[i],iov_[getinfo.key].iov_base,iov_[getinfo.key].iov_len);
        //std::cout <<"base:"<<base[i]<<std::endl;

        std::pair<unsigned short,struct iovec>p(block.getSlot(i),iov_[getinfo.key]);
        //std::cout <<*(int *)p.second.iov_base<<" "<<p.second.iov_len<<std::endl;
        keys.push_back(p);//这里没有问题*/
        
        unsigned char tempbuffer[Block::BLOCK_SIZE]={0};
        
        memcpy(tempbuffer,buffer_+block.getSlot(i),2);
        size_t length = 0;
        Integer it;Record record_;
        struct iovec *iov_=new struct iovec[iovcnt];
        int ret = it.decode((char *)tempbuffer, 2);
        if (!ret) {
            std::cout<<"error"<<std::endl;   
            return false;
        }
        length = it.get();
        unsigned char tempheader=1;
        memcpy(tempbuffer,buffer_+block.getSlot(i),length);

        record_.attach(tempbuffer, unsigned short(length));
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

    std::sort(keys.begin(), keys.end(),compare);
    int tempslotid=0;
    for(int i=0; i<keys.size(); i++) {
        block.setSlot(tempslotid,keys[i].first);
        std::cout <<"key:"<<*(int *)keys[i].second.iov_base<<" "<<keys[i].first<<std::endl;
        tempslotid++;
    }
    for(int i = 0; i <deiov.size(); i++) delete[] deiov[i];
    for(int i = 0; i <block.getSlotsNum(); i++) delete[] base[i];
    delete[] base;

    /*block.setChecksum();
    datafile_.write(Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);*/
    return S_OK;
}

int Table::insert(struct iovec *record, size_t iovcnt)
{   
    size_t offset=0;    
    int ret=0;unsigned char header=0;
    //读取root
    Root root;unsigned char rb[Root::ROOT_SIZE];
    datafile_.read(0, (char *) rb, Root::ROOT_SIZE);
    root.attach(rb);
    
    unsigned char newbuffer[Block::BLOCK_SIZE];
    unsigned char prebuffer[Block::BLOCK_SIZE];

    unsigned int first = root.getHead();
    offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    datafile_.read(offset, (char *) newbuffer, Block::BLOCK_SIZE);
    //TODO:怎么载入想要插入record的BLOCK？
    db::Block block;block.attach(buffer_);
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;getinfo=ret_.first->second;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//findDataType("INT");
    
    db::Block new_block;new_block.attach(newbuffer);
    db::Block previous_block;
    int next=block.getNextid();int current=block.blockid();

    struct iovec *iov_=new struct iovec[iovcnt];

    //比较
    int previous_id = 0;

    //找空隙足够的block插入record
    while (block.blockid()==root.getHead()||block.getNextid()!=0) {
        
        if(block.getSlotsNum()<=0) {
            ret=block.allocate(&header,record,(int)iovcnt);
            offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
            break;
        }

        size_t reoffset = Root::ROOT_SIZE+(block.blockid()-1)*Block::BLOCK_SIZE+block.getSlot(block.getSlotsNum()-1);//溢出问题
        unsigned char recordbuffer[Block::BLOCK_SIZE];
        //读长度，2B
 
        getRecord(iov_,reoffset,iovcnt,recordbuffer);
        std::cout<<"FIND "<<block.blockid()<<" "<<previous_id<<""<<*(int *)iov_[getinfo.key].iov_base<<" "<<reoffset<<std::endl;

        //if(record[getinfo.key]>iov_[getinfo.key]) compare是小于
        if(!dtype->compare(iov_[getinfo.key].iov_base, record[getinfo.key].iov_base,
        iov_[getinfo.key].iov_len,record[getinfo.key].iov_len)) {//不比当前block最大的大,<=max
            
            std::cout<<"hdhjks"<<std::endl;
            ret=block.allocate(&header,record,(int)iovcnt);//能插入当前block直接插入，然后截止
            if(ret) {
                offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;break;
            }

            reoffset = Root::ROOT_SIZE+(block.blockid()-1)*Block::BLOCK_SIZE+block.getSlot(0);
            getRecord(iov_,reoffset,iovcnt,recordbuffer);
            
            if(previous_id==0)break;
            size_t previous_offset=Root::ROOT_SIZE+(previous_id-1)* Block::BLOCK_SIZE;
            datafile_.read(previous_offset, (char *) prebuffer, Block::BLOCK_SIZE);  
            previous_block.attach(prebuffer);

            next=block.getNextid();current=block.blockid();

            if(!dtype->compare(iov_[getinfo.key].iov_base, record[getinfo.key].iov_base,
            iov_[getinfo.key].iov_len,record[getinfo.key].iov_len)) {//比当前block最小的小，就找上一个判断 <=min
                
                ret=previous_block.allocate(&header,record,(int)iovcnt);//上一个是否能插入
                if(ret) {
                    offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;break;
                }
            }
            else  {//比当前最小的大就分裂, >min 或者比最小的小但是两个block都插不进去
                
                previous_block.setNextid(root.getGarbage());
                offset= Root::ROOT_SIZE+(previous_block.blockid()-1)* Block::BLOCK_SIZE;
                datafile_.write(offset, (const char *) prebuffer, Block::BLOCK_SIZE);  

                new_block.clear(1,root.getGarbage());//初始化，写入下一个
                new_block.setNextid(next);

                root.setGarbage(root.getGarbage()+1);
                datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);
                
                ret=new_block.allocate(&header,record,(int)iovcnt);     

                for(int i = 0; i <block.getSlotsNum();i++) {
                //把比记录小的全删了，再新分配block插入,找大于要插入的记录的
                    reoffset = Root::ROOT_SIZE+(current-1)*Block::BLOCK_SIZE+block.getSlot(i);
                    getRecord(iov_,reoffset,iovcnt,recordbuffer);

                    if(dtype->compare(iov_[getinfo.key].iov_base, record[getinfo.key].iov_base,
                    iov_[getinfo.key].iov_len,record[getinfo.key].iov_len)){
                        std::cout<<"find " <<i<<" "<<*(int *)iov_[getinfo.key].iov_base<<std::endl;
                        ret=new_block.allocate(&header,iov_,(int)iovcnt);
                        remove(block.blockid(),i);
                    }else break;
                }
                offset = (root.getGarbage()-2) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
                memcpy(buffer_,newbuffer,Block::BLOCK_SIZE);
            }
        }
        else  {//比当前最大的block大,就往下找
            if(ret||block.getNextid()==0){break;}
            previous_id=block.blockid();
            datafile_.read(Root::ROOT_SIZE+(block.getNextid()-1)* Block::BLOCK_SIZE,(char *)buffer_,Block::BLOCK_SIZE);
            block.attach(buffer_);
            std::cout<<"next "<<block.getNextid()<<" "<<block.blockid()<<"  "<<root.getHead()<<" "<<block.getSlotsNum()<<std::endl;
        }
    }
    if(!ret){
        ret=block.allocate(&header,record,(int)iovcnt);//最后一个block
        std::cout<<"sdsdsfsfsf"<<std::endl;
    }
    
    //分配一个新的block
    if(!ret) {

        block.attach(buffer_);
        new_block.attach(newbuffer);

        size_t reoffset = 0;
        unsigned char recordbuffer[Block::BLOCK_SIZE];
        std::cout<<"ss "<<root.getGarbage()<<std::endl;

        block.setNextid(root.getGarbage());
        offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
        datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);  

        new_block.clear(1,root.getGarbage());//初始化，写入下一个
        new_block.setNextid(0);
        root.setGarbage(root.getGarbage()+1);
        datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);
        
        new_block.allocate(&header,record,(int)iovcnt);     
        
        std::cout<<"ss "<<block.getSlotsNum()<<std::endl;
        reoffset = Root::ROOT_SIZE+(block.blockid()-1)*Block::BLOCK_SIZE+block.getSlot(block.getSlotsNum()-1);
        getRecord(iov_,reoffset,iovcnt,recordbuffer);

        if(!dtype->compare(iov_[getinfo.key].iov_base, record[getinfo.key].iov_base,
        iov_[getinfo.key].iov_len,record[getinfo.key].iov_len))
        for(int i = 0; i <block.getSlotsNum();i++) {
        //把比记录小的全删了，再新分配block插入,找大于要插入的记录的
            reoffset = Root::ROOT_SIZE+(block.blockid()-1)*Block::BLOCK_SIZE+block.getSlot(i);
            std::cout<<block.getSlot(i)<<std::endl;
            getRecord(iov_,reoffset,iovcnt,recordbuffer);
                        std::cout<<"find2 " <<i<<std::endl;

            if(dtype->compare(iov_[getinfo.key].iov_base, record[getinfo.key].iov_base,
            iov_[getinfo.key].iov_len,record[getinfo.key].iov_len)) {
                ret=new_block.allocate(&header,iov_,(int)iovcnt);
                remove(block.blockid(),i);
            }else break;
        }

        offset = (root.getGarbage()-2) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        memcpy(buffer_,newbuffer,Block::BLOCK_SIZE);

    }
    else offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
    delete [] iov_;

    block.attach(buffer_);
    //排序
    sortSlots(block,(int)iovcnt);
        std::cout<<"new "<<block.blockid()<<std::endl;

    block.setChecksum();
    datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
    
    //读record
    std::cout <<"Head "<<block.getSlot(0)<<" "<<std::endl;
    return 0;
}
int Table::getRecord(struct iovec *iov, size_t offset, size_t iovcnt,unsigned char *recordbuffer) {

    datafile_.read(offset,(char *)recordbuffer,2);
    size_t length = 0;Integer it;Record record_;
    bool tempret = it.decode((char *) recordbuffer, 2);
        //if (!tempret) return false;
    length = it.get();
        unsigned char header=0;
    datafile_.read(offset,(char *)recordbuffer,length);

    record_.attach(recordbuffer, unsigned short(length));
    record_.ref(iov,(int)iovcnt, &header);
    return 1;
}

int Table::update(int blockid,int slotid,struct iovec *record, size_t iovcnt) {
    //TODO:定位到对应record
    datafile_.read(Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE, (char *)buffer_,Block::BLOCK_SIZE);

    Block block;
    block.attach(buffer_);
    if(slotid>block.getSlotsNum())return -1;
    size_t offset = Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE+block.getSlot(slotid);
    
    //读长度，2B
    datafile_.read(offset,(char *)buffer_,2);
    size_t length = 0;
    Integer it;
    bool ret = it.decode((char *) buffer_, 2);
    if (!ret) return false;
    length = it.get();
    Record updaterecord;
    const unsigned char header=0;
    unsigned char tempbuffer[Block::BLOCK_SIZE];
    updaterecord.attach(tempbuffer, unsigned short(length));
    updaterecord.set(record,(int)iovcnt, &header);
    int newlength=(int)updaterecord.length();

    //判断是不是能插入，能插入就更新
    if(newlength<=length) {
        block.setChecksum();
        datafile_.write(offset, (char *)tempbuffer,length);
        datafile_.read(Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE, (char *)buffer_,Block::BLOCK_SIZE);
        block.attach(buffer_);
        sortSlots(block,(int)iovcnt);
        std::cout <<block.getSlot(0)<<" "<<std::endl;
    }
    //不能删除原来的，调用insert重新插入
    else  {
        unsigned char header=0;
        remove(blockid,slotid);
        insert(record,iovcnt);
    }
    return S_OK;
}
int Table::remove(int blockid,int slotid) {
        
    //TODO:写到garbage，替换record记录长度前2B，形成链表，新删除的写入头部，替换原来头部record记录长度前2B
    datafile_.read(Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE, (char *)buffer_,Block::BLOCK_SIZE);

    Block block;
    block.attach(buffer_);
    if(slotid>block.getSlotsNum())return -1;
    unsigned short offset = block.getSlot(slotid);
    unsigned short temp= block.getGarbage();
    memcpy(buffer_ + offset + 2, &temp, sizeof(temp));
    block.setGarbage(slotid);
 
    //TODO:slots 重新排序，向前挤压
    for(int i=slotid-1;i<block.getSlotsNum()-1;i++) {
        block.setSlot(i,block.getSlot(i+1));
    }
    block.setSlot(block.getSlotsNum()-1,0);
    block.setSlotsNum(block.getSlotsNum()-1);
    datafile_.write(Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE, (char *) buffer_, Block::BLOCK_SIZE);   
    return S_OK;
}

std::pair<int,int> Table::findkey(struct iovec *key,int iovcnt){

    int blockid_=1;int blockoffset_;
    int slotid_=0;int offset_;
    unsigned char tempbuffer[Block::BLOCK_SIZE]={0};
    size_t length = 0;
    Integer it;
    Record record_;
    struct iovec *iov_=new struct iovec[iovcnt];
    unsigned char tempheader=1;
    int ret;
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;
    getinfo=ret_.first->second;
    unsigned char header=0;
    Block block;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//getinfo.fields[getinfo.key].type;

    //遍历block，取每个block第一个和最后一个key,blocknum怎么整
    while(blockid_!=0) {
        blockoffset_=Root::ROOT_SIZE+(blockid_-1)*Block::BLOCK_SIZE;
        datafile_.read(blockoffset_,(char *)buffer_,Block::BLOCK_SIZE);
        block.attach(buffer_);
        
        offset_=Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE+block.getSlot(0);
        std::cout <<block.getSlot(0)<<" "<<std::endl;

        datafile_.read(offset_,(char *)tempbuffer,2);
        ret= it.decode((char *)tempbuffer, 2);
        length = it.get();
        datafile_.read(offset_,(char *)tempbuffer,length);

        record_.attach(tempbuffer, unsigned short(length));
        record_.ref(iov_,(int)iovcnt, &header);

        if(dtype->compare(key->iov_base,iov_[getinfo.key].iov_base,key->iov_len,iov_[getinfo.key].iov_len)){
            printf("1");
            blockid_=block.getNextid();
            continue;
        }
        offset_=Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE+block.getSlot(block.getSlotsNum()-1);
        datafile_.read(offset_,(char *)tempbuffer,2);
        ret= it.decode((char *)tempbuffer, 2);
        length = it.get();
        datafile_.read(offset_,(char *)tempbuffer,length);
        record_.attach(tempbuffer, unsigned short(length));
        record_.ref(iov_,(int)iovcnt, &header);
        
        /*int a=2020;
        key->iov_base=(int *)&a;*/

        //std::cout <<*(int *)key->iov_base<<" "<<*(int *)iov_[getinfo.key].iov_base<<std::endl;
        if(!dtype->compare(key->iov_base,iov_[getinfo.key].iov_base,key->iov_len,iov_[getinfo.key].iov_len)&&
        !dtype->equal(key->iov_base,iov_[getinfo.key].iov_base,key->iov_len,iov_[getinfo.key].iov_len)) {
            printf("s");
            blockid_=block.getNextid();
            continue;
        }
        break;
    }
    std::pair<int,int> no(0,0);
    if(blockid_==0)return no;
    //遍历record
    int lower=0,big=block.getSlotsNum()-1;
    while(big>=lower) {
        offset_=Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE+block.getSlot((lower+big)/2);
        
        datafile_.read(offset_,(char *)tempbuffer,2);

        ret = it.decode((char *)tempbuffer, 2);
        length = it.get();
        datafile_.read(offset_,(char *)tempbuffer,length);
        record_.attach(tempbuffer, unsigned short(length));
        record_.ref(iov_,(int)iovcnt, &header);
        std::cout <<*(int *)key->iov_base<<" "<<*(int *)iov_[getinfo.key].iov_base<<std::endl;
        if(dtype->equal(key->iov_base,iov_[getinfo.key].iov_base,key->iov_len,iov_[getinfo.key].iov_len)) {
            slotid_=(lower+big)/2;
            break;
        }
        if(dtype->compare(key->iov_base,iov_[getinfo.key].iov_base,key->iov_len,iov_[getinfo.key].iov_len)){
            big=(lower+big)/2-1;
            continue;
        }
        else lower=(lower+big)/2+1;
    }
    /*for(int i = 0; i <block.getSlotsNum(); ++i) {
        offset_=Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE+block.getSlot(i);
        datafile_.read(offset_,(char *)tempbuffer,2);

        ret = it.decode((char *)tempbuffer, 2);
        length = it.get();
        datafile_.read(offset_,(char *)tempbuffer,length);
        record_.attach(tempbuffer, unsigned short(length));
        record_.ref(iov_,(int)iovcnt, &header);
        //std::cout <<*(int *)key->iov_base<<" "<<*(int *)iov_[getinfo.key].iov_base<<std::endl;
        if(edtype->compare(key->iov_base,iov_[getinfo.key].iov_base,key->iov_len,iov_[getinfo.key].iov_len)) {
            slotid_=i;break;
        }
    }*/
    std::pair<int,int> p(blockid_,slotid_);
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