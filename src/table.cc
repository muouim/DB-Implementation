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
        if (!ret) return false;
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

    //TODO:怎么载入想要插入record的BLOCK？
    db::Block block;block.attach(buffer_);
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;getinfo=ret_.first->second;
    DataType *dtype=findDataType(getinfo.fields[getinfo.key].datatype.c_str());//findDataType("INT");

    //比较
    int previous_id = 0;
    //找空隙足够的block插入record
    while (block.getNextid()!=0) {

        struct iovec *iov_=new struct iovec[iovcnt];

        unsigned short reoffset = Root::ROOT_SIZE+(block.blockid()-1)*Block::BLOCK_SIZE+block.getSlot(block.getSlotsNum());
        unsigned char tempbuffer[Block::BLOCK_SIZE];
        //读长度，2B
        datafile_.read(offset,(char *)tempbuffer,2);
        size_t length = 0;Integer it;Record record_;
        bool tempret = it.decode((char *) tempbuffer, 2);
        //if (!tempret) return false;
        length = it.get();
        unsigned char header=0;
        datafile_.read(reoffset,(char *)tempbuffer,length);

        record_.attach(tempbuffer, unsigned short(length));
        record_.attach(tempbuffer, unsigned short(length));
        record_.ref(iov_,(int)iovcnt, &header);
        //if(record[getinfo.key]>iov_[getinfo.key]) compare是小于
        if(!dtype->compare(iov_[getinfo.key].iov_base, record[getinfo.key].iov_base,
        iov_[getinfo.key].iov_len,record[getinfo.key].iov_len)) {//不比当前block最大的大
            reoffset = Root::ROOT_SIZE+(block.blockid()-1)*Block::BLOCK_SIZE+block.getSlot(0);
            datafile_.read(reoffset,(char *)tempbuffer,2);
            size_t length = 0;Integer it;Record record_;
            bool re_ret = it.decode((char *)tempbuffer, 2);
            //if (!re_ret) return false;
            length = it.get();
            unsigned char header=0;
            datafile_.read(reoffset,(char *)tempbuffer,length);

            record_.attach(tempbuffer, unsigned short(length));
            record_.attach(tempbuffer, unsigned short(length));
            record_.ref(iov_,(int)iovcnt, &header);
            if(dtype->compare(iov_[getinfo.key].iov_base, record[getinfo.key].iov_base,
            iov_[getinfo.key].iov_len,record[getinfo.key].iov_len)) {//比当前block最小的大，就插入当前block
                int next=block.getNextid();
                ret=block.allocate(&header,record,(int)iovcnt);
                if(!ret) {//当前插入不了就分配一个新的block，改变指针，当前指向新写的，新写的指向当前的下一个
                    sortSlots(block,(int)iovcnt);
                    offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
                    block.setChecksum();
                    datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE); 
                    
                    
                    block.attach(buffer_);//更新当前block
                    std::cout<<"Block number: "<<block.blockid()<<std::endl;
                    block.setNextid(root.getGarbage());
                    offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
                    datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);  

                    block.clear(1,root.getGarbage());//写入下一个
                    block.setNextid(next);
                    block.allocate(&header,record,(int)iovcnt);
                    offset = (root.getGarbage()-1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
                    root.setGarbage(root.getGarbage()+1);
                    datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);


                    block.setChecksum();
                    datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
                }
                else offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
            }
            else  {//比当前最大和最小都小，也就是在两个block范围之间，先判断两个block是否能写，不能写再新建
                int next=block.getNextid();                
                
                ret=block.allocate(&header,record,(int)iovcnt);//当前是否能插入
                if(!ret) {
                    db::Block previous_block;
                    unsigned short previous_offset=Root::ROOT_SIZE+(previous_id-1)* Block::BLOCK_SIZE;
                    datafile_.read(previous_offset, (char *) buffer_, Block::BLOCK_SIZE);  
                    previous_block.attach(buffer_);
                    ret=previous_block.allocate(&header,record,(int)iovcnt);//上一个是否能插入
                    if(!ret) {
                        //都插入不了就分配一个新的block，改变指针，当前指向新写的，新写的指向当前的下一个
                        sortSlots(block,(int)iovcnt);
                        offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
                        block.setChecksum();
                        datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE); 
        

                        block.attach(buffer_);//更新当前block
                        std::cout<<"Block number: "<<block.blockid()<<std::endl;
                        block.setNextid(root.getGarbage());
                        offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
                        datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);  

                        block.clear(1,root.getGarbage());//写入下一个
                        block.setNextid(next);
                        block.allocate(&header,record,(int)iovcnt);
                        offset = (root.getGarbage()-1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
                        root.setGarbage(root.getGarbage()+1);
                        datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);


                        block.setChecksum();
                        datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE); 
                    }
                    else offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
                }
                else offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
            }
        }
        else  {//比当前最大的block大,就往下找
            previous_id=block.blockid();
            datafile_.read(Root::ROOT_SIZE+(block.getNextid()-1)* Block::BLOCK_SIZE,(char *)buffer_,Block::BLOCK_SIZE);
            block.attach(buffer_);continue;
        }
        /*ret=block.allocate(&header,record,(int)iovcnt);
        if(!ret) {
            datafile_.read(Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE,(char *)buffer_,Block::BLOCK_SIZE);
            block.attach(buffer_);continue;
        }
        offset=Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;*/
        break;
    }
    if(!ret)ret=block.allocate(&header,record,(int)iovcnt);//最后一个block
    
    //分配一个新的block
    if(!ret) {
        sortSlots(block,(int)iovcnt);

        offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
        block.setChecksum();
        datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE); 
        
        block.attach(buffer_);

        std::cout<<"Block number: "<<block.blockid()<<std::endl;
        
        block.setNextid(root.getGarbage());
        block.clear(1,root.getGarbage());
        block.allocate(&header,record,(int)iovcnt);
        offset = (root.getGarbage()-1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;//这里好像有点问题？
        root.setGarbage(root.getGarbage()+1);
        datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);
        

        block.setChecksum();
        datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE); 
        
        /*block.attach(buffer_);

        std::cout<<"Block number: "<<block.blockid()<<std::endl;
        
        block.setNextid(root.getGarbage());
        block.clear(1,root.getGarbage());
        block.allocate(&header,record,(int)iovcnt);
        offset = (root.getGarbage()-1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        root.setGarbage(root.getGarbage()+1);
        datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);*/
    }
    else offset= Root::ROOT_SIZE+(block.blockid()-1)* Block::BLOCK_SIZE;
    
    /*block.setChecksum();
    datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);  */
    
    //排序
    //读record
    //std::cout <<"Head "<<block.getSlot(0)<<" "<<std::endl;
    return 0;
}

int Table::update(int blockid,int slotid,struct iovec *record, size_t iovcnt) {
    //TODO:定位到对应record
    datafile_.read(Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE, (char *)buffer_,Block::BLOCK_SIZE);

    Block block;
    block.attach(buffer_);
    if(slotid>block.getSlotsNum())return -1;
    unsigned short offset = Root::ROOT_SIZE+(blockid-1)*Block::BLOCK_SIZE+block.getSlot(slotid);
    
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