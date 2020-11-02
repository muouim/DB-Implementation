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
int Table::create(
    char *name,
    RelationInfo &info,
    char *indexname,
    RelationInfo &indexinfo)
{
    //创建schema,创建数据表的同时创建索引表
    info.file = datafile_;
    gschema.create(indexname, indexinfo);
    return gschema.create(name, info);
}
int Table::open(const char *name)
{
    // 查找schema
    gschema.open();
    std::pair<Schema::TableSpace::iterator, bool> ret = gschema.lookup(name);
    gschema.load(ret.first);
    taret_ = gschema.lookup(tablename.c_str());
    getinfo = taret_.first->second;
    dtype = findDataType(getinfo.fields[getinfo.key].datatype.c_str());
    if (!ret.second) return EINVAL;
    int ret_ = datafile_.open(name);
    unsigned long long length;
    ret_ = datafile_.length(length);

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
        block.clear(1, 1);
        // 写root和block
        datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);
        datafile_.write(
            Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    datafile_.read(0, (char *) rb, Root::ROOT_SIZE);

    index.open(indexname_.c_str());
    index.indexfile_.open(indexname_.c_str());
    index.indexfile_.read(0, (char *) indexrb, Root::ROOT_SIZE);

    return S_OK;
}

bool compare(
    std::pair<unsigned short, struct iovec> x,
    std::pair<unsigned short, struct iovec> y)
{
    std::pair<Schema::TableSpace::iterator, bool> ret_ =
        gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;
    getinfo = ret_.first->second;
    DataType *dtype =
        findDataType(getinfo.fields[getinfo.key].datatype.c_str());
    //载入数据类型
    return dtype->compare(
        x.second.iov_base,
        y.second.iov_base,
        x.second.iov_len,
        y.second.iov_len);
}
int Table::sortSlots(Block &block, size_t iovcnt, unsigned char *sortbuffer)
{
    size_t reoffset = 0;
    std::vector<std::pair<unsigned short, struct iovec> > keys;
    unsigned char header = 0;
    char **base = new char *[block.getSlotsNum()];
    struct iovec *iov_ = new struct iovec[iovcnt];
    unsigned char recordbuffer[Block::BLOCK_SIZE];

    //取用所有主键字段
    for (int i = 0; i < block.getSlotsNum(); i++) {
        reoffset = block.getSlot(i);
        getRecord(iov_, reoffset, iovcnt, sortbuffer, recordbuffer);

        base[i] = new char[iov_[getinfo.key].iov_len];
        memcpy(base[i], iov_[getinfo.key].iov_base, iov_[getinfo.key].iov_len);
        std::pair<unsigned short, struct iovec> p(
            block.getSlot(i), iov_[getinfo.key]);
        keys.push_back(p);
    }
    for (int i = 0; i < block.getSlotsNum(); ++i)
        keys[i].second.iov_base = base[i];

    std::sort(keys.begin(), keys.end(), compare);
    int tempslotid = 0;
    for (int i = 0; i < block.getSlotsNum(); i++) {
        block.setSlot(i, keys[i].first);
        tempslotid++;
    }
    for (int i = 0; i < block.getSlotsNum(); i++)
        delete[] base[i];
    delete[] iov_;
    delete[] base;

    return S_OK;
}

int Table::getRecord(
    struct iovec *iov,
    size_t offset,
    size_t iovcnt,
    unsigned char *tmpbuffer,
    unsigned char *recordbuffer)
{
    memcpy(recordbuffer, tmpbuffer + offset, 2);
    size_t length = 0;
    Integer it;
    Record record_;
    bool tempret = it.decode((char *) recordbuffer, 2);
    length = it.get();
    unsigned char header = 0;
    memcpy(recordbuffer, tmpbuffer + offset, length);

    record_.attach(recordbuffer, unsigned short(length));
    record_.ref(iov, (int) iovcnt, &header);
    return S_OK;
}

int Table::insert(struct iovec *record, size_t iovcnt)
{ 
    size_t offset = 0;
    size_t newoffset = 0;
    size_t reoffset = 0;

    int ret = 0;
    unsigned char header = 0;
    //读取root
    db::Root root;
    root.attach(rb);

    unsigned char newbuffer[Block::BLOCK_SIZE];

    // TODO:怎么载入想要插入record的BLOCK？
    db::Block block;

    struct iovec *iov_ = new struct iovec[iovcnt];
    unsigned char recordbuffer[Block::BLOCK_SIZE];

    //插入索引
    struct iovec *indexiov_ = new struct iovec[2];
    int a = *(int *) record[getinfo.key].iov_base;
    indexiov_[0].iov_base = &a;
    indexiov_[0].iov_len = record[getinfo.key].iov_len;

    Root indexroot;
    indexroot.attach(indexrb);

    //找到block所在的位置
    std::pair<int, int> tmpair = index.search(indexiov_, 2);
    int blockid = tmpair.first;
    int change = tmpair.second; //是否改变索引右指针

    // std::cout<<"Insert "<<a<<" "<<blockid<<std::endl;

    offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);

    block.attach(buffer_);
    db::Block newblock;
    newblock.attach(newbuffer);

    ret = block.allocate(&header,record,(int)iovcnt); 
    offset = Root::ROOT_SIZE + (block.blockid() - 1) * Block::BLOCK_SIZE;

    struct iovec *tmpiov_ = new struct iovec[iovcnt];
    int flag = 0; // 0插入左边，1插入右边

    if (!ret) { //无法插入：直接分裂 从中间劈开，保留左边的，右边新建一个block
        int left = block.getSlotsNum() / 2;
        reoffset = block.getSlot(left); //获取中间左指针
        getRecord(tmpiov_, reoffset, iovcnt, buffer_, recordbuffer);

        if (dtype->compare(
                record[getinfo.key].iov_base,
                tmpiov_[getinfo.key].iov_base,
                record[getinfo.key].iov_len,
                tmpiov_[getinfo.key].iov_len)) //判断是插入左block还是右block
            flag = 0;
        else
            flag = 1;

        int nextid = block.getNextid();
        int currentid = block.blockid();
        int garbage = root.getGarbage();
        std::cout << "New Block id: " << garbage << std::endl;

        newblock.clear(1, root.getGarbage()); //新分裂右block
        newblock.setNextid(nextid);           //
        for (int i = left + 1; i < block.getSlotsNum(); i++) {
            reoffset = block.getSlot(i);
            getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer);
            ret = newblock.allocate(&header, iov_, (int) iovcnt);
        }
        newoffset = (garbage - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        if (flag == 1) {
            newblock.allocate(&header, record, (int) iovcnt);
            sortSlots(newblock, (int) iovcnt, newbuffer);
        }
        datafile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);
        root.setGarbage(root.getGarbage() + 1);

        newblock.clear(1, block.blockid()); //左block不新开，覆盖原来的block
        newblock.setNextid(garbage);        //
        for (int i = 0; i < left + 1; i++) {
            reoffset = block.getSlot(i);
            getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer);
            ret = newblock.allocate(&header, iov_, (int) iovcnt);
        }
        newoffset = (currentid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        if (flag == 0) {
            newblock.allocate(&header, record, (int) iovcnt);
            sortSlots(newblock, (int) iovcnt, newbuffer);
        }
        datafile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);
        datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);

        a = *(int *) tmpiov_[getinfo.key].iov_base;
        indexiov_[0].iov_base = &a;
        indexiov_[0].iov_len = tmpiov_[getinfo.key].iov_len;
        int tmpid = -currentid;
        indexiov_[1].iov_base = &tmpid; //左指针指向当前block
        indexiov_[1].iov_len = sizeof(int);

        index.insert(indexiov_,2,change,-garbage); //如果是右指针改变nextid change=-1
        ////change>=0,需要改变原来的指针指向garbage
    } else {           //能插入直接写入
        sortSlots(block, (int) iovcnt, buffer_);
        block.setChecksum();
        datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    delete[] iov_;
    delete[] indexiov_;
    delete[] tmpiov_;

    return S_OK;
}
int Table::update(struct iovec *record, size_t iovcnt, struct iovec *newrecord)
{
    std::pair<int, int> tmpair = findkey(record, iovcnt);
    int blockid = tmpair.first;
    int slotid = tmpair.second;
    update(blockid, slotid, newrecord, iovcnt);
    return S_OK;
}

int Table::update(int blockid, int slotid, struct iovec *record, size_t iovcnt)
{
    size_t offset = Root::ROOT_SIZE + (blockid - 1) * Block::BLOCK_SIZE;
    datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    Block block;
    block.attach(buffer_);

    if (slotid > block.getSlotsNum() - 1) {
        std::cout << "num error" << std::endl;
        return -1;
    }
    if (slotid == -1) slotid = block.getSlotsNum() - 1;

    size_t length = 0;
    Integer it;

    unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};
    size_t reoffset = block.getSlot(slotid);
    memcpy(recordbuffer, buffer_ + reoffset, 2);
    bool ret = it.decode((char *) recordbuffer, 2);
    length = it.get();
    if (!ret) {
        std::cout << " error " << length << std::endl;
        return -1;
    }

    Record updaterecord;
    const unsigned char header = 0;
    unsigned char uprecordbuffer[Block::BLOCK_SIZE] = {0};
    updaterecord.attach(uprecordbuffer, Block::BLOCK_SIZE);
    updaterecord.set(record, (int) iovcnt, &header);
    int newlength = (int) updaterecord.length();

    //判断是不是能插入，能插入就更新
    if (newlength <= length) {
        block.setChecksum();
        datafile_.write(offset + reoffset, (char *) uprecordbuffer, newlength);
        std::cout << "can insert " << newlength << " " << length << std::endl;
    }
    //不能删除原来的，调用insert重新插入
    else {
        std::cout << "can not insert " << newlength << " " << length
                  << std::endl;
        remove(record, iovcnt);
        insert(record, iovcnt);
    }
    return S_OK;
}
int Table::getBrother(struct iovec *record, size_t iovcnt, int slotid)
{   
    //向兄弟节点借一个节点然后更新兄弟节点的索引节点
    //如果非最右节点，找next，如果是最右节点，找前一个

    int a = *(int *) record[getinfo.key].iov_base;
    struct iovec *indexiov_ = new struct iovec[2];
    indexiov_[0].iov_base = &a;
    indexiov_[0].iov_len = record[getinfo.key].iov_len;
    index.gettableBrother(indexiov_, 2, slotid);
    delete[] indexiov_;
    return S_OK;
}

int Table::countBlockSize(Block &block, unsigned char *tmpbuffer, size_t iovcnt)
{
    //计算BlockSize，返回所有记录累积大小
    int len = 0;
    unsigned char header = 0;
    unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};
    struct iovec *iov_ = new struct iovec[iovcnt];
    for (int i = 0; i < block.getSlotsNum(); i++) {
        int reoffset = block.getSlot(i);
        getRecord(iov_, reoffset, iovcnt, tmpbuffer, recordbuffer);
        for (int j = 0; j < iovcnt; j++)
            len += (int) iov_[j].iov_len;
    }
    return len;
}

int Table::merge(
    Block &block1,
    unsigned char *buffer1,
    Block &block2,
    unsigned char *buffer2,
    Block &newblock,
    unsigned char *newbuffer,
    size_t iovcnt)
{
    //合并Block，直接进行合并
    //如果可以合并，则直接合并，并删除后一个Block中最小的记录对应的索引记录
    //如果不可以合并，则只从兄弟节点借一个，并更新相应的索引记录

    int ret = 0;
    unsigned char header = 0;
    size_t reoffset = 0;
    newblock.clear(1, block1.blockid());
    newblock.setNextid(block2.blockid()); //
    struct iovec *iov_ = new struct iovec[iovcnt];
    unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};
    for (int i = 0; i < block1.getSlotsNum(); i++) {
        reoffset = block1.getSlot(i);
        getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer);
        ret = newblock.allocate(&header, iov_, (int) iovcnt);
        if (!ret) break;
    }
    for (int i = 0; i < block2.getSlotsNum(); i++) {
        if (!ret) break;
        size_t reoffset = block2.getSlot(i);
        getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer);
        ret = newblock.allocate(&header, iov_, (int) iovcnt);
    }

    reoffset = block2.getSlot(0);
    getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer);
    if (!ret) { //无法合并，只借一个，更新索引
        block1.allocate(&header, iov_, (int) iovcnt);
        remove(block2.blockid(), 0, iov_, iovcnt);
    } else { //合并，删除索引
        int a = *(int *) iov_[getinfo.key].iov_base;
        struct iovec *indexiov_ = new struct iovec[2];

        indexiov_[0].iov_base = &a;
        indexiov_[0].iov_len = iov_[getinfo.key].iov_len;
        index.remove(indexiov_, 2);
        delete [] indexiov_;

    }
    size_t newoffset =
        (block1.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    datafile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);

    block2.setSlotsNum(0);
    newoffset = (block2.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    datafile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);
    delete [] iov_;
    return S_OK;
}

int Table::remove(struct iovec *record, size_t iovcnt)
{
    std::pair<int, int> tmpair = findkey(record, iovcnt);
    int blockid = tmpair.first;
    int slotid = tmpair.second; //找到记录对应的Block和slotid
    remove(tmpair.first, tmpair.second, record, iovcnt);
    return S_OK;
}
int Table::remove(int blockid, int slotid, struct iovec *record, size_t iovcnt)
{ 
    //写到garbage，替换record记录长度前2B，形成链表，新删除的写入头部，替换原来头部record记录长度前2B
    datafile_.read(
        Root::ROOT_SIZE + (blockid - 1) * Block::BLOCK_SIZE,
        (char *) buffer_,
        Block::BLOCK_SIZE);

    Block block;
    block.attach(buffer_);
    if (slotid > block.getSlotsNum() - 1) return -1;
    int n = block.getSlotsNum();

    unsigned short offset = block.getSlot(slotid);
    unsigned short temp = block.getGarbage();
    memcpy(buffer_ + offset + 2, &temp, sizeof(temp));
    block.setGarbage(slotid);

    // slots重新排序，向前挤压
    for (int i = slotid; i < block.getSlotsNum() - 1; i++) {
        block.setSlot(i, block.getSlot(i + 1));
    }
    block.setSlot(block.getSlotsNum() - 1, 0);
    block.setSlotsNum(n - 1);
    datafile_.write(
        Root::ROOT_SIZE + (blockid - 1) * Block::BLOCK_SIZE,
        (char *) buffer_,
        Block::BLOCK_SIZE);

    if (slotid == 0 && block.getSlotsNum() >= 1) { //如果删第一个就要调整索引
        int reoffset = block.getSlot(0);
        unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};
        struct iovec *iov_ = new struct iovec[iovcnt];
        getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer);

        int a = *(int *) record[getinfo.key].iov_base;
        int b = *(int *) iov_[getinfo.key].iov_base;

        struct iovec *indexiov_ = new struct iovec[2];
        struct iovec *newindexiov_ = new struct iovec[2];

        indexiov_[0].iov_base = &a;
        indexiov_[0].iov_len = record[getinfo.key].iov_len;
        newindexiov_[0].iov_base = &b;
        newindexiov_[0].iov_len = iov_[getinfo.key].iov_len;

        index.update(indexiov_, 2, newindexiov_);
        delete[] iov_;
        delete[] indexiov_;
        delete[] newindexiov_;
    }
    if (slotid != -1 &&
        countBlockSize(block, buffer_, iovcnt) <= Block::BLOCK_SIZE / 2) {
        int brotherid = getBrother(record, iovcnt, slotid); //找到并载入兄弟节点
        if (brotherid == 0) return S_OK;

        unsigned char brobuffer[Block::BLOCK_SIZE] = {0};
        offset = (brotherid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        datafile_.read(offset, (char *) brobuffer, Block::BLOCK_SIZE);
        Block broblock;
        broblock.attach(brobuffer);
        unsigned char newbuffer[Block::BLOCK_SIZE] = {0};
        Block newblock;
        newblock.attach(newbuffer);

        merge(block,buffer_,broblock,newbuffer,newblock,newbuffer,iovcnt); //判断是取一个还是合并
    }
    return S_OK;
}

std::pair<int, int> Table::findkey(struct iovec *key, size_t iovcnt)
{
    int slotid_ = 0;
    size_t offset = 0, reoffset = 0;
    int ret = 0;
    unsigned char header = 1;

    //读取root
    Root root;
    root.attach(rb);

    size_t length = 0;
    Integer it;
    Record record_;
    struct iovec *iov_ = new struct iovec[iovcnt];
    unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};

    // TODO:使用b树找到对应block
    struct iovec *indexiov_ = new struct iovec[2];
    int a = *(int *) key[getinfo.key].iov_base;
    indexiov_[0].iov_base = &a;
    indexiov_[0].iov_len = key[getinfo.key].iov_len;

    Root indexroot;
    indexroot.attach(indexrb);

    //找到block所在的位置
    std::pair<int, int> tmpair = index.search(indexiov_, 2);
    offset = (tmpair.first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    Block block;
    block.attach(buffer_);

    //遍历record
    int flag = 0;
    int lower = 0, big = block.getSlotsNum() - 1;

    while (big >= lower) {
        reoffset = block.getSlot((lower + big) / 2);
        getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer);

        if (dtype->equal(
                key[getinfo.key].iov_base,
                iov_[getinfo.key].iov_base,
                key[getinfo.key].iov_len,
                iov_[getinfo.key].iov_len)) {
            slotid_ = (lower + big) / 2;
            flag = 1;
            break;
        }
        if (dtype->compare(
                key[getinfo.key].iov_base,
                iov_[getinfo.key].iov_base,
                key[getinfo.key].iov_len,
                iov_[getinfo.key].iov_len)) {
            big = (lower + big) / 2 - 1;
            continue;
        } else
            lower = (lower + big) / 2 + 1;
    }

    if (flag == 0) {
        std::pair<int, int> no(0, 0);
        return no;
    }
    std::pair<int, int> p(block.blockid(), slotid_);
    
    delete[] iov_;
    delete[] indexiov_;
    return p;
}

} // namespace db

