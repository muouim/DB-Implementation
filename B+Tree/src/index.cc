#include <db/index.h>
std::string indexname("index.dat");

namespace db {

BplusTree::~BplusTree() {}

int BplusTree::create(char *name, RelationInfo &info)
{
    info.file = indexfile_;
    return gschema.create(name, info);
}
int BplusTree::open(const char *name)
{
    // 查找schema
    gschema.open();
    std::pair<Schema::TableSpace::iterator, bool> ret = gschema.lookup(name);
    gschema.load(ret.first);
    indexret_ = gschema.lookup(indexname.c_str());
    getinfo = indexret_.first->second;
    dtype = findDataType(getinfo.fields[getinfo.key].datatype.c_str());
    if (!ret.second) return EINVAL;
    int ret_ = indexfile_.open(name);
    unsigned long long length;
    ret_ = indexfile_.length(length);

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
        block.clear(1, 1);
        block.setNextid(-1);
        // 写root和block
        indexfile_.write(0, (const char *) rb, Root::ROOT_SIZE);
        indexfile_.write(
            Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    return S_OK;
    indexfile_.read(0, (char *) rb, Root::ROOT_SIZE);

}

bool indexcompare(
    std::pair<unsigned short, struct iovec> x,
    std::pair<unsigned short, struct iovec> y)
{
    //载入数据类型
    std::pair<Schema::TableSpace::iterator, bool> ret_ =
        gschema.lookup(indexname.c_str());
    db::RelationInfo getinfo;
    getinfo = ret_.first->second;
    DataType *dtype = findDataType(
        getinfo.fields[getinfo.key].datatype.c_str());
    //比较
    return dtype->compare(
        x.second.iov_base,
        y.second.iov_base,
        x.second.iov_len,
        y.second.iov_len);
}
int BplusTree::sortSlots(
    Block &block,
    size_t iovcnt,
    unsigned char *sortbuffer,
    unsigned char header)
{

    size_t reoffset = 0;
    std::vector<std::pair<unsigned short, struct iovec> > keys;
    char **base = new char *[block.getSlotsNum()];
    struct iovec *iov_ = new struct iovec[iovcnt];
    unsigned char recordbuffer[Block::BLOCK_SIZE];
    //取用所有主键字段
    std::vector<iovec *> deiov;
    for (int i = 0; i < block.getSlotsNum(); ++i) {
        reoffset = block.getSlot(i);
        getRecord(iov_, reoffset, iovcnt, sortbuffer, recordbuffer, &header);

        base[i] = new char[iov_[getinfo.key].iov_len];
        memcpy(base[i], iov_[getinfo.key].iov_base, iov_[getinfo.key].iov_len);

        std::pair<unsigned short, struct iovec> p(
            block.getSlot(i), iov_[getinfo.key]);

        keys.push_back(p);
    }
    for (int i = 0; i < block.getSlotsNum(); ++i)
        keys[i].second.iov_base = base[i];

    std::sort(keys.begin(), keys.end(), indexcompare);
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
int BplusTree::getRecord(
    struct iovec *iov,
    size_t offset,
    size_t iovcnt,
    unsigned char *tmpbuffer,
    unsigned char *recordbuffer,
    unsigned char *header)
{
    memcpy(recordbuffer, tmpbuffer + offset, 2);
    size_t length = 0;
    Integer it;
    Record record_;
    bool tempret = it.decode((char *) recordbuffer, 2);
    length = it.get();
    memcpy(recordbuffer, tmpbuffer + offset, length);
    record_.attach(recordbuffer, unsigned short(length));
    record_.ref(iov, (int) iovcnt, header);
    return S_OK;
}

int BplusTree::findToLeaf(
    struct iovec *record,
    size_t iovcnt,
    std::vector<int> &mystack)
{ //找到要插入的叶子节点

    size_t offset = 0;
    int ret = 0;
    unsigned char header = 0;

    Root root;
    root.attach(rb);
    unsigned int first = root.getHead();

    offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    db::Block block;
    block.attach(buffer_);
    mystack.clear();
    mystack.push_back(block.blockid());

    if (block.getSlotsNum() == 0) return block.blockid();

    size_t reoffset = 0;
    unsigned char recordbuffer[Block::BLOCK_SIZE];
    struct iovec *iov_ = new struct iovec[iovcnt];
    int blockid = first;
    int con = 0; // con=1，继续往下找b树
    while (con ==0) { 
        for (int i = 0; i < block.getSlotsNum(); ++i) {
            reoffset = block.getSlot(i);
            getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer, &header);

            if (dtype->compare(
                    record[getinfo.key].iov_base,
                    iov_[getinfo.key].iov_base,
                    record[getinfo.key].iov_len,
                    iov_[getinfo.key].iov_len)) { //找到第一个比记录大的
                blockid = *(int *) iov_[1].iov_base;
                if (blockid < 0) {
                    blockid = block.blockid();
                    con = 1;
                    mystack.pop_back();
                } //叶节点
                break;
            } else if (
                i == block.getSlotsNum() -
                         1) { //右指针,正数代表非叶节点，负数代表叶节点
                blockid = block.getNextid();
                if (blockid < 0) {
                    blockid = block.blockid();
                    con = 1;
                    mystack.pop_back();
                } //说明当前的是叶节点
                break;
            }
        }
        if (con == 0) {
            std::cout << "next____________" << blockid << std::endl;
            mystack.push_back(blockid);
            offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
            block.attach(buffer_);
        } else
            break;
    }
    delete[] iov_;
    return blockid;
}
std::pair<int, int> BplusTree::search(struct iovec *record, size_t iovcnt)
{ // 负值代表叶节点 正值代表非叶节点，遍历时通过栈记录路径

    std::vector<int> mystack;
    int blockid = findToLeaf(record, iovcnt, mystack);

    int nextid = 0;
    unsigned char header = 0;
    size_t reoffset = 0;
    unsigned char recordbuffer[Block::BLOCK_SIZE];

    size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    db::Block block;
    block.attach(buffer_);
    struct iovec *iov_ = new struct iovec[iovcnt];

    if (block.getSlotsNum() == 0) {
        nextid = -1;
        blockid = block.blockid();
    } else
        for (int i = 0; i < block.getSlotsNum();
             i++) { //找到刚好比key大的，然后往下迭代找，直到找到叶子节
            reoffset = block.getSlot(i);
            getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer, &header);
            if (dtype->compare(
                    record[getinfo.key].iov_base,
                    iov_[getinfo.key].iov_base,
                    record[getinfo.key].iov_len,
                    iov_[getinfo.key]
                        .iov_len)) { //找到第一个比记录大的,它的左指针就是我们要找的blockid
                blockid = -*(int *) iov_[1].iov_base;
                nextid = i;
                break;
            }
            if (i == block.getSlotsNum() -
                         1) { //右指针,正数代表非叶节点，负数代表叶节点
                blockid = -block.getNextid();
                nextid = -1;
            }
        }
    //返回两个，如果右边有就返回右边的，没有就回溯mystack，往回找，因为是左指针，所以是后继，
    //不用管nextidid，如果使用nextid，可以直接使用nextid，因为是左指针
    std::pair<int, int> result(abs(blockid), nextid);
    delete[] iov_;
    return result;
}

int BplusTree::insert(
    struct iovec *record,
    size_t iovcnt,
    int change,
    int rightid)
{
    // b+树采取的左指针，指向比当前主键小的block，最后一个block放在nextid里面
    //正数代表非叶节点，负数代表叶节点(指向数据Block的节点)
    size_t offset = 0;
    size_t reoffset = 0;
    size_t newoffset = 0;

    int ret = 0;
    unsigned char header = 0;

    Root root;
    root.attach(rb);
    struct iovec *iov_ = new struct iovec[iovcnt];
    struct iovec *tempiov_ = new struct iovec[iovcnt];
    int blockid = findToLeaf(record, iovcnt, mystack);
    std::cout << "Insert Index Block id: " << blockid << std::endl;

    unsigned char newbuffer[Block::BLOCK_SIZE];
    unsigned char recordbuffer[Block::BLOCK_SIZE];
    offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    db::Block block;
    block.attach(buffer_);

    if (change == -1) { //更新 叶节点右指针
        std::cout << "right" << std::endl;
        block.setNextid(rightid);
    } else {
        reoffset = block.getSlot(change);
        getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer, &header);
        iov_[1].iov_base = &rightid;
        update(blockid, change, iov_, iovcnt);
        // block.attach(buffer_);
    }
    db::Block newblock;
    newblock.attach(newbuffer);

    int flag = 0; // 0插入左边，1插入右边
    ret = block.allocate(&header, record, (int) iovcnt);
    if (ret) { //分裂，插入父节点 //判断是否是root
        sortSlots(block, 2, buffer_, header);
        indexfile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
        std::cout << "next indexid " << block.getNextid() << std::endl;
        delete[] iov_;
        delete[] tempiov_;
        return S_OK;
    }
    int count = 0;
    while (!ret) {
        int garbage = root.getGarbage();
        int left = block.getSlotsNum() / 2 + 1;

        reoffset = block.getSlot(left);
        getRecord(tempiov_, reoffset, iovcnt, buffer_, recordbuffer, &header);
        //获取中间左指针，判断是插入左block还是右block
        if (dtype->compare(
                record[getinfo.key].iov_base,
                tempiov_[getinfo.key].iov_base,
                record[getinfo.key].iov_len,
                tempiov_[getinfo.key].iov_len))
            flag = 0;
        else
            flag = 1;

        int nextid = *(int *) tempiov_[1].iov_base;
        //新分裂左block
        newblock.clear(1, root.getGarbage());
        newblock.setNextid(nextid); //右指针
        for (int i = 0; i < left; i++) {
            reoffset = block.getSlot(i);
            getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer, &header);
            ret = newblock.allocate(&header, iov_, (int) iovcnt);
        }
        newoffset =
            (root.getGarbage() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        if (flag == 0) {
            newblock.allocate(
                &header, record, (int) iovcnt); //是否比当前最小的小
            sortSlots(newblock, 2, newbuffer, header);
        }
        indexfile_.write(
            newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);
        root.setGarbage(root.getGarbage() + 1);

        nextid = block.getNextid(); //叶节点，本身就是负的，不用特判
        newblock.clear(1, block.blockid()); //右block不新开，覆盖原来的block
        newblock.setNextid(nextid);         //右指针
        for (int i = left + 1; i < block.getSlotsNum(); i++) {
            reoffset = block.getSlot(i);
            getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer, &header);
            ret = newblock.allocate(&header, iov_, (int) iovcnt);
        }
        newoffset = (block.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        if (flag == 1) {
            newblock.allocate(&header, record, (int) iovcnt);
            sortSlots(newblock, 2, newbuffer, header);
        }
        indexfile_.write(
            newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);

        reoffset = block.getSlot(left); //获取中间左指针
        getRecord(tempiov_, reoffset, iovcnt, buffer_, recordbuffer, &header);
        tempiov_[1].iov_base = &garbage;
        tempiov_[1].iov_len = sizeof(int);

        //父节点,如果当前是根节点，新开一个block作为新的根节点
        if (block.blockid() == root.getHead()) {
            std::cout << (int) header
                      << " New Root Block id: " << root.getGarbage()
                      << std::endl;
            newblock.clear(1, root.getGarbage()); //新开父节点
            newblock.setNextid(block.blockid());  //右指针
            ret = newblock.allocate(
                &header, tempiov_, (int) iovcnt); //(unsigned char)0
            newoffset =
                (root.getGarbage() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            indexfile_.write(
                newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);

            root.setHead(root.getGarbage());
            root.setGarbage(root.getGarbage() + 1);
            indexfile_.write(0, (const char *) rb, Root::ROOT_SIZE);
            break;
        }
        indexfile_.write(0, (const char *) rb, Root::ROOT_SIZE);

        blockid = mystack[mystack.size() - count - 1]; //父节点
        std::cout << "Fa Block id: " << blockid << std::endl;

        offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
        block.attach(buffer_);
        ret = block.allocate(&header, tempiov_, (int) iovcnt);
        if (ret) {
            sortSlots(block, 2, buffer_, header);
            block.setChecksum();
            indexfile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
            break;
        } else {
            count++; //中间的往上插的记录给record
            record = tempiov_;
        }
    }
    delete[] tempiov_;
    delete[] iov_;
    return S_OK;
}

int BplusTree::update(
    struct iovec *record,
    size_t iovcnt,
    struct iovec *newrecord)
{
    //更新索引主键-table外部调用
    std::pair<int, int> tmp = findWay(record, iovcnt);
    int blockid = tmp.first, slotid = tmp.second;
    std::cout << "upid " << blockid << " " << slotid << std::endl;

    if (slotid == -1) return -1;

    size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    Block block;
    block.attach(buffer_);

    if (slotid > block.getSlotsNum() - 1) {
        std::cout << "Num Error" << std::endl;
        return -1;
    }
    struct iovec *iov_ = new struct iovec[iovcnt];
    unsigned char header = 0;
    unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};
    int reoffset = block.getSlot(slotid);
    getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer, &header);

    int id = *(int *) iov_[1].iov_base;
    newrecord[1].iov_base = &id;
    newrecord[1].iov_len = sizeof(int);
    update(blockid, slotid, newrecord, iovcnt);

    delete[] iov_;
    return S_OK;
}
int BplusTree::update(
    int blockid,
    int slotid,
    struct iovec *record,
    size_t iovcnt)
{
    //更新索引主键-index内部调用
    size_t offset = Root::ROOT_SIZE + (blockid - 1) * Block::BLOCK_SIZE;
    Block block;
    block.attach(buffer_);

    if (slotid > block.getSlotsNum() - 1) {
        std::cout << "Num Error" << std::endl;
        return -1;
    }
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
    updaterecord.attach(uprecordbuffer, unsigned short(length));
    updaterecord.set(record, (int) iovcnt, &header);
    int newlength = (int) updaterecord.length();

    //判断是不是能插入，能插入就更新
    if (newlength <= length) {
        memcpy(buffer_ + reoffset, (char *) uprecordbuffer, length);
    } else {
        std::cout << "can not insert " << newlength << " " << length
                  << std::endl;
        remove(block, slotid, iovcnt);
        insert(record, iovcnt, -2, 0);
    }
    return S_OK;
}

int BplusTree::gettableBrother(struct iovec *record, size_t iovcnt, int slotid)
{
    //找到数据Block的兄弟节点
    int blockid = findToLeaf(record, iovcnt, mystack);
    return getBrother(slotid, blockid, iovcnt);
}
int BplusTree::getBrother(int slotid, int fatherid, size_t iovcnt)
{
    //找到索引IndexBlock的兄弟节点
    unsigned char tmpbuffer[Block::BLOCK_SIZE] = {0};
    size_t offset = (fatherid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.read(offset, (char *) tmpbuffer, Block::BLOCK_SIZE);
    db::Block block;
    block.attach(tmpbuffer);

    unsigned char header = 0;
    unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};
    struct iovec *iov_ = new struct iovec[iovcnt];

    if (block.getSlotsNum() == 0) return 0;
    
    if (slotid == -1) {
        int reoffset = block.getSlot(block.getSlotsNum() - 1);
        getRecord(iov_, reoffset, iovcnt, tmpbuffer, recordbuffer, &header);
        int id = abs(*(int *) iov_[1].iov_base);
        delete[] iov_;
        return id;
    } else if (slotid != block.getSlotsNum() - 1) {
        int reoffset = block.getSlot(slotid + 1);
        getRecord(iov_, reoffset, iovcnt, tmpbuffer, recordbuffer, &header);
        int id = abs(*(int *) iov_[1].iov_base);
        delete[] iov_;
        return id;
    } else if (slotid == block.getSlotsNum() - 1) {
        int id = abs(block.getNextid());
        delete[] iov_;
        return id;
    }
    delete[] iov_;
    return S_OK;
}

int BplusTree::countBlockSize(
    Block &block,
    unsigned char *tmpbuffer,
    size_t iovcnt)
{
    //计算BlockSize，返回所有记录累积大小

    int len = 0;
    unsigned char header = 0;
    unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};
    struct iovec *iov_ = new struct iovec[iovcnt];
    for (int i = 0; i < block.getSlotsNum(); i++) {
        int reoffset = block.getSlot(i);
        getRecord(iov_, reoffset, iovcnt, tmpbuffer, recordbuffer, &header);
        for (int j = 0; j < iovcnt; j++)
            len += (int) iov_[j].iov_len;
    }
    return len;
}

int BplusTree::merge(
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
    newblock.setNextid(block2.blockid()); 
    struct iovec *iov_ = new struct iovec[iovcnt];
    unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};
    for (int i = 0; i < block1.getSlotsNum(); i++) {
        reoffset = block1.getSlot(i);
        getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer, &header);
        ret = newblock.allocate(&header, iov_, (int) iovcnt);
        if (!ret) break;
    }
    for (int i = 0; i < block2.getSlotsNum(); i++) {
        if (!ret) break;
        size_t reoffset = block2.getSlot(i);
        getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer, &header);
        ret = newblock.allocate(&header, iov_, (int) iovcnt);
    }

    reoffset = block2.getSlot(0);
    getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer, &header);
    if (!ret) { //无法合并，只借一个，更新索引
        block1.allocate(&header, iov_, (int) iovcnt);
        remove(block2, 0, iovcnt);
    } else { //合并，删除索引
        int a = *(int *) iov_[getinfo.key].iov_base;
        struct iovec *indexiov_ = new struct iovec[2];

        indexiov_[0].iov_base = &a;
        indexiov_[0].iov_len = iov_[getinfo.key].iov_len;
        delete [] indexiov_;
    }
    size_t newoffset =
        (block1.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);

    block2.setSlotsNum(0);
    newoffset = (block2.blockid() - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.write(newoffset, (const char *) newbuffer, Block::BLOCK_SIZE);
    delete [] iov_;

    return S_OK;
}

std::pair<int, int> BplusTree::findWay(struct iovec *record, size_t iovcnt)
{
    size_t offset = 0;
    int ret = 0;
    unsigned char header = 0;

    Root root;
    root.attach(rb);

    unsigned int first = root.getHead();
    offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    db::Block block;
    block.attach(buffer_);
    mystack.clear();
    mystack.push_back(block.blockid());

    if (block.getSlotsNum() == 0) {
        std::pair<int, int> tmp(0, 0);
        return tmp;
    }
    size_t reoffset = 0;
    unsigned char recordbuffer[Block::BLOCK_SIZE];
    struct iovec *iov_ = new struct iovec[iovcnt];
    int blockid = first, slotid = -1;
    int con = 0; // con=1，继续往下找b树
    while (con == 0) {
        for (int i = 0; i < block.getSlotsNum(); ++i) {
            reoffset = block.getSlot(i);
            getRecord(iov_, reoffset, iovcnt, buffer_, recordbuffer, &header);
            if (dtype->equal(
                    record[getinfo.key].iov_base,
                    iov_[getinfo.key].iov_base,
                    record[getinfo.key].iov_len,
                    iov_[getinfo.key].iov_len)) {
                con = 1;
                slotid = i;
                blockid = block.blockid();
                mystack.pop_back();
                break;
            }
            if (dtype->compare(
                    record[getinfo.key].iov_base,
                    iov_[getinfo.key].iov_base,
                    record[getinfo.key].iov_len,
                    iov_[getinfo.key].iov_len)) { //找到第一个比记录大的
                blockid = *(int *) iov_[1].iov_base;
                if (blockid < 0) {
                    blockid = block.blockid();
                    con = 1;
                    mystack.pop_back();
                }
                break;
            } else if (
                i == block.getSlotsNum() -
                         1) { //右指针,正数代表非叶节点，负数代表叶节点
                blockid = block.getNextid();
                if (blockid < 0) {
                    blockid = block.blockid();
                    con = 1;
                    mystack.pop_back();
                } //说明当前的是叶节点
                break;
            }
        }
        if (con == 0) {
            std::cout << "next____________" << blockid << std::endl;
            mystack.push_back(blockid);
            offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
            block.attach(buffer_);
        } else
            break;
    }
    delete[] iov_;
    std::pair<int, int> re(blockid, slotid);
    return re;
}

int BplusTree::remove(struct iovec *record, size_t iovcnt)
{ // index外部也就是table调它，从头开始找，找到了再去调它，目的是找到删除记录所在的位置以及他的father

    std::pair<int, int> tmp = findWay(record, iovcnt);
    int blockid = tmp.first, slotid = tmp.second;
    size_t offset = (blockid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    indexfile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    Block block;
    block.attach(buffer_);
    remove(block, slotid, iovcnt);
    return S_OK;
}

int BplusTree::remove(Block &block, int slotid, size_t iovcnt)
{ // index内部调它，直接给定id，路径已经生成

    if (slotid > block.getSlotsNum() - 1) return -1;
    unsigned short offset = block.getSlot(slotid);
    unsigned short temp = block.getGarbage();
    memcpy(buffer_ + offset + 2, &temp, sizeof(temp));
    block.setGarbage(slotid);

    for (int i = slotid; i < block.getSlotsNum() - 1; i++) {
        block.setSlot(i, block.getSlot(i + 1));
    }
    block.setSlot(block.getSlotsNum() - 1, 0);
    block.setSlotsNum(block.getSlotsNum() - 1);
    indexfile_.write(
        Root::ROOT_SIZE + (block.blockid() - 1) * Block::BLOCK_SIZE,
        (char *) buffer_,
        Block::BLOCK_SIZE);
    //删除完需要判断是否<50%,如果小于，那么需要向兄弟节点借一个节点然后更新兄弟节点的索引节点，并更新兄弟节点对应的索引
    //如果两个都 <50%，就合并两个，删除当前Block对应的索引记录。
    if (countBlockSize(block, buffer_, iovcnt) <= Block::BLOCK_SIZE / 2) {
        if (mystack.size() == 0) return S_OK;
        int brotherid = getBrother(slotid, mystack[mystack.size() - 1], iovcnt);
        unsigned char brobuffer[Block::BLOCK_SIZE] = {0};
        offset = (brotherid - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        indexfile_.read(offset, (char *) brobuffer, Block::BLOCK_SIZE);
        Block broblock;
        broblock.attach(brobuffer);

        Block newblock;
        unsigned char newbuffer[Block::BLOCK_SIZE] = {0};
        merge(block,buffer_,broblock,newbuffer,newblock,newbuffer,iovcnt); //判断是取一个还是合并
    }
    return S_OK;
}

} // namespace db
