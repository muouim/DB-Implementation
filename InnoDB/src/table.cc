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
int Table::create(char *name, RelationInfo &info)
{ 
    //创建schema
    info.file = datafile_;
    return gschema.create(name, info);
}
int Table::open(const char *name)
{ 
    //查找schema
    gschema.open();
    std::pair<Schema::TableSpace::iterator, bool> ret = gschema.lookup(name);
    gschema.load(ret.first);
    
    if (!ret.second) return EINVAL;
    
    int ret_ = datafile_.open(name);
    unsigned long long length;
    ret_ = datafile_.length(length);

    //如果meta.db长度为0，则写一个block
    if (ret_) return ret_;
    if (length) {
        //加载ROOT
        datafile_.read(0, (char *) buffer_, Root::ROOT_SIZE);
        //获取第1个block
        Root root;
        root.attach(buffer_);
        unsigned int first = root.getHead();
        size_t offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    } else {
        //创建root
        Root root;
        unsigned char rb[Root::ROOT_SIZE];
        root.attach(rb);
        root.clear(BLOCK_TYPE_META);
        root.setHead(1);
        root.setGarbage(2);
        //创建第1个block
        Block block;
        block.attach(buffer_);
        block.clear(1, 1);
        //写root和block
        datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);
        datafile_.write(
            Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
    }
    datafile_.read(0, (char *) rb, Root::ROOT_SIZE);
    return S_OK;
}

bool compare(std::pair<unsigned short, struct iovec> x, std::pair<unsigned short, struct iovec> y)
{
    std::pair<Schema::TableSpace::iterator, bool> ret_ =
        gschema.lookup(tablename.c_str());
    
    db::RelationInfo getinfo;
    getinfo = ret_.first->second;
    DataType *dtype = findDataType(getinfo.fields[getinfo.key].datatype.c_str());
    
    return dtype->compare(
        x.second.iov_base,
        y.second.iov_base,
        x.second.iov_len,
        y.second.iov_len);
}

int Table::sortSlots(Block &block, int iovcnt)
{
    //载入relationinfo
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;
    getinfo = ret_.first->second;

    std::vector<std::pair<unsigned short, struct iovec> > keys;
    unsigned char header = 0;
    char **base = new char *[block.getSlotsNum()];
    unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};
    size_t reoffset = 0;
    struct iovec *iov_ = new struct iovec[iovcnt];

    for (int i = 0; i < block.getSlotsNum(); ++i) { //取用所有主键字段
        reoffset = block.getSlot(i);
        getRecord(iov_, reoffset, iovcnt, recordbuffer);

        base[i] = new char[iov_[getinfo.key].iov_len];
        memcpy(base[i], iov_[getinfo.key].iov_base, iov_[getinfo.key].iov_len);
        std::pair<unsigned short, struct iovec> p(block.getSlot(i), iov_[getinfo.key]);
        keys.push_back(p);
    }

    for (int i = 0; i < block.getSlotsNum(); ++i)
        keys[i].second.iov_base = base[i];

    std::sort(keys.begin(), keys.end(), compare);
    int tempslotid = 0;
    for (int i = 0; i < keys.size(); i++) {
        block.setSlot(tempslotid, keys[i].first);
        tempslotid++;
    }

    for (int i = 0; i < block.getSlotsNum(); i++)
        delete[] base[i];
    
    delete[] base;
    delete[] iov_;
    return S_OK;
}

int Table::insert(struct iovec *record, size_t iovcnt)
{
    size_t offset = 0;
    int ret = 0;
    unsigned char header = 0;
    //读取root
    Root root;
    root.attach(rb);
    std::pair<Schema::TableSpace::iterator, bool> ret_ = gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;
    getinfo = ret_.first->second;
    DataType *dtype = findDataType(getinfo.fields[getinfo.key].datatype.c_str());

    unsigned char newbuffer[Block::BLOCK_SIZE];
    unsigned char prebuffer[Block::BLOCK_SIZE];

    unsigned int first = root.getHead();
    offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    db::Block block;
    block.attach(buffer_);
    db::Block new_block;
    new_block.attach(newbuffer);
    db::Block previous_block;

    int next = block.getNextid();
    int current = block.blockid();
    struct iovec *iov_ = new struct iovec[iovcnt];

    int previous_id = 0;
    
    //找对应范围的block插入record
    while (block.blockid() == root.getHead() || block.getNextid() != 0) {
        if (block.getSlotsNum() <= 0) {
            ret = block.allocate(&header, record, (int) iovcnt);
            offset = Root::ROOT_SIZE + (block.blockid() - 1) * Block::BLOCK_SIZE;
            break;
        }
        unsigned short reoffset = block.getSlot(block.getSlotsNum() - 1); //溢出问题
        unsigned char recordbuffer[Block::BLOCK_SIZE];
        getRecord(iov_, reoffset, iovcnt, recordbuffer);
        if (!dtype->compare(
                iov_[getinfo.key].iov_base,
                record[getinfo.key].iov_base,
                iov_[getinfo.key].iov_len,
                record[getinfo.key].iov_len)) { //不比当前block最大的大,<=max

            ret = block.allocate(&header, record, (int) iovcnt); //能插入当前block直接插入，然后截止
            if (ret) {
                offset = Root::ROOT_SIZE + (block.blockid() - 1) * Block::BLOCK_SIZE;
                break;
            }
            reoffset = block.getSlot(0);
            getRecord(iov_, reoffset, iovcnt, recordbuffer);

            if (previous_id == 0) break;
            size_t previous_offset =
                Root::ROOT_SIZE + (previous_id - 1) * Block::BLOCK_SIZE;
            datafile_.read(
                previous_offset, (char *) prebuffer, Block::BLOCK_SIZE);
            previous_block.attach(prebuffer);

            next = block.getNextid();
            current = block.blockid();
            if (!dtype->compare(
                    iov_[getinfo.key].iov_base,
                    record[getinfo.key].iov_base,
                    iov_[getinfo.key].iov_len,
                    record[getinfo.key]
                        .iov_len)) { //比当前block最小的小，就找上一个判断 <=min

                ret = previous_block.allocate(
                    &header, record, (int) iovcnt); //上一个是否能插入
                if (ret) {
                    offset = Root::ROOT_SIZE +
                             (previous_block.blockid() - 1) * Block::BLOCK_SIZE;
                    memcpy(buffer_, prebuffer, Block::BLOCK_SIZE);
                    break;
                }
            } else { //比当前最小的大就分裂, >min
                     //或者比最小的小但是两个block都插不进去
                int garbage = root.getGarbage();
                previous_block.setNextid(root.getGarbage());
                previous_offset =
                    Root::ROOT_SIZE +
                    (previous_block.blockid() - 1) * Block::BLOCK_SIZE;
                datafile_.write(
                    previous_offset,
                    (const char *) prebuffer,
                    Block::BLOCK_SIZE);

                root.setGarbage(root.getGarbage() + 1);
                datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);

                new_block.clear(1, garbage); //初始化，写入下一个
                new_block.setNextid(current);
                ret = new_block.allocate(
                    &header, record, (int) iovcnt); //多插入了一个

                for (int i = 0;i < block.getSlotsNum();) { //把比记录小的全删了，再新分配block插入,找大于要插入的记录的
                    reoffset = block.getSlot(i);
                    getRecord(iov_, reoffset, iovcnt, recordbuffer);

                    if (dtype->compare(
                            iov_[getinfo.key].iov_base,
                            record[getinfo.key].iov_base,
                            iov_[getinfo.key].iov_len,
                            record[getinfo.key].iov_len)) {
                        ret = new_block.allocate(&header, iov_, (int) iovcnt);
                        remove(block.blockid(), i); //这里删了i就会前移
                    } else
                        break;
                }
                offset = (garbage - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
                memcpy(buffer_, newbuffer, Block::BLOCK_SIZE);
            }
        } else { //比当前最大的block大,就往下找
            if (ret || block.getNextid() == 0) { break; }
            previous_id = block.blockid();
            offset =
                Root::ROOT_SIZE + (block.getNextid() - 1) * Block::BLOCK_SIZE;
            datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
            block.attach(buffer_);
        }
    }
    if (!ret) {
        offset = Root::ROOT_SIZE + (block.blockid() - 1) * Block::BLOCK_SIZE;
        ret = block.allocate(&header, record, (int) iovcnt); //最后一个block
    }
    if (!ret) { //可能同时是第一个block或者最后一个block//分配一个新的block
        int flag = 0; // 0代表分裂，1代表不分裂
        block.attach(buffer_);
        new_block.attach(newbuffer);

        size_t reoffset = 0;
        unsigned char recordbuffer[Block::BLOCK_SIZE];
        int garbage = root.getGarbage();
        reoffset = block.getSlot(block.getSlotsNum() - 1);
        getRecord(iov_, reoffset, iovcnt, recordbuffer);

        if (!dtype->compare(
                iov_[getinfo.key].iov_base,
                record[getinfo.key].iov_base, //最后一个block分裂
                iov_[getinfo.key].iov_len,
                record[getinfo.key].iov_len)) {
            if (previous_id !=
                0) { //是最后一个但不是第一个,考虑是分裂还是插到上一个
                reoffset = block.getSlot(0);
                getRecord(iov_, reoffset, iovcnt, recordbuffer);

                size_t previous_offset =
                    Root::ROOT_SIZE + (previous_id - 1) * Block::BLOCK_SIZE;
                datafile_.read(
                    previous_offset, (char *) prebuffer, Block::BLOCK_SIZE);
                previous_block.attach(prebuffer);

                if (!dtype->compare(
                        iov_[getinfo.key].iov_base,
                        record[getinfo.key].iov_base,
                        iov_[getinfo.key].iov_len,
                        record[getinfo.key].iov_len)) { //比当前block最小的小，就找上一个判断
                                                        //<=min

                    ret = previous_block.allocate(
                        &header, record, (int) iovcnt); //上一个是否能插入
                    if (ret) {                          //不分裂
                        flag = 1;
                        offset =
                            Root::ROOT_SIZE +
                            (previous_block.blockid() - 1) * Block::BLOCK_SIZE;
                        memcpy(buffer_, prebuffer, Block::BLOCK_SIZE);
                    }
                }
                if (flag == 0) { //不能插入/在当前block中不在上一个 就分裂
                    root.setGarbage(root.getGarbage() + 1);
                    datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);

                    new_block.clear(1, garbage);
                    new_block.setNextid(block.blockid());
                    new_block.allocate(&header, record, (int) iovcnt);

                    size_t previous_offset =
                        Root::ROOT_SIZE + (previous_id - 1) * Block::BLOCK_SIZE;
                    datafile_.read(
                        previous_offset, (char *) prebuffer, Block::BLOCK_SIZE);
                    previous_block.attach(prebuffer);
                    previous_block.setNextid(garbage);
                    datafile_.write(
                        previous_offset,
                        (const char *) prebuffer,
                        Block::BLOCK_SIZE);
                }
            } else { //是第一个，也可能同时是第一个和最后一个，一定要分裂
                root.setHead(garbage);
                root.setGarbage(root.getGarbage() + 1);
                datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);

                new_block.clear(1, garbage);
                new_block.setNextid(block.blockid());
                new_block.allocate(&header, record, (int) iovcnt);
            }
            if (flag == 0)
                for (int i = 0; i < block.getSlotsNum();) {
                    //把比记录小的全删了，再新分配block插入,找大于要插入的记录的
                    reoffset = block.getSlot(i);
                    getRecord(iov_, reoffset, iovcnt, recordbuffer);
                    if (dtype->compare(
                            iov_[getinfo.key].iov_base,
                            record[getinfo.key].iov_base,
                            iov_[getinfo.key].iov_len,
                            record[getinfo.key].iov_len)) {
                        ret = new_block.allocate(&header, iov_, (int) iovcnt);
                        remove(block.blockid(), i);
                    } else
                        break;
                }
        }
        //不用分裂，在最后新加
        else {
            root.setGarbage(root.getGarbage() + 1);
            datafile_.write(0, (const char *) rb, Root::ROOT_SIZE);

            new_block.clear(1, garbage);
            new_block.allocate(&header, record, (int) iovcnt);
            new_block.setNextid(0);
            block.setNextid(garbage);
            offset =
                Root::ROOT_SIZE + (block.blockid() - 1) * Block::BLOCK_SIZE;
            datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
        }
        if (flag == 0) {
            offset = (garbage - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
            memcpy(buffer_, newbuffer, Block::BLOCK_SIZE);
        }
    }
    delete[] iov_;

    block.attach(buffer_);
    sortSlots(block, (int) iovcnt); //排序
    block.setChecksum();
    datafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);
    return S_OK;
}
int Table::getRecord(struct iovec *iov, size_t offset, size_t iovcnt, unsigned char *recordbuffer)
{
    memcpy(recordbuffer, buffer_ + offset, 2);
    size_t length = 0;
    Integer it;
    Record record_;
    bool tempret = it.decode((char *) recordbuffer, 2);
    length = it.get();
    unsigned char header = 0;
    memcpy(recordbuffer, buffer_ + offset, length);

    record_.attach(recordbuffer, unsigned short(length));
    record_.ref(iov, (int) iovcnt, &header);
    return S_OK;
}

int Table::update(int blockid, int slotid, struct iovec *record, size_t iovcnt)
{
    //定位到对应record
    size_t offset = Root::ROOT_SIZE + (blockid - 1) * Block::BLOCK_SIZE;
    datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    Block block;
    block.attach(buffer_);

    if (slotid > block.getSlotsNum() - 1) {
        std::cout << "num error" << std::endl;
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
        unsigned char header = 0;
        remove(blockid, slotid);
        insert(record, iovcnt);
    }
    return S_OK;
}
int Table::remove(int blockid, int slotid)
{
    //写到garbage，替换record记录长度前2B，形成链表，新删除的写入头部，替换原来头部record记录长度前2B
    datafile_.read(
        Root::ROOT_SIZE + (blockid - 1) * Block::BLOCK_SIZE,
        (char *) buffer_,
        Block::BLOCK_SIZE);

    Block block;
    block.attach(buffer_);
    if (slotid > block.getSlotsNum()) return -1;

    int n = block.getSlotsNum();
    unsigned short offset = block.getSlot(slotid);
    unsigned short temp = block.getGarbage();
    memcpy(buffer_ + offset + 2, &temp, sizeof(temp));
    block.setGarbage(slotid);

    //slots重新排序，向前挤压
    for (int i = slotid; i < block.getSlotsNum() - 1; i++) {
        block.setSlot(i, block.getSlot(i + 1));
    }
    block.setSlot(block.getSlotsNum() - 1, 0);
    block.setSlotsNum(n - 1);
    datafile_.write(
        Root::ROOT_SIZE + (blockid - 1) * Block::BLOCK_SIZE,
        (char *) buffer_,
        Block::BLOCK_SIZE);
    return S_OK;
}

std::pair<int, int> Table::findkey(struct iovec *key, int iovcnt)
{
    size_t offset = 0;
    size_t reoffset = 0;
    int ret = 0;
    unsigned char header = 1;
    int slotid_ = 0;

    //读取root
    Root root;
    root.attach(rb);
    unsigned int first = root.getHead();
    offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
    Block block;
    block.attach(buffer_);

    size_t length = 0;
    Integer it;
    Record record_;
    struct iovec *iov_ = new struct iovec[iovcnt];
    unsigned char recordbuffer[Block::BLOCK_SIZE] = {0};

    std::pair<Schema::TableSpace::iterator, bool> ret_ =
        gschema.lookup(tablename.c_str());
    db::RelationInfo getinfo;
    getinfo = ret_.first->second;
    DataType *dtype =
        findDataType(getinfo.fields[getinfo.key].datatype.c_str());

    //遍历block，取每个block最后一个key
    while (block.getNextid() != 0) {
        if (block.getSlotsNum() == 0) {
            offset =
                Root::ROOT_SIZE + (block.getNextid() - 1) * Block::BLOCK_SIZE;
            datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
            block.attach(buffer_);
            continue;
        }
        reoffset = block.getSlot(block.getSlotsNum() - 1);
        getRecord(iov_, reoffset, iovcnt, recordbuffer);

        if (dtype->equal(
                key[getinfo.key].iov_base,
                iov_[getinfo.key].iov_base,
                key[getinfo.key].iov_len,
                iov_[getinfo.key].iov_len)) { //和最大的相等

            std::pair<int, int> p(block.blockid(), block.getSlotsNum() - 1);
            return p;
        }
        if (dtype->compare(
                iov_[getinfo.key].iov_base,
                key[getinfo.key].iov_base,
                iov_[getinfo.key].iov_len,
                key[getinfo.key].iov_len)) { //如果比最大的大

            std::cout << "next: " << block.getNextid() << " " << std::endl;
            offset =
                Root::ROOT_SIZE + (block.getNextid() - 1) * Block::BLOCK_SIZE;
            datafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
            block.attach(buffer_);
            continue;
        }
        break;
    }
    int flag = 0;
    //遍历record
    int lower = 0, big = block.getSlotsNum() - 1;
    while (big >= lower) {
        reoffset = block.getSlot((lower + big) / 2);
        getRecord(iov_, reoffset, iovcnt, recordbuffer);

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
    return p;
}

} // namespace db
