////
// @file schema.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <stdlib.h>
#include <db/schema.h>
#include <db/block.h>
#include <db/endian.h>
#include <db/record.h>

namespace db {

const char *Schema::META_FILE = "meta.db";

Schema::Schema(const char *name)
    : name_(name)
{
    name = META_FILE;
    buffer_ = (unsigned char *) malloc(Block::BLOCK_SIZE);
}
Schema::~Schema() { free(buffer_); }

int Schema::open()
{
    // 打开文件
    int ret = metafile_.open(name_.c_str());
    if (ret) return S_OK;

    // 如果meta.db长度为0，则写一个block
    unsigned long long length;
    ret = metafile_.length(length);
    if (ret) return S_OK;
    if (length) {
        // 加载
        metafile_.read(0, (char *) buffer_, Root::ROOT_SIZE);
        // TODO: 检查root？
        // 获取第1个block
        Root root;
        root.attach(buffer_);
        unsigned int first = root.getHead();
        size_t offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        metafile_.read(offset, (char *) buffer_, Block::BLOCK_SIZE);
        // 加载tablespace_
        MetaBlock block;
        block.attach(buffer_);
        unsigned short count = block.getSlotsNum();
        for (unsigned short i = 0; i < count; ++i) {
            // 获取slot
            unsigned short slotoff = block.getSlot(i);
            RelationInfo info;
            // 得到记录
            Record record;
            unsigned char *rb = buffer_ + slotoff;
            record.attach(rb, Block::BLOCK_SIZE);
            // 先分配iovec
            size_t fields = record.fields();
            struct iovec *iov = (struct iovec *) malloc(sizeof(iovec) * fields);
            unsigned char header;
            // 从记录得到iovec
            record.ref(iov, (int) fields, &header);
            // 填充info
            std::string table;
            retrieveInfo(table, info, iov, (int) fields);
            // 插入tablespace
            tablespace_.insert(
                std::pair<std::string, RelationInfo>(table, info));
            free(iov);
        }
    } else {
        // 创建root
        Root root;
        unsigned char rb[Root::ROOT_SIZE];
        root.attach(rb);
        root.clear(BLOCK_TYPE_META);
        root.setHead(1);
        // 创建第1个block
        MetaBlock block;
        block.attach(buffer_);
        block.clear(1);
        // 写root和block
        metafile_.write(0, (const char *) rb, Root::ROOT_SIZE);
        metafile_.write(
            Root::ROOT_SIZE, (const char *) buffer_, Block::BLOCK_SIZE);
    }

    return S_OK;
}

int Schema::create(const char *table, RelationInfo &info)
{
    if ((size_t) info.count != info.fields.size()) return EINVAL;

    // 先将info转化iov
    int total = 7; // 未包括域的描述信息，需要保存5个字段
    total += info.count * 4; // 未包括数据类型指针
    struct iovec *iov = (struct iovec *) calloc(total, sizeof(struct iovec));

    // 初始化iov
    initIov(table, info, iov);

    // 在表空间中添加
    std::string t(table);
    std::pair<TableSpace::iterator, bool> pret =
        tablespace_.insert(std::pair<std::string, RelationInfo>(t, info));
    if (!pret.second) return EEXIST;

    // 在当前meta块中分配
    MetaBlock meta;
    meta.attach(buffer_);
    unsigned char header = 0;
    bool ret = meta.allocate(&header, iov, total);
    if (!ret) {
        // TODO: 再分配一个block
    }
    // 不需要排序，因为有tablespace_
    // 处理checksum
    meta.setChecksum();
    // 写meta文件
    unsigned int blockid = meta.blockid() - 1;
    size_t offset = blockid * Block::BLOCK_SIZE + Root::ROOT_SIZE;
    metafile_.write(offset, (const char *) buffer_, Block::BLOCK_SIZE);

    free(iov);
    return S_OK;
}

std::pair<Schema::TableSpace::iterator, bool> Schema::lookup(const char *table)
{
    std::string t(table);
    TableSpace::iterator it = tablespace_.find(t);
    bool ret = it != tablespace_.end();
    return std::pair<TableSpace::iterator, bool>(it, ret);
}

int Schema::load(TableSpace::iterator it)
{
    return it->second.file.open(it->second.path.c_str());
}

void Schema::initIov(const char *table, RelationInfo &info, struct iovec *iov)
{
    iov[0].iov_base = (void *) table;
    iov[0].iov_len = strlen(table) + 1;
    iov[1].iov_base = (void *) info.path.c_str();
    iov[1].iov_len = info.path.size() + 1;
    unsigned short count = info.count;
    info.count = htobe16(info.count); // 底层保存为big endian
    iov[2].iov_base = &info.count;
    iov[2].iov_len = sizeof(unsigned short);
    info.type = htobe16(info.type);
    iov[3].iov_base = &info.type;
    iov[3].iov_len = sizeof(unsigned short);
    info.key = htobe32(info.key);
    iov[4].iov_base = &info.key;
    iov[4].iov_len = sizeof(unsigned int);
    iov[5].iov_base = &info.size; // 初始化为0，不需要转化为big endian
    iov[5].iov_len = sizeof(unsigned long long);
    iov[6].iov_base = &info.rows; // 初始化为0，不需要转化为big endian
    iov[6].iov_len = sizeof(unsigned long long);
    // 初始化field
    size_t index = 7;
    for (unsigned short i = 0; i < count; ++i) {
        iov[index].iov_base = (void *) info.fields[i].name.c_str();
        iov[index].iov_len = info.fields[i].name.size() + 1;
        ++index; // 假设是64bit机器
        info.fields[i].index = htobe64(info.fields[i].index);
        iov[index].iov_base = (void *) &info.fields[i].index;
        iov[index].iov_len = sizeof(unsigned long long);
        ++index;
        info.fields[i].length = htobe64(info.fields[i].length);
        iov[index].iov_base = (void *) &info.fields[i].length;
        iov[index].iov_len = sizeof(long long);
        ++index;
        iov[index].iov_base = (void *) info.fields[i].datatype.c_str();
        iov[index].iov_len = info.fields[i].datatype.size() + 1;
        ++index;
    }
}

void Schema::retrieveInfo(
    std::string &table,
    RelationInfo &info,
    struct iovec *iov,
    int iovcnt)
{
    table = (const char *) iov[0].iov_base;
    info.path = (const char *) iov[1].iov_base;
    ::memcpy(&info.count, iov[2].iov_base, sizeof(unsigned short));
    info.count = be16toh(info.count);
    ::memcpy(&info.type, iov[3].iov_base, sizeof(unsigned short));
    info.type = be16toh(info.type);
    ::memcpy(&info.key, iov[4].iov_base, sizeof(unsigned int));
    info.key = be32toh(info.key);
    ::memcpy(&info.size, iov[5].iov_base, sizeof(unsigned long long));
    info.size = be64toh(info.size);
    ::memcpy(&info.rows, iov[6].iov_base, sizeof(unsigned long long));
    info.rows = be64toh(info.rows);
    int count = (iovcnt - 7) / 4;
    info.fields.clear();
    for (int i = 0; i < count; ++i) {
        FieldInfo field;
        field.name = (const char *) iov[7 + i * 4].iov_base;
        ::memcpy(
            &field.index,
            iov[7 + i * 4 + 1].iov_base,
            sizeof(unsigned long long));
        field.index = be64toh(field.index);
        ::memcpy(&field.length, iov[7 + i * 4 + 2].iov_base, sizeof(long long));
        field.length = be64toh(field.length);
        field.datatype = (const char *) iov[7 + i * 4 + 3].iov_base;
        field.type = findDataType(field.name.c_str());
        info.fields.push_back(field);
    }
}

Schema gschema;

int dbInitialize() { return gschema.open(); }

} // namespace db