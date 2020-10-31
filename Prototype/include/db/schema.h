////
// @file schema.h
// @brief
// 定义schema
// meta.db存放所有表的元信息，每张表用meta.db的一行来描述。一行是变长的，它包含：
// 1. 以表名做键；
// 2. 文件路径；
// 3. 域的个数，键的位置；
// 4. 各域的描述；（变长）
// 5. 各种统计信息，表的大小，行数等；
// meta.db的所有信息被读入一个map，以加快对元信息的访问。
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_SCHEMA_H__
#define __DB_SCHEMA_H__

#include <string>
#include <map>
#include <vector>
#include "./datatype.h"
#include "./file.h"

namespace db {

// 描述域
struct FieldInfo
{
    std::string name;         // 域名
    unsigned long long index; // 位置
    long long length;         // 长度，高位表示是否固定大小
    std::string datatype;              //

    DataType *type;           // 指向数据类型

    FieldInfo()
        : index(0)
        , length(0)
        , type(NULL)
    {}
    FieldInfo(const FieldInfo &other) 
      : name(other.name)
      , index(other.index)
      , length(other.length)
      , datatype(other.datatype)
      , type(other.type)

    {
      //type=findDataType(name.c_str);

    };
};
// 内存中描述关系
struct RelationInfo
{
    std::string path;              // 文件路径
    unsigned short count;          // 域的个数
    unsigned short type;           // 类型
    unsigned int key;              // 键的位置
    File file;                     // 文件
    unsigned long long size;       // 大小
    unsigned long long rows;       // 行数
    std::vector<FieldInfo> fields; // 各域的描述

    RelationInfo()
        : count(0)
        , type(0)
        , key(0)
        , size(0)
        , rows(0)
    {}
    RelationInfo(const RelationInfo &other)
        : path(other.path)
        , count(other.count)
        , type(other.type)
        , key(other.key)
        , size(other.size)
        , rows(other.rows)
        , fields(other.fields)
    {}
};

////
// @brief
// Schema描述表空间
//
class Schema
{
  public:
    using TableSpace = std::map<std::string, RelationInfo>;

  public:
    static const char *META_FILE; // "meta.db";

  private:
    std::string name_;      // 源文件名
    File metafile_;         // 元文件
    TableSpace tablespace_; // 表空间
    unsigned char *buffer_; // block，TODO: 缓冲模块

  public:
    Schema(const char *name = META_FILE);
    ~Schema();

    int open(); // 打开元文件并加载
    int create(const char *table, RelationInfo &rel); // 新建一张表
    std::pair<TableSpace::iterator, bool> lookup(const char *table); // 查找表
    int load(TableSpace::iterator it); // 加载表

    // 删除元文件
    inline int destroy()
    {
        metafile_.close();
        return metafile_.remove(META_FILE);
    }

  private:
    void initIov(const char *table, RelationInfo &rel, struct iovec *iov);
    void retrieveInfo(
        std::string &table,
        RelationInfo &rel,
        struct iovec *iov,
        int iovcnt);
};

// 全局唯一schema
extern Schema gschema;
// 系统初始化
int dbInitialize();

} // namespace db

#endif // __DB_SCHEMA_H__
