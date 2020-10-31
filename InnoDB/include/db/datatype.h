////
// @file datatype.h
// @brief
// 定义数据类型
// https://www.w3school.com.cn/sql/sql_datatypes.asp
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_DATATYPE_H__
#define __DB_DATATYPE_H__

#include "./endian.h"

namespace db {

// sql数据类型
struct DataType
{
    using Compare = bool (*)(const void *, const void *, size_t, size_t);
    using Equal = bool (*)(const void *, const void *, size_t, size_t);

    using Copy = bool (*)(void *, const void *, size_t, size_t);

    const char *name; // 名字
    ptrdiff_t size;   // >0表示固定，<0表示最大大小
    Compare compare;  // 比较函数
    Equal equal; //相等判断
    Copy copy;        // 拷贝函数
};

// 根据数据类型名称数据类型，返回NULL表示失败
// CHAR VARCHAR TINYINT SMALLINT INT BIGINT
DataType *findDataType(const char *name);

} // namespace db

#endif // __DB_DATATYPE_H__
