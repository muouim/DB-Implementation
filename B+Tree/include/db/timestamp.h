////
// @file timestamp.h
// @brief
// 时辍
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#ifndef __DB_TIMESTAMP_H__
#define __DB_TIMESTAMP_H__

#include <chrono>
#include <utility>

namespace db {

// 时辍
struct TimeStamp
{
    std::chrono::system_clock::time_point stamp_;
    void now() { stamp_ = std::chrono::system_clock::now(); }
    bool toString(char *buffer, size_t size) const;
    void fromString(const char *time);
};

bool operator<(const TimeStamp &lhs, const TimeStamp &rhs);
bool operator>(const TimeStamp &lhs, const TimeStamp &rhs);
bool operator<=(const TimeStamp &lhs, const TimeStamp &rhs);
bool operator>=(const TimeStamp &lhs, const TimeStamp &rhs);
bool operator==(const TimeStamp &lhs, const TimeStamp &rhs);
bool operator!=(const TimeStamp &lhs, const TimeStamp &rhs);

} // namespace db

#endif // __DB_TIMESTAMP_H__
