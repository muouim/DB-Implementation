////
// @file timestampTest.cc
// @brief
// 时戳单元测试
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/timestamp.h>
using namespace db;

TEST_CASE("db/timestamp.h")
{
    SECTION("now")
    {
        TimeStamp ts;
        ts.now();
        REQUIRE(sizeof(ts.stamp_) == 8);
    }
}