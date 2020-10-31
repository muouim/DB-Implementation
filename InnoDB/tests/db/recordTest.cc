////
// @file recordTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/record.h>
using namespace db;

TEST_CASE("db/record.h")
{
    SECTION("size")
    {
        struct iovec iov[4];

        const char *table = "table.db";

        int type = 1;
        const char *hello = "hello, worl";
        size_t length = strlen(hello) + 1;

        iov[0].iov_base = (void *) table;
        iov[0].iov_len = strlen(table) + 1;
        iov[1].iov_base = &type;
        iov[1].iov_len = sizeof(int);
        iov[2].iov_base = (void *) hello;
        iov[2].iov_len = strlen(hello) + 1;
        iov[3].iov_base = &length;
        iov[3].iov_len = sizeof(size_t);

        std::pair<size_t, size_t> ret = Record::size(iov, 4);
        REQUIRE(ret.first == 39);
        REQUIRE(ret.second == 5);

        unsigned char buffer[80];
        Record record;
        record.attach(buffer, 80);
        unsigned char header = 0x48;
        size_t sret = record.set(iov, 4, &header);
        REQUIRE(sret == 40);

        REQUIRE(buffer[0] == 39);
        REQUIRE(buffer[4] == 1);                // 第1个field偏移量
        REQUIRE(buffer[3] == 10);               // 第2个field偏移量
        REQUIRE(strlen(table) + 1 == 10 - 1);   // 第1个field的大小
        REQUIRE(buffer[2] == 14);               // 第3个field偏移量
        REQUIRE(14 - 10 == sizeof(int));        // 第2个field的大小
        REQUIRE(buffer[1] == 26);               // 第4个field偏移量
        REQUIRE(26 - 14 == strlen(hello) + 1);  // 第3个field的大小
        REQUIRE(39 - 5 - 26 == sizeof(size_t)); // 第4个field的大小

        REQUIRE(record.length() == 39);
        REQUIRE(record.fields() == 4);

        struct iovec iov2[4];
        char b1[16];
        iov2[0].iov_base = (void *) b1;
        iov2[0].iov_len = 16;
        int type2;
        iov2[1].iov_base = &type2;
        iov2[1].iov_len = sizeof(int);
        char b2[16];
        iov2[2].iov_base = (void *) b2;
        iov2[2].iov_len = 16;
        size_t length2;
        iov2[3].iov_base = &length2;
        iov2[3].iov_len = sizeof(size_t);
        unsigned char header2;
        bool bret = record.get(iov2, 4, &header2);
        REQUIRE(bret);
        REQUIRE(strncmp(b1, table, strlen(table)) == 0);
        REQUIRE(iov2[0].iov_len == strlen(table) + 1);
        REQUIRE(type2 == type);
        REQUIRE(iov2[1].iov_len == sizeof(int));
        REQUIRE(strncmp(b2, hello, strlen(hello)) == 0);
        REQUIRE(iov2[2].iov_len == strlen(hello) + 1);
        REQUIRE(length == length2);
        REQUIRE(iov2[3].iov_len == sizeof(size_t));
    }
}