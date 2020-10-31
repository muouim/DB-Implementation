////
// @file tableTest.cc
// @brief
// 测试存储管理
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/table.h>
#include <time.h>
#include <windows.h>
#include <string.h>
#include <iostream>

using namespace db;

int gettimeofday(struct timeval *tp, void *tzp)
{
    time_t clock;
    struct tm tm;
    SYSTEMTIME wtm;
    GetLocalTime(&wtm);
    tm.tm_year = wtm.wYear - 1900;
    tm.tm_mon = wtm.wMonth - 1;
    tm.tm_mday = wtm.wDay;
    tm.tm_hour = wtm.wHour;
    tm.tm_min = wtm.wMinute;
    tm.tm_sec = wtm.wSecond;
    tm.tm_isdst = -1;
    clock = mktime(&tm);
    tp->tv_sec = (long) clock;
    tp->tv_usec = wtm.wMilliseconds * 1000;
    return (0);
}
TEST_CASE("db/table.h")
{
    SECTION("open")
    {
        db::RelationInfo info;
        Table table;
        db::FieldInfo field;
        REQUIRE(dbInitialize() == S_OK);

        field.name = "name";
        field.index = 0;
        field.length = -255;
        field.datatype = "VARCHAR";
        field.type = findDataType("VARCHAR");
        info.fields.push_back(field);

        field.name = "id";
        field.index = 1;
        field.length = sizeof(int);
        field.datatype = "INT";
        field.type = findDataType("INT");
        info.fields.push_back(field);

        field.name = "grade";
        field.index = 2;
        field.length = sizeof(int);
        field.datatype = "INT";
        field.type = findDataType("INT");
        info.fields.push_back(field);

        info.path = "test.dat";
        info.count = 3;
        info.size = 30;
        info.key = 1;
        REQUIRE(table.create("test.dat", info) == S_OK);
    }
    SECTION("iterator")
    {
        Table table;
        File file;
        unsigned char rb[Root::ROOT_SIZE];
        db::RelationInfo info;
        table.open("test.dat");
        table.datafile_.read(0, (char *) rb, Root::ROOT_SIZE);
        Root root;
        root.attach(rb);
        Record record;
        int i = 0;
        for (db::Table::iterator it = table.begin(); it != table.end(); ++it) {
            record = *it;
            iovec iov[3];
            unsigned char header = 0;
            record.ref(iov, 3, &header);
            std::cout << *(int *) iov[1].iov_base << std::endl;
            REQUIRE(*(int *) iov[1].iov_base == ++i);

            std::cout << "block " << it.getblockid() << " " << it.getslotid()
                      << " " << record.length() << " "
                      << (char *) iov[0].iov_base << std::endl;
        }
    }
    SECTION("insert")
    {
        struct timeval start, end;
        gettimeofday(&start, NULL);
        Table table;
        unsigned char rb[Root::ROOT_SIZE];
        db::RelationInfo info;
        table.open("test.dat");
        table.datafile_.open("test.dat");
        table.datafile_.read(0, (char *) rb, Root::ROOT_SIZE);
        Root root;
        root.attach(rb);

        bool vis[1005];
        srand((unsigned int) time(0));
        memset(vis, false, sizeof(vis));

        for (int i = 1; i <= 1000; i++) {
            std::cout << i << std::endl;
            char temp1[40] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
            iovec record[3];
            record[0].iov_base = temp1;
            record[0].iov_len = strlen(temp1);
            int a[1024];
            a[0] = rand() % 1000 + 1;
            while (vis[a[0]])
                a[0] = rand() % 1000 + 1;
            vis[a[0]] = true;

            record[1].iov_base = (int *) &a[0];
            record[1].iov_len = sizeof(int);
            a[1] = 90;
            record[2].iov_base = (int *) &a[1];
            record[2].iov_len = sizeof(int);
            unsigned char header = 0;
            REQUIRE(table.insert(record, 3) == S_OK);
        }

        gettimeofday(&end, NULL);
        double time_taken = (end.tv_sec - start.tv_sec) * 1e6;
        time_taken = (time_taken + (end.tv_usec - start.tv_usec)) * 1e-6;
        printf("Insert time is %lf s\n", time_taken);

        unsigned char buffer[Block::BLOCK_SIZE];
        unsigned int first = root.getHead();
        size_t offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        table.datafile_.read(offset, (char *) buffer, Block::BLOCK_SIZE);
        Block block;
        block.attach(buffer);

        std::pair<Schema::TableSpace::iterator, bool> ret =
            gschema.lookup("test.dat");
        db::RelationInfo getinfo;
        getinfo = ret.first->second;
        REQUIRE(getinfo.key == 1);
        REQUIRE(getinfo.fields[0].name == "name");

    }

    SECTION("remove")
    {
        Table table;
        table.open("test.dat");

        Root root;
        unsigned char rb[Root::ROOT_SIZE];
        table.datafile_.read(0, (char *) rb, Root::ROOT_SIZE);
        root.attach(rb);

        for (int i = 500; i <= 600; i++) {
            std::cout << i << std::endl;
            char temp1[40] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
            iovec record[3];
            record[0].iov_base = temp1;
            record[0].iov_len = strlen(temp1);
            int a[1024];
            a[0] = i;

            record[1].iov_base = (int *) &a[0];
            record[1].iov_len = sizeof(int);
            a[1] = 90;
            record[2].iov_base = (int *) &a[1];
            record[2].iov_len = sizeof(int);
            unsigned char header = 0;
            //载入record
            std::pair<int, int> p = table.findkey(record, 3);
            REQUIRE(table.remove(p.first, p.second) == S_OK);
        }
    }
    SECTION("update")
    {
        //载入record
        Table table;
        unsigned char rb[Root::ROOT_SIZE];
        db::RelationInfo info;
        table.open("test.dat");
        table.datafile_.open("test.dat");
        table.datafile_.read(0, (char *) rb, Root::ROOT_SIZE);
        Root root;
        root.attach(rb);

        for (int i = 200; i <= 300; i++) {
            std::cout << i << std::endl;
            char temp1[40] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
            iovec record[3];
            record[0].iov_base = temp1;
            record[0].iov_len = strlen(temp1);
            int a[1024];
            a[0] = i;

            record[1].iov_base = (int *) &a[0];
            record[1].iov_len = sizeof(int);
            a[1] = 90;
            record[2].iov_base = (int *) &a[1];
            record[2].iov_len = sizeof(int);
            unsigned char header = 0;
            //载入record
            std::pair<int, int> p = table.findkey(record, 3);
            REQUIRE(table.update(p.first, p.second, record, 3) == S_OK);
        }
    }
    SECTION("find")
    {
        std::string temp = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        iovec record[3];
        record[0].iov_base = (char *) temp.c_str();
        record[0].iov_len = temp.length();
        int a[1024];
        a[0] = 980;
        record[1].iov_base = (int *) &a[0];
        record[1].iov_len = sizeof(int);
        a[1] = 70;
        record[2].iov_base = (int *) &a[1];
        record[2].iov_len = sizeof(int);
        //载入record
        Table table;
        unsigned char rb[Root::ROOT_SIZE];
        db::RelationInfo info;
        table.open("test.dat");
        table.datafile_.open("test.dat");
        table.datafile_.read(0, (char *) rb, Root::ROOT_SIZE);
        Root root;
        root.attach(rb);
        std::pair<int, int> p = table.findkey(record, 3);
        std::cout << "block " << p.first << " " << p.second << std::endl;
        ;
    }
}
