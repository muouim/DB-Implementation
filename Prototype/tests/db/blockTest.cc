////
// @file blockTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/block.h>
#include <db/record.h>
using namespace db;

TEST_CASE("db/block.h")
{
    SECTION("clear")
    {
        Block block;
        unsigned char buffer[Block::BLOCK_SIZE];
        block.attach(buffer);
        block.clear(1, 2);

        // magic number：0xc1c6f01e
        REQUIRE(buffer[0] == 0xc1);
        REQUIRE(buffer[1] == 0xc6);
        REQUIRE(buffer[2] == 0xf0);
        REQUIRE(buffer[3] == 0x1e);

        int spaceid = block.spaceid();
        REQUIRE(spaceid == 1);

        int blockid = block.blockid();
        REQUIRE(blockid == 2);

        int nextid = block.getNextid();
        REQUIRE(nextid == 0);

        short garbage = block.getGarbage();
        REQUIRE(garbage == 0);

        short freespace = block.getFreespace();
        REQUIRE(freespace == Block::BLOCK_DEFAULT_FREESPACE);

        // block.setChecksum();
        unsigned int check = block.getChecksum();
        REQUIRE(check == Block::BLOCK_DEFAULT_CHECKSUM);
        // printf("check=%x\n", check);
        bool bcheck = block.checksum();
        REQUIRE(bcheck);

        unsigned short count = block.getSlotsNum();
        REQUIRE(count == 0);

        unsigned short f1 = block.getFreeLength();
        REQUIRE(
            f1 == Block::BLOCK_SIZE - Block::BLOCK_DEFAULT_FREESPACE -
                      Block::BLOCK_CHECKSUM_SIZE);

        unsigned short type = block.getType();
        REQUIRE(type == BLOCK_TYPE_DATA);
    }

    SECTION("root")
    {
        Root root;
        unsigned char buffer[Block::BLOCK_SIZE];
        root.attach(buffer);
        root.clear(BLOCK_TYPE_META);

        unsigned short type = root.getType();
        REQUIRE(type == BLOCK_TYPE_META);

        TimeStamp ts = root.getTimeStamp();
        char tb[64];
        REQUIRE(ts.toString(tb, 64));
        // printf("ts=%s\n", tb);

        REQUIRE(root.checksum());

        unsigned int garbage = root.getGarbage();
        REQUIRE(garbage == 0);
        unsigned int head = root.getHead();
        REQUIRE(head == 0);
    }

    SECTION("meta")
    {
        MetaBlock meta;
        unsigned char buffer[Block::BLOCK_SIZE];
        meta.attach(buffer);
        meta.clear(2);

        // magic number：0xc1c6f01e
        REQUIRE(buffer[0] == 0xc1);
        REQUIRE(buffer[1] == 0xc6);
        REQUIRE(buffer[2] == 0xf0);
        REQUIRE(buffer[3] == 0x1e);

        unsigned int spaceid = meta.spaceid();
        REQUIRE(spaceid == 0xffffffff);

        int blockid = meta.blockid();
        REQUIRE(blockid == 2);

        int nextid = meta.getNextid();
        REQUIRE(nextid == 0);

        short garbage = meta.getGarbage();
        REQUIRE(garbage == 0);

        short freespace = meta.getFreespace();
        REQUIRE(freespace == MetaBlock::META_DEFAULT_FREESPACE);

        bool bcheck = meta.checksum();
        REQUIRE(bcheck);

        unsigned short count = meta.getSlotsNum();
        REQUIRE(count == 0);

        unsigned short type = meta.getType();
        REQUIRE(type == BLOCK_TYPE_META);
    }

    SECTION("allocate")
    {
        Block block;
        unsigned char buffer[Block::BLOCK_SIZE];
        block.attach(buffer);
        block.clear(1, 2);

        struct iovec iov[3];
        const char *hello = "hello";
        iov[0].iov_base = (void *) hello;
        iov[0].iov_len = strlen(hello) + 1;
        int x = 3;
        iov[1].iov_base = &x;
        iov[1].iov_len = sizeof(int);
        const char *world = "world count xxx";
        iov[2].iov_base = (void *) world;
        iov[2].iov_len = strlen(world) + 1;

        unsigned char header = 0x84;

        unsigned short f1 = block.getFreespace();
        std::pair<size_t, size_t> ret = Record::size(iov, 3);

        REQUIRE(block.allocate(&header, iov, 3));
        unsigned short spos =
            Block::BLOCK_SIZE - Block::BLOCK_CHECKSUM_SIZE - 2;
        unsigned short f2 = *((unsigned short *) (buffer + spos));
        REQUIRE(f1 == f2);

        f2 = block.getSlotsNum();
        REQUIRE(f2 == 1);

        f1 = block.getFreespace();
        f2 = f1 - Block::BLOCK_FREESPACE_OFFSET - Block::BLOCK_FREESPACE_SIZE;
        REQUIRE(f2 >= (unsigned short) ret.first);
        REQUIRE(f2 % 8 == 0);
    }
}