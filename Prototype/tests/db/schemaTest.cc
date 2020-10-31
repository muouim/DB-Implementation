////
// @file schemaTest.cc
// @brief
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "../catch.hpp"
#include <db/schema.h>
using namespace db;

TEST_CASE("db/schema.h")
{
    SECTION("open")
    {
        Schema schema;
        int ret = schema.open();
        REQUIRE(ret == S_OK);

        // 填充关系
        RelationInfo relation;
        relation.path = "table.dat";

        // id char(20) varchar
        FieldInfo field;
        field.name = "id";
        field.index = 0;
        field.length = 8;
        relation.fields.push_back(field);

        field.name = "phone";
        field.index = 1;
        field.length = 20;
        relation.fields.push_back(field);

        field.name = "name";
        field.index = 2;
        field.length = -255;
        relation.fields.push_back(field);

        relation.count = 3;
        relation.key = 0;

        ret = schema.create("table", relation);
        REQUIRE(ret == S_OK);

    }

    SECTION("load")
    {
        Schema schema;
        int ret = schema.open();
        REQUIRE(ret == S_OK);
        std::pair<Schema::TableSpace::iterator, bool> bret =
            schema.lookup("table");
        REQUIRE(bret.second);

        ret = schema.load(bret.first);
        REQUIRE(ret == S_OK);

        // 删除表，删除元文件
        Schema::TableSpace::iterator it = bret.first;
        it->second.file.close();
        REQUIRE(it->second.file.remove("table.dat") == S_OK);
        REQUIRE(schema.destroy() == S_OK);

        File datafile_;
        datafile_.open("test");
        db::RelationInfo info;
            info.path="test";
            info.file=datafile_;
            info.size=0;
	    ret = gschema.open();
        gschema.create("test",info);

        REQUIRE(ret == S_OK);
        std::pair<Schema::TableSpace::iterator, bool>  bret_ =gschema.lookup("test");
        REQUIRE(bret_.second);

        ret = gschema.load(bret_.first);
        REQUIRE(ret == S_OK);

        // 删除表，删除元文件
        it = bret_.first;
        it->second.file.close();
        REQUIRE(it->second.file.remove("test") == S_OK);
        REQUIRE(gschema.destroy() == S_OK);
    }
}