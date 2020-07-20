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
using namespace db;

TEST_CASE("db/table.h")
{
    SECTION("open") {
        db::RelationInfo info;
        Table table;
        db::FieldInfo field;
        REQUIRE(dbInitialize()==S_OK); 

        field.name="name";
        field.index=0;
        field.length=-255;
        field.datatype="VARCHAR";
        field.type=findDataType("VARCHAR");
        info.fields.push_back(field);
        
        field.name="id";
        field.index=1;
        field.length=sizeof(int);
        field.datatype="INT";
        field.type=findDataType("INT");
        info.fields.push_back(field);

        field.name="grade";
        field.index=2;
        field.length=sizeof(int);
        field.datatype="INT";
        field.type=findDataType("INT");
        info.fields.push_back(field);

        info.path="test.dat";
        info.count=3;
        info.size=30;
        info.key=0;
        REQUIRE(table.create("test.dat",info)==S_OK); 
        /*std::pair<Schema::TableSpace::iterator, bool> ret = gschema.lookup("test.dat");
        db::RelationInfo getinfo;
        getinfo=ret.first->second;
        REQUIRE(getinfo.fields[0].name=="name");
        REQUIRE(getinfo.key==1); */
    }
    SECTION("iterator") {
        Table table;
        File file;
        unsigned char rb[Root::ROOT_SIZE];
        db::RelationInfo info;
	    table.open("test.dat");
        table.datafile_.read(0, (char *)rb, Root::ROOT_SIZE);
        Root root;
        root.attach(rb);
        Record record;
        int i=0;
        for(db::Table::iterator it=table.begin(1);it!=table.end(1);++it){
            REQUIRE(it.getblockid()==1);
            record=*it;
            iovec iov[3];
            unsigned char header=0;
            record.ref(iov, 3 , &header)  ;         
            std::cout<<(char *)iov[0].iov_base<<std::endl;
            REQUIRE(it.getslotid()==i);i++;
        }
    }
    SECTION("insert") {
        char temp1[9]="bbbbbbbb";
        iovec record[3];
		record[0].iov_base =temp1;
		record[0].iov_len =9;
        int a[1024];
        a[0]=2018;
        record[1].iov_base =(int *)&a[0];
        record[1].iov_len = sizeof(int);
        a[1]=90;
        record[2].iov_base =(int *)&a[1];
        record[2].iov_len = sizeof(int);
        unsigned char header=0;    
        //载入record
        Table table;
        unsigned char rb[Root::ROOT_SIZE];
        db::RelationInfo info;
	    table.open("test.dat");
        table.datafile_.open("test.dat");
        table.datafile_.read(0, (char *)rb, Root::ROOT_SIZE);
        Root root;
        root.attach(rb);
        table.insert(record,3);
            
        char temp2[9]="aaaaaaaa";
        record[0].iov_base =temp2;
		record[0].iov_len =9;
        
        a[2]=2019;
        record[1].iov_base =(int *)&a[2];
        record[1].iov_len =sizeof(int);
        
        a[3]=80;
        record[2].iov_base =(int *)&a[3];
        record[2].iov_len = sizeof(int);
        table.insert(record,3);

        char temp3[18]="cccccccc";
        record[0].iov_base =temp3;
		record[0].iov_len =9;
        a[4]=2020;
        record[1].iov_base = (int *)&a[4];
        record[1].iov_len = sizeof(int);
        a[5]=70;
        record[2].iov_base =(int *)&a[5];
        record[2].iov_len = sizeof(int);
        table.insert(record,3);
        unsigned char buffer[Block::BLOCK_SIZE];
        unsigned int first = root.getHead();
        size_t offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        table.datafile_.read(offset, (char *) buffer, Block::BLOCK_SIZE);
        Block block;
        block.attach(buffer);
        
        
        std::pair<Schema::TableSpace::iterator, bool> ret = gschema.lookup("test.dat");
        db::RelationInfo getinfo;
        getinfo=ret.first->second;
        REQUIRE(getinfo.key==1);
        REQUIRE(getinfo.fields[0].name=="name");
        REQUIRE(getinfo.fields[0].datatype=="name");

        REQUIRE(block.getSlotsNum()==1);
    }

    SECTION("remove") {
        Table table;
        table.open("test.dat");

        Root root;
        unsigned char rb[Root::ROOT_SIZE];
        table.datafile_.read(0, (char *)rb, Root::ROOT_SIZE);
        root.attach(rb);

        unsigned char buffer[Block::BLOCK_SIZE];
        unsigned int first = root.getHead();
        REQUIRE(first==1);
        size_t offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        table.datafile_.read(offset, (char *) buffer, Block::BLOCK_SIZE);
    
        REQUIRE(table.remove(1,1)==S_OK);
        REQUIRE(table.remove(1,5)==S_OK);
        
        table.datafile_.read(offset, (char *) buffer, Block::BLOCK_SIZE);
        Block block;
        block.attach(buffer);
        REQUIRE(block.getGarbage()==5);
        REQUIRE(block.getSlotsNum()==1);
    }
    SECTION("update") {

        std::string temp="xiaobbbbbbbbbbbbb";
        iovec record[3];
		record[0].iov_base =(char *)temp.c_str();
		record[0].iov_len =temp.length();
        int a[3];
        a[0]=2020;
        record[1].iov_base = (int *)a;
        record[1].iov_len = sizeof(int);
        a[1]=70;
        record[2].iov_base =(int *)a;
        record[2].iov_len =sizeof(int);  
        unsigned char header=0;    
        //载入record
        Table table;
        unsigned char rb[Root::ROOT_SIZE];
        db::RelationInfo info;
	    table.open("test.dat");
        table.datafile_.open("test.dat");
        table.datafile_.read(0, (char *)rb, Root::ROOT_SIZE);
        Root root;
        root.attach(rb);
        table.update(1,2,record,3);

        unsigned char buffer[Block::BLOCK_SIZE];
        unsigned int first = root.getHead();
        size_t offset = (first - 1) * Block::BLOCK_SIZE + Root::ROOT_SIZE;
        table.datafile_.read(offset, (char *) buffer, Block::BLOCK_SIZE);
        Block block;
        block.attach(buffer);
        REQUIRE(block.getGarbage()==6);
    }  
    SECTION("find") {

        std::string temp="xiaobbbbbbbbbbbbb";
        iovec record[3];
		record[0].iov_base =(char *)temp.c_str();
		record[0].iov_len =temp.length();
        int a[1024];
        a[0]=2020;
        record[1].iov_base =(int *)a;
        record[1].iov_len = sizeof(int);
        a[1]=70;
        record[2].iov_base =(int *)&a[1];
        record[2].iov_len = sizeof(int);
        //载入record
        Table table;
        unsigned char rb[Root::ROOT_SIZE];
        db::RelationInfo info;
	    table.open("test.dat");
        table.datafile_.open("test.dat");
        table.datafile_.read(0, (char *)rb, Root::ROOT_SIZE);
        Root root;
        root.attach(rb);
        std::pair<int,int>p=table.findkey(record,3);
        REQUIRE(p.first==1);
        REQUIRE(p.second==0);
    }  
    /*struct Compare
    {
        FieldInfo *fin;

        Compare(FieldInfo *f)
            : fin(f)
        {}
        bool operator<(unsigned short x, unsigned short y) const
        {
            // 根据x,y偏移量，引用两条记录；
            const Record *rx = ...;
            const Record *ry = ...;
            size_t lx = ;
            size_t ly = ;
            fin->compare(rx, ry, lx, ly);
            return fasle;
        }
    };*/
}