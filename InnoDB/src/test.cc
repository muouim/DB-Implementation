#include <vector>
#include <string>
#include <fstream>
#include <db/block.h>
#include <db/table.h>
#include <iostream>

using namespace std;
//插入数据默认按照主键升序
/*std::vector<RowData> query(string sql); //查询数据默认按照主键升序
void del(string sql);
void update(string sql);*/

/*查询sql为select * from table where + 条件语句
。条件语句只需考虑id的大小关系及or和and运算符 比如：select * from table where id
> 1556 and id <5000 删除数据sql类似：delete from table where +
条件语句。条件语句要求与查询sql一致。 delete from table where id > 300 or id >
784 更新数据sql类似：update table set 《字段名》 = ？ where + 条件语句。
条件语句要求与查询sql一致。 比如：update table set number = 20157894 where
id<900 or id>9950*/
struct RowData
{
    int id;
    string name;
    string number;
    bool sex;
    string email;
};
void initial(); //初始化操作，创建数据库文件等
void insert(std::vector<RowData> rows); //插入数据,将rows数据插入到数据库中
std::vector<RowData>
query(string sql); //查询数据,将满足要求的数据作为返回值返回。
void del(string sql);    //删除数据
void update(string sql); //更新数据*/

int main(int argc, char *argv[])
{
    std::vector<RowData> rows;
    RowData row, row1;
    row = {1, "test1", "test1", 0, "123@qq.com"};
    row1 = {1, "test2", "test2", 0, "321@qq.com"};

    rows.push_back(row);
    rows.push_back(row1);
    insert(rows);
    return 1;
}

void initial()
{
    db::Table table;

    table.open("test.dat");
}

void insert(std::vector<RowData> rows)
{
    db::Table table;
    db::RelationInfo info;
    db::File test;
    iovec record;
    table.create("test.dat", info);
    table.open("test.dat");

    for (int i = 0; i < (int) rows.size() - 1; i++) {
        std::string temp = rows[i].name + " " + rows[i].email + "\n";
        record.iov_base = (char *) temp.c_str();
        record.iov_len = temp.size();
        table.insert(&record, 1);
    }
}

std::vector<RowData> query(string sql)
{
    vector<RowData> row;
    return row;
}
void del(string sql)
{
    db::Table table;

    table.open("test.dat");
    table.remove(1, 1);
}
void update(string sql) {}