#include "mysqlapi.hpp"

int drop(MysqlApi& mysqlobj)
{
    std::string sql = "drop table if exists test_table";
    int ret = mysqlobj.execSQL(sql);
    if (ret == -1)
    {
        std::cout << "drop table error:" << mysqlobj.geterror() << std::endl;
        return -1;
    }
    std::cout << "drop success!" << std::endl;
}

int create(MysqlApi& mysqlobj)
{
    std::string sql = "create table test_table(id int, name varchar(30))";
    int ret = mysqlobj.execSQL(sql);
    if (ret == -1)
    {
        std::cout << "create table err:" << mysqlobj.geterror() << std::endl;
        return -1;
    }
    std::cout << "create table success!" << std::endl;
}

int insert(MysqlApi& mysqlobj)
{
    std::string sql = "insert into test_table(id ,name) values(?,?)";
    auto stmt = mysqlobj.getstmt(sql);
    if (!stmt)
    {
        std::cout << "insert table err:" << mysqlobj.geterror() << std::endl;
        return -1;
    }
    int ret = 0;
    for (int i = 0;i < 10;i++)
    {
        stmt.bindint(0,i);
        std::string name = "zhangyi" + std::to_string(i);
        stmt.bindstring(1,name);
        if (stmt.execute())
        {
            std::cout << "insert table err:" << mysqlobj.geterror() << std::endl;
            ret = -1;
        }
    }
    std::cout << "insert table success" << std::endl;
    return ret;
}


int query(MysqlApi& mysqlobj)
{
    std::string sql = "SELECT * FROM test_table";
    Result res = mysqlobj.query(sql);
    if (!res)
    {
        std::cout << "query failed,sql:" << sql << "err:" << mysqlobj.geterror() << std::endl;
        return -1;
    }
    while(Row row = res.next())
    {
        auto id = row.columnint(0);
        auto name = row.columntext(1);
        std::cout << "id:" << id << ", name:" << name.c_str() << std::endl;
    }
    return 0;
}

int main()
{
    std::string host = "9.134.239.95";
    auto &mysqlobj = MysqlApi::getinstance();
    if (!mysqlobj.connect(host.c_str(),"root","1234","test",3306,0,0))
    {
        std::cout << "can not connect to mysql:" << mysqlobj.geterror() << std::endl;
        return -1;
    }
    std::cout << "mysql connect success!" << std::endl;

    int res = query(mysqlobj);
    if (res)
    {
        std::cout << "mysql query failed,err:" << mysqlobj.geterror() << std::endl;
        return -1;
    }
    std::cout << "mysql query success!" << std::endl;

    drop(mysqlobj);
    query(mysqlobj);
    create(mysqlobj);
    insert(mysqlobj);
    query(mysqlobj);

    return 0;
}