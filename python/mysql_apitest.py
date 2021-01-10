import pymysql
import api_mysql

if __name__=='__main__':
    config = {
        'host':'9.134.239.95',
        'port':3306,
        'user':'root',
        'passwd':'1234',
        'charset':'utf8',
        'cursorclass':pymysql.cursors.DictCursor
    }
    mydb = api_mysql.MysqlApi(config)
    print(mydb.getVersion())

    db_name = 'test_db'
    mydb.createDatabase(db_name)

    # 选择数据库
    mydb.selectDatabase(db_name)

    # 创建数据表
    table_name = 'test_user'
    attr_dict = {'name':'varchar(30) not null'}
    constraint = 'primary key(id)'
    mydb.createTable(table_name,attr_dict,constraint)

    # 插入数据
    params = {}
    for i in range(5):
        params.update({"name":"testuser"+str(i)})
        mydb.insert(table_name,params)

    # 批量插入数据
    insert_value = []
    for i in range(5):
        insert_value.append([u"测试用户"+str(i)])
    insert_attr = ["name"]
    mydb.insertMany(table_name,insert_attr,insert_value)

    # 数据查询
    print(mydb.select("select * from %s" % table_name))
    print(mydb.select("select * from %s order by id desc" % table_name))

    # 删除数据
    delete_params = {"name":"测试用户2"}
    mydb.delete(table_name,delete_params)

    # 更新数据
    update_params = {"name":"测试用户99"} # 需要更新为什么值
    update_cond_dict = {"name":"测试用户3"} # 查询条件
    mydb.update(table_name,update_params,update_cond_dict)

    # 删除表
    mydb.dropTable(table_name)


