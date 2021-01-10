import pymysql

class MysqlApi(object):
    def __init__(self,config):
        self.host = config['host']
        self.username = config['user']
        self.password = config['passwd']
        self.port = config['port']
        self.con = None
        self.cur = None

        try:
            self.con = pymysql.connect(**config)
            self.con.autocommit(1)
            self.cur = self.con.cursor()
        except Exception as e:
            print("mysql connect failed,err:",e)

    def close(self):
        if not self.con:
            self.con.close()
        else:
            print("mysql doesn't connect,close connecting error")

    def createDatabase(self,db_name):
        self.cur.execute('CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci' % db_name)
        self.con.select_db(db_name)
        print('create DATABASE:',db_name)

    def selectDatabase(self,db_name):
        self.con.select_db(db_name)

    def getVersion(self):
        self.cur.execute('SELECT VERSION()')
        return self.getOneData()

    def getOneData(self):
        data = self.cur.fetchone()
        return data

    def createTable(self,table_name,attrdict,constraint):
        if self.isExitTable(table_name):
            print('%s is exit' % table_name)
            return
        sql = ''
        sql_mid = '`id` bigint(11) NOT NULL AUTO_INCREMENT,'
        for attr,value in attrdict.items():
            sql_mid += '`' + attr + '`' + ' ' + value + ','
        sql += 'CREATE TABLE IF NOT EXISTS %s (' % table_name
        sql += sql_mid
        sql += constraint
        sql += ') ENGINE=InnoDB DEFAULT CHARSET=utf8'
        print('create table:',sql)
        self.executeCommit(sql)

    def executeSql(self,sql=''):
        try:
            self.cur.execute(sql)
            records = self.cur.fetchall()
            return records
        except pymysql.Error as e:
            error = 'mysql execute failed! err (%s): %s' % (e.args[0],e.args[1])
            print(error)

    def executeCommit(self,sql=''):
        try:
            self.cur.execute(sql)
            self.con.commit()
        except pymysql.Error as e:
            self.con.rollback()
            error = 'mysql execute failed! err (%s): %s' % (e.args[0],e.args[1])
            print('error:',error)
            return error

    def insert(self,table_name,params):
        key = []
        value = []
        for tmpkey, tmpvalue in params.items():
            key.append(tmpkey)
            if isinstance(tmpvalue,str):
                value.append("\'" + tmpvalue + "\'")
            else:
                value.append(tmpvalue)
        attrs_sql = '(' + ','.join(key) + ')'
        values_sql = ' values(' + ','.join(value) + ')'
        sql = 'insert into %s' % table_name
        sql += attrs_sql + values_sql
        print("insert table:",sql)
        self.executeCommit(sql)

    def select(self,sql=''):
        if sql == '':
            print('select query sql is null')
            return
        print("select table:",sql)
        return self.executeSql(sql)

    def insertMany(self,table_name,attrs,values):
        values_sql = ['%s' for v in attrs]
        attrs_sql = '(' + ','.join(attrs) + ')'
        values_sql = ' value(' + ','.join(values_sql) + ')'
        sql = 'insert into %s' % table_name
        sql += attrs_sql + values_sql
        print("insertMany table:",sql)
        try:
            for i in range(0,len(values),20000):
                self.cur.executemany(sql,values[i:i+20000])
                self.con.commit()
        except pymysql.Error as e:
            self.con.rollback()
            error = 'insertMany executemany failed! ERROR (%s): %s' % (e.args[0],e.args[1])
            print(error)

    def delete(self,table_name,cond_dict):
        consql = ' '
        if cond_dict != '':
            for k,v in cond_dict.items():
                if isinstance(v,str):
                    v = "\'" + v + "\'"
                consql += table_name + "." + k + '=' + v + ' and '
        consql += ' 1=1 '
        sql = "DELETE FROM %s where %s" % (table_name,consql)
        print("delete table sql:",sql)
        return self.executeCommit(sql)

    def update(self,table_name,attrs_dict,cond_dict):
        attrs_list = []
        consql = ' '
        for tmpkey, tmpvalue in attrs_dict.items():
            attrs_list.append('`' + tmpkey + '`' + '=' + "\'" + tmpvalue + "\'")
        attrs_sql = ",".join(attrs_list)
        if cond_dict != '':
            for k,v in cond_dict.items():
                if isinstance(v,str):
                    v = "\'" + v + "\'"
                consql += '`' + table_name + "`." + '`' + k + '`' + '=' + v + ' and '
        consql += ' 1=1 '
        sql = "UPDATE %s SET %s where %s" % (table_name,attrs_sql,consql)
        print("update table sql:",sql)
        return self.executeCommit(sql)

    def dropTable(self,table_name):
        sql = "DROP TABLE %s" % table_name
        self.executeCommit(sql)

    def isExitTable(self,table_name):
        sql = "select * from %s limit 1" % table_name
        result = self.executeCommit(sql)
        if result is None:
            return True
        else:
            return False