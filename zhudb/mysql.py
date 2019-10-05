# -*- coding: utf-8 -*-
# Created by Bamboo - 04 Sep 2018 (Tue)


# from zhudb.utils import *
from zhudb.zhudb import ZhuDb
from zhudb.zhudb import Row
import time
import pymysql


class ZhuMysql(ZhuDb):
    """
    db = zhumysql.ZhuMysql(
        host="39.104.71.18",
        user="beedb",
        passwd="pl,Okm123",
        database="skl"
    )
    """
    def __init__(self, host, database, user, passwd=None, port=3306, **kwarg):
        self._last_use_time = time.time()  # timestamp
        if not isinstance(port, int):
            port = int(port)
        self._arguments = dict(
            host=host,
            port=port,
            user=user,
            passwd=passwd,
            database=database,
            charset="utf8",
            cursorclass=pymysql.cursors.DictCursor
        )
        if kwarg:
            for key in kwarg:
                self._arguments.update({key: kwarg[key]})
        self.dbname = database
        try:
            self._get_conn()
        except Exception as err:
            self._print_error(err)
            raise

    def _get_conn(self):
        self.conn = pymysql.connect(**self._arguments)

    def get_table_names(self):
        sql = '''
        SELECT table_name FROM information_schema.tables where table_schema='{dbname}';
        '''.format(dbname=self.dbname)
        result = [_['table_name'] for _ in self.query(sql)]
        return result

    def create_table(self, table_name, fields, noid=False):
        """
        # noid=True (False in default) - without 'id' field

        # "varchar(128)" in default
        fields = ["fname1", "fname2", "fname3"]

        # appoint a type for each field
        fields = [
            ("fname1", "varchar(128)"),
            ("fname2", "int not null")
        ]
        create_table("test_table", fields)
        """
        print('creating table {} ...'.format(table_name))
        sqlhead = "CREATE TABLE %s (id INTEGER PRIMARY KEY AUTO_INCREMENT , " % table_name
        if noid:
            sqlhead = "CREATE TABLE '%s' (" % table_name
        if fields is None:
            return
        if isinstance(fields[0], tuple):
            sqlmain = ', '.join([x[0] + " " + x[1] for x in fields])
        else:
            sqlmain = ', '.join([x + " varchar(128)" for x in fields])
        default_setting = "ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        sql = sqlhead + sqlmain + " ) " + default_setting
        try:
            cur = self._get_cursor()
            cur.execute(sql)
            self.conn.commit()
        except Exception as err:
            self.conn.rollback()
            # if table already exists
            if err.args[0] == 1050:
                return False
            self._print_error(err)
        return True

    def query(self, sql):
        """
        query('SELECT * FROM table')
        return [{key:val}, {key:val}]
        """
        cur = self._get_cursor()
        try:
            cur.execute(sql)
            # column_names = [d[0] for d in cur.description]
            return [Row(res) for res in cur]
        except Exception as err:
            self._print_error(err)
        finally:
            cur.close()
