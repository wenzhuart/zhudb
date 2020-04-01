# -*- coding: utf-8 -*-
# Created by Bamboo - 04 Sep 2018 (Tue)


# from zhudb.utils import *
from zhudb.zhudb import ZhuDb
from zhudb.zhudb import logger
# from zhudb.zhudb import Row


class ZhuSqlite(ZhuDb):

    # --- additional method ---
    def get_tables(self):
        try:
            cur = self._get_cursor()
            all_tbls = [x[0] for x in cur.execute('SELECT tbl_name FROM sqlite_master')]
            return all_tbls
        finally:
            cur.close()

    def insert_df(self, df, tablename, index=False, index_label=None):
        df.to_sql(tablename, self.conn, if_exists='replace', index=index, index_label=index_label)

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
        logger.debug('creating table "{}"'.format(table_name))
        sqlhead = "CREATE TABLE '%s' ('id' INTEGER PRIMARY KEY, " % table_name
        if noid:
            sqlhead = "CREATE TABLE '%s' (" % table_name
        if fields is None:
            return
        if isinstance(fields[0], tuple):
            sqlmain = ', '.join(["'" + x[0] + "' " + x[1] for x in fields])
        else:
            sqlmain = ', '.join(["'" + x + "' " + "varchar(128)" for x in fields])
        sql = sqlhead + sqlmain + " )"
        logger.debug(sql)
        try:
            self.conn.execute(sql)
            self.conn.commit()
        except Exception as err:
            if 'exists' in '{}'.format(err):
                logger.warning('table "{}" already exists'.format(table_name))
            else:
                self._print_error(err)
            return False
        return True

    # --- over write ---
    def insert(self, table, **kwargs):
        """
        new = {
            "field_1": "abc",
            "field_2": 123
        }
        insert("test_table", **new)
        """
        argKeys = ', '.join([key for key in kwargs.keys()])
        qmarks = ', '.join(['?' for x in range(len(kwargs))])
        clause = "INSERT INTO \'{tbl}\' ({argKeys}) VALUES({qms});".format(
            tbl=table,
            argKeys=argKeys,
            qms=qmarks)
        values = [kwargs[k] for k in kwargs]
        cur = self._get_cursor()
        cur.execute(clause, values)
        lastid = cur.lastrowid
        self.conn.commit()
        return lastid 
        
        

