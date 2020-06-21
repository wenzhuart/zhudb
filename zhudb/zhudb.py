# -*- coding: utf-8 -*-
# Created by Bamboo - 04 Sep 2018 (Tue)


"""
dbname = 'livehouse_log'
conn = sqlite3.connect(dbname)
    conn.commit()
    conn.rollback()
    conn.close()
    conn.cursor()

cur = conn.cursor()
    cur.execute()
    cur.executemany()
    cur.close()
    cur.fetchone()
    cur.fetchmany()
    cur.fetchall()
    cur.scroll()
"""


# from zhudb.utils import *
import datetime
import sqlite3
import os
import logging
logger = logging.getLogger('zhudb')

###################################
# TODO: 

# def get_day_data_by(date, **condition):
#     sql = 'SELECT * FROM {} WHERE '.format(TABLE)
#     kvlist = []
#     for key in condition:
#         val = condition[key]
#         kvlist.append("{k}='{v}'".format(k=key, v=val))
#     if len(kvlist) > 1:
#         kvstr = ' AND '.join(kvlist)
#     else:
#         kvstr = ''.join(kvlist)
#     sql += kvstr
#     result = self.query(sql)

###################################


class ZhuDb(object):
    """
    sqlite
    """
    def __init__(self, dbname):
        # --- check dbname ---
        if '.db' in dbname:
            if not len(dbname) - dbname.find('.db') == 3:
                self._print_error('dbname error')
            else:
                self.dbname = dbname
        else:
            self.dbname = '{}.db'.format(dbname)
        self._get_conn(dbname)

    def __del__(self):
        self.close_all()
        logger.debug('zhudb: closed all')

    def _get_conn(self, path=None):
        path = self.dbname
        conn = sqlite3.connect(path)
        if os.path.exists(path) and os.path.isfile(path):
            self.conn = conn

    def _get_cursor(self):
        return self.conn.cursor()

    def _insert_many(self, sql, data):
        cur = self._get_cursor()
        try:
            for each_d in data:
                cur.execute(sql, each_d)
        except sqlite3.OperationalError as err:
            self._print_error(err, sql, each_d)
        else:
            self.conn.commit()
        finally:
            cur.close()

    def close_all(self, conn=None, cur=None):
        if getattr(self, 'cur', None) is not None:
            logger.debug('zhudb: closing cur')
            self.cur.close()
        if getattr(self, 'conn', None) is not None:
            logger.debug('zhudb: closing connection')
            self.conn.close()

    def get_columns(self, tablename):
        try:
            cur = self._get_cursor()
            # cur.execute('select * from {}'.format(tablename)).fetchone()
            cur.execute('SELECT * FROM {} LIMIT 1'.format(tablename))
            column_names = [d[0] for d in cur.description]
            return column_names
        finally:
            cur.close()

    def execute(self, sql):
        """
        Executes given sql clause 
        (if only query, use DB.query())
        returns the -- lastrowid effected (if exist) --
        """
        try:
            cur = self._get_cursor()
            cur.execute(sql)
            self.conn.commit()
        except Exception as err:
            self._print_error(err, sql)
        finally:
            cur.close()

    def query(self, sql):
        """
        query('SELECT * FROM table')
        return -> fetchall()
        """
        try:
            cur = self._get_cursor()
            # return [Row(res) for res in cur]
            return cur.execute(sql).fetchall()
        except Exception as err:
            self._print_error(err, sql)
        finally:
            cur.close()

    def query_dict(self, sql):
        """
        query_dict('SELECT * FROM table')
        return -> Row object (dict)
        """
        try:
            cur = self._get_cursor()
            re = cur.execute(sql).fetchall()
            column_names = [d[0] for d in cur.description]
            return [Row(zip(column_names, row)) for row in re]
        finally:
            cur.close()

    def query_iter(self, sql):
        """
        # iterate row by row
        # useful when iterate so many rows (eg. 1000k+)

        # return -> a yield object

        y = self.query_iter('SELECT * FROM table')
        for curent_row in y:
            print(curent_row)
        """
        try:
            cur = self._get_cursor()
            re = cur.execute(sql).fetchall()
            column_names = [d[0] for d in cur.description]
            for row in re:
                yield Row(zip(column_names, row))
        finally:
            cur.close()

    def insert_many(self, table, fields, data):
        """
        fields = ['f1', 'f2', 'f3']
        data = [
             ('wen', 12, 'beijing'),
             ('zhu', 17, 'chengdu'),
        ]
        insert_many("table_name", fields, data)
        """
        line_len = len(data[0])
        fields = ', '.join([x for x in fields])
        qmarks = ', '.join(['?' for x in range(line_len)])
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (table, fields, qmarks)
        self._insert_many(sql, data)
        return self.conn.total_changes

    def insert_lines(self, table, data):
        """
        data = [
            (1, "v1", 123, "v3"),
            (2, "val", 567, "val4")
        ]
        insert_lines("test_table", data)
        """
        qmarks = ', '.join(['?' for x in range(len(data[0]))])
        sql = "INSERT INTO %s VALUES (%s)" % (table, qmarks)
        self._insert_many(sql, data)

    def insert(self, table, **kwargs):
        """
        new = {
            "field_1": "abc",
            "field_2": 123
        }
        insert("test_table", **new)
        """
        argKeys = ', '.join([key for key in kwargs.keys()])
        argVals = ', '.join(["{}".format(self._qt(val)) for val in kwargs.values()])
        query = "INSERT into {} ({}) VALUES ({})".format(table, argKeys, argVals)
        self.execute(query)

    def insert_if_not_exist(self, table, bykey=None, **kwargs):
        """
        new = {
            "field_1": "abc",
            "field_2": 123
        }
        insert_if_not_exist("test_table", **new)
        
        bykey = ['key1', 'key2']        
        if 'bykey' not set, check all fields except 'id'

        return False -> already existed
        retrun True -> insert completed
        """
        keydict = {}
        if bykey:
            for key in bykey:
                keydict[key] = kwargs[key]
            if self._check_existed(table, keydict):
                return False
        else:
            if 'id' in kwargs.keys():
                del kwargs['id']
            if self._check_existed(table, kwargs):
                return False
        self.insert(table, **kwargs)
        return True

    def _check_existed(self, tablename, keydict):
        sql = 'SELECT * FROM {} WHERE '.format(tablename)
        kvlist = []
        for key in keydict:
            val = keydict[key]
            kvlist.append("{k}='{v}'".format(k=key, v=val))
        if len(kvlist) > 1:
            kvstr = ' AND '.join(kvlist)
        else:
            kvstr = ''.join(kvlist)
        sql += kvstr
        result = self.query(sql)
        if result:
            logger.info(
                'Row "{}" Already existed as "{}" in table "{}"'.format(
                    keydict, result, tablename))
            return True
        else:
            return False

    def _print_error(self, err, *args):
        msg = ''
        for each in args:
            msg += u'{}'.format(each)
        errinfo = u'ERROR: {} - INFO: {}'.format(err, msg).encode('utf-8')
        lines = '-' * 70
        msg = '\n{}\n\t{}\n{}'.format(lines, errinfo, lines)
        logger.error(msg)

    def _qt(self, x):
        if isinstance(x, str):
            try:
                if "'" in x:
                    x = x.replace("'", r'\'')
                return u"'{}'".format(x)
            except UnicodeEncodeError as err:
                logger.error('unicode error in _qt: {}'.format(err))
                raise
        elif isinstance(x, int):
            return x
        elif isinstance(x, float):
            return x
        elif isinstance(x, datetime.date):
            return "('{}')".format(x)
        elif isinstance(x, bytes):
            return sqlite3.Binary(x)
        elif x is None:
            return 'NULL'
        else:
            logger.warning('can not detect type of "{}" return directly'.format(x))
            return x


class Row(dict):
    """
    the purpose of this class is to change dict object
    can be visited with object.value way (point syntax)
    """
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError('zhudb - Row Class: %s' % name)










