
![Alt text](https://img3.doubanio.com/icon/u3430532-14.jpg)

zhudb
====================

一个 mysql, sqlite 数据库工具 for python

![](https://img.shields.io/badge/language-python3-orange.svg)
[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)

- - -

依赖
====================
* sqlite3
* pymysql


使用
====================

	from zhudb import ZhuSqlite
	from zhudb import ZhuMysql

### 创建对象

##### sqlite

	db = ZhuSqlite('dbname')

##### mysql

	db = ZhuMysql(
	    host="127.0.0.1",
	    user="name",
	    passwd="pwd",
	    database="test"
	)

### 创建 table
	
	fields = ["fname1", "fname2", "fname3"]
	db.create_table("test_table", fields<, noid=True>)

* 字段默认 "varchar(128)" 类型
* noid, 创建 table 时不自动创建 'id' 字段, 默认为 False

给每个字段指定类型

	fields = [
	    ("fname1", "varchar(128)"),
	    ("fname2", "int not null")
	]
	db.create_table("test_table", fields<, noid=True>)

返回:
* True  -> 创建完成
* False -> 未能创建

### 取得 table 里所有字段名

	db.get_columns()

返回: 字段名 list [' ', ' ']


### 查

	db.query('SELECT * FROM table')

返回 list 包含 tuple: [ ( ), ( ) ]

	db.query_dict('SELECT * FROM table')

返回 list 包含 dict: [ { }, { } ]

	y = self.query_iter('SELECT * FROM table')
	for curent_row in y:
	    print(curent_row)

返回 yeild 对象


### 插入

##### 增加一行

	new_line = {
	    "field_1": "abc",
	    "field_2": 123
	}
	db.insert("test_table", **new_line)

##### 增加一行, 并检查重复

	new_line = {
	    "field_1": "abc",
	    "field_2": 123
	}
	db.insert_if_not_exist("test_table", <bykey=['key1', 'key2'], > **new_line)
	
bykey: 检查哪些字段需要检查重复,
如果 'bykey' 留空, 则检查所有字段 (新行的所有字段的值此前都没有出现过才会插入)

返回 
* False -> already existed
* True -> insert completed

##### 增加多行 (所有字段)

	data = [
	    (1, "v1", 123, "v3"),
	    (2, "val", 567, "val4")
	]
	db.insert_lines("test_table", data)

##### 增加多行 (特定字段)

	fields = ['f1', 'f2', 'f3']
	data = [
	     ('wen', 12, 'beijing'),
	     ('zhu', 17, 'chengdu'),
	]
	db.insert_many("table_name", fields, data)










