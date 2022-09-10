
import pymysql
from pymongo import MongoClient
from redis import ConnectionPool


def connect_db(db_name='zcs', name='', pwd=''):
    """
    连接192.168.1.119数据库，返回数据库变量
    :param db_name: 数据库名称，str
    :param name: 用户名，str
    :param pwd: 密码，str
    :return: 如果db_name是None，则返回MongoClient，否则返回Database变量，指向db_name数据库
    """
    client = MongoClient(host='192.168.1.106', port=27017, username=name, password=pwd)
    if db_name is None:
        return client
    else:
        return client[db_name]


def connect_db_other(host='', port=888):
    """
    连接其他数据库，返回数据库变量
    :return: 数据库变量
    """
    client = MongoClient(host=host, port=port)
    return client


def connect_sql():
    """
    连接SqlServer
    :return: pymssql.Cursor实例
    """
    conn = pymysql.connect(host='192.168.1.119', server='SZS\SQLEXPRESS', user='xxx', password='xxx')
    return conn.cursor(as_dict=True)


def connect_mysql():
    conn = pymysql.connect(host='192.168.1.119', port=3306, user='xxx', password='xxx', database='xxx')
    return conn.cursor()


def connect_redis(db_num=0):
    pool = ConnectionPool(host='192.168.1.120', port=6379, password='xxx', db=db_num, decode_responses=True)
    return pool