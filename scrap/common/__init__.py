#! /usr/local/bin/python3
# -*- coding: utf-8 -*-

import pymysql
import time
import datetime

def init_ts():
    connection = pymysql.connect(
        # host='127.0.0.1',port=3306,
        unix_socket="/tmp/mysql.sock",
        user='root', password='chunhui1234',
        db='ts_stock', charset='utf8',
        cursorclass=pymysql.cursors.DictCursor)
    return connection


def close_ts(connection):
    connection.close()


def get_stock_list(connection):

    with connection.cursor() as cursor:
        sql = "select * from stock_list ;"
        result = cursor.execute(sql)
        # print("result is : \n ",result)
        # row = cursor.fetchone()
        # print("row is: ",row , " type is: ",type(row))
        rows = cursor.fetchall()
        # print("rows is: ",rows , " type is: ",type(rows))
        return rows


def get_stock_list_by_ts_code(connection):
    with connection.cursor() as cursor:
        sql = "select * from stock_list ;"
        result = cursor.execute(sql)
        rows = cursor.fetchall()
        codes = []
        for row in rows:
            code = row['ts_code']
            codes.append(code)

        return codes


def stock_day_k_table_name(symbol):

    return stock_k_table_name('day', symbol)


def stock_week_k_table_name(symbol):

    return stock_k_table_name('week', symbol)


def stock_month_k_table_name(symbol):

    return stock_k_table_name('month', symbol)


def stock_k_table_name(type, symbol):

    if '.' in symbol:
        index = symbol.find('.')
        symbol = symbol[:index]

    try:
        s = int(symbol)
        suffix = s % 10
        return "stock_%s_k_%d" % (type, suffix)
        pass
    except:
        return 'stock_%s_k_0' % (type)


def stock_basic_info_table_name(symbol):

    if '.' in symbol:
        index = symbol.find('.')
        symbol = symbol[:index]

    try:
        s = int(symbol)
        suffix = s % 10
        return "stock_basic_%d" % (suffix)
        pass
    except:
        return 'stock_basic_0'

def bucket_for_symbol(symbol):

     if '.' in symbol:
        index = symbol.find('.')
        symbol = symbol[:index]

    #try:
        s = int(symbol)
        suffix = s % 10
        return suffix
    #except:
        return 0

def today(split=""):
    
    today = datetime.date.today()
    year = today.year;
    month = today.month;
    day = today.day
        
    if split != None:
        s = "%04d%s%02d%s%02d"%(year,split,month,split, day)
    else:
        s = "%04d%02d%02d"%(year,month,day)

    return s 

def is_weekday(date):

    week = date.weekday()
    return week >=0 and week < 5
