#! /usr/local/bin/python3

import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import common
import time
import calendar
# import init_ts

pymysql.install_as_MySQLdb()

db_engine = None
ts_pro = None
ts_connection = None


def init():
    global db_engine
    global ts_pro
    global ts_connection
    db_engine = create_engine(
        'mysql://root:chunhui1234@127.0.0.1/ts_stock?charset=utf8')
    ts_pro = ts.pro_api(
        '92588dc04b6971bef521d82ab4e1fada2dba1dc529281880be1f50ef')
    ts_connection = common.init_ts()


def scrap_stock_list():

    global ts_pro
    global db_engine
    # pro.stock_basic(list_status='L')
    # 交易所 SSE上交所 SZSE深交所 HKEX港交所(未上线)
    sz_stock_list = ts_pro.stock_basic(exchange='SZSE', list_status='L')
    print("sz stock list is: ", sz_stock_list)
    sz_stock_list.to_sql('stock_list', con=db_engine, if_exists='replace')


def scrap_hs_const():
    global ts_pro
    global db_engine

    # 获取沪股通成分
    df = ts_pro.hs_const(hs_type='SH')

    df.to_sql('hs_const', con=db_engine, if_exists='replace')
    # 获取深股通成分
    df = ts_pro.hs_const(hs_type='SZ')
    df.to_sql('hs_const', con=db_engine, if_exists='replace')


def scrap_company_info():
    global ts_pro
    global db_engine

    # df = ts_pro.stock_company(exchange='SSE')
    # df.to_sql('company_basic_info',con= db_engine,if_exists='replace')

    df = ts_pro.stock_company(exchange='SZSE')
    df.to_sql('company_basic_info', con=db_engine, if_exists='replace')


def scrap_day_k_data():
    global ts_connection
    global ts_pro
    global db_engine

    symobol_list = common.get_stock_list_by_ts_code(ts_connection)

    for symbol in symobol_list:
        print("===== will scrap %s ======" % (symbol))
        df = ts_pro.daily(ts_code=symbol)
        table_name = common.stock_day_k_table_name(symbol)
        df.to_sql(table_name, con=db_engine, if_exists='append')
        print("======scrap %s done ======" % (symbol))
        time.sleep(1)


def scrap_week_k_data():
    global ts_connection
    global ts_pro
    global db_engine

    symobol_list = common.get_stock_list_by_ts_code(ts_connection)

    for symbol in symobol_list:
        print("===== will scrap %s ======" % (symbol))
        df = ts_pro.weekly(ts_code=symbol)
        table_name = common.stock_week_k_table_name(symbol)
        df.to_sql(table_name, con=db_engine, if_exists='append')
        print("======scrap %s done ======" % (symbol))
        time.sleep(1)


def scrap_month_k_data():
    global ts_connection
    global ts_pro
    global db_engine

    symobol_list = common.get_stock_list_by_ts_code(ts_connection)

    for symbol in symobol_list:
        print("===== will scrap %s ======" % (symbol))
        df = ts_pro.monthly(ts_code=symbol)
        table_name = common.stock_month_k_table_name(symbol)
        df.to_sql(table_name, con=db_engine, if_exists='append')
        print("======scrap %s done ======" % (symbol))
        time.sleep(1)


def scrap_all_past_basic_infos():
    months = [1,2,3,4,5]
    c = calendar.Calendar()
    year = 2019
    for month in months:
        date_items = c.itermonthdates(2019,month)
        for day in date_items:
            if day.month != month:
                continue
            if day.weekday() >= 5:
                continue
            date = '%04d%02d%02d' %(year,day.month,day.day)
            print("date is: ",date)
            scrap_all_basic_info(date)

def scrap_all_basic_info(date =None):
    global ts_connection
    global ts_pro
    global db_engine

    if date is None:
        date = common.today()
        #date = '20190619'
    print("==== will scrap basic info on ",date," ====")
    basic_data = ts_pro.daily_basic(ts_code='',trade_date=date)
    basic_data.to_csv('./basic_info.csv')
    symbols = basic_data['ts_code']
    for symbol in symbols:
        row = basic_data[basic_data.ts_code == symbol]
        tb_name = common.stock_basic_info_table_name(symbol)
        row.to_sql(tb_name,con = db_engine, if_exists ='append')    
    print("===== scrap basic info done =====")

def db_test():
    global ts_connection

    common.get_stock_list(ts_connection)
def df_test():

    df = pd.read_csv('./df.csv')
    print("df is: ",df)
    print("column is: ",df.column)

if __name__ == '__main__':

    init()
    # scrap_stock_list()
    # scrap_hs_const()
    print("will scrap company")
    # scrap_company_info()
    # db_test()

    # scrap_day_k_data()
    #scrap_week_k_data()
    #scrap_all_basic_info()
    scrap_all_past_basic_infos()

    print("========scrap done======")
