#! /usr/local/bin/python3
# -*- coding:utf-8 -*-

import pandas as pd 
import common
from sqlalchemy import create_engine
import pymysql
import matplotlib.pyplot as plt
import time
import numpy as np

pymysql.install_as_MySQLdb()

db_engine = None
ts_pro = None
ts_connection = None


def init():
    global db_engine
    #global ts_pro
    #global ts_connection
    db_engine = create_engine(
        'mysql://root:chunhui1234@127.0.0.1/ts_stock?charset=utf8')
    #ts_pro = ts.pro_api(
    #    '92588dc04b6971bef521d82ab4e1fada2dba1dc529281880be1f50ef')
    

'''

        trade_dates = stock_df['trade_date']
        close_prices = stock_df['close']
        df_dates = pd.DataFrame(data=trade_dates.values,index=trade_dates.index,columns=['trade_date'])
        df_close = pd.DataFrame(data=close_prices.values,index = close_prices.index,columns =['price'])

        date_price = pd.merge(df_dates,df_close,left_index=True,right_index=True)
        date_price.plot(x='trade_date',y='price')
'''

def analyse_stocks():

    symbol = '000010.SZ'
    analyse_stock(symbol)

def analyse_stock(symbol):
    
    global db_engine
    

    
    sys = ['000010.SZ','000020.SZ','000001.SZ'] #
     #'0', '000001.SZ', '20190613', '12.54', '12.68', '12.43', '12.59', '12.57', '0.02', '0.1591', '530000.26', '666277.695'

    trade_dates = None 
    date_price = None
    prices = []
    columns = []

    for sy in sys:

        tb_name = common.stock_day_k_table_name(sy)
        sql = "select * from %s where ts_code = '%s' and trade_date >= '20100101' order by trade_date ;" %(tb_name,sy)        
        stock_df = pd.read_sql(sql,con=db_engine)
        if stock_df is None:
            continue
        
        trade_dates = stock_df['trade_date']

        close_prices = stock_df['close']

        #df_dates = pd.DataFrame(data=trade_dates.values,index=trade_dates.index,columns=['trade_date'])
        column = 'price_%s'%(sy)

        df_close = pd.DataFrame(data=close_prices.values,index = trade_dates.values,columns =[column])
        print("df close is: ",df_close)
        if date_price is None:
            date_price = df_close
        else:
            date_price = date_price.join(df_close)
            #print("after join date_price is: \n",date_price)
            #date_price = pd.merge(date_price,df_close,left_index=True,right_index=True)
        #date_price.plot(x='trade_date',y='price')

    print("date price is: ",date_price)
    date_price.plot()
    # plt.show()
    #plt.imshow()
    plt.savefig("./0002.png")

    #print("prices is: ",prices)
    # print("columns is: ",columns)
    # all_df = pd.DataFrame(prices, columns= columns)
    # print("all df is: \n",all_df)
    
    
def test():
    ts = pd.Series(np.random.randn(1000),index=pd.date_range('1/1/2000', periods=1000))
    ts = ts.cumsum()
    plt.figure()
    ts.plot()
    plt.show()

def stock_list_test():

    global db_engine

    stock_list = pd.read_sql_table('stock_list',columns=['ts_code'] , con=db_engine)    
    print("stock_list is: ",stock_list)
    print("type is: ",type(stock_list))
    code_list = stock_list['ts_code']
    

if __name__ == '__main__':

    #test()
    init()
    #analyse_stocks()
    stock_list_test()
        