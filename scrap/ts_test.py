#! /usr/local/bin/python3

import tushare as ts
import pandas as pd
from pandas import DataFrame
from pandas import Series

from sqlalchemy import create_engine
import pymysql
import datetime

pymysql.install_as_MySQLdb()

db_engine = None


def db_engine():
    engine = create_engine(
        'mysql://root:chunhui1234@127.0.0.1/ts_stock?charset=utf8')

    return engine


def ts_pro():
    pro = ts.pro_api(
        '92588dc04b6971bef521d82ab4e1fada2dba1dc529281880be1f50ef')
    return pro


def pro_test():

    engine = db_engine()
    print("engine is: ", engine)
    pro = ts_pro()
    # ss_stock_list = pro.stock_basic(list_status='L')
    # print("ss stock list is: \n",ss_stock_list)

    # ss_stock_list.to_sql('stock_list',con=engine,if_exists='append')

    # 交易所 SSE上交所 SZSE深交所 HKEX港交所(未上线)
    sz_stock_list = pro.stock_basic(exchange='', list_status='L')
    sz_stock_list.to_sql('stock_list', con=engine, if_exists='replace')


def test():
    industry = ts.get_industry_classified()
    print("data is: ", type(industry))
    codes = industry['code']
    print("codes is: \n", codes)
    for col in industry.columns:
        print(col)
        break


def date_test():

    d = datetime.date.today()
    print("current is: ", d, " year ", d.year)



def df_test():

    df = pd.read_csv('./df.csv')
    #print("df is: ", df)
    print("column is: ", df.columns)
    print("items is: ", df.items())
    print("first row is: ", (df.iloc[0]))
#     for item in df.items():
#         print("item is: ",item)
    index = df.columns 
    s = df.iloc[0]
    print("row 0 is: ",s)
    row_df = DataFrame(data=s,columns = index)
    print("row df is: ",row_df)
    df0 = df.query('date == "2018-01-08"')
    print("df0 is: \n",df0)
    df1 = df[df.date == '2018-01-08']
    print("df1 is: ",df1)
    date = df['date']
    print("data is: ", date)
    for d in date:
        print(d)
    
'''
    for  row in df.iterrows():
        print("row is: ",type(row)," content is: ",row)
        
        break
'''

if __name__ == '__main__':
    # test()
    # pro_test()
    # date_test()
    df_test()
