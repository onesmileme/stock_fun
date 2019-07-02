#! /usr/local/bin/python3
# -*- coding:utf-8 -*-
import pandas as pd
import baostock as bs


def analyse():
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond error_msg:' + lg.error_msg)
    # 获取某一天的全市场的证券和指数代码
    rs = bs.query_all_stock(day="2019-06-14")
    print('query_all_stock respond error_code:' + rs.error_code)
    print('query_all_stock respond error_msg:' + rs.error_msg)
    # 打印结果集
    code_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        code_list.append(rs.get_row_data()[0])
    print(code_list)

    df = pd.DataFrame()
    # 获取沪深A股历史K线数据
    for code in code_list:
        # 详细指标参数，参见“历史行情指标参数”章节
        rs = bs.query_history_k_data(code,
                                 "date,code,pbMRQ",
                                 start_date='2019-06-14',
                                 end_date='2019-06-14',
                                 frequency="d", adjustflag="3")
        if rs.error_code == '0':
            result = rs.get_data()
            n = result.shape[0]
            if n <= 0:
                continue
        # 删除pbMRQ为0的证券或指数
        if float(result.iloc[0, 2]) != 0:
            if df.empty:
                df = rs.get_data()
            else:
                df = pd.concat([df, rs.get_data()])
    # print(df)

    # 结果集输出到csv文件
    df.to_csv("./history_A_stock_k_data.csv", index=False)
    print(df)
    df['pbMRQ'] = df['pbMRQ'].astype(float)
    # 以pbMRQ进行升序排序
    df_sortby_pbMRQ = df.sort_values(by='pbMRQ')
    # 存入文件

    df_sortby_pbMRQ.to_csv("./history_A_stock_k_data2.csv",
                       index=False)
    print("当天A股市场pb最低的证券：" + df_sortby_pbMRQ.iloc[0][1])
    # 登出系统
    bs.logout()

if __name__ == '__main__':
     analyse()