#! /usr/local/bin/python3
import pymysql
import pandas as pd 
import baostock as bs 
import datetime 
import calendar

def test ():
    
    lg = bs.login()
    print('login response error_code is: ',lg.error_code)
    print('login response error msg is: ',lg.error_msg)
    #### 获取历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节
    rs = bs.query_history_k_data_plus("sh.600000",
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
        start_date='2017-06-01', end_date='2017-12-31', 
        frequency="d", adjustflag="3") #frequency="d"取日k线，adjustflag="3"默认不复权
    print('query_history_k_data_plus respond error_code:'+rs.error_code)
    print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

    #### 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())

    result = pd.DataFrame(data_list, columns=rs.fields)
    #### 结果集输出到csv文件 ####
    result.to_csv("/tmp/history_k_data.csv", encoding="gbk", index=False)
    print(result)

    #### 登出系统 ####
    bs.logout()
    

def time_test():
    
    today = datetime.date.today()
    year = today.year;
    month = today.month;
    day = today.day
    
    split = '-'
    if split != None:
        s = "%04d%s%02d%s%02d"%(year,split,month,split, day)
    else:
        s = "%04d%02d%02d"%(year,month,day)
    
    print("today is: ",today," type is: ",type(today)," s is: ",s," year type is: ",type(year))

def calendar_test():
    c = calendar.Calendar()
    iterm = c.itermonthdates(2018,5)
    for it in iterm:
        print("date is: ",it," month",it.month," day: ",it.day)
        

    '''    
    iterm = c.itermonthdays(2018,5)
    print("====for days =====")
    for it in iterm:
        print("date is: ",it)
    iterm = c.itermonthdays2(2018,5)
    print("====for days 2=====")
    for it in iterm:
        print("date is: ",it)

    '''

if __name__ == '__main__':
    #test()    
    #time_test()
    calendar_test()