# -*- encoding: UTF-8 -*-

import tushare as ts
import pandas as pd
import datetime
import logging
import settings
import talib as tl

import utils

import concurrent.futures

from pandas.tseries.offsets import *


def update_data(code_name):
    stock = code_name[0]
    old_data = utils.read_data(code_name)
    Astart_time = ''
    if not old_data.empty:
        start_time = utils.next_weekday(old_data.iloc[-1].date)
        current_time = datetime.datetime.now()
        if start_time > current_time:
            return
        Astart_time = str(start_time)

    from win32api import GetTickCount
    if Astart_time:
        tc = GetTickCount()
        df = ts.get_k_data(stock, autype='qfq', start = Astart_time)
        print('局部提数据耗时%d'%(GetTickCount()-tc));

        tc = GetTickCount()
        df = ts.get_k_data(stock, autype='qfq')
        print('全部提数据耗时%d'%(GetTickCount()-tc));
    else:
        df = ts.get_k_data(stock, autype='qfq')
    data = old_data.append(df, ignore_index = True)
    del data['p_change']

    if data is None or data.empty:
        logging.debug("股票："+stock+" 没有数据，略过...")
        return

    data['p_change'] = tl.ROC(data['close'], 1)

    return data


##        mask = (df['date'] >= start_time.strftime('%Y-%m-%d'))
##        appender = df.loc[mask]
##        if appender.empty:
##            return
##        else:
##            return appender


def init_data(code_name):
    stock = code_name[0]
    data = ts.get_k_data(stock, autype='qfq')

    if data is None or data.empty:
        logging.debug("股票："+stock+" 没有数据，略过...")
        return

    data['p_change'] = tl.ROC(data['close'], 1)

    return data


def run(stocks, update_fun = init_data):
    append_mode = False
    update_fun = init_data

    stocksH10 = stocks
##    stocksH10 = stocks[:10]
    # 多核心计算
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_stock = {executor.submit(update_fun, stock): stock for stock in stocksH10}
        for future in concurrent.futures.as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                data = future.result()
                data['code'] = data['code'].apply(lambda x: str(x))
                if data is not None:
                    file_name = stock[0] + '-' + stock[1].replace('*', '#') + '.h5'
                    data.to_hdf(settings.DATA_DIR + "/" + file_name, 'data', append=append_mode, format='table')
            except Exception as exc:
                print('%s(%r) generated an exception: %s' % (stock[1], stock[0], exc))
