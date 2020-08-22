# -*- encoding: UTF-8 -*-

import data_fetcher
import utils
import strategy.enter as enter
from strategy import turtle_trade
from strategy import backtrace_ma250
from strategy import breakthrough_platform
from strategy import parking_apron
from strategy import low_backtrace_increase
from strategy import keep_increasing
import tushare as ts
import notice
import logging
import db
import time
import datetime
import urllib
import settings
import pandas as pd
from win32api import GetTickCount, Beep


def process():
    tc = GetTickCount()
    logging.info("************************ process start ***************************************")
    try:
        #raise urllib.error.URLError('')
        #拉取股票数据，把代码，名称，流通市值存到csv文件
        tc = GetTickCount()
        Beep(1000, 500);
        all_data = ts.get_today_all()
        print("读取股票代码耗时%d毫秒"%(GetTickCount()-tc));tc = GetTickCount();
        Beep(1000, 500);
        subset = all_data[['code', 'name', 'nmc']]
        subset.to_csv(settings.STOCKS_FILE, index=None, header=True)
        stocks = [tuple(x) for x in subset.values]
        # 统计一下龙虎榜
        statistics(all_data, stocks)
    except urllib.error.URLError as e:
        subset = pd.read_csv(settings.STOCKS_FILE)
        subset['code'] = subset['code'].astype(str)
        stocks = [tuple(x) for x in subset.values]

    if utils.need_update_data():
        utils.prepare()
        data_fetcher.run(stocks[:1000])
        check_exit()
        print("读取K线耗时%d毫秒"%(GetTickCount()-tc));tc = GetTickCount();
        input('按键，继续');
    strategies = {
        '海龟交易法则': turtle_trade.check_enter,
        '放量上涨': enter.check_volume,
        '突破平台': breakthrough_platform.check,
        '均线多头': keep_increasing.check,
        '无大幅回撤': low_backtrace_increase.check,
        '停机坪': parking_apron.check,
        '回踩年线': backtrace_ma250.check,
    }

    if datetime.datetime.now().weekday() == 0:
        strategies['均线多头'] = keep_increasing.check

    for strategy, strategy_func in strategies.items():
        tc = GetTickCount();
        check(stocks, strategy, strategy_func)
        print("耗时%d毫秒"%(GetTickCount()-tc));
        time.sleep(2)

    logging.info("************************ process   end ***************************************")


def check(stocks, strategy, strategy_func):
    # 跑策略，跑完过滤出来合格的股票并显示
    end = None
    m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
    results = list(filter(m_filter, stocks))

    logging.info('**************"{0}"**************\n{1}\n**************"{0}"**************\n'.format(strategy, results))
    notice.strategy('**************"{0}"**************\n{1}\n**************"{0}"**************\n'.format(strategy, results))


def check_enter(end_date=None, strategy_fun=enter.check_volume):
    # 返回一个函数，如果跑过这个策略了就读了直接给，否则跑策略
    def end_date_filter(code_name):
        data = utils.read_data(code_name)
        if data is None:
            return False
        else:
            return strategy_fun(code_name, data, end_date=end_date)
        # if result:
        #     message = turtle_trade.calculate(code_name, data)
        #     logging.info("{0} {1}".format(code_name, message))
        #     notice.push("{0} {1}".format(code_name, message))

    return end_date_filter


# 统计数据
def statistics(all_data, stocks):
    limitup = len(all_data.loc[(all_data['changepercent'] >= 9.5)])
    limitdown = len(all_data.loc[(all_data['changepercent'] <= -9.5)])

    up5 = len(all_data.loc[(all_data['changepercent'] >= 5)])
    down5 = len(all_data.loc[(all_data['changepercent'] <= -5)])

    def ma250(stock):
        stock_data = utils.read_data(stock)
        return enter.check_ma(stock, stock_data)

    ma250_count = len(list(filter(ma250, stocks)))

    msg = "涨停数：{}   跌停数：{}\n涨幅大于5%数：{}  跌幅大于5%数：{}\n年线以上个股数量：    {}"\
        .format(limitup, limitdown, up5, down5, ma250_count)
    logging.info(msg)
    notice.statistics(msg)


def check_exit():
    t_shelve = db.ShelvePersistence()
    file = t_shelve.open()
    for key in file:
        code_name = file[key]['code_name']
        data = utils.read_data(code_name)
        if turtle_trade.check_exit(code_name, data):
            notice.strategy("{0} 达到退出条件".format(code_name))
            logging.info("{0} 达到退出条件".format(code_name))
            del file[key]
        elif turtle_trade.check_stop(code_name, data, file[key]):
            notice.strategy("{0} 达到止损条件".format(code_name))
            logging.info("{0} 达到止损条件".format(code_name))
            del file[key]

    file.close()

