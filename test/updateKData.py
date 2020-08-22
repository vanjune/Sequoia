# -*- encoding: UTF-8 -*-

import sys
sys.path.append("..")


import tushare as ts
import pandas as pd
import datetime
import logging
import settings
import talib as tl


import utils, data_fetcher

settings.init();
utils.prepare()
print(data_fetcher.update_data('688588'));

