import os
from os import listdir
from os.path import isfile, join
import sys
import numpy as np  # noqa
import pandas as pd  # noqa
from pandas import DataFrame
from functools import reduce
from operator import itemgetter
from pprint import pprint
import logging
import math
import pickle
import json
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import talib.abstract as ta
import pandas_ta as pta
import freqtrade.vendor.qtpylib.indicators as qtpylib

from strategies.Template import Template

# must be imported after Template
from attributedict.collections import AttributeDict


class BBands(Template):



    def __init__(self, address: str):

        self.address = address


    stoploss = -25 / 100


    buy_delta_ema_period = AttributeDict({'value': 25})
    buy_ema_short = AttributeDict({'value': 9})
    buy_ema_long = AttributeDict({'value': 26-9})


    # 22/200:    233 trades. 178/0/55 Wins/Draws/Losses. Avg profit  14.49%. Median profit  18.15%. Total profit 3338.49677328 USDT (  33.38%). Avg duration 0:02:00 min. Objective: -3338.49677
    minimal_roi = {
        "0": 0.836,
        "33": 0
    }
    buy_params = {
        "buy_bollinger_std": 3.24,
        "buy_bollinger_window": 40,
        "buy_delta_ema_lookback_boll": 27,
    }
    sell_params = {
        "sell_cooldown_candles": 5,
        "sell_line_long": 65,
        "sell_line_long_src": "fwma",
        "sell_line_short": 25,
        "sell_line_short_src": "hma",
        "sell_reds_in_a_row_exit": 7,
    }



    # 28/40:    778 trades. 537/0/241 Wins/Draws/Losses. Avg profit  11.69%. Median profit  10.56%. Total profit 9036.50914649 USDT (  90.37%). Avg duration 0:04:00 min. Objective: -9036.50915
    minimal_roi = {
        "0": 2.414,
        "59": 0
    }
    buy_params = {
        "buy_bollinger_std": 2.75,
        "buy_bollinger_window": 25,
        "buy_delta_ema_lookback_boll": 29,
    }
    # 6/500:    767 trades. 523/2/242 Wins/Draws/Losses. Avg profit  13.19%. Median profit  19.50%. Total profit 10059.09243222 USDT ( 100.59%). Avg duration 0:06:00 min. Objective: -10059.09243
    sell_params = {
        "sell_cooldown_candles": 70,
        "sell_line": 55,
        "sell_line_long": 12,
        #"sell_line_long_src": "linreg",
        "sell_line_long_src": "ema",
        "sell_line_short": 5,
        #"sell_line_short_src": "alma",
        "sell_line_short_src": "ema",
        #"sell_line_src": "swma",
        "sell_line_src": "ema",
        "sell_reds_in_a_row_exit": 4,
    }



    def populate_indicators(self, dataframe: DataFrame) -> DataFrame:


        if dataframe is None or not len(dataframe):
            return dataframe

        # dataframe['close'] *= 10 ** self.num_zeros(dataframe['close'].iloc[0])
        dataframe['volume_buy'] = dataframe['invested']
        dataframe['volume_sell'] = dataframe['withdrew']
        dataframe['volume'] = dataframe['volume_buy'] + dataframe['volume_sell']
        dataframe['date'] = pd.to_datetime(dataframe['timestamp'], utc=True, unit='s')
        dataframe['open'] = dataframe['close'].shift(1)
        dataframe['low'] = dataframe[['open','close']].min(axis=1)
        dataframe['high'] = dataframe[['open','close']].max(axis=1)

        if dataframe['close'].isnull().values.any():
            print(f'!!!!!!! NaN values in close')


        dataframe['volume_buy_std'] = dataframe['volume_buy'].rolling(6).std()
        dataframe['volume_sell_std'] = dataframe['volume_sell'].rolling(6).std()

        dataframe['trades'] = dataframe['trades_buy'] + dataframe['trades_sell']

        dataframe['trades_diff'] = (dataframe['trades_buy'] - dataframe['trades_sell']).rolling(7).sum()


        dataframe.reset_index(drop=True, inplace=True)

        dataframe['rsi'] = ta.RSI(dataframe, timeperiod=10)



        if 'i' not in dataframe.columns: dataframe['i'] = range(len(dataframe))

        
        dataframe['time_b'] = (dataframe['timestamp'] - dataframe['timestamp'].shift(8)) / 8


        dataframe['green'] = np.select([(dataframe['open'] < dataframe['close'])], [1], default=0)
        dataframe['red'] = np.select([(dataframe['open'] > dataframe['close'])], [1], default=0)
        dataframe['min_open_close'] = dataframe[['open','close']].min(axis=1)
        dataframe['max_open_close'] = dataframe[['open','close']].max(axis=1)
        dataframe['mid_open_close'] = dataframe['open'] + (dataframe['close'] - dataframe['open']) / 2



        return dataframe
    

    def populate_entry_trend(self, dataframe: DataFrame) -> DataFrame:


        if 'volume_buy' not in dataframe.columns:
            assert 0, f"volume_buy not in dataframe"
            print(dataframe)
            return dataframe
    



        dataframe[f'delta_ema_buy'] = self.makeline(dataframe, period=int(self.buy_delta_ema_period.value), src='ema', column=f'volume_buy')
        dataframe[f'delta_ema_sell'] = self.makeline(dataframe, period=int(self.buy_delta_ema_period.value), src='ema', column=f'volume_sell')
        dataframe[f'delta_ema'] = dataframe[f'delta_ema_buy'] - dataframe[f'delta_ema_sell']
        dataframe.drop([f'delta_ema_buy', f'delta_ema_sell'], axis=1, inplace=True)



    

        hists = ['delta_ema','trades_diff','vol_diff','macdhist']
        for name in hists:
            if name in dataframe.columns:
                dataframe[f'{name}_pos_up'] = dataframe[name].where((dataframe[name] >= 0) & (dataframe[name] >= dataframe[name].shift(1)), None)
                dataframe[f'{name}_pos_down'] = dataframe[name].where((dataframe[name] >= 0) & (dataframe[name] < dataframe[name].shift(1)), None)
                dataframe[f'{name}_neg_up'] = dataframe[name].where((dataframe[name] < 0) & (dataframe[name] >= dataframe[name].shift(1)), None)
                dataframe[f'{name}_neg_down'] = dataframe[name].where((dataframe[name] < 0) & (dataframe[name] < dataframe[name].shift(1)), None)




        dataframe['ema_short'] = self.makeline(dataframe, period=int(self.buy_ema_short.value), src='ema')
        dataframe['ema_short_uptrend'] = np.select([(dataframe['ema_short'].shift(1) <= dataframe['ema_short'])], [1], default=0)

        dataframe['ema_long'] = self.makeline(dataframe, period=int(self.buy_ema_short.value + self.buy_ema_long.value), src='ema')

        dataframe['long_trend'] = self.makeline(dataframe, period=120, src='sma')


        # Bollinger Bands
        bollinger = qtpylib.bollinger_bands(qtpylib.typical_price(dataframe), window=self.buy_params['buy_bollinger_window'], stds=self.buy_params['buy_bollinger_std'])
        dataframe['bb_lowerband'] = bollinger['lower']
        dataframe['bb_middleband'] = bollinger['mid']
        dataframe['bb_upband'] = bollinger['upper']
        dataframe["bb_percent"] = (
            (dataframe["close"] - dataframe["bb_lowerband"]) /
            (dataframe["bb_upband"] - dataframe["bb_lowerband"])
        )
        dataframe["bb_width"] = (
            (dataframe["bb_upband"] - dataframe["bb_lowerband"]) / dataframe["bb_middleband"]
        )



        dataframe['target'] = np.nan
        dataframe['enter_long'] = 0
        dataframe['enter_tag'] = None
        dataframe['exit_long'] = 0
        dataframe['exit_tag'] = None

        _cumulative_delta = []
        _same_color = []
        cc = -1
        i_day = -1
        lasts_used = []

        dfn = dataframe.to_dict(orient="records")
        wait_long, wait_short = 0, 0
        has_one_enter = False
        max_price = 0
        renounce_ownership = False
        current_color = None
        cant_enter_more = False
        first_trade_date = None
        wait_red_rsi = False
        first_bollinger_skiped = False

        for i, row in enumerate(dfn):

            cc += 1

            if not first_trade_date:
                first_trade_date = row['date']

            i_day += 1


            if i > 0 and dfn[i - 1]['date'].strftime('%d/%m/%Y') != dfn[i]['date'].strftime('%d/%m/%Y'):
                i_day = 0
                lasts_used = []


            
            if row['renounce_ownership'] == 1:
                if not renounce_ownership:
                    dataframe.at[row['i'], 'cross_orange'] = row['high'] - (row['high'] - row['low']) / 2
                renounce_ownership = True
                
                # dataframe.loc[row['i'], 'enter_long'] = 1

            if row['high'] == None:
                continue


            if row['high'] > max_price:
                max_price = row['high']


            if wait_long:

                if row['rsi'] >= 45:
                    dataframe.loc[row['i'], 'enter_long'] = 1
                    wait_long = False



            if any([x['liquidity'] >= 200000 for x in dfn[i - 15:i]]):
                continue


            # bollinger
            if 1:

                if renounce_ownership:

                    if 1 or row['high'] > row['long_trend']:

                        if i - self.buy_params['buy_delta_ema_lookback_boll'] >= 0 and not all([dfn[i - ii]['delta_ema'] <= 0 for ii in range(0, self.buy_params['buy_delta_ema_lookback_boll'])]):

                            if row['bb_lowerband'] >= row['close'] and row['time_b'] < 70:
                                dataframe.loc[row['i'], 'enter_long'] = 1



        return dataframe
    

    def populate_exit_trend(self, dataframe: DataFrame) -> DataFrame:


        _period = int(self.sell_params['sell_line'])
        dataframe['sell_line'] = self.makeline(dataframe, period=_period if self.sell_params['sell_line_src'] not in ['mama', 'fama'] else _period - 3, src=self.sell_params['sell_line_src'])
        dataframe['sell_line_uptrend'] = np.select([(dataframe['sell_line'].shift(1) <= dataframe['sell_line'])], [1], default=0)


        try:
            dataframe['sell_line_short'] = self.makeline(dataframe, period=int(self.sell_params['sell_line_short']), src=self.sell_params['sell_line_short_src'])
            dataframe['sell_line_long'] = self.makeline(dataframe, period=int(self.sell_params['sell_line_short'] + self.sell_params['sell_line_long']), src=self.sell_params['sell_line_long_src'])
        except TypeError:
            dataframe['sell_line_short'] = np.nan
            dataframe['sell_line_long'] = np.nan



        dfn = dataframe.to_dict(orient="records")
        wait_exit_short = False
        wait_exit_long = False
        enter_i = None
        exit_on_positive = False
        for i, row in enumerate(dfn):
            
            if i < 3: continue
            
            if 'enter_long' in dfn[i] and dfn[i]['enter_long'] == 1:
                wait_exit_long = True
                enter_i = i
                max_profit_was = 0
                min_profit_was = 0
                exit_on_positive = False
                exit_on_red = False
                continue

            if wait_exit_long:
                # print(row['date'], 'wait_exit_long')
                profit = 100 * dfn[i]['close'] / dfn[enter_i]['close'] - 100
                highest_price_was = min([x['high'] for x in dfn[i - enter_i:i + 1]])
                max_profit_was = max(max_profit_was, profit)
                min_profit_was = min(min_profit_was, profit)

                if profit <= -1 * abs(self.stoploss) * 100:
                    dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'stop_loss')
                    wait_exit_long = False
                
                if profit > 0 and exit_on_positive:
                    dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'exit_on_positive')
                    wait_exit_long = False

                if exit_on_red:
                    if not row['green']:
                        dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'exit_on_red')
                        wait_exit_long = False
                        exit_on_red = False


                if i - enter_i > self.sell_params['sell_cooldown_candles']:
                    exit_on_red = True

                if 1:
                    if i - enter_i > self.sell_params['sell_reds_in_a_row_exit'] and all([not dfn[i - ii]['green'] for ii in range(0, self.sell_params['sell_reds_in_a_row_exit'])]):
                        dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'red_in_a_row')
                        wait_exit_long = False
                        

                if 1:
                    if max_profit_was > 10 and min_profit_was < -10:
                        exit_on_positive = True


                # rsi
                if 1:

                    if any([x['rsi'] >= 65 for x in dfn[enter_i:i]]) and dfn[i]['rsi'] <= 55 and i - enter_i > 10:
                    
                        dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'rsi')
                        wait_exit_long = False


                # line cross
                if 1:
                    if profit > 20:
                        if dfn[i - 1]['sell_line_long'] != None and dfn[i - 1]['sell_line_short'] >= dfn[i - 1]['sell_line_long'] and dfn[i]['sell_line_short'] < dfn[i]['sell_line_long']:
                            dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'line_cross')
                            wait_exit_long = False
                
                # sell_line
                if 1:
                    if profit > 20:
                        if not row['sell_line_uptrend']:
                            dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'sell_line_trend')
                            wait_exit_long = False


        return dataframe
    
    

