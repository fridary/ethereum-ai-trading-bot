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

sys.path.append("..")
import utils

from strategies.Template import Template

# must be imported after Template
from attributedict.collections import AttributeDict


class MlCatboost(Template):



    def __init__(self, address: str, last_block: int, last_db_trade, model) -> None:

        self.address = address
        self.last_block = last_block
        self.last_db_trade = last_db_trade
        self.model = model


    stoploss = -99.99999 / 100


    buy_delta_ema_period = AttributeDict({'value': 25})
    buy_ema_short = AttributeDict({'value': 9})
    buy_ema_long = AttributeDict({'value': 26-9})


    # macd
    buy_macd_fastperiod = AttributeDict({'value': 12*2})
    buy_macd_slowperiod = AttributeDict({'value': (26-12)*2})
    buy_macd_signalperiod = AttributeDict({'value': 9*2})


    sell_macd_after_candles = AttributeDict({'value': 50})


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



    def populate_indicators(self, dataframe: DataFrame) -> DataFrame:


        if dataframe is None or not len(dataframe):
            return dataframe

        utils.save_log_strg(f"starting populate_indicators(), dataframe.shape={dataframe.shape}", address=self.address, block=self.last_block)


        dfn = dataframe[['close']].to_dict(orient="records")
        last_valid_value = None
        for i, row in enumerate(dfn):
            if i == 0: 
                last_valid_value = row['close']
                continue
            if 100 - 100 * row['close'] / last_valid_value > 99.999:
                dfn[i]['close'] = None
            else: last_valid_value = row['close']
        dataframe.reset_index(drop=True, inplace=True)
        dataframe['close_2'] = pd.DataFrame(dfn)['close'].interpolate(method='linear', axis=0)
        dataframe.drop(['close'], axis=1, inplace=True)
        dataframe.rename(columns={'close_2': 'close'}, inplace=True)




        # dataframe['close'] *= 10 ** self.num_zeros(dataframe['close'].iloc[0])
        dataframe['volume_buy'] = dataframe['invested']
        dataframe['volume_sell'] = dataframe['withdrew']
        dataframe['volume'] = dataframe['volume_buy'] + dataframe['volume_sell']
        dataframe['date'] = pd.to_datetime(dataframe['timestamp'], utc=True, unit='s')

        _open_0 = dataframe.at[0, 'open']
        dataframe['open'] = dataframe['close'].shift(1)
        dataframe.at[0, 'open'] = _open_0

        dataframe['low'] = dataframe[['open','close']].min(axis=1)
        dataframe['high'] = dataframe[['open','close']].max(axis=1)

        if dataframe['close'].isnull().values.any():
            utils.save_log_strg(f"!!!!!!! NaN values in close", address=self.address, block=self.last_block)
            # dataframe['close'] = dataframe['close'].fillna(method='ffill')




        save_path = '/disk_sdc/df_features_catboost'

        if os.path.exists(f"{save_path}/{self.address}.csv"):
            df_ml = pd.read_csv(f"{save_path}/{self.address}.csv", header=0)
            utils.save_log_strg(f"df_ml загружен из файла, shape={df_ml.shape}:", address=self.address, block=self.last_block)
            utils.save_log_strg(df_ml, address=self.address, block=self.last_block)


            lookback_df_ml = 150
            start_time = time.time()
            df_ml_cropped = self.create_features(dataframe.iloc[min(-lookback_df_ml,len(df_ml)-len(dataframe)):], pair=self.address)
            utils.save_log_strg(f"created df_ml_cropped with lookback_df_ml in {(time.time() - start_time):.2f}s, shape={df_ml_cropped.shape}:", address=self.address, block=self.last_block)
            utils.save_log_strg(df_ml_cropped, address=self.address, block=self.last_block)

            df_ml = df_ml.drop(df_ml[df_ml['block'] >= df_ml_cropped['block'].iloc[0]].index)
            utils.save_log_strg(f"сделали drop с блока {df_ml_cropped['block'].iloc[0]} для df_ml, теперь его shape={df_ml.shape}", address=self.address, block=self.last_block)

            df_ml = pd.concat([df_ml, df_ml_cropped])
            utils.save_log_strg(f"соединили df_ml и df_ml_cropped, теперь суммарный shape={df_ml.shape}", address=self.address, block=self.last_block)
            utils.save_log_strg(df_ml, address=self.address, block=self.last_block)
        else:
            utils.save_log_strg(f"df_ml не найден, создаю новый", address=self.address, block=self.last_block)
            start_time = time.time()
            df_ml = self.create_features(dataframe, pair=self.address)

            utils.save_log_strg(f"created new df_ml in {(time.time() - start_time):.2f}s (shape={df_ml.shape}):", address=self.address, block=self.last_block)
            utils.save_log_strg(df_ml, address=self.address, block=self.last_block)


        if df_ml is None or df_ml.empty:
            utils.save_log_strg(f"df_ml.empty => return", address=self.address, block=self.last_block)
            return pd.DataFrame()

        df_ml.loc[:, '%-candle-i'] = range(len(df_ml))
        df_ml.loc[:, '%-token_age'] = (df_ml['block'] - df_ml['block'].iloc[0]) * 12 / 60
        df_ml.loc[:, '%-token_is_old'] = 1 if np.isnan(dataframe['open'].iloc[0]) else 0


        if dataframe['block'].iloc[-2] != df_ml['block'].iloc[-2]:
            utils.save_log_strg(f"не совпадает dataframe['block'].iloc[-2] == df_ml['block'].iloc[-2], {dataframe['block'].iloc[-2]}!={df_ml['block'].iloc[-2]}, dataframe.shape={dataframe.shape}, df_ml.shape={df_ml.shape}", address=self.address, block=self.last_block)
            assert 0, f"не совпадает dataframe['block'].iloc[-2] == df_ml['block'].iloc[-2], {dataframe['block'].iloc[-2]}!={df_ml['block'].iloc[-2]}, dataframe.shape={dataframe.shape}, df_ml.shape={df_ml.shape}"



        predict_proba = list(self.model.predict_proba(df_ml[self.model.feature_names_]))
        for i, pp in enumerate(predict_proba):
            predict_proba[i] = pp[1]


        utils.save_log_strg(f"pred len={len(predict_proba)}, dataframe.shape={dataframe.shape}", address=self.address, block=self.last_block)

        dataframe['predict'] = predict_proba


        df_ml.to_csv(f"{save_path}/{self.address}.csv", index=False)


        dataframe['volume_buy_std'] = dataframe['volume_buy'].rolling(6).std()
        dataframe['volume_sell_std'] = dataframe['volume_sell'].rolling(6).std()

        dataframe['trades'] = dataframe['trades_buy'] + dataframe['trades_sell']

        dataframe['trades_diff'] = (dataframe['trades_buy'] - dataframe['trades_sell']).rolling(7).sum()

        dataframe.reset_index(drop=True, inplace=True)

        dataframe['rsi'] = ta.RSI(dataframe, timeperiod=10)

        if 'i' not in dataframe.columns: dataframe['i'] = range(len(dataframe))

        
        dataframe['time_b'] = (dataframe['timestamp'] - dataframe['timestamp'].shift(1)) / 1


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




        mult_ = 1
        macd = ta.MACD(dataframe, fastperiod=int(self.buy_macd_fastperiod.value*mult_), slowperiod=int(self.buy_macd_fastperiod.value+self.buy_macd_slowperiod.value)*mult_, signalperiod=int(self.buy_macd_signalperiod.value*mult_))
        dataframe['macd'] = macd['macd']
        dataframe['macdsignal'] = macd['macdsignal']
        dataframe['macdhist'] = macd['macdhist']


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
        dataframe['trans_value_usd_per_token_unit'] = None
        dataframe['trans_block_number'] = None


        _cumulative_delta = []
        _same_color = []
        cc = -1
        i_day = -1
        lasts_used = []


        # (!) тут надо поправить, если где-то важно учитывать как давно была покупка
        if self.last_db_trade and self.last_db_trade['strategy'] in ['MlMatt','MlCatboost'] and self.last_db_trade['backtest'] == 1 and self.last_db_trade['side'] == 'b':
            dataframe.loc[dataframe.index[0], ['enter_long', 'enter_tag']] = (1, f"db_{self.last_db_trade['tag']}")


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
                renounce_ownership = True


            if self.last_db_trade and self.last_db_trade['strategy'] in ['MlMatt','MlCatboost'] and self.last_db_trade['backtest'] == 1 and self.last_db_trade['trans_block_number'] == row['block'] and self.last_db_trade['side'] == 'b':

                dataframe.loc[row['i'], ['enter_long', 'enter_tag', 'trans_value_usd_per_token_unit', 'trans_block_number']] = (1, f"db_{self.last_db_trade['tag']}", row['close'], row['block'])
     


            if row['high'] == None:
                continue


            if row['high'] > max_price:
                max_price = row['high']



            if any([x['liquidity'] >= 200000 for x in dfn[i - 15:i]]):
                continue


            # bollinger
            if 0:

                if renounce_ownership:

                    if 1 or row['high'] > row['long_trend']:

                        if i - self.buy_params['buy_delta_ema_lookback_boll'] >= 0 and not all([dfn[i - ii]['delta_ema'] <= 0 for ii in range(0, self.buy_params['buy_delta_ema_lookback_boll'])]):

                            if row['bb_lowerband'] >= row['close'] and row['time_b'] < 70:
                                dataframe.loc[row['i'], 'enter_long'] = 1

            
            # prediction_matt
            if 0:
                if row['prediction_matt'] == 1:
                    dataframe.loc[row['i'], ['enter_long', 'enter_tag']] = (1, 'prediction_matt')

                    if row['block'] == self.last_block:
                        utils.log_strategy_trades(f"for address={self.address} last_block={self.last_block} prediction_matt, row: {row}")

            # prediction
            if 1:

                # renounce_ownership or
                if wait_long or (renounce_ownership or i > 100) and row['predict'] > 0.60:
                    if 1 or any([x['rsi'] < 38 for x in dfn[i - 5:i + 1]]) and row['rsi'] >= dfn[i - 1]['rsi'] and row['rsi'] >= dfn[i - 2]['rsi']:
                        dataframe.loc[row['i'], ['enter_long', 'trans_value_usd_per_token_unit', 'trans_block_number']] = (1, row['close'], row['block'])
                        wait_long = False
                        if row['block'] == self.last_block:
                            utils.save_log_strg(f"BUY! pred={row['predict']:.2f}% block={row['block']}", address=self.address, block=self.last_block)
                    else:
                        wait_long = True
                        if row['block'] == self.last_block:
                            utils.save_log_strg(f"BUY! pred={row['predict']:.2f}% block={row['block']}, but wait_long", address=self.address, block=self.last_block)





        return dataframe
    

    def populate_exit_trend(self, dataframe: DataFrame) -> DataFrame:

        dfn = dataframe.to_dict(orient="records")
        wait_exit_short = False
        wait_exit_long = False
        enter_i = None
        exit_on_positive = False
        exit_on_red_macd = False
        exit_on_trades_diff = False
        exit_on_delta_ema = False
        for i, row in enumerate(dfn):
            
            if i < 3: continue
            
            if not wait_exit_long and 'enter_long' in dfn[i] and dfn[i]['enter_long'] == 1:
                wait_exit_long = True
                enter_i = i
                max_profit_was = 0
                min_profit_was = 0
                exit_on_positive = False
                exit_on_red = False
                exit_on_trades_diff = False
                exit_on_delta_ema = False
                exit_on_red_macd = False
                continue

            if wait_exit_long:
                profit = 100 * dfn[i]['close'] / dfn[enter_i]['trans_value_usd_per_token_unit'] - 100
                highest_price_was = min([x['high'] for x in dfn[i - enter_i:i + 1]])
                max_profit_was = max(max_profit_was, profit)
                min_profit_was = min(min_profit_was, profit)

                if profit <= -1 * abs(self.stoploss) * 100:
                    dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'stop_loss')
                    wait_exit_long = False
                
                if profit > 0 and exit_on_positive:
                    dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'exit_on_positive')
                    wait_exit_long = False

                if any([x['predict'] > 0.50 for x in dfn[i - 10:i + 1]]):
                    continue

                # rsi
                if 1:

                    if row['rsi'] <= 55 and profit > 50:

                        dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'rsi')
                        wait_exit_long = False


                if 1:
                    if exit_on_red_macd:
                        if profit > 30 and row['macdhist'] < 0:
                            dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'macdhist')
                            wait_exit_long = False
                            exit_on_red_macd = False

                    if i - self.sell_macd_after_candles.value > 0 and all([x['macdhist'] >= 0 for x in dfn[i-self.sell_macd_after_candles.value:i+1]]):
                        exit_on_red_macd = True

                if 1:
                    if exit_on_trades_diff:
                        if profit > 0 and row['trades_diff'] <= 0:
                            dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'trades_diff')
                            wait_exit_long = False
                            exit_on_trades_diff = False

                    if all([x['trades_diff'] > 0 for x in dfn[i-12:i+1]]):
                        exit_on_trades_diff = True


                    if exit_on_delta_ema:
                        if profit > 0 and row['delta_ema'] <= 0:
                            dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'delta_ema')
                            wait_exit_long = False
                            exit_on_delta_ema = False

                if 1:
                    #print(f" i={i} max_profit_was={max_profit_was:.2f}% profit={profit:.2f}%")
                    if max_profit_was - profit > 50:
                        dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'trailing_stop')
                        wait_exit_long = False

                # roi
                if 1:
                    if profit >= 400:
                        dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'roi')
                        wait_exit_long = False
                    
                    if (row['block'] - dfn[enter_i]['trans_block_number']) * 12 / 60 >= 600:
                        dataframe.loc[row['i'], ['exit_long', 'exit_tag']] = (1, 'roi_600')
                        wait_exit_long = False


        return dataframe
    
    

