
import json
import traceback
from pprint import pprint
import numpy as np
import pandas as pd
import statistics
import time
from datetime import datetime
from os import listdir
from os.path import isfile, join
from math import floor, log10, inf
from ast import literal_eval 
from multiprocessing import Pool

import talib.abstract as ta
import pandas_ta as pta
from technical.indicators import atr_percent
import freqtrade.vendor.qtpylib.indicators as qtpylib

# import optuna

# from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error, mean_absolute_error, r2_score, explained_variance_score
from sklearn.metrics import log_loss, roc_auc_score, accuracy_score, classification_report, confusion_matrix, recall_score
# # from sklearn.neighbors import KNeighborsClassifier
# # from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier, ExtraTreesClassifier
# # from sklearn.svm import SVC
# # from sklearn.tree import DecisionTreeClassifier

from catboost import CatBoostClassifier, CatBoostRegressor


import utils
from utils import FILES_DIR, FILE_LABELS, WETH, USDT, USDC



path_stats = f"{FILES_DIR}/temp/defi_candles"
path_df_ready = f"{FILES_DIR}/temp/defi_candles_ml"
label_period_candles = 60



"""
признаки добавить

- holders_total сколько всего холдеров, надо добавить признак сколько осталось холдеров с балансом
"""



def num_zeros(decimal):
    return inf if decimal == 0 else -floor(log10(abs(decimal))) - 1

def make_lags(df, features, steps):
    for f in features:
        for lag in range(1, steps + 1):
            col = f'{f}_lag{lag}'
            df[col] = df[f].shift(lag)
    return df

def make_lags_range(df, features: list, _type: str, _range: int):
    values_f = {}
    output_f = {}

    for f in features:
        values_f[f] = df[f].values
        output_f[f] = []

    if _type == 'candles':

        for i in range(len(df)):

            for f in features:
                assert type(values_f[f][i]) == str or np.isnan(values_f[f][i]), f"{f}"
                if type(values_f[f][i]) == str:
                    res = eval(values_f[f][i])
                    assert type(res) == list, f"{res} {type(res)}"
                elif type(values_f[f][i]) == list:
                    res = values_f[f][i]
                else:
                    res = []

                if i >= 1:
                    for z in range(1, _range):
                        if i - z < 0: break
                        if type(values_f[f][i - z]) == str:
                            _var = eval(values_f[f][i - z])
                            assert type(_var) == list, f"{_var} {type(_var)}"
                            res += _var

                output_f[f].append(res)

    
    if _type == 'minutes':

        time_ = df['timestamp'].values

        for i in range(len(df)):

            for f in features:
                assert type(values_f[f][i]) == str or np.isnan(values_f[f][i]), f"{f}"
                if type(values_f[f][i]) == str:
                    res = eval(values_f[f][i])
                    assert type(res) == list, f"{res} {type(res)}"
                elif type(values_f[f][i]) == list:
                    res = values_f[f][i]
                else:
                    res = []

                if i >= 1:
                    for z in range(1, i + 1):
                        if (time_[i] - time_[i - z]) > _range * 60:
                            break
                        if type(values_f[f][i - z]) == str:
                            _var = eval(values_f[f][i - z])
                            assert type(_var) == list, f"{_var} {type(_var)}"
                            res += _var

                output_f[f].append(res)
        

    df = pd.concat([df] +
        [pd.DataFrame({f'{f}_[{_type}][{_range}]': output_f[f] }) for f in features]
    , axis=1)


    return df







def makeline(df, period=14, src='ema', column='close'):

    if column == 'hl2': masrc = pta.hl2(df['high'], df['low'])
    elif column == 'hlc3': masrc =  pta.hlc3(df['high'], df['low'], df['close'])
    else: masrc = df[column]

    if src not in ['mama', 'fama']:
        period = int(period)
    else:
        while period > 3:
            period /= 2
        if period < 1: period = 1.0

    
    if src == 'sma':
        col = ta.SMA(masrc, timeperiod=period)
    if src == 'ema':
        col = ta.EMA(masrc, timeperiod=period)
    if src == 'dema':
        col = ta.DEMA(masrc, timeperiod=period)
    if src == 'tema':
        col = ta.TEMA(masrc, timeperiod=period)
    if src == 'zema':
        col = zema(df, period=period)
    if src == 'wma':
        col = ta.WMA(df, timeperiod=period)
    if src == 'vwma':
        col = vwma(df, period)
    if src == 'vwap':
        col = qtpylib.rolling_vwap(df, window=period)
    if src == 'vidya':
        while period > 80:
            period = int(period * .33)
        col = VIDYA(df, length=period)
    if src == 'hwma':
        col = pta.hwma(masrc, na=0.2, nb=0.1, nc=0.1)
    if src == 'kama':
        col = pta.kama(masrc, length=period, fast=2, slow=30)
    if src == 'mcgd':
        col = pta.mcgd(masrc, length=period, c=1.0)
    if src == 'rma':
        col = pta.rma(masrc, length=period)
    if src == 'sinwma':
        col = pta.sinwma(masrc, length=period)
    if src == 'hilo':
        hilo = pta.hilo(df['high'], df['low'], df['close'], high_length=period, low_length=int(period*1.8), mamode='sma')
        col = hilo[f'HILO_{period}_{int(period*1.8)}']
    #----------------
    if src == 'alma':
        col = pta.alma(masrc, length=period, sigma=6.0, distribution_offset=0.85)
    if src == 'hma':
        col = pta.hma(masrc, length=period)
    if src == 'jma':
        col = pta.jma(masrc, length=period, phase=0.0)
    if src == 'linreg':
        col = pta.linreg(masrc, length=period)
    if src == 'ssf':
        col = pta.ssf(masrc, length=period, poles=2)
    if src == 'swma':
        col = pta.swma(masrc, length=period, asc=True)
    if src == 'trima':
        col = pta.trima(masrc, length=period)
    if src == 'fwma':
        col = pta.fwma(masrc, length=period, asc=True)
    if src == 'mama':
        mama, fama = ta.MAMA(masrc, fastlimit=0.5/period, slowlimit=0.05/period)
        col = mama
    if src == 'fama':
        mama, fama = ta.MAMA(masrc, fastlimit=0.5/period, slowlimit=0.05/period)
        col = fama


    return col



def calc_reward(df):

    dataframe = df.copy()
    if 'i' not in dataframe.columns: dataframe['i'] = range(len(dataframe))

    dfn = dataframe.to_dict(orient="records")
    for i, row in enumerate(dfn):


        #li = list(map(lambda r: r['trailingsl_uptrend'], dfn[i - past:i]))
        N = label_period_candles
        #N_find_min = 45
        profit_perc = 80
        perc_can_fall = -40
        #percent_for_zero_reward = None # 0.8
        #j = 0
        high_ = None
        # return_positive_reward = False
        # return_zero_reward = False
        return_reward = None

        if i + N >= len(dfn):
            dataframe.loc[row['i'], 'reward'] = np.nan
            continue


        if 1:
            buy_candles_lookback_prepare = 4
            if (row['timestamp'] - dfn[i - buy_candles_lookback_prepare]['timestamp']) / buy_candles_lookback_prepare  >= 2500:
                dataframe.loc[i, 'reward'] = np.nan
                continue

        j = 1
        max_profit = 0
        min_profit = 0
        hold_min_profit = False

        while j < N:
            # if dfn[i + j]['close'] < row['open']:
            #     if return_positive_reward:
            #         dataframe.loc[row['i'], 'reward'] = round(100 * high_ / row['open'] - 100, 4)
            #         break
            #     if return_zero_reward:
            #         dataframe.loc[row['i'], 'reward'] = 0
            #         break

            #     li = list(map(lambda r: r['low'], dfn[i + j:min(i + j + N_find_min,len(dfn))]))
            #     dataframe.loc[row['i'], 'reward'] = round(100 * min(li) / row['open'] - 100, 4)
            #     break

            buy_candles_lookback_prepare = 4
            if (dfn[i + j]['timestamp'] - dfn[i + j - buy_candles_lookback_prepare]['timestamp']) / buy_candles_lookback_prepare  >= 2500:
                return_reward = 'nan'
                break
        
            if not high_ or high_ != None and dfn[i + j]['high'] > high_: high_ = dfn[i + j]['high']
            # if 100 * dfn[i + j]['high'] / row['open'] - 100 >= percent:
            #     return_positive_reward = True
            # elif percent_for_zero_reward != None and 100 * dfn[i + j]['high'] / row['open'] - 100 >= percent_for_zero_reward:
            #     return_zero_reward = True
            
            # if j + 1 == N:
            #     if return_positive_reward:
            #         dataframe.loc[row['i'], 'reward'] = round(100 * high_ / row['open'] - 100, 4)
            #     elif return_zero_reward:
            #         dataframe.loc[row['i'], 'reward'] = 0
            #     else:
            #         dataframe.loc[row['i'], 'reward'] = 0

            # if dfn[i + j]['high'] > max_high: max_high = dfn[i + j]['high']
            # if dfn[i + j]['low'] < min_low: min_low = dfn[i + j]['low']

            current_profit = 100 * dfn[i + j]['high'] / row['close'] - 100 
            if current_profit > max_profit: max_profit = current_profit
            if current_profit < min_profit and not hold_min_profit: min_profit = current_profit

            if return_reward == 'negative' and current_profit >= profit_perc:
                hold_min_profit = True

            if return_reward == None and current_profit >= profit_perc:
                return_reward = 'positive'

            if return_reward == None and current_profit <= perc_can_fall:
                return_reward = 'negative'


            j += 1

        if return_reward == 'positive': dataframe.loc[row['i'], 'reward'] = round(max_profit,2)
        elif return_reward == 'negative': dataframe.loc[row['i'], 'reward'] = round(min_profit,2)
        elif return_reward == 'nan': dataframe.loc[row['i'], 'reward'] = np.nan
        else: dataframe.loc[row['i'], 'reward'] = round(max_profit,2)



    return dataframe['reward']



def create_features(file):



    df = pd.read_csv(f"{path_stats}/{file}", header=0)



    if len(df) < 200:
        print(f"skip {file} len={len(df)}<200")
        return pd.DataFrame()

    if len(df) > 10000:
        print(f"skip {file} len={len(df)}>10000")
        return pd.DataFrame()

    if np.isnan(df['open'].iloc[0]):
        print(f"skip {file} df['open'].iloc[0] = None")
        return pd.DataFrame()

    # if df['timestamp'].iloc[0] <= pd.Timestamp('2024-09-15 00:00:00').timestamp():
    #     return

    print(f"----------- loaded {file} -----------")


    # df = df.iloc[:600]


    print(df)

    df = df[sorted(df.columns.to_list())]
    #print(df.info(True))
    #print(df.columns.to_list())


    df.drop(['renounce_ownership', 'mev_bot_addresses', 'mev_volumes', 'buy_addresses', 'sell_addresses'], axis=1, inplace=True)


    df['mev_profits'] = df['mev_profits'].apply(lambda row: len(eval(row)))
    df['sell_amount_from_previous_balance_perc_abs'] = df['sell_amount_from_previous_balance_perc'].apply(lambda row: str(list(map(abs, eval(row)))))



    # stats_erc20_buy_addresses
    # stats_erc20_sell_addresses
    # stats_profit_window=10
    # stats_profit_window=70
    # stats_profit_window=99999
    
    cols_stats_erc20 = []
    cols_other = []
    for col in df.columns.to_list():
        if 'stats_erc20_buy_addresses' in col or 'stats_erc20_sell_addresses' in col:
            cols_stats_erc20.append(col)
        if 'stats_erc20_buy_addresses' not in col \
            and 'stats_erc20_sell_addresses' not in col \
            and 'stats_profit_window=10' not in col \
            and 'stats_profit_window=70' not in col \
            and 'stats_profit_window=99999' not in col:
            cols_other.append(col)
        if 'stats_profit_window=10' in col \
            or 'stats_profit_window=70' in col \
            or 'stats_profit_window=99999' in col:
            df.rename(columns={col:f'%-{col}'}, inplace=True)



    cols_range = ['buy_trans_indexes','sell_trans_indexes','transfer_trans_indexes','jaredfromsubway','repeated_buys_volumes','repeated_sells_volumes','numbers_of_repeated_buys','numbers_of_repeated_sells','invested_list','withdrew_list','amount_left_from_max_balance_who_sold_perc','sell_amount_from_previous_balance_perc','sell_amount_from_previous_balance_perc_abs']
    cols_other = list(set(cols_other) - set(cols_range))

    # print('cols_stats_erc20:')
    # print(cols_stats_erc20)
    # print('cols_range:')
    # print(cols_range)
    # print('cols_other:')
    # print(cols_other)


    #print(df[['block', 'timestamp', 'close', 'amount_left_from_max_balance_who_sold_perc', 'stats_profit_window=10|pnl_neg_sum', 'stats_erc20_sell_addresses|internal_to', 'stats_profit_window=99999|realized_profit_pos_sum']])
    
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')

    # df = df[['block', 'timestamp', 'datetime', 'close', 'stats_erc20_sell_addresses|internal_to']]
    # print(df[['block', 'timestamp', 'datetime', 'close', 'stats_erc20_sell_addresses|internal_to']])

    # df = make_lags(df, ['close'], steps=3)

    # df = make_lags_range(df, ['stats_erc20_sell_addresses|internal_to'], _type='minutes', _range=2)



    """
    делать lags_range на [] для:

    buy_trans_indexes
    sell_trans_indexes
    transfer_trans_indexes
    jaredfromsubway
    repeated_buys_volumes
    repeated_sells_volumes
    numbers_of_repeated_buys
    numbers_of_repeated_sells
    invested_list
    withdrew_list
    amount_left_from_max_balance_who_sold_perc
    sell_amount_from_previous_balance_perc
    sell_amount_from_previous_balance_perc_abs
    stats_erc20_buy_addresses
    stats_erc20_sell_addresses
    """

    #print(df[cols_other])
    #print(df[['sell_amount_from_previous_balance_perc', 'sell_amount_from_previous_balance_perc_abs']].to_string())



    dfn = df[['close']].to_dict(orient="records")
    last_valid_value = None
    for i, row in enumerate(dfn):
        if i == 0: 
            last_valid_value = row['close']
            continue
        if 100 - 100 * row['close'] / last_valid_value > 99.999:
            dfn[i]['close'] = None
        else: last_valid_value = row['close']

    df.reset_index(drop=True, inplace=True)
    df['close_2'] = pd.DataFrame(dfn)['close'].interpolate(method='linear', axis=0)
    df.drop(['close'], axis=1, inplace=True)
    df.rename(columns={'close_2': 'close'}, inplace=True)


    _num_zeros = num_zeros(df['close'].iloc[0])
    if not np.isnan(df.at[0, 'open']):
        df.at[0, 'open'] *= 10 ** _num_zeros
    df['close'] *= 10 ** _num_zeros


    _open_0 = df.at[0, 'open']
    df['open'] = df['close'].shift(1)
    df.at[0, 'open'] = _open_0
    df['low'] = df[['open','close']].min(axis=1)
    df['high'] = df[['open','close']].max(axis=1)
    df['volume'] = df['invested'] + df['withdrew']


    df['%-candle-i'] = range(len(df))
    df[f'%-token_age'] = (df['timestamp'] - df['timestamp'].iloc[0]) / 60
    for t in [1, 5, 15, 30, 50, 150]:
        df[f'%-time_b-{t}'] = (df['timestamp'] - df['timestamp'].shift(t)) / t
    df['%-token_is_old'] = 1 if np.isnan(df['open'].iloc[0]) else 0




    df = make_lags_range(df, cols_range + cols_stats_erc20, _type='candles', _range=5)
    df = make_lags_range(df, cols_range + cols_stats_erc20, _type='minutes', _range=15)

    for key in cols_range + cols_stats_erc20:
        for lag in ['[candles][5]', '[minutes][15]']:
            df = pd.concat([
                df,
                pd.DataFrame({f'%-{key}_{lag}_sum': df[f'{key}_{lag}'].apply(lambda row: sum(row) if row else np.nan)}),
                pd.DataFrame({f'%-{key}_{lag}_max': df[f'{key}_{lag}'].apply(lambda row: max(row) if row else np.nan)}),
                pd.DataFrame({f'%-{key}_{lag}_min': df[f'{key}_{lag}'].apply(lambda row: min(row) if row else np.nan)}),
                pd.DataFrame({f'%-{key}_{lag}_std': df[f'{key}_{lag}'].apply(lambda row: statistics.pstdev(row) if row else np.nan)}),
                pd.DataFrame({f'%-{key}_{lag}_median': df[f'{key}_{lag}'].apply(lambda row: statistics.median(row) if row else np.nan)})
            ], axis=1)
            df = df.drop(f'{key}_{lag}', axis=1)

    # print(df.columns.to_list())
    #print(df[['sell_amount_from_previous_balance_perc', 'sell_amount_from_previous_balance_perc_[candles][5]', 'sell_amount_from_previous_balance_perc_abs_[candles][5]']].tail(100))
    #exit()

    df = df.drop(cols_range + cols_stats_erc20, axis=1)


    

    df = pd.concat([df,
        pd.DataFrame({f'%-volume': df['invested'] + df['withdrew'] }),
        pd.DataFrame({f'%-volume_delta': df['invested'] - df['withdrew'] }),
        pd.DataFrame({f'%-volume_div': df['invested'] / (df['withdrew'] + df['invested'])}),

        # pd.DataFrame({f'%-volume_buy_sum_5': df['%-volume_buy'].rolling(5).sum() }),
        # pd.DataFrame({f'%-volume_buy_sum_30': df['%-volume_buy'].rolling(30).sum() }),
        # pd.DataFrame({f'%-volume_sell_sum_5': df['%-volume_sell'].rolling(5).sum() }),
        # pd.DataFrame({f'%-volume_sell_sum_30': df['%-volume_sell'].rolling(30).sum() }),
    ], axis=1)

    df = pd.concat([df,
        pd.DataFrame({f'%-volume_delta_sum_5': df['%-volume_delta'].rolling(5).sum() }),
        pd.DataFrame({f'%-volume_delta_sum_30': df['%-volume_delta'].rolling(30).sum() }),
        pd.DataFrame({f'%-volume_delta_std_30': df['%-volume_delta'].rolling(30).std() }),
        pd.DataFrame({f'%-volume_div_sum_20': df['%-volume_div'].rolling(20).sum() }),
        pd.DataFrame({f'%-volume_div_max_20': df['%-volume_div'].rolling(20).max() }),
        pd.DataFrame({f'%-volume_div_min_20': df['%-volume_div'].rolling(20).min() }),
        pd.DataFrame({f'%-volume_div_median_20': df['%-volume_div'].rolling(20).median() }),
        pd.DataFrame({f'%-volume_div_std_40': df['%-volume_div'].rolling(40).std() }),
    ], axis=1)

    df = pd.concat([df] +
        [pd.DataFrame({f'%-addresses_that_sold_to_zero_value_sum_{p}': df['addresses_that_sold_to_zero_value'].rolling(p).sum() }) for p in [5, 15, 40, 110]] +
        [pd.DataFrame({f'%-erc20_to_1_sum_{p}': df['erc20_to_1'].rolling(p).sum() }) for p in [5, 15, 40, 110]] +
        [pd.DataFrame({f'%-holders_new_sum_{p}': df['holders_new'].rolling(p).sum() }) for p in [5, 15, 40, 110]] +
        [pd.DataFrame({f'%-holders_new_minus_erc20_to_1_sum_{p}': df['holders_new_minus_erc20_to_1'].rolling(p).sum() }) for p in [5, 15, 40, 110]] +
        [pd.DataFrame({f'%-mev_profits_sum_{p}': df['mev_profits'].rolling(p).sum() }) for p in [5, 15, 40, 110]] +
        [pd.DataFrame({f'%-numbers_of_repeated_buys_count_sum_{p}': df['numbers_of_repeated_buys_count'].rolling(p).sum() }) for p in [5, 15, 40, 110]] +
        [pd.DataFrame({f'%-numbers_of_repeated_sells_count_sum_{p}': df['numbers_of_repeated_sells_count'].rolling(p).sum() }) for p in [5, 15, 40, 110]]
    , axis=1)
    df = df.drop(['erc20_to_1', 'holders_new', 'holders_new_minus_erc20_to_1', 'addresses_that_sold_to_zero_value', 'mev_profits', 'numbers_of_repeated_buys_count', 'numbers_of_repeated_sells_count'], axis=1)


    # (!) тут invested_list надо применить
    # for key in ['invested', 'withdrew']:
    #     df[f'%-{key}_std'] = df['invested'].rolling(6).std()
    for wi in [3, 7, 15, 30]:
        df = pd.concat([df,
            pd.DataFrame({f'%-invested_sum_{wi}': df['invested'].rolling(wi).sum() }),
            pd.DataFrame({f'%-withdrew_sum_{wi}': df['withdrew'].rolling(wi).sum() }),
        ], axis=1)

        df = pd.concat([df,
            pd.DataFrame({f'%-invested_minus_withdrew_sum_{wi}': df[f'%-invested_sum_{wi}'] - df[f'%-withdrew_sum_{wi}'] }),
            pd.DataFrame({f'%-invested_div_withdrew_sum_{wi}': df[f'%-invested_sum_{wi}'] / (df[f'%-invested_sum_{wi}'] + df[f'%-withdrew_sum_{wi}']) }),
        ], axis=1)


    for wi in [3, 10, 20, 40]:
        df = pd.concat([df,
            pd.DataFrame({f'%-trades_buy_sum_{wi}': df['trades_buy'].rolling(wi).sum() }),
            pd.DataFrame({f'%-trades_sell_sum_{wi}': df['trades_sell'].rolling(wi).sum() }),
            pd.DataFrame({f'%-trades_transfer_sum_{wi}': df['trades_transfer'].rolling(wi).sum() }),
        ], axis=1)

        df = pd.concat([df,
            pd.DataFrame({f'%-trades_sum_{wi}': (df['trades_buy'] + df['trades_sell']).rolling(wi).sum() }),
            pd.DataFrame({f'%-trades_delta_{wi}': df[f'%-trades_buy_sum_{wi}'] - df[f'%-trades_sell_sum_{wi}'] }),
            pd.DataFrame({f'%-trades_div_{wi}': df[f'%-trades_buy_sum_{wi}'] / (df[f'%-trades_buy_sum_{wi}'] + df[f'%-trades_sell_sum_{wi}']) }),
        ], axis=1)

    df = pd.concat([df,
        pd.DataFrame({f'%-trades_buy': df['trades_buy'] }),
        pd.DataFrame({f'%-trades_sell': df['trades_sell'] }),
    ], axis=1)
    df = df.drop(['trades_buy','trades_sell','trades_transfer'], axis=1)


    # --------------------------------------

    # technical indicators
    


    df = pd.concat([df] +
        [pd.DataFrame({f'%-rsi_{period}': ta.RSI(df, timeperiod=period) }) for period in [14, 30]] +
        [pd.DataFrame({f'%-atr_perc_{period}': atr_percent(df, period=period) }) for period in [4, 12]] +
        [pd.DataFrame({f'%-delta_ema_{period}': makeline(df, period=period, src='ema', column=f'invested') - makeline(df, period=period, src='ema', column=f'withdrew') }) for period in [7, 16, 30]] +
        [pd.DataFrame({f'%-roc_{period}': ta.ROCP(df, timeperiod=period) }) for period in [3, 7, 12, 18, 25, 35, 50, 100]] +
        [pd.DataFrame({f'%-willr_{period}': ta.WILLR(df, timeperiod=period) }) for period in [14, 14*6]] +
        [pd.DataFrame({f'%-cmf_{period}': pta.cmf(df['high'], df['low'], df['close'], df['volume'], length=period) }) for period in [18, 21*2]] +
        [pd.DataFrame({f'%-mfi_{period}': ta.MFI(df, timeperiod=period) }) for period in [7, 14, 25]] +
        [pd.DataFrame({f'%-cci_{period}': ta.CCI(df, timeperiod=period) }) for period in [14, 25]]
    , axis=1)


    for mult in [1, 3]:
        macd = ta.MACD(df, fastperiod=12*mult, slowperiod=26*mult, signalperiod=9*mult)
        df = pd.concat([df,
            pd.DataFrame({f'%-macd_{mult}': macd['macd'] }),
            pd.DataFrame({f'%-macdsignal_{mult}': macd['macdsignal'] }),
            pd.DataFrame({f'%-macdhist_{mult}': macd['macdhist'] }),
        ], axis=1)
            
    for period in [1, 5]:
        df = pd.concat([df,
            pd.DataFrame({f'%-adx_{period}': ta.ADX(df, timeperiod=14*period) }),
            pd.DataFrame({f'%-plus_di_{period}': ta.PLUS_DI(df, timeperiod=14*period) }),
            pd.DataFrame({f'%-minus_di_{period}': ta.MINUS_DI(df, timeperiod=14*period) }),
        ], axis=1)

    for mult in [3, 7]:
        stoch_rsi = pta.stochrsi(df['close'], length=14*mult, rsi_length=14*mult, k=3*3, d=3*mult)
        df = pd.concat([df,
            pd.DataFrame({f'%-stochd_rsi_{mult}': stoch_rsi[f'STOCHRSId_{14*mult}_{14*mult}_9_{3*mult}'] }),
            pd.DataFrame({f'%-stochk_rs_{mult}': stoch_rsi[f'STOCHRSIk_{14*mult}_{14*mult}_9_{3*mult}'] }),
        ], axis=1)

    for period in [3, 7]:
        stoch_rsi = ta.STOCHRSI(df, timeperiod=14*period, fastk_period=5*period, fastd_period=3*period, fastd_matype=0)
        df = pd.concat([df,
            pd.DataFrame({f'%-ta_stochd_rsi_{period}': stoch_rsi['fastd'] }),
            pd.DataFrame({f'%-ta_stochk_rsi_{period}': stoch_rsi['fastk'] }),
        ], axis=1)
    
    for period in [2, 5]:
        # trading view params: stoch = ta.STOCH(df, fastk_period=14, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
        stoch = ta.STOCH(df, fastk_period=5*period, slowk_period=3*period, slowk_matype=0, slowd_period=3*period, slowd_matype=0)
        df = pd.concat([df,
            pd.DataFrame({f'%-stoch_slowd_{period}': stoch['slowd'] }),
            pd.DataFrame({f'%-stoch_slowk_{period}': stoch['slowk'] }),
        ], axis=1)


    for period in [3]:
        stoch_fast = ta.STOCHF(df, fastk_period=5*period, fastd_period=3*period, fastd_matype=0)
        df = pd.concat([df,
            pd.DataFrame({f'%-stoch_fastd_{period}': stoch_fast['fastd'] }),
            pd.DataFrame({f'%-stoch_fastk_{period}': stoch_fast['fastk'] }),
        ], axis=1)

    for period in [1, 4]:
        aroon = ta.AROON(df, timeperiod=14*period)
        df = pd.concat([df,
            pd.DataFrame({f'%-aroonup_{period}': aroon['aroonup'] }),
            pd.DataFrame({f'%-aroondown_{period}': aroon['aroondown'] }),
        ], axis=1)

    df = pd.concat([df,
        pd.DataFrame({f'%-uo': ta.ULTOSC(df, timeperiod1=7, timeperiod2=14, timeperiod3=28) }),
    ], axis=1)


    bollinger = qtpylib.bollinger_bands(qtpylib.typical_price(df), window=20, stds=3)
    bb = pd.DataFrame({
        'bb_lowerband': bollinger['lower'],
        'bb_middleband': bollinger['mid'],
        'bb_upband': bollinger['upper'],
    })
    bb["close"] = df['close']
    bb["bb_percent"] = (
        (bb["close"] - bb["bb_lowerband"]) /
        (bb["bb_upband"] - bb["bb_lowerband"])
    )
    bb["bb_width"] = (
        (bb["bb_upband"] - bb["bb_lowerband"]) / bb["bb_middleband"]
    )
    df = pd.concat([df,
        pd.DataFrame({f'%-bb_percent': bb["bb_percent"] }),
        pd.DataFrame({f'%-bb_width': bb["bb_width"] }),
        pd.DataFrame({f'%-bb_signal': np.select([(bb['bb_lowerband'] >= bb['close'])], [1], default=0) }),
    ], axis=1)


    # BRAR
    brar = pta.brar(df['open'], df['high'], df['low'], df['close'], length=26, scalar=100.0)
    df = pd.concat([df,
        pd.DataFrame({'%-AR': brar['AR_26'] }),
        pd.DataFrame({'%-BR': brar['BR_26'] }),
        pd.DataFrame({'%-BR_diff': brar['AR_26'] - brar['BR_26'] }),
    ], axis=1)


    # KST Oscillator
    # kst = pta.kst(df['close'], roc1=10, roc2=15, roc3=20, roc4=30, sma1=10, sma2=10, sma3=10, sma4=15, signal=9)
    # df['%-KST'] = kst['KST_10_15_20_30_10_10_10_15']
    # df['%-KSTs'] = kst['KSTs_9']
    # df = pd.concat([df,
    #     pd.DataFrame({'%-KST': kst['KST_10_15_20_30_10_10_10_15'] }),
    #     pd.DataFrame({'%-KSTs': kst['KSTs_9'] }),
    # ], axis=1)

    # Percentage Price Oscillator
    ppo = pta.ppo(df['close'], fast=12, slow=26, signal=9, scalar=100.0)
    df = pd.concat([df,
        pd.DataFrame({'%-PPO': ppo['PPO_12_26_9'] }),
        pd.DataFrame({'%-PPOh': ppo['PPOh_12_26_9'] }),
        pd.DataFrame({'%-PPOs': ppo['PPOs_12_26_9'] }),
    ], axis=1)


    # Percentage Volume Oscillator
    fast = 12*5
    slow = 26*5
    signal = 9*5
    pvo = pta.pvo(df['volume'], fast=fast, slow=slow, signal=signal, scalar=100.0)
    df = pd.concat([df,
        pd.DataFrame({'%-PVO': pvo[f'PVO_{fast}_{slow}_{signal}'] }),
        pd.DataFrame({'%-PVOh': -1 * pvo[f'PVOh_{fast}_{slow}_{signal}'] }),
        pd.DataFrame({'%-PVOs': pvo[f'PVOs_{fast}_{slow}_{signal}'] }),
    ], axis=1)

    # # Quantitative Qualitative Estimation
    # qqe = pta.qqe(df['close'], length=14*3, smooth=5, factor=4.236)
    # df = pd.concat([df,
    #     pd.DataFrame({'%-QQE': qqe['QQE_42_5_4.236'] }),
    #     pd.DataFrame({'%-QQE_RSIMA': qqe['QQE_42_5_4.236_RSIMA'] }),
    #     pd.DataFrame({'%-QQEl': qqe['QQEl_42_5_4.236'].fillna(0) }),
    #     pd.DataFrame({'%-QQEsd': qqe['QQEs_42_5_4.236'].fillna(0) }),
    # ], axis=1)


    stc = pta.stc(df['close'], tclength=10, fast=12, slow=26, factor=0.5)
    df = pd.concat([df,
        pd.DataFrame({'%-STC': stc['STC_10_12_26_0.5'] }),
        pd.DataFrame({'%-STCmacd': stc['STCmacd_10_12_26_0.5'] }),
        pd.DataFrame({'%-STCstoch': stc['STCstoch_10_12_26_0.5'] }),
    ], axis=1)

    # Archer Moving Averages Trends
    amat = pta.amat(df['close'], fast=8, slow=21, lookback=2)
    df = pd.concat([df,
        pd.DataFrame({'%-AMATe_LR': amat['AMATe_LR_8_21_2'] }),
        pd.DataFrame({'%-AMATe_SR': amat['AMATe_SR_8_21_2'] }),
    ], axis=1)

    cksp = pta.cksp(df['high'], df['low'], df['close'], p=10)
    df = pd.concat([df,
        pd.DataFrame({'%-CKSPl': cksp['CKSPl_10_3_20'] }),
        pd.DataFrame({'%-CKSPs': cksp['CKSPs_10_3_20'] }),
    ], axis=1)


    df = pd.concat([df,
        pd.DataFrame({'%-ADOSC': ta.ADOSC(df, fastperiod=3*7, slowperiod=10*7) }),
        pd.DataFrame({'%-bias': pta.bias(df['close'], length=35) }),
        pd.DataFrame({'%-bop': pta.bop(df['open'], df['high'], df['low'], df['close'], scalar=3.0) }),
        pd.DataFrame({'%-cfo': pta.cfo(df['close'], length=9, scalar=100.0) }),
        pd.DataFrame({'%-cg': pta.cg(df['close'], length=10) }),
        pd.DataFrame({'%-cmo': pta.cmo(df['close'], length=14, scalar=100.0) }),
        pd.DataFrame({'%-coppock': pta.coppock(df['close'], length=10, fast=11, slow=14) }),
        pd.DataFrame({'%-cti': pta.cti(df['close'], length=12*2) }),
        pd.DataFrame({'%-mom': pta.mom(df['close'], length=10) }),
        pd.DataFrame({'%-pgo': pta.pgo(df['high'], df['low'], df['close'], length=14) }),
        pd.DataFrame({'%-psl': [np.nan] * len(df) }), # (!) это потом удалить
        pd.DataFrame({'%-rsx': pta.rsx(df['close'], length=14) }),
        pd.DataFrame({'%-dpo': pta.dpo(df['close'], length=20, lookahead=False) }),
        pd.DataFrame({'%-adosc': pta.adosc(df['high'], df['low'], df['close'], df['volume'], fast=3, slow=10) }),
        pd.DataFrame({'%-OBV': pta.aobv(df['close'], df['volume'], fast=4, slow=12)['OBV'] }),
        pd.DataFrame({'%-efi': pta.efi(df['close'], df['volume'], length=13) }),
    ], axis=1)


    # --------------------------------------


    df.rename(columns={'invested':'%-invested','withdrew':'%-withdrew'}, inplace=True)


    df = pd.concat([
        df,
        pd.DataFrame({'%-total_vb_minus_total_vs': df['total_volume_buy'] - df['total_volume_sell']}),
        pd.DataFrame({'%-total_vb_div_total_vs': df['total_volume_buy'] / (df['total_volume_buy'] + df['total_volume_sell'])}),
        pd.DataFrame({'%-total_tb_minus_total_ts': df['total_trades_buy'] - df['total_trades_sell']}),
        pd.DataFrame({'%-total_tb_div_total_ts': df['total_trades_buy'] / (df['total_trades_buy'] + df['total_trades_sell'])}),
        pd.DataFrame({'%-total_inv_minus_total_with': df['total_invested'] - df['total_withdrew']}),
        pd.DataFrame({'%-total_inv_div_total_with': df['total_invested'] / (df['total_invested'] + df['total_withdrew'])}),
        pd.DataFrame({'%-total_holders_avg_invested': df['total_invested'] / df['holders_total']}),
        pd.DataFrame({'%-total_holders_avg_withdrew': df['total_withdrew'] / df['holders_total']}),
    ], axis=1)
    df = pd.concat([
        df,
        pd.DataFrame({'%-total_holders_avg_inv_min_with': df['%-total_holders_avg_invested'] - df['%-total_holders_avg_withdrew']}),
        pd.DataFrame({'%-total_holders_avg_inv_div_with': df['%-total_holders_avg_invested'] / (df['%-total_holders_avg_invested'] + df['%-total_holders_avg_withdrew'])}),
    ], axis=1)

    df.rename(columns={
        'total_volume_buy':'%-total_volume_buy',
        'total_volume_sell':'%-total_volume_sell',
        'total_volume_transfer':'%-total_volume_transfer',
        'total_trades_buy':'%-total_trades_buy',
        'total_trades_sell':'%-total_trades_sell',
        'total_trades_transfer':'%-total_trades_transfer',
        'total_invested':'%-total_invested',
        'total_withdrew':'%-total_withdrew',
        'holders_total':'%-holders_total',
        'liquidity':'%-liquidity',
    }, inplace=True)




    cols_left = []
    for col in df.columns.to_list():
        if not col.startswith('%-'): cols_left.append(col)

    # print('cols_left:')
    # print(cols_left)

    # label_period_candles = 60

    # df["&-close"] = (
    #     df["close"]
    #     .rolling(label_period_candles)
    #     .mean()
    #     .shift(-label_period_candles)
    #     / df["close"]
    #     - 1
    # )

    df['&-reward'] = calc_reward(df)




    df = df[list(set(df.columns.to_list()) - set(cols_left)) + ['block']]

    # for col in df.columns:
    #     a = df.to_numpy() # df.values (pandas<0.24)
    #     if (a[0] == a).all():
    #         exit(col)
    # print(df['%-transfer_trans_indexes_[candles][5]_sum'].to_string())
    # print(df['%-transfer_trans_indexes_[candles][5]_sum'].info())
    # print(type(df['%-transfer_trans_indexes_[candles][5]_sum'].iloc[0]))


    for col in ['%-dpo', '%-atr_perc_12', '%-atr_perc_4', '%-pgo', '%-cfo', '%-bb_percent', '%-bias']:
        if df[col].dtype == object:
            df[col] = df[col].astype(float)

    object_columns = df.select_dtypes(include=['object']).columns
    assert not list(object_columns), f"{file} {object_columns}"


    # df = df.iloc[150:-label_period_candles]



    print(f'final df (shape={df.shape}) for {file}:')
    print(df)
    #print(df['%-invested'])
    #print(df.info())

    # assert df.shape[1] == 466, f"{file} {df.shape}"


    # assert not df['&-close'].isnull().values.any(), f"{file} nan={df['&-close'].isnull().values.sum()}, len={len(df)}"



    equal_cols = ['%-stats_erc20_sell_addresses|eth_sent_internal_median_[candles][5]_std', '%-mev_profits_sum_40', '%-stats_erc20_buy_addresses|eth_sent_internal_sum_[candles][5]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_sum_[minutes][15]_median', '%-stats_erc20_sell_addresses|eth_sent_external_min_[minutes][15]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_sum_[minutes][15]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_sum_[candles][5]_min', '%-stats_erc20_sell_addresses|internal_from_[minutes][15]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_min_[minutes][15]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_sum_[candles][5]_min', '%-stats_erc20_sell_addresses|eth_sent_external_min_[candles][5]_max', '%-stats_erc20_buy_addresses|eth_sent_internal_avg_[minutes][15]_median', '%-stats_erc20_buy_addresses|eth_sent_internal_std_[candles][5]_median', '%-stats_erc20_buy_addresses|eth_sent_internal_avg_[candles][5]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_median_[minutes][15]_max', '%-transfer_trans_indexes_[candles][5]_median', '%-stats_erc20_buy_addresses|eth_sent_internal_avg_[candles][5]_sum', '%-stats_erc20_buy_addresses|erc1155_from_[candles][5]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_std_[minutes][15]_min', '%-stats_erc20_sell_addresses|eth_sent_internal_sum_[candles][5]_median', '%-stats_erc20_buy_addresses|eth_sent_internal_std_[minutes][15]_max', '%-stats_erc20_buy_addresses|eth_sent_internal_median_[minutes][15]_std', '%-stats_erc20_sell_addresses|eth_sent_internal_std_[candles][5]_median', '%-stats_erc20_buy_addresses|internal_from_[minutes][15]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_min_[minutes][15]_max', '%-stats_erc20_sell_addresses|internal_from_[minutes][15]_max', '%-stats_erc20_buy_addresses|internal_from_[candles][5]_median', '%-stats_erc20_buy_addresses|internal_from_div_uniq_trans_[candles][5]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_min_[minutes][15]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_max_[minutes][15]_min', '%-stats_erc20_buy_addresses|internal_from_div_uniq_trans_[candles][5]_std', '%-stats_erc20_sell_addresses|eth_sent_internal_median_[candles][5]_max', '%-stats_erc20_buy_addresses|eth_sent_internal_median_[candles][5]_std', '%-stats_erc20_sell_addresses|internal_from_[candles][5]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_min_[minutes][15]_min', '%-stats_erc20_sell_addresses|internal_from_[minutes][15]_median', '%-stats_profit_window=70|realized_profit_neg_median', '%-stats_erc20_sell_addresses|eth_sent_internal_sum_[candles][5]_std', '%-stats_erc20_sell_addresses|internal_from_[candles][5]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_avg_[candles][5]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_max_[minutes][15]_max', '%-mev_profits_sum_5', '%-stats_erc20_buy_addresses|eth_sent_internal_min_[minutes][15]_max', '%-token_is_old', '%-stats_erc20_buy_addresses|eth_sent_internal_avg_[candles][5]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_sum_[minutes][15]_min', '%-stats_erc20_buy_addresses|internal_from_[minutes][15]_sum', '%-stats_erc20_sell_addresses|internal_from_[candles][5]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_std_[minutes][15]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_median_[candles][5]_sum', '%-stats_erc20_sell_addresses|eth_sent_internal_min_[candles][5]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_max_[candles][5]_median', '%-mev_profits_sum_15', '%-stats_erc20_buy_addresses|eth_sent_internal_min_[candles][5]_std', '%-stats_erc20_sell_addresses|eth_total_min_[candles][5]_std', '%-stats_erc20_sell_addresses|eth_sent_internal_sum_[minutes][15]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_median_[candles][5]_min', '%-stats_erc20_buy_addresses|internal_from_[candles][5]_sum', '%-stats_erc20_buy_addresses|internal_from_div_uniq_trans_[minutes][15]_min', '%-stats_erc20_sell_addresses|internal_from_div_uniq_trans_[minutes][15]_std', '%-stats_erc20_sell_addresses|eth_sent_internal_max_[candles][5]_sum', '%-stats_erc20_sell_addresses|eth_sent_internal_min_[candles][5]_median', '%-stats_erc20_buy_addresses|eth_sent_internal_avg_[candles][5]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_max_[candles][5]_std', '%-stats_erc20_sell_addresses|eth_sent_internal_median_[candles][5]_min', '%-stats_erc20_buy_addresses|internal_from_div_uniq_trans_[minutes][15]_std', '%-stats_erc20_sell_addresses|eth_total_min_[minutes][15]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_avg_[minutes][15]_sum', '%-transfer_trans_indexes_[candles][5]_std', '%-stats_erc20_sell_addresses|eth_sent_internal_avg_[minutes][15]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_avg_[candles][5]_max', '%-stats_erc20_sell_addresses|eth_total_min_[candles][5]_median', '%-stats_erc20_buy_addresses|eth_sent_internal_sum_[candles][5]_max', '%-stats_erc20_buy_addresses|internal_from_div_uniq_trans_[minutes][15]_median', '%-stats_erc20_sell_addresses|eth_total_min_[candles][5]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_median_[minutes][15]_sum', '%-stats_erc20_sell_addresses|eth_sent_internal_std_[minutes][15]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_median_[minutes][15]_sum', '%-stats_erc20_sell_addresses|eth_sent_external_min_[minutes][15]_median', '%-stats_erc20_sell_addresses|internal_from_div_uniq_trans_[candles][5]_median', '%-stats_erc20_buy_addresses|eth_sent_internal_avg_[minutes][15]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_min_[candles][5]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_sum_[minutes][15]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_avg_[minutes][15]_std', '%-stats_profit_window=70|roi_r_neg_median', '%-stats_erc20_sell_addresses|internal_from_div_uniq_trans_[minutes][15]_min', '%-stats_erc20_buy_addresses|internal_from_[minutes][15]_min', '%-transfer_trans_indexes_[minutes][15]_median', '%-stats_erc20_buy_addresses|eth_sent_internal_max_[minutes][15]_median', '%-stats_erc20_sell_addresses|internal_from_div_uniq_trans_[candles][5]_max', '%-transfer_trans_indexes_[minutes][15]_max', '%-transfer_trans_indexes_[minutes][15]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_max_[minutes][15]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_min_[minutes][15]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_max_[candles][5]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_std_[minutes][15]_sum', '%-stats_erc20_sell_addresses|internal_from_[minutes][15]_std', '%-stats_erc20_sell_addresses|internal_from_div_uniq_trans_[candles][5]_std', '%-stats_erc20_sell_addresses|internal_from_[candles][5]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_std_[minutes][15]_median', '%-stats_erc20_sell_addresses|eth_sent_external_min_[minutes][15]_std', '%-stats_erc20_sell_addresses|eth_total_min_[minutes][15]_min', '%-transfer_trans_indexes_[minutes][15]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_max_[candles][5]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_std_[minutes][15]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_std_[candles][5]_min', '%-stats_erc20_sell_addresses|internal_from_[candles][5]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_std_[candles][5]_max', '%-stats_erc20_buy_addresses|eth_sent_internal_sum_[minutes][15]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_median_[minutes][15]_std', '%-stats_erc20_sell_addresses|eth_sent_internal_std_[minutes][15]_std', '%-stats_erc20_sell_addresses|eth_sent_internal_avg_[candles][5]_std', '%-stats_erc20_sell_addresses|eth_sent_external_min_[minutes][15]_sum', '%-stats_erc20_buy_addresses|internal_from_div_uniq_trans_[minutes][15]_max', '%-stats_erc20_buy_addresses|internal_from_[minutes][15]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_max_[minutes][15]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_avg_[minutes][15]_min', '%-stats_erc20_buy_addresses|internal_from_[minutes][15]_std', '%-stats_erc20_sell_addresses|internal_from_div_uniq_trans_[candles][5]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_min_[candles][5]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_avg_[candles][5]_min', '%-stats_erc20_sell_addresses|eth_sent_internal_max_[minutes][15]_sum', '%-transfer_trans_indexes_[candles][5]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_median_[candles][5]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_median_[candles][5]_max', '%-stats_erc20_sell_addresses|internal_from_div_uniq_trans_[minutes][15]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_min_[minutes][15]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_sum_[minutes][15]_min', '%-stats_profit_window=99999|realized_profit_neg_median', '%-stats_erc20_sell_addresses|eth_total_min_[candles][5]_min', '%-stats_profit_window=99999|roi_r_neg_median', '%-volume_div_max_20', '%-stats_erc20_sell_addresses|eth_sent_internal_avg_[minutes][15]_median', '%-stats_erc20_sell_addresses|internal_from_[minutes][15]_min', '%-transfer_trans_indexes_[minutes][15]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_max_[candles][5]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_sum_[candles][5]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_median_[candles][5]_median', '%-transfer_trans_indexes_[candles][5]_max', '%-BR_diff', '%-stats_erc20_buy_addresses|eth_sent_internal_min_[candles][5]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_sum_[candles][5]_sum', '%-stats_erc20_buy_addresses|internal_from_[candles][5]_std', '%-stats_erc20_sell_addresses|eth_sent_internal_std_[minutes][15]_median', '%-stats_erc20_buy_addresses|internal_from_div_uniq_trans_[candles][5]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_median_[minutes][15]_min', '%-stats_erc20_sell_addresses|eth_sent_external_min_[candles][5]_sum', '%-stats_erc20_buy_addresses|internal_from_div_uniq_trans_[candles][5]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_min_[minutes][15]_sum', '%-stats_erc20_sell_addresses|eth_sent_internal_max_[minutes][15]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_max_[candles][5]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_min_[candles][5]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_min_[minutes][15]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_min_[candles][5]_min', '%-stats_erc20_sell_addresses|eth_sent_internal_std_[candles][5]_sum', '%-stats_erc20_sell_addresses|eth_sent_internal_median_[minutes][15]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_min_[candles][5]_min', '%-stats_erc20_sell_addresses|internal_from_div_uniq_trans_[minutes][15]_sum', '%-stats_erc20_buy_addresses|internal_from_[candles][5]_min', '%-stats_erc20_sell_addresses|eth_sent_internal_avg_[minutes][15]_sum', '%-stats_erc20_sell_addresses|eth_sent_internal_median_[minutes][15]_min', '%-stats_erc20_buy_addresses|internal_from_[candles][5]_max', '%-stats_erc20_sell_addresses|eth_total_min_[minutes][15]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_sum_[minutes][15]_sum', '%-stats_erc20_sell_addresses|eth_total_min_[minutes][15]_sum', '%-stats_erc20_sell_addresses|internal_from_div_uniq_trans_[candles][5]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_sum_[minutes][15]_sum', '%-stats_erc20_buy_addresses|internal_from_div_uniq_trans_[candles][5]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_std_[candles][5]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_std_[candles][5]_std', '%-stats_erc20_sell_addresses|eth_sent_internal_max_[candles][5]_min', '%-total_trades_transfer', '%-stats_erc20_buy_addresses|eth_sent_internal_max_[minutes][15]_min', '%-stats_erc20_sell_addresses|eth_sent_internal_avg_[candles][5]_sum', '%-stats_erc20_buy_addresses|eth_sent_internal_sum_[minutes][15]_median', '%-stats_erc20_buy_addresses|eth_sent_internal_sum_[candles][5]_std', '%-stats_erc20_sell_addresses|eth_sent_external_min_[candles][5]_min', '%-stats_erc20_buy_addresses|eth_sent_internal_std_[minutes][15]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_avg_[minutes][15]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_max_[candles][5]_median', '%-stats_erc20_sell_addresses|eth_total_min_[candles][5]_sum', '%-stats_erc20_sell_addresses|eth_sent_internal_sum_[candles][5]_max', '%-stats_erc20_buy_addresses|eth_sent_internal_std_[candles][5]_sum', '%-mev_profits_sum_110', '%-stats_erc20_sell_addresses|eth_total_min_[minutes][15]_max', '%-stats_erc20_buy_addresses|eth_sent_internal_max_[minutes][15]_sum', '%-stats_erc20_sell_addresses|eth_sent_internal_median_[candles][5]_median', '%-stats_erc20_sell_addresses|eth_sent_internal_min_[minutes][15]_sum', '%-transfer_trans_indexes_[candles][5]_sum', '%-stats_erc20_sell_addresses|eth_sent_external_min_[minutes][15]_min', '%-stats_erc20_sell_addresses|eth_sent_internal_avg_[minutes][15]_std', '%-stats_erc20_sell_addresses|eth_sent_external_min_[candles][5]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_max_[minutes][15]_max', '%-stats_erc20_sell_addresses|eth_sent_external_min_[candles][5]_median', '%-stats_erc20_buy_addresses|eth_sent_internal_median_[minutes][15]_max', '%-stats_erc20_sell_addresses|eth_sent_internal_std_[candles][5]_max', '%-stats_erc20_buy_addresses|internal_from_div_uniq_trans_[minutes][15]_sum', '%-stats_erc20_sell_addresses|eth_sent_internal_min_[candles][5]_sum', '%-stats_erc20_sell_addresses|eth_sent_internal_std_[candles][5]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_avg_[candles][5]_min', '%-stats_erc20_sell_addresses|eth_sent_internal_max_[candles][5]_std', '%-stats_erc20_buy_addresses|eth_sent_internal_median_[minutes][15]_median', '%-stats_erc20_sell_addresses|internal_from_div_uniq_trans_[minutes][15]_max']
    df = df[list(set(df.columns.to_list()) - set(equal_cols))]


    df = df[['block','&-reward','%-trades_buy','%-trades_sell','%-bb_percent','%-bb_width','%-bb_signal','%-liquidity','%-total_volume_transfer','%-candle-i','%-stats_profit_window=99999|roi_r_neg_count','%-stats_profit_window=99999|pnl_pos_std','%-total_tb_div_total_ts','%-adx_5','%-total_trades_sell','%-stats_profit_window=99999|minutes_hold_std_window=500','%-stats_profit_window=99999|roi_t_neg_median','%-holders_new_minus_erc20_to_1_sum_110','%-stats_profit_window=99999|pnl_neg_median','%-total_trades_buy','%-stats_profit_window=99999|roi_u_pos_std','%-stats_profit_window=99999|roi_r_neg_std','%-time_b-150','%-stats_profit_window=99999|roi_r_pos_std','%-stats_profit_window=99999|roi_r_pos_count','%-stats_profit_window=70|pnl_neg_std','%-stats_profit_window=99999|unrealized_profit_pos_sum','%-stats_profit_window=99999|realized_profit_neg_std','%-total_holders_avg_inv_div_with','%-atr_perc_12','%-stats_profit_window=99999|roi_r_neg_sum','%-total_tb_minus_total_ts','%-stats_profit_window=99999|roi_r_pos_median','%-numbers_of_repeated_sells_count_sum_110','%-stats_profit_window=99999|realized_profit_pos_median','%-total_holders_avg_withdrew','%-stats_profit_window=99999|roi_t_neg_std','%-stats_profit_window=99999|roi_t_pos_std','%-stats_profit_window=99999|roi_u_neg_sum','%-PVOs','%-numbers_of_repeated_buys_count_sum_110','%-stats_profit_window=99999|minutes_hold_sum_window=60','%-stats_profit_window=99999|realized_profit_pos_std','%-stats_profit_window=99999|roi_u_neg_std','%-stats_profit_window=70|realized_profit_neg_std','%-roc_100','%-stats_profit_window=99999|roi_t_pos_sum','%-stochd_rsi_7','%-CKSPs','%-total_vb_div_total_vs','%-stats_profit_window=70|roi_r_pos_median','%-stats_profit_window=99999|minutes_hold_median_window=500','%-stats_profit_window=99999|roi_u_pos_sum','%-stats_profit_window=70|roi_r_pos_std','%-stats_profit_window=99999|unrealized_profit_neg_median','%-stats_profit_window=99999|minutes_hold_sum_window=500','%-stats_profit_window=99999|unrealized_profit_pos_median','%-time_b-50','%-roc_50','%-stats_profit_window=99999|minutes_hold_median_window=60','%-stats_profit_window=70|realized_profit_neg_sum','%-stats_profit_window=99999|roi_u_neg_median','%-holders_total','%-stats_profit_window=70|roi_r_pos_sum','%-token_age','%-stats_profit_window=99999|realized_profit_pos_sum','%-stats_profit_window=70|roi_u_neg_std','%-macdsignal_3','%-total_volume_buy','%-total_withdrew','%-stats_profit_window=99999|pnl_pos_sum','%-stats_profit_window=99999|realized_profit_neg_sum','%-stats_profit_window=70|realized_profit_pos_median','%-stats_profit_window=70|roi_r_neg_sum','%-CKSPl','%-holders_new_sum_110','%-stats_profit_window=70|pnl_pos_std','%-stats_profit_window=70|roi_t_neg_median','%-stats_profit_window=99999|roi_u_neg_count','%-stats_profit_window=99999|roi_t_neg_count','%-stats_profit_window=70|unrealized_profit_neg_std','%-stats_profit_window=99999|unrealized_profit_pos_std','%-stochk_rs_7','%-aroondown_4','%-stats_profit_window=70|unrealized_profit_pos_std','%-stats_profit_window=99999|roi_u_pos_count','%-stats_profit_window=99999|minutes_hold_std_window=60','%-invested_sum_30','%-stats_profit_window=70|realized_profit_pos_std','%-stats_profit_window=70|pnl_pos_sum','%-numbers_of_repeated_sells_[candles][5]_max','%-numbers_of_repeated_sells_[minutes][15]_max','%-erc20_to_1_sum_110','%-stats_profit_window=99999|roi_t_pos_count','%-stats_profit_window=99999|roi_u_pos_median','%-stats_profit_window=99999|roi_r_pos_sum','%-holders_new_sum_40','%-stats_profit_window=70|roi_r_neg_std','%-numbers_of_repeated_sells_[minutes][15]_median','%-adx_1','%-stats_erc20_sell_addresses|eth_total_avg_[candles][5]_median','%-addresses_that_sold_to_zero_value_sum_110','%-stats_profit_window=70|realized_profit_pos_sum','%-erc20_to_1_sum_40','%-time_b-30','%-total_vb_minus_total_vs','%-stats_profit_window=70|roi_u_pos_sum','%-volume_delta_std_30','%-stats_profit_window=99999|pnl_pos_median','%-macdhist_3','%-stats_erc20_sell_addresses|erc1155_to_[minutes][15]_max','%-PVOh','%-coppock','%-stats_profit_window=99999|roi_t_neg_sum','%-stats_profit_window=70|roi_r_neg_count','%-stats_erc20_sell_addresses|first_in_transaction_block_[candles][5]_max','%-stats_erc20_sell_addresses|erc20_from_div_uniq_trans_[candles][5]_std','%-total_volume_sell','%-stats_profit_window=70|pnl_neg_sum','%-numbers_of_repeated_sells_[candles][5]_min','%-STCstoch','%-stats_erc20_sell_addresses|external_from_[candles][5]_max','%-stats_profit_window=70|roi_t_pos_count','%-time_b-15','%-stats_profit_window=70|pnl_pos_median','%-stats_profit_window=70|roi_u_pos_std','%-stats_profit_window=70|unrealized_profit_neg_sum','%-stats_erc20_sell_addresses|first_in_transaction_block_[minutes][15]_max','%-stats_profit_window=70|roi_t_neg_std','%-stats_profit_window=99999|roi_t_pos_median','%-trades_div_40','%-stats_profit_window=70|roi_t_neg_sum','%-minus_di_5','%-STC','%-stats_erc20_buy_addresses|unique_received_from_addresses_div_uniq_trans_[minutes][15]_min','%-stats_erc20_buy_addresses|first_in_transaction_block_[candles][5]_max','%-ta_stochd_rsi_7','%-stats_profit_window=70|roi_t_pos_median','%-total_inv_div_total_with','%-stats_profit_window=70|roi_u_neg_median','%-numbers_of_repeated_buys_count_sum_40','%-plus_di_5','%-addresses_that_sold_to_zero_value_sum_40','%-stats_erc20_sell_addresses|erc20_eth_send_min_[candles][5]_max','%-roc_7','%-bias','%-volume_div_std_40','%-stats_profit_window=70|roi_r_pos_count','%-stats_erc20_sell_addresses|eth_total_std_[candles][5]_min','%-roc_35','%-trades_transfer_sum_40','%-cmf_42','%-holders_new_minus_erc20_to_1_sum_40','%-stats_erc20_buy_addresses|erc20_from_[minutes][15]_max','%-stats_erc20_buy_addresses|first_in_transaction_block_[minutes][15]_min','%-numbers_of_repeated_sells_count_sum_40','%-numbers_of_repeated_sells_[minutes][15]_sum','%-dpo','%-macd_3','%-stats_erc20_buy_addresses|erc20_hold_time_block_median_[minutes][15]_std','%-stats_profit_window=70|roi_t_pos_sum','%-stats_erc20_buy_addresses|erc20_hold_time_block_min_[candles][5]_max','%-macdsignal_1','%-stats_erc20_sell_addresses|erc20_eth_received_std_[minutes][15]_median','%-stats_profit_window=70|unrealized_profit_neg_median','%-stats_erc20_buy_addresses|erc20_value_usd_median_[candles][5]_max','%-numbers_of_repeated_sells_[minutes][15]_std','%-stats_erc20_buy_addresses|maestro_trans_perc_[minutes][15]_max','%-stats_erc20_buy_addresses|unique_transactions_[minutes][15]_max','%-roc_18','%-stats_profit_window=70|roi_t_pos_std','%-PVO','%-buy_trans_indexes_[minutes][15]_max','%-stats_erc20_sell_addresses|maestro_trans_perc_[minutes][15]_max','%-stats_profit_window=70|pnl_neg_median','%-macdhist_1','%-trades_sum_40','%-withdrew_sum_30','%-invested_list_[minutes][15]_std','%-roc_12','%-stats_erc20_buy_addresses|erc20_eth_received_std_[minutes][15]_sum','%-stats_erc20_buy_addresses|maestro_trans_perc_[minutes][15]_std','%-stats_erc20_sell_addresses|balance_eth_[minutes][15]_min','%-stats_erc20_sell_addresses|erc20_hold_time_block_median_[minutes][15]_std','%-stats_erc20_sell_addresses|avg_blocks_per_trans_[minutes][15]_std','%-stats_erc20_buy_addresses|unique_received_from_addresses_[minutes][15]_max','%-stats_erc20_buy_addresses|maestro_trans_[candles][5]_max','%-stats_erc20_buy_addresses|eth_total_median_[minutes][15]_max','%-stats_profit_window=70|roi_u_pos_median','%-stats_erc20_buy_addresses|erc20_from_div_uniq_trans_[candles][5]_min','%-stats_erc20_buy_addresses|erc20_from_div_uniq_trans_[minutes][15]_max','%-stats_erc20_buy_addresses|internal_to_div_uniq_trans_[minutes][15]_max','%-stats_erc20_buy_addresses|unique_received_from_addresses_div_uniq_trans_[minutes][15]_std','%-PPOh','%-stats_erc20_sell_addresses|erc20_hold_time_block_sum_[minutes][15]_std','%-buy_trans_indexes_[minutes][15]_std','%-numbers_of_repeated_buys_[candles][5]_std','%-stats_erc20_buy_addresses|internal_to_[minutes][15]_max','%-stats_erc20_buy_addresses|eth_total_avg_[minutes][15]_sum','%-withdrew_list_[minutes][15]_std','%-stats_erc20_buy_addresses|erc721_to_[minutes][15]_std','%-numbers_of_repeated_buys_[candles][5]_min','%-stats_profit_window=70|unrealized_profit_pos_sum','%-stats_erc20_sell_addresses|erc20_value_usd_avg_[minutes][15]_min','%-stats_erc20_sell_addresses|internal_to_div_uniq_trans_[candles][5]_max','%-numbers_of_repeated_buys_[minutes][15]_min','%-stats_erc20_sell_addresses|nonce_[minutes][15]_max','%-sell_trans_indexes_[minutes][15]_max','%-stats_erc20_sell_addresses|banana_trans_perc_[minutes][15]_max','%-stats_profit_window=70|roi_u_neg_sum','%-stats_erc20_sell_addresses|diff_between_first_and_last_[candles][5]_max','%-stats_erc20_sell_addresses|erc20_eth_received_min_[minutes][15]_std','%-roc_3','%-stats_erc20_buy_addresses|first_in_transaction_block_[minutes][15]_max','%-stats_erc20_sell_addresses|eth_sent_external_median_[minutes][15]_std','%-stats_erc20_sell_addresses|internal_to_[minutes][15]_max','%-stats_erc20_buy_addresses|eth_total_avg_[minutes][15]_min','%-stats_erc20_buy_addresses|banana_trans_[minutes][15]_max','%-roc_25','%-stats_erc20_sell_addresses|eth_received_external_min_[minutes][15]_max','%-stats_erc20_buy_addresses|nonce_[candles][5]_std','%-numbers_of_repeated_buys_[minutes][15]_median','%-stats_erc20_sell_addresses|erc20_eth_send_sum_[minutes][15]_std','%-buy_trans_indexes_[candles][5]_min','%-stats_erc20_buy_addresses|diff_between_first_and_last_[candles][5]_max','%-stats_erc20_sell_addresses|erc20_hold_time_block_avg_[candles][5]_max','%-stats_erc20_buy_addresses|eth_received_external_median_[candles][5]_max','%-stats_erc20_sell_addresses|erc20_hold_time_block_avg_[minutes][15]_min','%-stats_erc20_buy_addresses|maestro_trans_perc_[minutes][15]_sum','%-stats_erc20_buy_addresses|avg_blocks_per_trans_[minutes][15]_median','%-cfo','%-stats_erc20_buy_addresses|eth_sent_external_min_[minutes][15]_std','%-stats_erc20_buy_addresses|unique_received_from_addresses_div_uniq_trans_[candles][5]_min','%-stats_erc20_buy_addresses|maestro_trans_[minutes][15]_sum','%-stats_erc20_sell_addresses|erc20_hold_time_block_sum_[minutes][15]_median','%-stats_erc20_buy_addresses|eth_received_internal_min_[candles][5]_max','%-stoch_slowd_5','%-jaredfromsubway_[minutes][15]_std','%-stats_erc20_sell_addresses|diff_between_first_and_last_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_hold_time_block_sum_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_hold_time_block_sum_[minutes][15]_max','%-PPOs','%-stats_erc20_buy_addresses|unique_received_to_addresses_div_uniq_trans_[minutes][15]_min','%-atr_perc_4','%-stats_erc20_buy_addresses|erc1155_from_[minutes][15]_max','%-stats_erc20_buy_addresses|eth_sent_external_median_[candles][5]_max','%-stats_erc20_sell_addresses|balance_usd_[minutes][15]_sum','%-minus_di_1','%-stats_erc20_sell_addresses|erc20_bought_n_not_sold_perc_[minutes][15]_median','%-erc20_to_1_sum_15','%-stats_erc20_buy_addresses|external_to_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_hold_time_block_max_[minutes][15]_std','%-stats_erc20_sell_addresses|eth_total_median_[minutes][15]_max','%-stats_erc20_buy_addresses|eth_received_external_std_[candles][5]_max','%-stats_erc20_sell_addresses|external_to_[minutes][15]_max','%-stats_erc20_sell_addresses|unique_received_to_addresses_div_uniq_trans_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_hold_time_block_median_[minutes][15]_sum','%-stats_erc20_buy_addresses|avg_blocks_per_trans_[candles][5]_min','%-stats_profit_window=10|realized_profit_neg_sum','%-stats_erc20_sell_addresses|eth_received_internal_median_[candles][5]_min','%-stats_erc20_buy_addresses|unique_received_from_addresses_[minutes][15]_sum','%-stats_erc20_sell_addresses|erc20_hold_time_block_max_[minutes][15]_max','%-stats_erc20_sell_addresses|eth_sent_external_avg_[minutes][15]_min','%-stats_erc20_buy_addresses|balance_eth_[candles][5]_max','%-stats_erc20_buy_addresses|internal_to_div_uniq_trans_[candles][5]_max','%-stats_erc20_buy_addresses|unique_received_from_addresses_div_uniq_trans_[candles][5]_median','%-stats_erc20_sell_addresses|balance_usd_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_eth_received_avg_[candles][5]_std','%-PPO','%-stats_erc20_sell_addresses|banana_trans_[minutes][15]_max','%-stats_erc20_buy_addresses|maestro_trans_[minutes][15]_max','%-cg','%-stats_erc20_buy_addresses|erc20_uniq_addresses_send_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_eth_received_min_[minutes][15]_median','%-stats_erc20_buy_addresses|eth_received_internal_avg_[candles][5]_min','%-stats_erc20_buy_addresses|avg_blocks_per_trans_[minutes][15]_sum','%-stats_erc20_buy_addresses|maestro_trans_perc_[candles][5]_sum','%-stats_profit_window=10|roi_r_pos_std','%-stats_profit_window=70|roi_u_pos_count','%-stats_erc20_buy_addresses|erc20_value_usd_min_[candles][5]_min','%-stats_erc20_buy_addresses|eth_received_external_sum_[minutes][15]_sum','%-volume_div_sum_20','%-numbers_of_repeated_sells_[candles][5]_sum','%-stats_erc20_sell_addresses|erc721_to_[minutes][15]_sum','%-stats_erc20_buy_addresses|eth_received_external_std_[minutes][15]_max','%-stats_erc20_sell_addresses|first_in_transaction_block_[minutes][15]_min','%-stats_erc20_buy_addresses|internal_to_div_uniq_trans_[minutes][15]_min','%-stats_erc20_buy_addresses|erc20_value_usd_max_[candles][5]_min','%-stats_erc20_sell_addresses|external_from_[candles][5]_std','%-stats_erc20_sell_addresses|erc20_eth_send_max_[candles][5]_std','%-stats_erc20_sell_addresses|erc20_eth_received_avg_[candles][5]_median','%-stats_erc20_sell_addresses|external_to_[minutes][15]_std','%-sell_amount_from_previous_balance_perc_[candles][5]_std','%-cti','%-numbers_of_repeated_sells_count_sum_15','%-stats_erc20_sell_addresses|erc20_bought_n_not_sold_perc_[minutes][15]_sum','%-stats_erc20_sell_addresses|eth_received_internal_min_[minutes][15]_std','%-stats_profit_window=70|roi_t_neg_count','%-stats_erc20_buy_addresses|erc20_hold_time_block_std_[minutes][15]_median','%-stats_erc20_buy_addresses|erc20_eth_send_sum_[minutes][15]_max','%-sell_trans_indexes_[minutes][15]_std','%-stats_erc20_sell_addresses|eth_sent_external_std_[minutes][15]_median','%-stats_erc20_sell_addresses|erc20_to_div_uniq_trans_[minutes][15]_max','%-stats_erc20_sell_addresses|eth_total_max_[minutes][15]_std','%-withdrew_list_[candles][5]_max','%-stats_erc20_buy_addresses|external_from_div_uniq_trans_[minutes][15]_min','%-stats_erc20_buy_addresses|eth_received_external_max_[minutes][15]_median','%-stats_erc20_sell_addresses|eth_received_internal_median_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_to_div_uniq_trans_[candles][5]_max','%-stats_erc20_buy_addresses|diff_between_first_and_last_[candles][5]_min','%-macd_1','%-trades_transfer_sum_20','%-stats_erc20_buy_addresses|erc721_to_[minutes][15]_max','%-stats_erc20_buy_addresses|eth_received_external_median_[minutes][15]_std','%-stats_erc20_sell_addresses|balance_usd_[minutes][15]_max','%-holders_new_minus_erc20_to_1_sum_15','%-numbers_of_repeated_buys_count_sum_15','%-stats_erc20_buy_addresses|diff_between_first_and_last_[minutes][15]_max','%-stats_erc20_buy_addresses|banana_trans_perc_[minutes][15]_max','%-stats_erc20_buy_addresses|eth_received_internal_avg_[minutes][15]_median','%-trades_delta_40','%-stats_profit_window=10|roi_u_neg_sum','%-repeated_buys_volumes_[minutes][15]_sum','%-stats_erc20_buy_addresses|maestro_trans_[minutes][15]_std','%-STCmacd','%-stats_erc20_sell_addresses|external_from_div_uniq_trans_[candles][5]_min','%-stats_erc20_sell_addresses|eth_received_external_std_[minutes][15]_max','%-stats_erc20_buy_addresses|banana_trans_[minutes][15]_std','%-stats_erc20_sell_addresses|eth_received_external_min_[minutes][15]_median','%-stats_erc20_sell_addresses|erc20_eth_received_min_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_value_usd_median_[candles][5]_std','%-repeated_buys_volumes_[minutes][15]_max','%-stats_erc20_buy_addresses|unique_received_to_addresses_div_uniq_trans_[minutes][15]_std','%-stats_erc20_buy_addresses|eth_received_internal_median_[candles][5]_max','%-stats_profit_window=10|roi_r_neg_median','%-stats_erc20_sell_addresses|eth_received_external_sum_[minutes][15]_max','%-invested_div_withdrew_sum_30','%-stats_erc20_buy_addresses|eth_received_external_avg_[minutes][15]_sum','%-stats_erc20_sell_addresses|internal_to_[candles][5]_max','%-stats_erc20_buy_addresses|unique_received_from_addresses_div_uniq_trans_[minutes][15]_median','%-stats_erc20_sell_addresses|eth_received_external_min_[candles][5]_std','%-stats_erc20_sell_addresses|erc20_uniq_addresses_received_[minutes][15]_sum','%-cci_25','%-stats_erc20_buy_addresses|erc20_hold_time_block_median_[minutes][15]_min','%-stats_profit_window=10|roi_r_neg_sum','%-ta_stochk_rsi_3','%-stats_erc20_sell_addresses|erc20_hold_time_block_median_[minutes][15]_median','%-stats_erc20_sell_addresses|eth_received_internal_min_[candles][5]_max','%-stats_erc20_sell_addresses|erc721_to_[minutes][15]_std','%-stats_erc20_buy_addresses|eth_received_internal_avg_[candles][5]_max','%-jaredfromsubway_[minutes][15]_max','%-repeated_buys_volumes_[minutes][15]_min','%-stats_erc20_sell_addresses|eth_sent_external_max_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_bought_n_not_sold_perc_[minutes][15]_min','%-stats_erc20_sell_addresses|diff_between_first_and_last_[candles][5]_sum','%-stats_erc20_buy_addresses|eth_received_internal_sum_[minutes][15]_max','%-stats_erc20_sell_addresses|unique_received_from_addresses_div_uniq_trans_[candles][5]_min','%-stats_erc20_sell_addresses|internal_to_div_uniq_trans_[minutes][15]_std','%-stats_erc20_sell_addresses|total_to_[minutes][15]_sum','%-jaredfromsubway_[minutes][15]_min','%-stats_erc20_sell_addresses|unique_received_from_addresses_div_uniq_trans_[candles][5]_max','%-stats_erc20_sell_addresses|erc20_eth_send_min_[minutes][15]_max','%-ta_stochk_rsi_7','%-stats_erc20_buy_addresses|erc20_eth_received_median_[candles][5]_median','%-stats_erc20_buy_addresses|internal_to_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_hold_time_block_sum_[minutes][15]_min','%-stats_erc20_buy_addresses|erc1155_from_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_to_div_uniq_trans_[candles][5]_min','%-stats_erc20_buy_addresses|erc721_from_[minutes][15]_sum','%-stats_erc20_sell_addresses|external_to_div_uniq_trans_[minutes][15]_sum','%-stats_erc20_buy_addresses|erc20_from_div_uniq_trans_[minutes][15]_median','%-stats_erc20_buy_addresses|eth_sent_external_avg_[minutes][15]_std','%-sell_amount_from_previous_balance_perc_[minutes][15]_sum','%-stats_erc20_sell_addresses|erc20_value_usd_median_[minutes][15]_median','%-stats_erc20_sell_addresses|total_to_[minutes][15]_median','%-stats_erc20_buy_addresses|erc20_eth_received_min_[candles][5]_sum','%-stats_erc20_buy_addresses|eth_received_external_avg_[candles][5]_max','%-stats_erc20_sell_addresses|eth_received_external_min_[minutes][15]_std','%-stats_erc20_buy_addresses|total_to_[minutes][15]_std','%-stats_erc20_buy_addresses|banana_trans_perc_[candles][5]_median','%-stats_profit_window=10|pnl_neg_std','%-stats_erc20_sell_addresses|erc20_value_usd_median_[candles][5]_sum','%-stats_erc20_sell_addresses|erc20_to_div_uniq_trans_[candles][5]_max','%-stats_erc20_buy_addresses|external_from_div_uniq_trans_[minutes][15]_std','%-stats_erc20_sell_addresses|external_from_[minutes][15]_max','%-stats_erc20_buy_addresses|first_in_transaction_block_[candles][5]_min','%-stats_erc20_sell_addresses|maestro_trans_perc_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_value_usd_sum_[minutes][15]_min','%-stats_erc20_sell_addresses|total_from_[minutes][15]_max','%-stats_erc20_sell_addresses|total_to_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_bought_n_not_sold_perc_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_bought_n_not_sold_perc_[candles][5]_sum','%-stats_erc20_sell_addresses|external_from_div_uniq_trans_[minutes][15]_median','%-stats_erc20_sell_addresses|eth_received_external_sum_[candles][5]_max','%-numbers_of_repeated_buys_[minutes][15]_std','%-stats_erc20_buy_addresses|transfers_sql_len_[minutes][15]_std','%-buy_trans_indexes_[minutes][15]_median','%-stats_erc20_sell_addresses|unique_received_from_addresses_[minutes][15]_min','%-stoch_slowd_2','%-stats_erc20_buy_addresses|eth_sent_external_median_[minutes][15]_sum','%-stats_erc20_buy_addresses|erc20_hold_time_block_median_[minutes][15]_sum','%-stats_erc20_buy_addresses|diff_between_first_and_last_[minutes][15]_median','%-stats_erc20_buy_addresses|avg_blocks_per_trans_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_hold_time_block_avg_[minutes][15]_max','%-stats_erc20_sell_addresses|avg_blocks_per_trans_[candles][5]_min','%-stats_erc20_sell_addresses|erc20_hold_time_block_std_[candles][5]_min','%-stats_erc20_sell_addresses|eth_received_external_max_[minutes][15]_std','%-stats_erc20_sell_addresses|maestro_trans_[minutes][15]_sum','%-jaredfromsubway_[candles][5]_std','%-stats_erc20_sell_addresses|erc20_hold_time_block_min_[candles][5]_max','%-stats_erc20_buy_addresses|eth_received_external_max_[candles][5]_max','%-stoch_slowk_5','%-stats_erc20_buy_addresses|erc20_eth_received_std_[minutes][15]_min','%-stats_erc20_sell_addresses|erc20_eth_send_min_[candles][5]_min','%-stats_erc20_buy_addresses|erc20_hold_time_block_sum_[candles][5]_min','%-stats_profit_window=10|unrealized_profit_neg_median','%-stats_erc20_sell_addresses|erc20_hold_time_block_avg_[minutes][15]_std','%-trades_div_10','%-stats_erc20_sell_addresses|eth_total_max_[candles][5]_min','%-repeated_sells_volumes_[candles][5]_max','%-sell_amount_from_previous_balance_perc_abs_[minutes][15]_max','%-stats_profit_window=10|unrealized_profit_pos_median','%-stats_erc20_buy_addresses|erc20_hold_time_block_min_[minutes][15]_std','%-stats_erc20_sell_addresses|maestro_trans_[minutes][15]_std','%-stats_erc20_sell_addresses|erc721_from_[candles][5]_std','%-stats_erc20_sell_addresses|internal_to_[minutes][15]_std','%-stats_erc20_buy_addresses|eth_received_external_max_[candles][5]_sum','%-stats_profit_window=10|new_holders','%-stats_erc20_sell_addresses|erc20_from_div_uniq_trans_[minutes][15]_max','%-stats_erc20_buy_addresses|maestro_trans_perc_[candles][5]_max','%-stats_erc20_sell_addresses|eth_total_std_[candles][5]_max','%-jaredfromsubway_[minutes][15]_median','%-stats_erc20_buy_addresses|erc20_eth_send_median_[candles][5]_sum','%-stoch_slowk_2','%-stats_erc20_buy_addresses|erc20_from_[minutes][15]_std','%-stats_erc20_buy_addresses|eth_total_sum_[candles][5]_min','%-stats_erc20_sell_addresses|eth_received_internal_min_[minutes][15]_median','%-stats_erc20_sell_addresses|eth_sent_external_sum_[minutes][15]_max','%-stats_erc20_buy_addresses|internal_to_div_uniq_trans_[minutes][15]_sum','%-repeated_buys_volumes_[minutes][15]_std','%-stats_erc20_buy_addresses|total_to_[minutes][15]_median','%-stats_erc20_sell_addresses|erc721_from_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_to_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_eth_send_sum_[minutes][15]_max','%-stats_erc20_buy_addresses|eth_total_median_[minutes][15]_std','%-stats_erc20_buy_addresses|internal_to_[candles][5]_max','%-stats_erc20_sell_addresses|eth_received_internal_std_[minutes][15]_max','%-efi','%-stats_erc20_sell_addresses|eth_received_external_std_[minutes][15]_median','%-stats_erc20_buy_addresses|erc1155_to_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_eth_received_avg_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_bought_n_not_sold_[candles][5]_median','%-stats_erc20_sell_addresses|erc20_eth_received_max_[candles][5]_median','%-stats_erc20_buy_addresses|erc20_eth_send_min_[candles][5]_min','%-stats_erc20_sell_addresses|nonce_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_eth_send_max_[minutes][15]_sum','%-stats_erc20_buy_addresses|erc20_eth_send_sum_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_bought_n_not_sold_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_eth_send_avg_[minutes][15]_median','%-stats_erc20_sell_addresses|maestro_trans_perc_[minutes][15]_sum','%-mfi_14','%-stats_erc20_sell_addresses|erc1155_from_[minutes][15]_std','%-time_b-5','%-stats_erc20_sell_addresses|eth_received_external_std_[candles][5]_min','%-stats_erc20_sell_addresses|erc20_eth_send_avg_[minutes][15]_sum','%-stats_erc20_sell_addresses|eth_received_external_sum_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_hold_time_block_median_[minutes][15]_min','%-withdrew_list_[candles][5]_sum','%-stats_erc20_buy_addresses|eth_received_external_sum_[candles][5]_std','%-stats_erc20_sell_addresses|external_to_div_uniq_trans_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_hold_time_block_max_[minutes][15]_std','%-stats_profit_window=10|roi_u_neg_count','%-stats_erc20_buy_addresses|erc20_hold_time_block_std_[candles][5]_std','%-stats_erc20_buy_addresses|eth_received_internal_min_[candles][5]_median','%-stats_erc20_sell_addresses|erc20_value_usd_std_[candles][5]_median','%-stats_erc20_buy_addresses|erc20_value_usd_median_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_value_usd_min_[minutes][15]_min','%-stats_erc20_sell_addresses|diff_between_first_and_last_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_from_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_uniq_addresses_received_[minutes][15]_min','%-stats_erc20_sell_addresses|erc20_value_usd_min_[minutes][15]_max','%-buy_trans_indexes_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_bought_n_not_sold_perc_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_uniq_addresses_send_[candles][5]_max','%-stats_profit_window=10|realized_profit_neg_median','%-stats_erc20_sell_addresses|erc20_hold_time_block_median_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_from_div_uniq_trans_[minutes][15]_std','%-stats_erc20_sell_addresses|avg_blocks_per_trans_[candles][5]_sum','%-stats_erc20_sell_addresses|maestro_trans_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_eth_send_std_[candles][5]_median','%-stats_profit_window=10|roi_r_pos_median','%-stats_erc20_sell_addresses|erc20_hold_time_block_max_[candles][5]_min','%-stats_erc20_buy_addresses|eth_received_internal_min_[candles][5]_min','%-stats_erc20_buy_addresses|unique_received_from_addresses_[minutes][15]_std','%-stats_erc20_buy_addresses|eth_received_external_min_[candles][5]_max','%-stats_erc20_sell_addresses|eth_sent_external_std_[minutes][15]_min','%-stats_erc20_buy_addresses|balance_eth_[minutes][15]_max','%-stats_erc20_sell_addresses|avg_blocks_per_trans_[candles][5]_std','%-stats_erc20_sell_addresses|eth_sent_external_avg_[minutes][15]_max','%-stats_erc20_sell_addresses|eth_received_external_sum_[minutes][15]_min','%-stats_erc20_sell_addresses|erc20_hold_time_block_sum_[candles][5]_sum','%-stats_erc20_sell_addresses|eth_received_internal_median_[minutes][15]_std','%-stats_erc20_buy_addresses|balance_eth_[minutes][15]_std','%-stats_erc20_sell_addresses|external_to_div_uniq_trans_[minutes][15]_min','%-uo','%-stats_erc20_sell_addresses|erc20_from_[minutes][15]_sum','%-stats_erc20_sell_addresses|external_to_div_uniq_trans_[candles][5]_median','%-stats_erc20_sell_addresses|total_from_[candles][5]_max','%-stats_erc20_buy_addresses|eth_total_std_[minutes][15]_max','%-stats_profit_window=10|unrealized_profit_neg_sum','%-sell_trans_indexes_[candles][5]_max','%-stats_erc20_buy_addresses|eth_sent_external_avg_[minutes][15]_sum','%-stats_erc20_buy_addresses|eth_total_median_[minutes][15]_median','%-stats_erc20_sell_addresses|unique_received_to_addresses_[minutes][15]_std','%-stats_erc20_sell_addresses|maestro_trans_perc_[candles][5]_min','%-stats_erc20_sell_addresses|banana_trans_[minutes][15]_sum','%-numbers_of_repeated_sells_[candles][5]_std','%-stats_erc20_buy_addresses|erc20_eth_send_max_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_eth_send_median_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_uniq_addresses_send_[minutes][15]_sum','%-stats_erc20_buy_addresses|external_to_div_uniq_trans_[candles][5]_median','%-stats_erc20_buy_addresses|erc20_eth_received_std_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_hold_time_block_avg_[candles][5]_max','%-stats_erc20_buy_addresses|eth_total_median_[candles][5]_sum','%-stats_erc20_sell_addresses|maestro_trans_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_eth_send_max_[minutes][15]_max','%-stats_erc20_sell_addresses|nonce_[candles][5]_std','%-stats_profit_window=10|roi_t_pos_std','%-pgo','%-stats_erc20_sell_addresses|erc20_bought_n_not_sold_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_hold_time_block_std_[candles][5]_std','%-stats_erc20_buy_addresses|unique_transactions_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_to_div_uniq_trans_[candles][5]_min','%-stats_erc20_buy_addresses|external_to_div_uniq_trans_[minutes][15]_max','%-stats_erc20_sell_addresses|eth_total_median_[candles][5]_min','%-stats_erc20_sell_addresses|erc20_to_div_uniq_trans_[minutes][15]_min','%-stats_erc20_sell_addresses|erc20_bought_n_not_sold_[minutes][15]_std','%-sell_amount_from_previous_balance_perc_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_hold_time_block_avg_[minutes][15]_std','%-stats_erc20_buy_addresses|eth_sent_external_median_[candles][5]_std','%-stats_erc20_sell_addresses|eth_received_external_sum_[minutes][15]_median','%-stats_erc20_buy_addresses|erc20_value_usd_sum_[minutes][15]_min','%-stats_erc20_sell_addresses|diff_between_first_and_last_[candles][5]_median','%-stats_erc20_sell_addresses|external_to_div_uniq_trans_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_hold_time_block_max_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_uniq_addresses_received_[minutes][15]_std','%-stochd_rsi_3','%-stats_erc20_sell_addresses|internal_to_div_uniq_trans_[minutes][15]_max','%-stats_erc20_sell_addresses|unique_received_from_addresses_div_uniq_trans_[candles][5]_median','%-trades_sell_sum_40','%-stats_erc20_sell_addresses|eth_received_external_median_[minutes][15]_max','%-rsi_30','%-stats_erc20_buy_addresses|unique_received_to_addresses_[minutes][15]_min','%-stats_erc20_buy_addresses|erc20_value_usd_min_[minutes][15]_median','%-stats_erc20_buy_addresses|unique_received_to_addresses_[minutes][15]_sum','%-stats_erc20_sell_addresses|erc20_eth_received_max_[minutes][15]_median','%-repeated_buys_volumes_[candles][5]_min','%-stats_erc20_buy_addresses|erc1155_to_[minutes][15]_max','%-stats_erc20_sell_addresses|banana_trans_perc_[minutes][15]_std','%-stats_erc20_buy_addresses|first_in_transaction_block_[minutes][15]_median','%-stats_erc20_buy_addresses|erc20_value_usd_min_[minutes][15]_sum','%-stats_erc20_sell_addresses|balance_eth_[candles][5]_std','%-stats_erc20_buy_addresses|first_in_transaction_block_[minutes][15]_std','%-trades_sell_sum_20','%-stats_erc20_sell_addresses|erc20_eth_send_max_[minutes][15]_std','%-stats_erc20_buy_addresses|eth_received_external_sum_[minutes][15]_std','%-stats_erc20_sell_addresses|eth_received_external_sum_[candles][5]_sum','%-stats_erc20_sell_addresses|eth_sent_external_median_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_hold_time_block_median_[candles][5]_min','%-withdrew_list_[minutes][15]_sum','%-stats_erc20_buy_addresses|erc20_hold_time_block_avg_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_hold_time_block_avg_[minutes][15]_sum','%-stats_erc20_buy_addresses|external_to_div_uniq_trans_[minutes][15]_sum','%-stats_erc20_sell_addresses|diff_between_first_and_last_[minutes][15]_min','%-stats_erc20_buy_addresses|eth_received_external_median_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_eth_received_max_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_value_usd_median_[minutes][15]_sum','%-stats_erc20_sell_addresses|erc20_uniq_addresses_received_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_eth_received_max_[candles][5]_median','%-stats_profit_window=10|realized_profit_pos_std','%-stats_erc20_buy_addresses|erc20_hold_time_block_max_[candles][5]_max','%-stats_erc20_sell_addresses|erc20_to_[candles][5]_max','%-trades_div_20','%-stats_erc20_sell_addresses|unique_received_from_addresses_[candles][5]_max','%-stats_erc20_sell_addresses|erc20_value_usd_avg_[candles][5]_median','%-stats_erc20_sell_addresses|first_in_transaction_block_[minutes][15]_median','%-stats_profit_window=70|unrealized_profit_pos_median','%-stats_erc20_buy_addresses|unique_received_from_addresses_[candles][5]_max','%-stats_erc20_buy_addresses|first_in_transaction_block_[minutes][15]_sum','%-stats_erc20_buy_addresses|erc20_to_[minutes][15]_sum','%-stats_erc20_buy_addresses|unique_received_to_addresses_[candles][5]_min','%-stats_erc20_sell_addresses|eth_received_internal_max_[candles][5]_max','%-stats_erc20_buy_addresses|balance_usd_[minutes][15]_std','%-stats_erc20_sell_addresses|unique_received_from_addresses_[minutes][15]_median','%-stats_profit_window=10|pnl_neg_sum','%-withdrew_sum_15','%-stats_profit_window=10|roi_u_neg_median','%-stats_erc20_buy_addresses|external_from_[minutes][15]_std','%-stats_erc20_sell_addresses|external_from_div_uniq_trans_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_eth_received_std_[candles][5]_median','%-stats_erc20_sell_addresses|eth_received_external_max_[candles][5]_max','%-stats_erc20_buy_addresses|eth_total_max_[candles][5]_max','%-stats_erc20_sell_addresses|erc20_uniq_addresses_send_[minutes][15]_std','%-stats_erc20_sell_addresses|eth_total_avg_[minutes][15]_min','%-stats_erc20_sell_addresses|eth_received_external_sum_[candles][5]_std','%-stats_erc20_sell_addresses|unique_received_to_addresses_div_uniq_trans_[minutes][15]_min','%-stats_profit_window=10|roi_t_neg_std','%-stats_erc20_sell_addresses|erc20_value_usd_std_[candles][5]_min','%-stats_erc20_sell_addresses|nonce_[candles][5]_max','%-stats_erc20_sell_addresses|banana_trans_perc_[candles][5]_max','%-stats_erc20_sell_addresses|unique_transactions_[minutes][15]_std','%-stats_profit_window=10|roi_u_pos_std','%-plus_di_1','%-stats_erc20_buy_addresses|transfers_sql_len_[candles][5]_max','%-stats_erc20_buy_addresses|banana_trans_perc_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_from_[minutes][15]_max','%-stats_erc20_sell_addresses|external_from_div_uniq_trans_[candles][5]_median','%-stats_erc20_sell_addresses|internal_to_div_uniq_trans_[candles][5]_min','%-stats_erc20_buy_addresses|eth_received_external_std_[candles][5]_min','%-stats_erc20_sell_addresses|eth_received_external_max_[minutes][15]_median','%-stats_erc20_sell_addresses|erc20_eth_send_avg_[minutes][15]_max','%-stats_erc20_buy_addresses|unique_received_to_addresses_div_uniq_trans_[candles][5]_max','%-stats_erc20_sell_addresses|erc20_eth_send_min_[minutes][15]_min','%-stats_erc20_sell_addresses|external_to_div_uniq_trans_[candles][5]_min','%-stats_erc20_sell_addresses|unique_received_from_addresses_div_uniq_trans_[candles][5]_sum','%-stats_erc20_sell_addresses|banana_trans_perc_[candles][5]_std','%-stats_erc20_buy_addresses|external_to_[minutes][15]_median','%-stats_erc20_buy_addresses|internal_to_[candles][5]_sum','%-stats_erc20_buy_addresses|eth_sent_external_std_[minutes][15]_max','%-stats_erc20_buy_addresses|eth_received_internal_std_[minutes][15]_sum','%-stats_erc20_buy_addresses|eth_total_sum_[minutes][15]_std','%-sell_trans_indexes_[candles][5]_std','%-invested_minus_withdrew_sum_7','%-stats_erc20_sell_addresses|avg_blocks_per_trans_[minutes][15]_median','%-stats_erc20_sell_addresses|total_to_[minutes][15]_max','%-stats_erc20_buy_addresses|eth_received_external_min_[minutes][15]_median','%-stats_erc20_sell_addresses|external_to_div_uniq_trans_[minutes][15]_median','%-stats_erc20_sell_addresses|erc20_eth_received_std_[minutes][15]_std','%-stats_erc20_buy_addresses|eth_sent_external_max_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_eth_received_min_[candles][5]_min','%-withdrew_list_[candles][5]_median','%-stats_erc20_buy_addresses|maestro_trans_[candles][5]_median','%-stats_erc20_buy_addresses|erc20_value_usd_sum_[candles][5]_min','%-stats_erc20_sell_addresses|erc20_to_div_uniq_trans_[candles][5]_median','%-stats_erc20_sell_addresses|eth_received_external_std_[minutes][15]_min','%-stats_erc20_sell_addresses|eth_received_external_avg_[minutes][15]_std','%-stats_erc20_sell_addresses|eth_sent_external_avg_[candles][5]_std','%-stats_erc20_buy_addresses|eth_total_median_[minutes][15]_min','%-stats_erc20_sell_addresses|erc20_eth_received_min_[candles][5]_sum','%-stats_erc20_buy_addresses|erc20_hold_time_block_min_[minutes][15]_min','%-withdrew_sum_7','%-stats_profit_window=70|roi_u_neg_count','%-stats_erc20_buy_addresses|erc20_eth_send_min_[minutes][15]_std','%-stats_erc20_buy_addresses|eth_sent_external_max_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_bought_n_not_sold_perc_[minutes][15]_sum','%-stats_profit_window=10|unrealized_profit_pos_sum','%-stats_erc20_sell_addresses|erc20_uniq_addresses_send_[minutes][15]_sum','%-stats_erc20_sell_addresses|unique_received_from_addresses_div_uniq_trans_[minutes][15]_median','%-stats_erc20_buy_addresses|eth_received_external_max_[minutes][15]_sum','%-stats_erc20_sell_addresses|banana_trans_perc_[minutes][15]_median','%-stats_erc20_buy_addresses|erc20_uniq_addresses_send_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_hold_time_block_min_[minutes][15]_median','%-stats_erc20_buy_addresses|erc20_eth_send_std_[candles][5]_median','%-stats_erc20_sell_addresses|eth_received_internal_avg_[candles][5]_max','%-stats_erc20_sell_addresses|external_from_div_uniq_trans_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_from_[candles][5]_min','%-stats_erc20_buy_addresses|erc20_hold_time_block_std_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_hold_time_block_min_[candles][5]_std','%-stats_erc20_sell_addresses|unique_received_to_addresses_[candles][5]_min','%-stats_erc20_buy_addresses|erc20_uniq_addresses_send_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_eth_send_max_[minutes][15]_min','%-stats_erc20_sell_addresses|eth_received_external_max_[minutes][15]_max','%-trades_buy_sum_40','%-stats_erc20_buy_addresses|erc20_from_[candles][5]_median','%-stats_erc20_buy_addresses|eth_sent_external_avg_[minutes][15]_min','%-stats_erc20_buy_addresses|internal_to_div_uniq_trans_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_hold_time_block_min_[candles][5]_sum','%-stats_erc20_sell_addresses|erc20_hold_time_block_min_[minutes][15]_max','%-stats_erc20_sell_addresses|eth_received_internal_max_[candles][5]_median','%-jaredfromsubway_[candles][5]_sum','%-stats_erc20_sell_addresses|internal_to_[minutes][15]_median','%-stats_erc20_buy_addresses|maestro_trans_[candles][5]_std','%-stats_erc20_buy_addresses|total_from_[candles][5]_max','%-stats_erc20_sell_addresses|eth_sent_external_median_[candles][5]_max','%-stats_erc20_sell_addresses|eth_received_external_median_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_eth_send_std_[minutes][15]_max','%-stats_erc20_buy_addresses|unique_received_from_addresses_[minutes][15]_median','%-withdrew_list_[candles][5]_min','%-stats_erc20_buy_addresses|external_to_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_bought_n_not_sold_[candles][5]_std','%-stats_profit_window=10|realized_profit_neg_std','%-stats_erc20_sell_addresses|internal_to_[minutes][15]_min','%-stats_profit_window=10|roi_t_neg_sum','%-invested_list_[minutes][15]_sum','%-amount_left_from_max_balance_who_sold_perc_[minutes][15]_median','%-stats_erc20_buy_addresses|erc20_hold_time_block_sum_[minutes][15]_std','%-stats_erc20_buy_addresses|erc20_value_usd_median_[minutes][15]_median','%-stats_erc20_sell_addresses|erc20_eth_received_max_[minutes][15]_std','%-stats_erc20_buy_addresses|avg_blocks_per_trans_[minutes][15]_min','%-stats_erc20_sell_addresses|erc1155_to_[minutes][15]_sum','%-stats_erc20_buy_addresses|avg_blocks_per_trans_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_eth_received_min_[candles][5]_median','%-stats_erc20_sell_addresses|first_in_transaction_block_[minutes][15]_std','%-stats_erc20_sell_addresses|erc20_eth_received_max_[minutes][15]_min','%-stats_erc20_buy_addresses|erc20_eth_send_min_[minutes][15]_median','%-stats_erc20_buy_addresses|unique_received_to_addresses_div_uniq_trans_[candles][5]_std','%-stats_erc20_sell_addresses|balance_eth_[candles][5]_min','%-stats_erc20_buy_addresses|erc20_hold_time_block_max_[minutes][15]_median','%-stats_erc20_sell_addresses|erc20_eth_send_sum_[candles][5]_max','%-stats_erc20_sell_addresses|eth_received_internal_min_[candles][5]_median','%-stats_erc20_buy_addresses|eth_received_internal_min_[candles][5]_std','%-stats_erc20_sell_addresses|internal_to_div_uniq_trans_[candles][5]_std','%-stats_erc20_sell_addresses|erc20_bought_n_not_sold_[minutes][15]_min','%-stats_erc20_buy_addresses|unique_received_to_addresses_[minutes][15]_max','%-stats_erc20_sell_addresses|balance_eth_[candles][5]_sum','%-stats_erc20_buy_addresses|erc20_to_[minutes][15]_median','%-stats_erc20_buy_addresses|erc20_eth_send_median_[minutes][15]_min','%-stats_erc20_buy_addresses|erc1155_from_[candles][5]_max','%-willr_84','%-stats_erc20_sell_addresses|balance_eth_[minutes][15]_median','%-stats_erc20_buy_addresses|erc20_from_div_uniq_trans_[candles][5]_median','%-repeated_sells_volumes_[minutes][15]_min','%-stats_erc20_buy_addresses|diff_between_first_and_last_[minutes][15]_min','%-stats_erc20_buy_addresses|eth_received_internal_max_[candles][5]_median','%-stats_erc20_sell_addresses|eth_sent_external_max_[candles][5]_min','%-stats_erc20_sell_addresses|eth_received_internal_min_[minutes][15]_min','%-stats_erc20_sell_addresses|erc20_hold_time_block_median_[candles][5]_median','%-stats_erc20_buy_addresses|external_from_div_uniq_trans_[candles][5]_max','%-stats_erc20_sell_addresses|erc20_from_div_uniq_trans_[minutes][15]_median','%-stats_erc20_buy_addresses|eth_received_external_max_[candles][5]_std','%-stats_erc20_buy_addresses|eth_received_internal_median_[candles][5]_sum','%-stats_erc20_buy_addresses|external_from_[minutes][15]_median','%-stats_erc20_sell_addresses|erc20_eth_received_sum_[minutes][15]_min','%-stats_erc20_buy_addresses|banana_trans_[candles][5]_sum','%-stats_erc20_buy_addresses|erc20_hold_time_block_avg_[candles][5]_sum','%-stats_erc20_buy_addresses|erc721_to_[candles][5]_median','%-stats_erc20_buy_addresses|erc20_hold_time_block_std_[minutes][15]_sum','%-stats_erc20_sell_addresses|erc20_eth_received_avg_[candles][5]_std','%-stats_erc20_buy_addresses|eth_total_median_[candles][5]_max','%-amount_left_from_max_balance_who_sold_perc_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_hold_time_block_sum_[candles][5]_std','%-stats_erc20_buy_addresses|erc20_eth_received_min_[minutes][15]_min','%-stats_erc20_sell_addresses|maestro_trans_perc_[candles][5]_max','%-stats_erc20_buy_addresses|unique_received_to_addresses_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_eth_send_sum_[minutes][15]_min','%-stats_erc20_sell_addresses|maestro_trans_[minutes][15]_median','%-stats_erc20_sell_addresses|eth_received_external_min_[minutes][15]_min','%-invested_div_withdrew_sum_15','%-stats_erc20_sell_addresses|balance_eth_[minutes][15]_std','%-stats_erc20_sell_addresses|diff_between_first_and_last_[minutes][15]_sum','%-stats_erc20_buy_addresses|erc20_value_usd_median_[minutes][15]_min','%-stats_erc20_sell_addresses|erc20_eth_send_sum_[candles][5]_min','%-stats_erc20_buy_addresses|erc20_from_div_uniq_trans_[minutes][15]_std','%-stats_erc20_buy_addresses|unique_received_to_addresses_div_uniq_trans_[minutes][15]_sum','%-stats_erc20_buy_addresses|unique_received_from_addresses_div_uniq_trans_[candles][5]_sum','%-stats_erc20_buy_addresses|internal_to_[candles][5]_std','%-stats_erc20_buy_addresses|eth_received_internal_std_[candles][5]_max','%-stats_erc20_buy_addresses|erc20_value_usd_min_[candles][5]_median','%-stats_erc20_sell_addresses|unique_received_to_addresses_[minutes][15]_min','%-stats_erc20_sell_addresses|total_to_[minutes][15]_min','%-stats_erc20_buy_addresses|erc20_hold_time_block_median_[minutes][15]_max','%-stats_erc20_buy_addresses|erc20_hold_time_block_sum_[candles][5]_median','%-stats_erc20_sell_addresses|erc20_uniq_addresses_received_[minutes][15]_min','%-stats_erc20_buy_addresses|erc20_eth_send_max_[candles][5]_median','%-stats_profit_window=10|roi_t_neg_count','%-stats_erc20_sell_addresses|external_to_[minutes][15]_sum','%-stats_erc20_buy_addresses|erc20_hold_time_block_std_[candles][5]_max','%-stats_erc20_sell_addresses|eth_received_external_std_[candles][5]_std','%-stats_erc20_buy_addresses|erc20_from_div_uniq_trans_[minutes][15]_min','%-jaredfromsubway_[minutes][15]_sum','%-stats_erc20_buy_addresses|erc20_eth_received_avg_[candles][5]_max','%-stats_erc20_sell_addresses|maestro_trans_[candles][5]_sum','%-stats_erc20_sell_addresses|banana_trans_[candles][5]_max','%-stats_erc20_sell_addresses|eth_received_internal_min_[candles][5]_sum','%-stats_erc20_sell_addresses|erc20_bought_n_not_sold_[candles][5]_std','%-stats_erc20_sell_addresses|unique_received_from_addresses_div_uniq_trans_[minutes][15]_max','%-stats_erc20_sell_addresses|erc20_eth_send_min_[minutes][15]_std','%-repeated_buys_volumes_[candles][5]_sum']]


    if 0:
        for i, col in enumerate(df.columns):
            for index, row in df.iterrows():
                if row[col] > 10 ** 15:
                    # print(f"[{i}/{len(self.df.columns)}] {col}: {row[col]}")
                    df.loc[index, col] = 0

        # (!) надо проверить где ошибка
        df = df.dropna(subset=['&-reward'])

        df = df.iloc[:, :120]
        df = df.fillna(0)


    address = file.split(',')[0].split('.')[0]
    df.to_csv(f"{path_df_ready}/{file}", index=False)
    print(f"saved to {path_df_ready}/{file}")

    del df
    

    return


    
if __name__ == '__main__':



    start_time_e = time.time()
    print('start training..')



    files = [f for f in listdir(path_stats) if isfile(join(path_stats, f))]
    addresses_ready = [f.split(',')[0].split('.')[0] for f in listdir(path_df_ready) if isfile(join(path_df_ready, f))]


    # for adr in files[:]:
    #     if '0xd8a56034B27bc5C21aa30a09aF972505b6eD4F1d' not in adr:
    #         files.remove(adr)

    print(f"len files={len(files)}, addresses_ready={len(addresses_ready)}")


    # files = [
    #     "0x123455Db64A72aBd71650aAd722db3E3B8488b82,l550,13.7k.csv",
    # ]
    # xx = create_features('0x4214202672a4471aA212b0d8044216bEc42c0420,l604,4.51k.csv')
    # exit()


    if 1:
        params = []
        for file in reversed(files):
            if 1:
                address = file.split(',')[0].split('.')[0]
                if address in addresses_ready:
                    print(f"file {file} already done")
                    continue
            if '.csv' not in file: file += '.csv'
            params.append((file,))

        with Pool(40) as pool:
            pool.starmap_async(create_features, params).get()

        exit()



    addresses_ready = [f for f in listdir(path_df_ready) if isfile(join(path_df_ready, f))]
    df = pd.DataFrame()

    for i, file in enumerate(addresses_ready[:]):
        _df = pd.read_csv(f"{path_df_ready}/{file}", header=0)
        _df['token_address'] = '.'.join(file.split('.')[:-1])

        _df = _df.iloc[100:-label_period_candles]


        print(f'[{i + 1}/{len(addresses_ready)}] loaded df.shape={_df.shape} {file}')
        df = pd.concat([df, _df])



    if 0:
        df = df.fillna(0)
        
        cols_equal = []
        for col in df.columns:
            a = df[col].to_numpy() # s.values (pandas<0.24)
            if (a[0] == a).all():
                cols_equal.append(col)

        print(f'cols_equal ({len(cols_equal)}):')
        print(cols_equal)


    print('df final:')
    print(df)
    print(df.info())




    # missing values in catboost https://www.geeksforgeeks.org/handling-missing-values-with-catboost/

    # df = df.dropna(axis=0, how='any')
    # df.dropna(inplace=True)
    # df.replace(np.nan, 0, inplace=True)



    print(f'df nan columns len={len(df.columns[df.isna().any()])}')
    #print(df.columns[df.isna().any()].tolist())

    df = df.drop(['block'], axis=1)


    for col in df:
        # get the maximum value in column
        # check if it is less than or equal to the defined threshold
        threshold = 10 ** 30
        if df[col].dtype != object and df[col].max() >= threshold:
            _len_found = len(df[(df[col] >= threshold)])
            print(f"у {col} (их {_len_found}) max значение {df[col].max()}")
            if _len_found > 500:
                df = df.drop([col], axis=1)
            else:
                df.drop(df[df[col] >= threshold].index, inplace=True)

    # print("df after cropped:")
    # print(df)


    # print("np.isnan(X):", np.isnan(df))
    # print(np.where(np.isnan(df)))
    # exit()
    #print(np.where(df.values >= np.finfo(np.float32).max))
    #exit()


    #df['&-target'] = np.select([(df['&-close'] >= 0.4)], [1], default=0)

    # (!) надо сделать проверку на nan значения в reward
    df['&-target'] = np.select([(df['&-reward'] != np.nan) & (df['&-reward'] >= 70)], [1], default=0)
    X = df.drop(['&-target', '&-reward'], axis=1)
    y = df['&-target']

    #X.fillna(X.mean(), inplace=True)

    # scaler = MinMaxScaler(feature_range=(-1, 1))
    # X_scaled_data = scaler.fit_transform(X)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, shuffle=False, random_state=42) # 0.2
    X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.01, shuffle=False, random_state=1) # 0.25

    # print(f"X_test token_address ({len(X_test['token_address'])}):")
    # print(X_test['token_address'].to_list())

    print(f"X_val token_address ({len(X_val['token_address'])}):")
    X_train_addresses = set(X_train['token_address'].to_list())
    X_test_addresses = set(X_test['token_address'].to_list()) - X_train_addresses
    X_val_addresses = set(X_val['token_address'].to_list()) - X_test_addresses - X_train_addresses
    print(f"X_train_addresses={len(X_train_addresses)}, X_test_addresses={len(X_test_addresses)}, X_val_addresses={len(X_val_addresses)}")
    print(f"X_test_addresses:")
    print(list(X_test_addresses))
    print(f"X_val_addresses:")
    print(list(X_val_addresses))

    X_train = X_train.select_dtypes(exclude=['object'])
    X_test = X_test.select_dtypes(exclude=['object'])
    X_val = X_val.select_dtypes(exclude=['object'])

    print(f"lens: X={len(X)} df={len(df)}")
    print(f"X_train={len(X_train)} ({round(100*len(X_train)/len(X))}%), X_test={len(X_test)} ({round(100*len(X_test)/len(X))}%), X_val={len(X_val)} ({round(100*len(X_val)/len(X))}%)")

    # print('X_train:')
    # print(type(X_train))
    # print(X_train)
    # print(list(X_train.columns.values))

    if 0:

        study = optuna.create_study(direction="maximize", pruner=optuna.pruners.MedianPruner())
        study.optimize(objective, n_trials=250, timeout=120*60) # timeout=600

        print("Number of finished trials: {}".format(len(study.trials)))

        print("Best trial:")
        trial = study.best_trial

        print("  Value: {}".format(trial.value))

        print("  Params: ")
        for key, value in trial.params.items():
            print("    {}: {}".format(key, value))
            

        exit()



    if 1:

        # MLPClassifier sklearn

        model = CatBoostClassifier(iterations=1000, verbose=25)

        model.fit(X_train, y_train)
        class_predictions = model.predict(X_test)
        probability_predictions = model.predict_proba(X_test)

        print('get_best_score():', model.get_best_score())

        log_loss_value = log_loss(y_test, probability_predictions[:,1])
        print(f'Log Loss: {log_loss_value}')

        roc_auc = roc_auc_score(y_test, probability_predictions[:,1])
        print(f'ROC AUC: {roc_auc}')

        class_report = classification_report(y_test, class_predictions)
        print(f'Classification Report test:\n {class_report}')

        print(f"accuracy_score: {accuracy_score(y_test, class_predictions)}")

        class_predictions_val = model.predict(X_val)
        class_report = classification_report(y_val, class_predictions_val)
        print(f'Classification Report val:\n {class_report}')




        print()
        feature_importance = model.feature_importances_
        sorted_idx = np.argsort(feature_importance)
        feat_list = []
        for si in reversed(sorted_idx[-100:]):
            print(f"{np.array(X_test.columns)[si]} {feature_importance[si]}")
        for si in reversed(sorted_idx):
            feat_list.append({'feature': np.array(X_test.columns)[si], 'importance': feature_importance[si]})
        print()

        pd.DataFrame.from_records(feat_list).to_csv('feature_importance.csv', index=False)

        model.save_model(f'{FILES_DIR}/lib/models/erc20_model_classif')




    # regression
    if 0:

        #cat_features = X_train.select_dtypes(include=['object']).columns.tolist()
        
        # model = CatBoostClassifier(cat_features=cat_features, iterations=1500, verbose=25)
        model = CatBoostRegressor(
            loss_function='RMSE',
            iterations=700,
            # task_type="GPU",
            # devices='0',
            verbose=25,
            #custom_metric=['RMSE', 'MAE', 'R2'],
        )
        

        model.fit(
            X=X_train,
            y=y_train,
            eval_set=(X_val, y_val)
        )
        


        for data in [
            [X_test, y_test],
            [X_val, y_val],
        ]:

            pred = model.predict(data[0])
            print('---')
            print(f" rmes: {root_mean_squared_error(data[1], pred):.4f}")
            print(f" mae: {mean_absolute_error(data[1], pred):.4f}")
            print(f" r2: {r2_score(data[1], pred):.4f}")
            print(f" explained_variance_score: {explained_variance_score(data[1], pred):.4f}")
            print(f" get_best_score(): {model.get_best_score()}")
            print(f" get_best_iteration(): {model.get_best_iteration()}")



        # correct_0 = 0
        # total_0 = 0
        # correct_1 = 0
        # total_1 = 0
        # y_test_list = y_test.values.tolist()
        # for i, _ in enumerate(y_test_list):
        #     if y_test_list[i] == 0: total_0 += 1
        #     else: total_1 += 1
        #     if y_test_list[i] == 0 and class_predictions[i] == 0: correct_0 += 1
        #     if y_test_list[i] == 1 and class_predictions[i] == 1: correct_1 += 1
        # print(f"correct_0: {correct_0} / {total_0} ({round(100*correct_0/total_0)}%) {total_0 - correct_0}")
        # print(f"correct_1: {correct_1} / {total_1} ({round(100*correct_1/total_1)}%)")
        

        # print('---')

        # class_predictions_val = model.predict(X_val)
        # print(f"accuracy_score val: {accuracy_score(y_val, class_predictions_val)}")

        # correct_0 = 0
        # total_0 = 0
        # correct_1 = 0
        # total_1 = 0
        # y_val_list = y_val.values.tolist()
        # for i, _ in enumerate(y_val_list):
        #     if y_val_list[i] == 0: total_0 += 1
        #     else: total_1 += 1
        #     if y_val_list[i] == 0 and class_predictions_val[i] == 0: correct_0 += 1
        #     if y_val_list[i] == 1 and class_predictions_val[i] == 1: correct_1 += 1



        # print(f"correct_0 val: {correct_0} / {total_0} ({round(100*correct_0/total_0)}%) {total_0 - correct_0}")
        # print(f"correct_1 val: {correct_1} / {total_1} ({round(100*correct_1/total_1)}%)")



        # print('class_predictions:')
        # print(class_predictions)

        # print('probability_predictions:')
        # print(probability_predictions)

        # print(f'rmse: {np.sqrt(mean_squared_error(y_test, class_predictions))}')


        print()
        feature_importance = model.feature_importances_
        sorted_idx = np.argsort(feature_importance)
        feat_list = []
        for si in reversed(sorted_idx[-100:]):
            print(f"{np.array(X_test.columns)[si]} {feature_importance[si]}")
        for si in reversed(sorted_idx):
            feat_list.append({'feature': np.array(X_test.columns)[si], 'importance': feature_importance[si]})
        print()

        pd.DataFrame.from_records(feat_list).to_csv('feature_importance.csv', index=False)

        model.save_model(f'{FILES_DIR}/lib/models/erc20_model_regress')





    # здесь StandartScalar()
    # https://www.kaggle.com/code/kaanboke/beginner-friendly-catboost-with-optuna

    # lr = LogisticRegression(solver='liblinear')
    # lda= LinearDiscriminantAnalysis()


        
    print("executied in %.3fs" % (time.time() - start_time_e))

