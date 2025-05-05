
import os
import sys
import json
import pickle
import traceback
from pprint import pprint
import numpy as np
import pandas as pd
import time
import random
import string
import statistics
from datetime import datetime
from multiprocessing import Pool
import tabulate
from eth_utils import to_checksum_address
from web3 import Web3
from tqdm import tqdm
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TaskProgressColumn, TextColumn,
                           TimeElapsedColumn, TimeRemainingColumn)

import psycopg2
from psycopg2.extras import Json, DictCursor, RealDictCursor


import logging
from logging.handlers import RotatingFileHandler
formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s %(message)s')
# logging.basicConfig(filename='logs/task/db.log', format='%(asctime)s %(levelname)s %(message)s')
# logger = logging.getLogger()

info_handler = RotatingFileHandler('/home/root/python/ethereum/lib/logs/mints.log', mode='a',
    maxBytes=10*1024*1024,
    backupCount=0, encoding=None, delay=0)
info_handler.setFormatter(formatter)
info_handler.setLevel(logging.INFO)
info_logger = logging.getLogger('any-info') # root
info_logger.setLevel(logging.INFO)
if (info_logger.hasHandlers()):
    info_logger.handlers.clear()
info_logger.addHandler(info_handler)


import ml

import utils
from utils import FILES_DIR, FILE_LABELS


MINUTES_BACKTEST = 30
LIMIT_ROWS = 30000

conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)




def calculate_token(kwards):

    # print(f"starting pid {os.getpid()}, {kwards}")

    pi = kwards[0]
    address = kwards[1]

    # time.sleep(random.randint(1,5))


    for _ in range(10):
        try:
            with psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:

                    try:
                        cursor.execute(f"""
                            SELECT
                                transactionHash,
                                blockNumber,
                                timestamp,
                                from_ as from,
                                to_ as to,
                                value_usd,
                                value_usd_per_token_unit,
                                extra_data
                            FROM _transfers
                            WHERE address='{address.lower()}'
                                AND blockNumber>19218200
                            ORDER BY blockNumber ASC, transactionIndex ASC
                            LIMIT {LIMIT_ROWS}
                        """)
                        _rows = cursor.fetchall()
                    except psycopg2.ProgrammingError:
                        _rows = []

                    first_trade_block = None
                    first_block_liquidity = None
                    first_value_usd_per_token_unit = None
                    max_after_30m_value_usd_per_token_unit = 0
                    first_trade_timestamp = None
                    first_30m_value_usd_per_token_unit = []
                    first_30m_avg_value_usd_per_token_unit = None
                    max_price_change_from_first_block = 0
                    max_price_change_after_30_m = 0
                    max_liquidity = 0
                    max_liquidity_left = 0
                    hours_past = None
                    liquidity = { # ликвидность после стольких часов с начала первого трейда
                        '10m': {'liquidity': None, 'price_change': None, f'max_price_change_after_{MINUTES_BACKTEST}_m': 0, 'liquidity_left': None},
                        '1h': {'liquidity': None, 'price_change': None, f'max_price_change_after_{MINUTES_BACKTEST}_m': 0, 'liquidity_left': None},
                        '6h': {'liquidity': None, 'price_change': None, f'max_price_change_after_{MINUTES_BACKTEST}_m': 0, 'liquidity_left': None},
                        '16h': {'liquidity': None, 'price_change': None, f'max_price_change_after_{MINUTES_BACKTEST}_m': 0, 'liquidity_left': None},
                        'max': {'liquidity': None, 'price_change': None, f'max_price_change_after_{MINUTES_BACKTEST}_m': 0, 'liquidity_left': None},
                        '1month': {'liquidity': None, 'price_change': None, f'max_price_change_after_{MINUTES_BACKTEST}_m': 0, 'liquidity_left': None},
                        'last': {'liquidity': None, 'price_change': None, f'max_price_change_after_{MINUTES_BACKTEST}_m': 0, 'liquidity_left': None},
                    }

                    # пример токена, где было много Transfers() до первого Mint(): 0x5ca83216Fae72717332469E6A2eb28c4BF9AF9ec

                    temp_first = None
                    for i, row in enumerate(_rows):
                        #print(f"#{i}")
                        #pprint(dict(row))
                        if first_block_liquidity == None and row['extra_data'] != None:
                            first_block_liquidity = row['extra_data']['rp']
                            #print(i, row['blocknumber'], datetime.fromtimestamp(row['timestamp']), 'first_block_liquidity:', first_block_liquidity)
                        if row['value_usd_per_token_unit'] != None and first_value_usd_per_token_unit == None:
                            if not temp_first:
                                #print(i, row['blocknumber'], datetime.fromtimestamp(row['timestamp']), 'first_value_usd_per_token_unit!', "row['extra_data']:", row['extra_data'], 'value_usd_per_token_unit:', row['value_usd_per_token_unit'])

                                temp_first = {
                                    #'first_block_liquidity': row['extra_data']['rp'],
                                    'first_trade_block': row['blocknumber'],
                                    'first_value_usd_per_token_unit': row['value_usd_per_token_unit'],
                                    'first_trade_timestamp': row['timestamp'],
                                }
                                continue

                            else:
                                if row['value_usd_per_token_unit'] != temp_first['first_value_usd_per_token_unit']:
                                    if row['blocknumber'] - temp_first['first_trade_block'] == 1:
                                        #first_block_liquidity = temp_first['first_block_liquidity']
                                        first_trade_block = temp_first['first_trade_block']
                                        first_value_usd_per_token_unit = temp_first['first_value_usd_per_token_unit']
                                        first_trade_timestamp = temp_first['first_trade_timestamp']

                                        #print(i, row['blocknumber'], datetime.fromtimestamp(row['timestamp']), 'первый трейд; засчитываем переменные из прошлого блока', 'value_usd_per_token_unit:', row['value_usd_per_token_unit'])
                                    else:
                                        #first_block_liquidity = row['extra_data']['rp']
                                        first_trade_block = row['blocknumber']
                                        first_value_usd_per_token_unit = row['value_usd_per_token_unit']
                                        first_trade_timestamp = row['timestamp']

                        if first_block_liquidity and first_value_usd_per_token_unit != None and row['value_usd_per_token_unit'] != None:

                            if first_30m_avg_value_usd_per_token_unit == None and (row['blocknumber'] - first_trade_block) * 12 / 60 >= MINUTES_BACKTEST:
                                # first_30m_avg_value_usd_per_token_unit = statistics.mean(first_30m_value_usd_per_token_unit)
                                first_30m_avg_value_usd_per_token_unit = _rows[i - 1]['value_usd_per_token_unit']
                                #print(i, row['blocknumber'], 'MINUTES_BACKTEST has past!', datetime.fromtimestamp(row['timestamp']), 'first_30m_avg_value_usd_per_token_unit changed from None to ', _rows[i - 1]['value_usd_per_token_unit'])

                            if first_30m_avg_value_usd_per_token_unit != None:
                                if max_after_30m_value_usd_per_token_unit < row['value_usd_per_token_unit']:
                                    max_after_30m_value_usd_per_token_unit = row['value_usd_per_token_unit']

                                if max_price_change_after_30_m < float(100 * max_after_30m_value_usd_per_token_unit / first_30m_avg_value_usd_per_token_unit - 100):
                                    max_price_change_after_30_m = float(100 * max_after_30m_value_usd_per_token_unit / first_30m_avg_value_usd_per_token_unit - 100)

                            price_change_from_first_block = float(100 * row['value_usd_per_token_unit'] / first_value_usd_per_token_unit - 100)
                            hours_past = (row['timestamp'] - first_trade_timestamp) / 60 / 60
                            if price_change_from_first_block > max_price_change_from_first_block:
                                max_price_change_from_first_block = price_change_from_first_block
                            if row['extra_data'] != None:
                                if max_liquidity < row['extra_data']['rp']: max_liquidity = row['extra_data']['rp']
                                if max_liquidity_left < row['extra_data']['rp'] - first_block_liquidity: max_liquidity_left = row['extra_data']['rp'] - first_block_liquidity
                                _data = {'liquidity': row['extra_data']['rp'], 'price_change': price_change_from_first_block, f'max_price_change_after_{MINUTES_BACKTEST}_m': max_price_change_after_30_m, 'liquidity_left': max([x['extra_data']['rp'] for x in _rows[max(i - 15,0):i + 1] if x['extra_data'] != None]) - first_block_liquidity}
                                if hours_past >= 10/60 and liquidity['10m']['liquidity'] == None: liquidity['10m'] = _data
                                if hours_past >= 1 and liquidity['1h']['liquidity'] == None: liquidity['1h'] = _data
                                if hours_past >= 6 and liquidity['6h']['liquidity'] == None: liquidity['6h'] = _data
                                if hours_past >= 16 and liquidity['16h']['liquidity'] == None: liquidity['16h'] = _data
                                if hours_past >= 24*30 and liquidity['1month']['liquidity'] == None: liquidity['1month'] = _data


                                if (row['blocknumber'] - first_trade_block) * 12 / 60 / 60 > 6: # hours
                                    continue

                                if (row['blocknumber'] - first_trade_block) * 12 / 60 / 60 / 24 > 31.0: # days
                                    break

                                if first_30m_avg_value_usd_per_token_unit:
                                    if liquidity['max']['liquidity'] == None:
                                        liquidity['max'] = _data
                                    else:
                                        if liquidity['max']['liquidity'] < _data['liquidity']:
                                            liquidity['max']['liquidity'] = _data['liquidity']
                                        if liquidity['max']['price_change'] < _data['price_change']:
                                            liquidity['max']['price_change'] = _data['price_change']
                                        if liquidity['max'][f'max_price_change_after_{MINUTES_BACKTEST}_m'] < _data[f'max_price_change_after_{MINUTES_BACKTEST}_m']:
                                            liquidity['max'][f'max_price_change_after_{MINUTES_BACKTEST}_m'] = _data[f'max_price_change_after_{MINUTES_BACKTEST}_m']
                                        if liquidity['max']['liquidity_left'] < _data['liquidity_left']:
                                            liquidity['max']['liquidity_left'] = _data['liquidity_left']

                    cursor.execute(f"""
                        SELECT
                            transactionHash,
                            blockNumber,
                            timestamp,
                            from_ as from,
                            to_ as to,
                            value_usd,
                            value_usd_per_token_unit,
                            extra_data
                        FROM _transfers
                        WHERE address='{address.lower()}'
                            AND blockNumber>19218200
                        ORDER BY blockNumber DESC, transactionIndex DESC
                        LIMIT 10
                    """)
                    _rows_backward = cursor.fetchall()
                    for i, row in enumerate(_rows_backward):
                        if first_value_usd_per_token_unit != None and row['value_usd_per_token_unit'] != None:
                            price_change_from_first_block = float(100 * row['value_usd_per_token_unit'] / first_value_usd_per_token_unit - 100)
                            if row['extra_data'] != None and first_block_liquidity != None:
                                liquidity['last'] = {'liquidity': row['extra_data']['rp'], 'price_change': price_change_from_first_block, f'max_price_change_after_{MINUTES_BACKTEST}_m': float(100 * row['value_usd_per_token_unit'] / first_30m_avg_value_usd_per_token_unit - 100) if first_30m_avg_value_usd_per_token_unit else 0, 'liquidity_left': row['extra_data']['rp'] - first_block_liquidity}
                                break

                    if len(_rows) < LIMIT_ROWS:
                        if liquidity['16h']['liquidity'] == None: liquidity['16h'] = liquidity['last']
                        if liquidity['6h']['liquidity'] == None: liquidity['6h'] = liquidity['last']
                        if liquidity['1h']['liquidity'] == None: liquidity['1h'] = liquidity['last']
                        if liquidity['10m']['liquidity'] == None: liquidity['10m'] = liquidity['last']
                    elif len(_rows) == LIMIT_ROWS:
                        if liquidity['16h']['liquidity'] == None and liquidity['6h']['liquidity'] != None: liquidity['16h'] = liquidity['max']


            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            print(f"psycopg2 error: {e}")
            time.sleep(8)

        
    # print(f"ending pid {os.getpid()}")

    return {
        'pi': pi,
        'address': address,
        'rows': len(_rows),
        'max_price_change_from_first_block': max_price_change_from_first_block,
        'max_liquidity': max_liquidity,
        'max_liquidity_left': max_liquidity_left,
        'hours_past': hours_past,
        'liquidity': liquidity,
    }



def liq_locked(kwards):

    # print(f"starting pid {os.getpid()}, {kwards}")

    pi = kwards[0]
    address = kwards[1]

    # time.sleep(random.randint(1,5))


    with psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs) as conn:

        txs_sql_token_owners = utils.get_token_owners(conn, address=address, last_block=LAST_BACKTESTING_BLOCK, verbose=0)
        if txs_sql_token_owners and txs_sql_token_owners[-1]['topics_2'] in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:
            return address


    return None



def find_addresses():

    global conn

    
    print(f"loading {FOLDER_RANGE} range MINUTES_BACKTEST={MINUTES_BACKTEST}")

    try:

        # raise FileNotFoundError

        with open(f"{FILES_DIR}/temp/collected_addresses_{FOLDER_RANGE}.json", "r") as file:
            list_addresses = json.load(file)


    except FileNotFoundError:

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                SELECT blockNumber, pair_created, topics_1, topics_2
                FROM _logs
                WHERE pair_created is not null
                    AND blockNumber>{FIRST_BACKTESTING_BLOCK}
                    AND blockNumber<{LAST_BACKTESTING_BLOCK}
                ORDER BY blockNumber ASC
            """)
            _rows = cursor.fetchall()

            list_addresses = []

            print('len rows:', len(_rows))
            for row in tqdm(_rows):
                if row['topics_2'] in [utils.WETH.lower(), utils.USDT.lower(), utils.USDC.lower()]:
                    address = row['topics_1']
                elif row['topics_1'] in [utils.WETH.lower(), utils.USDT.lower(), utils.USDC.lower()]:
                    address = row['topics_2']
                else:
                    print(f"Ошибка в связке, topics_1={row['topics_1']}, topics_2={row['topics_2']}")
                    continue

                if address in list_addresses:
                    print(f"Адрес {address} уже есть в списке")
                    continue

                # if utils.have_mints_before(conn, pair=row['pair_created'], until_block=19218200):
                #     print(f"Mint() этой пары ({row['pair_created']}) был ранее")
                #     continue

                # if address == '0x7ae0f19d2ae2f490e710579284a58000d4e8c85f':
                #     if utils.have_mints_before(conn, pair=row['pair_created'], until_block=19218200):
                #         print('yes')
                #     else:
                #         print('no')
                #     exit()

                list_addresses.append(address)

            with open(f"{FILES_DIR}/temp/collected_addresses_{FOLDER_RANGE}.json", "w") as file:
                json.dump(list_addresses, file)


    print(f"collected {len(list_addresses)} addresses")



    params = []
    for i, address in enumerate(list_addresses[:]):
        params.append((i, address,))


    

    
    with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=None),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            expand=True,) as pbar:

        pbar_task = pbar.add_task("loop", total=len(params))



        with Pool(min(len(params), 75)) as pool:
            stat_results = []
            for result in pool.imap_unordered(calculate_token, params):
                stat_results.append(result)
                pbar.update(pbar_task, advance=1)


    stat_results = sorted(stat_results, key=lambda d: d['pi'])

    with open(f"{FILES_DIR}/temp/tokensniffer_results.json", "r") as file:
        tokensniffer_results = json.load(file)

    modified_w = []
    good_tokens = []
    high_profit_tokens = []
    not_scam_tokens = []
    not_scam_tokens_detailed = []
    scam_tokens = []
    tokens_500rows_less = []
    #tokens_1000_plus = []
    for i, stat in enumerate(stat_results):

        # pprint(stat)
        # continue

        MIN_TRADES = 500

        if stat['rows'] < MIN_TRADES:
            tokens_500rows_less.append(stat['address'])
            #continue
            
        #tokens_1000_plus.append(stat['address'])
        # continue

        a_c = {}
        a_c['i'] = i
        a_c['address'] = stat['address']
        # a_c['atl'] = 1 if list_results[stat['pi']] else 0
        a_c['rows'] = stat['rows']
        # a_c['hours'] = f"{stat['hours_past']:.2f}" if stat['hours_past'] else None

        # if not stat['hours_past']:
        #     print(f"У адреса {stat['address']} не было первого трейда")

        a_c['max_c'] = None
        if stat['max_price_change_from_first_block'] != None:
            if stat['max_price_change_from_first_block'] < 5000000:
                a_c['max_c'] = f"{utils.human_format(round(stat['max_price_change_from_first_block']))}%"
            else:
                a_c['max_c'] = f"5m+%"

            
        # a_c['max_l'] = None
        # if stat['max_liquidity_left'] != 0:
        #     a_c['max_l'] = f"{utils.human_format(round(stat['max_liquidity_left']))} $"

        for key in ['10m', '1h', '16h', 'max', '1month']: # 6h
            a_c[key] = None
            if stat['liquidity'][key]['liquidity_left'] != None:
                _xtr = ''
                if key in ('last', '1month'):
                    _xtr = f", liq={round(stat['liquidity'][key]['liquidity'])} $"
                if stat['liquidity'][key][f'max_price_change_after_{MINUTES_BACKTEST}_m'] < 5000000:
                    a_c[key] = f"{stat['liquidity'][key]['liquidity_left']:.2f} $ / {round(stat['liquidity'][key][f'max_price_change_after_{MINUTES_BACKTEST}_m'])} %{_xtr}"
                else:
                    a_c[key] = f"{stat['liquidity'][key]['liquidity_left']:.2f} $ / 5m+ %{_xtr}"
            

        abi = utils.get_abi(stat['address'])
        a_c['vrf'] = 1 if abi and 'error' not in abi else 0

        goplus = utils.check_gopluslabs(stat['address'], debug=0)
        a_c['gpl'] = 1 if goplus and 'scam' in goplus and not goplus['scam'] else 0

        honeypot = utils.check_honeypot(stat['address'], debug=0)
        a_c['hny'] = 1 if honeypot and 'scam' in honeypot and not honeypot['scam'] else 0

        """
        v24.06
        if  a_c['gpl'] == 1 and \
            a_c['hny'] == 1 and \
            stat['max_price_change_from_first_block'] < 5000000 and \
            stat['max_liquidity'] >= 20000 and \
            stat['liquidity']['last']['liquidity'] != None and \
            stat['rows'] >= MIN_TRADES and \
            ((
                stat['rows'] >= 10000 and \
                stat['max_price_change_from_first_block'] > 500 and \
                (
                    stat['liquidity']['1h']['liquidity'] != None and \
                    stat['liquidity']['1h']['liquidity'] > 5000 and \
                    stat['liquidity']['1h']['price_change'] > 500 \
                    or \
                    stat['liquidity']['16h']['liquidity'] != None and \
                    stat['liquidity']['16h']['liquidity'] > 10000 and \
                    stat['liquidity']['16h']['price_change'] > 500
                )
            or \
                stat['rows'] < 10000 and \
                stat['max_price_change_from_first_block'] > 1000 and \
                (
                    stat['liquidity']['10m']['liquidity'] != None and \
                    stat['liquidity']['10m']['liquidity'] > 3000 and \
                    stat['liquidity']['10m']['price_change'] > 100
                    or \
                    stat['liquidity']['1h']['liquidity'] != None and \
                    stat['liquidity']['1h']['liquidity'] > 7000 and \
                    stat['liquidity']['1h']['price_change'] > 200
                ) and \
                1
                # stat['liquidity']['16h']['liquidity'] != None and \
                # stat['liquidity']['16h']['liquidity'] > 20000 and \
                # stat['liquidity']['16h']['price_change'] > 500
            )):

            a_c['mark'] = 'green'
            good_tokens.append(stat['address'])
        """



        """
        v02.07
        """
        """
        if  a_c['gpl'] == 1 and \
            a_c['hny'] == 1 and \
            stat['max_price_change_from_first_block'] < 5000000 and \
            stat['max_liquidity'] >= 10000 and \
            stat['liquidity']['last']['liquidity'] != None and \
            stat['rows'] >= MIN_TRADES and \
            ((
                stat['rows'] >= 10000 and \
                stat['max_price_change_from_first_block'] > 500 and \
                (
                    stat['liquidity']['1h']['liquidity'] != None and \
                    stat['liquidity']['1h']['liquidity'] > 5000 and \
                    stat['liquidity']['1h']['price_change'] > 500 \
                    or \
                    stat['liquidity']['16h']['liquidity'] != None and \
                    stat['liquidity']['16h']['liquidity'] > 10000 and \
                    stat['liquidity']['16h']['price_change'] > 500
                )
            or \
                stat['rows'] < 10000 and \
                stat['max_price_change_from_first_block'] > 300 and \
                (
                    stat['liquidity']['10m']['liquidity'] != None and \
                    stat['liquidity']['10m']['liquidity'] > 3000 and \
                    stat['liquidity']['10m']['price_change'] > 200
                    or \
                    stat['liquidity']['1h']['liquidity'] != None and \
                    stat['liquidity']['1h']['liquidity'] > 7000 and \
                    stat['liquidity']['1h']['price_change'] > 200
                ) and \
                1
                # stat['liquidity']['16h']['liquidity'] != None and \
                # stat['liquidity']['16h']['liquidity'] > 20000 and \
                # stat['liquidity']['16h']['price_change'] > 500
            )):

            # a_c['mark'] = 'green'
            good_tokens.append(stat['address'])
        """
        

        if  (a_c['gpl'] == 1 or stat['rows'] >= LIMIT_ROWS) and \
            a_c['hny'] == 1 and \
            (
                stat['liquidity']['last']['liquidity'] != None and stat['liquidity']['last']['liquidity'] >= 500 \
                or stat['liquidity']['1month']['liquidity'] != None and stat['liquidity']['1month']['liquidity'] >= 500
            ):

            not_scam_tokens.append(stat['address'])
            not_scam_tokens_detailed.append({
                'token_address': stat['address'],
                f'max_price_change_after_{MINUTES_BACKTEST}_m': round(stat['liquidity']['max'][f'max_price_change_after_{MINUTES_BACKTEST}_m'], 2),
                'rows': stat['rows'],
                '1h_liquidity_left': stat['liquidity']['1h']['liquidity_left'],
                '6h_liquidity_left': stat['liquidity']['6h']['liquidity_left'],
                '16h_liquidity_left': stat['liquidity']['16h']['liquidity_left'],
                '1month_liquidity_left': stat['liquidity']['1month']['liquidity_left'],
                'max_liquidity_left': stat['liquidity']['max']['liquidity_left'],
                'last_liquidity_left': stat['liquidity']['last']['liquidity_left'],
            })
        
        if  (a_c['gpl'] == 0 and stat['rows'] < LIMIT_ROWS) or \
            a_c['hny'] == 0 or \
            stat['rows'] < LIMIT_ROWS and (stat['liquidity']['1month']['liquidity'] == None or stat['liquidity']['1month']['liquidity'] < 500) or \
            stat['rows'] == LIMIT_ROWS and (stat['liquidity']['last']['liquidity'] == None or stat['liquidity']['last']['liquidity'] < 500):

            scam_tokens.append(stat['address'])
        
        """
        ver < 26.10.2024
        if  a_c['gpl'] == 1 and \
            a_c['hny'] == 1 and \
            stat['max_price_change_from_first_block'] < 5000000 and \
            stat['max_liquidity'] >= 10000 and \
            stat['liquidity']['last']['liquidity'] != None and \
            stat['rows'] >= 5000 and \
            stat['max_price_change_from_first_block'] > 1000 and \
            (
                stat['liquidity']['1h']['liquidity'] != None and \
                stat['liquidity']['1h']['liquidity'] > 5000 and \
                stat['liquidity']['1h']['price_change'] > 500 \
                or \
                stat['liquidity']['16h']['liquidity'] != None and \
                stat['liquidity']['16h']['liquidity'] > 20000 and \
                stat['liquidity']['16h']['price_change'] > 500
            ):

            a_c['mark'] = 'green'
            high_profit_tokens.append(stat['address'])
        """


        if stat['rows'] >= LIMIT_ROWS and a_c['hny'] == 0 and (stat['liquidity']['last']['liquidity'] != None and stat['liquidity']['last']['liquidity'] > 500):
            print(f"stat['rows']={stat['rows']}, but a_c['gpl']={a_c['gpl']}, a_c['hny']={a_c['hny']}, stat['liquidity]['last']['liquidity']={stat['liquidity']['last']['liquidity']}, address={stat['address']}")

        """
        v26.10.2024
        """
        if  a_c['gpl'] == 1 and \
            a_c['hny'] == 1 and \
            stat['rows'] >= 3000 and \
            stat['liquidity']['max']['liquidity_left'] != None and \
            stat['liquidity']['max']['liquidity_left'] >= 10000 and \
            stat['liquidity']['max'][f'max_price_change_after_{MINUTES_BACKTEST}_m'] >= 500 and \
            stat['liquidity']['last']['liquidity_left'] != None and \
            stat['liquidity']['last']['liquidity_left'] > 500 and \
            (stat['rows'] >= LIMIT_ROWS or \
                stat['liquidity']['16h']['liquidity_left'] != None and \
                stat['liquidity']['16h']['liquidity_left'] > 500
            ):
            
            a_c['mark'] = 'green'
            high_profit_tokens.append(stat['address'])



        modified_w.append(a_c)


    for j in range(len(modified_w)):
        if 'mark' in modified_w[j] and modified_w[j]['mark'] == 'green':
            for it, vv in modified_w[j].items():
                if it == 'vrf' and modified_w[j]['vrf'] == 0 or \
                    it == 'gpl' and modified_w[j]['gpl'] == 0 or \
                    it == 'hny' and modified_w[j]['hny'] == 0 or \
                    it == 'tsf' and modified_w[j]['tsf'] == 0:
                    modified_w[j][it] = f"\33[105m" + "0" + "\033[0m"
                elif it == 'last' and modified_w[j]['last'] == None:
                    # modified_w[j][it] = f"\x1b[31;1m" + "?" + "\x1b[0m" # red
                    modified_w[j][it] = f"\33[105m" + "?" + "\033[0m" # highlighed pink
                elif modified_w[j][it] != None:
                    # modified_w[j][it] = f"\x1b[31;1m{str(vv)}\x1b[0m" # red
                    modified_w[j][it] = f"\x1b[35;1m{str(vv)}\x1b[0m" # purple
            del modified_w[j]['mark']


    print(tabulate.tabulate(modified_w[:], tablefmt='psql', headers='keys', stralign="right"))

    print(f'good tokens ({len(good_tokens)}):')
    for nst in good_tokens:
        print(nst)

    # print('not_scam_tokens:')
    # print(not_scam_tokens)
    # exit()

    result_csv = []
    for address in list_addresses:
        result_csv.append({'token_address': address, 'TARGET': 1 if address in good_tokens else 0})

    df = pd.DataFrame(result_csv)
    print(df)
    df.to_csv(f"{FILES_DIR}/temp/{FOLDER_RANGE}_target-column.csv", encoding='utf-8', index=False)


    print(f'all tokens ({len(list_addresses)})')
    with open(f"{FILES_DIR}/temp/{FOLDER_RANGE}_all_tokens.txt", "w") as file:
        json.dump(list_addresses, file)
    
    print(f'tokens 500 rows less ({len(tokens_500rows_less)})')
    # print(tokens_500rows_less)
    with open(f"{FILES_DIR}/temp/{FOLDER_RANGE}_tokens_500rows_less.txt", "w") as file:
        json.dump(tokens_500rows_less, file)

    print(f'scam tokens ({len(scam_tokens)}):')
    # print(scam_tokens)
    with open(f"{FILES_DIR}/temp/{FOLDER_RANGE}_scam_tokens.txt", "w") as file:
        json.dump(scam_tokens, file)

    print(f'not scam tokens ({len(not_scam_tokens)}):')
    # print(not_scam_tokens)
    with open(f"{FILES_DIR}/temp/{FOLDER_RANGE}_not_scam_tokens.txt", "w") as file:
        json.dump(not_scam_tokens, file)
    pd.DataFrame(not_scam_tokens_detailed).to_csv(f"{FILES_DIR}/temp/{FOLDER_RANGE}_not_scam_tokens_{MINUTES_BACKTEST}m_detailed.csv", index=False)
    
    fall_fast_tokens = list(set(not_scam_tokens) - set(high_profit_tokens) - set(good_tokens))
    # print(f'fall fast tokens ({len(fall_fast_tokens)}):')
    # print(fall_fast_tokens)
    with open(f"{FILES_DIR}/temp/{FOLDER_RANGE}_fall_fast_tokens.txt", "w") as file:
        json.dump(fall_fast_tokens, file)
        
    print(f'high profit tokens ({len(high_profit_tokens)}):')
    print(high_profit_tokens)
    with open(f"{FILES_DIR}/temp/{FOLDER_RANGE}_high_profit_tokens_{MINUTES_BACKTEST}m.txt", "w") as file:
        json.dump(high_profit_tokens, file)


    _high_profit_tokens_test = [
        '0x8802269D1283cdB2a5a329649E5cB4CdcEE91ab6',
        '0x790814Cd782983FaB4d7B92CF155187a865d9F18',
        '0x42069cc15F5BEfb510430d22Ff1c9A1b3ae22CfE',
        '0xfefe157c9d0aE025213092ff9a5cB56ab492BaB8',
        '0xB8A914A00664e9361eaE187468EfF94905dfbC15',
        '0x683A4ac99E65200921f556A19dADf4b0214B5938',
        '0x636bd98fc13908e475f56d8a38a6e03616ec5563',
        '0xD875a85C5Fe61a84fe6bdB16644B52118fDb8Dfd',
        '0xce0CA7A150a441Cc52415526F2EaC97Aa4140D13',
        '0x07040971246a73ebDa9Cf29ea1306bB47C7C4e76',
        '0x4aBD5745F326932B1b673bfa592a20d7BB6bc455',
        '0x777BE1c6075c20184C4fd76344b7b0B7c858fe6B',
        '0xcee99db49fe7B6e2d3394D8df2989B564bB613DB',
        '0x5B1543C4EA138eFAE5b0836265dfF75e1Ab6227D',
        '0xb8D6196D71cdd7D90A053a7769a077772aAac464',
        '0x6dB6FDb5182053EECeC778aFec95E0814172A474',
        '0x5200B34E6a519F289f5258dE4554eBd3dB12E822',
        '0x66b5228CfD34d9f4d9f03188d67816286C7c0b74',
        '0x230EA9AEd5d08AfDb22Cd3c06c47cf24aD501301',
        '0x42069026EAC8Eee0Fd9b5f7aDFa4f6E6D69a2B39',
        '0xF63E309818E4EA13782678CE6C31C1234fa61809',
        '0x594DaaD7D77592a2b97b725A7AD59D7E188b5bFa',
        '0x812Ba41e071C7b7fA4EBcFB62dF5F45f6fA853Ee',
        '0x5200B34E6a519F289f5258dE4554eBd3dB12E822',
        '0xb612bFC5cE2FB1337Bd29F5Af24ca85DbB181cE2',
        '0xCeD88f3Ca5034AbFC1F5492a808a39eE2B16F321',
        '0xc56C7A0eAA804f854B536A5F3D5f49D2EC4B12b8',
        '0x1121AcC14c63f3C872BFcA497d10926A6098AAc5',
        '0xD29DA236dd4AAc627346e1bBa06A619E8c22d7C5',
        '0xeBb66a88cEdd12bfE3a289df6DFEe377F2963F12',
        '0xc8F69A9B46B235DE8d0b77c355FFf7994F1B090f',
        '0xEE2a03Aa6Dacf51C18679C516ad5283d8E7C2637',
        '0xe5018913F2fdf33971864804dDB5fcA25C539032',
        '0xb3E41D6e0ea14b43BC5DE3C314A408af171b03dD',
        '0x6E79B51959CF968d87826592f46f819F92466615',
        '0x41D06390b935356b46aD6750bdA30148Ad2044A4',
        '0x391cF4b21F557c935C7f670218Ef42C21bd8d686',
        '0x3fFEea07a27Fab7ad1df5297fa75e77a43CB5790',
        '0x4e6221c07DAE8D3460a46fa01779cF17FdD72ad8',
        '0xd1A6616F56221A9f27eB9476867b5bDB2B2d101d',
        '0x0000e3705d8735ee724a76F3440c0a7ea721eD00',
        '0x289Ff00235D2b98b0145ff5D4435d3e92f9540a6',
        '0xB3912b20b3aBc78C15e85E13EC0bF334fbB924f7',
        '0xCb76314C2540199f4B844D4ebbC7998C604880cA',
        '0x240Cd7b53d364a208eD41f8cEd4965D11F571B7a',
        '0x67466BE17df832165F8C80a5A120CCc652bD7E69',
        '0x1495bc9e44Af1F8BCB62278D2bEC4540cF0C05ea',
        '0x69babE9811CC86dCfC3B8f9a14de6470Dd18EDA4',
        '0xeD89fc0F41d8Be2C98B13B7e3cd3E876D73f1d30',
        '0x5981E98440E41fa993B26912B080922b8Ed023c3',
        '0xfefe157c9d0aE025213092ff9a5cB56ab492BaB8',
        '0x8bAF5d75CaE25c7dF6d1E0d26C52d19Ee848301a',
        '0x28561B8A2360F463011c16b6Cc0B0cbEF8dbBcad',
        '0x6969f3A3754AB674B48B7829A8572360E98132Ba',
        '0x155788DD4b3ccD955A5b2D461C7D6504F83f71fa',
        '0x36C7188D64c44301272Db3293899507eabb8eD43',
        '0xCE176825AFC335d9759cB4e323eE8b31891DE747',
        '0x00F116ac0c304C570daAA68FA6c30a86A04B5C5F',
        '0x95Ed629B028Cf6AADd1408Bb988c6d1DAAbe4767',
        '0xed2D1Ef84a6a07dD49E8cA934908E9De005b7824',
        '0xa00453052A36D43A99Ac1ca145DFe4A952cA33B8',
        '0x4C44A8B7823B80161Eb5E6D80c014024752607F2',
        '0x13E4b8CfFe704d3De6F19E52b201d92c21EC18bD',
        '0x4AdE2b180f65ed752B6F1296d0418AD21eb578C0',
        '0xE81C4A73bfDdb1dDADF7d64734061bE58d4c0b4C',
        '0x71297312753EA7A2570a5a3278eD70D9a75F4f44',
        '0x85bEA4eE627B795A79583FcedE229E198aa57055',
        '0x9D09bCF1784eC43f025d3ee071e5b632679a01bA', # TEE
        '0x556c3cbDCA77a7f21AFE15b17e644e0e98e64Df4', # MAO
        '0xd6203889C22D9fe5e938a9200f50FDFFE9dD8e02', # SBR
    ]
    for token in _high_profit_tokens_test:
        token = token.lower()
        if token in list_addresses:
            if token in high_profit_tokens:
                print(f"+ high profit {token}")
            else:
                print(f"- high profit {token}")




if __name__ == '__main__':



    FOLDER_RANGE = "13feb_14nov"
    FIRST_BACKTESTING_BLOCK = 19218200
    LAST_BACKTESTING_BLOCK = 21185600



    """
    target=0, хотя должен 1
    """
    # 0x9b465aab9e8f325418f222c37de474b1bd38ded2

    """
    scam с ошибками
    """
    # 0xDb4B3A261B4Da8b3909E80bAEf4d4837c43cd850 - по goplus Potential risk of external call
    # 0x4Ac20ed3477AD24F3eB4e1bd7B81468Ed1D1F6b1 - тоже самое

    """
    scam, а определило как not_scam_tokens
    """
    # 0xBD993c4002B06117E17416db5A7676aD2D040622

    """
    not scam, а определано как scam_tokens
    """
    # 0xb6B6C0172dA88A493bd36E935Ec1573ef863C653
    # 0xb538d9f3e1Ae450827618519ACd96086Fc4C0a59

    try:
        find_addresses()
    finally:
        conn.close()
