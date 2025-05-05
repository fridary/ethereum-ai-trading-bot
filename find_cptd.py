import os
import sys
import requests
import json
import traceback
import time
import pickle
import random
import string
from web3 import Web3
from web3.logs import DISCARD
from web3._utils.events import get_event_data
from web3_input_decoder import decode_constructor, decode_function
from pprint import pprint
import statistics
from eth_utils import to_checksum_address, from_wei
from hexbytes import HexBytes
from datetime import datetime
from functools import cached_property
from multiprocessing import Pool
from colorama import Fore, Back, Style
import psutil
import copy
import tabulate
tabulate.PRESERVE_WHITESPACE = True
from rich.progress import (BarColumn, MofNCompleteColumn, Progress, TaskProgressColumn, TextColumn,
                           TimeElapsedColumn, TimeRemainingColumn)
import gc

import psycopg2
import psycopg2.extras
from psycopg2.extras import Json, DictCursor, RealDictCursor
psycopg2.extras.register_uuid()

import utils
from utils import FILES_DIR
from wallet_profit import Wallet



# conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)


def launch(kwards):

    #start_block = kwards[0]
    start_block = kwards

    print(f"[#{os.getpid()}] {(psutil.Process().memory_info().rss / (1024 ** 2)):.2f} Mb process start")

    with psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:

            start_time = time.time()
            try:
                sql = f"""
                    select
                        t1.blockNumber,
                        array_agg(JSON_BUILD_OBJECT('transaction_to',t2.to_,'address',t1.address,'transactionHash',t1.transactionhash,'transaction_id',t1.transaction_id,'from',t1.from_,'to',t1.to_,'value',t1.value,'value_usd',t1.value_usd,'value_usd_per_token_unit',t1.value_usd_per_token_unit,'extra_data',t1.extra_data,'category',t1.category,'logindex',t1.logindex)) transfers_array
                    FROM _transfers t1
                    left join _transactions t2 on t2.id = t1.transaction_id
                    WHERE t1.blockNumber>={start_block}
                    and t1.blockNumber<={start_block + BLOCK_LOOP_N}
                    group by t1.blockNumber
                """
                cursor.execute(sql)
                _rows = cursor.fetchall()
            except psycopg2.ProgrammingError:
                _rows = []
            # print(f"[#{os.getpid()}] {(time.time() - start_time):.3f}s select _transfers")

    print(f'[#{os.getpid()}]', 'start_block:', start_block, f"end block: {start_block + BLOCK_LOOP_N}", 'rows:', len(_rows))
    # for row in _rows:
    #     print('--')
    #     print(row)

    print(f"[#{os.getpid()}] {(psutil.Process().memory_info().rss / (1024 ** 2)):.2f} Mb process after db, _rows size: {(sys.getsizeof(_rows) / (1024 ** 2)):.2f} Mb")


    tokens = {}
    wallets = {} # ключ - адрес кошелька; value - состоит из кошелька повторов и его параметры
    uniq_addresses_transactions_hashes = {} # key - кошелек, value - dict из hash транзакций
    uniq_tokens_addresses = {} # key - кошелек, value - dict из tokens
    uniq_maestro_transactions_hashes = {} # key - кошелек, value - dict из hash транзакций
    token_repeats_count_hashes = {} # здесь сколько повторов токенов было повторяющимися кошельками; key - кошелек primary, value - dict из key - address, value - dict из key - transaction_id, value - массив value_usd
    price_change = {} # на сколько цена выросла после покупки primary wallet на след блоке; key - кошелек primary, value - dict из key - token, value - dict из key - value_usd_per_token_unit первого блока primary wallet, value - массив из value_usd_per_token_unit след блока (нужно, если на след блоке это значение у transfers вдруг разное)

    for i_, row in enumerate(_rows):
        cc = 0
        msg = ''
        start_ram_mb = psutil.Process().memory_info().rss / (1024 ** 2)
        for trf in row['transfers_array']:

            # утечка RAM fix
            if psutil.Process().memory_info().rss / (1024 ** 2) - start_ram_mb > 1500:
                print(f"  block={row['blocknumber']} [#{os.getpid()}] ({cc}) {(psutil.Process().memory_info().rss / (1024 ** 2)):.2f} Mb выход раньше")
                break

            if trf['to'] != None and trf['category'] == 'erc20' and trf['value_usd_per_token_unit'] != None and trf['address'] not in [utils.WETH.lower(), utils.USDT.lower(), utils.USDC.lower()] and trf['to'] not in [utils.UNISWAP_UNIVERSAL_ROUTER.lower(), utils.UNISWAP_V2_ROUTER.lower(), utils.UNISWAP_V3_ROUTER.lower(), utils.ADDRESS_0XDEAD.lower(), utils.ADDRESS_0X0000.lower()]:
                
                cc += 1

                if trf['address'] not in tokens:
                    tokens[trf['address']] = {}

                if trf['to'] not in uniq_addresses_transactions_hashes:
                    uniq_addresses_transactions_hashes[trf['to']] = {}
                uniq_addresses_transactions_hashes[trf['to']][trf['transactionHash']] = None

                if trf['to'] not in uniq_tokens_addresses:
                    uniq_tokens_addresses[trf['to']] = {}
                uniq_tokens_addresses[trf['to']][trf['address']] = None

                if trf['to'] not in uniq_maestro_transactions_hashes:
                    uniq_maestro_transactions_hashes[trf['to']] = {}
                # Maestro Router 2
                if trf['transaction_to'] == "0x80a64c6D7f12C47B7c66c5B4E20E72bc1FCd5d9e".lower():
                    uniq_maestro_transactions_hashes[trf['to']][trf['transactionHash']] = None

                # покупка
                # последний блок в tokens не добавляем, т.к. он будет добавлен в следующем Pool()
                if row['blocknumber'] != start_block + BLOCK_LOOP_N:
                    if row['blocknumber'] not in tokens[trf['address']]:
                        tokens[trf['address']][row['blocknumber']] = {}
                    # print(f"добавили trf['to']={trf['to']} в tokens")
                    tokens[trf['address']][row['blocknumber']][trf['to']] = trf['value_usd_per_token_unit']



                    if trf['to'] not in price_change:
                        price_change[trf['to']] = {}
                    if trf['address'] not in price_change[trf['to']]:
                        price_change[trf['to']][trf['address']] = {}



                # повторение
                if row['blocknumber'] - 1 in tokens[trf['address']]:

                    for wallet_ in tokens[trf['address']][row['blocknumber'] - 1].keys():
                        if trf['to'] != wallet_:
                            if wallet_ not in wallets:
                                wallets[wallet_] = {}
                            if trf['to'] not in wallets[wallet_]:
                                wallets[wallet_][trf['to']] = []

                            if wallet_ not in token_repeats_count_hashes:
                                token_repeats_count_hashes[wallet_] = {}
                            if trf['address'] not in token_repeats_count_hashes[wallet_]:
                                token_repeats_count_hashes[wallet_][trf['address']] = {}
                            if trf['transaction_id'] not in token_repeats_count_hashes[wallet_][trf['address']]:
                                token_repeats_count_hashes[wallet_][trf['address']][trf['transaction_id']] = []
                            token_repeats_count_hashes[wallet_][trf['address']][trf['transaction_id']].append(round(trf['value_usd'], 2))

                            if wallet_ not in price_change:
                                price_change[wallet_] = {}
                            if trf['address'] not in price_change[wallet_]:
                                price_change[wallet_][trf['address']] = {}
                            value_usd_per_token_unit_previous_block = tokens[trf['address']][row['blocknumber'] - 1][wallet_]
                            if value_usd_per_token_unit_previous_block not in price_change[wallet_][trf['address']]:
                                # какая была цена -> какая стала
                                price_change[wallet_][trf['address']][value_usd_per_token_unit_previous_block] = [trf['value_usd_per_token_unit']]
                            else:
                                # если почему-то в этом же блоке другое value_usd_per_token_unit
                                if trf['value_usd_per_token_unit'] not in price_change[wallet_][trf['address']][value_usd_per_token_unit_previous_block]:
                                    price_change[wallet_][trf['address']][value_usd_per_token_unit_previous_block].append(trf['value_usd_per_token_unit'])

                            found_the_same_trade = False
                            for ii, arr in enumerate(wallets[wallet_][trf['to']]):
                                if arr['to'] == trf['to'] and arr['blocknumber'] == row['blocknumber'] and arr['address'] == trf['address']:
                                    # assert arr['value_usd_per_token_unit'] == trf['value_usd_per_token_unit'], f"{arr}, {trf}"
                                    #if arr['value_usd_per_token_unit'] == trf['value_usd_per_token_unit']:
                                    wallets[wallet_][trf['to']][ii]['value'].append(trf['value'])
                                    wallets[wallet_][trf['to']][ii]['value_usd_per_token_unit'].append(trf['value_usd_per_token_unit'])
                                    wallets[wallet_][trf['to']][ii]['value_usd'].append(round(trf['value_usd'], 2))
                                    found_the_same_trade = True
                                    break
                            if not found_the_same_trade:
                                # if type(trf['value']) != list:
                                x = trf.copy()
                                x['value'] = [x['value']]
                                x['value_usd'] = [round(x['value_usd'], 2)]
                                x['value_usd_per_token_unit'] = [x['value_usd_per_token_unit']]
                                wallets[wallet_][trf['to']].append({'blocknumber': row['blocknumber']} | x)


                # if row['blocknumber'] in (20615587, 20632734, 20619049):
                #     msg += f"  block={row['blocknumber']} [#{os.getpid()}] ({cc}) {(psutil.Process().memory_info().rss / (1024 ** 2)):.2f} Mb добавили erc20\n"



        if i_ % 1 == 0:
            print(f"[#{os.getpid()}] {(psutil.Process().memory_info().rss / (1024 ** 2)):.2f} Mb on i={i_}, block={row['blocknumber']}, transfers len={len(row['transfers_array'])}, erc20={cc}")
        # print(f"blocknumber: {row['blocknumber']}, _transfers: {len(row['transfers_array'])}")


        if msg != '':
            print(msg)



    print(f"[#{os.getpid()}] finished _row loop, {(psutil.Process().memory_info().rss / (1024 ** 2)):.2f} Mb process")
      
    # for wallet_ in wallets.keys():
    #     print(f"wallet: {wallet_}")
    #     for repeated_wallet in wallets[wallet_].keys():
    #         if len(wallets[wallet_][repeated_wallet]) > 1:
    #             print(f"repeated {repeated_wallet} {len(wallets[wallet_][repeated_wallet])} times")
    #             pprint(wallets[wallet_][repeated_wallet])

    
    # print('-'*90)

    uniq_addresses_transactions = {} # key - кошелек, value - count
    uniq_tokens = {} # key - кошелек, value - count
    uniq_maestro_transactions = {} # key - кошелек, value - count

    for wallet_ in uniq_addresses_transactions_hashes.keys():
        uniq_addresses_transactions[wallet_] = len(uniq_addresses_transactions_hashes[wallet_])

    for wallet_ in uniq_tokens_addresses.keys():
        uniq_tokens[wallet_] = len(uniq_tokens_addresses[wallet_])
    
    for wallet_ in uniq_maestro_transactions_hashes.keys():
        uniq_maestro_transactions[wallet_] = len(uniq_maestro_transactions_hashes[wallet_])
    

    print(f"[#{os.getpid()}] {(psutil.Process().memory_info().rss / (1024 ** 2)):.2f} Mb process before del")
    # del _rows
    # del tokens
    # del uniq_addresses_transactions_hashes
    # del uniq_tokens_addresses
    # del uniq_maestro_transactions_hashes
    # gc.collect()
    # print(f"[#{os.getpid()}] {(psutil.Process().memory_info().rss / (1024 ** 2)):.2f} Mb process after del")

    
    # print(f"wallets size: {sys.getsizeof(wallets)}, uniq_addresses_transactions: {sys.getsizeof(uniq_addresses_transactions)}, token_repeats_count_hashes: {sys.getsizeof(token_repeats_count_hashes)}")
    # print(f"total size process #{os.getpid()}: {((sys.getsizeof(wallets)+sys.getsizeof(uniq_addresses_transactions)+sys.getsizeof(uniq_tokens)+sys.getsizeof(uniq_maestro_transactions)+sys.getsizeof(token_repeats_count_hashes)+sys.getsizeof(price_change)) / (1024 ** 2)):.2f} Mb")

    return {'wallets': wallets, 'uniq_addresses_transactions': uniq_addresses_transactions, 'uniq_tokens': uniq_tokens, 'uniq_maestro_transactions': uniq_maestro_transactions, 'token_repeats_count_hashes': token_repeats_count_hashes, 'price_change': price_change}



if __name__ == '__main__':

    # testnet = 'https://eth.merkle.io' # нет forbidden блока
    #testnet = 'https://eth-pokt.nodies.app'
    testnet = 'http://127.0.0.1:9545/?wp'

    w3 = Web3(Web3.HTTPProvider(testnet))
    if not w3.is_connected():
        raise Exception("w3 not connected")

    # LAST_BACKTESTING_BLOCK = 20471442
    LAST_BACKTESTING_BLOCK = w3.eth.block_number
    START_BLOCK = LAST_BACKTESTING_BLOCK - 70000
    BLOCK_LOOP_N = 1000



    try:
        start_time_e = time.time()

        params = []
        for i in range(START_BLOCK, LAST_BACKTESTING_BLOCK, BLOCK_LOOP_N):
            params.append((i,))
        

        # with Progress(
        #     TextColumn("[progress.description]{task.description}"),
        #     BarColumn(bar_width=None),
        #     MofNCompleteColumn(),
        #     TaskProgressColumn(),
        #     TimeElapsedColumn(),
        #     TimeRemainingColumn(),
        #     expand=True,) as pbar:

        #     pbar_task = pbar.add_task("loop", total=len(params))

        #     with Pool(min(len(params), 15)) as pool:
        #         results = []
        #         for result in pool.imap_unordered(launch, params):
        #             results.append(result)
        #             pbar.update(pbar_task, advance=1)

        with Pool(min(len(params), 50)) as pool:
            results = pool.starmap_async(launch, params).get()


        print('progress finished')

        wallets = {}
        uniq_addresses_transactions_v2 = {} # key - кошелек, value - count
        uniq_tokens_v2 = {} # key - кошелек, value - count
        uniq_maestro_transactions = {} # key - кошелек, value - count
        token_repeats_count_hashes = {} # здесь сколько повторов токенов было повторяющимися кошельками; key - кошелек primary, value - dict из key - address, value - dict из key - transaction_id, value - массив value_usd
        price_change = {}

        for _dict in results:
            res = _dict['wallets']

            if uniq_addresses_transactions_v2 == {}:
                uniq_addresses_transactions_v2 = _dict['uniq_addresses_transactions']
            else:
                for uat in _dict['uniq_addresses_transactions'].keys():
                    if uat not in uniq_addresses_transactions_v2: uniq_addresses_transactions_v2[uat] = _dict['uniq_addresses_transactions'][uat]
                    else: uniq_addresses_transactions_v2[uat] += _dict['uniq_addresses_transactions'][uat]

            if uniq_tokens_v2 == {}:
                uniq_tokens_v2 = _dict['uniq_tokens']
            else:
                for uat in _dict['uniq_tokens'].keys():
                    if uat not in uniq_tokens_v2: uniq_tokens_v2[uat] = _dict['uniq_tokens'][uat]
                    else: uniq_tokens_v2[uat] += _dict['uniq_tokens'][uat]
            
            if uniq_maestro_transactions == {}:
                uniq_maestro_transactions = _dict['uniq_maestro_transactions']
            else:
                for uat in _dict['uniq_maestro_transactions'].keys():
                    if uat not in uniq_maestro_transactions: uniq_maestro_transactions[uat] = _dict['uniq_maestro_transactions'][uat]
                    else: uniq_maestro_transactions[uat] += _dict['uniq_maestro_transactions'][uat]

            if token_repeats_count_hashes == {}:
                token_repeats_count_hashes = _dict['token_repeats_count_hashes']
            else:
                for wallet_ in _dict['token_repeats_count_hashes'].keys():
                    if wallet_ not in token_repeats_count_hashes: token_repeats_count_hashes[wallet_] = _dict['token_repeats_count_hashes'][wallet_]
                    else:
                        for token in _dict['token_repeats_count_hashes'][wallet_].keys():
                            if token not in token_repeats_count_hashes[wallet_]:
                                token_repeats_count_hashes[wallet_][token] = {}
                            token_repeats_count_hashes[wallet_][token] |= _dict['token_repeats_count_hashes'][wallet_][token]
            
            if price_change == {}:
                price_change = _dict['price_change']
            else:
                for wallet_ in _dict['price_change'].keys():
                    if wallet_ not in price_change: price_change[wallet_] = _dict['price_change'][wallet_]
                    else:
                        for token in _dict['price_change'][wallet_].keys():
                            if token not in price_change[wallet_]:
                                price_change[wallet_][token] = {}
                            price_change[wallet_][token] |= _dict['price_change'][wallet_][token]

            for primary_wallet, repeated_wallets in res.items():
                if primary_wallet not in wallets:
                    wallets[primary_wallet] = repeated_wallets
                else:
                    for repeated, transfers in repeated_wallets.items():
                        if repeated not in wallets[primary_wallet]:
                            wallets[primary_wallet][repeated] = transfers
                        else:
                            wallets[primary_wallet][repeated] += transfers

        del results

        # print('~'*90)
        uniq_addresses_repeated = {} # key - кошелек, value - сколько repeated кошельков
        uniq_addresses_transactions = {} # key - кошелек, value - dict из hash транзакций
        uniq_tokens_repeated = {} # key - кошелек, value - dict: key - repeated кошелька и value - список повторяющихся адресов
        median_addresses_price_change = {} # key - кошелек, value - значение


        min_times_N = 2 # min times repeated
        min_uniq_addresses = 3
        # min_times_N = 0 # min times repeated
        # min_uniq_addresses = 0



        for wallet_ in wallets.keys():
            # print(f"wallet: {wallet_}")

            how_many_repeated = 0
            for repeated_wallet in wallets[wallet_].keys():


                # (!) тут проработать
                if repeated_wallet not in uniq_addresses_transactions:
                    uniq_addresses_transactions[repeated_wallet] = {}
                for arr in wallets[wallet_][repeated_wallet]:
                    uniq_addresses_transactions[repeated_wallet][arr['transactionHash']] = None

                if len(wallets[wallet_][repeated_wallet]) >= min_times_N:

                    if wallet_ not in uniq_tokens_repeated.keys():
                        uniq_tokens_repeated[wallet_] = {}
                    
                    if repeated_wallet not in uniq_tokens_repeated[wallet_]:
                        uniq_tokens_repeated[wallet_][repeated_wallet] = {}


                    for arr in wallets[wallet_][repeated_wallet]:
                        uniq_tokens_repeated[wallet_][repeated_wallet][arr['address']] = None
                        

                    _len_uni_tokens = len(uniq_tokens_repeated[wallet_][repeated_wallet])
                    # print(f"wallet: {utils.print_address(wallet_)}, repeated {utils.print_address(repeated_wallet)} {len(wallets[wallet_][repeated_wallet])} times, uniq tokens: {_len_uni_tokens}")
                    # pprint(wallets[wallet_][repeated_wallet])

                    if _len_uni_tokens >= min_uniq_addresses:
                        how_many_repeated += 1
            
            # if len(wallets[wallet_]) > 2:
            #     print('wallet_:', wallet_)
            #     pprint(wallets[wallet_])
            #     exit()

            uniq_addresses_repeated[wallet_] = how_many_repeated

            _price_changes = []
            for token in price_change[wallet_].keys():
                if price_change[wallet_][token] != {}:
                    for prev_value, next_values in price_change[wallet_][token].items():
                        _value = 100*(sum(next_values)/len(next_values))/prev_value-100
                        if _value < 50000:
                            _price_changes.append(_value)
            
            median_addresses_price_change[wallet_] = statistics.median(_price_changes) if _price_changes else 0

            # if wallet_ in uniq_tokens_repeated and repeated_wallet in uniq_tokens_repeated[wallet_]:
            #     _len = len(uniq_tokens_repeated[wallet_][repeated_wallet])
            #     print(f"for wallet={wallet_} and repeated_wallet={repeated_wallet} len={_len}")
            #     # if wallet_ == '0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad':
            #     #     print('-'*40)
            #     #     print(_len)
            #     #     pprint(uniq_tokens_repeated[wallet_][repeated_wallet])
            #     #     exit()
            #     if _len >= min_uniq_addresses:
            #         uniq_addresses_repeated[wallet_] = _len

            # if wallet_ in uniq_tokens_repeated and repeated_wallet in uniq_tokens_repeated[wallet_]:
            #     uniq_addresses_repeated[wallet_] = len(uniq_tokens_repeated[wallet_][repeated_wallet])
                        

        #uniq_addresses_repeated = dict(sorted(uniq_addresses_repeated.items(), key=lambda x: x[1]))
        median_addresses_price_change = dict(sorted(median_addresses_price_change.items(), key=lambda x: x[1]))
        

        for wallet_ in list(median_addresses_price_change.keys()):

            if uniq_tokens_v2[wallet_] < 4 or uniq_tokens_v2[wallet_] > 200 or median_addresses_price_change[wallet_] < 10:
                continue

            print(f"{Fore.GREEN}wallet: {utils.print_address(wallet_)}{Style.RESET_ALL} (repeated wallets: {uniq_addresses_repeated[wallet_]}, uniq trans: {uniq_addresses_transactions_v2[wallet_]}, uniq tokens: {uniq_tokens_v2[wallet_]}, median price change: {round(median_addresses_price_change[wallet_], 2)}%, maestro trans: {uniq_maestro_transactions[wallet_]})")
            #for token in token_repeats_count_hashes[wallet_].keys():
            for token in price_change[wallet_].keys():
                if price_change[wallet_][token] == {}:
                    _prc = 'No'
                else:
                    _prc = ''
                    for prev_value, next_values in price_change[wallet_][token].items():
                        if len(next_values) == 1:
                            _prc += f"{round(100*next_values[0]/prev_value-100,2)}%"
                        else:
                            _prc += '['
                            for nv in next_values:
                                _prc += f"{round(100*nv/prev_value-100,2)}% "
                            _prc = _prc.rstrip(' ')
                            _prc += ']'
                        _prc += ', '
                    _prc = _prc.rstrip(', ')
                print(f"  erc20 {token}, {Fore.RED}price_change: {_prc}{Style.RESET_ALL}")
                if token in token_repeats_count_hashes[wallet_]:
                    print(f"    trans repeats {len(token_repeats_count_hashes[wallet_][token])}: {list(token_repeats_count_hashes[wallet_][token].values())} $")

            # wallet_ может не быть в uniq_tokens_repeated, т.к. выше стоит if len(wallets[wallet_][repeated_wallet]) > 1
            if wallet_ in uniq_tokens_repeated:
                for repeated_wallet in uniq_tokens_repeated[wallet_].keys():
                    if len(uniq_tokens_repeated[wallet_][repeated_wallet]) >= min_uniq_addresses:
                        print(f"    {Fore.BLUE}repeated_wallet: {utils.print_address(repeated_wallet)}{Style.RESET_ALL}, uniq addresses: {len(uniq_tokens_repeated[wallet_][repeated_wallet])} ({round(100 * len(uniq_tokens_repeated[wallet_][repeated_wallet]) / uniq_tokens_v2[wallet_], 1)}%), times: {len(wallets[wallet_][repeated_wallet])}, uniq trans: {uniq_addresses_transactions_v2[repeated_wallet]}, uniq tokens: {uniq_tokens_v2[repeated_wallet]}, maestro trans: {uniq_maestro_transactions[repeated_wallet]}")
                        # pprint(wallets[wallet_][repeated_wallet])
                        for arr in wallets[wallet_][repeated_wallet]:
                            print(f"      erc20 {arr['address']} {arr['value_usd']} $")
                            # print(f"    {arr}")
                            pass
        
        
    finally:
        # conn.close()
        print(f"{(time.time() - start_time_e):.3f}s total time")
