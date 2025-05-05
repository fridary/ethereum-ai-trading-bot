import os
import sys
from web3 import Web3, Account
from web3.logs import DISCARD
from web3.exceptions import BadFunctionCallOutput, ContractLogicError
from web3._utils.events import get_event_data
import requests
import json
import traceback
from pprint import pprint
import statistics
import pandas as pd
import time
import random
import pickle
import string
import copy
import socket
from datetime import datetime
from eth_utils import to_checksum_address, from_wei, to_wei
from hexbytes import HexBytes
import tabulate
import joblib
from filelock import FileLock
from multiprocessing import Pool
from threading import Thread
import threading
# from multiprocessing import Pool, cpu_count
# from concurrent.futures import ProcessPoolExecutor

import logging
from logging.handlers import RotatingFileHandler
formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s %(message)s')
# logging.basicConfig(filename='logs/task/db.log', format='%(asctime)s %(levelname)s %(message)s')
# logger = logging.getLogger()

mints_handler = RotatingFileHandler('/home/root/python/ethereum/lib/logs/mints.log', mode='a',
    maxBytes=10*1024*1024,
    backupCount=0, encoding=None, delay=0)
mints_handler.setFormatter(formatter)
mints_handler.setLevel(logging.INFO)
mints_logger = logging.getLogger('any-mints') # root
mints_logger.setLevel(logging.INFO)
if (mints_logger.hasHandlers()):
    mints_logger.handlers.clear()
mints_logger.addHandler(mints_handler)

blocks_handler = RotatingFileHandler('/home/root/python/ethereum/lib/logs/new_blocks.log', mode='a',
    maxBytes=10*1024*1024,
    backupCount=1, encoding=None, delay=0)
blocks_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
blocks_handler.setLevel(logging.INFO)
blocks_logger = logging.getLogger('any-blocks') # root
blocks_logger.setLevel(logging.INFO)
if (blocks_logger.hasHandlers()):
    blocks_logger.handlers.clear()
blocks_logger.addHandler(blocks_handler)

lt_handler = RotatingFileHandler('/home/root/python/ethereum/lib/logs/last_trades.log', mode='a',
    maxBytes=10*1024*1024,
    backupCount=0, encoding=None, delay=0)
lt_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
lt_handler.setLevel(logging.INFO)
lt_logger = logging.getLogger('any-last-trades') # root
lt_logger.setLevel(logging.INFO)
if (lt_logger.hasHandlers()):
    lt_logger.handlers.clear()
lt_logger.addHandler(lt_handler)


trades_handler = RotatingFileHandler('/home/root/python/ethereum/lib/logs/trades.log', mode='a',
    maxBytes=10*1024*1024,
    backupCount=0, encoding=None, delay=0)
trades_handler.setFormatter(formatter)
trades_handler.setLevel(logging.INFO)
trades_logger = logging.getLogger('any-trades') # root
trades_logger.setLevel(logging.INFO)
if (trades_logger.hasHandlers()):
    trades_logger.handlers.clear()
trades_logger.addHandler(trades_handler)

from cysystemd import journal


from strategies.BBands import BBands
from strategies.MlMatt import MlMatt
from strategies.Time import Time
from strategies.MlCatboost import MlCatboost

import ml
from last_trades import LastTrades

from catboost import CatBoostClassifier

import psycopg2
import psycopg2.extras
from psycopg2.extras import Json, DictCursor, RealDictCursor

import utils
from utils import FILES_DIR, FILE_LABELS, DIR_LOAD_OBJECTS

# внизу еще в exec_block() объявляется
conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)

# https://stackoverflow.com/questions/26741175/psycopg2-db-connection-hangs-on-lost-network-connection
# s = socket.fromfd(conn.fileno(), socket.AF_INET, socket.SOCK_STREAM)
# s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
# s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 6)
# s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
# s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 2)
# s.setsockopt(socket.IPPROTO_TCP, socket.TIPC_SUBSCR_TIMEOUT, 10000)

dup_blocks = []

BACKTEST_MODE = 1 # 0/1

WATCH_WALLETS = ['0xE1F2d1Bae92a429Edcf25d35e902F61E8F9Fd034', '0x5DDf30555EE9545c8982626B7E3B6F70e5c2635f', '0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13']


# ML_MODEL_RFC = joblib.load("/home/root/python/ethereum/temp/RFC_model.pkl")
# ML_SCALER = joblib.load("/home/root/python/ethereum/temp/scaler.pkl")

# catboost_model = CatBoostClassifier()
# catboost_model.load_model(f'/home/root/python/ethereum/lib/models/erc20_model_classif')
catboost_model = None


class SyncBlock():

    
    def __init__(self, testnet: str, realtime: bool, verbose: int = 0):


        self.testnet = testnet
        self.realtime = realtime
        self.verbose = verbose

        self.test_flag = False
        self.flag_no_buy_anymore = False

        os.makedirs(f"logs_address", exist_ok=True)
    

    def call_node(self, action, block_number):

        # print(f'call_node(), action={action}, block_number={block_number}')

        start_time = time.time()

        if action == 'eth_getBlockByNumber':
            block = requests.post(self.testnet, json={"method":"eth_getBlockByNumber","params":[hex(block_number),True],"id":1,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']
        if action == 'eth_getBlockReceipts':
            block = requests.post(self.testnet, json={"method":"eth_getBlockReceipts","params":[hex(block_number)],"id":2,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']
        if action == 'debug_traceBlockByNumber':
            block = requests.post(self.testnet, json={"method":"debug_traceBlockByNumber","params":[hex(block_number),{"tracer":"callTracer"}],"id":3,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']
        if action == 'eth_price':
            w3 = Web3(Web3.HTTPProvider(f"{self.testnet}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
            block = utils.get_eth_price(w3, block_number, prefix='call_node')
        if action == 'eth_price_no_reduce':
            w3 = Web3(Web3.HTTPProvider(f"{self.testnet}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
            block = utils.get_eth_price(w3, block_number, can_reduce_blocks=False, prefix='call_node[no_reduce]')

        # print("%s in %.3fs" % (action, time.time() - start_time))
        # print(f'ended call_node(), action={action}, block_number={block_number}')
        return block



    def exec_block(self, pool, block_number):

        global conn
        # print(f'exec_block() on block {block_number}')

        if self.realtime:
            journal.write(f" starting exec_block() {block_number}")

        if self.realtime:
            blocks_logger.info(f"START exec_block(), block_number={block_number}")


        minted_pairs = []
        transfer_erc20_addresses_in_block = []
        transactions_by_wallets_watchlist = [] # список из _transfers

        start_time_ex_bl = time.time()

        self.check_connection(msg="start exec_block()")


        # https://www.postgresqltutorial.com/postgresql-python/transaction/

        with conn.cursor() as cursor:

            if not self.realtime:
                journal.write(f" Начинаю SELECT 1 FROM _blocks")

                try:
                    # cursor.execute(f"SELECT EXISTS(SELECT 1 FROM _blocks WHERE number={block_number})")
                    cursor.execute(f"SELECT 1 FROM _blocks WHERE number={block_number}")
                    _exists_block = cursor.fetchone()
                except:
                    if self.realtime:
                        journal.write(f" Ошибка SELECT 1 FROM _blocks")
                        journal.write(sys.exc_info())
                        journal.write(traceback.format_exc())
                    return

                if self.realtime:
                    journal.write(f" Получили SELECT 1 FROM _blocks")

                if _exists_block != None:
                    print(f"[Block] {block_number} exists")
                    return

            # start_time = time.time()
            # pool_4 = Pool(4)
            # # self.pool_cpu = Pool(72)
            # print("%.3fs inited Pool(x)" % (time.time() - start_time))


            params = [('eth_getBlockByNumber', block_number,), ('eth_getBlockReceipts', block_number,), ('debug_traceBlockByNumber', block_number,), ('eth_price', block_number), ('eth_price_no_reduce', block_number),]
            # print('starting pool')
            start_time = time.time()
            blocks = pool.starmap_async(self.call_node, params).get()
            # blocks = pool_4.map(self.call_node, (1,), (2,))
            # print(f"ending pool {(time.time() - start_time):.3f}s")
            # print(blocks)
            # time.sleep(18)
            # print(f"finished exec_block() {block_number}")
            # return
            # print("%.3fs call_node()" % (time.time() - start_time))

            if self.realtime:
                journal.write(f" сделали pool.starmap_async на 4 blocks")


            # #hash_ = '0xed1edbc10544dba1273f14f57775bb0a85a7156deeb17a39254eb7e623c51816'
            # #hash_ = '0xcd487ee0071d3ff000212cb125b3cfaccc43c932d145e5e47e38945ea82fad17' # swap'a нет
            # hash_ = '0x7b826d84f34595cc530a1616792b96118822677e363685d532708e42fb7072bf'
            # blocks = [
            #     {'transactions': [requests.post(testnet, json={"method":"eth_getTransactionByHash","params":[hash_],"id":1,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']]},
            #     [requests.post(testnet, json={"method":"eth_getTransactionReceipt","params":[hash_],"id":2,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result'],],
            #     [{'txHash': hash_, 'result': requests.post(testnet, json={"jsonrpc":"2.0", "method":"debug_traceTransaction", "params":[hash_, {"tracer":"callTracer"}], "id":3}, headers={"Content-Type": "application/json"}).json()['result']}],
            #     utils.get_eth_price(w3, block_number),
            # ]


            # start_time = time.time()
            # with ProcessPoolExecutor(4) as executor:
            #     future = list(executor.map(self.call_node, ('eth_getBlockByNumber', 'eth_getBlockReceipts', 'debug_traceBlockByNumber', 'eth_price'), (block_number, block_number, block_number, block_number)))
            # # print("%.3fs ProcessPoolExecutor(4)" % (time.time() - start_time))
            # blocks = [future[0], future[1], future[2], future[3]]


            assert len(blocks[0]['transactions']) == len(blocks[1]) == len(blocks[2]), f"{block_number} {len(blocks[0]['transactions'])} {len(blocks[1])} {len(blocks[2])}"


            params = []
            # _a, _b, _c, _d, _e, _f = [], [], [], [], [], []
            for i in range(len(blocks[0]['transactions'][:])):
                params.append((i, blocks[0]['transactions'][i], blocks[1][i], blocks[2][i], blocks[3], {
                    'timestamp': int(blocks[0]['timestamp'], 16),
                    'baseFeePerGas': int(blocks[0]['baseFeePerGas'], 16),
                    'gasUsed': int(blocks[0]['gasUsed'], 16),
                    'gasLimit': int(blocks[0]['gasLimit'], 16),
                    'miner': blocks[0]['miner'],
                }))
                # _a.append(i)
                # _b.append(blocks[0]['transactions'][i])
                # _c.append(blocks[1][i])
                # _d.append(blocks[2][i])
                # _e.append(blocks[3])
                # _f.append({
                #     'timestamp': int(blocks[0]['timestamp'], 16),
                #     'baseFeePerGas': int(blocks[0]['baseFeePerGas'], 16),
                #     'gasUsed': int(blocks[0]['gasUsed'], 16),
                #     'gasLimit': int(blocks[0]['gasLimit'], 16),
                # })
            start_time = time.time()
            while 1:
                try:
                    # print('starting pool _rows')
                    _db_rows = pool.starmap_async(self.sync_transaction, params).get()
                    # print('ending pool _rows')
                    # with ProcessPoolExecutor(72) as executor:
                    #     _db_rows = list(executor.map(self.sync_transaction, tuple(_a), tuple(_b), tuple(_c), tuple(_d), tuple(_e), tuple(_f)))
                    break
                except requests.exceptions.ChunkedEncodingError as e:
                    print('ChunkedEncodingError:', e)
                except Exception as e:
                    print('~'*60, file=sys.stderr)
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} Exception exec_block() pool.starmap_async(self.sync_transaction) block={block_number}", file=sys.stderr)
                    print(traceback.format_exc(), file=sys.stderr)
                    return
            
            if self.realtime:
                journal.write(f" сделали pool.starmap_async на self.sync_transaction {(time.time() - start_time_ex_bl):.3f}s")


            # for item in _db_rows:
            #     for z in item['transfers']:
            #         pprint(z)
            

            if not self.realtime:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [Block] {block_number}", "\x1b[34;1m%.3fs\x1b[0m sync_transaction()" % (time.time() - start_time))
            else:
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [Block] {block_number} {(time.time() - start_time_ex_bl):.3f}s sync_transaction()")
                mints_logger.info(f"New block {block_number} sync_transaction() {(time.time() - start_time_ex_bl):.3f}s, {(start_time_ex_bl - int(blocks[0]['timestamp'], 16)):.2f}s block received")
                
                _transs = []
                for item in _db_rows:
                    _transs.append({'i':item['transaction']['transactionIndex'],'h':item['transaction']['hash']})
                _transs = sorted(_transs, key=lambda d: d['i'])
                blocks_logger.info(f" New block {block_number} sync_transaction() {(time.time() - start_time_ex_bl):.3f}s, {(start_time_ex_bl - int(blocks[0]['timestamp'], 16)):.2f}s block received, trans ({len(_transs)}):\n{_transs}")


            # return
            # assert 0

            start_time_e = time.time()
            try:

                # https://hakibenita.com/fast-load-data-python-postgresql


                # print(
                #         int(blocks[0]['baseFeePerGas'], 16),
                #         int(blocks[0]['blobGasUsed'], 16) if 'blobGasUsed' in blocks[0] else None,
                #         int(blocks[0]['difficulty'], 16),
                #         int(blocks[0]['excessBlobGas'], 16) if 'excessBlobGas' in blocks[0] else None,
                #         blocks[0]['extraData'],
                #         int(blocks[0]['gasLimit'], 16),
                #         int(blocks[0]['gasUsed'], 16),
                #         blocks[0]['hash'],
                #         blocks[0]['miner'],
                #         blocks[0]['mixHash'],
                #         int(blocks[0]['nonce'], 16),
                #         int(blocks[0]['number'], 16),
                #         int(blocks[0]['size'], 16),
                #         int(blocks[0]['timestamp'], 16),
                #         int(blocks[0]['totalDifficulty'], 16)
                # )


                start_time = time.time()
                cursor.execute("""
                    INSERT INTO _blocks (
                        baseFeePerGas,
                        blobGasUsed,
                        difficulty,
                        excessBlobGas,
                        extraData,
                        gasLimit,
                        gasUsed,
                        hash,
                        miner,
                        mixHash,
                        nonce,
                        number,
                        size,
                        timestamp,
                        totalDifficulty,
                        eth_price
                    ) VALUES %s;
                """, ((
                        int(blocks[0]['baseFeePerGas'], 16),
                        int(blocks[0]['blobGasUsed'], 16) if 'blobGasUsed' in blocks[0] else None,
                        int(blocks[0]['difficulty'], 16),
                        int(blocks[0]['excessBlobGas'], 16) if 'excessBlobGas' in blocks[0] else None,
                        blocks[0]['extraData'],
                        int(blocks[0]['gasLimit'], 16),
                        int(blocks[0]['gasUsed'], 16),
                        blocks[0]['hash'],
                        blocks[0]['miner'],
                        blocks[0]['mixHash'],
                        int(blocks[0]['nonce'], 16),
                        int(blocks[0]['number'], 16),
                        int(blocks[0]['size'], 16),
                        int(blocks[0]['timestamp'], 16),
                        int(blocks[0]['totalDifficulty'], 16),
                        blocks[4] # eth_price_no_reduce
                ),))
                # print(" %.3fs _insert_block" % (time.time() - start_time))


                if self.realtime:
                    journal.write(f" сделали INSERT INTO _blocks для block_number={block_number}")
                    blocks_logger.info(f" сделали INSERT INTO _blocks для block_number={block_number}")


                _insert_transactions = []
                for item in _db_rows:
                    _insert_transactions.append(item['transaction'])
                    if item['minted_pair'] != None:
                        minted_pairs.append({'pair': item['minted_pair'], 'mint_transaction': item['transaction']['hash']})

                    # for trf in item['transfers']:
                    #     if type(trf['transactionIndex']) != int:
                    #         print(f"transactionIndex not int: {trf['transactionIndex']}")
                    #     if type(trf['blockNumber']) != int:
                    #         print(f"blockNumber not int: {trf['blockNumber']}")
                    #     if type(trf['timestamp']) != int:
                    #         print(f"timestamp not int: {trf['timestamp']}")
                    #     if trf['value'] != None and type(trf['value']) != int:
                    #         print(f"value not int: {trf['value']}")
                    #     if trf['value_usd'] != None and type(trf['value_usd']) != float:
                    #         print(f"value_usd not float: {trf['value_usd']}")
                    #     if trf['value_usd_per_token_unit'] != None and type(trf['value_usd_per_token_unit']) != float:
                    #         print(f"value_usd_per_token_unit not float: {trf['value_usd_per_token_unit']}")
                    #     if trf['logIndex'] != None and type(trf['logIndex']) != int:
                    #         print(f"logIndex not int: {trf['logIndex']}")
                    #     if trf['status'] != None and type(trf['status']) != int:
                    #         print(f"status not int: {trf['status']}")

                    # for log in item['logs']:
                    #     _insert_logs.append(log)


                start_time = time.time()
                ids_ = psycopg2.extras.execute_values(cursor, """
                    INSERT INTO _transactions (
                        blockHash,
                        blockNumber,
                        chainId,
                        from_,
                        gas,
                        gasPrice,
                        hash,
                        input,
                        maxFeePerGas,
                        maxPriorityFeePerGas,
                        nonce,
                        r,
                        s,
                        to_,
                        transactionIndex,
                        type,
                        v,
                        value,
                        yParity,
                        contractAddress,
                        cumulativeGasUsed,
                        gasUsed,
                        status,
                        executed_in,
                        timestamp,
                        bribe_eth
                    ) VALUES %s RETURNING id, hash;
                """, ((
                        x['blockHash'],
                        x['blockNumber'],
                        x['chainId'],
                        x['from_'],
                        x['gas'],
                        x['gasPrice'],
                        x['hash'],
                        x['input'],
                        x['maxFeePerGas'],
                        x['maxPriorityFeePerGas'],
                        x['nonce'],
                        x['r'],
                        x['s'],
                        x['to_'],
                        x['transactionIndex'],
                        x['type'],
                        x['v'],
                        x['value'],
                        x['yParity'],
                        x['contractAddress'],
                        x['cumulativeGasUsed'],
                        x['gasUsed'],
                        x['status'],
                        x['executed_in'],
                        x['timestamp'],
                        x['bribe_eth']
                ) for x in _insert_transactions), page_size=1000, fetch=True)
                # print(" %.3fs _insert_transactions" % (time.time() - start_time))

                if self.realtime:
                    blocks_logger.info(f" сделали INSERT INTO _transactions для block_number={block_number}")

                hashes_ = {}
                for item in ids_:
                    hashes_[item[1]] = item[0] # item[0] = transaction_id

                _insert_transfers = []
                _insert_logs = []
                _transfers_by_transactionHash = {} # index - transactionHash
                for item in _db_rows:
                    for z in item['transfers']:
                        if z['category'] == 'erc20' and z['exchange'] != None and z['address'] not in [utils.WETH.lower(), utils.USDC.lower(), utils.USDT.lower()]:
                            _trf = {
                                'address': z['address'],
                                'value_usd_per_token_unit': z['value_usd_per_token_unit'],
                                'extra_data': z['extra_data'],
                                'pair_address': z['pair_address'],
                                'token1_currency': z['token1_currency'],
                                'exchange': z['exchange'],
                                'total_supply': z['total_supply'],
                                'reserves_token': z['reserves_token'],
                                'reserves_token_usd': z['reserves_token_usd'],
                                'reserves_pair': z['reserves_pair'],
                                'reserves_pair_usd': z['reserves_pair_usd'],
                                'token_symbol': z['token_symbol'],
                                'token_decimals': z['token_decimals'],
                            }
                            if _trf not in transfer_erc20_addresses_in_block:
                                transfer_erc20_addresses_in_block.append(_trf)

                        z['transaction_id'] = hashes_[z['transactionHash']]
                        _insert_transfers.append(z)

                        if z['transactionHash'] not in _transfers_by_transactionHash:
                            _transfers_by_transactionHash[z['transactionHash']] = []
                        _transfers_by_transactionHash[z['transactionHash']].append(z)
                        if z['from_'] in map(str.lower, WATCH_WALLETS) or z['to_'] in map(str.lower, WATCH_WALLETS):
                            transactions_by_wallets_watchlist.append(z)

                    for z in item['logs']:
                        z['transaction_id'] = hashes_[z['transactionHash']]
                        _insert_logs.append(z)



                start_time = time.time()
                psycopg2.extras.execute_values(cursor, """
                    INSERT INTO _logs (
                        transaction_id,
                        transactionHash,
                        transactionIndex,
                        blockNumber,
                        timestamp,
                        address,
                        topics_0,
                        topics_1,
                        topics_2,
                        topics_3,
                        pair_created,
                        decoded_data,
                        logIndex
                    ) VALUES %s;
                """, ((
                        x['transaction_id'],
                        x['transactionHash'],
                        x['transactionIndex'],
                        x['blockNumber'],
                        x['timestamp'],
                        x['address'],
                        x['topics_0'],
                        x['topics_1'],
                        x['topics_2'],
                        x['topics_3'],
                        x['pair_created'],
                        json.dumps(x['decoded_data']),
                        x['logIndex']
                ) for x in _insert_logs), page_size=1000)
                # print(" %.3fs _insert_logs" % (time.time() - start_time))
            
                if self.realtime:
                    blocks_logger.info(f" сделали INSERT INTO _logs для block_number={block_number}")

                start_time = time.time()
                psycopg2.extras.execute_values(cursor, """
                    INSERT INTO _transfers (
                        transaction_id,
                        transactionHash,
                        transactionIndex,
                        blockNumber,
                        timestamp,
                        address,
                        from_,
                        to_,
                        value,
                        value_usd,
                        value_usd_per_token_unit,
                        extra_data,
                        category,
                        logIndex,
                        status,
                        pair_address,
                        token1_currency,
                        exchange ,
                        total_supply,
                        reserves_token,
                        reserves_token_usd,
                        reserves_pair,
                        reserves_pair_usd,
                        token_symbol,
                        token_decimals
                    ) VALUES %s;
                """, ((
                        x['transaction_id'],
                        x['transactionHash'],
                        x['transactionIndex'],
                        x['blockNumber'],
                        x['timestamp'],
                        x['address'],
                        x['from_'],
                        x['to_'],
                        x['value'],
                        x['value_usd'],
                        x['value_usd_per_token_unit'],
                        json.dumps(x['extra_data']),
                        x['category'],
                        x['logIndex'],
                        x['status'],
                        x['pair_address'],
                        x['token1_currency'],
                        x['exchange'],
                        x['total_supply'],
                        x['reserves_token'],
                        x['reserves_token_usd'],
                        x['reserves_pair'],
                        x['reserves_pair_usd'],
                        x['token_symbol'],
                        x['token_decimals']
                ) for x in _insert_transfers), page_size=1000)
                # print(" %.3fs _insert_transfers" % (time.time() - start_time))

                if self.realtime:
                    blocks_logger.info(f" сделали INSERT INTO _transfers для block_number={block_number}")

                start_time = time.time()
                conn.commit()
                # print(" %.3fs commit" % (time.time() - start_time))
                # cursor.close()

                if self.realtime:
                    journal.write(f" exec_block() block {block_number} was added {(time.time() - start_time_ex_bl):.3f}s")
                    blocks_logger.info(f"END exec_block() block_number={block_number} was added {(time.time() - start_time_ex_bl):.3f}s, len(_transs)={len(_transs)}, len(_insert_transactions)={len(_insert_transactions)}")



            except Exception as e:
                print('-'*50, file=sys.stderr)
                print(traceback.format_exc(), file=sys.stderr)
                # cursor.close()
                conn.rollback()
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} rolling back: {e}")
                minted_pairs = []

                if 'duplicate key value violates unique constraint "_transactions_hash_key"' in str(e):
                    dup_hash = str(e).split('Key (hash)=(')[1].split(')')[0]
                    print(f" current block_number={block_number}, dup_hash={dup_hash}")
                    cursor.execute(f"SELECT blockNumber from _transactions where hash='{dup_hash}'")
                    dup_block = cursor.fetchone()[0]
                    print('dup block:', dup_block)
                    dup_blocks.append(dup_block)
                    print(dup_blocks)

                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} Сейчас на блоке {block_number}, транзакция эта уже есть на блоке {dup_block}", file=sys.stderr)
                    if self.realtime:
                        mints_logger.info(f"  Сейчас на блоке {block_number}, транзакция эта уже есть на блоке {dup_block}, dup_hash={dup_hash}")
                        blocks_logger.info(f"  Сейчас на блоке {block_number}, транзакция эта уже есть на блоке {dup_block}, dup_hash={dup_hash}; ошибка: {str(e)}")

                    # delete from _blocks where number in (19989697);
                    # delete from _transactions where blockNumber in (19989697);
                    # delete from _transfers where blockNumber in (19989697);
                    # delete from _logs where blockNumber in (19989697);
                else:
                    if self.realtime:
                        mints_logger.info(f"  Сейчас на блоке {block_number}, сторонняя ошибка:\n{str(traceback.format_exc())}")
                        blocks_logger.info(f"  Сейчас на блоке {block_number}, сторонняя ошибка:\n{str(traceback.format_exc())}")


                return

            if self.realtime:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [exec_block()] {(time.time() - start_time_e):.3f}s total sql exec all inserts for block")
    

        if 0 and self.realtime and transactions_by_wallets_watchlist:

            

            for tbww in transactions_by_wallets_watchlist:

                has_in_token, has_out_token = False, False
                has_in_usd, has_out_usd = False, False
                how_much_usd_was_spent = {} # здесь заложено сколько $ было потрачено
                how_much_usd_was_gained = {} # здесь заложено сколько $ было получено
                how_much_native_spent, how_much_native_gained = {}, {} # значение в value токена
                for i, trf in enumerate(tbww):
                    
                    # Wei failed transaction
                    try:
                        if trf['address'] == None and trf['status'] == 0:
                            continue
                    except TypeError as e:
                        print('TypeError err!!!!! trf:')
                        print(trf)
                    if trf['address'] == None and trf['value'] == 0:
                        continue

                    if trf['from'] and trf['from'] not in how_much_usd_was_spent: how_much_usd_was_spent[trf['from']] = 0
                    if trf['to'] and trf['to'] not in how_much_usd_was_gained: how_much_usd_was_gained[trf['to']] = 0


                    if trf['address'] == [None] + map(str.lower(), [utils.WETH, utils.USDC, utils.USDT]): # None = Wei

                        if trf['from'] in map(str.lower, WATCH_WALLETS):
                            has_out_usd = True
                        if trf['to'] in map(str.lower, WATCH_WALLETS):
                            has_in_usd = True

                        if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                        if trf['to']: how_much_usd_was_gained[trf['to']] += trf['value_usd']

                    # elif trf["address"] == self.address:

                    #     new_item = {
                    #         'value': 0,
                    #         'value_usd': None,
                    #         'invested': 0,
                    #         'withdrew': 0,
                    #         'volume_buy': 0,
                    #         'volume_sell': 0,
                    #         'trades_buy': 0,
                    #         'trades_sell': 0,
                    #         'transfers_send': 0,
                    #         'transfers_received': 0,
                    #         'gas_usd': 0,  # total gas used $
                    #         'liquidity_first': {'p':trf['extra_data']['rp'], 't':trf['extra_data']['rt']} if trf['extra_data'] else None, # ликвидность usd в момент первого трейда
                    #         'realized_profit': 0, # $
                    #         'unrealized_profit': 0, # $
                    #         'pnl': 0, # $
                    #         'roi_r': 0,
                    #         'roi_u': 0,
                    #         'roi_t': 0,
                    #         'trades_list': [], # нужно для вычисления avg_price для profits
                    #         'first_trade': tx_sql['block_timestamp'], # timestamp
                    #         'first_trade_block': tx_sql['block_number'],
                    #         'first_trade_index': tx_sql['transactionindex'],
                    #         'last_trade': tx_sql['block_timestamp'], # timestamp
                    #         'last_trade_block': tx_sql['block_number'],
                    #         'days': None, # days holding token
                    #         'router': None,
                    #         'stat_wallet_address': None,
                    #     }
                    #     if trf['from'] and trf['from'] not in self.balances:
                    #         if not(trf['from'] in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]):
                    #             self.balances[trf['from']] = {'i': len(self.balances)} | copy.deepcopy(new_item)
                    #     if trf['to'] and trf['to'] not in self.balances:
                    #         self.balances[trf['to']] = {'i': len(self.balances)} | copy.deepcopy(new_item)

                    #     if trf['from'] == tx_sql['from']:
                    #         has_out_token = True
                    #     if trf['to'] == tx_sql['from']:
                    #         has_in_token = True

                    #     addresses_received_native_token.append(trf['to'])

                    #     # from
                    #     if trf['from'] and trf['from'] in self.balances:
                    #         self.balances[trf['from']]['value'] -= trf['value']
                    #         self.balances[trf['from']]['last_trade_block'] = tx_sql['block_number']
                    #         self.balances[trf['from']]['last_p_i'] = p_i
                    #         self.balances[trf['from']]['last_trade'] = tx_sql['block_timestamp']
                    #         if self.balances[trf['from']]['value'] < 0:
                    #             self.print(f"      \x1b[31;1m(!)\x1b[0m balance for {trf['from']}={self.balances[trf['from']]['value']} < 0 on address={trf['address']}")

                    #         if trf['from'] not in how_much_native_spent: how_much_native_spent[trf['from']] = 0
                    #         how_much_native_spent[trf['from']] += trf['value']

                    #     # to
                    #     if trf['to']:
                    #         self.balances[trf['to']]['value'] += trf['value']
                    #         self.balances[trf['to']]['last_trade_block'] = tx_sql['block_number']
                    #         self.balances[trf['to']]['last_p_i'] = p_i
                    #         self.balances[trf['to']]['last_trade'] = tx_sql['block_timestamp']
                            
                    #         if trf['to'] not in how_much_native_gained: how_much_native_gained[trf['to']] = 0
                    #         how_much_native_gained[trf['to']] += trf['value']
                        
                    #     if trf['value_usd_per_token_unit'] != None:
                    #         if trf['from'] and trf['from'] != utils.ADDRESS_0X0000: self.balances[trf['from']]['value_usd'] = trf['value_usd_per_token_unit'] / 10 ** self.contract_data_token['token_decimals'] * self.balances[trf['from']]['value']
                    #         if trf['to'] and trf['to'] != utils.ADDRESS_0X0000: self.balances[trf['to']]['value_usd'] = trf['value_usd_per_token_unit'] / 10 ** self.contract_data_token['token_decimals'] * self.balances[trf['to']]['value']
                        
                    #         if trf['to'] == tx_sql['from']:
                    #             self.balances[tx_sql['from']]['volume_buy'] += trf['value_usd']
                    #             if not is_transaction_mint:
                    #                 trans_volume_buy += trf['value_usd']
                    #             self.print(f"покупка токена адресом {tx_sql['from']} value={trf['value'] / 10 ** self.contract_data_token['token_decimals']} на сумму {trf['value_usd']:.2f} $ (self.traded_volume_buy={self.traded_volume_buy:.2f} $)")
                    #         if trf['from'] == tx_sql['from']:
                    #             self.balances[tx_sql['from']]['volume_sell'] += trf['value_usd']
                    #             if not is_transaction_mint:
                    #                 trans_volume_sell += trf['value_usd']
                    #             self.print(f"продажа/трансфер токена block={tx_sql['block_number']} адресом {tx_sql['from']} value={trf['value'] / 10 ** self.contract_data_token['token_decimals']} на сумму {trf['value_usd']:.2f} $ (self.traded_volume_sell={self.traded_volume_sell:.2f} $)")


                    #     if tx_sql['block_number'] not in self.blocks_data:
                    #         self.blocks_data[tx_sql['block_number']] = {
                    #             'volume_buy': 0,
                    #             'volume_sell': 0,
                    #             'trades_buy': 0,
                    #             'trades_sell': 0,
                    #             'invested': 0,
                    #             'withdrew': 0,
                    #             'buy_addresses': [],
                    #             'sell_addresses': [],
                    #             'buy_trans_indexes': [],
                    #         }
                    #     self.blocks_data[tx_sql['block_number']]['timestamp'] = tx_sql['block_timestamp']
                    #     self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'] = trf['value_usd_per_token_unit']
                    #     self.blocks_data[tx_sql['block_number']]['extra_data'] = trf['extra_data']
                    #     self.blocks_data[tx_sql['block_number']]['liquidity'] = trf['extra_data']['rp'] if trf['extra_data'] else None

                    else:

                        if trf['from'] == tx_sql['from']:
                            has_out_other = True
                        if trf['to'] == tx_sql['from']:
                            has_in_other = True
                        
                        if trf['value_usd_per_token_unit'] != None:
                            if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                            if trf['to']: how_much_usd_was_gained[trf['to']] += trf['value_usd']



        # print('transfer_erc20_addresses_in_block len=', len(transfer_erc20_addresses_in_block))
        if 0 and self.realtime and transfer_erc20_addresses_in_block:

            # print('transfer_erc20_addresses_in_block:')
            # pprint(transfer_erc20_addresses_in_block)
            # {'address': '0x3106a0a076bedae847652f42ef07fd58589e001f',
            # 'exchange': 'V2',
            # 'extra_data': {'rp': 30607.61, 'rt': 30607.61},
            # 'pair_address': '0xF5E875B9F457F2DD8112BD68999Eb72beFB17b03',
            # 'token1_currency': 'WETH',
            # 'value_usd_per_token_unit': 1.4638852702882043e-19},

            params = []
            last_trades_by_address = {} # index - token address; список из последних трейдов


            self.check_connection(msg="in transfer_erc20_addresses_in_block and before FROM last_trades_token_status")

            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:


                    # cursor.execute(f"select number from _blocks where number<{block_number} and number>={block_number-30} order by number asc")
                    # _db_blocks = cursor.fetchall()
                    # block < 25
                    # block>=25-5=20
                    # 20 21 22 23 24 
                    # raise Exception()

                    cursor.execute(f"select * from (select distinct on (address) address, * from trades where backtest={BACKTEST_MODE} order by address, trans_block_number desc) t order by trans_block_number asc")
                    _db_trades = cursor.fetchall()
                    for trade in _db_trades:
                        last_trades_by_address[trade['address']] = trade


                for _trf in transfer_erc20_addresses_in_block:
                    if not _trf['extra_data'] or _trf['extra_data']['rp'] < 500 or _trf['extra_data']['rp'] > 200000:
                        continue

                    if 1 or _trf['address'].lower() == '0x6dB6FDb5182053EECeC778aFec95E0814172A474'.lower():

                        # проверка если токен торгуется на других парах, убираем дубль
                        dont_add = False
                        for pa in params[:]:
                            if pa[0] == _trf['address']:
                                # оставляем токен с парой бо'льшей ликвидности
                                if pa[1]['extra_data']['rp'] < _trf['extra_data']['rp']:
                                    params.remove(pa)
                                else:
                                    dont_add = True
                        
                        last_db_trade = None
                        if _trf['address'] in last_trades_by_address:
                            last_db_trade = last_trades_by_address[_trf['address']]

                        if not dont_add:
                            params.append((_trf['address'],_trf,block_number,blocks[3],start_time_ex_bl,last_db_trade,))



                if 1:
                    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                        cursor.execute(f"""
                            SELECT
                                address, started
                            FROM last_trades_token_status
                            WHERE script='realtime'
                        """)
                        _rows = cursor.fetchall()
                        _count = len(_rows)
                        executing_addresses = [x['address'] for x in _rows]
                        for param in params[:]:
                            if param[0] in executing_addresses:
                                params.remove(param)
                        executing_time = sorted([(datetime.now() - x['started']).total_seconds() for x in _rows])
                        executing_time = [f"{round(x / 60, 1)}m" for x in executing_time]

                        max_processes = 40
                        lt_logger.info(f"block_number={block_number} сейчас выполняется {_count} потоков в LastTrades() [{' '.join(executing_time)}] => отправим max_processes={max_processes}-{_count}={max_processes - _count} новых, len params={len(params)}")

                        params = params[:max(0, max_processes - _count)]

                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} sending {len(params)} tokens to new_trades_strategy()")

                pool.starmap_async(self.new_trades_strategy, params).get()
            except:
                try:
                    conn.rollback()
                except:
                    pass
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} EXCEPTION exec_block() in new_trades_strategy() {sys.exc_info()}")
                #lt_logger.info(f"EXCEPTION exec_block() block_number={block_number}: {sys.exc_info()}")
                lt_logger.info(f"EXCEPTION exec_block() in new_trades_strategy() block_number={block_number}: {str(traceback.format_exc())}")
                print(f"-"*50, file=sys.stderr)
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} EXCEPTION exec_block() in new_trades_strategy() {sys.exc_info()}", file=sys.stderr)
                print(traceback.format_exc(), file=sys.stderr)
                # print(traceback.format_exc())


            
            # print(f'FINISHED block={block_number} for loop transfer_erc20_addresses_in_block (len={len(transfer_erc20_addresses_in_block)}), params len={len(params)}')

                


        if 0 and self.realtime:

            threads_c = []

            thread_w = Thread(target=self.watch_for_predict, args=(pool, block_number, blocks[3], start_time_ex_bl))  # create the thread
            thread_w.start()
            # print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} запущен thread watch()")

            thread_wt = Thread(target=self.watch_trades, args=(pool, block_number, blocks[3], start_time_ex_bl))
            thread_wt.start()

            # if not self.test_flag:
            #     minted_pairs = [
            #         {'pair': '0x9BdBEf447D43d691544aD9762F76D96D0eCbdc16'.lower(), 'mint_transaction': 'abcde'},
            #         #{'pair': '0xE19B94676D9aD6d3b7e7527ac30A4F2253D6949d'.lower(),' mint_transaction': 'assss'},
            #     ]
            #     self.test_flag = True

            if 1 or not self.test_flag:
                for _p in minted_pairs:
                    thread = Thread(target=self.check_pair_for_watching, args=(pool, _p['pair'], _p['mint_transaction'], block_number, blocks[3], start_time_ex_bl))
                    thread.start()
                    threads_c.append(thread)
                    # print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} запущен thread check_pair_for_watching()")


            # ожидание завершения всех потоков
            thread_w.join()
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [exec_block()] завершился thread_w [{thread_w.native_id}]")
            thread_wt.join()
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [exec_block()] завершился thread_wt [{thread_w.native_id}]")
            for th in threads_c:
                th.join()
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [exec_block()] завершился thread_c [{th.native_id}] (всего {len(threads_c)})")

            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [exec_block()] {(time.time() - start_time_ex_bl):.3f}s ушло времени на ВЕСЬ блок {block_number}")

        return




    def new_trades_strategy(self, address, trf_sql, last_block, eth_price, start_time_ex_bl, last_db_trade, force_execute=False):
        """
        trf_sql
            {'address': '0x3106a0a076bedae847652f42ef07fd58589e001f',
            'exchange': 'V2',
            'extra_data': {'rp': 30607.61, 'rt': 30607.61},
            'pair_address': '0xF5E875B9F457F2DD8112BD68999Eb72beFB17b03',
            'token1_currency': 'WETH',
            'value_usd_per_token_unit': 1.4638852702882043e-19}
        """

        utils.save_log_strg(f"[new_trades_strategy()] START address={address}, {(time.time() - start_time_ex_bl):.3f}s from exec_block() past", address=address, block=last_block)
        lt_logger.info(f"  block_number={last_block} address={address} запущен")

        
        try:
            with psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs) as conn_l:
                

                try:

                    start_time_g = time.time()

                    # lt_logger.info(f"  block_number={last_block} address={address} сейчас будет INSERT")

                    with conn_l.cursor(cursor_factory=RealDictCursor) as cursor:
                        try:
                            cursor.execute("""INSERT INTO last_trades_token_status (
                                    address,
                                    script
                                ) VALUES %s;
                            """, ((
                                    address.lower(),
                                    "realtime"
                            ),))
                            conn_l.commit()
                        except Exception as e:
                            conn_l.rollback()
                            raise e
                    

                    # lt_logger.info(f"  block_number={last_block} address={address} INSERT сделан")

                    start_time_g = time.time()

                    if 0:
                        if os.path.isfile(f"{DIR_LOAD_OBJECTS}/{to_checksum_address(address)}.pkl"):
                            with open(f"{DIR_LOAD_OBJECTS}/{to_checksum_address(address)}.pkl", 'rb') as o:
                                last_trades = pickle.load(o)
                            last_trades.reinit(conn=conn_l, last_block=last_block, from_block=last_trades.last_block_by_last_transaction + 1, limit=2500, realtime=True, verbose=0)
                            time_last_trades = time.time() - start_time_g
                            utils.save_log_strg(f"[new_trades_strategy()] LastTrades() reinit() in {(time.time() - start_time_g):.3f}s, len transactions_tupl={len(last_trades.transactions_tupl)}, from_block={last_trades.last_block_by_last_transaction + 1}", address=address, block=last_block)
                            lt_logger.info(f"  block_number={last_block} address={address} LastTrades() reinit() in {(time.time() - start_time_g):.3f}s, len transactions_tupl={len(last_trades.transactions_tupl)}, from_block={last_trades.last_block_by_last_transaction + 1}")
                        else:
                            last_trades = LastTrades(conn=conn_l, address=address, last_block=last_block, can_be_old_token=False, limit=500, sort='asc', realtime=True, verbose=0)
                            time_last_trades = time.time() - start_time_g
                            utils.save_log_strg(f"[new_trades_strategy()] LastTrades() __init__ in {(time.time() - start_time_g):.3f}s, len transactions_tupl={len(last_trades.transactions_tupl)}", address=address, block=last_block)
                            lt_logger.info(f"  block_number={last_block} address={address} LastTrades() __init__ in {(time.time() - start_time_g):.3f}s, len transactions_tupl={len(last_trades.transactions_tupl)}")
                   
                    else:
                        last_trades = LastTrades(conn=conn_l, address=address, last_block=last_block, can_be_old_token=True, limit=320, sort='desc', realtime=True, verbose=0)
                        time_last_trades = time.time() - start_time_g
                        utils.save_log_strg(f"[new_trades_strategy()] LastTrades() __init__ in {(time.time() - start_time_g):.3f}s, len transactions_tupl={len(last_trades.transactions_tupl)}", address=address, block=last_block)
                        lt_logger.info(f"  block_number={last_block} address={address} LastTrades() __init__ in {(time.time() - start_time_g):.3f}s, len transactions_tupl={len(last_trades.transactions_tupl)}")
                        

                    start_time2 = time.time()
                    last_trades.get_trades()
                    time_get_trades = time.time() - start_time2
                    utils.save_log_strg(f"[new_trades_strategy()] get_trades() in {(time.time() - start_time2):.3f}s", address=address, block=last_block)

                    # balances_ = sorted(last_trades.balances.items(), key=lambda d: (d[1]['pnl']))
                    # if 1 or last_trades.verbose:
                    #     print('-'*50)
                    #     print('balances addresses list:')
                    #     for address, value in balances_:
                    #         print(address)

                    #     print(last_trades.balance_table(order='pnl'))

                    start_time = time.time()
                    last_trades.create_dataframe()
                    dataframe = last_trades.dataframe
                    utils.save_log_strg(f"[new_trades_strategy()] create_dataframe() in {(time.time() - start_time):.3f}s, dataframe len={len(dataframe) if dataframe is not None else None}, total_trades={last_trades.total_trades_buy}/{last_trades.total_trades_sell}", address=address, block=last_block)

                    utils.save_log_strg(dataframe, address=address, block=last_block)
                    utils.save_log_strg(f"LastTrades() + get_trades() done in {(time.time() - start_time_g):.3f}s, dataframe.shape={dataframe.shape if dataframe is not None else 'None'}", address=address, block=last_block)

                    # _tx_mint_hashes = list(last_trades.txs_sql_mints.keys()) if last_trades.txs_sql_mints else {}
                    # if _tx_mint_hashes and (time.time() - last_trades.txs_sql_mints[_tx_mint_hashes[0]]['block_timestamp']) / 60 / 60 <= 0.5: # hours
                    #     # _time_tr = (time.time() - last_trades.txs_sql_mints[_tx_mint_hashes[0]]['block_timestamp']) / 60 / 60
                    #     # print(f"Отмена для address={address}, т.к. с момента первого Mint() прошло {_time_tr} часов")
                    #     utils.save_log_strg(f"_tx_mint_hashes and (time.time() - last_trades.txs_sql_mints[_tx_mint_hashes[0]]['block_timestamp']) / 60 / 60 <= 0.5 => return", address=address, block=last_block)
                    #     raise ArithmeticError(f"_tx_mint_hashes and (time.time() - last_trades.txs_sql_mints[_tx_mint_hashes[0]]['block_timestamp']) / 60 / 60 <= 0.5 => return")

                    if dataframe is None:
                        with FileLock(f"{FILES_DIR}/lib/logs/strategy_errors.log.lock"):
                            with open(f"{FILES_DIR}/lib/logs/strategy_errors.log", "a+") as file:
                                file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} почему-то dataframe=None для address={address}, block={last_block}\n")
                        utils.save_log_strg(f"dataframe is None => return", address=address, block=last_block)
                        raise ArithmeticError(f"dataframe is None => return")
                    
                    if len(dataframe) <= 3:
                        # print(f"(!) почему-то len(dataframe)={len(dataframe)} < 20 для address={address} на block={last_block} в new_trades_strategy()")
                        utils.save_log_strg(f"len(dataframe) <= 3 => return", address=address, block=last_block)
                        raise ArithmeticError(f"len(dataframe) <= 3 => return")





                    # (!)
                    if 0:
                        with open(f"{DIR_LOAD_OBJECTS}/{to_checksum_address(address)}.pkl", 'wb') as o:
                            pickle.dump(last_trades, o)






                    if 1 or dataframe.iloc[-1]['block'] == last_block or force_execute: # если был совершен трейд на last_block, который записан в dataframe
                        #self.execute_strategy(conn=conn_l, address=address, last_trades=last_trades, last_block=last_block, last_db_trade=last_db_trade, trf_sql=trf_sql, dataframe=dataframe, strategy_name='MlMatt', start_time_ex_bl=start_time_ex_bl, time_last_trades=time_last_trades, time_get_trades=time_get_trades)
                        
                        self.execute_strategy(strategy_name='Time', conn=conn_l, address=address, last_trades=last_trades, last_block=last_block, last_db_trade=last_db_trade, trf_sql=trf_sql, dataframe=dataframe, start_time_ex_bl=start_time_ex_bl, time_last_trades=time_last_trades, time_get_trades=time_get_trades, extra_data={})
                        #self.execute_strategy(strategy_name='MlCatboost', conn=conn_l, address=address, last_trades=last_trades, last_block=last_block, last_db_trade=last_db_trade, trf_sql=trf_sql, dataframe=dataframe, start_time_ex_bl=start_time_ex_bl, time_last_trades=time_last_trades, time_get_trades=time_get_trades, extra_data={})
                    else:
                        utils.save_log_strg(f"dataframe.iloc[-1]['block'] != last_block => return", address=address, block=last_block)
                        # print(f"Не будет отправки в execute_strategy() для address={address}, т.к. dataframe.iloc[-1]['block'] != last_block, {dataframe.iloc[-1]['block']} != {last_block}")


                    lt_logger.info(f"  block_number={last_block} address={address} завершен успешно за {(time.time() - start_time_g):.3f}s, len dataframe={len(dataframe)}")

                except:
                    if sys.exc_info()[0] == ArithmeticError or sys.exc_info()[0] == AssertionError or any([rr in str(sys.exc_info()[1]) for rr in ['Пары не найдено', 'Ошибка распределения txs_sql_token_owners', 'Неверное распределение token0/token1']]):
                        utils.save_log_strg(f"{sys.exc_info()[0]} {str(sys.exc_info()[1])}", address=address, block=last_block)
                        utils.save_log_strg(f"[new_trades_strategy()] done in {(time.time() - start_time_g):.3f}s (with error)", address=address, block=last_block)
                    else:
                        with FileLock(f"{FILES_DIR}/lib/logs/strategy_errors.log.lock"):
                            with open(f"{FILES_DIR}/lib/logs/strategy_errors.log", "a+") as file:
                                file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} error #1 on address={address}, block={last_block}, done in {(time.time() - start_time_g):.3f}s\n")
                                if sys.exc_info()[0] == KeyError and 'eth_total_min' in str(sys.exc_info()[1]):
                                    file.write(f"{sys.exc_info()}\n")
                                else:
                                    file.write(str(traceback.format_exc()) + "\n")
                        
                        utils.save_log_strg(traceback.format_exc(), address=address, block=last_block)
                        utils.save_log_strg(f"[new_trades_strategy()] done in {(time.time() - start_time_g):.3f}s (with error)", address=address, block=last_block)

                    lt_logger.info(f"  block_number={last_block} address={address} завершен с ошибкой #1 за {(time.time() - start_time_g):.3f}s {sys.exc_info()[0]}: {str(sys.exc_info()[1])}")
                    # lt_logger.info(f"  block_number={last_block} address={address} завершен с ошибкой за {(time.time() - start_time_g):.3f}s {str(traceback.format_exc())}")

                finally:

                    while 1:
                        try:
                            if conn_l.closed:
                                conn_l = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)
                            with conn_l.cursor() as cursor:
                                cursor.execute(f"""DELETE FROM last_trades_token_status
                                    WHERE script='realtime'
                                    and address='{address.lower()}'
                                """)
                                conn_l.commit()
                                # lt_logger.info(f"  block_number={last_block} address={address} сделан DELETE")
                                break
                        except:
                            try:
                                conn_l.rollback()
                            except:
                                pass
                            with FileLock(f"{FILES_DIR}/lib/logs/strategy_errors.log.lock"):
                                with open(f"{FILES_DIR}/lib/logs/strategy_errors.log", "a+") as file:
                                    file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} error DELETE FROM last_trades_token_status address={address}, last_block={last_block} [t{threading.get_native_id()} p{os.getpid()}]\n{str(traceback.format_exc())}")
                            lt_logger.info(f"  block_number={last_block} address={address} [t{threading.get_native_id()} p{os.getpid()}] завершен с ошибкой за {(time.time() - start_time_g):.3f}s DELETE FROM: {sys.exc_info()[0]} {str(sys.exc_info()[1])}")

                        time.sleep(1)


        except:


            lt_logger.info(f"EXCEPTION #1 new_trades_strategy() block_number={last_block} address={address}: {str(traceback.format_exc())}")
            print(f"-"*50, file=sys.stderr)
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} EXCEPTION #1 new_trades_strategy() block_number={last_block} address={address}: {sys.exc_info()}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)


            
        utils.save_log_strg(f"[new_trades_strategy()] END in {(time.time() - start_time_g):.3f}s, {(time.time() - start_time_ex_bl):.3f}s from exec_block()", address=address, block=last_block)
            

        return


    def execute_strategy(self, conn, address, last_trades, last_block, last_db_trade, trf_sql, dataframe, strategy_name, start_time_ex_bl, time_last_trades, time_get_trades, extra_data={}):

        start_time = time.time()

        if strategy_name == 'BBands':
            strategy = BBands(address=address, last_block=last_block)
        if strategy_name == 'MlMatt':
            strategy = MlMatt(address=address, last_block=last_block, last_db_trade=last_db_trade, model=ML_MODEL_RFC, scaler=ML_SCALER)
        if strategy_name == 'Time':
            strategy = Time(address=address, last_block=last_block, last_db_trade=last_db_trade)
        if strategy_name == 'MlCatboost':
            strategy = MlCatboost(address=address, last_block=last_block, last_db_trade=last_db_trade, model=catboost_model)


        dataframe = strategy.populate_indicators(dataframe=dataframe)

        utils.save_log_strg(f"dataframe after populate_indicators():", address=address, block=last_block)
        utils.save_log_strg(dataframe, address=address, block=last_block)
        if dataframe is None or dataframe.empty:
            utils.save_log_strg(f"dataframe = None => return", address=address, block=last_block)
            return
        else:
            utils.save_log_strg(dataframe.columns, address=address, block=last_block)

        dataframe = strategy.populate_entry_trend(dataframe=dataframe)
        utils.save_log_strg(f"dataframe['enter_long'].tail(200) after populate_entry_trend():", address=address, block=last_block)
        utils.save_log_strg(dataframe['enter_long'].tail(200).values, address=address, block=last_block)

        dataframe = strategy.populate_exit_trend(dataframe=dataframe)
        utils.save_log_strg(f"dataframe['exit_long'].tail(200) after populate_exit_trend():", address=address, block=last_block)
        utils.save_log_strg(dataframe['exit_long'].tail(200).values, address=address, block=last_block)

        time_strategy = time.time() - start_time

        _tx_mint_hashes = list(last_trades.txs_sql_mints.keys())
        token_age_days = (time.time() - last_trades.txs_sql_mints[_tx_mint_hashes[0]]['block_timestamp']) / 60 / 60 / 24 if _tx_mint_hashes else None

        
        last_action, last_action_tag = None, None
        if strategy_name == 'MlMatt':
            if dataframe['enter_long'].iloc[-1] == 1:
                last_action = 'buy'
                last_action_tag = dataframe['enter_tag'].iloc[-1]
            elif dataframe['exit_long'].iloc[-1] == 1:
                last_action = 'sell'
                last_action_tag = dataframe['exit_tag'].iloc[-1]
        else:
            dfn = dataframe.to_dict(orient="records")
            for i, row in enumerate(reversed(dfn)):
                # print(i, row['i'], row['date'], row['block'], f"enter_long={row['enter_long']}", f"enter_tag={row['enter_tag']}", f"exit_long={row['exit_long']}", f"exit_tag={row['exit_tag']}")
                if row['exit_long']:
                    # print(f" found exit_long, row['exit_tag']={row['exit_tag']}")
                    last_action = 'sell'
                    last_action_tag = row['exit_tag']
                    if i != 0: last_action_tag = f"{last_action_tag},delay={i}"
                    break
                if row['enter_long']:
                    # print(f" found enter_long, row['enter_tag']={row['enter_tag']}")
                    last_action = 'buy'
                    last_action_tag = row['enter_tag']
                    break


        # print(f"last_action={last_action}, last_action_tag={last_action_tag}")
        utils.save_log_strg(f"last_action={last_action}, last_action_tag={last_action_tag}", address=address, block=last_block)

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:


            cursor.execute(f"SELECT * FROM trades WHERE address='{address.lower()}' and backtest={BACKTEST_MODE} ORDER BY trans_block_number DESC LIMIT 1")
            _db_trade = cursor.fetchone()

            # if dataframe['exit_long'].iloc[-1] == 1:
            # if last_trade != None and last_trade['side'] == 'b' and last_trade['strategy'] == 'BBands' and last_action == 'sell':
            if _db_trade and _db_trade['side'] == 'b' and last_action == 'sell' and _db_trade['strategy'] == strategy_name:

                if BACKTEST_MODE:
                    tx_hash = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
                else:
                    pass
                    # ...

                if tx_hash != None:
                    assert trf_sql['token1_currency'] in ['WETH', 'USDC', 'USDT']
                    if trf_sql['token1_currency'] == 'WETH': token1_address = utils.WETH
                    if trf_sql['token1_currency'] == 'USDC': token1_address = utils.USDC
                    if trf_sql['token1_currency'] == 'USDT': token1_address = utils.USDT


                    profit  = 100 * dataframe['close'].iloc[-1] / float(_db_trade['trans_value_usd_per_token_unit']) - 100

                    cursor.execute("""INSERT INTO trades (
                            address,
                            pair,
                            token1_address,
                            exchange,
                            trans_block_number, -- номер блока в момент создания транзакции
                            trans_block_number_timestamp,
                            trans_value_usd_per_token_unit,
                            trans_liquidity_rp_usd,
                            value, -- в полном формате без деления на token_decimals
                            withdrew_usd,
                            gas_usd,
                            side,
                            status,
                            hash,
                            strategy,
                            backtest,
                            tag
                        ) VALUES %s;
                    """, ((
                            address.lower(),
                            trf_sql['pair_address'].lower(),
                            token1_address.lower(),
                            trf_sql['exchange'],
                            last_block,
                            last_trades.blocks_data[last_block]['timestamp'],
                            trf_sql['value_usd_per_token_unit'],
                            trf_sql['extra_data']['rp'],
                            _db_trade['value'] if BACKTEST_MODE else None, # тут поменять при realtime
                            100 + profit if BACKTEST_MODE else None, # и тут
                            0 if BACKTEST_MODE else None,
                            's',
                            None if not BACKTEST_MODE else 1,
                            tx_hash,
                            strategy_name,
                            1 if BACKTEST_MODE else 0,
                            last_action_tag
                    ),))
                    conn.commit()
                    

                    if profit > 0: emoji = "\N{ROCKET}"
                    else: emoji = "\N{CROSS MARK}"

                    duration = (last_block - _db_trade['trans_block_number']) * 12 / 60
                    msg = f"{emoji} SELL {strategy_name}! address=`{address}` profit=`{round(profit, 2)}%` tag=`{last_action_tag}` dur=`{round(duration, 1)}m` price=`{dataframe['close'].iloc[-1]}` price-buy=`{_db_trade['trans_value_usd_per_token_unit']}` liq=`{dataframe['liquidity'].iloc[-1]}` executed-in=`{(time.time() - start_time_ex_bl):.1f}s` time-last-trades=`{time_last_trades:.1f}s` time-get-trades=`{time_get_trades:.1f}s` time-strategy=`{time_strategy:.1f}s` block=`{last_block}` block-time=`{datetime.fromtimestamp(last_trades.blocks_data[last_block]['timestamp'])}` df-len=`{len(dataframe)}` token-age-days=`{round(token_age_days, 1) if token_age_days != None else 'old'}`"
                    utils.log_strategy_trades(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{last_block}] [{address}] [{strategy_name}] {msg}")

                    utils.save_log_strg(msg, address=address, block=last_block)


                    if strategy_name in ['MlMatt']:
                        resp = requests.get(f"https://api.telegram.org/bot..:../sendMessage?chat_id=-0&text={msg}&parse_mode=markdown")
                    else:
                        resp = requests.get(f"https://api.telegram.org/bot..:..-N6AN2Lg_c/sendMessage?chat_id=0&text={msg}&parse_mode=markdown")
                




            elif dataframe['enter_long'].iloc[-1] == 1 and dataframe.iloc[-1]['block'] == last_block:


                w3 = Web3(Web3.HTTPProvider(f"{self.testnet}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
                w3_eth_block_number = w3.eth.block_number
                if w3_eth_block_number - last_block >= 30:
                    utils.log_strategy_trades(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{last_block}] [{address}] [{strategy_name}] Не вошли в сделку, т.к. w3_eth_block_number - last_block = {w3_eth_block_number} - {last_block} = {w3_eth_block_number - last_block} >= 1, executed-in={(time.time() - start_time_ex_bl):.1f}s, time-last-trades={time_last_trades:.1f}s time-get-trades={time_get_trades:.1f}s, time-strategy={time_strategy:.1f}s, df-len={len(dataframe)}")
                    utils.save_log_strg(f"[{strategy_name}] Не вошли в сделку, т.к. w3_eth_block_number - last_block = {w3_eth_block_number} - {last_block} = {w3_eth_block_number - last_block} >= 1, executed-in={(time.time() - start_time_ex_bl):.1f}s, time-last-trades={time_last_trades:.1f}s time-get-trades={time_get_trades:.1f}s, time-strategy={time_strategy:.1f}s, df-len={len(dataframe)}", address=address, block=last_block)
                else:
        
                    # if last_trade and last_trade['side'] == 'b':
                    if _db_trade and _db_trade['side'] == 'b':
                        utils.log_strategy_trades(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{last_block}] [{address}] [{strategy_name}] Не вошли в сделку, т.к. уже есть трейд с side='b' по strategy={_db_trade['strategy']}")
                        utils.save_log_strg(f"[{strategy_name}] Не вошли в сделку, т.к. уже есть трейд с side='b' по strategy={_db_trade['strategy']}", address=address, block=last_block)
                    else:

                        if BACKTEST_MODE:
                            tx_hash = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
                        else:
                            tx_hash = self.enter_trade(w3, token, last_block, eth_price, start_time_ex_bl)

                        #if (tx_hash := self.enter_trade(w3, token, last_block, eth_price, start_time_ex_bl)):
                        #if (tx_hash := ''.join(random.choices(string.ascii_letters + string.digits, k=20))) != None:
                        if tx_hash != None:
                            assert trf_sql['token1_currency'] in ['WETH', 'USDC', 'USDT']
                            if trf_sql['token1_currency'] == 'WETH': token1_address = utils.WETH
                            if trf_sql['token1_currency'] == 'USDC': token1_address = utils.USDC
                            if trf_sql['token1_currency'] == 'USDT': token1_address = utils.USDT
                            cursor.execute("""INSERT INTO trades (
                                    address,
                                    pair,
                                    token1_address,
                                    exchange,
                                    trans_block_number, -- номер блока в момент создания транзакции
                                    trans_block_number_timestamp,
                                    trans_value_usd_per_token_unit,
                                    trans_liquidity_rp_usd,
                                    value, -- в полном формате без деления на token_decimals
                                    invested_usd,
                                    gas_usd,
                                    side,
                                    status,
                                    hash,
                                    strategy,
                                    backtest,
                                    tag
                                ) VALUES %s;
                            """, ((
                                    address.lower(),
                                    trf_sql['pair_address'].lower(),
                                    token1_address.lower(),
                                    trf_sql['exchange'],
                                    last_block,
                                    last_trades.blocks_data[last_block]['timestamp'],
                                    trf_sql['value_usd_per_token_unit'],
                                    trf_sql['extra_data']['rp'],
                                    100 // trf_sql['value_usd_per_token_unit'] if BACKTEST_MODE else None,
                                    100 if BACKTEST_MODE else None,
                                    0 if BACKTEST_MODE else None,
                                    'b',
                                    None if not BACKTEST_MODE else 1,
                                    tx_hash,
                                    strategy_name,
                                    1 if BACKTEST_MODE else 0,
                                    last_action_tag
                            ),))
                            conn.commit()


                            # if BACKTEST_MODE:
                            #     utils.log_strategy_trades(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{last_block}] [{address}] [{strategy_name}] BACKTEST_MODE mode, поэтому купили на 100$ кол-во {100 // trf_sql['value_usd_per_token_unit']} токенов")

                            msg = f"BUY {strategy_name}! address=`{address}` price=`{dataframe['close'].iloc[-1]}` liq=`{dataframe['liquidity'].iloc[-1]}` tag=`{last_action_tag}` executed-in=`{(time.time() - start_time_ex_bl):.1f}s` time-last-trades=`{time_last_trades:.1f}s` time-get-trades=`{time_get_trades:.1f}s` time-strategy=`{time_strategy:.1f}s` block=`{last_block}` block-time=`{datetime.fromtimestamp(last_trades.blocks_data[last_block]['timestamp'])}` df-len=`{len(dataframe)}` token-age-days=`{round(token_age_days, 1) if token_age_days != None else 'old'}`"
                            utils.save_log_strg(f"{msg}", address=address, block=last_block)

                            utils.log_strategy_trades(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{last_block}] [{address}] [{strategy_name}] {msg}")
                            

                            if strategy_name in ['MlMatt']:
                                resp = requests.get(f"https://api.telegram.org/bot..:../sendMessage?chat_id=-0&text={msg}&parse_mode=markdown")
                            else:
                                resp = requests.get(f"https://api.telegram.org/bot..:../sendMessage?chat_id=0&text={msg}&parse_mode=markdown")
                        
        return



    def check_connection(self, msg=""):

        global conn

        if conn.closed:


            with FileLock(f"{FILES_DIR}/lib/logs/strategy_errors.log.lock"):
                with open(f"{FILES_DIR}/lib/logs/strategy_errors.log", "a+") as file:
                    file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} conn closed check_connection() {msg}\n")

            conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)

            with FileLock(f"{FILES_DIR}/lib/logs/strategy_errors.log.lock"):
                with open(f"{FILES_DIR}/lib/logs/strategy_errors.log", "a+") as file:
                    file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} conn created check_connection() {msg}\n")

        # try:
        #     with conn.cursor() as cursor:
        #         pass
        # except psycopg2.InterfaceError as e: # connection already closed
        #     print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} error check_connection() {msg} {type(e)}: {e}", file=sys.stderr)
        #     with FileLock(f"{FILES_DIR}/lib/logs/strategy_errors.log.lock"):
        #         with open(f"{FILES_DIR}/lib/logs/strategy_errors.log", "a+") as file:
        #             file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} error check_connection() {msg} {type(e)}: {e}\n")
        #     conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)



    def check_pair_for_watching(self, pool, pair, mint_transaction, last_block, eth_price, start_time_ex_bl):

        global conn

        try:
            with conn.cursor() as cursor:
                pass
        except psycopg2.InterfaceError as e: # connection already closed
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {type(e)}: {e}", file=sys.stderr)
            conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)

        # threading.get_native_id() 2665478
        # threading.get_ident() 140294804281088

        thread_id = f"[check_pair_for_watching() t{threading.get_native_id()}]"
        prefix = f"[l{last_block}] {thread_id}"

        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} START for pair={pair}")

        start_time_e = time.time()
        
        with conn.cursor() as cursor:


            if pair == '0x9BdBEf447D43d691544aD9762F76D96D0eCbdc16'.lower():

                address = '0x284DEC557DBc13353fc14c393a9DfAA56773C479'

            elif pair == '0xE19B94676D9aD6d3b7e7527ac30A4F2253D6949d'.lower():
                
                address = '0xbc30a376BF142E578E07d048eb2E9579F3AA4480'

            else:

                start_time = time.time()
                have_mints_before = utils.have_mints_before(conn, pair=pair, until_block=last_block)
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {(time.time() - start_time):.3f}s have_mints_before()")
                if have_mints_before:
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP Уже был Mint() этой пары ({pair})")
                    return

                w3_g = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
                if not w3_g.is_connected():
                    raise Exception("w3_g not connected")

                start_time = time.time()
                address = utils.get_address_by_pair(w3_g, conn, pair=pair, last_block=last_block)
                if address == None:
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP address=None")
                    return
                address = to_checksum_address(address)
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {(time.time() - start_time):.3f}s get_address_by_pair(), address={address}", address)

            start_time = time.time()
            transactions_tupl = utils.get_token_transactions(conn, address=address, pair=pair, last_block=last_block, realtime=True)
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {(time.time() - start_time):.3f}s total get_token_transactions()", address)
            if transactions_tupl == 'stop':
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP transactions_tupl=stop", address)
                return

            mints_logger.info(f'new mint for address={address}, pair={pair} on block={last_block}, hash={mint_transaction}')
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} -> new mint for address={address}, pair={pair} on block={last_block}, hash={mint_transaction}", address)

            # msg = f"new mint: address=`{address}`, pair=`{pair}`"
            # start_time = time.time()
            # requests.get(f'http://sushko.net/telegram.php?m={msg}')
            # print(f"{(time.time() - start_time):.3f}s requests.get() sushko.net")

            try:
                _id = psycopg2.extras.execute_values(cursor, """
                    INSERT INTO watch (
                        address,
                        pair,
                        block_first_mint,
                        block_first_trade,
                        mint_transaction,
                        verified_at_block,
                        status
                    ) VALUES %s RETURNING id;
                """, ((
                        address.lower(),
                        pair.lower(),
                        last_block,
                        None,
                        mint_transaction,
                        None,
                        1
                ),), fetch=True)
                conn.commit()
            except:
                print(traceback.format_exc(), file=sys.stderr)
                conn.rollback()
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP insert #1 rollback(): {sys.exc_info()}", address)
                return


        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {(time.time() - start_time_e):.3f}s sql check pair for watching and inserting", address)

        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} returned id={_id[0][0]}, now will start decide_whats_next() for address={address}, pair={pair}", address)

        start_time = time.time()
        pool.starmap_async(self.decide_whats_next, [({'watch_id':_id[0][0],'address':address,'pair':pair,'block_first_mint':last_block,'block_first_trade':None,'verified_at_block':None,'watch_rows':None},last_block,eth_price,start_time_ex_bl,)]).get()

        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {(time.time() - start_time):.3f}s pool.starmap_async() на self.decide_whats_next() (это 1 адрес всего)", address)
        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} END for address={address}, pair={pair}", address)

        self.test_flag = True


        return


    # мониторинг пар только что созданных после первого Mint()
    def watch_for_predict(self, pool, last_block, eth_price, start_time_ex_bl):

        global conn

        try:
            with conn.cursor() as cursor:
                pass
        except psycopg2.InterfaceError as e: # connection already closed
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {type(e)}: {e}", file=sys.stderr)
            conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)


        thread_id = f"[watch() t{threading.get_native_id()}]"
        prefix = f"[l{last_block}] {thread_id}"

        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} START")



        with conn.cursor(cursor_factory=RealDictCursor) as cursor:

            try:
                # https://dba.stackexchange.com/questions/69655/select-columns-inside-json-agg

                # array_agg(json_build_object('block',block,'value_usd_per_token_unit',value_usd_per_token_unit,'predict',predict,'predict_prob',predict_prob,'data',data,'http_data',http_data)) watch_rows
                # jsonb_agg('block',block,'value_usd_per_token_unit',value_usd_per_token_unit,'predict',predict,'predict_prob',predict_prob,'data',data,'http_data',http_data) watch_rows
                # cursor.execute(f"""
                #     SELECT
                #         w.id as watch_id,
                #         address,
                #         pair,
                #         block_first_mint,
                #         block_first_trade,
                #         verified_at_block,
                #         jsonb_agg('block',block,'value_usd_per_token_unit',value_usd_per_token_unit) watch_rows
                #     FROM watch w
                #     LEFT JOIN watch_predicts wp ON w.id = wp.watch_id
                #     WHERE status=1 and block_first_mint!={last_block}
                #     GROUP BY w.id
                # """)

                # (!) order by почему-то не работает
                cursor.execute(f"""
                    SELECT
                        w.id as watch_id,
                        address,
                        pair,
                        block_first_mint,
                        block_first_trade,
                        verified_at_block,
                        case when count(wp) = 0 then '[]' else jsonb_agg((SELECT x FROM (SELECT wp.id, wp.block, wp.data, wp.http_data, wp.executed_in ORDER BY wp.block ASC) AS x ORDER BY wp.block ASC)) end AS watch_rows
                    FROM watch w
                    LEFT JOIN watch_predicts wp ON w.id = wp.watch_id
                    WHERE status=1 and block_first_mint!={last_block}
                    GROUP BY w.id
                """)
                _rows = cursor.fetchall()
            except:
                print(traceback.format_exc(), file=sys.stderr)
                conn.rollback()
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} EXCEPTION select * from watch w left join rollback(): {sys.exc_info()}")
                return


        params = []
        for watch_sql in _rows:
            params.append((dict(watch_sql),last_block,eth_price,start_time_ex_bl,))

        start_time = time.time()
        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} запускаю pool в watch(), _rows/params кол-во адресов={len(_rows)}")

        try:
            _ = pool.starmap_async(self.decide_whats_next, params).get()
        except:
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} EXCEPTION watch() {sys.exc_info()}")
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} watch() Exception {sys.exc_info()}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)

        
        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {(time.time() - start_time):.3f}s pool.starmap_async() на self.decide_whats_next() (кол-во адресов={len(params)})")





        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} END")



        return
    


    # ф-ция вызывается из watch_for_predict() и check_pair_for_watching() для решения входить в сделку или нет после первого Mint()
    def decide_whats_next(self, watch_sql, last_block, eth_price, start_time_ex_bl):
        """
        watch_sql
            :watch_id:
            :address:
            :pair:
            :block_first_mint:
            :block_first_trade:
            :verified_at_block:
            :watch_rows: результаты с предыдущих предсказаний
                :id:
                :block:
                :data:
                :http_data:
        last_block
        """

        global conn


        address = watch_sql['address']
        pair = watch_sql['pair']
        watch_sql['watch_rows'] = [] if watch_sql['watch_rows'] == None else watch_sql['watch_rows']


        thread_ether = None
        if watch_sql['verified_at_block'] == None:
            thread_ether = utils.ThreadWithReturnValue(target=utils.http_request, args=('ether', address))
            thread_ether.start()

        thread_gopluslabs, thread_honeypotis = None, None
        if len(watch_sql['watch_rows']) <= 3 \
            or len(watch_sql['watch_rows']) in [7, 11] \
            or len(watch_sql['watch_rows']) > 15 and len(watch_sql['watch_rows']) % 8 == 0:
            thread_gopluslabs = utils.ThreadWithReturnValue(target=utils.http_request, args=('gopluslabs', address))
            thread_gopluslabs.start()

            thread_honeypotis = utils.ThreadWithReturnValue(target=utils.http_request, args=('honeypotis', address))
            thread_honeypotis.start()
            


        prefix = f"[l{last_block}] [decide_whats_next() t{threading.get_native_id()} p{os.getpid()}]"


        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} START on block={last_block}, address={address}", address)

        start_time = time.time()
        try:
            token = ml.get_token_stat_for_ml(ia=None, address=address, pair=pair, last_block=last_block, realtime=True)
            if token != None and not(type(token) == dict and 'error' in token):
                #assert token.last_transaction_block == last_block, f"{token.last_transaction_block} {last_block} address={address}"
                if token.last_transaction_block != last_block:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Похоже нет транзакций на last_block={last_block}, token.last_transaction_block={token.last_transaction_block} для address={address}", address)
                assert watch_sql['block_first_trade'] == None or watch_sql['block_first_trade'] != None and token.block_first_trade == watch_sql['block_first_trade'], f"{token.block_first_trade}, {watch_sql['block_first_trade']} address={address}"
        except Exception as e:
            token = None
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} token Exception: {e}", address)
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} token Exception: {e}, address={address}", file=sys.stderr)
            print(traceback.format_exc())
            print(traceback.format_exc(), file=sys.stderr)
        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {(time.time() - start_time):.3f}s total ml.get_token_stat_for_ml() для address={address}", address)


        try:
            with conn.cursor() as cursor:
                pass
        except psycopg2.InterfaceError as e: # connection already closed
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {type(e)}: {e}", file=sys.stderr)
            conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)


        with conn.cursor() as cursor:

            
            if token == None or type(token) == dict and 'error' in token or token.block_first_trade == None:

                if token == None:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} token = None", address)
                elif type(token) == dict and 'error' in token:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} token error={token['error']}", address)
                elif token != None and token.block_first_trade == None:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} block_first_trade = None", address)


                # stop watching
                if last_block - watch_sql['block_first_mint'] > 200 or (type(token) == dict and 'error' in token and token['error'] == "Unknown owner confirmation"):

                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Завершаем watch с ошибкой для address={address}, прошло {last_block - watch_sql['block_first_mint']} блоков", address)
                    mints_logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Завершаем watch с ошибкой для address={address}, прошло {last_block - watch_sql['block_first_mint']} блоков")
                    try:
                        cursor.execute(f"""
                            UPDATE watch
                            SET
                                status=0
                            WHERE id={watch_sql['watch_id']}
                        """)
                        # FOR UPDATE SKIP LOCKED
                        conn.commit()
                    except:
                        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP update #1 rollback(): {sys.exc_info()}", file=sys.stderr)
                        print(traceback.format_exc(), file=sys.stderr)
                        conn.rollback()
                        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP update #1 rollback(): {sys.exc_info()}", address)
                    return
                
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} => continue", address)
                return


            # update block_first_trade when trading started
            if token.block_first_trade != None and watch_sql['block_first_trade'] == None:
                watch_sql['block_first_trade'] = token.block_first_trade
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} token.block_first_trade={token.block_first_trade} и block_first_trade=None для address={address}, обновим в бд", address)
                try:
                    cursor.execute(f"""
                        UPDATE watch
                        SET
                            block_first_trade={token.block_first_trade}
                        WHERE id={watch_sql['watch_id']}
                    """)
                    conn.commit()
                except:
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} ERROR update #1.1 rollback(): {sys.exc_info()}", file=sys.stderr)
                    print(traceback.format_exc(), file=sys.stderr)
                    conn.rollback()
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} ERROR update #1.1 rollback(): {sys.exc_info()}", address)


            blocks_now_ft_diff = last_block - watch_sql['block_first_trade'] if watch_sql['block_first_trade'] != None else None
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} blocks_now_ft_diff={blocks_now_ft_diff}", address)

            # # _x3s = int(list(_sl['all_blocks_stats'].keys())[-1])
            # _x3s = token.last_transaction_block - token.block_first_trade
            # if blocks_now_ft_diff != _x3s:
            #     utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} (!) почему-то blocks_now_ft_diff={blocks_now_ft_diff}!={_x3s} разнице token.last_transaction_block - token.block_first_trade для address={address}", address)
            #     mints_logger.info(f"(!) почему-то blocks_now_ft_diff={blocks_now_ft_diff}!={_x3s} разнице token.last_transaction_block - token.block_first_trade для address={address}")
            # else:
            #     utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} все сходится, blocks_now_ft_diff={blocks_now_ft_diff} равен token.last_transaction_block - token.block_first_trade для address={address}", address)
            # assert blocks_now_ft_diff == _x3s


            # if pair.lower() in ['0x9BdBEf447D43d691544aD9762F76D96D0eCbdc16'.lower()]:
                
            #     new_blocks_now_ft_diff = int(list(_sl['all_blocks_stats'].keys())[-1])
            #     print(f"{prefix} для backtest'a меняем blocks_now_ft_diff={blocks_now_ft_diff} на {new_blocks_now_ft_diff}")
            #     blocks_now_ft_diff = new_blocks_now_ft_diff

            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} stat done on block={last_block}, blocks_now_ft_diff={blocks_now_ft_diff} for address={address}, watch_rows len={len(watch_sql['watch_rows'])}", address)
            if token == None:
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP token == None", address)
                return
            else:
                if token.block_first_trade != None:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} for address={address}, transactions_count={token.blocks_data[str(token.last_transaction_block)]['transactions_count']}, trades=[{token.blocks_data[str(token.last_transaction_block)]['trades_buy']}/{token.blocks_data[str(token.last_transaction_block)]['trades_sell']}], price_change={token.blocks_data[str(token.last_transaction_block)][list(token.pairs.keys())[0]]['price_change']:.2f}% value_usd_per_token_unit={token.value_usd_per_token_unit}", address)
                else:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} for address={address}, transactions_count={token.blocks_data[str(token.last_transaction_block)]['transactions_count']}, trades=[{token.blocks_data[str(token.last_transaction_block)]['trades_buy']}/{token.blocks_data[str(token.last_transaction_block)]['trades_sell']}], value_usd_per_token_unit={token.value_usd_per_token_unit}", address)



            # print(f'{prefix} watch_rows:')
            # pprint(watch_sql['watch_rows'])

            # utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Массив watch_rows: {watch_sql['watch_rows']}", address)
            # watch_sql['watch_rows'] = sorted(watch_sql['watch_rows'], key=lambda d: d['block'])
            # msg = f"Предыдущие значения watch_row ({len(watch_sql['watch_rows'])}) для address={address}:"
            # for wr in watch_sql['watch_rows']:
            #     # print(f'{prefix} wr:', wr)
            #     msg += f"\n block: {wr['block']}, blocks_from_first_trade: {wr['block'] - watch_sql['block_first_trade']}, transactions: {wr['data']['transactions_count']}, price_change: {wr['data']['price_change_from_inited']:.2f}%, trades: {wr['data']['trades_buy'] + wr['data']['trades_sell']} [{wr['data']['trades_buy']}/{wr['data']['trades_sell']}], predict: {wr['predict']} {round(100*wr['predict_prob'])}%, data: {wr['data']}"
            # utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {msg}", address)


            # utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} начинаем predict_token() на блоке {last_block}, blocks_now_ft_diff={blocks_now_ft_diff}, address={address}", address)
            # utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} all_blocks_stats.key(): {list(_sl['all_blocks_stats'].keys())}", address)
            
            
            # predicts = token.blocks_data[str(token.last_transaction_block)]['prediction']

            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} block prediction: {token.blocks_data[str(token.last_transaction_block)]['prediction']}, all {token.blocks_data[str(token.last_transaction_block)]['prediction_all']}", address)
            
            # if list(token.steps_data.keys()):
            #     last_step_data = token.steps_data[list(token.steps_data.keys())[-1]]
            #     utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} last_step={list(token.steps_data.keys())[-1]}, step prediction: {last_step_data['prediction']}, all {last_step_data['prediction_all']}", address)
            #     predicts = last_step_data['prediction_all']
            #     utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} predicts взят из token.steps_data {predicts}")
            # else:
            #     utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} last_step=None", address)
            #     predicts = token.blocks_data[str(token.last_transaction_block)]['prediction_all']
            #     utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} predicts взят из token.blocks_data {predicts}")
            
            _predict_source = 'None'
            predicts = token.blocks_data[str(token.last_transaction_block)]['prediction_all']
            if token.blocks_data[str(token.last_transaction_block)]['prediction'] != None and token.blocks_data[str(token.last_transaction_block)]['prediction'][0] == 1 and token.blocks_data[str(token.last_transaction_block)]['prediction'][1] >= 0.80:
                predicts = token.blocks_data[str(token.last_transaction_block)]['prediction']
                _predict_source = f'pred-by-block'
            elif token.blocks_data[str(token.last_transaction_block)]['prediction_all'][0] == 1:
                predicts = token.blocks_data[str(token.last_transaction_block)]['prediction_all']
                _predict_source = f'predall'
            else:
                for step in list(token.steps_data.keys()):
                    if token.steps_data[step]['prediction'][0] == 1 and token.steps_data[step]['prediction'][1] >= 0.80:
                        predicts = token.steps_data[step]['prediction']
                        _predict_source = f'step{step}-pred'
                        break
                    elif token.steps_data[step]['prediction_all'][0] == 1:
                        predicts = token.steps_data[step]['prediction_all']
                        _predict_source = f'step{step}-predall'
                        break
                
            
            prediction_all = token.blocks_data[str(token.last_transaction_block)]['prediction_all'][1] if token.blocks_data[str(token.last_transaction_block)]['prediction_all'][0] == 1 else 1 - token.blocks_data[str(token.last_transaction_block)]['prediction_all'][1]
            prediction = 0
            if token.blocks_data[str(token.last_transaction_block)]['prediction'] != None:
                prediction = token.blocks_data[str(token.last_transaction_block)]['prediction'][1] if token.blocks_data[str(token.last_transaction_block)]['prediction'][0] == 1 else 1 - token.blocks_data[str(token.last_transaction_block)]['prediction'][1]
            prediction_steptrades = 0
            prediction_steptrades_all = 0
            for step in list(token.steps_data.keys()):
                if token.steps_data[step]['prediction'][0] == 1 and token.steps_data[step]['prediction'][1] > prediction_steptrades:
                    prediction_steptrades = token.steps_data[step]['prediction'][1]
                if token.steps_data[step]['prediction_all'][0] == 1 and token.steps_data[step]['prediction_all'][1] > prediction_steptrades_all:
                    prediction_steptrades_all = token.steps_data[step]['prediction_all'][1]
        


            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} находимся перед if thread_ether != None:", address)
            if thread_ether != None:
                contract_verified = thread_ether.join()
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Сделали запрос contract_verified={contract_verified}", address)
                if contract_verified:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Сохраняем watch_sql[verified_at_block]={last_block}", address)
                    watch_sql['verified_at_block'] = last_block
                    try:
                        cursor.execute("""UPDATE watch
                            SET
                                verified_at_block=%s
                            WHERE id=%s
                        """, (watch_sql['verified_at_block'], watch_sql['watch_id']))
                        conn.commit()
                    except:
                        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP update #3 rollback(): {sys.exc_info()}", file=sys.stderr)
                        print(traceback.format_exc(), file=sys.stderr)
                        conn.rollback()
                        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP update #3 rollback(): {sys.exc_info()}", address)


            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} находимся перед if thread_gopluslabs:", address)
            if thread_gopluslabs:
                thread_gopluslabs = thread_gopluslabs.join()
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} находимся перед if thread_honeypotis:", address)
            if thread_honeypotis:
                thread_honeypotis = thread_honeypotis.join()
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} thread_honeypotis подсчитан", address)

            msg = f"[{watch_sql['address'][:10]}] Predict #{len(watch_sql['watch_rows']) + 1} на block={last_block}, blocks_now_ft_diff={blocks_now_ft_diff} (watch rows={len(watch_sql['watch_rows'])}), executed_in={(time.time() - start_time_ex_bl):.3f}s, transactions={token.blocks_data[str(token.last_transaction_block)]['transactions_count']}, verified_at_block={watch_sql['verified_at_block']} для address={address}: {predicts[0]}, {round(predicts[1]*100)}%"
            http_data = {}
            if thread_gopluslabs != None:
                msg += f"\n gopluslabs={thread_gopluslabs}"
                msg += f"\n honeypotis={thread_honeypotis}"
                http_data = {'gopluslabs': thread_gopluslabs, 'honeypotis': thread_honeypotis}

            mints_logger.info(msg)
            utils.save_log(msg, address)

            # смысла append() здесь не несет, но нужно для mints_logger print() дальше
            watch_sql['watch_rows'].append({
                'id': '?',
                'watch_id': watch_sql['watch_id'],
                'block': last_block,
                'http_data': http_data,
                'executed_in': time.time() - start_time_ex_bl
            })

            # print(f'KKKKKK WILL SEND MESSAGE blocks_now_ft_diff=`{blocks_now_ft_diff}`')
            # #msg = f"BUY! block=`{last_block}` blocks_now_ft_diff=`{blocks_now_ft_diff}` address=`{address}` pair=`{pair}`"
            # msg = f"BUY! block=`{last_block}` blocks_now_ft_diff={blocks_now_ft_diff} address=`{address}` pair=`{pair}`"
            # #requests.get(f'http://sushko.net/telegram.php?m={msg}')
            # requests.get(f"https://api.telegram.org/bot..:../sendMessage?chat_id=0&text={msg}&parse_mode=markdown")
            # print('DONE!')




            # if predicts['firstblock'][0] == 1:
            #     msg = f"beforemint: {predicts['beforemint'][1][1]:2f}, firstblock: {predicts['firstblock'][1][1]:2f}\n"
            #     msg += f"address=`{_sl['address']}`, pair=`{pair}`"
            #     requests.get(f'http://sushko.net/telegram.php?m={msg}')


            watch_sql['watch_rows'] = sorted(watch_sql['watch_rows'], key=lambda d: d['block'])

            # делаем в цикле, так как не на каждом блоке запрашивали данные
            last_http_data = {}
            msg_watch_rows = ''
            for ii, wr in enumerate(watch_sql['watch_rows']):

                if str(wr['block']) in token.blocks_data:

                    # msg_watch_rows += f"\n #{ii+1} wr.id: {wr['id']}, block: {wr['block']}, blocks_from_first_trade: {wr['block'] - watch_sql['block_first_trade']}, transactions: {wr['data']['transactions_count']}, trades: {wr['data']['trades_buy'] + wr['data']['trades_sell']} [{wr['data']['trades_buy']}/{wr['data']['trades_sell']}], price_change: {wr['data']['price_change_from_inited']:.2f}%, executed_in={wr['executed_in']:.2f}s"
                    msg_watch_rows += f"\n "
                    msg_watch_rows += f"#{ii} wr.id: {wr['id']}, "
                    msg_watch_rows += f"block: {wr['block']} (block{wr['block'] - watch_sql['block_first_trade']}), "
                    msg_watch_rows += f"transactions: {token.blocks_data[str(wr['block'])]['transactions_count']}, "
                    msg_watch_rows += f"trades: {token.blocks_data[str(wr['block'])]['trades_buy'] + token.blocks_data[str(wr['block'])]['trades_sell']} [{token.blocks_data[str(wr['block'])]['trades_buy']}/{token.blocks_data[str(wr['block'])]['trades_sell']}], "
                    msg_watch_rows += f"price_change: {token.blocks_data[str(wr['block'])][list(token.pairs.keys())[0]]['price_change']:.2f}%, "
                    msg_watch_rows += f"predict: {token.blocks_data[str(wr['block'])]['prediction']}, all {token.blocks_data[str(wr['block'])]['prediction_all']} "
                    msg_watch_rows += f"executed_in={wr['executed_in']:.2f}s"

                    msg_pred_steps = ''
                    for step in token.steps_data.keys():
                        if token.steps_data[step]['block'] == wr['block']:
                            msg_pred_steps += f"\n    "
                            msg_pred_steps += f"block{token.steps_data[step]['blocks_now_ft_diff']}, "
                            msg_pred_steps += f"trades {token.steps_data[step]['trades_buy']+token.steps_data[step]['trades_sell']} [{token.steps_data[step]['trades_buy']}/{token.steps_data[step]['trades_sell']}], "
                            msg_pred_steps += f"price_change {round(token.steps_data[step]['price_change'])}%, "
                            msg_pred_steps += f"prediction {token.steps_data[step]['prediction']}, "
                            msg_pred_steps += f"prediction_all {token.steps_data[step]['prediction_all']}"
                    msg_watch_rows += msg_pred_steps
                
                else:
                    msg_watch_rows += f"\n "
                    msg_watch_rows += f"#{ii} wr.id: {wr['id']}, "
                    msg_watch_rows += f"block: {wr['block']} (block{wr['block'] - watch_sql['block_first_trade']}), "
                    msg_watch_rows += f"NO block key in token.blocks_data[], "
                    msg_watch_rows += f"executed_in={wr['executed_in']:.2f}s"

                if wr['http_data'] != {}:
                    last_http_data = wr['http_data']


            # _uniq_trades = None
            # try:

            #     if not conn.closed:
            #         print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} conn почему-то закрыто перед SELECT COUNT(address), поэтому открываем новое", file=sys.stderr)
            #         try:
            #             with conn.cursor() as cursor:
            #                 pass
            #         except psycopg2.InterfaceError as e: # connection already closed
            #             print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} ошибка открытия conn перед SELECT COUNT(address) {type(e)}: {e}", file=sys.stderr)
            #             conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)
            #         cursor = conn.cursor()

            #     cursor.execute(f"SELECT COUNT(address) FROM trades where side='b'")
            #     _uniq_trades = cursor.fetchone()
            #     if _uniq_trades != None: _uniq_trades = _uniq_trades[0]
            #     else: _uniq_trades = 0
            # except psycopg2.ProgrammingError: # no results to fetch
            #     _uniq_trades = 0
            # except psycopg2.DatabaseError:
            #     print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} Для address={address} возникла psycopg2.DatabaseError", file=sys.stderr)
            #     print(traceback.format_exc(), file=sys.stderr)
            #     if not conn.closed:
            #         print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} conn НЕ закрыто, поэтому закрываем и открываем новое", file=sys.stderr)
            #         conn.close()
            #     else:
            #         print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} conn закрыто, поэтому открываем новое", file=sys.stderr)
            #     conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)
            #     cursor = conn.cursor()
            # except Exception as e:
            #     print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} rolling SELECT trades #1: {e}", file=sys.stderr)
            #     print(traceback.format_exc(), file=sys.stderr)
            #     conn.rollback()
            #     utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} rolling SELECT trades #1: {e}", address)


            # print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} _uniq_trades={_uniq_trades}")
            # #if _uniq_trades > 0:
            # trades_logger.info(f"{prefix} _uniq_trades={_uniq_trades}")



            entered_trade = False

            
            

            # if blocks_now_ft_diff >= 1 \
            #     and _uniq_trades == 0:


            # and not self.flag_no_buy_anymore \
            # and _uniq_trades == 0 \

            # msg = f"BUY! block=`{last_block}` ft=`{blocks_now_ft_diff}` address=`{address}` pair=`{pair}`, predict=`{round(100*predicts[1])}%` {_predict_source} slippage=00 trades={token.blocks_data[str(token.last_transaction_block)]['trades_buy'] + token.blocks_data[str(token.last_transaction_block)]['trades_sell']} [{token.blocks_data[str(token.last_transaction_block)]['trades_buy']}/{token.blocks_data[str(token.last_transaction_block)]['trades_sell']}]"
            # #msg = f"BUY! block=`{last_block}`"
            # resp = requests.get(f"https://api.telegram.org/bot..:/sendMessage?chat_id=0&text={msg}&parse_mode=markdown")
            # mints_logger.info('resp res:')
            # mints_logger.info(resp.text)

            if blocks_now_ft_diff >= 0 \
                and token.blocks_data[str(token.last_transaction_block)][list(token.pairs.keys())[0]]['price_change'] < 5000 \
                and watch_sql['verified_at_block'] != None \
                and (
                    last_http_data['gopluslabs'] and 'error' not in last_http_data['gopluslabs'] and 'scam' in last_http_data['gopluslabs'] and not last_http_data['gopluslabs']['scam'] \
                    or 'scam' not in last_http_data['gopluslabs'] \
                ) \
                and last_http_data['honeypotis'] and 'error' not in last_http_data['honeypotis'] and 'scam' in last_http_data['honeypotis'] and not last_http_data['honeypotis']['scam'] \
                and blocks_now_ft_diff <= 3 \
                and (prediction >= 0.66 or prediction_steptrades >= 0.69):

                    slippage = 0
                    if 'warning' not in last_http_data['gopluslabs']: slippage = '?'
                    elif 'slippage_modifiable=1' in last_http_data['gopluslabs']['warning']: slippage = 1

                    msg = f"BUY! block=`{last_block}` ft=`{blocks_now_ft_diff}` address=`{address}` prediction=`{round(100*prediction)}%` prediction-steptrades=`{round(100*prediction_steptrades)}%` pred-source=`{_predict_source}` slippage=`{slippage}` trades={token.blocks_data[str(token.last_transaction_block)]['trades_buy'] + token.blocks_data[str(token.last_transaction_block)]['trades_sell']} [{token.blocks_data[str(token.last_transaction_block)]['trades_buy']}/{token.blocks_data[str(token.last_transaction_block)]['trades_sell']}] price-change=`{token.blocks_data[str(token.last_transaction_block)][list(token.pairs.keys())[0]]['price_change']:.1f}%` executed-in=`{(time.time() - start_time_ex_bl):.1f}s` block-time=`{datetime.fromtimestamp(token.blocks_data[str(token.last_transaction_block)]['timestamp'])}` red flags: {token.red_flags}, bribe sent: {token.blocks_data[str(token.block_first_trade)]['bribe_sent']}"

                    # requests.get(f'http://sushko.net/telegram.php?m={msg}')
                    # urlencode($messaggio)
                    mints_logger.info(msg)
                    resp = requests.get(f"https://api.telegram.org/bot..:../sendMessage?chat_id=0&text={msg}&parse_mode=markdown")
                    mints_logger.info(resp.text)
                    entered_trade = True




            if blocks_now_ft_diff >= 0 \
                and 0 \
                and token.blocks_data[str(token.last_transaction_block)][list(token.pairs.keys())[0]]['price_change'] < 4000 \
                and watch_sql['verified_at_block'] != None \
                and last_http_data['gopluslabs'] and 'error' not in last_http_data['gopluslabs'] and 'scam' in last_http_data['gopluslabs'] and not last_http_data['gopluslabs']['scam'] \
                and last_http_data['honeypotis'] and 'error' not in last_http_data['honeypotis'] and 'scam' in last_http_data['honeypotis'] and not last_http_data['honeypotis']['scam'] \
                and predicts[0] == 1 and predicts[1] >= 0.50:



                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Базовые условия для входа в сделку есть, address={address}", address)


                w3 = Web3(Web3.HTTPProvider(f"{self.testnet}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
                w3_eth_block_number = w3.eth.block_number
                if w3_eth_block_number - last_block > 1:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Не вошли в сделку, т.к. w3_eth_block_number - last_block = {w3_eth_block_number - last_block} > 1, address={address}", address)
                else:
                
                    try:
                        cursor.execute(f"SELECT 1 FROM trades WHERE address='{address.lower()}'")
                        _exists_block = cursor.fetchone()
                        if _exists_block != None:
                            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Уже есть трейд на address={address}", address)
                        else:
                            #if (tx_hash := self.enter_trade(w3, token, last_block, eth_price, start_time_ex_bl)):
                            if (tx_hash := ''.join(random.choices(string.ascii_letters + string.digits, k=20))) != None:
                                cursor.execute("""INSERT INTO trades (
                                        address,
                                        pair,
                                        token1_address,
                                        trans_block_number,
                                        trans_value_usd_per_token_unit,
                                        http_data,
                                        side,
                                        status,
                                        hash
                                    ) VALUES %s;
                                """, ((
                                        address.lower(),
                                        pair.lower(),
                                        token.pairs[list(token.pairs.keys())[0]]['token1_address'].lower(),
                                        last_block,
                                        token.value_usd_per_token_unit,
                                        json.dumps(last_http_data),
                                        'b',
                                        None,
                                        tx_hash
                                ),))
                                conn.commit()

                                mints_logger.info(f"{prefix} + BUY! address={address}, pair={pair}, predict={round(100*predicts[1])}%")
                                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} + BUY! address={address}, pair={pair}, predict={round(100*predicts[1])}%", address)

                                msg = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Покупка для address={address}, verified_at_block={watch_sql['verified_at_block']}:"
                                msg += msg_watch_rows
                                utils.save_log(f"{msg}", address)
                                utils.save_log(f"{token.value_usd_per_token_unit:.20f} - value_usd_per_token_unit", address)

                                trades_logger.info(msg)
                                trades_logger.info(f"{token.value_usd_per_token_unit:.20f} - value_usd_per_token_unit")


                                msg = f"BUY! block=`{last_block}` ft=`{blocks_now_ft_diff}` address=`{address}` pair=`{pair}`, predict=`{round(100*predicts[1])}%`"
                                requests.get(f'http://sushko.net/telegram.php?m={msg}')


                                try:
                                    token1_address = token.pairs[list(token.pairs.keys())[0]]['token1_address']
                                    if token1_address == utils.WETH: token1_currency = 'WETH'
                                    elif token1_address == utils.USDC: token1_currency = 'USDC'
                                    elif token1_address == utils.USDT: token1_currency = 'USDT'
                                    _kwards = utils.usd_per_one_token(w3=w3, address=to_checksum_address(address), block=last_block, pair_address=to_checksum_address(pair), token1_currency=token1_currency, tx_eth_price=eth_price)
                                    value_usd_per_token_unit = _kwards['value_usd_per_token_unit']
                                    reserves_token = _kwards['reserves_token']
                                    reserves_token_usd = _kwards['reserves_token_usd']
                                    reserves_pair = _kwards['reserves_pair']
                                    reserves_pair_usd = _kwards['reserves_pair_usd']
                                    pair_address = _kwards['pair_address']
                                    token1_currency = _kwards['token1_currency']
                                    exchange = _kwards['exchange']
                                    utils.save_log(f"{value_usd_per_token_unit:.20f} - value_usd_per_token_unit via utils.usd_per_one_token()", address)
                                except Exception as e:
                                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Ошибка получения utils.usd_per_one_token для adress={address}: {e}")
                                    print(traceback.format_exc(), file=sys.stderr)
                                    print(traceback.format_exc())


                                msg = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} last_http_data:"
                                for service, res in last_http_data.items():
                                    msg += f"\n  {service}: {res}"
                                utils.save_log(f"{msg}", address)

                                entered_trade = True

                    except Exception as e:
                        print(traceback.format_exc(), file=sys.stderr)
                        conn.rollback()
                        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} rolling INSERT INTO trades: {e}", address)

            else:
                msg = f'Не соблюдены условия для входа в сделку для address={address}'
                msg += f"\n blocks_now_ft_diff={blocks_now_ft_diff}"
                msg += f"\n verified_at_block={watch_sql['verified_at_block']}"
                msg += f"\n price_change={token.blocks_data[str(token.last_transaction_block)][list(token.pairs.keys())[0]]['price_change']}"
                msg += f"\n last_http_data['gopluslabs']={last_http_data['gopluslabs']}"
                msg += f"\n last_http_data['honeypotis']={last_http_data['honeypotis']}"
                msg += f"\n predicts: {predicts}"
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {msg}", address, stdout=False)




            # blocks_now_ft_diff | len(watch_sql['watch_rows'])
            if entered_trade or len(watch_sql['watch_rows']) >= 30 or token.blocks_data[str(token.last_transaction_block)]['transactions_count'] > 150:

                msg = 'Т.к. '
                if entered_trade:
                    msg += 'entered_trade=True'
                elif len(watch_sql['watch_rows']) >= 30:
                    msg += "len(watch_sql['watch_rows']) >= 30"
                elif token.blocks_data[str(token.last_transaction_block)]['transactions_count'] > 150:
                    msg += "transactions_count > 150"

                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {msg}, завершаем watch для address={address}, pair={pair} на blocks_now_ft_diff={blocks_now_ft_diff}, watch_rows len={len(watch_sql['watch_rows'])}, transactions={token.blocks_data[str(token.last_transaction_block)]['transactions_count']}", address)
                mints_logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {msg}, завершаем watch для address={address}, pair={pair} на blocks_now_ft_diff={blocks_now_ft_diff}, watch_rows len={len(watch_sql['watch_rows'])}, transactions={token.blocks_data[str(token.last_transaction_block)]['transactions_count']}")

                msg = f"Итого для address={address}, verified_at_block={watch_sql['verified_at_block']}:"
                
                for service, res in watch_sql['watch_rows'][0]['http_data'].items():
                    msg += f"\n  {service}: {res}"

                msg += msg_watch_rows
                
                for service, res in last_http_data.items():
                    msg += f"\n  {service}: {res}"
                mints_logger.info(f"{msg}")
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {msg}", address)


                try:
                    cursor.execute(f"""
                        UPDATE watch
                        SET
                            status=0
                        WHERE id={watch_sql['watch_id']}
                    """)
                    conn.commit()
                except:
                    print(traceback.format_exc(), file=sys.stderr)
                    conn.rollback()
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP update #2 rollback(): {sys.exc_info()}", address)
                    return



            try:
                cursor.execute("""INSERT INTO watch_predicts (
                        watch_id,
                        block,
                        data,
                        http_data,
                        executed_in
                    ) VALUES %s;
                """, ((
                        watch_sql['watch_id'],
                        last_block,
                        json.dumps({
                            'transactions_count': token.blocks_data[str(token.last_transaction_block)]['transactions_count'],
                            'trades_buy': token.blocks_data[str(token.last_transaction_block)]['trades_buy'],
                            'trades_sell': token.blocks_data[str(token.last_transaction_block)]['trades_sell'],
                            'price_change_from_inited': token.blocks_data[str(token.last_transaction_block)][list(token.pairs.keys())[0]]['price_change'],
                        }),
                        json.dumps(http_data),
                        time.time() - start_time_ex_bl
                ),))
                conn.commit()
            except:
                print(traceback.format_exc(), file=sys.stderr)
                conn.rollback()
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP insert #2 rollback(): {sys.exc_info()}", address)
                return
            

            cursor.close()



        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} END address={address}, pair={pair}", address)

        return
    




    # мониторинг созданных трейдов
    def watch_trades(self, pool, last_block, eth_price, start_time_ex_bl):

        global conn

        try:
            with conn.cursor() as cursor:
                pass
        except psycopg2.InterfaceError as e: # connection already closed
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {type(e)}: {e}", file=sys.stderr)
            conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)


        thread_id = f"[watch_trades() t{threading.get_native_id()}]"
        prefix = f"[l{last_block}] {thread_id}"

        trades_logger.info(f"{prefix} START")



        with conn.cursor(cursor_factory=RealDictCursor) as cursor:

            try:
                # https://dba.stackexchange.com/questions/69655/select-columns-inside-json-agg

                # array_agg(json_build_object('block',block,'value_usd_per_token_unit',value_usd_per_token_unit,'predict',predict,'predict_prob',predict_prob,'data',data,'http_data',http_data)) watch_rows
                # jsonb_agg('block',block,'value_usd_per_token_unit',value_usd_per_token_unit,'predict',predict,'predict_prob',predict_prob,'data',data,'http_data',http_data) watch_rows
                # cursor.execute(f"""
                #     SELECT
                #         w.id as watch_id,
                #         address,
                #         pair,
                #         block_first_mint,
                #         block_first_trade,
                #         verified_at_block,
                #         jsonb_agg('block',block,'value_usd_per_token_unit',value_usd_per_token_unit) watch_rows
                #     FROM watch w
                #     LEFT JOIN watch_predicts wp ON w.id = wp.watch_id
                #     WHERE status=1 and block_first_mint!={last_block}
                #     GROUP BY w.id
                # """)

                # (!) order by почему-то не работает
                # cursor.execute(f"""
                #     SELECT
                #         address,
                #         pair,
                #         token1_address,
                #         block_number,
                #         blocks_now_ft_diff,
                #         value,
                #         value_eth,
                #         value_usd,
                #         value_usd_per_token_unit,
                #         http_data,
                #         side
                #     FROM trades t
                #     GROUP BY t.address
                # """)
                # cursor.execute(f"""
                #     SELECT
                #         address,
                #         array_agg(json_build_object('block_number',block_number,'value',value,'value_usd',value_usd,'value_usd_per_token_unit',value_usd_per_token_unit,'side',side)) data
                #     FROM trades t
                #     GROUP BY t.address
                # """)
                cursor.execute(f"""
                    select
                        address,
                        sum(value) as sum_value,
                        json_agg(json_build_object(
                            'id', id,
                            'pair', pair,
                            'token1_address', token1_address,
                            'trans_block_number', trans_block_number,
                            'trans_value_usd_per_token_unit', trans_value_usd_per_token_unit,
                            'block_number', block_number,
                            'value', value,
                            'invested_usd', invested_usd,
                            'gas_usd', gas_usd,
                            'realized_profit', realized_profit,
                            'http_data', http_data,
                            'side', side,
                            'status', status,
                            'hash', hash
                        ) ORDER BY block_number ASC) as trades
                    from trades
                    where trans_block_number!={last_block}
                    group by address
                """)

                _addresses_sql = cursor.fetchall()
            except:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Exception select watch_trades() {sys.exc_info()}", file=sys.stderr)
                print(traceback.format_exc(), file=sys.stderr)
                conn.rollback()
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} STOP select rollback() on watch_trades(): {sys.exc_info()}")
                return

            msg = f"_addresses_sql:"
            threads_c = []
            for address_sql in _addresses_sql:
                msg += f"\n {address_sql}"
                thread = Thread(target=self.update_token, args=(address_sql, last_block, eth_price, start_time_ex_bl))
                thread.start()
                threads_c.append({'thread': thread, 'address': address_sql['address']})
            trades_logger.info(msg)


            # ожидание завершения всех потоков
            for th in threads_c:
                th['thread'].join()
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{prefix}] завершился thread_c [{th['thread'].native_id}] (всего {len(threads_c)}) для address={th['address']}", th['address'])




        trades_logger.info(f"{prefix} END")

        return




    def update_token(self, address_sql, last_block, eth_price, start_time_ex_bl):

        """
        :address_sql: = RealDictRow([('address', '0xd39cb6aa6aa345c719b518d8efe2a92807f49cc1'),
             ('sum_value', None),
             ('trades',
              [{'block_number': None,
                'gas_usd': None,
                'hash': '0x8b59099842b0721fc35cdeaff1daf8befe8a03ad43e18aa828fdf11e00226c7f',
                'http_data': {'gopluslabs': {'danger': [],
                                             'info': [],
                                             'scam': False,
                                             'warning': []},
                              'honeypotis': {'danger': [],
                                             'info': [],
                                             'scam': False,
                                             'warning': ['sellTax=4.186017214284029<=30']}},
                'id': 2,
                'invested_usd': None,
                'pair': '0xc4eddb3949e14255baeb9b3f66c38495c8a73ecd',
                'realized_profit': None,
                'side': 'b',
                'status': None,
                'token1_address': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                'trans_block_number': 20121844,
                'trans_value_usd_per_token_unit': 1.1466452838214847e-06,
                'value': None}])])
        """

        global conn

        address = address_sql['address']
        pair = address_sql['trades'][0]['pair']

        thread_id = f"[update_token() t{threading.get_native_id()}]"
        prefix = f"[l{last_block}] {thread_id}"


        # берем последний трейд
        # делаем проверку прошла ли транзакция в блокчейне
        if address_sql['trades'][-1]['status'] == None:
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} status=None, сделаем проверку прошла ли транзакция в блокчейне", address)

            if len(address_sql['trades']) > 1 and address_sql['trades'][-2]['status'] == None:
                assert 0, f"address={address}, почему-то status предпоследнего трейда равен None"

            trade = address_sql['trades'][-1]

            w3 = Web3(Web3.HTTPProvider(f"{self.testnet}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
            if w3.eth.block_number - trade['trans_block_number'] < 50:
                testnet = f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"
            else:
                testnet = f"http://127.0.0.1:8545/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"


            # tx = requests.post(testnet, json={"method":"eth_getTransactionByHash","params":[trade['hash']],"id":1,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']

            tx_receipt = requests.post(testnet, json={"method":"eth_getTransactionReceipt","params":[trade['hash']],"id":1,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']

            assert tx_receipt['from'] == utils.ACCOUNT_ADDRESS.lower(), tx_receipt



            self.check_connection(msg="update_token() #1")


            with conn.cursor(cursor_factory=RealDictCursor) as cursor:

                
                if int(tx_receipt['status'], 16) == 0:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} транзакция {trade['hash']} не прошла, ставим status=0 в бд", address)
                    try:
                        cursor.execute(f"""
                            UPDATE trades
                            SET
                                status=0
                            WHERE id={trade['id']}
                        """)
                        conn.commit()
                    except:
                        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} EXCEPTION UPDATE trades SET status=0 rollback(): {sys.exc_info()}", file=sys.stderr)
                        print(traceback.format_exc(), file=sys.stderr)
                        conn.rollback()
                        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} EXCEPTION UPDATE trades SET status=0 rollback(): {sys.exc_info()}", address)
                    return

                start_time = time.time()
                try:
                    cursor.execute("""SELECT
                            address,
                            from_ as from,
                            to_ as to,
                            value,
                            value_usd,
                            value_usd_per_token_unit,
                            extra_data,
                            category,
                            status
                        FROM _transfers WHERE transactionHash=%s
                    """, (trade['hash'],))
                    _transfers = cursor.fetchall()
                except:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} EXCEPTION SELECT * FROM _transfers WHERE rollback(): {sys.exc_info()}", address)
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} EXCEPTION SELECT * FROM _transfers WHERE rollback(): {sys.exc_info()}", file=sys.stderr)
                    print(traceback.format_exc(), file=sys.stderr)
                    conn.rollback()
                    return
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {(time.time() - start_time):.3f}s SELECT .. FROM _transfers", address)
            

                msg = f"transfers:"
                for trf in _transfers:
                    msg += f"\n {trf}"
                utils.save_log(f"{msg}", address)

                how_much_usd_was_spent = {} # здесь заложено сколько $ было потрачено
                how_much_usd_was_gained = {} # здесь заложено сколько $ было получено
                _value_native_token = 0 # сколько токенов было куплено

                for i, trf in enumerate(_transfers):

                    if _transfers[i]['address'] != None:
                        _transfers[i]['address'] = to_checksum_address(_transfers[i]['address'])
                    if _transfers[i]['from'] != None:
                        _transfers[i]['from'] = to_checksum_address(_transfers[i]['from'])
                    if _transfers[i]['to'] != None:
                        _transfers[i]['to'] = to_checksum_address(_transfers[i]['to'])
                    trf = _transfers[i]


                    if trf['from'] and trf['from'] not in how_much_usd_was_spent: how_much_usd_was_spent[trf['from']] = 0
                    if trf['to'] and trf['to'] not in how_much_usd_was_gained: how_much_usd_was_gained[trf['to']] = 0

                    if trf['address'] == None: # Wei
                        if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                        if trf['to']: how_much_usd_was_gained[trf['to']] += trf['value_usd']
                    

                    elif trf['address'] in [utils.WETH, utils.USDC, utils.USDT]:

                        if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                        if trf['to']: how_much_usd_was_gained[trf['to']] += trf['value_usd']


                    else:  
                        # не делаем подсчета других токенов, а именно покупаемого токена (для подсчета invested_usd)
                        # if trf['value_usd_per_token_unit'] != None:
                        #     if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                        #     if trf['to']: how_much_usd_was_gained[trf['to']] += trf['value_usd']

                        if trf['address'].lower() == address_sql['address'] and trf['to'] == utils.ACCOUNT_ADDRESS:
                            _value_native_token += trf['value']


                msg = 'how_much_usd_was_spent $:'
                for key, value in how_much_usd_was_spent.items():
                    msg += f"\n {key}: {value}"
                msg += '\nhow_much_usd_was_gained $:'
                for key, value in how_much_usd_was_gained.items():
                    msg += f"\n {key}: {value}"

                # abi = [
                #     # decimals
                #     json.loads('{"constant": true, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": false, "stateMutability": "view", "type": "function"}'),
                # ]
                # _co = w3.eth.contract(address=to_checksum_address(address_sql["address"]), abi=abi)
                # contract_data = utils.get_contract_data(w3, _co, block=int(tx_receipt['blockNumber'], 16))
                # msg += f"\n contract_data: {contract_data}"

                value_usd_per_token_unit = float(how_much_usd_was_spent[utils.ACCOUNT_ADDRESS] / _value_native_token * 10 ** token.contract_data_token['token_decimals'])
                gas_usd = int(tx_receipt['gasUsed'], 16) * int(tx_receipt['effectiveGasPrice'], 16) / eth_price
                
                msg += f"\n value tokens: {_value_native_token}"
                msg += f"\n invested_usd: {how_much_usd_was_spent[utils.ACCOUNT_ADDRESS]:.2f} $"
                msg += f"\n gas_usd: {round(gas_usd, 2)} $"
                msg += f"\n block: {int(tx_receipt['blockNumber'], 16)} (trans_block_number={address_sql['trades'][-1]['trans_block_number']})"
                msg += f"\n {address_sql['trades'][-1]['trans_value_usd_per_token_unit']} - trans_value_usd_per_token_unit"
                msg += f"\n {value_usd_per_token_unit} - value_usd_per_token_unit (diff {round(100 * value_usd_per_token_unit / address_sql['trades'][-1]['trans_value_usd_per_token_unit'] - 100, 2)}%)"

                utils.save_log(f"{msg}", address)

                try:
                    cursor.execute("""UPDATE trades
                        SET
                            status=1,
                            block_number=%s,
                            value=%s,
                            invested_usd=%s,
                            gas_usd=%s
                        WHERE id=%s
                    """, (int(tx_receipt['blockNumber'], 16), _value_native_token * 10 ** contract_data['token_decimals'], how_much_usd_was_spent[utils.ACCOUNT_ADDRESS], gas_usd, trade['id'],))
                    conn.commit()
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Обновили данные транзакции {trade['hash']} в бд", address)
                except:
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} EXCEPTION UPDATE trades SET status=1, ... rollback(): {sys.exc_info()}", file=sys.stderr)
                    print(traceback.format_exc(), file=sys.stderr)
                    conn.rollback()
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} EXCEPTION UPDATE trades SET status=1, ... rollback(): {sys.exc_info()}", address)
                


        # if address_sql['sum_value'] == 100: # (!) тут поменять на 0
        #     utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Для address={address} sum_value=0, поэтому не будет подсчетов", address)
        #     return

        start_time = time.time()
        try:
            token = ml.get_token_stat_for_ml(ia=None, address=address, pair=pair, last_block=last_block, realtime=True)
            if token != None and not(type(token) == dict and 'error' in token):
                if token.last_transaction_block != last_block:
                    utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Похоже нет транзакций на last_block={last_block}, token.last_transaction_block={token.last_transaction_block} для address={address}", address)
        except Exception as e:
            token = None
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} token Exception: {e}", address)
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} token Exception: {e}, address={address}", file=sys.stderr)
            print(traceback.format_exc())
            print(traceback.format_exc(), file=sys.stderr)
            return
        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {(time.time() - start_time):.3f}s total ml.get_token_stat_for_ml() для address={address}", address)

        # не обязательная проверка на sorted()
        assert address_sql['trades'] == sorted(address_sql['trades'], key=lambda d: d['block_number']), f"address={address}, {address_sql['trades']}"



        # utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} value_usd_per_token_unit во время покупки {address_sql['trades'][0]['value_usd_per_token_unit']}", address)

        msg = f"{prefix} Значения blocks_data на последних блоках:"
        ii = None
        # for block, data in token.blocks_data.items():
        for block in list(token.blocks_data.keys())[-15:]:
            data = token.blocks_data[str(block)]
            if list(token.pairs.keys())[0] in data:
                if ii == None:
                    ii = 1
                _pair_d = data[list(token.pairs.keys())[0]]
                msg += f"\n #{ii} block={block}, price_change={round(_pair_d['price_change'])}%, traded_volume_buy={round(data['traded_volume_buy'])}$, traded_volume_sell={round(data['traded_volume_sell'])}$, trades_buy={data['trades_buy']}, trades_sell={data['trades_sell']}, transactions_count={data['transactions_count']}, prediction={(str(data['prediction'][0]) + ' ' + str(round(100*data['prediction'][1]))+'%') if 'prediction' in data and data['prediction'] != None else '?'}, value_usd_per_token_unit={_pair_d['value_usd_per_token_unit']}"
            if ii != None:
                ii += 1
        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {msg}", address)

        # (!) в реале здесь будет суммирование по всем invested_usd значениям
        msg = f"\n sum_value: {address_sql['sum_value']}"
        value_usd = float(address_sql['sum_value']) * token.value_usd_per_token_unit / 10 ** token.contract_data_token['token_decimals'] / 10 ** token.contract_data_token['token_decimals']
        msg += f"\n value_usd: {value_usd:.2f} $"
        profit = 100 * value_usd / address_sql['trades'][0]['invested_usd'] - 100
        msg += f"\n profit: {profit:.2f}%"

        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} {msg}", address)


        if profit > -100:

            pass





        return


    
    def enter_trade(self, w3, token, last_block, eth_price, start_time_ex_bl):

        thread_id = f"[enter_trade() t{threading.get_native_id()}]"
        prefix = f"[l{last_block}] {thread_id}"

        address = token.address
        account = Account.from_key(utils.ACCOUNT_KEY)


        # print(f"{w3.eth.gas_price} - gas price")  # кол-во Wei за единицу газа
        # print(f"{w3.eth.max_priority_fee} - max_priority_fee")
        # print(f"{w3.eth.get_block('latest')['baseFeePerGas']} - baseFeePerGas")
        # print(f"{w3.eth.get_block('latest')['baseFeePerGas'] / 10**9} - baseFeePerGas in gwei")


        if token.pairs[list(token.pairs.keys())[0]]['exchange'] != 'V2':
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} exchange={token.pairs[list(token.pairs.keys())[0]]['exchange']} != V2, address={address}", address)
            return False
        if token.pairs[list(token.pairs.keys())[0]]['token1_address'] != utils.WETH:
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} token1_address={token.pairs[list(token.pairs.keys())[0]]['token1_address']} != utils.WETH, address={address}", address)
            return False
            


        router_contract = w3.eth.contract(address=utils.UNISWAP_V2_ROUTER, abi=utils.get_abi(utils.UNISWAP_V2_ROUTER))

        buy_path = [utils.WETH, address]


        amount_to_buy_for = int(0.001 * 10**18) # ETH

        try:
            weth_token_amount, native_token_amount = router_contract.functions.getAmountsOut(amount_to_buy_for, buy_path).call()
            # print(f"for {weth_token_amount/10**18} WETH you would get {native_token_amount/10**9} ELON token")
            if native_token_amount <= 0:
                raise Exception(f'native_token_amount={native_token_amount}')
        except Exception as e:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Exception getAmountsOut() {e}, address={address}")
            print(traceback.format_exc(), file=sys.stderr)
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Exception getAmountsOut() {e}, address={address}", address)
            return False

        
        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} weth_token_amount={weth_token_amount}, native_token_amount={native_token_amount}", address)

        # uni_token_to_buy = 500 * 10**9
        # weth_token_amount, native_token_amount = router_contract.functions.getAmountsIn(uni_token_to_buy, buy_path).call()
        # print(f"for {uni_token_to_buy/10**9} ELON token you have to pay {weth_token_amount/10**18} WETH")



        base_fee_per_gas = w3.eth.gas_price   # baseFeePerGas in the latest block (in wei)
        max_priority_fee_per_gas = w3.to_wei(1, 'gwei') # Priority fee to include the transaction in the block
        max_fee_per_gas = base_fee_per_gas + int(0.06 * base_fee_per_gas) + max_priority_fee_per_gas // 100 # Maximum amount you’re willing to pay


        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {base_fee_per_gas}=base_fee_per_gas", address)
        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {max_fee_per_gas}=max_fee_per_gas", address)
        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {amount_to_buy_for}=amount_to_buy_for", address)
        utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {utils.get_balance(w3, address=utils.ACCOUNT_ADDRESS, block_identifier=w3.eth.block_number)}=balance()", address)

        buy_tx_params = {
            "nonce": w3.eth.get_transaction_count(account.address),
            "from": account.address,
            "chainId": 1,
            # 'gasPrice': w3.eth.gas_price,
            "gas": 500_000,
            # 'gas': 21000,  # Gas limit for the transaction
            #"maxPriorityFeePerGas": w3.eth.max_priority_fee,
            # "maxFeePerGas": 100 * 10**9,
            # "maxPriorityFeePerGas": max_priority_fee_per_gas,
            # "maxFeePerGas": max_fee_per_gas,
            "value": amount_to_buy_for,    
        }

        try:
            buy_tx = router_contract.functions.swapExactETHForTokens(
                0, # min amount out
                buy_path,
                account.address,
                int(time.time())+10 # deadline now + seconds
            ).build_transaction(buy_tx_params)

            signed_buy_tx = w3.eth.account.sign_transaction(buy_tx, account.key)

            # pprint(signed_buy_tx)
            # pprint(signed_buy_tx.raw_transaction)

            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} w3.eth.block_number={w3.eth.block_number}, last_block={last_block}, start_time_ex_bl={(time.time() - start_time_ex_bl):.3f}s", address)

            tx_hash = w3.eth.send_raw_transaction(signed_buy_tx.raw_transaction)
            tx_hash = Web3.to_hex(tx_hash)
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} transaction sent! tx_hash={tx_hash}", address)
        
        except:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Exception send transaction {sys.exc_info()}, address={address}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {prefix} Exception send transaction {sys.exc_info()}, address={address}", address)
            return False


        self.flag_no_buy_anymore = True

        return tx_hash




    def sync_transaction(self, p_i, tx, tx_receipt, tx_internal, tx_eth_price, block_data):

        assert tx['hash'] == tx_receipt['transactionHash'] == tx_internal['txHash']

        w3 = Web3(Web3.HTTPProvider(f"{self.testnet}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))


        start_time_t = time.time()

        if self.verbose:
            print('.'*90)

            print('tx:')
            pprint(tx)
            print('tx_receipt:')
            aaa = copy.deepcopy(tx_receipt)
            #del aaa['logs']
            pprint(aaa)
            # print('tx_internal:')
            # pprint(tx_internal)

            print('tx hash:', tx['hash'])
        
        # print('tx:')
        # pprint(tx)
        # print('tx_receipt:')
        # aaa = copy.deepcopy(tx_receipt)
        # del aaa['logs']
        # pprint(aaa)
        # # print('tx_internal:')
        # # pprint(tx_internal)
        # assert 0

        tx['from'] = to_checksum_address(tx['from'])
        tx['blockNumber'] = int(tx['blockNumber'], 16)
        tx['value'] = int(tx['value'], 16)
        tx['gas'] = int(tx['gas'], 16)
        tx['gasPrice'] = int(tx['gasPrice'], 16)
        tx['maxFeePerGas'] = int(tx['maxFeePerGas'], 16) if 'maxFeePerGas' in tx else None
        tx['maxPriorityFeePerGas'] = int(tx['maxPriorityFeePerGas'], 16) if 'maxPriorityFeePerGas' in tx else None
        tx_receipt['gasUsed'] = int(tx_receipt['gasUsed'], 16)
        tx_receipt['effectiveGasPrice'] = int(tx_receipt['effectiveGasPrice'], 16)
        tx_receipt['cumulativeGasUsed'] = int(tx_receipt['cumulativeGasUsed'], 16)
        tx_receipt['status'] = int(tx_receipt['status'], 16)

        state_change = utils.get_state_change(testnet=self.testnet, tx=tx, miner=block_data['miner'], method='trace_replayTransaction') # trace_replayTransaction|debug_traceCall
        bribe_eth = utils.calc_bribe(state_diff=state_change['post'] - state_change['pre'], gasUsed=tx_receipt['gasUsed'], maxPriorityFeePerGas=tx['maxPriorityFeePerGas']) if state_change != None else None
        # print(f"block={tx['blockNumber']}, tx={tx['hash']}, bribe_eth={bribe_eth}")

        _db_transaction = {
            # id bigserial primary key,
            'blockHash': tx['blockHash'],
            'blockNumber': tx['blockNumber'],
            'chainId': int(tx['chainId'], 16) if 'chainId' in tx else None,
            'from_': tx['from'].lower(),
            'gas': tx['gas'],
            'gasPrice': tx['gasPrice'],
            'hash': tx['hash'],
            'input': tx['input'],
            'maxFeePerGas': tx['maxFeePerGas'],
            'maxPriorityFeePerGas': tx['maxPriorityFeePerGas'],
            'nonce': int(tx['nonce'], 16),
            'r': tx['r'],
            's': tx['s'],
            'to_': tx['to'].lower() if tx['to'] else None,
            'transactionIndex': int(tx['transactionIndex'], 16),
            'type': int(tx['type'], 16),
            'v': int(tx['v'], 16),
            'value': tx['value'],
            'yParity': int(tx['yParity'], 16) if 'yParity' in tx else 0,
            # executed_in numeric(8, 3) not null
            # -- из tx_receipt
            'contractAddress': tx_receipt['contractAddress'],
            'cumulativeGasUsed': tx_receipt['cumulativeGasUsed'],
            'gasUsed': tx_receipt['gasUsed'],
            'status': tx_receipt['status'],
            # -- из block
            'timestamp': block_data['timestamp'],
            'bribe_eth': bribe_eth,
            # 'block_baseFeePerGas': block_data['baseFeePerGas'],
            # 'block_gasUsed': block_data['gasUsed'],
            # 'block_gasLimit': block_data['gasLimit']
        }


        if tx['to'] == None:
            tx['to'] = tx_receipt['contractAddress']
        tx['to'] = to_checksum_address(tx['to'])


        _transfers_template = {
            # transaction_id bigint not null,
            'transactionHash': tx['hash'],
            'transactionIndex': int(tx['transactionIndex'], 16),
            'blockNumber': tx['blockNumber'],
            'timestamp': block_data['timestamp'],
            # address varchar(42),
            # from_ varchar(42),
            # to_ varchar(42),
            'value': None,
            'value_usd': None,
            'value_usd_per_token_unit': None,
            # category varchar(50),
            'status': None,
            'extra_data': None,
            'logIndex': None,
            'pair_address': None,
            'token1_currency': None,
            'exchange': None,
            'total_supply': None,
            'reserves_token': None,
            'reserves_token_usd': None,
            'reserves_pair': None,
            'reserves_pair_usd': None,
            'token_symbol': None,
            'token_decimals': None,
        }


        transaction_transfers = []
        b_transaction = {}


        # if tx['value'] != 0 and tx_receipt['status'] != 0:
        transaction_transfers.append(_transfers_template | {
            'address': None,
            'from_': tx['from'].lower(),
            'to_': tx['to'].lower(),
            'value': tx['value'],
            'value_usd': tx['value'] / tx_eth_price,
            'value_usd_per_token_unit': 1 / tx_eth_price,
            'status': tx_receipt['status'],
            'category': 'external',
        })
        if self.verbose:
            print(f"      -> Internal {tx['value']} Wei ({from_wei(tx['value'], 'ether')} ETH, {round(tx['value'] / tx_eth_price, 2)} $)")


        if self.verbose:
            print('len logs:', len(tx_receipt['logs']))

        start_time_logs = time.time()

        _db_logs = []
        minted_pair = None

        if len(tx_receipt['logs']):


            # (!) это надо бы добавить
            # contract = w3.eth.contract(address=tx["to"], abi=utils.BASIC_ERC20_ABI)
            # func_obj, func_params = contract.decode_function_input(tx["input"])




            # log_errors = 0

            saved_value_usd_per_token_unit = {}


            if len(tx_receipt['logs']) > 3000:
                tx_receipt['logs'] = tx_receipt['logs'][:1500] + tx_receipt['logs'][-1500:]
                # assert 0, f"logs len={len(tx_receipt['logs'])}, {tx['hash']}"


            _contracts = {}

            
            addresses_for_abi = [utils.UNI_V2_FACTORY, utils.UNI_V3_FACTORY, utils.WETH, utils.USDT, utils.USDC]
            for _, item in FILE_LABELS['ethereum'].items():
                addresses_for_abi += item.keys()


            pair_addresses = {} # key - это адрес пары
            contracts_pairs = {} # key - это адрес контракта
            # заранее находим пару через Swap для usd_per_one_token()
            for log in tx_receipt['logs'][:20] + tx_receipt['logs'][-20:]:

                # Swap V2
                # 0x75fd71e367ed33e2af03e973576560e5a1551c2955adc5e109627b8c6af8faf5
                # Swap V3
                # 0x2925108ffe69b92b6a071bf6ed3eb7f3bbfbb5980c6e594d35750c6e6fccafa9

                log["address"] = to_checksum_address(log["address"])

                # ищем в log['topics'][0] Swap V2 или Swap V3
                if log["address"] not in pair_addresses \
                    and log['topics'] and log['topics'][0] in ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822', '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67']:
                    
                    pair_addresses[log["address"]] = None

                    # token0, token1
                    abi = json.loads('[{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]')

                    pair_contract = w3.eth.contract(address=log["address"], abi=abi)
                    try:
                        token0 = pair_contract.functions.token0().call(block_identifier=tx['blockNumber'])
                        token1 = pair_contract.functions.token1().call(block_identifier=tx['blockNumber'])
                    except ContractLogicError as e:
                        if 'no data' in str(e) or 'execution reverted: Diamond: Function does not exist' in str(e):
                            continue
                        else:
                            print(traceback.format_exc(), file=sys.stderr)
                            raise


                    if token0 in [utils.WETH, utils.USDC, utils.USDT] and token1 in [utils.WETH, utils.USDC, utils.USDT]:
                        # 0xa7dff5fd56ab1245624621f2cb1041013865bc98bafb6d73ebf14f5f85668961
                        pass
                    else:
                        # могут встречаться другие связки, пример:
                        # hash=0x297bf9e67127a1a38e72003cd8f98e9db1b58006d736ef073d8a798b7ab1fdeb, token0=0x423f4e6138E475D85CF7Ea071AC92097Ed631eea, token1=0xb9f599ce614Feb2e1BBe58F180F370D05b39344E
                        # assert token0 in [utils.WETH, utils.USDC, utils.USDT] or token1 in [utils.WETH, utils.USDC, utils.USDT], f"hash={tx['hash']}, token0={token0}, token1={token1}"
                        if token0 in [utils.WETH, utils.USDC, utils.USDT] or token1 in [utils.WETH, utils.USDC, utils.USDT]:
                            if log['topics'][0] == '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822': _exchange = 'V2'
                            if log['topics'][0] == '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67': _exchange = 'V3'
                            if token0 in [utils.WETH, utils.USDC, utils.USDT]:
                                if token0 == utils.WETH: token1_currency = 'WETH'
                                if token0 == utils.USDC: token1_currency = 'USDC'
                                if token0 == utils.USDT: token1_currency = 'USDT'
                                contracts_pairs[token1] = {'pair_address': log["address"], 'token1_currency': token1_currency, 'exchange': _exchange}
                                if self.verbose:
                                    print(f'Для пары {log["address"]} нашли контракт {token1} на {_exchange}')
                            if token1 in [utils.WETH, utils.USDC, utils.USDT]:
                                if token1 == utils.WETH: token1_currency = 'WETH'
                                if token1 == utils.USDC: token1_currency = 'USDC'
                                if token1 == utils.USDT: token1_currency = 'USDT'
                                contracts_pairs[token0] = {'pair_address': log["address"], 'token1_currency': token1_currency, 'exchange': _exchange}
                                if self.verbose:
                                    print(f'Для пары {log["address"]} нашли контракт {token0} на {_exchange}')


            for ii, log in enumerate(tx_receipt['logs']):


                if not log['topics']:
                    continue


                assert len(log['topics']) <= 4, f"hash={tx['hash']}, len={len(log['topics'])}"

                assert len(log['topics'][0]) == 66, f"hash={tx['hash']}, len={len(log['topics'][0])}, log['topics'][0]={log['topics'][0]}"


                # https://github.com/tradingstrategy-ai/web3-ethereum-defi/blob/3148126167e7452fc3769b55d82fbc8987281602/eth_defi/event_reader/reader.py#L362
                # кажется здесь есть правильный эквивалент этой функции
                # convert_uint256_hex_string_to_address()
                def convert_hex(string):
                    if string.startswith('0x'):
                        if len(string) <= 42: return string.lower()
                        if len(string) == 66:
                            # print(f" \x1b[31;1m(!)\x1b[0m в значении topics лишние нули: {string}, len={len(string)}, hash={tx['hash']}")
                            # return hex(int(string, 16))
                            string = string.replace('0x000000000000000000000000', '0x')
                            return string.lower()
                        else:
                            return string.lower()
                    
                    return string


                topics_0 = log['topics'][0]
                topics_1 = convert_hex(log['topics'][1]) if len(log['topics']) >= 2 else None
                topics_2 = convert_hex(log['topics'][2]) if len(log['topics']) >= 3 else None
                topics_3 = convert_hex(log['topics'][3]) if len(log['topics']) >= 4 else None

                _db_logs.append({
                    # transaction_id
                    'transactionHash': tx['hash'],
                    'transactionIndex': int(tx['transactionIndex'], 16),
                    'blockNumber': tx['blockNumber'],
                    'timestamp': block_data['timestamp'],
                    'address': log['address'].lower(),
                    'topics_0': topics_0,
                    'topics_1': topics_1,
                    'topics_2': topics_2,
                    'topics_3': topics_3,
                    'pair_created': None,
                    'data': log['data'],
                    'decoded_data': None,
                    'logIndex': int(log['logIndex'], 16),
                })





                log["address"] = to_checksum_address(log["address"])
                # if log_errors >= 300:
                #     print(f"aborted due to {log_errors} log errors")
                #     assert 0, f"aborted due to {log_errors} log errors"
                #     break




                # TransferSingle
                # 0x6040430c5d50dadf9bebbc570df92051218a152bab6e906b5d664bfe6535cefb
                # 0x214eeed820d7bf45ab4cb53e60a1d7d4ba1ca2b67a4599658930f2832688ece8
                # 0x6c973a9cdba7dc064794f03bea06d77346288c169bf16c9c7788b4cfa431b9a3
                # 0x43c83045538209cb1b593ac608e0a10b6b0f987da03286db860b73538262c813
                if log['topics'][0] == '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62':

                    transaction_transfers.append(_transfers_template | {
                        'address': log['address'].lower(),
                        'from_': topics_2,
                        'to_': topics_3,
                        # 'v': log['topics'][3], # id
                        'category': 'erc1155',
                    })
                    continue


                # TransferBatch
                # 0xc64601979aff26c939d4ba21608ee2b36c070fac0b6514305a885b87522c64dc
                if log['topics'][0] == '0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb':

                    transaction_transfers.append(_transfers_template | {
                        'address': log['address'].lower(),
                        'from_': topics_2,
                        'to_': topics_3,
                        # 'v': log['topics'][3], # id
                        'category': 'erc1155',
                    })
                    continue
                

                # Transfer (erc721)
                # 0x214eeed820d7bf45ab4cb53e60a1d7d4ba1ca2b67a4599658930f2832688ece8
                # 0xcfacb0793f9d7bfbf5377eae6ddabb5529820b81b485a475e7e3d188096a3a50
                # 0xdc48c6088775c67766399412d675ae137ce0314f4950198e2d02502ab1629495
                # (!) пример erc721, но data != "0x" 0x40ed5993fa9de6694e824f370a1dbbef68dbb1ecf440dffc5f074cba48bf04ff
                if log['data'] in ["0x", HexBytes("0x"), 0] and topics_0 in ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0xe19260aff97b920c7df27010903aeb9c8d2be5d310a2c67824cf3f15396e4c16']:

                    transaction_transfers.append(_transfers_template | {
                        'address': log['address'].lower(),
                        'from_': topics_1, # или decoded_data['from']
                        'to_': topics_2, # decoded_data['to']
                        'value': int(topics_3, 16), # decoded_data['id/tokenId/..']
                        'category': 'erc721',
                    })
                    continue

                    
                abi = None
                if log["address"] in addresses_for_abi:
                    # (!) здесь get_abi стоит переместить в начало init класса, так как постоянно одни и теже значения
                    abi = utils.get_abi(log["address"])
                    # assert 'error' not in abi, f"abi={abi}, address={log['address']}"
                if abi == None or 'error' in abi:
                    abi = [
                        # name
                        json.loads('{"constant": true, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "payable": false, "stateMutability": "view", "type": "function"}'),
                        # symbol
                        json.loads('{"constant": true, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "payable": false, "stateMutability": "view", "type": "function"}'),
                        # decimals
                        json.loads('{"constant": true, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": false, "stateMutability": "view", "type": "function"}'),
                        # Transfer(address,address,uint256) 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
                        json.loads('{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}'),
                        # Transfer(address,address,uint256,bytes) 0xe19260aff97b920c7df27010903aeb9c8d2be5d310a2c67824cf3f15396e4c16
                        json.loads('{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"},{"indexed":false,"name":"data","type":"bytes"}],"name":"Transfer","type":"event"}'),
                        # Mint(address,uint256,uint256) V2
                        json.loads('{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"}'),
                        # Mint(address,address,int24,int24,uint128,uint256,uint256) V3
                        json.loads('{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"}'),
                    ]


                if log['address'] not in _contracts:
                    _co = w3.eth.contract(address=log["address"], abi=abi)
                    _contracts[log['address']] = {
                        'contract': _co,
                        'data': utils.get_contract_data(w3, _co, block=tx['blockNumber'])
                    }

                contract = _contracts[log['address']]['contract']
                contract_data = _contracts[log['address']]['data']

                if self.verbose:
                    print(f" \x1b[34;1m [{ii + 1}]\x1b[0m address={log['address']}: {contract_data}")



                def _make_decode():
                    start_time = time.time()
                    decoded_data, evt_name, logs_dlc = utils.decode_log_contract(w3, contract, abi, log)
                    # print(f" decode_log_contract() {(time.time() - start_time):.4f} sec")
                    if self.verbose:
                        for lo in logs_dlc:
                            print(lo)
                    if self.verbose:
                        print(f"      \x1b[34;1m{evt_name}\x1b[0m: {decoded_data}")
                    return decoded_data, evt_name


                decoded_data, evt_name = None, None




                if log['address'] == utils.WETH:

                    if topics_0 in ['0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c', '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65']:
                        decoded_data, evt_name = _make_decode()
                        _db_logs[-1]['decoded_data'] = decoded_data

                        value = decoded_data['wad']
                        value_usd = decoded_data['wad'] / tx_eth_price
                        # value_usd_per_token_unit = 1 / tx_eth_price * 10 ** contract_data['token_decimals']
                        value_usd_per_token_unit = 1 / tx_eth_price

                        # Deposit
                        if topics_0 == '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c':
                            transaction_transfers.append(_transfers_template | {
                                'address': log['address'].lower(),
                                'from_': None,
                                'to_': decoded_data['dst'].lower(),
                                'value': value,
                                'value_usd': value_usd,
                                'value_usd_per_token_unit': value_usd_per_token_unit,
                                'category': 'erc20',
                            })
                            if self.verbose:
                                print(f"      -> {round(value / 10 ** contract_data['token_decimals'], 3)} {contract_data['token_symbol']} ({value}), {value_usd:.2f} $")
                        
                        # Withdrawal
                        if topics_0 == '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65':
                            transaction_transfers.append(_transfers_template | {
                                'address': log['address'].lower(),
                                'from_': decoded_data['src'].lower(),
                                'to_': None,
                                'value': value,
                                'value_usd': value_usd,
                                'value_usd_per_token_unit': value_usd_per_token_unit,
                                'category': 'erc20',
                            })
                            if self.verbose:
                                print(f"      -> {round(value / 10 ** contract_data['token_decimals'], 3)} {contract_data['token_symbol']} ({value}), {value_usd:.2f} $")



                if log['address'] in [utils.USDC, utils.USDT] and evt_name in ['Deposit', 'Withdrawal']:
                    decoded_data, evt_name = _make_decode()
                    assert 0, f"{evt_name} in {log['address']}"




                # Transfer
                if topics_0 in ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0xe19260aff97b920c7df27010903aeb9c8d2be5d310a2c67824cf3f15396e4c16']:
                    
                    decoded_data, evt_name = _make_decode()
                    _db_logs[-1]['decoded_data'] = decoded_data

                    value_usd_per_token_unit = None
                    reserves_token_usd = None
                    reserves_pair_usd = None
                
                    value = None
                    if 'value' in decoded_data: value = decoded_data['value']
                    if 'tokens' in decoded_data:
                        assert value == None
                        value = decoded_data['tokens']
                    if 'amount' in decoded_data:
                        assert value == None
                        value = decoded_data['amount']
                    if 'wad' in decoded_data:
                        assert value == None
                        value = decoded_data['wad']
                    if 'amountOrId' in decoded_data:
                        assert value == None
                        value = decoded_data['amountOrId']
                    if 'valueOrId' in decoded_data:
                        assert value == None
                        value = decoded_data['valueOrId']
                    if 'tokenId' in decoded_data:
                        value = decoded_data['tokenId']
                    if 'id' in decoded_data:
                        value = decoded_data['id']
                    if '' in decoded_data:
                        assert value == None
                        value = decoded_data['']
                    #assert value != None, decoded_data
                    if value == None:
                        if self.verbose:
                            print(f" \x1b[31;1m(!)\x1b[0m value == None, {decoded_data}")
                        # pprint(tx_receipt)
                        if len(log['topics']) == 3:
                            # assert 0, f"hash={tx['hash']}, value == None, logIndex={int(log['logIndex'], 16)}, decoded_data={decoded_data}"
                            print(f" \x1b[31;1m(!)\x1b[0m value == None, {decoded_data}, topics len={len(log['topics'])}, hash={tx['hash']}")
                            pass
                        else:
                            print(f" \x1b[31;1m(!)\x1b[0m value == None, {decoded_data}, topics len={len(log['topics'])}, hash={tx['hash']}")
                            # здесь не ясно что делать 0x7b826d84f34595cc530a1616792b96118822677e363685d532708e42fb7072bf
                            # на log #2 топики без from и to
                        
                    
                    # print(type(value))
                    # if type(value) != int:
                    #     if '0x' in value:
                    #         print('value=', value)
                    #         value = hex(value)
                    #         print('after convert=', value)
                    #         assert 0
                    #     else:
                    #         assert 0, f"type(value)={type(value)}, value={value}, hash={tx['hash']}"
                    

                    if log["address"] == utils.WETH:
                        assert 'wad' in decoded_data, decoded_data

                        transaction_transfers.append(_transfers_template | {
                            'address': log['address'].lower(),
                            'from_': decoded_data['src'].lower(),
                            'to_': decoded_data['dst'].lower(),
                            'value': value,
                            'value_usd': value / tx_eth_price,
                            'value_usd_per_token_unit': 1 / tx_eth_price, # (!) тут надо проверить, может = 1 / tx_eth_price * 10 ** contract_data['token_decimals']
                            'category': 'erc20',
                        })

                        if self.verbose:
                            print(f"      -> {value} Wei ({from_wei(value, 'ether')} ETH, {round(value / tx_eth_price, 2)} $)")

                    elif log["address"] in [utils.USDC, utils.USDT]:

                        transaction_transfers.append(_transfers_template | {
                            'address': log['address'].lower(),
                            'from_': decoded_data['from'].lower(),
                            'to_': decoded_data['to'].lower(),
                            'value': value,
                            'value_usd': value / 10 ** contract_data['token_decimals'],
                            'value_usd_per_token_unit': 1 / 10 ** contract_data['token_decimals'], # (!) проверить
                            'category': 'erc20',
                        })

                        if self.verbose:
                            print(f"      -> {round(value / 10 ** contract_data['token_decimals'], 3)} {contract_data['token_symbol']} ({value}), {(value / 10 ** contract_data['token_decimals']):.2f} $")

                    else:

                        if log['address'] not in saved_value_usd_per_token_unit:

                            start_time = time.time()

                            # test_delete = ''
                            _params = {}
                            if log['address'] in contracts_pairs:
                                if self.verbose:
                                    print(f'Уже знаем, что надо искать для контракта {log["address"]} пару {contracts_pairs[log["address"]]}')
                                # test_delete += f'Уже знаем, что надо искать для контракта {log["address"]} пару {contracts_pairs[log["address"]]}\n'
                                _params = contracts_pairs[log["address"]]
                            
                            _kwards = utils.usd_per_one_token(w3=w3, address=log['address'], block=tx['blockNumber'], tx_eth_price=tx_eth_price, **_params)
                            value_usd_per_token_unit = _kwards['value_usd_per_token_unit']
                            reserves_token = _kwards['reserves_token']
                            reserves_token_usd = _kwards['reserves_token_usd']
                            reserves_pair = _kwards['reserves_pair']
                            reserves_pair_usd = _kwards['reserves_pair_usd']
                            pair_address = _kwards['pair_address']
                            token1_currency = _kwards['token1_currency']
                            exchange = _kwards['exchange']
                            # print('Подсчитали из utils.usd_per_one_token для', log['address'])
                            # print(' reserves_token:', reserves_token)
                            # print(' reserves_token_usd:', reserves_token_usd)
                            # print(' reserves_pair:', reserves_pair)
                            # print(' reserves_pair_usd:', reserves_pair_usd)
                            # print(' pair_address:', pair_address)
                            # print(' token1_currency:', token1_currency)
                            # print(' exchange:', exchange)

                            total_supply = utils.contract_totalSupply(w3=w3, address=log['address'], block_identifier=tx['blockNumber'])
                            _kwards['total_supply'] = total_supply

                            if self.verbose:
                                print(f"usd_per_one_token() {(time.time() - start_time):.4f} sec")

                            saved_value_usd_per_token_unit[log['address']] = _kwards


                            # if not self.realtime:
                            #     test_delete += f"_kwards['pair_address']: {_kwards['pair_address']}\n"
                            #     total_supply = utils.contract_totalSupply(w3=w3, address=log['address'], block_identifier=tx['blockNumber'])
                            #     total_supply_pair = utils.contract_totalSupply(w3=w3, address=_kwards['pair_address'], block_identifier=tx['blockNumber'])
                            #     test_delete += f"for tx={tx['hash']} from={tx['from']} address={log['address']} token={contract_data['token_name'] if 'token_name' in contract_data else contract_data} perc={round(value * 100 / total_supply, 4) if total_supply != None else None}% total_supply_pair={total_supply_pair} perc_to_pair={round(value * 100 / total_supply_pair, 4) if total_supply_pair != None else None}%\n"
                            #     test_delete += str(_kwards.keys()) + '\n'
                            #     test_delete += '---\n'
                            #     print(test_delete)


                        else:
                            value_usd_per_token_unit = saved_value_usd_per_token_unit[log['address']]['value_usd_per_token_unit']
                            reserves_token = saved_value_usd_per_token_unit[log['address']]['reserves_token']
                            reserves_token_usd = saved_value_usd_per_token_unit[log['address']]['reserves_token_usd']
                            reserves_pair = saved_value_usd_per_token_unit[log['address']]['reserves_pair']
                            reserves_pair_usd = saved_value_usd_per_token_unit[log['address']]['reserves_pair_usd']
                            pair_address = saved_value_usd_per_token_unit[log['address']]['pair_address']
                            token1_currency = saved_value_usd_per_token_unit[log['address']]['token1_currency']
                            exchange = saved_value_usd_per_token_unit[log['address']]['exchange']
                            total_supply = saved_value_usd_per_token_unit[log['address']]['total_supply']


                        _from = None
                        _to = None
                        if 'from' in decoded_data: _from = decoded_data['from']
                        if 'src' in decoded_data: _from = decoded_data['src']
                        if 'to' in decoded_data: _to = decoded_data['to']
                        if 'dst' in decoded_data: _to = decoded_data['dst']

                        if value != None: # из-за бага на log #2 0x7b826d84f34595cc530a1616792b96118822677e363685d532708e42fb7072bf
                            assert _from != None and _to != None, decoded_data



                        value_usd = None
                        if value_usd_per_token_unit == None:
                            if self.verbose:
                                print(f" \x1b[31;1m(!)\x1b[0m Не удалось получить usd для address={log['address']} в usd_per_one_token()")
                        elif value != None:
                            value_usd = value * value_usd_per_token_unit / 10 ** contract_data['token_decimals']
                            if self.verbose:
                                print(f"      -> Токен отправлен {round(value / 10 ** contract_data['token_decimals'], 3)} {contract_data['token_symbol']} ({value}) {value_usd:.2f} $")


                        transaction_transfers.append(_transfers_template | {
                            'address': log['address'].lower(),
                            'from_': _from.lower() if _from else None,
                            'to_': _to.lower() if _to else None,
                            'value': value,
                            'value_usd': value_usd,
                            'value_usd_per_token_unit': value_usd_per_token_unit / 10 ** contract_data['token_decimals'] if value_usd_per_token_unit else None,
                            'category': 'erc20',
                            'extra_data': {'rt': round(reserves_token_usd, 2), 'rp': round(reserves_pair_usd, 2)} if reserves_token_usd else None,
                            'pair_address': pair_address,
                            'token1_currency': token1_currency,
                            'exchange': exchange,
                            'total_supply': total_supply,
                            'reserves_token': reserves_token,
                            'reserves_token_usd': reserves_token_usd,
                            'reserves_pair': reserves_pair,
                            'reserves_pair_usd': reserves_pair_usd,
                            'token_symbol': contract_data['token_symbol'][:20] if 'token_symbol' in contract_data else None,
                            'token_decimals': contract_data['token_decimals'] if 'token_decimals' in contract_data else None,
                        })



                # PairCreated
                if log['address'] == utils.UNI_V2_FACTORY and topics_0 == '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9':

                    decoded_data, evt_name = _make_decode()
                    _db_logs[-1]['decoded_data'] = decoded_data
                    _db_logs[-1]['pair_created'] = decoded_data['pair'].lower()

                # PoolCreated
                if log['address'] == utils.UNI_V3_FACTORY and topics_0 == '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118':

                    decoded_data, evt_name = _make_decode()
                    _db_logs[-1]['decoded_data'] = decoded_data
                    _db_logs[-1]['pair_created'] = decoded_data['pool'].lower()
                

                # Mint V2
                if topics_0 == '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f':

                    # sender param
                    if topics_1 == utils.UNISWAP_V2_ROUTER.lower():
                        decoded_data, evt_name = _make_decode()
                        _db_logs[-1]['decoded_data'] = decoded_data
                        minted_pair = log['address'].lower()
                    else:
                        # AssertionError: Mint() V2 different sender, hash=0x7de7f5baf8df0ddec57b034a14491337960d3e88f931d51f4becb652fe3c6b5b, 0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d vs 0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D
                        # assert 0, f"Mint() V2 different sender, hash={tx['hash']}, {topics_1} vs {utils.UNISWAP_V2_ROUTER}"
                        pass
                    

                # Mint V3
                if topics_0 == '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde':

                    # owner param
                    if topics_1 == utils.UNISWAP_V3_POSITIONS_NFT.lower():
                        decoded_data, evt_name = _make_decode()
                        _db_logs[-1]['decoded_data'] = decoded_data
                        minted_pair = log['address'].lower()
                    else:
                        # assert 0, f"Mint() V3 different owner, hash={tx['hash']}"
                        pass

                
                # Burn
                # 0xb73b4f65cb300c7fa8b04662fec0e2d86609e4eef4b7afc328874a13e72139af
                if topics_0 == '0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5':

                    pass




                # (!) добавить поля тип транзакции send/receive/trade (и тип trade'a buy/sell)
                #  если send или recevied, то сколько еще было send или recevied в результате данной транзакции за эту одну транзакцию
                # добавить транзакция приватная или нет
                # добавить critical_errors в бд
                # gas_usd в _transaction
                # писать инфу про владение контрактов (spam/not spam токен), какой % владения от общего supply, contract verified или нет
                # транзакция снайп или нет, сколько времени прошло с момента создания токена
                # добавить ликвидити данного токена в момент транзакции (для calculate_wallets() в token_stat.py)
                # добавить сколько invested, withdrew
                # был ли titan builder в транзакции как bribe (https://etherscan.io/tx/0xd3f4d0b42c23166b392005bedb284e0ffc35ac1b2d05c00c9c7adabf603c0506)
                # token_decimals добавить в _transfers
                # в _transfers может быть добавить трансфер на gas значение
                # bribe в builder подсчитывать и подсчитывать % bribe относительно остальных транзакций в блоке (перепроверить результат в geth и erigon, в geth неверный выдает)
                # если транзакция trade, то добавить поле по какой pair был трейд
                # добавить инфу на какой exchange (V2/V3) был trade
                # external eth транзакции при value=0 не добавлять в бд
                # сохраняться в transfer название токена, symbol, token_decimals
                # в extra_data в transfers помимо usd значений сохранять еще значение в native token value
                # logIndex проверить у _transfers, выдавало None
                # initial_price во время Mint() пары сохранять куда-то
                # помечать, покупка была ли через роутер или напрямую; если роутер, то сколько там transfers было
                # пометка в _transfers если liq_burned и verified
                # в _tranfers добавить какой роутер транзакции (поле to)
                # проверить logIndex в _transfers_ не None ли
                # писать сколько в данной транзакции/transfer кошельков получило токенов в результате большого Swap
                # block builder в _blocks выводить - это поле miner
                # в бд заноксить значение сколько eth/$ добавлено в пулы через mint

                # (!) баги
                # почему-то в этой транзакции 0x850d8617f43cd89233675a1bd661228986171f111c4a8314655bb47c7778e980
                #  нет никаких данных в _transfers, хотя было пополнение IN с бинанса на кошелек 0xb157E8210a701760410E19e13d47be3929e9757B



        if self.verbose:
            print("logs in %.3fs" % (time.time() - start_time_logs))


        def check_calls(level, tx_internal):
            if level > 1000:
                # print('level:', level)
                # if self.verbose:
                #     print(f"\x1b[31;1m(!)\x1b[0m calls level > 60")
                # assert 0, "calls level > 60"
                return
            if 'calls' in tx_internal:
                for t_i in tx_internal['calls']:
                    if 'value' in t_i:
                        value_wei = int(t_i['value'], 16)
                        if value_wei != 0:

                            # (!) может встретиться execution reverted, не ясно что с этим делать
                            # пример: https://etherscan.io/tx/0x0bf874d3b8aa9c917fd2c3e18ddbbe283b0c07b11ca7008a5561f2ddf833ee40#internal

                            transaction_transfers.append(_transfers_template | {
                                'address': None,
                                'from_': t_i['from'].lower(),
                                'to_': t_i['to'].lower(),
                                'value': value_wei,
                                'value_usd': value_wei / tx_eth_price,
                                'value_usd_per_token_unit': 1 / tx_eth_price,
                                'category': 'internal',
                            })
                            if self.verbose:
                                print(f"  \x1b[34;1m[x] Call\x1b[0m, from={to_checksum_address(t_i['from'])}, to={to_checksum_address(t_i['to'])}")
                                print(f"      -> {value_wei} Wei ({from_wei(value_wei, 'ether')} ETH, {round(value_wei / tx_eth_price, 2)} $)")
                            
                    else:
                        pass
                    check_calls(level + 1, t_i)
        check_calls(1, tx_internal['result'])





        if self.verbose:
            print("executied in %.3fs" % (time.time() - start_time_t))


        return {'transaction': _db_transaction | {'executed_in': time.time() - start_time_t}, 'transfers': transaction_transfers, 'logs': _db_logs, 'minted_pair': minted_pair}







if __name__ == '__main__':


    testnet = 'http://127.0.0.1:8545'

    w3 = Web3(Web3.HTTPProvider(f"{testnet}/?t"))
    if not w3.is_connected():
        raise Exception("w3 not connected")


    # erc20
    # 0xe63ace900dd603e20f10b5a2a38fd35bbe08c80901587ec935936704b4a3ca4d

    # erc721
    # 0x35c3b65cb778f618ac54a9fb1b529bb1983b41418dcfb553ed8ed9d18778f384

    # erc1155
    # 0x1d6814c396edb3e7299b6095adfa2d68a78926676a85f754f795f66556612a1a
    # 0xc64601979aff26c939d4ba21608ee2b36c070fac0b6514305a885b87522c64dc
    # 0x6040430c5d50dadf9bebbc570df92051218a152bab6e906b5d664bfe6535cefb
    # 0x75c4a6235eeb7aecacb2ecb51b0c5a89cf09c7e8911664b6390cd2561ed77210
    # 0x6e549d772335d24a017eeeec2b4db51b42e72aa4898a4e9165e0b98793923fdb
    # event TransferSingle(address indexed _operator, address indexed _from, address indexed _to, uint256 _id, uint256 _value);
    # event TransferBatch(address indexed _operator, address indexed _from, address indexed _to, uint256[] _ids, uint256[] _values);



    sb = SyncBlock(testnet=testnet, realtime=False, verbose=0)
    

    try:
        # 13916166=Jan-01-2022 12:00:03 AM +UTC
        # 18689000 1 Dec 2023
        # 18995767 13 Jan - до сюда проиндексирована база
        # 19131585 1 Feb
        # 19331585 29 Feb
        # 19815000 7 May
        # 19412158 11 March
        # 20400000 27 July
        # 21200000 16 Nov
        with Pool(72) as pool:

            #for block_number in range(21200000, w3.eth.block_number + 1 - 3):
            for block_number in reversed(range(20400000, w3.eth.block_number + 1 - 3)):
                thread = Thread(target=sb.exec_block, args=(pool, block_number))  # create the thread
                thread.start() # start the thread
                thread.join() # wait thread ends

    finally:
        # sb.pool_4.close()
        # sb.pool_cpu.close()
        conn.close()
        print('all closed')

