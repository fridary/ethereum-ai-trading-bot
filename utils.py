import os
from os import listdir
from os.path import isfile, join
import sys
import json
import random
import pickle
import requests
import traceback
import time
import string
import pandas as pd
import statistics
from math import floor, log10, inf
from eth_utils import keccak
from web3 import Web3, eth as web3_eth
from web3.exceptions import BadFunctionCallOutput, ContractLogicError, Web3ValidationError, Web3RPCError
from web3._utils.events import get_event_data
from eth_utils import event_abi_to_log_topic, to_hex, to_checksum_address, from_wei, to_wei
from eth_abi import abi as abi_eth_lib
from hexbytes import HexBytes
from pprint import pprint
from datetime import datetime
from filelock import FileLock
from threading import Thread
from multiprocessing import Pool
import multiprocessing.pool
from attributedict.collections import AttributeDict
from alchemy_sdk_py import Alchemy # https://github.com/Cyfrin/alchemy_sdk_py
from moralis import evm_api
import dns.resolver
from dns.resolver import NoAnswer
import socket
import whois
import talib.abstract as ta

import psycopg2
from psycopg2.extras import Json, DictCursor, RealDictCursor


FILES_DIR = "/home/root/python/ethereum"
DIR_LOAD_OBJECTS = '/disk_sdc/last_trades_objects'
MINIMAL_WALLET_BLOCKS_DIFF_DB = 7000 # ~24 h

GETH_PORT = 8545


API_ETHERSCAN = [""]
API_ALCHEMY = [""]
API_COVALENT = [""]
API_CHAINBASE = ""
API_MORALIS = [""]

UNISWAP_UNIVERSAL_ROUTER = "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD"
UNISWAP_V2_ROUTER = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
UNISWAP_V3_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
UNISWAP_V3_POSITIONS_NFT = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"
UNI_V2_FACTORY = '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f'
UNI_V3_FACTORY = '0x1F98431c8aD98523631AE4a59f267346ea31F984'
WETH = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
USDT = '0xdAC17F958D2ee523a2206206994597C13D831ec7'
USDC = '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'

ACCOUNT_ADDRESS = '...'
ACCOUNT_KEY = '...'

ADDRESS_0X0000 = '0x0000000000000000000000000000000000000000'
ADDRESS_0XDEAD = '0x000000000000000000000000000000000000dEaD'

MEV_BOTS_LIST = ["0xe76014c179F19dA26Bb30A0f085FF0A466B92829","0x5DDf30555EE9545c8982626B7E3B6F70e5c2635f","0xa57Bd00134B2850B2a1c55860c9e9ea100fDd6CF"]


# https://medium.com/@hazenbart/determining-nft-ownership-at-large-scale-using-web3-py-f1d5752edf08
ERC721_ABI_TRANSFER_EVENT = json.loads('{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"}')
ERC20_TRANSFER_KECCAK = keccak(text="Transfer(address,address,uint256)")

# basic ERC-20: name(), symbol(), decimals(), balanceOf(), Transfer(), totalSupply()
BASIC_ERC20_ABI = json.loads('[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"type":"function"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"pure","type":"function"}]')

db_config_psql = {'host': '127.0.0.1', 'port': 60222, 'user': '...', 'password': '...', 'database': '...', 'sslmode': 'disable'}
keepalive_kwargs = {
    # "connect_timeout": 3,
    "keepalives": 1,
    "keepalives_idle": 30, # Wait 30 seconds before sending keepalive
    "keepalives_interval": 10, # Interval between keepalives
    "keepalives_count": 5, # Send 5 keepalives before declaring connection dead
}
# https://www.psycopg.org/docs/pool.html
# https://www.geeksforgeeks.org/python-postgresql-connection-pooling-using-psycopg2/


chain_contract_geth, chain_contract_erigon = None, None

with open(f"{FILES_DIR}/labels.json", "r") as file:
    FILE_LABELS = json.load(file)





# https://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
class NoDaemonProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False
    @daemon.setter
    def daemon(self, value):
        pass

class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess

class NestablePool(multiprocessing.pool.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(NestablePool, self).__init__(*args, **kwargs)



class ThreadWithReturnValue(Thread):
    
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self, *args):
        Thread.join(self, *args)
        return self._return





def init_workspace():


    os.makedirs(f"{FILES_DIR}/abi", exist_ok=True)
    os.makedirs(f"{FILES_DIR}/contract", exist_ok=True)
    os.makedirs(f"{FILES_DIR}/temp", exist_ok=True)
    os.makedirs(f"{FILES_DIR}/temp/check_gopluslabs", exist_ok=True)
    os.makedirs(f"{FILES_DIR}/temp/check_honeypot", exist_ok=True)


def get_transaction(testnet, hash_):

    # https://docs.nodereal.io/reference/zksync-ethereum-rpc#eth_gettransactionreceipt

    tx = requests.post(testnet, json={"method":"eth_getTransactionByHash","params":[hash_],"id":1,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result'] 
    tx_receipt = requests.post(testnet, json={"method":"eth_getTransactionReceipt","params":[hash_],"id":2,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']

    return tx, tx_receipt


def get_transaction_internals(testnet, hash_):
    tx_internal = requests.post(testnet, json={"jsonrpc":"2.0", "method":"debug_traceTransaction", "params":[hash_, {"tracer":"callTracer"}], "id":1}, headers={"Content-Type": "application/json"}).json()['result']
    
    return tx_internal


def get_transactions_for_train_api(address, last_block):

    def make_req(i):

        if i == 1:
            if not os.path.exists(f"{FILES_DIR}/temp/trans_train_3/{address[-12:]}_lb{last_block}_alchTo.txt"):
                res = requests.post(f'https://eth-mainnet.g.alchemy.com/v2/{random.choice(API_ALCHEMY)}', json={
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "alchemy_getAssetTransfers",
                    "params": [
                        {
                            "toAddress": address,
                            "toBlock": hex(last_block),
                            "category": ['external', 'internal', 'erc20', 'erc721', 'erc1155', 'specialnft'],
                            "excludeZeroValue": False,
                            "maxCount": hex(1000),
                            "order": "desc",
                        }
                    ],
                })
                res = json.loads(res.text)
                with open(f"{FILES_DIR}/temp/trans_train_3/{address[-12:]}_lb{last_block}_alchTo.txt", "w") as file:
                    json.dump(res, file)
                return res
            else:
                with open(f"{FILES_DIR}/temp/trans_train_3/{address[-12:]}_lb{last_block}_alchTo.txt", "r") as file:
                    return json.load(file)
        
        if i == 2:
            if not os.path.exists(f"{FILES_DIR}/temp/trans_train_3/{address[-12:]}_lb{last_block}_alchFrom.txt"):
                res = requests.post(f'https://eth-mainnet.g.alchemy.com/v2/{random.choice(API_ALCHEMY)}', json={
                    "id": 2,
                    "jsonrpc": "2.0",
                    "method": "alchemy_getAssetTransfers",
                    "params": [
                        {
                            "fromAddress": address,
                            "toBlock": hex(last_block),
                            "category": ['external', 'internal', 'erc20', 'erc721', 'erc1155', 'specialnft'],
                            "excludeZeroValue": False,
                            "maxCount": hex(1000),
                            "order": "desc",
                        }
                    ],
                })
                res = json.loads(res.text)
                with open(f"{FILES_DIR}/temp/trans_train_3/{address[-12:]}_lb{last_block}_alchFrom.txt", "w") as file:
                    json.dump(res, file)
                return res
            else:
                with open(f"{FILES_DIR}/temp/trans_train_3/{address[-12:]}_lb{last_block}_alchFrom.txt", "r") as file:
                    return json.load(file)
        
        if i == 3:
            return []

    
    results = [make_req(1), make_req(2), make_req(3)]

    return results


def get_wallet_transactions(address, last_block, max_transactions, debug):


    if 0 or not os.path.exists(f"{FILES_DIR}/temp/trans_address/trans_m{max_transactions}_lb{last_block}_{address[-12:]}.txt"):


        # alchemy
        if 1:
            api_key_alchemy = random.choice(API_ALCHEMY)
            res = requests.post(f'https://eth-mainnet.g.alchemy.com/v2/{api_key_alchemy}', json={
                "id": 1,
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [
                    {
                        "toAddress": address,
                        "toBlock": hex(last_block),
                        "category": ['external', 'internal', 'erc20', 'erc721', 'erc1155', 'specialnft'],
                        "excludeZeroValue": False,
                        "maxCount": hex(max_transactions + 200),
                        "order": "desc",
                    }
                ],
            })
            res_headers = res.headers
            res = json.loads(res.text)
            if 'error' in res:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} api_key_alchemy={api_key_alchemy}")
                pprint(res)
                pprint(res_headers)
                exit()
            transactions_alchemy_to = res['result']['transfers']
            
            api_key_alchemy = random.choice(API_ALCHEMY)
            res = requests.post(f'https://eth-mainnet.g.alchemy.com/v2/{api_key_alchemy}', json={
                "id": 2,
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [
                    {
                        "fromAddress": address,
                        "toBlock": hex(last_block),
                        "category": ['external', 'internal', 'erc20', 'erc721', 'erc1155', 'specialnft'],
                        "excludeZeroValue": False,
                        "maxCount": hex(max_transactions + 200),
                        "order": "desc",
                    }
                ],
            })
            res_headers = res.headers
            res = json.loads(res.text)
            if 'error' in res:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} api_key_alchemy={api_key_alchemy}")
                pprint(res)
                pprint(res_headers)
                exit()
            transactions_alchemy_from = res['result']['transfers']

            transactions_alchemy = transactions_alchemy_from + transactions_alchemy_to

        transactions = {}
        for t in transactions_alchemy:
            if t['hash'] not in transactions:
                transactions[t['hash']] = {'b': int(t['blockNum'], 16)}
        print(f"downloaded transactions after loop in alchemy: {len(transactions)}")

        transactions = [(v['b'], k) for k, v in sorted(transactions.items(), key=lambda item: item[1]['b'])]
        transactions = transactions[-max_transactions:]

        with open(f"{FILES_DIR}/temp/trans_address/trans_m{max_transactions}_lb{last_block}_{address[-12:]}.txt", "w") as file:
            json.dump(transactions, file)





    with open(f"{FILES_DIR}/temp/trans_address/trans_m{max_transactions}_lb{last_block}_{address[-12:]}.txt", "r") as file:
        transactions = json.load(file)


    return transactions




def get_address_by_pair(w3, conn, pair, last_block=None):


    # token0, token1
    abi = json.loads('[{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]')

    pair_contract = w3.eth.contract(address=to_checksum_address(pair), abi=abi)
    try:
        token0 = pair_contract.functions.token0().call(block_identifier=last_block)
        token1 = pair_contract.functions.token1().call(block_identifier=last_block)
    except ContractLogicError as e:
        print(traceback.format_exc())
        return None

    # могут встречаться другие связки, пример:
    # hash=0x297bf9e67127a1a38e72003cd8f98e9db1b58006d736ef073d8a798b7ab1fdeb, token0=0x423f4e6138E475D85CF7Ea071AC92097Ed631eea, token1=0xb9f599ce614Feb2e1BBe58F180F370D05b39344E
    # assert token0 in [utils.WETH, utils.USDC, utils.USDT] or token1 in [utils.WETH, utils.USDC, utils.USDT], f"hash={tx['hash']}, token0={token0}, token1={token1}"
    if token0 in [WETH, USDC, USDT] or token1 in [WETH, USDC, USDT]:
        if token0 in [WETH, USDC, USDT]:
            return token1
        if token1 in [WETH, USDC, USDT]:
            return token0
    else:
        print(f"Проблема со связками, pair={pair}, token0={token0}, token1={token1}")
        return None



def have_mints_before(conn, pair, until_block):

    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT 1 FROM _logs
                WHERE address='{pair}'
                    AND blockNumber<{until_block}
                    AND (topics_0='0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f' AND topics_1='{UNISWAP_V2_ROUTER.lower()}'
                        OR topics_0='0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde' AND topics_1='{UNISWAP_V3_POSITIONS_NFT.lower()}')   
            """)
        _exists_block = cursor.fetchone()
        return _exists_block != None


def get_mints_txs(conn, pairs, last_block, verbose=0):

    if len(pairs) == 1: pairs.append('any-value') # bug with one tuple elem comma https://stackoverflow.com/questions/36637564/how-to-perfectly-convert-one-element-list-to-tuple-in-python

    pairs = tuple([p.lower() for p in pairs])

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:

        start_time_e = time.time()

        sql = f"""
            with cte as 
            (
                SELECT
                    transaction_id id,
                    address,
                    topics_0,
                    topics_1,
                    topics_2,
                    topics_3,
                    decoded_data
                FROM _logs
                WHERE address in {pairs}
                    AND blockNumber<={last_block}
                    AND (topics_0='0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f' AND topics_1='{UNISWAP_V2_ROUTER.lower()}'
                        OR topics_0='0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde' AND topics_1='{UNISWAP_V3_POSITIONS_NFT.lower()}') 
            )
            select
                t.from_ as from,
                t.to_ as to,
                t.gas,
                t.gasPrice,
                t.hash,
                t.maxFeePerGas,
                t.maxPriorityFeePerGas,
                t.nonce,
                t.value,
                t.contractAddress,
                t.cumulativeGasUsed,
                t.gasUsed,
                t.status,
                t.transactionIndex,
                bl.number as block_number,
                bl.timestamp as block_timestamp,
                bl.baseFeePerGas as block_basefeepergas,
                bl.gasUsed as block_gasused,
                bl.gasLimit as block_gaslimit,
                bl.eth_price,
                c.address log_address,
                c.topics_0,
                c.topics_1,
                c.topics_2,
                c.topics_3,
                (SELECT array_agg(JSON_BUILD_OBJECT('address',address,'topics_0',topics_0,'topics_1',topics_1,'topics_2',topics_2,'topics_3',topics_3,'pair_created',pair_created,'decoded_data',decoded_data,'logIndex',logIndex)) FROM _logs WHERE transaction_id = c.id) logs_array
            FROM _transactions t
            left JOIN _blocks bl ON bl.number = t.blockNumber
            right join cte c on t.id = c.id
            ORDER BY t.blockNumber asc, t.transactionIndex asc
        """

        cursor.execute(sql)
        _rows = cursor.fetchall()

        if verbose:
            print(f"{(time.time() - start_time_e):.3f}s select get_mints_txs()")

        return _rows


def get_address_transfers(conn, address, N, last_block):


    address = address.lower()

    if address in [WETH.lower(), USDT.lower(), USDC.lower()]:
        print(f"\x1b[31;1m(!)\x1b[0m !!!!!!!! address in WETH/USDT/USDC, address={address}")
        return []

        

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:

        # # Set the statement timeout to 2 seconds
        # cur.execute("SET statement_timeout = '2000'")

        cursor.execute(f"""
            SELECT
                transactionHash,
                transactionIndex,
                blockNumber,
                timestamp,
                address,
                from_ as from,
                to_ as to,
                value,
                value_usd,
                value_usd_per_token_unit,
                category,
                status
            FROM _transfers
            WHERE (from_='{address}' or to_='{address}') and blockNumber<={last_block}
            ORDER BY blockNumber DESC, transactionIndex ASC
            LIMIT {N}
        """)

        _rows = cursor.fetchall()

        return _rows
            



def get_tokens_pairs(conn, addresses, last_block, verbose=0):

    addresses_pairs = {} # key - address, value - list of pairs
    for adr in addresses:
        addresses_pairs[adr] = []

    if not addresses:
        return {}
        
    addresses = list(map(str.lower, addresses))
    if len(addresses) == 1: addresses = tuple(addresses[0],)
    else: addresses = tuple(addresses)


    with conn.cursor(cursor_factory=RealDictCursor) as cursor:

        start_time = time.time()
        sql = f"""SELECT pair_created, topics_1, topics_2 FROM _logs WHERE 
        (
            topics_1 in {addresses} or topics_2 in {addresses}
        ) and
        (
            address='{UNI_V2_FACTORY.lower()}' and topics_0='0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'
            or
            address='{UNI_V3_FACTORY.lower()}' and topics_0='0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
        ) and blockNumber<={last_block}"""
        cursor.execute(sql)
        _rows_pairs = cursor.fetchall() # [RealDictRow([('pair_created', '0x63496638bb5ab6332a74908f1a13eb8c0c0515b3')])]
        if verbose:
            print("%.3fs select pair_created" % (time.time() - start_time))

        for rp in _rows_pairs:
            if to_checksum_address(rp['topics_1']) in addresses_pairs:
                addresses_pairs[to_checksum_address(rp['topics_1'])].append(to_checksum_address(rp['pair_created']))
            if to_checksum_address(rp['topics_2']) in addresses_pairs:
                addresses_pairs[to_checksum_address(rp['topics_2'])].append(to_checksum_address(rp['pair_created']))

    
    return addresses_pairs



def get_token_transactions(conn, address, pair, last_block=None, realtime=False, realtime_bMint=False):

    address = address.lower()

    start_time_e = time.time()

    print(f'starting get_token_transactions(), address={address}, pair={pair}')

    if pair == None:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:

            start_time = time.time()
            sql = f"""SELECT pair_created FROM _logs WHERE 
            (
                topics_1='{address}' or topics_2='{address}'
            ) and
            (
                address='{UNI_V2_FACTORY.lower()}' and topics_0='0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'
                or
                address='{UNI_V3_FACTORY.lower()}' and topics_0='0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
            )"""
            cursor.execute(sql)
            _rows_pairs = cursor.fetchall() # [RealDictRow([('pair_created', '0x63496638bb5ab6332a74908f1a13eb8c0c0515b3')])]
            print("%.3fs select pair_created" % (time.time() - start_time))
            print(sql)

            _addresses = (address,)
            for rp in _rows_pairs:
                _addresses += (rp['pair_created'],)
                if pair == None:
                    pair = rp['pair_created']

            print('_addresses:')
            pprint(_addresses)
            if len(_addresses) == 1:
                print(f'Пары не найдено по address={address}')
                return 'stop'
        
        print(f'pair found={pair} (first one)')
    
    else:
        pair = pair.lower()
        _addresses = (address,pair,)

        print(f'pair in args: {pair}')

        if not realtime_bMint:
            with conn.cursor() as cursor:
                print(f'Проверим, есть ли пара в _logs')
                start_time = time.time()
                #cursor.execute(f"SELECT EXISTS(SELECT 1 FROM _logs WHERE pair_created='{pair}')")
                cursor.execute(f"SELECT 1 FROM _logs WHERE pair_created='{pair}'")
                _exists = cursor.fetchone()
                print("%.3fs запрос SELECT pair_created в _logs" % (time.time() - start_time))
                #if not _exists[0]:
                if _exists == None:
                    print(f"Не существует пары в базе, выходим")
                    return 'stop'



    # (!)
    # здесь важно сделать проверку если пара или адрес = utils.WETH, USDT, USDC
    for aa in _addresses:
        if aa in [WETH.lower(), USDT.lower(), USDC.lower()]:
            print(f"(!) address or pair in WETH/USDT/USDC, address={address}, pair={pair}")
            assert 0, "Тут посмотерть utils.py"
    
    if realtime and len(_addresses) > 2:
        print("len(_addresses) > 2")
        return 'stop'


    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        


        start_time = time.time()



        sql = f"""
            WITH 
                a AS MATERIALIZED (
                    SELECT l.transaction_id
                    FROM _logs l
                    WHERE l.address in {_addresses} or l.pair_created in {_addresses} or l.topics_1='{address}'
                    LIMIT 20000
                ),
                b AS MATERIALIZED (
                    SELECT tr.transaction_id
                    FROM _transfers tr
                    WHERE tr.address in {_addresses} or tr.from_ in {_addresses} or tr.to_ in {_addresses}
                    LIMIT 20000
                )
            SELECT 
                t.from_ as from,
                t.to_ as to, 
                t.gas,
                t.gasPrice,
                t.hash,
                t.maxFeePerGas,
                t.maxPriorityFeePerGas,
                t.nonce,
                t.value,
                t.contractAddress,
                t.cumulativeGasUsed,
                t.gasUsed,
                t.status,
                t.transactionIndex,
                (SELECT array_agg(JSON_BUILD_OBJECT('address',address,'from',from_,'to',to_,'value',value,'value_usd',value_usd,'value_usd_per_token_unit',value_usd_per_token_unit,'category',category,'status',status)) FROM _transfers WHERE transaction_id = t.id) transfers_array,
                (SELECT array_agg(JSON_BUILD_OBJECT('address',address,'topics_0',topics_0,'topics_1',topics_1,'topics_2',topics_2,'topics_3',topics_3,'pair_created',pair_created,'decoded_data',decoded_data,'logIndex',logIndex)) FROM _logs WHERE transaction_id = t.id) logs_array,
                bl.number as block_number,
                bl.timestamp as block_timestamp,
                bl.baseFeePerGas as block_basefeepergas,
                bl.gasUsed as block_gasused,
                bl.gasLimit as block_gaslimit
            FROM _transactions t 
            INNER JOIN _blocks bl ON bl.number = t.blockNumber
            WHERE t.id IN 
                (SELECT transaction_id FROM a 
                UNION 
                SELECT transaction_id FROM b)
            ORDER BY t.blockNumber asc, t.transactionIndex asc
            LIMIT 1500
        """


        print(f"starting SELECT transactions_tupl get_token_transactions() for address={address}")


        cursor.execute(sql)
        _rows = cursor.fetchall()

        print(f"{(time.time() - start_time):.3f}s select transactions_tupl get_token_transactions(), len(_rows): {len(_rows)}, address={address}")

        if realtime and len(_rows) == 1500:
            print('Похоже ранее был Mint(), что так много строк')
            return 'stop'


    
    return _rows



def get_wallet_transactions_only_transfers(conn, address, last_block, limit):

    address = address.lower()

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        start_time = time.time()

        # Titan Builder = 0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97

        # ставим t.transactionIndex desc, т.к. в конце делаем reverse()
        sql = f"""
            with cte as 
            (
                select
                    transaction_id id,
                    array_agg(JSON_BUILD_OBJECT('address',address,'from',from_,'to',to_,'value',value,'value_usd',value_usd,'value_usd_per_token_unit',value_usd_per_token_unit,'extra_data',extra_data,'category',category,'status',status)) transfers_array
                from _transfers tr 
                WHERE (tr.from_='{address}' or tr.to_='{address}' or tr.to_='0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97')
                and tr.blockNumber<={last_block}
                group by transaction_id, blockNumber, logIndex
                ORDER BY blockNumber DESC, logIndex ASC
                LIMIT {limit}
            )
            select
                t.from_ as from,
                t.to_ as to,
                t.gas,
                t.gasPrice,
                t.hash,
                t.maxFeePerGas,
                t.maxPriorityFeePerGas,
                t.nonce,
                t.value,
                t.contractAddress,
                t.cumulativeGasUsed,
                t.gasUsed,
                t.status,
                t.transactionIndex,
                c.transfers_array,
                bl.number as block_number,
                bl.timestamp as block_timestamp,
                bl.baseFeePerGas as block_basefeepergas,
                bl.gasUsed as block_gasused,
                bl.gasLimit as block_gaslimit,
                bl.eth_price
            FROM _transactions t
            left JOIN _blocks bl ON bl.number = t.blockNumber
            right join cte c on t.id = c.id
            ORDER BY t.blockNumber desc, t.transactionIndex desc
        """

        cursor.execute(sql)
        _rows = cursor.fetchall()

        # убираем первый row, так как MATERIALIZED поиск был с LIMIT и _transfers array может быть не полным
        # del _rows[0]

    
    return list(reversed(_rows))



def get_token_transactions_only_transfers(w3, conn, address, last_block, from_block, limit, sort, realtime=False, can_be_old_token=False, verbose=0):

    address = address.lower()

    start_time_e = time.time()

    if verbose:
        print(f'starting get_token_transactions_only_transfers(), address={address}')


    _addresses = (address,)

    pairs_list, txs_sql_pairs = None, None



    if 1:
    
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:

            start_time = time.time()

            cursor.execute(f"""
                SELECT
                    t.from_ as from,
                    t.to_ as to, 
                    t.gas,
                    t.gasPrice,
                    t.hash,
                    t.maxFeePerGas,
                    t.maxPriorityFeePerGas,
                    t.nonce,
                    t.value,
                    t.contractAddress,
                    t.cumulativeGasUsed,
                    t.gasUsed,
                    t.status,
                    t.transactionIndex,
                    bl.number as block_number,
                    bl.timestamp as block_timestamp,
                    bl.baseFeePerGas as block_basefeepergas,
                    bl.gasUsed as block_gasused,
                    bl.gasLimit as block_gaslimit,
                    bl.eth_price,
                    l.address as log_address,
                    l.topics_0,
                    l.topics_1,
                    l.topics_2,
                    l.decoded_data,
                    l.pair_created
                FROM _logs l
                LEFT JOIN _transactions t ON l.transaction_id = t.id
                LEFT JOIN _blocks bl ON l.blockNumber = bl.number
                WHERE
                    (
                        topics_1='{address}' or topics_2='{address}'
                    ) and
                    (
                        address='{UNI_V2_FACTORY.lower()}' and topics_0='0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'
                        or
                        address='{UNI_V3_FACTORY.lower()}' and topics_0='0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
                    )
                    and l.blockNumber<={last_block}
                ORDER BY l.blockNumber asc, l.logIndex asc
            """)
            txs_sql_pairs = cursor.fetchall() # [RealDictRow([('pair_created', '0x63496638bb5ab6332a74908f1a13eb8c0c0515b3')])]

            if verbose:
                print("%.3fs select pair_created" % (time.time() - start_time))


        pairs_list = []

        if not can_be_old_token:
            
            for rp in txs_sql_pairs:

                if rp['decoded_data']['token0'] in [WETH, USDC, USDT] and rp['decoded_data']['token1'] in [WETH, USDC, USDT]:
                    if verbose:
                        print('Почему-то token0 и token1 в [WETH, USDC, USDT]')
                    continue
                else:
                    if rp['decoded_data']['token0'] in [WETH, USDC, USDT] or rp['decoded_data']['token1'] in [WETH, USDC, USDT]:
                        if rp['decoded_data']['token0'] in [WETH, USDC, USDT]:
                            if rp['decoded_data']['token0'] == WETH: token1_currency = 'WETH'
                            if rp['decoded_data']['token0'] == USDC: token1_currency = 'USDC'
                            if rp['decoded_data']['token0'] == USDT: token1_currency = 'USDT'
                            _pair_info = {'token1_currency': token1_currency, 'token1_address': rp['decoded_data']['token0']}
                        if rp['decoded_data']['token1'] in [WETH, USDC, USDT]:
                            if rp['decoded_data']['token1'] == WETH: token1_currency = 'WETH'
                            if rp['decoded_data']['token1'] == USDC: token1_currency = 'USDC'
                            if rp['decoded_data']['token1'] == USDT: token1_currency = 'USDT'
                            _pair_info = {'token1_currency': token1_currency, 'token1_address': rp['decoded_data']['token1']}
                        _pair_info |= {'block_created': rp['block_number'], 'block_timestamp': rp['block_timestamp']}
                    else:
                        continue


                
                
                #_addresses += (rp['pair_created'],)
                exchange = None
                if rp['log_address'] == UNI_V2_FACTORY.lower(): exchange = 'V2'
                if rp['log_address'] == UNI_V3_FACTORY.lower(): exchange = 'V3'
                pairs_list.append({'pair': rp['pair_created'], 'token1_address': _pair_info['token1_address'], 'exchange': exchange, 'block_created': _pair_info['block_created'], 'block_timestamp': _pair_info['block_timestamp']})
            
                if verbose:
                    print(f"Для pair={rp['pair_created']} {_pair_info}, exchange={exchange}")
            
        
        else:

            start_time = time.time()
            for token2 in [WETH, USDT, USDC]:
                if __pair := get_token_pair(w3=w3, address=address, exchange='V2', fee=None, token2_address=token2, block=last_block):
                    pairs_list.append({'pair': __pair, 'token1_address': token2, 'exchange': 'V2'})
                    # _addresses += (__pair.lower(),)
                if __pair := get_token_pair(w3=w3, address=address, exchange='V3', fee=3000, token2_address=token2, block=last_block):
                    pairs_list.append({'pair': __pair, 'token1_address': token2, 'fee': 3000, 'exchange': 'V3'})
                if __pair := get_token_pair(w3=w3, address=address, exchange='V3', fee=5000, token2_address=token2, block=last_block):
                    pairs_list.append({'pair': __pair, 'token1_address': token2, 'fee': 5000, 'exchange': 'V3'})
                if __pair := get_token_pair(w3=w3, address=address, exchange='V3', fee=10000, token2_address=token2, block=last_block):
                    pairs_list.append({'pair': __pair, 'token1_address': token2, 'fee': 10000, 'exchange': 'V3'})

            for pa in pairs_list:
                pa['block_created'] = None
                pa['block_timestamp'] = None

            if verbose:
                print(f"{(time.time() - start_time):.2f}s checking all get_token_pair() WETH/USDT/USDC V2/V3")

            # _addresses = (address,'0x7373126710174da79118A553731Bd702c89244b1'.lower(),)
            # pairs_list = [{'exchange': 'V3',
            # 'fee': 10000,
            # 'pair': '0x7373126710174da79118A553731Bd702c89244b1',
            # 'token1_address': '0xdAC17F958D2ee523a2206206994597C13D831ec7'}]


        if 0:
            if len(pairs_list) > 1:
                max_reserves_pair_usd = 0
                max_liq_pair = None
                for pair in pairs_list:
                    
                    token1_currency = None
                    if pair['token1_address'] == WETH: token1_currency = 'WETH'
                    if pair['token1_address'] == USDC: token1_currency = 'USDC'
                    if pair['token1_address'] == USDT: token1_currency = 'USDT'
                    assert token1_currency != None, f"token1_currency == None"
                    _kwards = usd_per_one_token(w3=w3, address=to_checksum_address(address), block=last_block, pair_address=to_checksum_address(pair['pair']), token1_currency=token1_currency, tx_eth_price=379795027300409.4)

                    if _kwards['reserves_pair_usd'] > max_reserves_pair_usd:
                        max_reserves_pair_usd = _kwards['reserves_pair_usd']
                        max_liq_pair = pair['pair']

                for pair in pairs_list[:]:
                    if pair['pair'] != max_liq_pair:
                        pairs_list.remove(pair)

                if verbose:
                    print(f"Из {len(pairs_list)} пар выбрали самую ликвидную {round(max_reserves_pair_usd,2)}$ pair={pairs_list[0]['pair']}")


        for p in pairs_list:
            _addresses += (p['pair'],)

    else:

        _addresses += (primary_pair.lower(),)


    if verbose:
        print('_addresses:')
        pprint(_addresses)
        print('pairs_list:')
        pprint(pairs_list)
    if len(_addresses) == 1:
        if verbose:
            print(f'Пары не найдено по address={address}')
        return [], [], []
    

    


    # (!)
    # здесь важно сделать проверку если пара или адрес = utils.WETH, USDT, USDC
    for aa in _addresses:
        if aa in [WETH.lower(), USDT.lower(), USDC.lower()]:
            print(f"(!) address or pair in WETH/USDT/USDC, address={address}, pair={pair}")
            assert 0, "Тут посмотерть utils.py"
    
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        


        sql = f"""
            WITH 
                b AS MATERIALIZED (
                    SELECT blockNumber, transaction_id, logIndex
                    -- SELECT blockNumber
                    FROM _transfers
                    WHERE (address in {_addresses} or from_ in {_addresses} or to_ in {_addresses})
                        and blockNumber>={from_block}
                        and blockNumber<={last_block}
                    -- GROUP BY blockNumber
                    GROUP BY blockNumber, logIndex, transaction_id
                    -- ORDER BY blockNumber {sort}
                    ORDER BY blockNumber {sort}, logIndex asc, transaction_id
                    LIMIT {limit}
                )
            SELECT 
                t.from_ as from,
                t.to_ as to, 
                t.gas,
                t.gasPrice,
                t.hash,
                t.maxFeePerGas,
                t.maxPriorityFeePerGas,
                t.nonce,
                t.value,
                t.contractAddress,
                t.cumulativeGasUsed,
                t.gasUsed,
                t.status,
                t.transactionIndex,
                t.bribe_eth,
                (SELECT array_agg(JSON_BUILD_OBJECT('address',address,'from',from_,'to',to_,'value',value,'value_usd',value_usd,'value_usd_per_token_unit',value_usd_per_token_unit,'extra_data',extra_data,'category',category,'status',status,'logIndex',logIndex,'total_supply',total_supply,'pair_address',pair_address)) FROM _transfers WHERE transaction_id = t.id) transfers_array,
                bl.number as block_number,
                bl.miner as block_miner,
                bl.timestamp as block_timestamp,
                bl.baseFeePerGas as block_basefeepergas,
                bl.gasUsed as block_gasused,
                bl.gasLimit as block_gaslimit,
                bl.eth_price
            FROM _transactions t 
            INNER JOIN _blocks bl ON bl.number = t.blockNumber
            WHERE t.id IN (SELECT transaction_id FROM b)
            -- WHERE t.blockNumber IN (SELECT blockNumber FROM b)
            ORDER BY t.blockNumber asc, t.transactionIndex asc
        """

        #save_log_strg(sql, address=address, block=last_block)

        if verbose:
            print(f"starting SELECT transactions_tupl get_token_transactions() for address={address}")


        start_time = time.time()
        cursor.execute(sql)
        _rows = cursor.fetchall()

        # убираем первый row, так как MATERIALIZED поиск был с LIMIT и _transfers array может быть не полным
        if sort == 'desc':
            _rows_first_block = _rows[0]['block_number']
            for row in _rows[:]:
                if row['block_number'] == _rows_first_block:
                    _rows.remove(row)
                else:
                    break
        elif sort == 'asc':
            _rows_last_block = _rows[-1]['block_number']
            for row in reversed(_rows[:]):
                if row['block_number'] == _rows_last_block:
                    _rows.remove(row)
                else:
                    break

        if verbose:
            print(f"{(time.time() - start_time):.3f}s select transactions_tupl get_token_transactions_only_transfers(), len(_rows): {len(_rows)}, address={address}")


    
    
    # print("%.3fs get_token_transactions()" % (time.time() - start_time_e))
    return _rows, pairs_list, txs_sql_pairs


def get_token_owners(conn, address, last_block, verbose=0):

    address = address.lower()

    start_time_e = time.time()

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:

        start_time = time.time()
        sql = f"""
        select * from (
            SELECT
                t.from_ as from,
                t.to_ as to, 
                t.gas,
                t.gasPrice,
                t.hash,
                t.maxFeePerGas,
                t.maxPriorityFeePerGas,
                t.nonce,
                t.value,
                t.contractAddress,
                t.cumulativeGasUsed,
                t.gasUsed,
                t.status,
                t.transactionIndex,
                bl.number as block_number,
                bl.timestamp as block_timestamp,
                bl.baseFeePerGas as block_basefeepergas,
                bl.gasUsed as block_gasused,
                bl.gasLimit as block_gaslimit,
                bl.eth_price,
                l.topics_0,
                l.topics_1,
                l.topics_2
            FROM _logs l
            LEFT JOIN _transactions t ON l.transaction_id = t.id
            LEFT JOIN _blocks bl ON l.blockNumber = bl.number
            WHERE
                l.address='{address}' and
                (
                    l.topics_0='0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0' -- OwnershipTransferred()
                    and l.topics_1!=l.topics_2
                )
                --or
                --(
                --    l.topics_0='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- Transfer()
                --    and l.topics_1='0x0000000000000000000000000000000000000000'
                --)
                --)
                and l.blockNumber<={last_block}

            UNION

            select * from (SELECT
                t.from_ as from,
                t.to_ as to, 
                t.gas,
                t.gasPrice,
                t.hash,
                t.maxFeePerGas,
                t.maxPriorityFeePerGas,
                t.nonce,
                t.value,
                t.contractAddress,
                t.cumulativeGasUsed,
                t.gasUsed,
                t.status,
                t.transactionIndex,
                bl.number as block_number,
                bl.timestamp as block_timestamp,
                bl.baseFeePerGas as block_basefeepergas,
                bl.gasUsed as block_gasused,
                bl.gasLimit as block_gaslimit,
                bl.eth_price,
                l.topics_0,
                l.topics_1,
                l.topics_2
            FROM _logs l
            LEFT JOIN _transactions t ON l.transaction_id = t.id
            LEFT JOIN _blocks bl ON l.blockNumber = bl.number
            WHERE
                l.address='{address}' and
                (
                    l.topics_0='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- Transfer()
                    and l.topics_1='0x0000000000000000000000000000000000000000'
                )
                and l.blockNumber<={last_block}
            ORDER BY block_number ASC
            LIMIT 1) xx -- делается LIMIT 1, т.к. может быть много Transfer() от 0x00000.. на разные адреса

        ) t
            ORDER BY t.block_number ASC
        """
        cursor.execute(sql)
        _rows = cursor.fetchall()
        if verbose:
            print("%.3fs select OwnershipTransferred" % (time.time() - start_time))

        # fix если owner уже есть и Transfer() найдено, а sql запрос не верно написан; пример 0x36e2829a74045d58c4595c05e24ceb566c27deb7fc1f6767974ad512cb654534
        _000_made = False
        for i, v in enumerate(_rows[:]):
            if v['topics_1'] == ADDRESS_0X0000:
                _000_made = True
            if i != 0 and v['topics_0'] == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' and _000_made:
                _rows.remove(v)
        

        return _rows
    
    



def get_pair_info(pair_address, last_block, w3=None):

    if w3 == None:
        w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))


    # token0, token1
    abi = json.loads('[{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]')

    pair_contract = w3.eth.contract(address=to_checksum_address(pair_address), abi=abi)
    try:
        token0 = pair_contract.functions.token0().call(block_identifier=last_block)
        token1 = pair_contract.functions.token1().call(block_identifier=last_block)
    except ContractLogicError as e:
        if 'no data' in str(e):
            print('get_pair_info() no data:', e)
            return None
        else:
            print(traceback.format_exc())
            print(traceback.format_exc(), file=sys.stderr)
            return None


    if token0 in [WETH, USDC, USDT] and token1 in [WETH, USDC, USDT]:
        print('Почему-то token0 и token1 в [WETH, USDC, USDT]')
        return None
    else:
        # могут встречаться другие связки, пример:
        # hash=0x297bf9e67127a1a38e72003cd8f98e9db1b58006d736ef073d8a798b7ab1fdeb, token0=0x423f4e6138E475D85CF7Ea071AC92097Ed631eea, token1=0xb9f599ce614Feb2e1BBe58F180F370D05b39344E
        # assert token0 in [WETH, USDC, USDT] or token1 in [WETH, USDC, USDT], f"hash={tx['hash']}, token0={token0}, token1={token1}"
        if token0 in [WETH, USDC, USDT] or token1 in [WETH, USDC, USDT]:
            # if log['topics'][0] == '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822': _exchange = 'V2'
            # if log['topics'][0] == '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67': _exchange = 'V3'
            if token0 in [WETH, USDC, USDT]:
                if token0 == WETH: token1_currency = 'WETH'
                if token0 == USDC: token1_currency = 'USDC'
                if token0 == USDT: token1_currency = 'USDT'
                return {'token1_currency': token1_currency, 'token1_address': token0}
            if token1 in [WETH, USDC, USDT]:
                if token1 == WETH: token1_currency = 'WETH'
                if token1 == USDC: token1_currency = 'USDC'
                if token1 == USDT: token1_currency = 'USDT'
                return {'token1_currency': token1_currency, 'token1_address': token1}



def get_token_transactions_by_api(w3, address, last_block=None):


    if 0 or not os.path.exists(f"{FILES_DIR}/temp/trans_token/trans_token_{address[-12:]}.txt"):
            

        transactions = {}
        pair_address = None
        print(f"get_token_transactions() on {address}")

    

        testnet_2 = 'https://eth-pokt.nodies.app'
        #testnet_2 = 'http://127.0.0.1:8545'
        #testnet_2 = 'https://rpc.lokibuilder.xyz/wallet'
        #testnet_2 = 'https://eth.rpc.blxrbdn.com'
        
        event_signature_hash = w3.keccak(text="OwnershipTransferred(address,address)").hex()
        events = requests.post(testnet_2, json={"method":"eth_getLogs","params":[{"fromBlock": hex(10000000), "address": address.lower(), "topics": ['0x'+str(event_signature_hash)]}],"id":1,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()
        events = events['result']
        #assert len(events)
        if not len(events):
            print('no owner!')
            return
        for ev in events:
            transactions[ev['transactionHash']] = {'b': int(ev['blockNumber'], 16), 't': int(ev['blockNumber'], 16)}



        pair_address = get_token_pair(w3, address, 'V2', None, WETH, block=last_block)
        if not pair_address: pair_address = get_token_pair(w3, address, 'V2', None, USDC, block=last_block)
        if not pair_address: pair_address = get_token_pair(w3, address, 'V2', None, USDT, block=last_block)
        if not pair_address: pair_address = get_token_pair(w3, address, 'V3', None, WETH, block=last_block)
        if not pair_address: pair_address = get_token_pair(w3, address, 'V3', None, USDC, block=last_block)
        if not pair_address: pair_address = get_token_pair(w3, address, 'V3', None, USDT, block=last_block)
        
        print(f"pair_address={pair_address}")


        # alchemy
        # 15693
        if 1:
            while 1:
                try:
                    alchemy = Alchemy(api_key=random.choice(API_ALCHEMY), network=1)
                    transfers_to = alchemy.get_asset_transfers(to_address=address, category=['external', 'internal', 'erc20', 'erc721', 'erc1155', 'specialnft'], max_count=1000, get_all_flag=False)
                    # this is a paginated version, we could add  `get_all_flag=True` but we'd make a LOT of API calls!
                    transactions_alchemy_to = transfers_to[0]
                    print(f"transactions to_address alchemy={len(transactions_alchemy_to)}")
                    # 'blockNum': '0x128c429', 'uniqueId': '...:log:366', 'hash': '...'

                    transfers_from = alchemy.get_asset_transfers(from_address=address, category=['external', 'internal', 'erc20', 'erc721', 'erc1155', 'specialnft'], max_count=1000, get_all_flag=False)
                    transactions_alchemy_from = transfers_from[0]
                    print(f"transactions from_address alchemy={len(transactions_alchemy_from)}")

                    transactions_alchemy_pair_from = []
                    transactions_alchemy_pair_to = []
                    if pair_address:
                        transfers = alchemy.get_asset_transfers(from_address=pair_address, category=['external', 'internal', 'erc20', 'erc721', 'erc1155', 'specialnft'], max_count=1000, get_all_flag=False)
                        transactions_alchemy_pair_from = transfers[0]
                        print(f"transactions pair from_address alchemy={len(transactions_alchemy_pair_from)}")
                        transfers = alchemy.get_asset_transfers(to_address=pair_address, category=['external', 'internal', 'erc20', 'erc721', 'erc1155', 'specialnft'], max_count=1000, get_all_flag=False)
                        transactions_alchemy_pair_to = transfers[0]
                        print(f"transactions pair to_address alchemy={len(transactions_alchemy_pair_to)}")

                    transactions_alchemy = transactions_alchemy_from + transactions_alchemy_to + transactions_alchemy_pair_from + transactions_alchemy_pair_to

                    break

                except ConnectionError:
                    print('Поймали ConnectionError')


        # covalent
        # 37519
        transactions_covalent = []
        if 1:
            page = 1
            while 1:
                # limit 100
                res = requests.get(f'https://api.covalenthq.com/v1/eth-mainnet/address/{address}/transactions_v3/page/{page}/?&key={random.choice(API_COVALENT)}&no-logs=true&block-signed-at-asc=true')
                try:
                    data = res.json()['data']
                except requests.exceptions.JSONDecodeError as e:
                    # too many requests
                    if res.status_code == 429:
                        time.sleep(5)
                        continue
                    else:
                        print('JSONDecodeError error:', e)
                        print(res, res.text)
                        exit()
                if not len(data['items']): break
                transactions_covalent += data['items']
                # {'block_signed_at': '2024-03-17T13:00:47Z', 'block_height': 19454722, 'block_hash': '...', 'tx_hash': '...', ..}
                print(f"#{page} covalent page, len(items)={len(data['items'])}, total={len(transactions_covalent)}", data['items'][0]['block_height'], '..', data['items'][-1]['block_height'])
                page += 1
                if page == 4:
                    print("(!) break earler")
                    break

        


        # moralis
        # 38901
        if 0:
            plan_rate_limit = 150
            endpoint_rate_limit = 5
            allowed_requests = plan_rate_limit / endpoint_rate_limit

            cursor = ""

            response = evm_api.token.get_token_transfers(
                api_key=random.choice(API_MORALIS),
                params={
                    "address": address,
                    "chain": "eth",
                    "limit": 100, # max
                    "cursor": cursor,
                },
            )

            cursor = response["cursor"]
            print(f"#{response['page']} moralis page, transactions: {len(response['result'])}")

            transactions_moralis = response["result"]
            # 'block_hash': '...', 'block_number': '19456031', 'block_timestamp': '2024-03-17T17:24:35.000Z', 'transaction_hash': '...'

            while cursor != "" and cursor is not None:
                if allowed_requests <= 1:
                    time.sleep(1.1)
                    allowed_requests = plan_rate_limit / endpoint_rate_limit

                response = evm_api.token.get_token_transfers(
                    api_key=random.choice(API_MORALIS),
                    params={
                        "address": address,
                        "chain": "eth",
                        "limit": 100, # max
                        "cursor": cursor,
                    },
                )
                for t in response["result"]:
                    transactions_moralis.append(t)
                print(f"#{response['page']} moralis page, transactions: {len(response['result'])}, total: {len(transactions_moralis)}")

                cursor = response["cursor"]
                allowed_requests -= 1

        
        # etherscan
        # 7705
        if 1:
            transactions_etherscan = []
            step = 10000
            page = 1
            while 1:
                # limit 10,000 ?
                res = requests.get(f'http://api.etherscan.io/api?module=account&action=tokentx&address={address}&page={page}&offset={step}&startblock=0&endblock=999999999&sort=asc&apikey={random.choice(API_ETHERSCAN)}')
                data = res.json()['result']
                # if res.json()['status'] == '0' and 'message' in res.json():
                #     print(res.json())
                if not data or not len(data): break
                transactions_etherscan += data
                print(f"#{page} etherscan page, {len(data)}, total={len(transactions_etherscan)}", data[0]['blockNumber'], '..', data[-1]['blockNumber'])
                page += 1
                print("(!) break earler")
                break
            # 'blockNumber': '19448873', 'timeStamp': '1710609359', 'hash': '...', 'blockHash': '...'

            if pair_address:
                page = 1
                while 1:
                    # limit 10,000 ?
                    res = requests.get(f'http://api.etherscan.io/api?module=account&action=tokentx&address={pair_address}&page={page}&offset={step}&startblock=0&endblock=999999999&sort=asc&apikey={random.choice(API_ETHERSCAN)}')
                    data = res.json()['result']
                    # if res.json()['status'] == '0' and 'message' in res.json():
                    #     print(res.json())
                    if not data or not len(data): break
                    transactions_etherscan += data
                    print(f"#{page} etherscan pair page, {len(data)}, total={len(transactions_etherscan)}", data[0]['blockNumber'], '..', data[-1]['blockNumber'])
                    page += 1
                    print("(!) break earler")
                    break


        
        # chainbase
        # 10000
        if 0:
            transactions_chainbase = []
            page = 1
            while 1:
                # limit 100
                res = requests.get(f'https://api.chainbase.online/v1/token/transfers?chain_id=1&contract_address={address}&page={page}&limit=100', headers={
                    'x-api-key': API_CHAINBASE,
                    'accept': 'application/json'
                })
                data = res.json()['data']
                if not data: break
                transactions_chainbase += data
                print(f"#{page} page chainbase, len(data)={len(data)}, total={len(transactions_chainbase)}", data[0]['block_number'], '..', data[-1]['block_number'])
                page += 1
            print(f"transactions chainbase={len(transactions_chainbase)}")
            # 'block_number': 19456037, 'block_timestamp': '2024-03-17T17:25:47Z', 'transaction_hash': '...'


        # thegraph
        # 9476
        if 0:
            transactions_graph = []
            last_timestamp = 0
            while 1:
                query = f"""{{
                    swaps(first: 1000, where: {{ timestamp_gte: {last_timestamp}, pair: "{address.lower()}" }}, orderBy: timestamp, orderDirection: asc) {{
                        transaction {{
                            id
                            blockNumber
                        }}
                        id
                        timestamp
                    }}
                }}"""

                req = requests.post('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2', json={'query': query})
                req = json.loads(req.text)
                if 'data' not in req:
                    print(req)
                    exit()
                swaps = req['data']['swaps']
                # transactions_graph += swaps
                for swa in swaps:
                    if swa not in transactions_graph:
                        transactions_graph.append(swa)
                    #else: print('duplicate', swa['transaction']['id'])
                #pprint(transactions)
                print(f"#? page graph, len(data)={len(swaps)}, total={len(transactions_graph)}")
                if not len(swaps) or len(swaps) == 1:
                    break
                last_timestamp = swaps[-1]['timestamp']
                # if skip >= 3000:
                #     break
                #exit()
            #pprint(transactions)
            print('len transactions_graph', len(transactions_graph))
            # transactions_graph = list(set(transactions_graph))
            # print('len transactions_graph after set()', len(transactions_graph))
            print(transactions_graph)
            exit()
        

        for t in transactions_alchemy:
            if t['hash'] not in transactions and (not last_block or last_block and int(t['blockNum'], 16) <= last_block):
                # print(f"tx={t['hash']} is missing by alchemy at {datetime.fromtimestamp(get_block_time(w3, int(t['blockNum'], 16)))}, blockNumber={int(t['blockNum'], 16)}")
                transactions[t['hash']] = {'b': int(t['blockNum'], 16), 't': int(t['blockNum'], 16)}
                # print(f"tx={t['hash']} alchemy at {datetime.fromtimestamp(get_block_time(w3, int(t['blockNum'], 16)))}, blockNumber={int(t['blockNum'], 16)}")
        print(f"uniq transactions alchemy: {len(transactions)}")
        for t in transactions_etherscan:
            if t['hash'] not in transactions and (not last_block or last_block and int(t['blockNumber']) <= last_block):
                print(f"tx={t['hash']} is missing by etherscan at {datetime.fromtimestamp(get_block_time(w3, t['blockNumber']))}, blockNumber={t['blockNumber']}")
                transactions[t['hash']] = {'b': int(t['blockNumber']), 't': int(t['blockNumber'])}
        for t in transactions_covalent:
            if t['tx_hash'] not in transactions and (not last_block or last_block and int(t['block_height']) <= last_block):
                print(f"tx={t['tx_hash']} is missing by covalent at {datetime.fromtimestamp(get_block_time(w3, int(t['block_height'])))}, blockNumber={int(t['block_height'])}")
                transactions[t['tx_hash']] = {'b': int(t['block_height']), 't': int(t['block_height'])}

        print(f"final transactions len={len(transactions)}")
        transactions = [(v['b'], k) for k, v in sorted(transactions.items(), key=lambda item: item[1]['b'])]
        with open(f"{FILES_DIR}/temp/trans_token/trans_token_{address[-12:]}.txt", "w") as file:
            json.dump(transactions, file)



        


    with open(f"{FILES_DIR}/temp/trans_token/trans_token_{address[-12:]}.txt", "r") as file:
        transactions = json.load(file)


    return transactions


def get_transaction_count(w3, address, block_identifier):

    return w3.eth.get_transaction_count(address, block_identifier=block_identifier)



def get_balance(w3, address, block_identifier):

    return w3.eth.get_balance(to_checksum_address(address), block_identifier=block_identifier)

def contract_totalSupply(w3, address, block_identifier):

    contract = w3.eth.contract(address=address, abi=BASIC_ERC20_ABI)
    try:
        return contract.functions.totalSupply().call(block_identifier=block_identifier)
    except Exception as e:
        if 'no data' not in str(e): # execution reverted
            print(f'there was an error contract_totalSupply() on address={address}:', e)
    
    return None    



def get_event_name_by_sig(signature):

    if signature == '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde':
        return 'Mint' # V3

    signature = signature[:10]
    _signatures = {}

    was_error = False
    for _ in range(30):
        try:
            if was_error:
                print('making new attempt get_event_name_by_sig() for signature:', signature)
            with open(f"{FILES_DIR}/signatures.json", "r") as file:
                _signatures = json.load(file)
        except json.decoder.JSONDecodeError:
            print('json.decoder.JSONDecodeError!! get_event_name_by_sig() signature:', signature)
            time.sleep(.15)
            was_error = True
        except FileNotFoundError:
            break

    if signature in _signatures:
        return _signatures[signature]

    resp = requests.get(f"https://www.4byte.directory/api/v1/signatures/?hex_signature={signature}").json()
    # if len(resp['results']) > 1:
    #     assert 0, resp


    if not resp['results']:
        _signatures[signature] = '?'
    else:
        _signatures[signature] = resp['results'][-1]['text_signature']

    with open(f"{FILES_DIR}/signatures.json", "w") as file:
        json.dump(_signatures, file)

    return _signatures[signature]



def contract_balanceOf(w3, token_address, wallet_address, block_identifier):
    wallet_address = to_checksum_address(wallet_address)

    contract = w3.eth.contract(address=token_address, abi=BASIC_ERC20_ABI)
    try:
        balance = contract.functions.balanceOf(wallet_address).call(block_identifier=block_identifier)
    except Exception as e:
        # print('there was an error contract_balanceOf():', e)
        contract = w3.eth.contract(address=token_address, abi=BASIC_ERC20_ABI)
        try:
            balance = contract.functions.balanceOf(wallet_address).call(block_identifier=block_identifier)
        #except BadFunctionCallOutput as e: # web3.exceptions.BadFunctionCallOutput: Could not decode contract function call to balanceOf with return data: b'', output_types: ['uint256']
        except:
            return None
        

    return balance


def print_address(address, crop=False, w3=None, last_block=None, token_address=None, pairs=[]):
    if address == None:
        return 'None'
    
    mark = None
    address = to_checksum_address(address)
    for _, item in FILE_LABELS['ethereum'].items():
        if address in item:
            mark = item[address]
            break

    mark_black = None
    if address == token_address:
        mark_black = 'адрес токена'
    if address in pairs:
        mark_black = 'адрес пары'

    # if mark in ['Uniswap: Universal Router']: mark = 'Uni'
    # elif mark == 'Gate.io': mark = 'Gate'
    # else: mark = None

    # if mark == 'Gravity Bridge: Bridge': mark = 'Gravity Br'
    # if mark == 'Uniswap Protocol: Permit2': mark = 'Permit2'
    # https://www.lihaoyi.com/post/BuildyourownCommandLinewithANSIescapecodes.html

    extra = ''
    if w3:
        _code = w3.eth.get_code(address)
        if _code != b'':
            extra += f" \033[1m[contract]\033[0m"
        if last_block:
            _nonce = w3.eth.get_transaction_count(address, block_identifier=last_block)
            extra += f", nonce: {_nonce}"
    
    return (address if not crop else address[:5] + '..' + address[-4:]) + (f" \x1b[35;1m({mark})\x1b[0m" if mark else '') + (f" \033[1m({mark_black})\033[0m" if mark_black else '') + extra


def get_block_time(w3, block):

    timestamp = requests.post(str(w3.manager._provider).split('connection ')[1], json={"method":"eth_getBlockByNumber","params":[hex(block), False],"id":1,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()
    if 'result' not in timestamp:
        print(str(w3.manager._provider).split('connection ')[1])
        print(w3.manager._provider)
        pprint(timestamp)
    timestamp = timestamp['result']['timestamp']
    return int(timestamp, 16)

def get_abi(address, debug=0):
    address = to_checksum_address(address)

    if not debug:
        was_error = False
        for _ in range(30):
            try:
                if was_error:
                    print('making new attempt for address:', address)
                with open(f"{FILES_DIR}/abi/{address}.json", "r") as file:
                    return json.load(file)
            except json.decoder.JSONDecodeError:
                print('json.decoder.JSONDecodeError!! address:', address)
                time.sleep(.05)
                was_error = True
            except FileNotFoundError:
                break

    while 1:
        try:
            # print(f'making abi request to api.etherscan.io for {address}')
            abi = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={random.choice(API_ETHERSCAN)}", timeout=2).text)
        except ConnectionResetError as e:
            print(f"ConnectionResetError api.etherscan.io {address}")
            time.sleep(5)
            continue
        except requests.exceptions.ReadTimeout as e:
            print(f"ReadTimeout api.etherscan.io {address}")
            time.sleep(5)
            continue
        except requests.exceptions.ConnectTimeout as e:
            print(f"ConnectTimeout api.etherscan.io {address}")
            time.sleep(5)
            continue

        if int(abi['status']) == 0:
            if abi['result'] == 'Max rate limit reached':
                print(f"sleep api.etherscan.io {address}", abi['result'])
                time.sleep(5)
                continue
            elif abi['result'] == 'Max calls per sec rate limit reached':
                print(f"sleep api.etherscan.io {address}", abi['result'])
                time.sleep(5)
                continue
            elif abi['result'] == "Contract source code not verified":
                # Uniswap V3: CHEQ-USDT, с багом последняя в logs операция Swap()
                # https://etherscan.io/tx/0x6861df0ce5c444670b764a56d9c6434ade05518e89068c3922b74200d216a5ed
                # print(f'ABI error for address={address}:', abi)
                pass
            elif abi['result'] == "Invalid API Key":
                print(f'ABI error for address={address}:', abi)
                exit()
            else:
                print(f'ABI error for address={address}:', abi)
                exit()
            abi = {'error': abi['result']}
        else:
            abi = json.loads(abi['result'])

        break

    with open(f"{FILES_DIR}/abi/{address}.json", "w") as file:
        json.dump(abi, file)

    #print(f'ending get_abi() for address={address}')
    return abi



def get_contract_source(address, debug=0):
    address = to_checksum_address(address)

    if not debug:
        was_error = False
        for _ in range(30):
            try:
                if was_error:
                    print('making new attempt get_contract_source() for address:', address)
                with FileLock(f"{FILES_DIR}/contract_source/{address}.json.lock"):
                    with open(f"{FILES_DIR}/contract_source/{address}.json", "r") as file:
                        return json.load(file)
            except json.decoder.JSONDecodeError:
                print('json.decoder.JSONDecodeError!! address:', address)
                time.sleep(.05)
                was_error = True
            except FileNotFoundError:
                break

    while 1:
        try:
            # print(f'making contract source request to api.etherscan.io for {address}')
            abi = json.loads(requests.get(f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={random.choice(API_ETHERSCAN)}").text)
        except ConnectionResetError as e:
            print(f"ConnectionResetError api.etherscan.io {address}")
            time.sleep(3)
            continue


        if int(abi['status']) == 0:

            if abi['result'] == 'Max rate limit reached':
                print(f"sleep api.etherscan.io get_contract_source() {address}")
                time.sleep(5)
                continue

            elif abi['result'] == "Contract source code not verified":
                # Uniswap V3: CHEQ-USDT, с багом последняя в logs операция Swap()
                # https://etherscan.io/tx/0x6861df0ce5c444670b764a56d9c6434ade05518e89068c3922b74200d216a5ed
                # print(f'ABI error for address={address}:', abi)
                pass

            elif abi['result'] == "Invalid API Key":
                print(f'ABI error get_contract_source() for address={address}:', abi)
                exit()
            else:
                print(f'ABI error get_contract_source() for address={address}:', abi)
                exit()
            abi = {'error': abi['result']}
        else:
            # abi = json.loads(abi['result'])
            abi = abi['result']

        break

    with FileLock(f"{FILES_DIR}/contract_source/{address}.json.lock"):
        with open(f"{FILES_DIR}/contract_source/{address}.json", "w") as file:
            json.dump(abi, file)

    return abi


def get_contract_data(w3, contract, block, debug=0):
    """
    file_content содержит значения contract_data, где dict key - это номер блока; хранится только минимальный и максимальные значения блоков
    """

    if contract.address in ['Wei', None]:
        return {'token_name':'Wei','token_symbol':'Wei','token_decimals':18}

    contract_data = {}
    
    try:
        file_content = False
        if debug:
            raise FileNotFoundError
        was_error = False
        for i in range(1):
            try:
                if was_error:
                    # print('making new attempt get_contract_data for address:', contract.address)
                    pass
                with open(f"{FILES_DIR}/contract/{contract.address}.json", "r") as file:
                    file_content = json.load(file)
                    # print(f'found in file (block {block})!', f"{FILES_DIR}/contract/{contract.address}.json", file_content)
                break
            except json.decoder.JSONDecodeError:
                #print('json.decoder.JSONDecodeError!! get_contract_data address:', contract.address)
                #time.sleep(0.15+i*0.035)
                was_error = True
        if file_content == False:
            # if was_error:
            #     print('Так и не получили contract_data из файла')
            raise FileNotFoundError
        else:

            try:
                _keys = list(map(int, file_content.keys()))
                #print('_keys:', _keys)
            except ValueError:
                file_content = False
                # assert 0, f"address={contract.address}, {file_content}"
                print(f"ValueError/AttributeError ошибка в get_contract_data() address={contract.address}, {file_content}")
                raise FileNotFoundError

            if file_content[str(min(_keys))] != {}:
                return file_content[str(min(_keys))]
            if file_content[str(max(_keys))] != {}:
                return file_content[str(max(_keys))]

            # если ранее не получали значение contract_data
            if block < min(_keys):
                raise FileNotFoundError
            # проверка на изменения значений contract_data
            elif block > max(_keys):
                raise FileNotFoundError
            else:
                contract_data = file_content[str(max(_keys))]
                if contract_data == {}:
                    raise FileNotFoundError

    except FileNotFoundError:
        #print('NOT found file', f"{FILES_DIR}/contract/{contract.address}.json")
        try: contract_data['token_name'] = contract.functions.name().call(block_identifier=block)
        except: pass
        try: contract_data['token_symbol'] = contract.functions.symbol().call(block_identifier=block)
        except: pass
        try: contract_data['token_decimals'] = contract.functions.decimals().call(block_identifier=block)
        except: pass


        if (not contract_data or 'token_decimals' not in contract_data) and w3:
            # can be proxy contract (GALA token 0xd1d2Eb1B1e90B638588728b4130137D262C87cae)
            #abi = get_abi(contract.address)
            abi = []
            abi_name, abi_symbol, abi_decimals = [], [], []
            if isinstance(abi, dict) and 'error' in abi: abi = []
            if not abi or abi and 'token_name' not in contract_data:
                abi_name = json.loads('{"constant": true, "inputs": [], "name": "name", "outputs": [{"name": "", "type": "string"}], "payable": false, "stateMutability": "view", "type": "function"}')
            if not abi or abi and 'token_symbol' not in contract_data:
                abi_symbol = json.loads('{"constant": true, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "payable": false, "stateMutability": "view", "type": "function"}')
            if not abi or abi and 'token_decimals' not in contract_data:
                abi_decimals = json.loads('{"constant": true, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": false, "stateMutability": "view", "type": "function"}')
            abi += [abi_name, abi_symbol, abi_decimals]

            contract = w3.eth.contract(address=to_checksum_address(contract.address), abi=abi)
            try: contract_data['token_name'] = contract.functions.name().call(block_identifier=block)
            except: pass
            try: contract_data['token_symbol'] = contract.functions.symbol().call(block_identifier=block)
            except: pass
            try: contract_data['token_decimals'] = contract.functions.decimals().call(block_identifier=block)
            except Exception as e: pass




        if 'token_name' in contract_data and isinstance(contract_data['token_name'], (bytes, bytearray)):

            contract_data['token_name'] = contract_data['token_name'].decode()
        if 'token_symbol' in contract_data and isinstance(contract_data['token_symbol'], (bytes, bytearray)):
            contract_data['token_symbol'] = contract_data['token_symbol'].decode()

        
        dont_save = False
        if file_content != False:
            save_content = {}
            _keys = list(map(int, file_content.keys()))

            _min_block = min(_keys)
            if block < _min_block:
                # for col in ['token_name', 'token_symbol', 'token_decimals']:
                for col in ['token_decimals']:
                    if col in contract_data and col in file_content[str(_min_block)]:
                        if contract_data[col] != file_content[str(_min_block)][col]:
                            # assert 0, f"address={contract.address}, block={block} {col}: {contract_data[col]} != {file_content[str(_min_block)][col]}, {file_content}"
                            print(f" \x1b[31;1m(!)\x1b[0m address={contract.address}, block={block} {col}: {contract_data[col]} != {file_content[str(_min_block)][col]}, {file_content}")
                            dont_save = True
                save_content[block] = contract_data
            else:
                save_content[_min_block] = file_content[str(_min_block)]

            _max_block = max(_keys)
            if block > _max_block:
                # for col in ['token_name', 'token_symbol', 'token_decimals']:
                for col in ['token_decimals']:
                    if col in contract_data and col in file_content[str(_max_block)]:
                        if contract_data[col] != file_content[str(_max_block)][col]:
                            # assert 0, f"address={contract.address}, block={block} {col}: {contract_data[col]} != {file_content[str(_max_block)][col]}, {file_content}"
                            print(f" \x1b[31;1m(!)\x1b[0m address={contract.address}, block={block} {col}: {contract_data[col]} != {file_content[str(_max_block)][col]}, {file_content}")
                            dont_save = True
                save_content[block] = contract_data
            else:
                save_content[_max_block] = file_content[str(_max_block)]

        else:
            save_content = {block: contract_data}


        if not dont_save:
            with FileLock(f"{FILES_DIR}/contract/{contract.address}.json.lock"):
                with open(f"{FILES_DIR}/contract/{contract.address}.json", "w") as file:
                    json.dump(save_content, file)
            


    if 'token_decimals' in contract_data:
        assert type(contract_data['token_decimals']) == int
        if 'token_symbol' not in contract_data and 'token_name' in contract_data: contract_data['token_symbol'] = contract_data['token_name']
        if 'token_symbol' not in contract_data and 'token_name' not in contract_data:
            contract_data['token_symbol'] = contract.address
            contract_data['token_name'] = contract.address
    
    if not 'token_decimals' in contract_data and 'token_symbol' in contract_data:
        contract_data['token_decimals'] = 18
        # print(f"\x1b[31;1m(!)\x1b[0m token_decimals was set 18 by default for address={contract.address}, symbol={contract_data['token_symbol']}, name={contract_data['token_name']}")

    if contract.address == 'Wei':
        contract_data['token_symbol'] = 'Wei'
    
    return contract_data


    
def decode_tuple(t, target_field):
    if type(t) == dict:
        return t
    else:
        output = dict()
        for i in range(len(t)):
            if isinstance(t[i], (bytes, bytearray)):
                output[target_field[i]['name']] = to_hex(t[i])
            elif isinstance(t[i], (tuple)):
                output[target_field[i]['name']] = decode_tuple(t[i], target_field[i]['components'])
            else:
                output[target_field[i]['name']] = t[i]

        return output

def decode_list_tuple(l, target_field):
    output = l
    for i in range(len(l)):
        output[i] = decode_tuple(l[i], target_field)
    return output

def decode_list(l):
    output = l
    for i in range(len(l)):
        if isinstance(l[i], (bytes, bytearray)):
            output[i] = to_hex(l[i])
        else:
            output[i] = l[i]
    return output

def convert_to_hex(arg, target_schema):
    """
    utility function to convert byte codes into human readable and json serializable data structures
    """
    output = dict()
    for k in arg:
        if isinstance(arg[k], (bytes, bytearray)):
            output[k] = to_hex(arg[k])
        elif isinstance(arg[k], (list)) and len(arg[k]) > 0:
            target = [a for a in target_schema if 'name' in a and a['name'] == k][0]
            if target['type'] == 'tuple[]':
                target_field = target['components']
                output[k] = decode_list_tuple(arg[k], target_field)
            else:
                output[k] = decode_list(arg[k])
        elif isinstance(arg[k], (tuple)):
            target_field = [a['components'] for a in target_schema if 'name' in a and a['name'] == k][0]
            output[k] = decode_tuple(arg[k], target_field)
        else:
            output[k] = arg[k]
    return output

def run_query(uri, query, variables):
    request = requests.post(uri, json={'query': query, 'variables': variables}, headers={"Content-Type": "application/json"})
    if request.status_code == 200:
        return request.json()
    else:
        print(request.text)
        raise Exception(f"Unexpected status code returned: {request.status_code}")

def get_eth_price(w3, block, node=None, can_reduce_blocks=True, realtime=False, prefix=None, save=False):
    global chain_contract_geth, chain_contract_erigon

    if type(block) == str:
        block = int(block, 16)
    
    orig_block = block

    CHAIN_ETH_USD = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
    if node in [None, 'erigon']:
        if not chain_contract_erigon:
            chain_contract_erigon = w3.eth.contract(address=CHAIN_ETH_USD, abi=get_abi(CHAIN_ETH_USD))
        chain_contract = chain_contract_erigon
    else:
        if not chain_contract_geth:
            chain_contract_geth = w3.eth.contract(address=CHAIN_ETH_USD, abi=get_abi(CHAIN_ETH_USD))
        chain_contract = chain_contract_geth


    # uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound
    #print(f'calling latestRoundData() on block={block}')

    was_error = False
    error_count = 0
    for ii in range(13):
        try:
            late_ = chain_contract.functions.latestRoundData().call(block_identifier=block)

            # Chainlink USD datafeeds return price data with 8 decimals precision
            # number of wei that 1 USD
            return 10 ** (18 + 8) / late_[1]

        except BadFunctionCallOutput as e:
            error_count += 1
            was_error = True
            if can_reduce_blocks and error_count % 1 == 0:
                block -= 12000
        except ValueError as e:
            error_count += 1
            was_error = True
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{prefix}] \x1b[31;1m(!)\x1b[0m #{error_count} latestRoundData ValueError on node={node} block={block}: {e}")
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{prefix}] #{error_count} latestRoundData ValueError on node={node} block={block}: {e}", file=sys.stderr)
        except Web3RPCError as e:
            error_count += 1
            was_error = True
            if error_count == 13:
                if not realtime:
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{prefix}] \x1b[31;1m(!)\x1b[0m #{error_count} latestRoundData Web3RPCError on node={node} block={block}: {e}")
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{prefix}] #{error_count} latestRoundData Web3RPCError on node={node} block={block}: {e}", file=sys.stderr)
            if can_reduce_blocks and error_count % 2 == 0:
                block -= 1
                if not realtime:
                    print(' уменьшили блок')
                    print(' уменьшили блок', file=sys.stderr)
        except requests.exceptions.ConnectionError as e:
            error_count += 1
            was_error = True
        except:
            error_count += 1
            was_error = True
            if not realtime:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{prefix}] #{error_count} Error exception in get_eth_price() on node={node} block={block}")
                print(traceback.format_exc())
            else:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{prefix}] #{error_count} Error exception in get_eth_price() on node={node} block={block}", file=sys.stderr)
                print(traceback.format_exc(), file=sys.stderr)
            if can_reduce_blocks and error_count % 2 == 0:
                block -= 1
                print(' уменьшили блок')
                print(' уменьшили блок', file=sys.stderr)

    
    return None




def get_eth_price_oracle(block, prefix=None):


    error_count = 0
    while 1:
        try:
            # https://www.reddit.com/r/ethereum/comments/6xbwxp/ethereum_price_history_api/
            # https://www.reddit.com/r/ethereum/comments/6xbwxp/comment/kk86v7y
            # url = "https://api.thegraph.com/subgraphs/name/aave/protocol-v2"
            # query = """query PriceOracleAsset($priceOracleAssetId: ID!, $block: Int) {
            #     priceOracleAsset(id: $priceOracleAssetId, block: {number: $block}) {
            #         oracle {
            #             usdPriceEth
            #         }
            #     }
            # }"""

            # 2-й вариант
            # https://docs.uniswap.org/contracts/v2/reference/API/queries
            # url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
            # query = """{
            #     bundle(id: "1", block: {number: $block}) {
            #         ethPrice
            #     }
            # }"""

            # 3-й вариант
            #url = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"
            url = "https://gateway-arbitrum.network.thegraph.com/api/8e48afc2ef6339eaef9f7b9d8c832087/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
            query = """{
                bundles(block: {number: $block}) {
                    ethPriceUSD
                }
            }"""

            # 4-й вариант
            # How to Calculate the Price of ETH? (Uniswap V3, sqrtPriceX96)
            # https://www.youtube.com/watch?v=9iXkq09_bwc
            # WETH/USDC Uniswap V3 0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8

            # 1-й вариант
            # start_time = time.time()
            # result = run_query(url, query, {
            #     "priceOracleAssetId": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            #     "block": int(block)
            # })
            # print("%.2fs loaded" % (time.time() - start_time))
            # eth_price = int(result['data']['priceOracleAsset']['oracle']['usdPriceEth'])

            # 3-й вариант
            result = run_query(url, query, {
                "block": int(block)
            })
            eth_price = float(result['data']['bundles'][0]['ethPriceUSD'])

            # number of wei that 1 USD
            return 1 / eth_price * 10 ** 18

            
        except KeyError as e:
            error_count += 1
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [{prefix}] get_eth_price_oracle() error:", e)
            break
            # block -= 1
            # if error_count == 2:
            #     break


    return None


def get_token_pair(w3, address, exchange, fee, token2_address, block):
    """
    :param fee: 3000 = 0.3%, 10000, 500
    :param token2_address: Can be USDT, USDC, WETH
    """

    pair_address = None

    start_time = time.time()
    if exchange == 'V2':
        uni_factory_v2 = w3.eth.contract(address=UNI_V2_FACTORY, abi=get_abi(UNI_V2_FACTORY))
        pair_address = uni_factory_v2.functions.getPair(to_checksum_address(address), token2_address).call(block_identifier=block)

    if exchange == 'V3':
        uni_factory_v3 = w3.eth.contract(address=UNI_V3_FACTORY, abi=get_abi(UNI_V3_FACTORY))
        try:
            pair_address = uni_factory_v3.functions.getPool(to_checksum_address(address), token2_address, fee).call(block_identifier=block)
        except Web3ValidationError as e:
            print(f'\x1b[31;1m(!)\x1b[0m get_token_pair() V3 Web3ValidationError для address={pair_address}', e)
            pair_address = None
            
    if pair_address in [ADDRESS_0X0000, ADDRESS_0XDEAD]:
        pair_address = None


    return pair_address



def usd_per_one_token(w3, address, block, pair_address=None, token1_currency=None, exchange=None, involved_addresses=[], tx_eth_price=None, save=False):
    """
    :param involved_addresses: список задействованных адресов; нужно, чтобы определить какую пару искать на какой бирже (Uniswap v2/v3)
    """

    pair_address_list = {'V2_WETH': None, 'V2_USDC': None, 'V2_USDT': None, 'V3_WETH': None, 'V3_USDC': None, 'V3_USDT': None}

    # check on V2
    if pair_address == None:
        token1_currency = 'WETH'
        pair_address = get_token_pair(w3, address, 'V2', None, WETH, block=block)
        pair_address_list['V2_WETH'] = pair_address
        if pair_address and involved_addresses and pair_address not in involved_addresses:
            # print(f"pair_address найден, но его нет в involved_addresses")
            pair_address = None
        if not pair_address:
            token1_currency = 'USDC'
            pair_address = get_token_pair(w3, address, 'V2', None, USDC, block=block)
            pair_address_list['V2_USDC'] = pair_address
            if pair_address and involved_addresses and pair_address not in involved_addresses:
                pair_address = None
        if not pair_address:
            token1_currency = 'USDT'
            pair_address = get_token_pair(w3, address, 'V2', None, USDT, block=block)
            pair_address_list['V2_USDT'] = pair_address
            if pair_address and involved_addresses and pair_address not in involved_addresses:
                pair_address = None
        if not pair_address:
            pass
            # print(f'Так и не нашли пару V2 для address={address}')

    if pair_address and (not exchange or exchange == 'V2'):
        # print(f"На V2 для address={address} pair={pair_address} с {token1_currency} block={block} ({'найдено с involved_addresses' if involved_addresses else 'найдено без involved_addresses'})")
        # token_contract = web3.eth.contract(address=token_address, abi=get_abi(token_address))
        # token_dec = token_contract.functions.decimals().call(block_identifier=block)
        contract_data = get_contract_data(w3, contract=AttributeDict({'address':address}), block=block)

        try:
            if 'token_decimals' not in contract_data:
                raise BadFunctionCallOutput

            token_dec = contract_data['token_decimals']

            try:
                abi_reserves_token_0_1 = json.loads('[{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}]')
                pair_contract = w3.eth.contract(address=pair_address, abi=abi_reserves_token_0_1)
            except ValueError as e:
                print(f'\x1b[31;1m(!)\x1b[0m w3.eth.contract() V2 ValueError для address={address}, pair={pair_address}, block={block}')
                raise BadFunctionCallOutput

            try:
                reserves = pair_contract.functions.getReserves().call(block_identifier=block)
                token0 = pair_contract.functions.token0().call(block_identifier=block)
                token1 = pair_contract.functions.token1().call(block_identifier=block)
            except ContractLogicError as e:
                if 'no data' in str(e):
                    pass
                else:
                    print(traceback.format_exc())
                    print(f'\x1b[31;1m(!)\x1b[0m w3.eth.contract() V2 ContractLogicError on reserves, address={address}, pair={pair_address}, block={block}', e)
                raise BadFunctionCallOutput

            if to_checksum_address(token0) == to_checksum_address(address):
                reserves_token = reserves[0]
                reserves_pair = reserves[1]
            else:
                reserves_token = reserves[1]
                reserves_pair = reserves[0]

            token_price_usd = None

            # print(f"V2 token1_currency={token1_currency}")

            if token1_currency == 'WETH':
                if tx_eth_price == None:
                    tx_eth_price = get_eth_price(w3, block)
            for _ in range(2):
                if reserves_token != 0:
                    if token1_currency == 'WETH':
                        token_price_usd = (reserves_pair / tx_eth_price) / (reserves_token / 10 ** token_dec)
                    elif token1_currency in ['USDT', 'USDC']:
                        token_price_usd = (reserves_pair / 10 ** 6) / (reserves_token / 10 ** token_dec)
                
                # (!) bug with reserves
                if (not token_price_usd or token_price_usd > 100000) and reserves_pair != 0: # $
                    # print(f'\x1b[31;1m(!)\x1b[0m V2 баг с reserves для address={address}, pair={pair_address}, block={block}, token1_currency={token1_currency}, token_price_usd={token_price_usd}')
                    if to_checksum_address(token0) == to_checksum_address(address):
                        reserves_token = reserves[1]
                        reserves_pair = reserves[0]
                    else:
                        reserves_token = reserves[0]
                        reserves_pair = reserves[1] 
                else:
                    break
            

            if not token_price_usd:
                raise BadFunctionCallOutput

            if token_price_usd > 100000: # $
                print(f'\x1b[31;1m(!)\x1b[0m V2 баг с reserves ЗАМЕНА не помогла для address={address}, pair={pair_address}, block={block}, token1_currency={token1_currency}, token_price_usd={token_price_usd}')
            

            # print(f"reserves token: {reserves_token / 10 ** token_dec} {address}, {round(reserves_token / 10 ** token_dec * token_price_usd, 2)} $")
            # print(f"reserves token: {reserves_token} {address}, reserves pair: {reserves_pair}")
            if token1_currency == 'WETH':
                reserves_pair_usd = reserves_pair / tx_eth_price
                # print(f"reserves pair: {reserves_pair / 10 ** 18} WETH, {round(reserves_pair / tx_eth_price, 2)} $")
            elif token1_currency in ['USDT', 'USDC']:
                # print(f"reserves pair: {reserves_pair / 10 ** 6} USD")
                reserves_pair_usd = reserves_pair / 10 ** 6
            



            return {'value_usd_per_token_unit':token_price_usd, 'reserves_token':reserves_token, 'reserves_token_usd':reserves_token / 10 ** token_dec * token_price_usd, 'reserves_pair':reserves_pair, 'reserves_pair_usd':reserves_pair_usd, 'pair_address':pair_address, 'token1_currency':token1_currency, 'exchange':'V2', 'token_dec': token_dec}

        except BadFunctionCallOutput as e:
            pass


    # check on V3
    if pair_address == None:
        # fee судя по всему может быть любым
        # на token1_currency он находит pair_address при всех значениях (3000, 10000, 500)
        token1_currency = 'WETH'
        fee = 3000
        pair_address = get_token_pair(w3, address, 'V3', fee, WETH, block=block)
        pair_address_list['V3_WETH'] = pair_address
        if pair_address and involved_addresses and pair_address not in involved_addresses:
            pair_address = None
        if not pair_address:
            token1_currency = 'USDC'
            fee = 3000
            pair_address = get_token_pair(w3, address, 'V3', fee, USDC, block=block)
            pair_address_list['V3_USDC'] = pair_address
            if pair_address and involved_addresses and pair_address not in involved_addresses:
                pair_address = None
        if not pair_address:
            token1_currency = 'USDT'
            fee = 3000
            pair_address = get_token_pair(w3, address, 'V3', fee, USDT, block=block)
            pair_address_list['V3_USDT'] = pair_address
            if pair_address and involved_addresses and pair_address not in involved_addresses:
                pair_address = None
        if not pair_address:
            pass
            # if not involved_addresses:
            #     print(f'\x1b[31;1m(!)\x1b[0m Так и не нашли пару V3 для address={address}')

    if pair_address and (not exchange or exchange == 'V3'):
        # print(f"На V3 для address={address} pair={pair_address} с {token1_currency} ({'найдено с involved_addresses' if involved_addresses else 'найдено без involved_addresses'})")
        contract_data = get_contract_data(w3, contract=AttributeDict({'address':address}), block=block)

        try:
            if 'token_decimals' not in contract_data:
                raise BadFunctionCallOutput

            token_dec = contract_data['token_decimals']

            try:
                abi_slot0 = json.loads('[{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"}]')
                pair_contract = w3.eth.contract(address=pair_address, abi=abi_slot0)
            except ValueError as e:
                print(f'\x1b[31;1m(!)\x1b[0m w3.eth.contract() V3 ValueError для address={address}, pair={pair_address}, block={block}')
                raise BadFunctionCallOutput

            #except Exception as e:
            # abi_slot = json.loads('[{"inputs": [], "name": "slot0", "type": "function", "outputs": [{"internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"}, {"internalType": "int24", "name": "tick", "type": "int24"}, {"internalType": "uint16", "name": "observationIndex", "type": "uint16"}, {"internalType": "uint16", "name": "observationCardinality", "type": "uint16"}, {"internalType": "uint16", "name": "observationCardinalityNext", "type": "uint16"}, {"internalType": "uint8", "name": "feeProtocol", "type": "uint8"}, {"internalType": "bool", "name": "unlocked", "type": "bool"}]}]')
            # pair_contract = w3.eth.contract(address=pair_address, abi=abi_slot)
            
            try:
                slot0 = pair_contract.functions.slot0().call(block_identifier=block)
            except ContractLogicError as e:
                if 'no data' in str(e):
                    pass
                else:
                    print(traceback.format_exc())
                    print(f'\x1b[31;1m(!)\x1b[0m w3.eth.contract() V3 ContractLogicError on slot0(), address={address}, pair={pair_address}, block={block}', e)
                raise BadFunctionCallOutput
            sqrtPriceX96 = slot0[0]



            # print(f"V3 token1_currency={token1_currency}")

            if token1_currency in ['WETH']:
                buyOneOfToken0 = ((sqrtPriceX96 / 2**96)**2) / (10**(18 - token_dec)) # WETH
                if tx_eth_price == None:
                    tx_eth_price = get_eth_price(w3, block)
                token_price_usd = w3.to_wei(buyOneOfToken0, 'ether') / tx_eth_price
            elif token1_currency in ['USDC', 'USDT']:
                buyOneOfToken0 = ((sqrtPriceX96 / 2**96)**2) / (10**(6 - token_dec)) # USDC/USDT
                token_price_usd = buyOneOfToken0

            # (!) bug fix
            if token_price_usd > 100000: # $
                # print(f'\x1b[31;1m(!)\x1b[0m V3 баг с reserves для address={address}, pair={pair_address}, token1_currency={token1_currency}, block={block}, token_price_usd={token_price_usd}')
                buyOneOfToken1 = 1 / buyOneOfToken0
                if token1_currency in ['WETH']:
                    token_price_usd = w3.to_wei(buyOneOfToken1, 'ether') / tx_eth_price
                elif token1_currency in ['USDC', 'USDT']:
                    token_price_usd = buyOneOfToken1
        
            if token_price_usd > 100000: # $
                print(f'\x1b[31;1m(!)\x1b[0m V3 баг с reserves ЗАМЕНА не помогла для address={address}, pair={pair_address}, token1_currency={token1_currency}, block={block}, token_price_usd={token_price_usd}')
            

            reserves_token = contract_balanceOf(w3, token_address=address, wallet_address=pair_address, block_identifier=block)
            if token1_currency == 'WETH':
                reserves_pair = contract_balanceOf(w3, token_address=WETH, wallet_address=pair_address, block_identifier=block)
            elif token1_currency == 'USDC':
                reserves_pair = contract_balanceOf(w3, token_address=USDC, wallet_address=pair_address, block_identifier=block)
            if token1_currency == 'USDT':
                reserves_pair = contract_balanceOf(w3, token_address=USDT, wallet_address=pair_address, block_identifier=block)
            
            if reserves_token == None:
                print(f'\x1b[31;1m(!)\x1b[0m w3.eth.contract() V3 почему-то reserves_token=None, address={address}, pair={pair_address}, block={block}')
                raise BadFunctionCallOutput

            # print(f"reserves token: {reserves_token / 10 ** token_dec} {address}, {round(reserves_token / 10 ** token_dec * token_price_usd, 2)} $")
            if token1_currency == 'WETH':
                reserves_pair_usd = reserves_pair / tx_eth_price
                # print(f"reserves pair: {reserves_pair / 10 ** 18} WETH, {round(reserves_pair / tx_eth_price, 2)} $")
            elif token1_currency in ['USDT', 'USDC']:
                # print(f"reserves pair: {reserves_pair / 10 ** 6} USD")
                reserves_pair_usd = reserves_pair / 10 ** 6

            return {'value_usd_per_token_unit':token_price_usd, 'reserves_token':reserves_token, 'reserves_token_usd':reserves_token / 10 ** token_dec * token_price_usd, 'reserves_pair':reserves_pair, 'reserves_pair_usd':reserves_pair_usd, 'pair_address':pair_address, 'token1_currency':token1_currency, 'exchange':'V3', 'token_dec': token_dec}

        except BadFunctionCallOutput as e:
            pass 


    if involved_addresses:
        _pair_address = None
        _exchange = None
        if pair_address_list['V2_WETH']:
            _pair_address = pair_address_list['V2_WETH']
            _exchange = 'V2'
        elif pair_address_list['V2_USDC']:
            _pair_address = pair_address_list['V2_USDC']
            _exchange = 'V2'
        elif pair_address_list['V2_USDT']:
            _pair_address = pair_address_list['V2_USDT']
            _exchange = 'V2'
        elif pair_address_list['V3_WETH']:
            _pair_address = pair_address_list['V3_WETH']
            _exchange = 'V3'
        elif pair_address_list['V3_USDC']:
            _pair_address = pair_address_list['V3_USDC']
            _exchange = 'V3'
        elif pair_address_list['V3_USDT']:
            _pair_address = pair_address_list['V3_USDT']
            _exchange = 'V3'
        if _pair_address:
            return usd_per_one_token(w3, address, block, pair_address=_pair_address, exchange=_exchange)


    # (!) здесь стоит exit() поставить
    # exit()

    return {'value_usd_per_token_unit':None, 'reserves_token':None, 'reserves_token_usd':None, 'reserves_pair':None, 'reserves_pair_usd':None, 'pair_address':None, 'token1_currency':None, 'exchange':None}



def decode_log_contract(w3, contract, abi, log):
    
    start_time = time.time()
    logs = []
    event_abi = [a for a in contract.abi if a["type"] == "event"]
    topic2abi = {event_abi_to_log_topic(_): _ for _ in event_abi}
    log_ = {
        'address': None, #Web3.toChecksumAddress(address),
        'blockHash': None, #HexBytes(blockHash),
        'blockNumber': None,
        'data': log['data'], 
        'logIndex': None,
        'topics': [HexBytes(_) for _ in log["topics"]],
        'transactionHash': None, #HexBytes(transactionHash),
        'transactionIndex': None
    }
    try:
        if HexBytes(log['topics'][0]) not in topic2abi and HexBytes(log['topics'][0]) == HexBytes('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'):
            # bug that not all ABI has Transfer() topic with hex=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
            # https://www.reddit.com/r/ethdev/comments/1b9xfc1/etherscans_abi_and_transaction_receipt_topic0_are/
            abi_0xddf2_b3ef = json.loads('{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "from", "type": "address"}, {"indexed": true, "internalType": "address", "name": "to", "type": "address"}, {"indexed": false, "internalType": "uint256", "name": "value", "type": "uint256"}], "name": "Transfer", "type": "event"}')
            abi.append(abi_0xddf2_b3ef)
            contract = w3.eth.contract(address=to_checksum_address(log["address"]), abi=abi)
            topic2abi[HexBytes('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')] = abi_0xddf2_b3ef
        
        # erc721
        if log['data'] == "0x" and HexBytes(log['topics'][0]) == HexBytes('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'):
            abi_transfer_erc721 = json.loads('{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"id","type":"uint256"}],"name":"Transfer","type":"event"}')
            abi.append(abi_transfer_erc721)
            contract = w3.eth.contract(address=to_checksum_address(log["address"]), abi=abi)
            topic2abi[HexBytes('0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')] = abi_transfer_erc721

        event_abi = topic2abi[HexBytes(log['topics'][0])]
    except KeyError as e:
        logs.append(f"      \x1b[31;1m(!)\x1b[0m topic not found in abi")
        logs.append(f"      topic[0]={log['topics'][0]}")
        return {}, None, logs

    evt_name = event_abi['name']

    try:
        data = get_event_data(w3.codec, event_abi, log_)['args']
        target_schema = event_abi['inputs']
        decoded_data = convert_to_hex(data, target_schema)
    except Exception as e:
        print(f"      \x1b[31;1m(!)\x1b[0m decode error on log: {e}")
        logs.append(f"      \x1b[31;1m(!)\x1b[0m decode error on log: {e}")
        decoded_data = {}

        # https://henrytirla.medium.com/how-to-decode-transactions-data-logs-with-eth-abi-f2bfaa4b34d7
        # DATA = bytes('0xE7E65068aAb67F30F8ae7627B79c131Db36196c8' + '0xe20E337DB2a00b1C37139c873B92a0AAd3F468bF' + log['data'], encoding='utf8')
        # decodedABI = abi_eth_lib.decode(['address', 'address', 'uint256', 'bytes'], DATA)
        # print(decodedABI)
    
    # print("%.3fs decoded_data()" % (time.time() - start_time))
        
    
    return decoded_data, evt_name, logs



def save_log(msg, address=None, stdout=True):

    if stdout:
        print(msg)


def save_log_strg(msg, address, block, stdout=False):

    if stdout:
        print(msg)

    _path = f"{FILES_DIR}/lib/logs_strg"
    with open(f"{_path}/{address.lower()}.log", "a+") as file:
        file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} [b{block}] {str(msg)}\n")


def log_strategy_trades(msg):
    with FileLock(f"{FILES_DIR}/lib/logs/strategy_trades.log.lock"):
        with open(f"{FILES_DIR}/lib/logs/strategy_trades.log", "a+") as file:
            file.write(msg + '\n')



def http_request(action, address):

    try:

        if action == 'ether':
            return check_contract_verified(address)

        if action == 'gopluslabs':
            return check_gopluslabs(address)

        if action == 'honeypotis':
            return check_honeypot(address)
    
    except:
        print(traceback.format_exc(), file=sys.stderr)
        return None



def check_gopluslabs(address, debug=1):

    # https://gopluslabs.io/token-security/1/{address}

    try:
        if debug == 1:
            raise FileNotFoundError
        with open(f"{FILES_DIR}/temp/check_gopluslabs/{address}.json", "r") as file:
            res = json.load(file)
    except FileNotFoundError:
        try:
            res = requests.get(f"https://api.gopluslabs.io/api/v1/token_security/1?contract_addresses={address}", timeout=3).json()
            if debug == 0:
                with open(f"{FILES_DIR}/temp/check_gopluslabs/{address}.json", "w") as file:
                    json.dump(res, file)
        except requests.exceptions.Timeout:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} requests.exceptions.Timeout check_gopluslabs()")
            return {'error': 'http_timeout'}
        except requests.exceptions.ConnectionError:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} requests.exceptions.ConnectionError check_gopluslabs()")
            return {'error': 'http_connectionerror'}
        except requests.exceptions.SSLError:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} requests.exceptions.SSLError check_gopluslabs()")
            return {'error': 'ssl_error'}


    # pprint(res)

    if res['code'] != 1:
        return res

    if address.lower() not in res['result']:
        print(f"address={address} not in check_gopluslabs() results!: {res}")
        return res

    res = res['result'][address.lower()]
    res['buy_tax'] = float(res['buy_tax']) if 'buy_tax' in res and res['buy_tax'] != '' else None
    res['cannot_buy'] = int(res['cannot_buy']) if 'cannot_buy' in res else None
    res['cannot_sell_all'] = int(res['cannot_sell_all']) if 'cannot_sell_all' in res else None
    res['creator_balance'] = float(res['creator_balance']) if 'creator_balance' in res else None
    res['creator_percent'] = float(res['creator_percent']) if 'creator_percent' in res else None
    res['honeypot_with_same_creator'] = int(res['honeypot_with_same_creator']) if 'honeypot_with_same_creator' in res else None
    res['is_in_dex'] = int(res['is_in_dex']) if 'is_in_dex' in res else None
    res['is_open_source'] = int(res['is_open_source']) if 'is_open_source' in res else None
    res['is_proxy'] = int(res['is_proxy']) if 'is_proxy' in res else None
    res['sell_tax'] = float(res['sell_tax']) if 'sell_tax' in res and res['sell_tax'] != '' else None
    res['anti_whale_modifiable'] = int(res['anti_whale_modifiable']) if 'anti_whale_modifiable' in res else None
    res['can_take_back_ownership'] = int(res['can_take_back_ownership']) if 'can_take_back_ownership' in res else None
    res['is_anti_whale'] = int(res['is_anti_whale']) if 'is_anti_whale' in res else None
    res['is_blacklisted'] = int(res['is_blacklisted']) if 'is_blacklisted' in res else None
    res['is_honeypot'] = int(res['is_honeypot']) if 'is_honeypot' in res else None
    res['is_mintable'] = int(res['is_mintable']) if 'is_mintable' in res else None
    res['is_whitelisted'] = int(res['is_whitelisted']) if 'is_whitelisted' in res else None
    res['owner_address'] = res['owner_address'] if 'owner_address' in res else None
    res['owner_balance'] = float(res['owner_balance']) if 'owner_balance' in res else None
    res['owner_change_balance'] = int(res['owner_change_balance']) if 'owner_change_balance' in res else None
    res['owner_percent'] = float(res['owner_percent']) if 'owner_percent' in res else None 
    res['personal_slippage_modifiable'] = int(res['personal_slippage_modifiable']) if 'personal_slippage_modifiable' in res else None
    res['selfdestruct'] = int(res['selfdestruct']) if 'selfdestruct' in res else None
    res['slippage_modifiable'] = int(res['slippage_modifiable']) if 'slippage_modifiable' in res else None
    res['trading_cooldown'] = int(res['trading_cooldown']) if 'trading_cooldown' in res else None
    res['transfer_pausable'] = int(res['transfer_pausable']) if 'transfer_pausable' in res else None
    res['hidden_owner'] = int(res['hidden_owner']) if 'hidden_owner' in res else None
    res['external_call'] = int(res['external_call']) if 'external_call' in res else None
    res['gas_abuse'] = int(res['gas_abuse']) if 'gas_abuse' in res else None


    danger = []
    warning = []
    info = []
    if res['buy_tax'] == None:
        warning.append(f"buy_tax=None")
    if res['buy_tax'] != None:
        if res['buy_tax'] * 100 > 30:
            danger.append(f"buy_tax={res['buy_tax']*100}>30")
        elif 0 < res['buy_tax'] * 100 <= 30:
            warning.append(f"buy_tax={res['buy_tax']*100}<=30")
    if res['cannot_buy'] != None and res['cannot_buy'] != 0:
        danger.append(f"cannot_buy={res['cannot_buy']}")
    if res['cannot_sell_all'] != None and res['cannot_sell_all'] != 0:
        danger.append(f"cannot_sell_all={res['cannot_sell_all']}")
    if res['creator_balance'] != 0.0:
        warning.append(f"creator_balance={res['creator_balance']}")
    if res['creator_percent'] != 0:
        warning.append(f"creator_percent={res['creator_percent']}")
    if res['honeypot_with_same_creator'] != None and res['honeypot_with_same_creator'] != 0:
        danger.append(f"honeypot_with_same_creator={res['honeypot_with_same_creator']}")
    if res['is_in_dex'] != None and res['is_in_dex'] != 1:
        warning.append(f"is_in_dex={res['is_in_dex']}")
    if res['is_open_source'] != None and res['is_open_source'] != 1:
        warning.append(f"is_open_source={res['is_open_source']}")
    if res['is_proxy'] != None and res['is_proxy'] != 0:
        danger.append(f"is_proxy={res['is_proxy']}")
    if res['sell_tax'] == None:
        warning.append(f"sell_tax=None")
    if res['sell_tax'] != None:
        if res['sell_tax'] * 100 > 30:
            danger.append(f"sell_tax={res['sell_tax']*100}>30")
        elif 0 < res['sell_tax'] * 100 <= 30:
            warning.append(f"sell_tax={res['sell_tax']*100}<=30")
    if res['anti_whale_modifiable'] != None and res['anti_whale_modifiable'] != 0:
        warning.append(f"anti_whale_modifiable={res['anti_whale_modifiable']}")
    if res['can_take_back_ownership'] != None and res['can_take_back_ownership'] != 0:
        danger.append(f"can_take_back_ownership={res['can_take_back_ownership']}")
    if res['is_anti_whale'] != None and res['is_anti_whale'] != 0:
        warning.append(f"is_anti_whale={res['is_anti_whale']}")
    if res['is_blacklisted'] != None and res['is_blacklisted'] != 0:
        warning.append(f"is_blacklisted={res['is_blacklisted']}")
    if res['is_honeypot'] != None and res['is_honeypot'] != 0:
        danger.append(f"is_honeypot={res['is_honeypot']}")
    if res['is_mintable'] != None and res['is_mintable'] != 0:
        danger.append(f"is_mintable={res['is_mintable']}")
    if res['is_whitelisted'] != None and res['is_whitelisted'] != 0:
        warning.append(f"is_whitelisted={res['is_whitelisted']}")
    if res['owner_address'] != None and res['owner_address'].lower() not in ['', '0x000000000000000000000000000000000000dead', '0x0000000000000000000000000000000000000000']:
        info.append(f"owner_address={res['owner_address']}")
    if res['owner_balance'] != None and res['owner_balance'] != 0.0:
        info.append(f"owner_balance={res['owner_balance']}")
    if res['owner_percent'] != None and res['owner_percent'] != 0.0:
        info.append(f"owner_percent={res['owner_percent']}")
    if res['owner_change_balance'] != None and res['owner_change_balance'] != 0:
        danger.append(f"owner_change_balance={res['owner_change_balance']}")
    if res['personal_slippage_modifiable'] != None and res['personal_slippage_modifiable'] != 0:
        danger.append(f"personal_slippage_modifiable={res['personal_slippage_modifiable']}")
    if res['selfdestruct'] != None and res['selfdestruct'] != 0:
        danger.append(f"selfdestruct={res['selfdestruct']}")
    if res['slippage_modifiable'] != None and res['slippage_modifiable'] != 0:
        warning.append(f"slippage_modifiable={res['slippage_modifiable']}")
    if res['trading_cooldown'] != None and res['trading_cooldown'] != 0:
        warning.append(f"trading_cooldown={res['trading_cooldown']}")
    if res['transfer_pausable'] != None and res['transfer_pausable'] != 0:
        warning.append(f"transfer_pausable={res['transfer_pausable']}")
    if res['hidden_owner'] != None and res['hidden_owner'] != 0:
        warning.append(f"hidden_owner={res['hidden_owner']}")
    if res['external_call'] != None and res['external_call'] != 0:
        warning.append(f"external_call={res['external_call']}")
    if res['gas_abuse'] != None and res['gas_abuse'] != 0:
        danger.append(f"gas_abuse={res['gas_abuse']}")
    
    

    return {'scam': True if danger else False, 'danger': danger, 'warning': warning, 'info': info}


def check_honeypot(address, debug=1):

    try:
        if debug == 1:
            raise FileNotFoundError
        with open(f"{FILES_DIR}/temp/check_honeypot/{address}.json", "r") as file:
            res = json.load(file)
    except FileNotFoundError:
        try:
            res = requests.get(f"https://api.honeypot.is/v2/IsHoneypot?address={address}", timeout=3).json()
            if debug == 0:
                with open(f"{FILES_DIR}/temp/check_honeypot/{address}.json", "w") as file:
                    json.dump(res, file)
        except requests.exceptions.Timeout:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} requests.exceptions.Timeout check_honeypot()")
            return {'error': 'http_timeout'}
        except requests.exceptions.ConnectionError:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} requests.exceptions.ConnectionError check_honeypot()")
            return {'error': 'http_connectionerror'}
        except requests.exceptions.SSLError:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} requests.exceptions.SSLError check_honeypot()")
            return {'error': 'ssl_error'}


    if 'error' in res:
        return {'scam': True, 'danger': [res['error']], 'warning': []}

    # pprint(res)

    danger = []
    warning = []
    info = []
    if 'honeypotResult' in res and res['honeypotResult']['isHoneypot']:
        danger.append(f"honeypotResult={res['honeypotResult']['isHoneypot']}")
    if res['simulationSuccess'] == False:
        if 'honeypotResult' in res:
            if res['honeypotResult']['isHoneypot']:
                assert res['honeypotResult']['isHoneypot']
        else:
            danger.append(f"simulationSuccess={res['simulationSuccess']}")
    if res['contractCode']:
        if res['contractCode']['hasProxyCalls']:
            danger.append(f"hasProxyCalls={res['contractCode']['hasProxyCalls']}")
        if res['contractCode']['isProxy']:
            danger.append(f"isProxy={res['contractCode']['isProxy']}")
        if not res['contractCode']['openSource']:
            warning.append(f"openSource={res['contractCode']['openSource']}")
        if not res['contractCode']['rootOpenSource']:
            warning.append(f"rootOpenSource={res['contractCode']['rootOpenSource']}")
    if 'simulationResult' in res:
        if res['simulationResult']['buyTax'] > 30:
            danger.append(f"buyTax={res['simulationResult']['buyTax']}>30")
        elif 0 < res['simulationResult']['buyTax'] <= 30:
            warning.append(f"buyTax={res['simulationResult']['buyTax']}<=30")
        if res['simulationResult']['sellTax'] > 42:
            danger.append(f"sellTax={res['simulationResult']['sellTax']}>42")
        elif 0 < res['simulationResult']['sellTax'] <= 30:
            warning.append(f"sellTax={res['simulationResult']['sellTax']}<=30")
        if res['simulationResult']['transferTax'] > 30:
            warning.append(f"transferTax={res['simulationResult']['transferTax']}>30")
        elif 0 < res['simulationResult']['transferTax'] <= 30:
            warning.append(f"transferTax={res['simulationResult']['transferTax']}<=30")

    for flag in res['summary']['flags']:
        # severity: info, high, medium, low, critical
        if flag['severity'] == 'info':
            info.append(f"{flag['flag']} ({flag['severity']})")
        elif flag['severity'] in ['medium', 'low'] or flag['flag'] == 'closed_source' or flag['flag'] == 'No pairs found':
            warning.append(f"{flag['flag']} ({flag['severity']})")
        else:
            danger.append(f"{flag['flag']} ({flag['severity']})")

    if danger == ['No pairs found']:
        warning.append('No pairs found')

    return {'scam': True if danger else False, 'danger': danger, 'warning': warning, 'info': info}


def check_contract_verified(address):
    errors = 0
    while 1:
        if errors == 7: return None

        key_ = random.choice(API_ETHERSCAN)
        try:
            resp_ = requests.get(f"https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={key_}").text
            abi = json.loads(resp_)
        except ConnectionResetError as e:
            print(f"ConnectionResetError api.etherscan.io")
            time.sleep(0.3)
            errors += 1
            continue
        except json.decoder.JSONDecodeError as e:
            print(f"JSONDecodeError: https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={key_}", file=sys.stderr)
            print(resp_, file=sys.stderr)
            time.sleep(0.3)
            errors += 1
            continue


        if int(abi['status']) == 0:
            if abi['result'] == 'Max rate limit reached':
                print(f"sleep api.etherscan.io # max rate")
                time.sleep(0.3)
                errors += 1
                continue
            elif abi['result'] == "Contract source code not verified":
                contract_verified = False
            else:
                print(f'ABI error:', abi)
                contract_verified = None
        else:
            contract_verified = True

        break
    
    return contract_verified


def get_whois(site):

    site = site.lower()

    try:
        with open(f"{FILES_DIR}/whois/{site}.data", "rb") as file:
            return pickle.load(file)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f'Exception {e}, site', site)

    ns_ = []
    try:
        answers = dns.resolver.resolve(site, 'NS')
        for server in answers:
            ns_.append(server.target.to_text().lower())
            # print('NS', server.target, socket.gethostbyname(str(server.target)))
    except Exception as e:
        pass

    a_ = []
    try:
        answers = dns.resolver.resolve(site, 'A')
        for server in answers:
            # print('A', server.to_text())
            a_.append(server.to_text())
    except Exception as e:
        pass

    # answers = dns.resolver.resolve(site, 'AAAA')
    # for server in answers:
    #     print('AAAA', server.to_text())

    mx_ = []
    try:
        answers = dns.resolver.resolve(site, 'MX')
        for server in answers:
            # print('MX', server)
            mx_.append(server.to_text())
    except Exception as e:
        # print('Error MX:', e)
        pass

    # result = dns.resolver.resolve(site, 'TXT')
    # for val in result:
    #     print('TXT Record : ', val.to_text())

    # result = dns.resolver.resolve(site, 'CNAME')
    # for cnameval in result:
    #     print(' cname target address:', cnameval.target)

    # result = dns.resolver.resolve('mail.google.com', 'MX')
    # for exdata in result:
    #     print(' MX Record:', exdata.exchange.text())

    dm_info = {}
    try:
        dm_info =  whois.whois(site)
    except Exception as e:
        pass


    _result = {'ns': ns_, 'a': a_, 'mx': mx_, 'whois': dm_info}

    with open(f"{FILES_DIR}/whois/{site}.data", "wb") as file:
        pickle.dump(_result, file)

    return _result



def get_stat_wallets_address(w3, address, last_block, N=2000, token_address=None, verbose=0):
    """
        token_address - на который идет подсчет LastTrades
    """
    """
        Avg min between sent tnx: Average time between sent transactions for account in minutes
        Avg_min_between_received_tnx: Average time between received transactions for account in minutes
        + Time_Diff_between_first_and_last(Mins): Time difference between the first and last transaction
        + Sent_tnx: Total number of sent normal transactions
        + Received_tnx: Total number of received normal transactions
        Number_of_Created_Contracts: Total Number of created contract transactions
        + Unique_Received_From_Addresses: Total Unique addresses from which account received transactions
        + Unique_Sent_To_Addresses20: Total Unique addresses from which account sent transactions
        + Min_Value_Received: Minimum value in Ether ever received
        + Max_Value_Received: Maximum value in Ether ever received
        + Avg_Value_Received5Average value in Ether ever received
        + Min_Val_Sent: Minimum value of Ether ever sent
        + Max_Val_Sent: Maximum value of Ether ever sent
        + Avg_Val_Sent: Average value of Ether ever sent
        Min_Value_Sent_To_Contract: Minimum value of Ether sent to a contract
        Max_Value_Sent_To_Contract: Maximum value of Ether sent to a contract
        Avg_Value_Sent_To_Contract: Average value of Ether sent to contracts
        Total_Transactions(Including_Tnx_to_Create_Contract): Total number of transactions
        + Total_Ether_Sent:Total Ether sent for account address
        + Total_Ether_Received: Total Ether received for account address
        Total_Ether_Sent_Contracts: Total Ether sent to Contract addresses
        Total_Ether_Balance: Total Ether Balance following enacted transactions
        + Total_ERC20_Tnxs: Total number of ERC20 token transfer transactions
        + ERC20_Total_Ether_Received: Total ERC20 token received transactions in Ether
        + ERC20_Total_Ether_Sent: Total ERC20token sent transactions in Ether
        ERC20_Total_Ether_Sent_Contract: Total ERC20 token transfer to other contracts in Ether
        + ERC20_Uniq_Sent_Addr: Number of ERC20 token transactions sent to Unique account addresses
        + ERC20_Uniq_Rec_Addr: Number of ERC20 token transactions received from Unique addresses
        ERC20_Uniq_Rec_Contract_Addr: Number of ERC20token transactions received from Unique contract addresses
        ERC20_Avg_Time_Between_Sent_Tnx: Average time between ERC20 token sent transactions in minutes
        ERC20_Avg_Time_Between_Rec_Tnx: Average time between ERC20 token received transactions in minutes
        ERC20_Avg_Time_Between_Contract_Tnx: Average time ERC20 token between sent token transactions
        + ERC20_Min_Val_Rec: Minimum value in Ether received from ERC20 token transactions for account
        + ERC20_Max_Val_Rec: Maximum value in Ether received from ERC20 token transactions for account
        + ERC20_Avg_Val_Rec: Average value in Ether received from ERC20 token transactions for account
        + ERC20_Min_Val_Sent: Minimum value in Ether sent from ERC20 token transactions for account
        + ERC20_Max_Val_Sent: Maximum value in Ether sent from ERC20 token transactions for account
        + ERC20_Avg_Val_Sent: Average value in Ether sent from ERC20 token transactions for account
        ERC20_Uniq_Sent_Token_Name: Number of Unique ERC20 tokens transferred
        ERC20_Uniq_Rec_Token_Name: Number of Unique ERC20 tokens received
        ERC20_Most_Sent_Token_Type: Most sent token for account via ERC20 transaction
        ERC20_Most_Rec_Token_Type: Most received token for account via ERC20 transactions
    """

    # (!) добавить:
    # avg_transactionIndex - средний индекс транзакции в блоке, median, max, min
    # среднее время когда покупка после создания токена (снайпер ли)


    if verbose:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} begin stat_wallets_address()")
    start_time_h = time.time()

    if token_address: token_address = token_address.lower()



    with psycopg2.connect(**db_config_psql, **keepalive_kwargs) as conn:
        start_time = time.time()
        transfers_sql = get_address_transfers(conn=conn, address=address, N=N, last_block=last_block)
        if verbose:
            print("%.3fs get_address_transfers() for stat_wallet_address" % (time.time() - start_time))


        if 0:
            erc20_addresses = []
            for alch in transfers_sql:
                if alch['category'] == 'erc20' and alch['address'] not in erc20_addresses:
                    erc20_addresses.append(alch['address'])


            #erc20_addresses = ["0xd811178c3c73c25e36a5dbbd0edfb75072b53b72", "0x0d08db747095e91780711724267a183e8522aa64", "0x88022d9d88d499f1ff9a014c3682f53435e4f580", "0xd239120c71f19c45367bd9494ead517e747abb37"]
            erc20_addresses = ["0xd811178c3c73c25e36a5dbbd0edfb75072b53b72"] # scam
            erc20_addresses = ['0x178221cbc1a669771d53e22725c96cbcf223ab33'] # not scam

            if len(erc20_addresses) == 1: erc20_addresses = tuple(erc20_addresses, '-')
            else: erc20_addresses = tuple(erc20_addresses)

            print('erc20_addresses!:')
            print(erc20_addresses)

            start_time = time.time()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:



                try:
                    sql = f"""
                        SELECT
                            address,
                            count(*) as total
                        FROM _transfers
                        WHERE address in {erc20_addresses}
                        GROUP BY address
                        """
                    print(sql)
                    _rows = cursor.fetchall()
                except psycopg2.ProgrammingError:
                    _rows = []
                
                pprint(_rows)
                exit()


                try:
                    sql = f"""
                        SELECT
                            l.address
                        FROM _logs l
                        WHERE
                            l.address in {erc20_addresses}
                            and l.topics_0='0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0' -- OwnershipTransferred()
                            and l.topics_2='0x0000000000000000000000000000000000000000'
                        """
                    print(sql)
                    _rows = cursor.fetchall()
                except psycopg2.ProgrammingError:
                    _rows = []

            print('_rows:')    
            pprint(_rows)
            print("%.3fs erc20 SELECT _logs OwnershipTransferred() to 0x000.." % (time.time() - start_time))
            exit()


    try:
        if w3 == None: balance_eth = 0
        else: balance_eth = float(from_wei(get_balance(w3, address=address, block_identifier=last_block), 'ether'))
    except:
        balance_eth = 0
        print(f"(!) get_balance() Exception: {sys.exc_info()[0]}")

    start_time = time.time()
    try:
        if w3 == None:
            balanceOf_USDC = 0
            balanceOf_USDT = 0
        else:
            balanceOf_USDC = contract_balanceOf(w3, token_address=USDC, wallet_address=address, block_identifier=last_block)
            balanceOf_USDT = contract_balanceOf(w3, token_address=USDT, wallet_address=address, block_identifier=last_block)
            if balanceOf_USDC == None:
                if verbose:
                    print(f"\x1b[31;1m(!)\x1b[0m почему-то balanceOf_USDC=None address={address}")
                balanceOf_USDC = 0
            if balanceOf_USDT == None:
                if verbose:
                    print(f"\x1b[31;1m(!)\x1b[0m почему-то balanceOf_USDT=None address={address}")
                balanceOf_USDT = 0
    except:
        balanceOf_USDC = 0
        balanceOf_USDT = 0
        print(f"(!) contract_balanceOf() Exception: {sys.exc_info()[0]}")

    # balance_eth = 0.1
    # balanceOf_USDC = 0
    # balanceOf_USDT = 0


    stat = {
        'transfers_sql_len': len(transfers_sql),
        'balance_eth': balance_eth,
        'balance_usd': balanceOf_USDC / 10 ** 6 + balanceOf_USDT / 10 ** 6,
        # (!) сделать
        #'balance_eth_usd': ...
        'total_from': 0,
        'total_to': 0,
        'external_from': 0,
        'external_to': 0,
        'internal_from': 0,
        'internal_to': 0,
        'erc20_from': 0,
        'erc20_to': 0,
        'erc721_from': 0,
        'erc721_to': 0,
        'erc1155_from': 0,
        'erc1155_to': 0,
        'diff_between_first_and_last': transfers_sql[0]['blocknumber'] - transfers_sql[-1]['blocknumber'] if transfers_sql else last_block - 18995767, # 18995767 13 Jan
        'banana_trans': 0, # кол-во banana транзакций
        'maestro_trans': 0,
        'first_in_transaction_from_address': None, # alchemy_to[-1]['from']
        'first_in_transaction_block': None,
        'nonce': w3.eth.get_transaction_count(address, block_identifier=last_block) if w3 != None else None,
        'external_from_addresses_n_values': {}, # key - address, value - list eth vals
    }
    if verbose:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {(time.time() - start_time):.3f}s get_balance() ether и 2x contract_balanceOf() USDT/USDC")
    first_in_transaction_block = {} # {'blocknumber': ..., 'transactionindex': ..., 'from': ...}
    hashes_ = {}
    unique_received_from_addresses = {}
    unique_received_to_addresses = {}
    eth_v = {
        'eth_received_external': [],
        'eth_received_internal': [],
        'eth_sent_external': [],
        'eth_sent_internal': [],
        'eth_total': [],
        'erc20_eth_send': [],
        'erc20_eth_received': [],
        'erc20_hold_time_block': [], # сколько времени (блоков) токен был у адреса с момента первой покупки до первой продажи
        'erc20_value_usd': [], # erc20 трейды в $
    }
    erc20_hashes = {}
    erc20_addresses = {}
    erc20_uniq_addresses_send = {}
    erc20_uniq_addresses_received = {}


    if verbose:
        print(f" address={address}, transfers_sql len={len(transfers_sql)}")



    start_time_f = time.time()
    for alch in transfers_sql:
        # 'external', 'internal', 'erc20', 'erc721', 'erc1155'

        if alch['value'] != None:
            assert float(int(alch['value'])) == float(alch['value'])
            alch['value'] = int(alch['value'])
        if alch['value_usd'] != None:
            alch['value_usd'] = float(alch['value_usd'])
        if alch['value_usd_per_token_unit'] != None:
            alch['value_usd_per_token_unit'] = float(alch['value_usd_per_token_unit'])

        if alch['from'] == address.lower():
            stat['total_from'] += 1
        if alch['to'] == address.lower():
            stat['total_to'] += 1
            if alch['category'] == 'external' and (first_in_transaction_block == {} or alch['blocknumber'] <= first_in_transaction_block['blocknumber'] or alch['blocknumber'] == first_in_transaction_block['blocknumber'] and alch['transactionindex'] < first_in_transaction_block['transactionindex']):
                first_in_transaction_block = {'blocknumber': alch['blocknumber'], 'transactionindex': alch['transactionindex'], 'from': alch['from'], 'value': alch['value']}

        if alch['category'] == 'external':
            if alch['from'] == address.lower():
                stat['external_from'] += 1
                if alch['value'] != 0:
                    if alch['to'] not in stat['external_from_addresses_n_values']:
                        stat['external_from_addresses_n_values'][alch['to']] = []
                    stat['external_from_addresses_n_values'][alch['to']].append(float(from_wei(alch['value'], 'ether')))
                    
            if alch['to'] == address.lower(): stat['external_to'] += 1

            if alch['to'] == '0x3328F7f4A1D1C57c35df56bBf0c9dCAFCA309C49'.lower():
                stat['banana_trans'] += 1
            if alch['to'] == '0x80a64c6D7f12C47B7c66c5B4E20E72bc1FCd5d9e'.lower():
                stat['maestro_trans'] += 1
        if alch['category'] == 'internal':
            if alch['from'] == address.lower(): stat['internal_from'] += 1
            if alch['to'] == address.lower(): stat['internal_to'] += 1
        if alch['category'] == 'erc20':
            if alch['from'] == address.lower(): stat['erc20_from'] += 1
            if alch['to'] == address.lower(): stat['erc20_to'] += 1
        if alch['category'] == 'erc721':
            if alch['from'] == address.lower(): stat['erc721_from'] += 1
            if alch['to'] == address.lower(): stat['erc721_to'] += 1
        if alch['category'] == 'erc1155':
            if alch['from'] == address.lower(): stat['erc1155_from'] += 1
            if alch['to'] == address.lower(): stat['erc1155_to'] += 1

        if alch['transactionhash'] not in hashes_: hashes_[alch['transactionhash']] = []
        hashes_[alch['transactionhash']].append(alch)

        if alch['category'] == 'erc20':
            erc20_hashes[alch['transactionhash']] = None

            if alch['address'] not in erc20_addresses:
                erc20_addresses[alch['address']] = {'block_buy': None, 'block_sell': None}
            if alch['from'] == address.lower():
                if erc20_addresses[alch['address']]['block_sell'] == None:
                    erc20_addresses[alch['address']]['block_sell'] = alch['blocknumber']
                else:
                    erc20_addresses[alch['address']]['block_sell'] = min(alch['blocknumber'], erc20_addresses[alch['address']]['block_sell'])
            if alch['to'] == address.lower():
                if erc20_addresses[alch['address']]['block_buy'] == None:
                    erc20_addresses[alch['address']]['block_buy'] = alch['blocknumber']
                else:
                    erc20_addresses[alch['address']]['block_buy'] = min(alch['blocknumber'], erc20_addresses[alch['address']]['block_buy'])
                erc20_addresses[alch['address']]['hash_buy'] = alch['transactionhash']


        if alch['to'] == address.lower():
            unique_received_from_addresses[alch['from']] = None
        if alch['from'] == address.lower():
            unique_received_to_addresses[alch['to']] = None

        if alch['address'] == None:
            _value = float(from_wei(alch['value'], 'ether'))
            if alch['category'] == 'external':
                if alch['to'] == address.lower(): eth_v['eth_received_external'].append(_value)
                if alch['from'] == address.lower(): eth_v['eth_sent_external'].append(_value)
            if alch['category'] == 'internal':
                if alch['to'] == address.lower(): eth_v['eth_received_internal'].append(_value)
                if alch['from'] == address.lower(): eth_v['eth_sent_internal'].append(_value)
            eth_v['eth_total'].append(_value)
    
    if verbose:
        print(f"{(time.time() - start_time_f):.3f}s for alch in transfers_sql (len={len(transfers_sql)})")


    start_time_k = time.time()


    # здесь делаем новый for цикл, потому что подразумевалось tx_hashes будем пинговать, но сейчас без этого (можно этот for вставить в предыдущий)
    for erc20_hash in erc20_hashes.keys():
        # (!) здесь надо бы добавить значения из tx_hashes, так как alchemy не все транзакции присылает сколько было потрачено на покупку/продажу токена
        # print(f"from={tx_hashes[erc20_hash]['from']}, to={tx_hashes[erc20_hash]['to']}, value={from_wei(int(tx_hashes[erc20_hash]['value'], 16), 'ether')}")
        for alch in hashes_[erc20_hash]:
            if alch['address'] != None:
                if alch['from'] == address.lower():
                    erc20_uniq_addresses_send[alch['address']] = None
                if alch['to'] == address.lower():
                    erc20_uniq_addresses_received[alch['address']] = None
                if alch['value_usd'] != None:
                    if alch['value_usd'] < 10 ** 7:
                        eth_v['erc20_value_usd'].append(alch['value_usd'])
            else:
                if float(alch['value']) != float(0):
                    _value = float(from_wei(alch['value'], 'ether'))
                    if alch['from'] == address.lower():
                        eth_v['erc20_eth_send'].append(_value)
                    if alch['to'] == address.lower():
                        eth_v['erc20_eth_received'].append(_value)
    
    erc20_bought_n_not_sold = []
    for erc20_addr, item in erc20_addresses.items():
        block_time_ = None
        if item['block_buy'] and item['block_sell']: block_time_ = item['block_sell'] - item['block_buy']
        # (!) ниже 2 строки стоит открыть, но похоже alchemy пропускает транзакции, что неизвестно buy/sell даты
        # elif item['block_buy'] and not item['block_sell']: block_time_ = last_block - item['block_buy']
        elif not item['block_buy'] and item['block_sell']: block_time_ = item['block_sell'] - transfers_sql[-1]['blocknumber']
        #elif not item['block_buy'] and item['block_sell']: block_time_ = item['block_sell'] -  min(int(alchemy_to[-1]['blockNum'], 16), int(alchemy_from[-1]['blockNum'], 16))
        if block_time_ != None:
            eth_v['erc20_hold_time_block'].append(block_time_)
        
        if erc20_addr != token_address and item['block_buy'] and not item['block_sell']:
            erc20_bought_n_not_sold.append(erc20_addr)


    for key, value in eth_v.items():
        if not value: value = [0]
        stat[f'{key}_max'] = max(value)
        stat[f'{key}_min'] = min(value)
        stat[f'{key}_sum'] = sum(value)
        stat[f'{key}_std'] = statistics.pstdev(value)
        stat[f'{key}_median'] = statistics.median(value)
        stat[f'{key}_avg'] = statistics.mean(value)


    if verbose:
        print(f"{(time.time() - start_time_k):.3f}s for 3 loops")
    
    if hashes_:
        stat |= {
            'avg_blocks_per_trans': stat['diff_between_first_and_last'] / len(hashes_),
            'external_from_div_uniq_trans': stat['external_from'] / len(hashes_),
            'external_to_div_uniq_trans': stat['external_to'] / len(hashes_),
            'internal_from_div_uniq_trans': stat['internal_from'] / len(hashes_),
            'internal_to_div_uniq_trans': stat['internal_to'] / len(hashes_),
            'erc20_from_div_uniq_trans': stat['erc20_from'] / len(hashes_),
            'erc20_to_div_uniq_trans': stat['erc20_to'] / len(hashes_),
            'unique_transactions': len(hashes_),
            'unique_received_from_addresses_div_uniq_trans': len(unique_received_from_addresses) / len(hashes_),
            'unique_received_to_addresses_div_uniq_trans': len(unique_received_to_addresses) / len(hashes_),
            'banana_trans_perc': stat['banana_trans'] / len(hashes_),
            'maestro_trans_perc': stat['maestro_trans'] / len(hashes_),
        }
    else:
        stat |= {
            'avg_blocks_per_trans': 12 * 5 * 60 * 24 * 30 * 8, # 8 месяцев
            'external_from_div_uniq_trans': 0,
            'external_to_div_uniq_trans': 0,
            'internal_from_div_uniq_trans': 0,
            'internal_to_div_uniq_trans': 0,
            'erc20_from_div_uniq_trans': 0,
            'erc20_to_div_uniq_trans': 0,
            'unique_transactions': len(hashes_),
            'unique_received_from_addresses_div_uniq_trans': 0,
            'unique_received_to_addresses_div_uniq_trans': 0,
            'banana_trans_perc': 0,
            'maestro_trans_perc': 0,
        }

    stat |= {
        'erc20_uniq_addresses_send': len(erc20_uniq_addresses_send),
        'erc20_uniq_addresses_received': len(erc20_uniq_addresses_received),
        'unique_received_from_addresses': len(unique_received_from_addresses),
        'unique_received_to_addresses': len(unique_received_to_addresses),
        'erc20_bought_n_not_sold': len(erc20_bought_n_not_sold),
        'erc20_bought_n_not_sold_perc': (100 * len(erc20_bought_n_not_sold) / (len(erc20_addresses) - (1 if token_address else 0))) if (len(erc20_addresses) - (1 if token_address else 0)) != 0 else 0, # вычитаем 1, чтобы не учитывать token_address
        'erc20_bought_n_not_sold_addresses': erc20_bought_n_not_sold,

        # баг транзакции 0x850d8617f43cd89233675a1bd661228986171f111c4a8314655bb47c7778e980, что нет данных _transfers в бд, хотя IN должен быть трансфер
        # (!) надо улучшить: транзакция должна быть external только
        'first_in_transaction_from_address': first_in_transaction_block['from'] if 'from' in first_in_transaction_block else None,
        'first_in_transaction_from_address_value_eth': float(from_wei(first_in_transaction_block['value'], 'ether')) if 'from' in first_in_transaction_block else None,
        'first_in_transaction_block': first_in_transaction_block['blocknumber'] if 'blocknumber' in first_in_transaction_block else 18995767, # 13 Jan
        # 'executed_in': time.time() - start_time_h, # этот параметр нужен для отладки, он будет удален далее
    }
    stat |= {
        'age_days': (last_block - stat['first_in_transaction_block']) * 12 / 60 / 60 / 24,
    }

    stat |= {
        'erc20_addresses': list(erc20_addresses.keys())
    }
    # del stat['external_from_addresses_n_values']


    if verbose:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} end stat_wallets_address(), took {(time.time() - start_time_h):.3f}s")


    return stat



def get_state_change(testnet, tx, miner, method, verbose=0):

    if method == 'debug_traceCall':

        WALLETS_ETH = [
            '0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8', # Binance 7
        ]

        # to be sure ETH balance is enough
        tx['from'] = WALLETS_ETH[0].lower()

        traces = requests.post(testnet, json={
            'id': int(tx['transactionIndex'], 16),
            'method': 'debug_traceCall',
            'jsonrpc': '2.0',
            'params': [
                {'to': tx['to'], 'gas': tx['gas'], 'gasPrice': tx['gasPrice'], 'value': tx['value'], 'data': tx['input'], 'from': tx['from']},
                tx['blockNumber'],
                {
                    "tracer": "prestateTracer",
                    "tracerConfig": {
                        "diffMode": True,
                        "withLog": True,
                        "onlyTopCall": False,
                    },
                },
            ],
        }, headers={"Content-Type": "application/json"}).json()


        if 'result' not in traces:
            #pprint(traces)
            #exit("result not in traces")
            if verbose:
                print(f"\x1b[31;1m(!)\x1b[0m get_state_change() 'result' not in traces, tx['hash']={tx['hash']}")
            return None

        if 'error' in traces:
            #pprint(traces)
            #return traces
            if verbose:
                print(f"\x1b[31;1m(!)\x1b[0m get_state_change() 'error' in traces, tx['hash']={tx['hash']}")
            return None

        traces = traces['result']


        for trace_address, data in traces['pre'].items():
            if trace_address == miner:
                return {'post': int(traces['post'][miner]['balance'], 16), 'pre': int(traces['pre'][miner]['balance'], 16)}
            


    if method == 'trace_replayTransaction':

        traces = requests.post(testnet, json={
            'id': 1,
            'method': 'trace_replayTransaction',
            'jsonrpc': '2.0',
            "params":[
                tx['hash'],
                [
                    "trace",
                    "stateDiff"
                ]
            ],
        }, headers={"Content-Type": "application/json"}).json()

        # pprint(traces)
        # exit()

        if 'result' not in traces:
            #pprint(traces)
            #exit("result not in traces")
            if verbose:
                print(f"\x1b[31;1m(!)\x1b[0m get_state_change() 'result' not in traces, tx['hash']={tx['hash']}")
            return None

        if 'error' in traces:
            #pprint(traces)
            #return traces
            if verbose:
                print(f"\x1b[31;1m(!)\x1b[0m get_state_change() 'error' in traces, tx['hash']={tx['hash']}")
            return None

        if traces['result'] is None:
            #pprint(traces)
            #exit("result not in traces")
            if verbose:
                print(f"\x1b[31;1m(!)\x1b[0m get_state_change() traces['result'] is None, tx['hash']={tx['hash']}")
            return None

        traces = traces['result']

        for trace_address, data in traces['stateDiff'].items():
            if trace_address == miner:
                if '*' not in traces['stateDiff'][miner]['balance']:
                    if verbose:
                        print(f"\x1b[31;1m(!)\x1b[0m get_state_change() '*' not in traces['stateDiff'][miner]['balance'], tx['hash']={tx['hash']}")
                    return None

                return {'post': int(traces['stateDiff'][miner]['balance']['*']['to'], 16), 'pre': int(traces['stateDiff'][miner]['balance']['*']['from'], 16)}
            



    return None



def calc_bribe(state_diff, gasUsed, maxPriorityFeePerGas):

    if maxPriorityFeePerGas == None: maxPriorityFeePerGas = 0
    bribe = state_diff - gasUsed * maxPriorityFeePerGas
    if bribe < 0:
        # print(f"\x1b[31;1m(!)\x1b[0m bribe={bribe} < 0")
        return None

    return from_wei(bribe, 'ether')


def is_address_contract(w3, address):
    return w3.eth.get_code(address) != b''


# https://stepik.org/lesson/613339/step/10
def variance(data):
    mu = statistics.mean(data)
    n = len(data)
    square_deviation = lambda x : (x - mu) ** 2 
    return sum( map(square_deviation, data) ) / n



def is_hex(s):
    hex_digits = set(string.hexdigits)
    return all(c in hex_digits for c in s)


def human_format(num):
    num = float('{:.3g}'.format(num))
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'k', 'm', 'B', 'T'][magnitude])



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