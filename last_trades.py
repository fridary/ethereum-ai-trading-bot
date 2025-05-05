
import os
import sys
import json
import pickle
import traceback
from pprint import pprint, pformat
import numpy as np
import pandas as pd
import time
import random
import string
import copy
import statistics
import itertools
import requests
from datetime import datetime
from multiprocessing import Pool
import tabulate
from filelock import FileLock
from colorama import Fore, Back, Style
from eth_utils import to_checksum_address, from_wei, to_wei
from web3 import Web3
from attributedict.collections import AttributeDict
import talib.abstract as ta
from os import listdir
from os.path import isfile, join

import psycopg2
from psycopg2.extras import Json, DictCursor, RealDictCursor




import utils
from utils import FILES_DIR, FILE_LABELS, DIR_LOAD_OBJECTS
import ml

try:
    import analyse_30m
except ImportError:
    pass



# включить для производительности
# def pformat(msg):
#     return msg



class LastTrades():


    def __init__(self, conn, address: str, last_block: int, can_be_old_token: bool, limit: int, sort: str, realtime: bool, verbose: int=0):

        # self.conn = conn
        self.address = to_checksum_address(address)
        self.last_block = last_block
        self.can_be_old_token = can_be_old_token
        self.limit = limit
        self.sort = sort
        self.realtime = realtime
        self.verbose = verbose

        self.pairs_dict = {} # список Uniswap пар адреса; key - адрес пары, value - token1_address
        self.dataframe = None
        self.df_analyse = None # df из analyse_30m для catboost
        self.df_analyse_total_seconds = 0
        self.df_analyse_total_counts = 0
        self.df_analyse_last_total_trades = 0
        self.df_analyse_last_block = None

        self.blocks_data = {} # данные, где ключ есть номер блока
        """
            timestamp
            value_usd_per_token_unit
            extra_data
            liquidity
            volume_buy
            volume_sell
            trades_buy
            trades_sell
            invested
            invested_list
            invested_list_by_addr
            withdrew
            withdrew_list
            withdrew_list_by_addr
            total_volume_buy
            total_volume_sell
            total_volume_transfer
            total_invested
            total_withdrew
            total_trades_buy
            total_trades_sell
            total_trades_transfer
            total_supply
            buy_addresses # список адресов, которые сделали покупку на этом блоке
            sell_addresses
            transfer_sent_addresses
            transfer_received_addresses
            buy_trans_indexes # индексы транзакций покупок в блоке
            sell_trans_indexes
            holders_total # сколько холдеров по self.balances, не считая pair, native, owner
            holder_new # на данном блоке
            trades_list # список трейдов 'b'/'s'
            stats_profit_window=<N>
            stats_erc20_median
            jaredfromsubway # массив из trans_volume_buy + trans_volume_sell
            total_p_i # последний total_p_i на этом блоке
            'transfers': 0,
            'transfers_list_usd': [],
            'transfers_list_perc': [],
            total_volume_transfer
            total_trades_transfer
            tx_eth_price

            |= _stats
        """


        self.balances = {}
        """
        {'%': 44.25564618159304,
        'days': 0.0,
        'first_trade': 1717027043,
        'first_trade_block': ...,
        first_trade_index
        history
        history_balance
        'gas_usd': 6.019402890115204,
        'i': 4, # номер покупателя, начиная с 0
        'invested': ...,
        'volume_buy': 3763.29, - разница от volume_buy в том, что volume_buy высчитывается на основании цены токена в конце блока
        'volume_sell': ...,
        'last_trade_block': 19978933,
        'last_p_i': 10, # последний трейд по p_i
        'last_trade': 1717027043,
        'last_trade_block': 19978933,
        'liquidity_first': {'p': 7005.681047524752,'t': 7.005681047524752e-15},
        'pnl': 2000.2845081898126,
        'realized_profit': 0,
        'roi_r': 0.0,
        'roi_t': 53.152547589737,
        'roi_u': 53.152547589737,
        'router': 'Banana Gun',
        'trades_buy': 1,
        'trades_list': {
            <номер_блока>: [{'q': 429279767.96145254, 's': 'buy', 't': 1717027043, 'vut': 8.76652076539961e-06, 'invested': 100$, 'withdrew': 50$}],
        },
        'transfers_list': {
            <номер_блока>: [{'q': 429279767.96145254, 'a': <address>, 't': s/r}], # type sent/received, т.е. был отправлен кому трансфер или от кого получен
        },
        'trades_sell': 0,
        'unrealized_profit': 2000.2845081898126,
        'value': 429279767961452528960879884,
        'value_usd': 5763.574508189812,
        'widthdrew': ...
        'transfers_sent': 0,
        'transfers_sent_list_usd': {}, # key - номер блока, value - массив
        transfers_sent_list_usd_total - суммарный transfers_sent_list_usd
        'transfers_sent_list_perc': {},
        transfers_sent_list_perc_total
        'transfers_received': 0,
        'transfers_received_list_usd': {},
        'transfers_received_list_perc': {}
        
        'stat_wallet_address' - финальное значение статы erc20
        'stat_wallet_address_first' - значение на момент получения токена

        }
        """

        self.pairs = {}
        # self.pairs = {
        #     '..pair_address..': {
        #         'block_created_number': ..., # на каком блоке создалась пара
        #         'block_created_timestamp': ..., # timestamp этого блока
        #         'exchange': .., # V2, V3
        #         'initial_liquidity': {'value': .., 'value_usd': .., 'value_eth': ..}, # value в валюте token1 (WETH/USDC/USDT)
        #         'extra_liquidities': [{'value': .., 'value_usd': ..}, ...] # массив сколько новых Mint() было сделано
        #         'token1_address': .., # WETH, USDC, USDT
        #         'token1_liquidity': ..,
        #         'token1_liquidity_usd': ..,
        #         'balances': [],
        #         'wallet_token': {'Wei': {'value': 0, 'value_usd': None}}, # токены на адресе контракта
        #     }
        # }

        #self.primary_pair = None # адрес пары, по которой будет все считаться (первая созданная и с макс ликвидностью)


        self.total_volume_buy = 0 # $
        self.total_volume_sell = 0 # $
        self.total_volume_transfer = 0 # $
        self.total_trades_buy = 0
        self.total_trades_sell = 0
        self.total_trades_transfer = 0
        self.total_invested = 0
        self.total_withdrew = 0
        self.total_p_i = None # суммируется, когда загружаем объект класса снова
        self.first_trade_tx_sql = None
        self.txs_sql_token_owners = None # транзакции OwnershipTransferred
        self.txs_sql_to_addresses = {} # список адресов, где в транзакции to стоит данный адрес; ключ - router, value - первый address с этим router
        self.txs_sql_pairs = None
        self.pairs_list = None
        self.transaction_absolute_first = None
        self.initial_value_usd_per_token_unit = None # цена токена в момент Mint()
        self.initial_value_native_per_token_unit = None # значение в WETH или USDC/USDT
        self.holders_before_first_mint = [] # адреса без owner, pairs, token_address
        self.total_holders_before_first_trade = None # без owner, pairs, token_address
        self.stat_erc20_first_owner = None
        self.value_usd_per_token_unit = None # последнее значение на итерации в blocks_data
        self.total_supply = None # последнее значение на итерации в blocks_data

        self.first_in_transaction_from_address_rating = {} # ключ - адрес, value - кол-во

        self.block0_green_flags = []
        self.wallet_stats_by_blocks = {} # dict, где index = адрес кошелька, values - dict со значениями блоков, на которых получен stats, value - значение stats
        self.wallet_stats_cache = 10 # hours
        #self.wallet_stats_cache = 9999999999 # hours


        self.token_ml = None
        self.last_block_by_last_transaction = None

        self.full_log_text = None # сохраняются логи текстовые
        self.full_log_text_limit = 0 # до скольки блоков в blocks_data

        self.start_time_g = time.time()

        # -----------------


        # (!) тут проработать где задавать
        w3_g = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
        if not w3_g.is_connected():
            raise Exception("w3_g not connected")
        self.w3_g_eth_block_number = w3_g.eth.block_number


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
        ]
        if self.w3_g_eth_block_number - last_block < 50:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?ini"))
        else:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?ini"))

        contract_token = w3.eth.contract(address=self.address, abi=abi) # ! нельзя contract_token сохранять в self, потому что тогда в stat_wallets() пул не создастся
        self.contract_data_token = utils.get_contract_data(w3, contract_token, block=last_block, debug=1)
        if self.contract_data_token == {}:
            self.contract_data_token = utils.get_contract_data(Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?ini")), contract_token, block=self.w3_g_eth_block_number)

        self.addresses_for_abi = [utils.UNI_V2_FACTORY, utils.UNI_V3_FACTORY, utils.WETH, utils.USDT, utils.USDC]
        for _, item in FILE_LABELS['ethereum'].items():
            self.addresses_for_abi += item.keys()

        self._contracts = {}


        
        self.get_db_data(w3=w3, conn=conn, from_block=18995767) # 18995767 13 Jan


                


    def get_db_data(self, w3, conn, from_block):


        start_time = time.time()
        self.transactions_tupl, pairs_list, __txs_sql_pairs = utils.get_token_transactions_only_transfers(w3=w3, conn=conn, address=self.address, last_block=self.last_block, from_block=from_block, limit=self.limit, sort=self.sort, realtime=self.realtime, can_be_old_token=self.can_be_old_token, verbose=self.verbose)

        if self.transactions_tupl:
            msg = f"[LastTrades.get_db_data()] transactions_tupl len={len(self.transactions_tupl)} in {(time.time() - start_time):.2f}s, first tx: {datetime.fromtimestamp(self.transactions_tupl[0]['block_timestamp'])}, last tx: {datetime.fromtimestamp(self.transactions_tupl[-1]['block_timestamp'])}"
            self.print(f"{msg} for address={self.address}", force=True if not self.realtime else False)
            if self.realtime: utils.save_log_strg(msg, address=self.address, block=self.last_block)
        else:
            msg = f"[LastTrades.get_db_data()] transactions_tupl len={len(self.transactions_tupl)} in {(time.time() - start_time):.2f}s"
            self.print(f"{msg} for address={self.address}", force=True if not self.realtime else False)
            if self.realtime: utils.save_log_strg(msg, address=self.address, block=self.last_block)


        assert pairs_list, f'Пары не найдено по address={self.address}'
        self.print('pairs_list:')
        self.print(pformat(pairs_list))
        for item in pairs_list:
            self.pairs_dict[to_checksum_address(item['pair'])] = item['token1_address']
            self.pairs[to_checksum_address(item['pair'])] = {
                'initial_liquidity': None,
                'token1_address': item['token1_address'],
                'exchange': item['exchange'],
                'block_created': item['block_created'],
                'block_timestamp': item['block_timestamp'],
            }
        self.print('self.pairs_dict:')
        self.print(pformat(self.pairs_dict))
        self.pairs_list = list(self.pairs_dict.keys())
        self.txs_sql_pairs = __txs_sql_pairs


        start_time = time.time()
        self.txs_sql_token_owners = utils.get_token_owners(conn, address=self.address, last_block=self.last_block, verbose=self.verbose)
        msg = f"[LastTrades.get_db_data()] utils.get_token_owners() in {(time.time() - start_time):.2f}s"
        self.print(f"{msg} for address={self.address}")
        if self.realtime: utils.save_log_strg(msg, address=self.address, block=self.last_block)

        for i, ow in enumerate(self.txs_sql_token_owners):
            # Transfer()
            if ow['topics_0'] == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef':
                self.print('Ownership было через Transfer(), изменим topics_2 для простоты на from транзакции')
                self.txs_sql_token_owners[i]['topics_2'] = self.txs_sql_token_owners[i]['from']

        self.print(f'txs_sql_token_owners ({len(self.txs_sql_token_owners)}):')
        self.print(pformat(self.txs_sql_token_owners))
        self.print('self.owner_addresses:')
        self.print(pformat(self.owner_addresses))



        if not self.can_be_old_token:
            assert len(self.txs_sql_token_owners) and self.txs_sql_token_owners[0]['topics_1'] in [utils.ADDRESS_0X0000.lower(), utils.ADDRESS_0XDEAD.lower()], f"Ошибка распределения txs_sql_token_owners на {self.address} {self.txs_sql_token_owners}"
  

        start_time = time.time()
        _txs_sql_mints = utils.get_mints_txs(conn, pairs=self.pairs_list, last_block=self.last_block, verbose=self.verbose)
        msg = f"[LastTrades.get_db_data()] utils.get_mints_txs() in {(time.time() - start_time):.2f}s"
        self.print(f"{msg} for address={self.address}")
        if self.realtime: utils.save_log_strg(msg, address=self.address, block=self.last_block)

        self.txs_sql_mints = {}
        for tx in _txs_sql_mints:
            self.txs_sql_mints[tx['hash']] = tx

        self.print(f'self.txs_sql_mints len={len(self.txs_sql_mints)}. Первый минт:')
        for tx in self.txs_sql_mints.values():
            self.print(f"  {tx['hash']}")
            for key, value in tx.items():
                if key != 'logs_array':
                    self.print(f"      {key}: {value}")
            break
        

    def reinit(self, conn, last_block: int, from_block: int, limit: int, realtime: bool, verbose: int):

        assert from_block < last_block, f"from_block < last_block address={self.address}, from_block={from_block}, last_block={last_block}"

        self.last_block = last_block
        self.limit = limit
        self.realtime = realtime
        self.verbose = verbose

        self.print(f"reinit(last_block={last_block}, from_block={from_block})")


        w3_g = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
        if not w3_g.is_connected():
            raise Exception("w3_g not connected")
        self.w3_g_eth_block_number = w3_g.eth.block_number

        if self.w3_g_eth_block_number - last_block < 50:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?ini"))
        else:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?ini"))

        # if not hasattr(self, 'primary_pair'):
        #     self.primary_pair = None


        self.get_db_data(w3=w3, conn=conn, from_block=from_block)









    #def print(self, *args, **kwargs):
    def print(self, msg: str, force: bool = False):
        if (self.verbose or force) and not self.realtime:
            print(msg)

        if len(self.blocks_data.keys()) <= self.full_log_text_limit:
            if self.full_log_text == None: self.full_log_text = ""
            self.full_log_text += f"{msg}\n"


    @property
    def owner_addresses(self):
        owners = []
        for tx_sql in self.txs_sql_token_owners:
            if not owners or owners and owners[-1] != to_checksum_address(tx_sql['topics_2']):
                # topics_1 - предыдущий owner
                # topics_2 - новый
                owners.append(to_checksum_address(tx_sql['topics_2']))
        return owners
        

    def balance_table(self, order=None):

        if order == None:
            balances_ = sorted(self.balances.items(), key=lambda d: (d[1]['last_trade_block'], d[1]['last_p_i']))
            #balances_ = [value | {'address': address} for address, value in balances_]
        else:
            balances_ = sorted(self.balances.items(), key=lambda d: (d[1][order], d[1]['pnl']))

        modified_w = []
        total_supply = self.blocks_data[max(self.blocks_data.keys())]['total_supply'] if self.blocks_data else None
        for address, value in balances_:

            #value = {'date': datetime.fromtimestamp(value['block_timestamp']).strftime("%d-%m-%Y"), 'address': address} | value
            # value = value | {'address': address}
            # print('value:')
            # print(value)

            a_c = {}
            a_c['i'] = value['i']
            a_c['l_block'] = value['last_trade_block']

            _value = round(value['value'] / 10 ** self.contract_data_token['token_decimals'], 2)
            if value['value_usd']:
                a_c['value'] = f"{_value}, {round(value['value_usd'], 2)} $"
                if len(a_c['value']) > 27:
                    a_c['value'] = f"{str(_value)[:10]}.. {round(value['value_usd'], 2)} $"
            else:
                a_c['value'] = _value


            if total_supply:
                a_c['%'] = round(100 * value['value'] / total_supply, 2)
                if a_c['%'] >= 100: a_c['%'] = round(a_c['%'], 1)
                if a_c['%'] <= -100: a_c['%'] = round(a_c['%'])
            else:
                a_c['%'] = None
            # a_c['invested / vol_buy'] = f"{round(value['invested'], 2)} $ / {round(value['volume_buy'], 2)} $"
            a_c['invested'] = f"{round(value['invested'], 2)} $"
            #a_c['vol_sell'] = f"{round(value['volume_sell'], 2)} $"
            #a_c['withdrew / vol_sell'] = f"{round(value['withdrew'], 2)} $ / {round(value['volume_sell'], 2)} $"
            a_c['withdrew'] = f"{round(value['withdrew'], 2)} $"
            a_c['gas'] = f"{round(value['gas_usd'], 2)} $"

            a_c['liq_first'] = None
            if value['liquidity_first']:
                if value['liquidity_first']['t'] == value['liquidity_first']['p']:
                    a_c['liq_first'] = f"{utils.human_format(round(2 * value['liquidity_first']['t'], 2))} $"
                else:
                    a_c['liq_first'] = f"{utils.human_format(round(value['liquidity_first']['t'], 2)) if value['liquidity_first']['t'] else 'N/A'}/{utils.human_format(round(value['liquidity_first']['p'], 2)) if value['liquidity_first']['p'] else 'N/A'} $"

            a_c['router'] = None
            if value['router'] != None:
                for f_address, router in FILE_LABELS['ethereum']['routers'].items():
                    if f_address == value['router']:
                        a_c['router'] = router.split(' ')[0].rstrip(':').lower()
                        break
                if a_c['router'] == None and value['router'] != None:
                    a_c['router'] = value['router'][:5] + '..'

            a_c['r_prof'] = f"{round(value['realized_profit'], 2)} $" if value['realized_profit'] != 0 else 0
            a_c['ur_prof'] = f"{round(value['unrealized_profit'], 2)} $" if value['unrealized_profit'] != 0 else 0
            a_c['roi_r'] = f"{round(value['roi_r'])}%" if value['roi_r'] != 0 else 0
            a_c['roi_u'] = f"{round(value['roi_u'])}%" if value['roi_u'] != 0 else 0
            a_c['roi_t'] = f"{round(value['roi_t'])}%" if value['roi_t'] != 0 else 0

            a_c['days'] = round(value['days'], 2) if value['days'] else None

            a_c['trades'] = f"{value['trades_buy']}/{value['trades_sell']}"

            if not(address == self.address or address in self.pairs_list):
                if value['transfers_sent'] != 0 or value['transfers_received'] != 0:
                    a_c['trades'] += f"/{value['transfers_sent']}/{value['transfers_received']}"

            a_c['pnl'] = f"{round(value['pnl'], 2)} $" if value['pnl'] != 0 else 0

            a_c['address'] = address[-7:]
            if address in self.owner_addresses:
                a_c['address'] = f"{Fore.RED}{a_c['address']}{Style.RESET_ALL}"
            if address == self.address:
                a_c['address'] = f"{Fore.GREEN}{a_c['address']}{Style.RESET_ALL}"
            if address in self.pairs_list:
                a_c['address'] = f"{Fore.BLUE}{a_c['address']}{Style.RESET_ALL}"
            if address in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:
                a_c['address'] = f"{Fore.MAGENTA}{a_c['address']}{Style.RESET_ALL}"


            
            modified_w.append(a_c)



        return tabulate.tabulate(modified_w[:], tablefmt='psql', headers='keys', stralign="right")
    


    def get_trades(self):


        # (!) брать значения тех свечей, где price_change > хотя бы 100% относительно inited, чтобы не портить предсказания выборкой уже упавшего токена, где вряд ли цена вырастет 
        # как делать проверку на honeypot: после первых нескольких блоков после первого Mint(), ждать когда цена упадет хотя бы на 30%. Потом можно
        # смотреть если один и тот же адрес шажками в 1-10 минут покупает все больше токена (пример https://dexscreener.com/ethereum/0x5d721a3292e1941217fd07d196524b0b5ea2af8d?maker=0xdb92c566c6Be96b95D5C3Ea16F2cd65aD830f2fD) file:///Users/sirjay/Downloads/buy_defi_steps_trans.png
        #  то есть метрика сколько повторных покупок адреса
        # индикатор как долго в сделке в среднем адрес, тогда понятно как долго еще будут держать токен только что купившие

        
        w3_g = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
        w3_e = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))



        # if self.verbose:
        #     self.transactions_tupl = self.transactions_tupl[:350]

        start_time_block = None
        make_break_at_last_block_transaction = False

        #print(f"запустили for p_i, tx_sql in enumerate(self.transactions_tupl)")
        start_time_t = time.time()

        last_N_blocks_for_erc20_stat = [self.last_block]
        for tx_sql in reversed(self.transactions_tupl):
            if tx_sql['block_number'] not in last_N_blocks_for_erc20_stat:
                last_N_blocks_for_erc20_stat.append(tx_sql['block_number'])
            if len(last_N_blocks_for_erc20_stat) == 1:
                break

        start_time_pi = time.time()
        for p_i, tx_sql in enumerate(self.transactions_tupl):


            if self.total_p_i == None:
                assert p_i == 0, f"{self.address}, {p_i}"
                self.total_p_i = 0
            else:
                self.total_p_i += 1



            # (!)
            # if self.first_trade_tx_sql and not self.realtime:
            
            #     if (tx_sql['block_number'] - self.first_trade_tx_sql['block_number']) * 12 / 60 > 30:

            #         print(f"\n (!) break 30m \n")
            #         break


            
            try:
                if p_i % 100 == 0 and p_i > 1:
                    if 1 or len(list_addresses) == 1:
                        print(f"{p_i} / {len(self.transactions_tupl)} {(time.time() - start_time_pi):.2f}s, trades={self.total_trades_buy+self.total_trades_sell}, traded blocks={len(self.blocks_data.keys())}, holders={len(self.balances)}, {str(round((tx_sql['block_number'] - self.first_trade_tx_sql['block_number']) * 12 / 60)) + 'm' if self.first_trade_tx_sql else ''}, df_analyse_total_seconds={round(self.df_analyse_total_seconds)}s, df_analyse_total_counts={self.df_analyse_total_counts} {self.address}")
                    start_time_pi = time.time()
            except NameError:
                pass

            # if not make_break_at_last_block_transaction and self.df_analyse_last_total_trades != 0 and self.total_trades_buy + self.total_trades_sell - self.df_analyse_last_total_trades > 700:
            if not make_break_at_last_block_transaction and self.df_analyse_last_block != None and (tx_sql['block_number'] - self.df_analyse_last_block) * 12 / 60 / 60 / 24 > 6 and self.df_analyse_last_total_trades >= 2000:
                print(f"(tx_sql['block_number'] - self.df_analyse_last_block) * 12 / 60 / 60 / 24 = {(tx_sql['block_number'] - self.df_analyse_last_block) * 12 / 60 / 60 / 24} > 6, self.df_analyse_last_total_trades={self.df_analyse_last_total_trades} >= 2000 => break, address={self.address}")
                make_break_at_last_block_transaction = True


            # if tx_sql['block_timestamp'] > (pd.to_datetime('2024-06-17 19:57:23') + pd.Timedelta(hours=3)).timestamp():
            #     print('exit on', p_i)
            #     exit()

            # print('current time:', datetime.fromtimestamp(tx_sql['block_timestamp']), 'time need:', pd.to_datetime('2024-06-17 19:57:23') + pd.Timedelta(hours=0))

            #if len(self.blocks_data) == 1000: break

            # if p_i == 31:
            #     exit()

            
            # if self.first_trade_tx_sql and tx_sql['block_number'] - self.first_trade_tx_sql['block_number'] >= 2:
            #     if tx_sql['block_number'] - self.first_trade_tx_sql['block_number'] >= 2 and self.first_trade_tx_sql['block_number'] + 1 not in self.blocks_data.keys():
            #         self.print(f"Похоже блока {self.first_trade_tx_sql['block_number'] + 1}, следующего после первого трейда, не было (а следующий {tx_sql['block_number']})")
            #     self.print("Выходим из цикла, т.к. последняя транзакция в блоке первого трейда")
            #     break


            if self.w3_g_eth_block_number - tx_sql['block_number'] < 50:
                w3 = w3_g
                _testnet = f"http://127.0.0.1:{utils.GETH_PORT}/?ini2"
            else:
                w3 = w3_e
                _testnet = f"http://127.0.0.1:8545/?ini2"


            if start_time_block == None or self.transactions_tupl[p_i]['block_number'] != self.transactions_tupl[p_i - 1]['block_number']:
                start_time_block = time.time()


            if tx_sql['to'] == None:
                tx_sql['to'] = tx_sql['contractaddress']

            tx_sql['from'] = to_checksum_address(tx_sql['from'])
            tx_sql['to'] = to_checksum_address(tx_sql['to'])
            tx_sql['value'] = int(tx_sql['value']) # Decimal convert

            # (!) сохранять значение если в том же блоке
            tx_eth_price = tx_sql['eth_price']
            if tx_eth_price in [0, None]:
                tx_eth_price = utils.get_eth_price(w3=w3, block=tx_sql['block_number'], realtime=self.realtime)
            if tx_eth_price == None:
                self.print(f"\x1b[31;1m(!)\x1b[0m tx_eth_price=None for address {self.address} on block={tx_sql['block_number']} for last_block={self.last_block}", force=True)
                # print(f"tx_eth_price=None for address {self.address} on block={tx_sql['block_number']} for last_block={self.last_block}", file=sys.stderr)
                # if self.realtime:
                #     with FileLock(f"{FILES_DIR}/lib/logs/strategy_errors.log.lock"):
                #         with open(f"{FILES_DIR}/lib/logs/strategy_errors.log", "a+") as file:
                #             file.write(f"tx_eth_price=None for address {self.address} on block={tx_sql['block_number']} for last_block={self.last_block}" + "\n")
                continue
            tx_eth_price = float(tx_eth_price)



            self.print(f"\x1b[32;1m [{p_i}/{self.total_p_i}]\x1b[0m tx_hash={tx_sql['hash']}, tx_block={tx_sql['block_number']} ({datetime.fromtimestamp(tx_sql['block_timestamp'])}), tx_index={tx_sql['transactionindex']}")
            self.print(f"     from: {utils.print_address(tx_sql['from'])}, to: {utils.print_address(tx_sql['to'])}, value: {tx_sql['value']}, gas: {round(tx_sql['gasused'] * tx_sql['gasprice'] / tx_eth_price, 2) if tx_sql['gasused'] != None else None} $")




            # if len(tx_sql['transfers_array']) == 1 and tx_sql['transfers_array'][0]['address'] == None and tx_sql['transfers_array'][0]['value'] == 0:
            #     if p_i + 1 == len(self.transactions_tupl) or tx_sql['block_number'] != self.transactions_tupl[p_i + 1]['block_number']:
            #         self.print(f"это последняя транзакция в данном блоке {tx_sql['block_number']}")
            #         if tx_sql['block_number'] in self.blocks_data:
            #             self.update_balances(value_usd_per_token_unit=self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'], last_tx_sql=tx_sql)
            #     continue

            self.print(pformat(dict(tx_sql)))

            tx_sql['gas_usd'] = tx_sql['gasused'] * tx_sql['gasprice'] / tx_eth_price if tx_sql['gasused'] != None else 0

            # print('transaction_transfers:')
            # pprint(tx_sql['transfers_array'])



            if tx_sql['hash'] in self.txs_sql_mints.keys():
                self.txs_sql_mints[tx_sql['hash']]['p_i'] = self.total_p_i
                if 'gas_usd' not in self.txs_sql_mints[tx_sql['hash']]:
                    self.txs_sql_mints[tx_sql['hash']]['gas_usd'] = tx_sql['gas_usd']
                self.proceed_mint_logs(w3=w3, tx_hash=tx_sql['hash'], tx_eth_price=tx_eth_price)
                self.print('self.pairs:')
                self.print(pformat(self.pairs))

            for i in range(len(self.txs_sql_pairs)):
                if tx_sql['hash'] == self.txs_sql_pairs[i]['hash']:
                    self.txs_sql_pairs[i]['p_i'] = self.total_p_i
                    if 'gas_usd' not in self.txs_sql_pairs[i]:
                        self.txs_sql_pairs[i]['gas_usd'] = tx_sql['gas_usd']



            self.last_block_by_last_transaction = tx_sql['block_number']
            
            transaction_transfers = tx_sql['transfers_array']
            is_transaction_mint = tx_sql['hash'] in self.txs_sql_mints.keys()

            if list(self.txs_sql_mints.keys()) and tx_sql['hash'] == list(self.txs_sql_mints.keys())[0]:
                assert self.holders_before_first_mint == [], f"{self.holders_before_first_mint} {self.address}"
                self.holders_before_first_mint = list(set(self.balances.keys()) - set([self.address] + self.owner_addresses + self.pairs_list + [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]))
                self.print(f"self.holders_before_first_mint={self.holders_before_first_mint}")


            if 1 or not self.can_be_old_token:
                if tx_sql['block_number'] < 21022453: # c этого блока началось индексирование total_supply
                    state_change = utils.get_state_change(testnet=_testnet, tx={'hash':tx_sql['hash']}, miner=tx_sql['block_miner'], method='trace_replayTransaction', verbose=self.verbose) # trace_replayTransaction|debug_traceCall
                    tx_sql['bribe_eth'] = utils.calc_bribe(state_diff=state_change['post'] - state_change['pre'], gasUsed=tx_sql['gasused'], maxPriorityFeePerGas=tx_sql['maxpriorityfeepergas']) if state_change != None else None

                tx_sql['bribe_eth'] = float(tx_sql['bribe_eth']) if tx_sql['bribe_eth'] != None else tx_sql['bribe_eth']
                self.print(f"bribe_eth={tx_sql['bribe_eth']} ETH")
            else:
                tx_sql['bribe_eth'] = None

            tx_sql['p_i'] = self.total_p_i

            if self.transaction_absolute_first == None:
                self.transaction_absolute_first = tx_sql
            

            has_in_token, has_out_token = False, False
            has_in_other, has_out_other = False, False
            how_much_usd_was_spent = {} # здесь заложено сколько $ было потрачено без учета self.address
            how_much_usd_was_received = {} # здесь заложено сколько $ было получено без учета self.address
            how_much_native_spent, how_much_native_received = {}, {} # значение в value токена
            action_with_token = None
            what_pair_was_traded = None
            trans_volume_buy, trans_volume_sell = 0, 0
            addresses_received_native_token = []
            #pair_addresses_in_trans_transfers = []

            for i, trf in enumerate(transaction_transfers):
                if transaction_transfers[i]['address'] != None:
                    transaction_transfers[i]['address'] = to_checksum_address(transaction_transfers[i]['address'])
                if transaction_transfers[i]['from'] != None:
                    transaction_transfers[i]['from'] = to_checksum_address(transaction_transfers[i]['from'])
                if transaction_transfers[i]['to'] != None:
                    transaction_transfers[i]['to'] = to_checksum_address(transaction_transfers[i]['to'])
                trf = transaction_transfers[i]

                # Wei failed transaction
                if trf['address'] == None and trf['status'] == 0:
                    continue
                if trf['address'] == None and trf['value'] == 0:
                    continue

                if trf['from'] and trf['from'] not in how_much_usd_was_spent: how_much_usd_was_spent[trf['from']] = 0
                if trf['to'] and trf['to'] not in how_much_usd_was_received: how_much_usd_was_received[trf['to']] = 0


                if trf['address'] == [None, utils.WETH, utils.USDC, utils.USDT]: # Wei

                    if trf['from'] == tx_sql['from']:
                        has_out_other = True
                    if trf['to'] == tx_sql['from']:
                        has_in_other = True

                    if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                    if trf['to']: how_much_usd_was_received[trf['to']] += trf['value_usd']

                    # Titan Builder
                    # if trf['to'] == '0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97':
                    #     self.bribe_sent.append(float(from_wei(trf['value'], 'ether')))


                elif trf["address"] == self.address:

                    new_item = {
                        'value': 0,
                        'value_usd': None,
                        'history': [], # история пополнений или снятий value
                        'history_balance': [], # история value баланса
                        'invested': 0,
                        'withdrew': 0,
                        'volume_buy': 0,
                        'volume_sell': 0,
                        'trades_buy': 0,
                        'trades_sell': 0,
                        'transfers_list': {},
                        'transfers_sent': 0,
                        'transfers_sent_list_usd': {},
                        'transfers_sent_list_usd_total': 0,
                        'transfers_sent_list_perc': {},
                        'transfers_sent_list_perc_total': 0,
                        'transfers_received': 0,
                        'transfers_received_list_usd': {},
                        'transfers_received_list_perc': {},
                        'gas_usd': 0,  # total gas used $
                        'liquidity_first': {'p':trf['extra_data']['rp'], 't':trf['extra_data']['rt']} if trf['extra_data'] else None, # ликвидность usd в момент первого трейда
                        'realized_profit': 0, # $
                        'unrealized_profit': 0, # $
                        'pnl': 0, # $
                        'roi_r': 0,
                        'roi_u': 0,
                        'roi_t': 0,
                        'trades_list': {}, # нужно для вычисления avg_price для profits
                        'first_trade': tx_sql['block_timestamp'], # timestamp
                        'first_trade_block': tx_sql['block_number'],
                        'first_trade_index': tx_sql['transactionindex'],
                        'last_trade': tx_sql['block_timestamp'], # timestamp
                        'last_trade_block': tx_sql['block_number'],
                        'days': None, # days holding token
                        'router': None,
                        'stat_wallet_address': None,
                        'roi_by_blocks': {}, # key - номер блока, value - stat. первые 30m
                    }
                    if trf['from'] and trf['from'] not in self.balances:
                        if not(trf['from'] in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]):
                            self.balances[trf['from']] = {'i': len(self.balances)} | copy.deepcopy(new_item)
                    if trf['to'] and trf['to'] not in self.balances:
                        self.balances[trf['to']] = {'i': len(self.balances)} | copy.deepcopy(new_item)

                    if trf['from'] == tx_sql['from']:
                        has_out_token = True
                    if trf['to'] == tx_sql['from']:
                        has_in_token = True


                    # from
                    if trf['from'] and trf['from'] in self.balances:
                        self.balances[trf['from']]['history_balance'].append(self.balances[trf['from']]['value'])
                        self.balances[trf['from']]['history'].append(-trf['value'])
                        self.balances[trf['from']]['value'] -= trf['value']
                        self.balances[trf['from']]['last_trade_block'] = tx_sql['block_number']
                        self.balances[trf['from']]['last_p_i'] = self.total_p_i
                        self.balances[trf['from']]['last_trade'] = tx_sql['block_timestamp']
                        if self.balances[trf['from']]['value'] < 0:
                            self.print(f"      \x1b[31;1m(!)\x1b[0m balance for {trf['from']}={self.balances[trf['from']]['value']} < 0 on address={trf['address']}")

                        if trf['from'] not in how_much_native_spent: how_much_native_spent[trf['from']] = 0
                        how_much_native_spent[trf['from']] += trf['value']

                    # to
                    if trf['to']:

                        addresses_received_native_token.append(trf['to'])

                        if self.balances[trf['to']]['history'] or self.balances[trf['to']]['value'] != 0:
                            self.balances[trf['to']]['history_balance'].append(self.balances[trf['to']]['value'])
                        self.balances[trf['to']]['history'].append(trf['value'])
                        self.balances[trf['to']]['value'] += trf['value']
                        self.balances[trf['to']]['last_trade_block'] = tx_sql['block_number']
                        self.balances[trf['to']]['last_p_i'] = self.total_p_i
                        self.balances[trf['to']]['last_trade'] = tx_sql['block_timestamp']
                        
                        if trf['to'] not in how_much_native_received: how_much_native_received[trf['to']] = 0
                        how_much_native_received[trf['to']] += trf['value']
                    
                    if trf['value_usd_per_token_unit'] != None:
                        if trf['from'] and trf['from'] not in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]: self.balances[trf['from']]['value_usd'] = trf['value_usd_per_token_unit'] / 10 ** self.contract_data_token['token_decimals'] * self.balances[trf['from']]['value']
                        if trf['to'] and trf['to'] not in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]: self.balances[trf['to']]['value_usd'] = trf['value_usd_per_token_unit'] / 10 ** self.contract_data_token['token_decimals'] * self.balances[trf['to']]['value']
                    
                        if trf['to'] == tx_sql['from']:
                            self.balances[tx_sql['from']]['volume_buy'] += trf['value_usd']
                            if not is_transaction_mint:
                                trans_volume_buy += trf['value_usd']
                            self.print(f"покупка токена адресом {tx_sql['from']} value={trf['value'] / 10 ** self.contract_data_token['token_decimals']} на сумму {trf['value_usd']:.2f} $ (self.total_volume_buy={self.total_volume_buy:.2f} $)")
                        if trf['from'] == tx_sql['from']:
                            self.balances[tx_sql['from']]['volume_sell'] += trf['value_usd']
                            if not is_transaction_mint:
                                trans_volume_sell += trf['value_usd']
                            self.print(f"продажа/трансфер токена block={tx_sql['block_number']} адресом {tx_sql['from']} value={trf['value'] / 10 ** self.contract_data_token['token_decimals']} на сумму {trf['value_usd']:.2f} $ (self.total_volume_sell={self.total_volume_sell:.2f} $)")


                    if tx_sql['block_number'] not in self.blocks_data:
                        self.blocks_data[tx_sql['block_number']] = {
                            'volume_buy': 0,
                            'volume_sell': 0,
                            'trades_buy': 0,
                            'trades_sell': 0,
                            'trades_transfer': 0,
                            'invested': 0,
                            'invested_list': [],
                            #'invested_list_by_addr': [],
                            'withdrew': 0,
                            'withdrew_list': [],
                            #'withdrew_list_by_addr': [],
                            'total_supply': None,
                            'buy_addresses': [],
                            'sell_addresses': [],
                            'transfer_sent_addresses': [],
                            'transfer_received_addresses': [],
                            'buy_trans_indexes': [],
                            'sell_trans_indexes': [],
                            'transfer_trans_indexes': [],
                            'trades_list': [],
                            'jaredfromsubway': [],
                            # 'value_usd_per_token_unit': None,
                            'block_basefeepergas': tx_sql['block_basefeepergas'],
                            'transfers': 0,
                            'transfers_list_usd': [],
                            'transfers_list_perc': [],
                            'tx_eth_price': tx_eth_price,
                        }
                        self.print(f"Создали новый blocks_data={tx_sql['block_number']}")
                    self.blocks_data[tx_sql['block_number']]['timestamp'] = tx_sql['block_timestamp']
                    for _ in [0]:
                        # if trf['pair_address'] != None:
                        #     if to_checksum_address(trf['pair_address']) != self.primary_pair:
                        #         self.print(f"trf['pair_address']={to_checksum_address(trf['pair_address'])} != self.primary_pair={self.primary_pair} => не будем заносить в self.blocks_data[tx_sql['block_number']], trf={trf}")
                        #         break

                        if 'value_usd_per_token_unit' in self.blocks_data[tx_sql['block_number']]:
                            # if trf['pair_address'] != None:
                            #     if trf['pair_address'].lower() == self.primary_pair.lower():
                            #         assert self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'] == trf['value_usd_per_token_unit'] and self.blocks_data[tx_sql['block_number']]['extra_data'] == trf['extra_data'], f"address={self.address}, {self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit']} {trf['value_usd_per_token_unit']}, diff={100 - self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'] * 100 / trf['value_usd_per_token_unit']}%, extra_data in block={self.blocks_data[tx_sql['block_number']]['extra_data']}, trf['extra_data']={trf['extra_data']}, last_trf={pformat(self.blocks_data[tx_sql['block_number']]['last_trf'])},\n trf={pformat(trf)}, self.primary_pair={self.primary_pair}, tx={tx_sql['hash']}"
                            #     else:
                            #         self.print(f"trf['pair_address']={trf['pair_address']} != self.primary_pair={self.primary_pair}, trf['extra_data']['rp']={trf['extra_data']['rp']}, self.blocks_data[tx_sql['block_number']]['liquidity']={self.blocks_data[tx_sql['block_number']]['liquidity']}")
                            #         if trf['extra_data']['rp'] > self.blocks_data[tx_sql['block_number']]['liquidity']:
                            #             self.print(f"перезапишем значения self.blocks_data[tx_sql['block_number']], т.к. у нового trf больше значение ликвидности, вероятно изначально в блок записалось не верно из-за сортировки")
                            #         else:
                            #             break
                            if trf['extra_data'] != None and self.blocks_data[tx_sql['block_number']]['liquidity'] != None and trf['extra_data']['rp'] > self.blocks_data[tx_sql['block_number']]['liquidity']:
                                self.print(f"перезапишем значения self.blocks_data[tx_sql['block_number']], т.к. у нового trf больше значение ликвидности {trf['extra_data']['rp']} > {self.blocks_data[tx_sql['block_number']]['liquidity']}\n  last_trf={self.blocks_data[tx_sql['block_number']]['last_trf']},\n  trf={trf}")
                            else:
                                break

                        self.value_usd_per_token_unit = trf['value_usd_per_token_unit']
                        self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'] = trf['value_usd_per_token_unit']
                        self.blocks_data[tx_sql['block_number']]['extra_data'] = trf['extra_data']
                        self.blocks_data[tx_sql['block_number']]['liquidity'] = trf['extra_data']['rp'] if trf['extra_data'] else None
                        self.blocks_data[tx_sql['block_number']]['last_trf'] = trf
                        # if trf['pair_address'] != None and to_checksum_address(trf['pair_address']) not in pair_addresses_in_trans_transfers:
                        #     pair_addresses_in_trans_transfers.append(to_checksum_address(trf['pair_address']))


                    if tx_sql['block_number'] >= 21022453: # c этого блока началось индексирование total_supply
                        if self.blocks_data[tx_sql['block_number']]['total_supply'] != None:
                            assert self.blocks_data[tx_sql['block_number']]['total_supply'] == trf['total_supply'], f"self.blocks_data[tx_sql['block_number']]['total_supply'] != trf['total_supply'] {self.blocks_data[tx_sql['block_number']]['total_supply']} != {trf['total_supply']}, address={self.address}"
                        self.blocks_data[tx_sql['block_number']]['total_supply'] = trf['total_supply']
                    else:
                        if self.blocks_data[tx_sql['block_number']]['total_supply'] == None:
                            self.blocks_data[tx_sql['block_number']]['total_supply'] = utils.contract_totalSupply(w3=w3, address=self.address, block_identifier=tx_sql['block_number'])
                            self.print(f"Сделали обращение в utils.contract_totalSupply(), т.к. total_supply был None на текущем блоке, теперь он {self.blocks_data[tx_sql['block_number']]['total_supply']}")
                            if self.blocks_data[tx_sql['block_number']]['total_supply'] == None:
                                self.print(f"  т.к. None осталось, изменим на self.total_supply={self.total_supply}")
                                self.blocks_data[tx_sql['block_number']]['total_supply'] = self.total_supply

                    if self.blocks_data[tx_sql['block_number']]['total_supply'] != None and not self.can_be_old_token:
                        self.total_supply = self.blocks_data[tx_sql['block_number']]['total_supply']

                else:

                    if trf['from'] == tx_sql['from']:
                        has_out_other = True
                    if trf['to'] == tx_sql['from']:
                        has_in_other = True
                    
                    if trf['value_usd_per_token_unit'] != None:
                        if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                        if trf['to']: how_much_usd_was_received[trf['to']] += trf['value_usd']


                 
            # -----



            # if pair_addresses_in_trans_transfers and 'value_usd_per_token_unit' in self.blocks_data[tx_sql['block_number']]:
            #     if self.primary_pair not in pair_addresses_in_trans_transfers:
            #         self.print(f"Похоже транзакция {tx_sql['block_number']} {tx_sql['hash']} была с парой не self.primary_pair={self.primary_pair}, пропускаем ее, pair_addresses_in_trans_transfers={pair_addresses_in_trans_transfers}", force=True)
            #         if self.blocks_data[tx_sql['block_number']]['trades_buy'] == 0 and self.blocks_data[tx_sql['block_number']]['trades_sell'] == 0 and self.blocks_data[tx_sql['block_number']]['trades_transfer'] == 0 and self.blocks_data[tx_sql['block_number']]['invested'] == 0 and self.blocks_data[tx_sql['block_number']]['withdrew'] == 0:
            #             self.print(f"  => удаляем self.blocks_data[{tx_sql['block_number']}]", force=True)
            #             del self.blocks_data[tx_sql['block_number']]
            #         continue


            self.print('how_much_usd_was_spent $: # сколько $ потратил каждый кошелек в данной транзакции')
            for adr, value in how_much_usd_was_spent.items():
                self.print(f"  {utils.print_address(adr, token_address=self.address, pairs=self.pairs_list)}: {value}")

            self.print('how_much_usd_was_received $: # сколько $ получил каждый кошелек в данной транзакции')
            for adr, value in how_much_usd_was_received.items():
                self.print(f"  {utils.print_address(adr, token_address=self.address, pairs=self.pairs_list)}: {value}")
            self.print('total values spent/received $: # их разница')
            for adr in list(set(list(how_much_usd_was_spent.keys()) + list(how_much_usd_was_received.keys()))):
                _sum = 0
                if adr in how_much_usd_was_spent: _sum -= how_much_usd_was_spent[adr]
                if adr in how_much_usd_was_received: _sum += how_much_usd_was_received[adr]
                self.print(f"  {utils.print_address(adr, token_address=self.address, pairs=self.pairs_list)}: {_sum:.2f}")


            if self.total_trades_buy == 0 and self.total_trades_sell == 0 and tx_sql['from'] in how_much_usd_was_spent and tx_sql['from'] in self.owner_addresses:
                del how_much_usd_was_spent[tx_sql['from']]
                self.print(f"Т.к. здесь Transfer() owner'a после Mint(), удалим его del how_much_usd_was_spent[{tx_sql['from']}]")
        

            is_event_trade = False
            tx_sql['event'] = None

            if has_in_token and has_out_other or has_out_token and has_in_other: is_event_trade = True
            elif has_in_token and not has_out_other: tx_sql['event'] = 'receive'
            elif has_out_token and not has_in_other: tx_sql['event'] = 'send'
            else: tx_sql['event'] = '-'

            self.print(f"trans_volume_buy: {trans_volume_buy:.2f} $, trans_volume_sell: {trans_volume_sell:.2f} $")

            if tx_sql['from'] in self.balances and tx_sql['to'] not in [self.address, utils.UNISWAP_V2_ROUTER, utils.UNISWAP_V3_ROUTER, utils.UNISWAP_UNIVERSAL_ROUTER]:
                if tx_sql['to'] in self.txs_sql_to_addresses.keys():
                    self.balances[tx_sql['from']]['router'] = tx_sql['to']
                    self.balances[self.txs_sql_to_addresses[tx_sql['to']]]['router'] = tx_sql['to']

                if tx_sql['to'] not in self.txs_sql_to_addresses:
                    self.txs_sql_to_addresses[tx_sql['to']] = tx_sql['from']

            if tx_sql['from'] not in how_much_usd_was_spent \
                and tx_sql['to'] in how_much_usd_was_spent \
                and how_much_usd_was_spent[tx_sql['to']] > 0 \
                and tx_sql['from'] not in how_much_native_spent \
                and tx_sql['from'] in how_much_native_received \
                and how_much_native_received[tx_sql['from']] > 0:
                # пример такой транзакции 0xe0b8720f90f844df4a1533cf85f5459e0aa63a1ef881bae77f0e30219fdf5584
                # может еще на 0xc2d9d1dfbf4ee5e1e01dc56397fa850349df07a07092f06657ca18d9a38c7828
                self.print(f"Похоже покупка была через Swap from контракта, меняем how_much_usd_was_spent для tx_sql[from]={how_much_usd_was_spent[tx_sql['to']]} $")
                how_much_usd_was_spent[tx_sql['from']] = how_much_usd_was_spent[tx_sql['to']]
                is_event_trade = True
                tx_sql['event'] = None
                self.total_volume_buy += trans_volume_buy
                # self.balances[tx_sql['from']]['router'] = '_contr_'
            



            if tx_sql['event'] in ['receive', 'send']:
                if self.total_p_i != 0:
                    for i, trf in enumerate(transaction_transfers):
                        if trf["address"] == self.address:

                            if trf['from'] not in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:
                                self.balances[trf['from']]['transfers_sent'] += 1
                                if tx_sql['block_number'] not in self.balances[trf['from']]['transfers_sent_list_perc']:
                                    self.balances[trf['from']]['transfers_sent_list_perc'][tx_sql['block_number']] = []
                                if self.total_supply:
                                    self.balances[trf['from']]['transfers_sent_list_perc'][tx_sql['block_number']].append(100 * trf['value'] / self.total_supply)
                                    self.balances[trf['from']]['transfers_sent_list_perc_total'] += 100 * trf['value'] / self.total_supply
                                if self.value_usd_per_token_unit:
                                    # self.balances[trf['from']]['transfers_sent_list_usd'].append(trf['value_usd'] if trf['value_usd'] != None else self.value_usd_per_token_unit * trf['value'])
                                    if tx_sql['block_number'] not in self.balances[trf['from']]['transfers_sent_list_usd']:
                                        self.balances[trf['from']]['transfers_sent_list_usd'][tx_sql['block_number']] = []
                                    self.balances[trf['from']]['transfers_sent_list_usd'][tx_sql['block_number']].append(self.value_usd_per_token_unit * trf['value'])
                                    self.balances[trf['from']]['transfers_sent_list_usd_total'] += self.value_usd_per_token_unit * trf['value']

                                # 'transfers_list': {
                                #     <номер_блока>: [{'q': 429279767.96145254, 'a': <address>, 't': s/r}], # type sent/received, т.е. был отправлен кому трансфер или от кого получен
                                # },
                                if tx_sql['block_number'] not in self.balances[trf['from']]['transfers_list']:
                                    self.balances[trf['from']]['transfers_list'][tx_sql['block_number']] = []
                                self.balances[trf['from']]['transfers_list'][tx_sql['block_number']].append({
                                    'q': trf['value'] / 10 ** self.contract_data_token['token_decimals'],
                                    'a': trf['to'],
                                    't': 's',
                                })
                            

                            if tx_sql['block_number'] not in self.balances[trf['to']]['transfers_list']:
                                self.balances[trf['to']]['transfers_list'][tx_sql['block_number']] = []
                            self.balances[trf['to']]['transfers_list'][tx_sql['block_number']].append({
                                'q': trf['value'] / 10 ** self.contract_data_token['token_decimals'],
                                'a': trf['from'],
                                't': 'r',
                            })

                            self.balances[trf['to']]['transfers_received'] += 1
                            if tx_sql['block_number'] not in self.balances[trf['to']]['transfers_received_list_perc']:
                                self.balances[trf['to']]['transfers_received_list_perc'][tx_sql['block_number']] = []
                            if self.total_supply:
                                self.balances[trf['to']]['transfers_received_list_perc'][tx_sql['block_number']].append(100 * trf['value'] / self.total_supply)
                            if self.value_usd_per_token_unit:
                                # self.balances[trf['to']]['transfers_received_list_usd'].append(trf['value_usd'] if trf['value_usd'] != None else self.value_usd_per_token_unit * trf['value'])
                                if tx_sql['block_number'] not in self.balances[trf['to']]['transfers_received_list_usd']:
                                    self.balances[trf['to']]['transfers_received_list_usd'][tx_sql['block_number']] = []
                                self.balances[trf['to']]['transfers_received_list_usd'][tx_sql['block_number']].append(self.value_usd_per_token_unit * trf['value'])

                            self.blocks_data[tx_sql['block_number']]['transfers'] += 1
                            if self.total_supply:
                                self.blocks_data[tx_sql['block_number']]['transfers_list_perc'].append(100 * trf['value'] / self.total_supply)
                            if self.value_usd_per_token_unit:
                                self.blocks_data[tx_sql['block_number']]['transfers_list_usd'].append(self.value_usd_per_token_unit * trf['value'])
                                #self.total_volume_transfer += trf['value_usd'] if trf['value_usd'] != None else self.value_usd_per_token_unit * trf['value']
                                self.total_volume_transfer += self.value_usd_per_token_unit * trf['value']
                            self.total_trades_transfer += 1
                            self.blocks_data[tx_sql['block_number']]['total_volume_transfer'] = self.total_volume_transfer
                            self.blocks_data[tx_sql['block_number']]['total_trades_transfer'] = self.total_trades_transfer
                            if trf['from'] not in self.blocks_data[tx_sql['block_number']]['transfer_sent_addresses']:
                                self.blocks_data[tx_sql['block_number']]['transfer_sent_addresses'].append(trf['from'])
                            if trf['to'] not in self.blocks_data[tx_sql['block_number']]['transfer_received_addresses']:
                                self.blocks_data[tx_sql['block_number']]['transfer_received_addresses'].append(trf['to'])




            if tx_sql['from'] in how_much_native_spent:
                self.print(f"how_much_native_spent[{tx_sql['from']}]: {how_much_native_spent[tx_sql['from']] / 10 ** self.contract_data_token['token_decimals']} # сколько текущего токена ушло от кошелька данной транзакции")
            else:
                self.print(f"how_much_native_spent[{tx_sql['from']}]: - # сколько текущего токена ушло от кошелька данной транзакции")
            if tx_sql['from'] in how_much_native_received:
                self.print(f"how_much_native_received[{tx_sql['from']}]: {how_much_native_received[tx_sql['from']] / 10 ** self.contract_data_token['token_decimals']} # сколько текущего токена пришло на кошелек данной транзакции")
            else:
                self.print(f"how_much_native_received[{tx_sql['from']}]: - # сколько текущего токена пришло на кошелек данной транзакции")

            self.print('how_much_native_spent: # сколько текущего токена ушло с кошельков, которых затронула данная транзакция')
            for adr, value in how_much_native_spent.items():
                if self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit']:
                    self.print(f"  {utils.print_address(adr, token_address=self.address, pairs=self.pairs_list)}: {value} ({(value * self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit']):.2f} $)")
                else:
                    self.print(f"  {utils.print_address(adr, token_address=self.address, pairs=self.pairs_list)}: {value}")
            self.print('how_much_native_received: # сколько текущего токена пришло на кошельки, которых затронула данная транзакция')
            for adr, value in how_much_native_received.items():
                if self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit']:
                    self.print(f"  {utils.print_address(adr, token_address=self.address, pairs=self.pairs_list)}: {value} ({(value * self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit']):.2f} $)")
                else:
                    self.print(f"  {utils.print_address(adr, token_address=self.address, pairs=self.pairs_list)}: {value}")
            
            # if len(how_much_native_spent) > 1:
            #     self.print(f"\x1b[31;1m(!)\x1b[0m how_much_native_spent > 1 ({len(how_much_native_spent)})")
            # if len(how_much_native_received) > 1:
            #     self.print(f"\x1b[31;1m(!)\x1b[0m how_much_native_received > 1 ({len(how_much_native_received)})")
        
            if tx_sql['from'] not in how_much_native_received and len(how_much_native_received) == 1:

                if list(how_much_native_received.keys())[0] not in [self.address] + self.pairs_list:
                    self.print(f"Похоже был 1 Swap в транзакции через контракт, меняем how_much_native_received[{tx_sql['from']}]=None -> {how_much_native_received[list(how_much_native_received.keys())[0]]}")
                    how_much_native_received[tx_sql['from']] = how_much_native_received[list(how_much_native_received.keys())[0]]
                    
                    # (!) тут надо исправить
                    # сделать from=tx_sql['from'] и поменять from на tx_sql['to'], так как токены пришли не на from, а на to, так как через контракт вызывалось
                    # пример 0x68ec61bf38f748fef6efd1d252597af1b8f357fd81d0ce8f7401e1d028b46202

                    # (!) и потом добавить стату как много контрактов держат токен, а не обычных кошельков

                    # b1 = utils.contract_balanceOf(w3, token_address=self.address, wallet_address='0x338B9920971Dcaf5b6d83b6b292231110d3Bec40', block_identifier=tx_sql['block_number'])
                    # b2 = utils.contract_balanceOf(w3, token_address=self.address, wallet_address='0x79C06846633123d23Ed6a71C456BB2fF09129938', block_identifier=tx_sql['block_number'])
                    # print('b1:', b1)
                    # print('b2:', b2)
                    # assert 0, f"address={self.address}, p_i={p_i}: тут проверить, нужно ли менять how_much_native_received "


            if is_event_trade and tx_sql['block_number'] in self.blocks_data and not is_transaction_mint:

                if trf['value_usd'] == None:
                    self.print(f"!!!! {self.address} trf[value_usd] == None")
                else:
                    self.blocks_data[tx_sql['block_number']]['volume_buy'] += trans_volume_buy
                    self.blocks_data[tx_sql['block_number']]['volume_sell'] += trans_volume_sell
                    if tx_sql['from'] in how_much_native_received and how_much_native_received[tx_sql['from']] > 0:
                        self.blocks_data[tx_sql['block_number']]['trades_buy'] += 1
                        self.total_trades_buy += 1
                        self.total_volume_buy += trf['value_usd']
                        self.blocks_data[tx_sql['block_number']]['buy_addresses'].append(tx_sql['from'])
                        self.blocks_data[tx_sql['block_number']]['buy_trans_indexes'].append(tx_sql['transactionindex'])
                        tx_sql['event'] = 'buy'
                    if tx_sql['from'] in how_much_native_spent and how_much_native_spent[tx_sql['from']] > 0:
                        # это условие на транзакции Mint() owner'ом
                        if self.total_trades_buy == 0 and self.total_trades_sell == 0 and tx_sql['from'] in self.owner_addresses:
                            tx_sql['event'] = '-'
                        else:
                            self.blocks_data[tx_sql['block_number']]['trades_sell'] += 1
                            self.total_trades_sell += 1
                            self.total_volume_sell += trf['value_usd']
                            self.blocks_data[tx_sql['block_number']]['sell_addresses'].append(tx_sql['from'])
                            self.blocks_data[tx_sql['block_number']]['sell_trans_indexes'].append(tx_sql['transactionindex'])
                            tx_sql['event'] = 'sell'

                    self.print(f"по итогу транзакции:")
                    self.print(f"  blocks_data[trades_buy]={self.blocks_data[tx_sql['block_number']]['trades_buy']}")
                    self.print(f"  blocks_data[trades_sell]={self.blocks_data[tx_sql['block_number']]['trades_sell']}")
                    self.print(f"  blocks_data[trades_transfer]={self.blocks_data[tx_sql['block_number']]['trades_transfer']}")
                    self.print(f"  trans_volume_buy={trans_volume_buy:.2f} $, trans_volume_sell={trans_volume_sell:.2f} $, trans_volume_transfer= ? ")
            


            if tx_sql['from'] not in how_much_native_received: how_much_native_received[tx_sql['from']] = 0
            if tx_sql['from'] not in how_much_native_spent: how_much_native_spent[tx_sql['from']] = 0


            if tx_sql['event'] == 'buy':
                self.print(f"transaction event type: {Fore.GREEN}\033[1m{tx_sql['event']}\033[0m{Style.RESET_ALL}")
            elif tx_sql['event'] == 'sell':
                self.print(f"transaction event type: {Fore.RED}\033[1m{tx_sql['event']}\033[0m{Style.RESET_ALL}")
            elif tx_sql['event'] in ['receive', 'send']:
                self.print(f"transaction event type: {Fore.BLUE}\033[1m{tx_sql['event']}\033[0m{Style.RESET_ALL}")
            else:
                self.print(f"transaction event type: \033[1m{tx_sql['event']}\033[0m")


            if is_event_trade and self.first_trade_tx_sql == None and tx_sql['from'] not in self.owner_addresses:
                self.print(f"случился первый трейд")
                self.first_trade_tx_sql = tx_sql

                self.total_holders_before_first_trade = len(set(self.balances.keys()) - set([self.address] + self.owner_addresses + self.pairs_list + [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD])) - 1
                self.print(f"self.total_holders_before_first_trade={self.total_holders_before_first_trade}")

                # total_supply = utils.contract_totalSupply(w3=w3, address=self.address, block_identifier=tx_sql['block_number'])
                # print(f"{total_supply}=total_supply")
                # print(f"{self.get_total_supply} - class supply")
                # pprint(self.txs_sql_mints[list(self.txs_sql_mints.keys())[0]])
                # exit()

            _invested = None
            if tx_sql['from'] in how_much_usd_was_spent:
                if has_in_token:
                    _invested = how_much_usd_was_spent[tx_sql['from']] - how_much_usd_was_received[tx_sql['from']]
                    if tx_sql['bribe_eth'] != None:
                        _invested_no_bribe = _invested - to_wei(tx_sql['bribe_eth'], 'ether') / tx_eth_price
                    else:
                        _invested_no_bribe = _invested
                    # assert _invested >= 0, f"_invested={_invested}, address={self.address}"
                    if _invested < 0:
                        self.print(f"\x1b[31;1m(!)\x1b[0m _invested={_invested}, address={self.address}")
                        # if not self.realtime:
                        #     assert 0
                    if _invested_no_bribe < 0:
                        self.print(f"\x1b[31;1m(!)\x1b[0m _invested_no_bribe={_invested_no_bribe}, address={self.address}")
                    if _invested == 0:
                        self.print(f"\x1b[31;1m(!)\x1b[0m how_much_usd_was_spent[tx_sql['from']] - how_much_usd_was_received[tx_sql['from']] == 0")
                        #exit("op")
                    self.print(f"invested={(_invested):.2f} $ # сколько итого суммарно $ ушло с кошелька данной транзакции ({tx_sql['from']})")
                    if _invested != _invested_no_bribe:
                        self.print(f"_invested_no_bribe={(_invested_no_bribe):.2f} $")
                    self.blocks_data[tx_sql['block_number']]['invested'] += _invested_no_bribe
                    self.blocks_data[tx_sql['block_number']]['invested_list'].append(_invested_no_bribe)
                    #self.blocks_data[tx_sql['block_number']]['invested_list_by_addr'].append({tx_sql['from']: _invested_no_bribe})
                    self.balances[tx_sql['from']]['invested'] += _invested_no_bribe
                    self.balances[tx_sql['from']]['trades_buy'] += 1
                    self.total_invested += _invested_no_bribe
            _withdrew = None
            if tx_sql['from'] in how_much_usd_was_received:
                if self.total_trades_buy == 0 and self.total_trades_sell == 0 and tx_sql['from'] in self.owner_addresses:
                    pass
                elif has_out_token:
                    _withdrew = how_much_usd_was_received[tx_sql['from']] - how_much_usd_was_spent[tx_sql['from']]
                    # assert _withdrew >= 0, f"_withdrew={_withdrew}, address={self.address}"
                    if tx_sql['bribe_eth'] != None:
                        _withdrew_no_bribe = _withdrew + to_wei(tx_sql['bribe_eth'], 'ether') / tx_eth_price
                    else:
                        _withdrew_no_bribe = _withdrew
                    if _withdrew_no_bribe < 0:
                        self.print(f"\x1b[31;1m(!)\x1b[0m", force=True)
                        self.print(f"\x1b[31;1m(!)\x1b[0m", force=True)
                        self.print(f"\x1b[31;1m(!)\x1b[0m _withdrew_no_bribe={_withdrew_no_bribe}, bribe={round(to_wei(tx_sql['bribe_eth'], 'ether') / tx_eth_price) if tx_sql['bribe_eth'] != None else None}$, address={self.address}", force=True)
                        self.print(f"\x1b[31;1m(!)\x1b[0m", force=True)
                        self.print(f"\x1b[31;1m(!)\x1b[0m", force=True)
                        _withdrew_no_bribe = 0
                        # if not self.realtime:
                        #     assert 0, f"_withdrew < 0, tx_sql['from']={tx_sql['from']}"
                    if _withdrew == 0:
                        self.print(f"\x1b[31;1m(!)\x1b[0m how_much_usd_was_received[tx_sql['from']] - how_much_usd_was_spent[tx_sql['from']] == 0")
                        #exit('op2')
                    self.print(f"withdrew={(_withdrew):.2f} $ # сколько итого суммарно $ пришло на кошелек данной транзакции ({tx_sql['from']})")
                    if _withdrew != _withdrew_no_bribe:
                        self.print(f"_withdrew_no_bribe={(_withdrew_no_bribe):.2f} $")
                    self.blocks_data[tx_sql['block_number']]['withdrew'] += _withdrew_no_bribe
                    self.blocks_data[tx_sql['block_number']]['withdrew_list'].append(_withdrew_no_bribe)
                    #self.blocks_data[tx_sql['block_number']]['withdrew_list_by_addr'].append({tx_sql['from']: _withdrew_no_bribe})
                    self.balances[tx_sql['from']]['withdrew'] += _withdrew_no_bribe
                    self.balances[tx_sql['from']]['trades_sell'] += 1
                    self.total_withdrew += _withdrew_no_bribe

            # адрес может сам себе отправить токен. пример: 0x4172a1aea7b53340ecc3dcdf54d9d16308aa719b53e412766c84bfd1c67e48d1
            # assert not(tx_sql['from'] in how_much_usd_was_spent and has_in_token and tx_sql['from'] in how_much_usd_was_received and has_out_token), f"address={self.address}, p_i={p_i}"

            self.print(f"self.total_trades_buy: {self.total_trades_buy}, self.total_volume_buy: {self.total_volume_buy:.2f} $")
            self.print(f"self.total_trades_sell: {self.total_trades_sell}, self.total_volume_sell: {self.total_volume_sell:.2f} $")
            self.print(f"self.total_trades_transfer: {self.total_trades_transfer}, self.total_volume_transfer: {self.total_volume_transfer:.2f} $")
            self.print(f"self.total_invested={self.total_invested:.2f}$, self.total_withdrew={self.total_withdrew:.2f}$")

            
            if tx_sql['from'] in self.balances:
                self.balances[tx_sql['from']]['gas_usd'] += float(tx_sql['gas_usd'])

            # for address, router in FILE_LABELS['ethereum']['routers'].items():
            #     if address == tx_sql['to']:
            #         assert tx_sql['from'] in self.balances.keys(), self.address
            #         self.balances[tx_sql['from']]['router'] = router
            #         break


            if tx_sql['block_number'] in self.blocks_data:
                self.print(f"self.blocks_data на блоке {tx_sql['block_number']}:")
                self.print(pformat(self.blocks_data[tx_sql['block_number']]))
        


            if not self.realtime:
                if self.first_trade_tx_sql != None:

                    if 1 or not self.realtime or self.realtime and tx_sql['block_number'] in last_N_blocks_for_erc20_stat: # для performance

                        _adr_list = [tx_sql['from']]
                        if tx_sql['block_number'] in self.blocks_data:
                            _adr_list = list(set(_adr_list + self.blocks_data[tx_sql['block_number']]['transfer_received_addresses']))
                        if self.first_trade_tx_sql['block_number'] == tx_sql['block_number']:
                            _adr_list += self.holders_before_first_mint

                        for adr in _adr_list:
                            if adr in self.balances and (self.balances[adr]['stat_wallet_address'] == None or (tx_sql['block_number'] - max(self.wallet_stats_by_blocks[adr].keys())) * 12 / 60 / 60 > self.wallet_stats_cache): # часов

                                start_time = time.time()
                                self.balances[adr]['stat_wallet_address'] = utils.get_stat_wallets_address(w3=w3_e, address=adr, last_block=tx_sql['block_number'], token_address=self.address, verbose=0)

                                if 'stat_wallet_address_first' not in self.balances[adr] or not self.balances[adr]['stat_wallet_address_first']:
                                    self.balances[adr]['stat_wallet_address_first'] = copy.deepcopy(self.balances[adr]['stat_wallet_address'])


                                self.print(f"на блоке {tx_sql['block_number']} при last_block={self.last_block} сделан подсчет за {(time.time() - start_time):.4f}s get_stat_wallets_address() для address={adr}")

                                if adr in self.wallet_stats_by_blocks:
                                    self.print(f"  был последний запрос на stats на блоке {max(self.wallet_stats_by_blocks[adr])}, это {round((tx_sql['block_number'] - max(self.wallet_stats_by_blocks[adr].keys())) * 12 / 60 / 60, 2)}ч назад => делаем повторно")
                                

                                if self.balances[adr]['stat_wallet_address']['first_in_transaction_from_address'] not in self.first_in_transaction_from_address_rating:
                                    self.first_in_transaction_from_address_rating[self.balances[adr]['stat_wallet_address']['first_in_transaction_from_address']] = 0
                                self.first_in_transaction_from_address_rating[self.balances[adr]['stat_wallet_address']['first_in_transaction_from_address']] += 1

                                if 1 or adr not in self.wallet_stats_by_blocks:
                                    self.wallet_stats_by_blocks[adr] = {}
                                self.wallet_stats_by_blocks[adr][tx_sql['block_number']] = copy.deepcopy(self.balances[adr]['stat_wallet_address'])

                                if self.owner_addresses and adr == self.owner_addresses[0]:
                                    self.stat_erc20_first_owner = self.balances[adr]['stat_wallet_address_first']
                        

                    if self.stat_erc20_first_owner == None and self.owner_addresses:

                        self.stat_erc20_first_owner = utils.get_stat_wallets_address(w3=w3_e, address=self.owner_addresses[0], last_block=tx_sql['block_number'], token_address=self.address, verbose=0)

                        if self.owner_addresses[0] in self.balances and 'stat_wallet_address_first' not in self.balances[self.owner_addresses[0]]:
                            self.balances[self.owner_addresses[0]]['stat_wallet_address'] = copy.deepcopy(self.stat_erc20_first_owner)
                            self.balances[self.owner_addresses[0]]['stat_wallet_address_first'] = copy.deepcopy(self.stat_erc20_first_owner)

                            if self.owner_addresses[0] not in self.wallet_stats_by_blocks:
                                self.wallet_stats_by_blocks[self.owner_addresses[0]] = {}
                            self.wallet_stats_by_blocks[self.owner_addresses[0]][tx_sql['block_number']] = copy.deepcopy(self.stat_erc20_first_owner)

            # -----


            if tx_sql['block_number'] in self.blocks_data:
                if is_event_trade and self.first_trade_tx_sql != None and self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'] == None:
                    self.print(f"\x1b[31;1m(!)\x1b[0m tx_sql['event']={tx_sql['event']}, но self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'] == None, p_i={p_i}/{self.total_p_i}, tx={tx_sql['hash']}, address={self.address}", force=True)

                    if 0:
                        for block_ in reversed(self.blocks_data.keys()):
                            if self.blocks_data[block_]['value_usd_per_token_unit'] != None:
                                self.print(f"  => сделали фикс, пройдя циклом назад до блока {block_}, value_usd_per_token_unit={self.blocks_data[block_]['value_usd_per_token_unit']}", force=True)
                                self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'] = self.blocks_data[block_]['value_usd_per_token_unit']
                                break


            if tx_sql['block_number'] in self.blocks_data and self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'] != None:


                # (!) это условие убрать
                # условие ниже на how_much_was_received из-за ошибки на 0x4aBD5745F326932B1b673bfa592a20d7BB6bc455
                if tx_sql['from'] in self.balances and tx_sql['from'] in how_much_usd_was_received:

                    avg_price = None

                    maxFeePerGas = tx_sql['maxfeepergas'] if tx_sql['maxfeepergas'] != None else tx_sql['block_basefeepergas']
                    maxPriorityFeePerGas = tx_sql['maxpriorityfeepergas'] if tx_sql['maxpriorityfeepergas'] != None else 0
                    _trade = {
                        't': tx_sql['block_timestamp'], # timestamp
                        'bribe': tx_sql['bribe_eth'],
                        'gas_usd': tx_sql['gas_usd'],
                        'index': tx_sql['transactionindex'],
                        'gas_1': maxFeePerGas / tx_sql['block_basefeepergas'],
                        'gas_2': maxPriorityFeePerGas / tx_sql['block_basefeepergas'],
                        'gas_3': maxFeePerGas / maxPriorityFeePerGas if maxPriorityFeePerGas != 0 else maxFeePerGas,
                        'gas_4': maxFeePerGas / tx_sql['gasprice'],
                    }

                    if how_much_native_received[tx_sql['from']] > 0 and how_much_native_spent[tx_sql['from']] > 0:
                        self.print(f"\x1b[31;1m(!)\x1b[0m how_much_native_received[tx_sql['from']] > 0 and how_much_native_spent[tx_sql['from']] > 0, address={self.address}", force=True)

                    if how_much_native_received[tx_sql['from']] > 0:

                        if tx_sql['from'] in how_much_usd_was_spent:
                            new_value_usd_per_token_unit = (how_much_usd_was_spent[tx_sql['from']] - how_much_usd_was_received[tx_sql['from']]) / (how_much_native_received[tx_sql['from']] / 10 ** self.contract_data_token['token_decimals'])
                        else:
                            new_value_usd_per_token_unit = self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit']
                        

                        if tx_sql['block_number'] not in self.balances[tx_sql['from']]['trades_list']:
                            self.balances[tx_sql['from']]['trades_list'][tx_sql['block_number']] = []
                        _trade |= {
                            'q': how_much_native_received[tx_sql['from']] / 10 ** self.contract_data_token['token_decimals'], # qnt
                            'vut': new_value_usd_per_token_unit,
                            's': 'buy', # side
                            'invested': _invested,
                        }
                        self.balances[tx_sql['from']]['trades_list'][tx_sql['block_number']].append(_trade)
                        self.blocks_data[tx_sql['block_number']]['trades_list'].append({'address': tx_sql['from']} | _trade)
                        # _sum_qnt_buy = sum([x['q'] for x in self.balances[tx_sql['from']]['trades_list'] if x['s'] == 'buy'])
                        # if _sum_qnt_buy != 0:
                        #     avg_price = sum([x['vut'] * x['q'] for x in self.balances[tx_sql['from']]['trades_list'] if x['s'] == 'buy']) / _sum_qnt_buy


                    if how_much_native_spent[tx_sql['from']] > 0:

                        if self.total_trades_buy == 0 and self.total_trades_sell == 0 and tx_sql['from'] in self.owner_addresses:
                            pass

                        else:
                            if tx_sql['from'] in how_much_usd_was_received:
                                new_value_usd_per_token_unit = (how_much_usd_was_received[tx_sql['from']] - how_much_usd_was_spent[tx_sql['from']]) / (how_much_native_spent[tx_sql['from']] / 10 ** self.contract_data_token['token_decimals'])
                            else:
                                new_value_usd_per_token_unit = self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit']

                            # new_value_usd_per_token_unit = tt['value_usd_per_token_unit']
                            # print('..:', new_value_usd_per_token_unit)

                            if tx_sql['block_number'] not in self.balances[tx_sql['from']]['trades_list']:
                                self.balances[tx_sql['from']]['trades_list'][tx_sql['block_number']] = []
                            _trade |= {
                                'q': how_much_native_spent[tx_sql['from']] / 10 ** self.contract_data_token['token_decimals'],
                                'vut': new_value_usd_per_token_unit,
                                's': 'sell',
                                'withdrew': _withdrew,
                            }
                            self.balances[tx_sql['from']]['trades_list'][tx_sql['block_number']].append(_trade)
                            self.blocks_data[tx_sql['block_number']]['trades_list'].append({'address': tx_sql['from']} | _trade)
                            _all_trades_list = list(itertools.chain.from_iterable(self.balances[tx_sql['from']]['trades_list'].values()))
                            _sum_qnt_buy = sum([x['q'] for x in _all_trades_list if x['s'] == 'buy'])
                            if _sum_qnt_buy != 0:
                                avg_price = sum([x['vut'] * x['q'] for x in _all_trades_list if x['s'] == 'buy']) / _sum_qnt_buy
                                profit_from_current_trade = (new_value_usd_per_token_unit - avg_price) * (how_much_native_spent[tx_sql['from']] / 10 ** self.contract_data_token['token_decimals'])
                                self.balances[tx_sql['from']]['realized_profit'] += profit_from_current_trade
                                #invested_sum = sum([x['price_usd_per_unit'] * x['qnt'] for x in self.wallet[log['address']]['trades_list'] if x['side'] == 'sell'])
                            else:
                                # tx_sql['critical_error'] = '_sum_qnt_buy = 0'
                                # здесь подсчет кошельков, у которых invested=0
                                profit_from_current_trade = new_value_usd_per_token_unit * (how_much_native_spent[tx_sql['from']] / 10 ** self.contract_data_token['token_decimals'])
                                self.balances[tx_sql['from']]['realized_profit'] += profit_from_current_trade


                    if tx_sql['from'] in self.balances:
                        self.update_balance_address(tx_sql['from'], self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'], tx_sql['block_timestamp'], block_number=tx_sql['block_number'], token_supply=self.blocks_data[tx_sql['block_number']]['total_supply'])



            if tx_sql['from'] in self.balances:
                self.print("self.balances[tx_sql['from']]['trades_list']:")
                self.print(pformat(self.balances[tx_sql['from']]['trades_list']))



            if not self.realtime and (self.verbose or len(self.blocks_data.keys()) <= self.full_log_text_limit):
                self.print(self.balance_table())
            
            if tx_sql['block_number'] in self.blocks_data:
                self.blocks_data[tx_sql['block_number']]['total_p_i'] = self.total_p_i
            
            if tx_sql['from'] == '0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13' or tx_sql['to'] in ['0x6b75d8AF000000e20B7a7DDf000Ba900b4009A80', '0x1f2F10D1C40777AE1Da742455c65828FF36Df387']: # jaredfromsubway
                
                _sum = 0
                for adr in ['0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13', '0x6b75d8AF000000e20B7a7DDf000Ba900b4009A80', '0x1f2F10D1C40777AE1Da742455c65828FF36Df387']:
                    if adr in how_much_usd_was_spent: _sum -= how_much_usd_was_spent[adr]
                    if adr in how_much_usd_was_received: _sum += how_much_usd_was_received[adr]

                if tx_sql['block_number'] in self.blocks_data:
                    self.blocks_data[tx_sql['block_number']]['jaredfromsubway'].append(abs(_sum)) 
                    #self.blocks_data[tx_sql['block_number']]['jaredfromsubway'].append(trans_volume_buy + trans_volume_sell)
                    self.print(f"появился jaredfromsubway, добавили в self.blocks_data[{tx_sql['block_number']}]['jaredfromsubway']: {self.blocks_data[tx_sql['block_number']]['jaredfromsubway']} (trans_volume_buy={trans_volume_buy:.2f}$, trans_volume_sell={trans_volume_sell:.2f}$, _invested={_invested}$, _withdrew={_withdrew}$)")
                else:
                    self.print(f"\x1b[31;1m(!)\x1b[0m появился jaredfromsubway, но почему-то {tx_sql['block_number']} нет в self.blocks_data, address={self.address}")

            if p_i + 1 == len(self.transactions_tupl) or tx_sql['block_number'] != self.transactions_tupl[p_i + 1]['block_number']:
                self.print(f"это последняя транзакция в данном блоке {tx_sql['block_number']}")

                if 1 or not self.realtime:
                    if tx_sql['block_number'] in self.blocks_data:
                        start_time = time.time()
                        self.update_balances(value_usd_per_token_unit=self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'], token_supply=self.blocks_data[tx_sql['block_number']]['total_supply'], last_tx_sql=tx_sql, start_time_block=start_time_block, w3=w3)
                        self.print(f"update_balances() выполнился за {(time.time() - start_time):.3f}s")
                        # if self.first_trade_tx_sql != None:
                        #     exit()
                        if make_break_at_last_block_transaction:
                            break


                # tx_sql['block_number'] может не быть в self.blocks_data, если был только Transfer()
                if not self.realtime and self.first_trade_tx_sql and tx_sql['block_number'] in self.blocks_data and not self.verbose:
                    self.print('----')

                    _total_trades = self.total_trades_buy + self.total_trades_sell
                    _minutes_past = (tx_sql['block_number'] - self.first_trade_tx_sql['block_number']) * 12 / 60
                    _len_traded_blocks_by_block_data = len([_block for _block, item in self.blocks_data.items() if _block >= self.first_trade_tx_sql['block_number'] and (item['trades_buy'] != 0 or item['trades_sell'] != 0)])

                    if _minutes_past <= 30:
                        _closest_trade = round(_total_trades / 10) * 15
                    elif _total_trades <= 3000:
                        _closest_trade = round(_total_trades / 25) * 25
                    else:
                        _closest_trade = round(_total_trades / 50) * 50

                    if _minutes_past <= 60:
                        _closest_minute = round(_minutes_past / 2) * 2
                    elif _minutes_past <= 180:
                        _closest_minute = round(_minutes_past / 3) * 3
                    elif _minutes_past <= 480:
                        _closest_minute = round(_minutes_past / 5) * 5
                    else:
                        _closest_minute = round(_minutes_past / 10) * 10

                    if self.verbose:
                        print('tx_sql[block_number]:', tx_sql['block_number'])
                        print('_total_trades:', _total_trades)
                        print('_minutes_past:', _minutes_past)
                        print('_closest_trade:', _closest_trade)
                        print('_closest_minute:', _closest_minute)

                    
                    # trades={self.total_trades_buy+self.total_trades_sell}
                    # traded blocks={len(self.blocks_data.keys())}
                    # (tx_sql['block_number'] - self.first_trade_tx_sql['block_number']) * 12 / 60 / 60 / 24


                    if (_total_trades <= 2000 or _minutes_past <= 300) and _minutes_past >= 1 and _len_traded_blocks_by_block_data >= 5:

                        if not self.can_be_old_token:


                            #(_minutes_past <= 45 and (not (self.df_analyse['_closest_trade'] == _closest_trade).any() or not (self.df_analyse['_closest_minute'] == _closest_minute).any())) or \
                            if type(self.df_analyse) == type(None) or \
                                (not (self.df_analyse['_closest_trade'] == _closest_trade).any() and not (self.df_analyse['_closest_minute'] == _closest_minute).any()):

                                blocks_since_first_trade = list(self.blocks_data.keys())[-1] - self.first_trade_tx_sql['block_number']
                                if type(self.df_analyse) != type(None) and (self.df_analyse['blocks_since_first_trade'] == blocks_since_first_trade).any():
                                    self.print(f"blocks_since_first_trade={blocks_since_first_trade} уже есть в df_analyse, пропускаем")
                                else:

                                    start_time = time.time()
                                    stat = analyse_30m.create_features(w3=w3, address=self.address, BACKTEST_VALUE=_closest_minute, BACKTEST_MODE='m', backtest_block=tx_sql['block_number'], last_trades=self, save_dict=False, verbose=self.verbose, is_LastTrades=True)
                                    self.df_analyse_total_seconds += time.time() - start_time
                                    self.df_analyse_total_counts += 1
                                    self.df_analyse_last_total_trades = self.total_trades_buy + self.total_trades_sell
                                    self.df_analyse_last_block = tx_sql['block_number']

                                    if stat == None:
                                        self.print(f"\x1b[31;1m(!)\x1b[0m analyse_30m.create_features() stat == None, _total_trades={_total_trades}, _minutes_past={_minutes_past}, len blocks_data={len(self.blocks_data)}, address={self.address}", force=True)
                                    elif 'error' in stat:
                                        traded_blocks = sum([1 if _block >= self.first_trade_tx_sql['block_number'] else 0 for _block in self.blocks_data.keys()]) if self.first_trade_tx_sql else 0
                                        self.print(f"\x1b[31;1m(!)\x1b[0m analyse_30m.create_features() error=\"{stat['error']}\", _minutes_past={_minutes_past}, total_trades={_total_trades}, traded_blocks={traded_blocks}, len blocks_data={len(self.blocks_data)}, address={self.address}", force=True)
                                        if 'exit' in stat and stat['exit']:
                                            assert 0, f"exit stat analyse_30m.create_features(), address={self.address}"
                                    else:
                                        self.print(f"{(time.time() - start_time):.2f}s analyse_30m.create_features(), p_i={p_i}, total_trades={_total_trades}, len(stat)={len(stat)}")
                                        
                                        stat = {'block_number': tx_sql['block_number'], 'close': self.blocks_data[tx_sql['block_number']]['value_usd_per_token_unit'], 'block_timestamp': tx_sql['block_timestamp'], '_closest_minute': _closest_minute, '_closest_trade': _closest_trade, '_actual_minutes_past': _minutes_past} | stat
                                        for key, value in stat.items():
                                            if value == None:
                                                stat[key] = np.nan
                                        _df_stat = pd.DataFrame.from_dict([stat])

                                        if type(self.df_analyse) == type(None):
                                            self.df_analyse = _df_stat
                                        else:
                                            self.df_analyse = pd.concat([
                                                self.df_analyse,
                                                _df_stat
                                                #_df_stat.astype(self.df_analyse.dtypes),
                                            ])

                                        if self.verbose and self.df_analyse:
                                            print(f"self.df_analyse, shape={self.df_analyse.shape}:")
                                            print(self.df_analyse[['block_number', 'blocks_since_first_trade', '_closest_minute', '_closest_trade', '_actual_minutes_past','total_trades','holders_total','liquidity']])




            # if tx_sql['block_number'] == 20820945 and self.transactions_tupl[p_i + 1]['block_number'] != 20820945:

            #     print('check!')

            #     total_invested, total_withdrew = 0, 0
            #     realized_profit = 0
            #     for adr in ['0xaf379deD3caD49dB91e55b47a0d72302967c69F1', '0x742a56733F3c57803b8d684534AC6C418eB47250', '0x586ab2e3D9ab20EECC7D612937360296Ce80BE87', '0x20c730Ff1BA6A2e1d3efBd9015C5368792bCc0A5', '0xe06B55615F9b469ffBD654fEA09211e8cCe7B712', '0x1C6573A139823d005e07CfD9640880e4e89abbB2', '0x876e03C039E18Cc5dEe2B2197F048935D45F884A', '0x4E0B74cAD957D901A4177dFb9513b35D986cE637', '0xCf0b12AaD32F02E5A62b3442c7258c940f09c474', '0xC36A00D2A7918Da3b668802A0ea001784C0b31A4', '0x22ded582FA41f3c29E1e5828103C21278F08B1Bb', '0x016dD82f7D46DE18cfC860a12271B71Cd8443e2e', '0x5d75866D620BC60f56a564045bf5C7ED1424B470', '0x5F7D3112F2C78ED7c19ad27A98638C0035BBe5De', '0xAf3f867B1B99af6154B13A0870F7D53098BE1843', '0xBf2499e4Cda11Eb33eEa341Ae425d85B6e93F028', '0x2b1F442B39AA8E2483344dBE2C9B77eae7703783', '0xBaBF360523ff8e2C869b025E8f4d849FBf2474dd', '0x037d6D7ee801c624cD62BdF3203E0150dbD3f699', '0x1Fa5f0a0E85e51999Ab28F0451AFE3Cf522276e3', '0x0845cb6a53E4db33FC5ED4042644886E01AFea29', '0x16CDb6912e4bF4D51E62bA04f62c74d753F2C11e', '0x37493656717b87eB2843FfE62E0DEc91F7468910', '0xc672604D798d8cBd0cf2cC654099F82bD02647AB', '0x7D5D6eCE83B737b409A5167f53F994990EF8c567', '0x07Fd424c9f49FF45d628093b40964E8109080BF4', '0x5D4849C66C6b62DAB1D95cE4E9eD4fE7d6b81C65', '0x3A58720C1C112e513726c9b31bB78c46B859d040', '0x348EDb49714febA562b57E248Fdef71E33679032', '0xd35D6e5AC63350d8734B0535302cBb55eAeb4621', '0xB9d074afD94df661AB3Df48668A6C6c22aC04B68', '0x8E38d123035F46034b994E3e2C156437240F4777', '0xEEEf7013DC383A214cdD4cC7c85Ab14Ef7A6D30E', '0xAE1d9e7ce74bA854945E8a6C5F0476EF413a28Be', '0x4CC8284589432532541A5bDb175AAbE1F25a21a8', '0xACE65Af8E351f283884a6302a740cEC7f623aa2C', '0x2F4B44EB4261B435fCF2f6493Dd52344dC4f9dB4', '0x30B7CAab584A551C10d3e703E4650C36cf623681', '0xDdEa90E3dc0d08D80D8A580f394AEb13969d81eB', '0xc9C70C9C09990928e30b4269A98EEe02951Eb772', '0x3d3DEfA35EbF8e624e5127Dd8D8dfF57dA2A4422', '0x5854A731cB77d11f0F5E083333D09adF1495E007', '0xaf83263F5f29BEc74704417E851d02aD6cf923F3', '0xE0C3e4512B819dd45bC2752D7212E292150D4Bbf', '0x270B175C7FBcC0B09CB811f472aD561D93C88571', '0xfA02f580dE6E20fc0A24Edd7E20795705143B237', '0xC58E4d309C4f5C88aFE355c260EE9a1a699ED1dd', '0x01a97CCF6EFf63F19e0879c9Cbb27aAfb1adFF06', '0xC7a70BF9F2B0874296444D19185dD62066666666', '0x66DCeB2b3f5BEb480B743e0889460FC7be54E2F8', '0xbD72Fd15E71D86b81c3C44C3834dc2113b73D289', '0x396FB8c9d3619f39A0ce4A8a51b4f1518C52119B', '0x27fBE7b0155B77BEB207E850Fc07379A68E7697e', '0x1044aC502759E6E32D52AA1ac60882E0A3b4eDeF', '0xf7ceC8c63DC609A85e0B2CE79C8fdc25b774689E', '0x35D5B184F6B025C4f6d94708002c034B759B09b3', '0x402Cf0CAC36C6FaA342B8379D77111e158Fe75D0', '0x6813cec8AE3B7b0F09D2Db373711B99a5E0a7349', '0x6cf72f3d4EF225fb16844DcF4567822ad882638C', '0x4Fa9a083ab95622F2B0Cb4F92CBeEfd28d087147', '0x4565ed8A7D0c039afA8DBaA4d4dD3F9662aCdfad', '0x9fF773eE65E915E47B88Ff30694C7873Ed29646e', '0xC6Cb1Df8a65a32Ed78Fc93060b59588F635dc7f1', '0x6dAac9A4AAda9A2D3F09337dc72e029aD71F7f18', '0x5975e5eD166294cE8797283F78EfBC8563460398', '0x9aA503c706E740F66a070b4885791D3F4F4125EA', '0xaCc19a4B9017Eb8aA825e8618Fb947ADf77e9286', '0xD904696560B44e6E7437c2ce7eeD161EE3D20498', '0xa8021AEcb8541328Fde1544224da0D526F3dA58D', '0x0AC3339E8be893aF614DF0A9125524b7e2E9E4a9', '0x86EcC9D6Ac1471bBeCb2A36ED41705305e3AA8DA', '0x502c4B31e10c7FD8056cAA431E396EFB691433DF', '0x9da2c1A62024Aee510cAFd221b72B8010a60BEf3', '0xA162f0A40646395Ca484A85204Fb0DF4977670f1', '0x49aBEA60c6b58bF9e21537dF763357D6253e379D', '0x181FEa712616C6332590ed91F29CdB5A315BA9a4', '0x68000301484EB90BA1294ca50A653C4C058408B7', '0xdd1236068759EF9a1630EE03dd7f54760340EfeB', '0x6567fb5b841BC12ACee6B7D7F11B88d234Ca16e1', '0x8742D05b9d34B124B4126fd4Ca9b3a03095DDEd9', '0xDe277Ba9EE90e11390c421b5DBCf5166B52e4395', '0x0A60089a148225DE598844622d489ba303Aa76b5', '0xb55e5779102073465c22d3fb4907012D474A7fC5', '0xB1fE8d7274EFCaD6571F11465709a7dF0521B2A9', '0xfe0dd0E80997bce29Ee3D7A677c4C7a250d5b144', '0x8D43AA226F36549dE6782c5D9578C3344bacAEa2', '0x762Cc48DCbD51587DEa1B5424F75B40bbf707459', '0xF458aE9b003D2c410571ac8Ee4950546Fd1ddF5D', '0xA0387D38f39B825ebc64004dCc4f282A7b11B40E', '0xD91a90280AF69eCE6965DA415d7335230a57dBCD', '0x601e26277Bf943Ad5dECF88e3dC678E1ba14e2A6', '0x6517A1aaBd767de3A0213B5A9810C9A76a6BBD11', '0x21a9160890384c3557C51049E5a5fe6e41dAEc33', '0xe921bD998EC91A68913a756D390ba36814a429f7', '0xaC8d6138db3E394188E9115277FF4e59B276BFda', '0xAA61A58830996a2b54902bAC0913641ccD362828', '0x27D61f2bA9ccB4EF7b021281B9FEB55956ce7A0A', '0x578ffe98C5Fb7635E03d8Ca37a97e2c99F599E4a', '0xa9E1C418109C5ab2d5DA2f427f36A7B653c18EaA', '0xB6A4782aD7fA2b99CF03C1c48781C1592BCeA95a', '0x5214bABd8b0dC3226650c08b2CeB10A90398B10a', '0x24261E083fB32Cfecf45cf8508Ae42da82F12232', '0x9ABbdCD2f90B83b09F29012F1F17E1017C33e57A', '0x734740c70dB02f5710207F1d12DF47c710665206', '0xBdd53E37b3abF86e9BFF2B3e369C3393A0F22b3d', '0x4156E14173e079f27bC43C32e7838b6E61103b9D', '0xfEfea9427Bef554Cc572bC4E887a0b9642fd8f4E', '0xC641D70620Ab3ed8B684f24D21C334223EBe7046', '0x4d5905a961Abfc8c9DE0aFd4e8D4aE042494fE47', '0x966c5496E978621B24DB1177187ff1F9a02F4B9f', '0x55C3e5aCa1ea88BA0fc467777F5Be07eF699E477', '0xF5E40aC9ed4351fDFe8fc7B583FD5a55F5Df292C', '0x02fE1C9bE537cEaFA69E4ee140e402A9586bF615', '0xaC41BB42Dac7B0c5A90337D97eF0C26C86968b65', '0xcDCE167dE7152d44928Fa3F453ef027c1f974cCa', '0x2a4446Dea22165A222902a0a4CFC5723514A48F8', '0x5936868A4336Da16C0b0bB4C879DDbb7a3FeeAA9', '0x9Cfec1d3ae29Ff67c7b57ab96A480B4E61eF2B18', '0x513CB622E4e9A69812B046e8c3d2e3051562d273', '0x612C45551195f15Bf05D722b9f137C914b077FC7', '0xD5B3551677f39335C6c5a699Aba102b6fe5d295B', '0x251857E07702A3c68a7E9f4Db754febcc4F81365', '0x0C32442524be643E1B50028C1AB72F17b3fa219b', '0x8838055b5d5dD0677780Bb1dE77B294F5DaFD6e5', '0xfD62B5b9a7465CB195ce3B2dab686425CFf16060', '0xE9F228089b21C8d9E3E26704154b2931330f2531', '0xB6D6E1865bFa3DE860ef331aAbf13f08915027c2', '0x6bfA9B370A72Ad21a45Bb9f6f7D5697A8Af7A8BE', '0xd1bed3B46d2baeFDB9B61Ec404C95834D3D5D664', '0xc6Cb1DB82E58c0A69eb0C9FB6E7eA6cC183022eE', '0x9FAeF208746669B693b5f7aB5a64CeeA83e20e6F', '0x3F2B2018cf57B995cD10A7f2cC1505126261328a', '0x2D86499A410a9ac2139833Ba8d6942b8A8700a7B', '0xBa29C0822d515b8d228F88d2c93B6369Bc63A098', '0x5315e97D23c9E5a7083c9356bb606A775284A914', '0x41a0623ad3E3708968A42F6c3c090A72eeE51757', '0x6d9A02434D05993d5f934ccce3b2189A539E50C4', '0xFBb6247de6d98dE7d7701CA7dAA7288C8dE487D0', '0x156e49d4B9F3810ec3Dda4c88135c53698375409', '0x4C0986bC29D4196fa3A159429F6f4910ab89a297', '0x2B4DdAC882b0ab94bE6DBe3AC864e84331CB75de', '0xb66362B192Cf63A458ca9f43777D8C1fBaBa1F6e', '0xCb2B4dE648B04673E559eBF3e588d60506d82520', '0xC1C6D5Af5F447a939191Dcf725797447eE00e3f1', '0x761e1A520826B3a90d8a31487df75FC56e0d7D94', '0xe707c039f0b5cc6392d47c67249d2747D0CBf238', '0x326b04e6D10f92e4739573a5980f7B330Ea764da', '0x1Ed63f8Fec26cc8042e67e4d07b460A2DE1c1fbe', '0x12c3Ef4Cf3bD970aB933F642B3D260B0381d9807', '0xE685245Afbc7d1dAd055a6DDbCF3DEc6e8238185', '0x09d6101377eDb252BFc4E714D6C32CcfcF9BbdD1', '0x2D0E4d4b6E9528dF25245112d622F6aEefE44754', '0x91005C6bab044B8fAab35DcF8BE7209AdfC29483', '0x0bc66E114F603eAF5d1e813141D88dc88fB96da9', '0x0ca85b489A1276eF7042F1d032D0eF3831EaF4ca', '0x0219Ab3584db4b71Fc90a8F861b86f2a384A7eC0', '0x181bf32ea075B8894d36cC7554d0db8A6DEfaDF5', '0x2F0cEc713cbDA6D11B51bf69c7d4D6B9B0771B1A', '0x14Be72435645d810FD2e42d2d4CEaC092e767393', '0x6068eb0E57175958f8b129815b8A5a59ed9A86ac', '0x26642CAcb43e74eB46d68F6A3C0f4238A4f192bA', '0x6BC84F7475A724Da765997aC46b81745e20fdC10', '0xf3a17537cBB18Ee864eDE4fAB91ca3656000995d', '0x4584AB9Eeb44c5e70ddB2CC947B18D58d4B471e3', '0x31333c2ff868163aC54086f4cDcA69a554f6F420', '0x04f35499021eF736Dd017783c559312d523d6633', '0x066Af8d14A4EA690403c5Ac5bE8Ba9948d9BAc9B', '0xF9BF908FD7Ca73cc700e62175712B4CC110cBa8C', '0x90Da7E08D3609D609b461266848100Bf8851f7EE', '0x0051dc77bFF43eFD0eC9F0BFa3237Bc57B67De5e', '0x5f2fFa394b298a8161621995F471Ef314eFeFB89', '0x21A4884445670876Ec3AFE4D893A2b31FF938Df3', '0x45f19093B2F75eaA34AF8f1f879E30c76D7D84ee', '0x733141096aBcAF192e23Fff8FF797a1b10ac5784', '0x7B765d7860AC9F934303A4D3109E8d030c6d9970', '0xe0b6BCB3840707590b3C6D449fbdf486293BfC9C', '0x45670CB5B4F9BC3EA6244C57cc734071B62BE37A', '0xEc7FEB6909034E60049825F048a9a453004F4C2a', '0x8Fa3bc09770070b371C3EBA5eAa9Ee04e60694D1', '0x45e705B5F15E809b45Ca2150FE33538229dC5EAE', '0x43E6B6EEC9C4E59B877d52fb275456E09B3Dee5F', '0x7bF408F18F78Cd69b2864957A4fBEfC26314fa66', '0x7aC6E02EE7Edecd16d398ddC6cf30D8446E61290', '0xa278F06355E64011272d6765dA2eE9fe4a6bD6Cd', '0x4FF25aE1b4bf1ceE786D5458B43f116D3203D374']:
            #         total_invested += self.balances[adr]['invested']
            #         total_withdrew += self.balances[adr]['withdrew']
            #         realized_profit += self.balances[adr]['realized_profit']

            #         print(f"realized_profit: {self.balances[adr]['realized_profit']} roi_r: {self.balances[adr]['roi_r']}, roi_u: {self.balances[adr]['roi_u']}, pnl: {self.balances[adr]['pnl']}")

            #     print('total_invested:', total_invested)
            #     print('total_withdrew:', total_withdrew)
            #     print('realized_profit:', realized_profit)
            #     exit()


        # print(f"закончился цикл for p_i, tx_sql in enumerate(self.transactions_tupl) из len(self.transactions_tupl)={len(self.transactions_tupl)} за {(time.time() - start_time_t):.3f}s")



        if 0 and not self.realtime and self.sort == 'asc':
            print('='*40 + ' ml [start] ' + '='*40)
            self.token_ml = ml.get_token_stat_for_ml(ia=None, address=self.address, pair=None, last_block=LAST_BACKTESTING_BLOCK, realtime=self.realtime)
            ml.print_token_ml_results(self.token_ml)
            print('='*40 + ' ml [end] ' + '='*40)

        
        if not self.can_be_old_token:

            if not self.realtime:
                assert type(self.df_analyse) != type(None), f"type(self.df_analyse) == type(None), total_trades={self.total_trades_buy+self.total_trades_sell}, address={self.address}"


        
        # для экономии памяти
        self.transactions_tupl = None


    def create_dataframe(self):

        self.print('-'*30 + ' закончился for цикл по тразнакциям, создаем dataframe ' + '-'*30)
        self.print(f"len blocks_data={len(self.blocks_data.keys())}, address={self.address}")
        self.print('self.blocks_data.keys():')
        self.print(pformat(self.blocks_data.keys()))

        dataframe = pd.DataFrame.from_dict(self.blocks_data, orient='index')
        dataframe.rename(columns={'value_usd_per_token_unit':'close'}, inplace=True)
        dataframe['open'] = np.nan
        #dataframe = dataframe[['timestamp', 'close', 'liquidity']]
        dataframe['block'] = dataframe.index
        dataframe['renounce_ownership'] = np.nan

        dataframe = dataframe[['block', 'timestamp'] + list(set(dataframe.columns.tolist()) - set(['block', 'timestamp']))]

        if not self.txs_sql_token_owners:
            if not self.realtime:
                self.print(f"!!! {self.address} self.txs_sql_token_owners empty")

        renounce_ownership = False
        if self.txs_sql_token_owners and self.txs_sql_token_owners[-1]['topics_2'] in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:
            #dataframe['renounce_ownership'].loc[dataframe['block'] == self.txs_sql_token_owners[-1]['block_number']] = 1
            #dataframe.loc[dataframe['block'] == self.txs_sql_token_owners[-1]['block_number'], 'renounce_ownership'] = 1

            nearest_renounce_ownership_ = dataframe.loc[dataframe['block'] >= self.txs_sql_token_owners[-1]['block_number'], 'block']
            if not nearest_renounce_ownership_.empty:
                _block = nearest_renounce_ownership_.iloc[0]
                dataframe.loc[dataframe['block'] == _block, 'renounce_ownership'] = 1

            renounce_ownership = True

            nearest_renounce_ownership_block = dataframe.iloc[(dataframe['block'] - self.txs_sql_token_owners[-1]['block_number']).abs().argsort().iloc[0]]['block']
            self.print(f"nearest_renounce_ownership_block={nearest_renounce_ownership_block}, self.txs_sql_token_owners[-1]['block_number']={self.txs_sql_token_owners[-1]['block_number']}")
            if nearest_renounce_ownership_block >= self.txs_sql_token_owners[-1]['block_number']:
                dataframe.loc[dataframe['block'] == nearest_renounce_ownership_block, 'renounce_ownership'] = 1

        dataframe = dataframe.drop(dataframe[(dataframe['volume_buy'] + dataframe['volume_sell'] == 0)].index)

        if self.initial_value_usd_per_token_unit == None:
            if not self.can_be_old_token and self.sort == 'asc':
                self.print(f"\x1b[31;1m(!)\x1b[0m self.initial_value_usd_per_token_unit == None", force=True)
        else:
            if self.first_trade_tx_sql != None:
                dataframe.loc[dataframe.index[0], 'open'] = self.initial_value_usd_per_token_unit / 10 ** self.contract_data_token['token_decimals']

        dataframe['erc20_to_1'] = 0
        for block, item in self.blocks_data.items():

            if 'erc20_to_1' in item and item['erc20_to_1'] > 0:
                dataframe.loc[dataframe['block'] == block, 'erc20_to_1'] = item['erc20_to_1']


        if not len(dataframe):
            self.print(f"no len dataframe for address={self.address}", force=True)
            return

        self.print(f"first df tx: {datetime.fromtimestamp(dataframe.iloc[0]['timestamp'])}, last df tx: {datetime.fromtimestamp(dataframe.iloc[-1]['timestamp'])}, df len: {len(dataframe)}")
        self.print(f"first trade tx_sql block {self.first_trade_tx_sql['block_number']} on {datetime.fromtimestamp(self.first_trade_tx_sql['block_timestamp'])}")
        self.print(f"first trade df block {dataframe.iloc[0]['block']} on {datetime.fromtimestamp(dataframe.iloc[0]['timestamp'])}")

        if self.sort == 'asc':
            #assert self.first_trade_tx_sql['block_number'] == dataframe.iloc[0]['block'], f" self.first_trade_tx_sql['block_number'] != dataframe.iloc[0]['block'] {self.address} {self.first_trade_tx_sql['block_number']}!={dataframe.iloc[0]['block']}"
            if self.first_trade_tx_sql['block_number'] != dataframe.iloc[0]['block']:
                self.print(f"\x1b[31;1m(!)\x1b[0m", force=True)
                self.print(f"\x1b[31;1m(!)\x1b[0m self.first_trade_tx_sql['block_number'] != dataframe.iloc[0]['block'] {self.first_trade_tx_sql['block_number']}!={dataframe.iloc[0]['block']}, address={self.address}", force=True)
                self.print(f"\x1b[31;1m(!)\x1b[0m", force=True)


        has_prediction = False
        if self.sort == 'asc':
            blocks_preds = {} # index - номер блока
            if self.token_ml:
                dataframe['prediction'] = {}
                if self.token_ml.block_first_trade:
                    for i in range(self.token_ml.last_transaction_block - self.token_ml.block_first_trade + 1):

                        block = self.token_ml.block_first_trade + i

                        if str(block) not in self.token_ml.blocks_data:
                            continue
                        
                        if block not in blocks_preds:
                            blocks_preds[block] = {}

                        if self.token_ml.blocks_data[str(block)]['prediction'] != None:
                            blocks_preds[block]['prediction_block'] = self.token_ml.blocks_data[str(block)]['prediction'][1] if self.token_ml.blocks_data[str(block)]['prediction'][0] == 1 else 1 - self.token_ml.blocks_data[str(block)]['prediction'][1] 

                        blocks_preds[block]['prediction_all'] = self.token_ml.blocks_data[str(block)]['prediction_all'][1] if self.token_ml.blocks_data[str(block)]['prediction_all'][0] == 1 else 1 - self.token_ml.blocks_data[str(block)]['prediction_all'][1]

                        if blocks_preds[block]['prediction_all'] >= 0.50 or 'prediction_block' in blocks_preds[block] and blocks_preds[block]['prediction_block'] >= 0.50:
                            has_prediction = True
                
                    blocks_preds[self.token_ml.block_first_trade]['prediction_bMint'] = self.token_ml.blocks_data[str(self.token_ml.transactions_mint[0]['block_number'])]['prediction_bMint'][1] if self.token_ml.blocks_data[str(self.token_ml.transactions_mint[0]['block_number'])]['prediction_bMint'][0] == 1 else 1 - self.token_ml.blocks_data[str(self.token_ml.transactions_mint[0]['block_number'])]['prediction_bMint'][1]
                    if blocks_preds[self.token_ml.block_first_trade]['prediction_bMint'] >= 0.50:
                        has_prediction = True
                        
                for step, item in self.token_ml.steps_data.items():
                    block = item['block']
                    blocks_preds[block][f"steps_trades_{item['trades_buy'] + item['trades_sell']}"] = item['prediction'][1] if item['prediction'][0] == 1 else 1 - item['prediction'][1]
                    blocks_preds[block][f"steps_trades_{item['trades_buy'] + item['trades_sell']}_all"] = item['prediction_all'][1] if item['prediction_all'][0] == 1 else 1 - item['prediction_all'][1]
                    if blocks_preds[block][f"steps_trades_{item['trades_buy'] + item['trades_sell']}"] >= 0.50 or blocks_preds[block][f"steps_trades_{item['trades_buy'] + item['trades_sell']}_all"] >= 0.50:
                        has_prediction = True

                for block, pred in blocks_preds.items():
                    dataframe.loc[dataframe['block'] == block, 'prediction'] = [pred]
                


        if not self.realtime:


            self.print(dataframe[['volume_buy', 'volume_sell', 'trades_buy', 'trades_sell', 'invested', 'withdrew', 'close', 'block', 'liquidity', 'renounce_ownership']])
            self.print(dataframe.info())
            self.print(list(dataframe.columns))


            if self.can_be_old_token:
                dataframe.drop(['extra_data','prediction'], axis=1, inplace=True, errors='ignore')

            dataframe.drop(['trades_list', 'last_trf'], axis=1, inplace=True)


            # _name = f"{LABEL_TOKENS if LABEL_TOKENS != None else ''}{self.address},l{utils.human_format(len(dataframe))},{utils.human_format(dataframe.iloc[-1]['liquidity'])}"
            # if not self.can_be_old_token:
            #     if renounce_ownership:
            #         _name += ",lqb"
            #     if has_prediction:
            #         _name += ",hpr"
            # save_to = f"{FILES_DIR}/temp/defi_candles/{_name}.csv"
            save_to = f"/disk_sdc/defi_candles/{self.address}.csv"
            
            if 1 or has_prediction or not has_prediction and renounce_ownership:
                dataframe.to_csv(save_to, index=False)
                self.print(f"saved to {save_to} in {(time.time() - self.start_time_g):.2f}s", force=True)
            else:
                self.print(f"not saved to {save_to}, because has_prediction or not renounce_ownership", force=True)




        # dataframe['ema_short'] = ta.EMA(dataframe['close'], timeperiod=9)
        # dataframe['ema_long'] = ta.EMA(dataframe['close'], timeperiod=26)
        # dataframe['ema_long_uptrend'] = np.select([(dataframe['ema_long'].shift(1) <= dataframe['ema_long'])], [1], default=0)

        if self.verbose:
            self.print(f'final dataframe for address={self.address}, last_block={self.last_block}:')
            self.print(dataframe[['block','close','volume_buy', 'volume_sell', 'trades_buy', 'trades_sell', 'invested', 'withdrew']])
            # print(dataframe.info())
            self.print(f"columns num: {len(dataframe.columns)}")
            self.print(list(dataframe.columns))

        # if 'i' not in dataframe.columns: dataframe['i'] = range(len(dataframe))

        # dfn = dataframe.to_dict(orient="records")
        # for i, row in enumerate(dfn):

        #     if i < 26: continue

        #     if dfn[i - 1]['ema_short'] <= dfn[i - 1]['ema_long'] and dfn[i]['ema_short'] > dfn[i]['ema_long']:
        #         print(f"{datetime.fromtimestamp(row['timestamp'])} buy signal")

        self.dataframe = dataframe



        if not self.can_be_old_token:
            return

        if self.verbose:
            return

        if self.verbose:
            print('dataframe:')
            print(dataframe)
            if type(self.df_analyse) != type(None):
                print('df_analyse:')
                print(self.df_analyse[['block_number', 'blocks_since_first_trade', '_closest_minute', '_closest_trade', '_actual_minutes_past','total_trades']])

        if type(self.df_analyse) != type(None):
            self.df_analyse.reset_index(drop=True, inplace=True)
            _len = len(self.df_analyse)
            self.df_analyse = pd.concat([self.df_analyse,
                    pd.DataFrame({'&-max_price_5m': [np.nan]*_len }),
                    pd.DataFrame({'&-min_price_5m': [np.nan]*_len }),
                    pd.DataFrame({'&-max_liq_5m': [np.nan]*_len }),
                    pd.DataFrame({'&-min_liq_5m': [np.nan]*_len }),

                    pd.DataFrame({'&-max_price_15m': [np.nan]*_len }),
                    pd.DataFrame({'&-min_price_15m': [np.nan]*_len }),
                    pd.DataFrame({'&-max_liq_15m': [np.nan]*_len }),
                    pd.DataFrame({'&-min_liq_15m': [np.nan]*_len }),

                    pd.DataFrame({'&-max_price_30m': [np.nan]*_len }),
                    pd.DataFrame({'&-min_price_30m': [np.nan]*_len }),
                    pd.DataFrame({'&-max_liq_30m': [np.nan]*_len }),
                    pd.DataFrame({'&-min_liq_30m': [np.nan]*_len }),

                    pd.DataFrame({'&-max_price_1h': [np.nan]*_len }),
                    pd.DataFrame({'&-min_price_1h': [np.nan]*_len }),
                    pd.DataFrame({'&-max_liq_1h': [np.nan]*_len }),
                    pd.DataFrame({'&-min_liq_1h': [np.nan]*_len }),

                    pd.DataFrame({'&-max_price_4h': [np.nan]*_len }),
                    pd.DataFrame({'&-min_price_4h': [np.nan]*_len }),
                    pd.DataFrame({'&-max_liq_4h': [np.nan]*_len }),
                    pd.DataFrame({'&-min_liq_4h': [np.nan]*_len }),

                    pd.DataFrame({'&-max_price_8h': [np.nan]*_len }),
                    pd.DataFrame({'&-min_price_8h': [np.nan]*_len }),
                    pd.DataFrame({'&-max_liq_8h': [np.nan]*_len }),
                    pd.DataFrame({'&-min_liq_8h': [np.nan]*_len }),

                    pd.DataFrame({'&-max_price_16h': [np.nan]*_len }),
                    pd.DataFrame({'&-min_price_16h': [np.nan]*_len }),
                    pd.DataFrame({'&-max_liq_16h': [np.nan]*_len }),
                    pd.DataFrame({'&-min_liq_16h': [np.nan]*_len }),

                    pd.DataFrame({'&-max_price_2d': [np.nan]*_len }),
                    pd.DataFrame({'&-min_price_2d': [np.nan]*_len }),
                    pd.DataFrame({'&-max_liq_2d': [np.nan]*_len }),
                    pd.DataFrame({'&-min_liq_2d': [np.nan]*_len }),

                    pd.DataFrame({'&-max_price_5d': [np.nan]*_len }),
                    pd.DataFrame({'&-min_price_5d': [np.nan]*_len }),
                    pd.DataFrame({'&-max_liq_5d': [np.nan]*_len }),
                    pd.DataFrame({'&-min_liq_5d': [np.nan]*_len }),

                    pd.DataFrame({'&-max_price_inf': [np.nan]*_len }),
                    pd.DataFrame({'&-min_price_inf': [np.nan]*_len }),
                    pd.DataFrame({'&-max_liq_inf': [np.nan]*_len }),
                    pd.DataFrame({'&-min_liq_inf': [np.nan]*_len }),
                ], axis=1)

            start_time_g = time.time()
            dfn = dataframe.to_dict(orient="records")
            _blocks_df_anal_values = list(self.df_analyse['block_number'].values)
            for i, row in enumerate(dfn[:-1]):
                #if (self.df_analyse['block_number'] == row['block']).any():
                if row['block'] in _blocks_df_anal_values:
                    _blocks_df_anal_values.remove(row['block'])

                    max_price_5m, min_price_5m, max_liq_5m, min_liq_5m = 0, 0, 0, 0
                    max_price_15m, min_price_15m, max_liq_15m, min_liq_15m = 0, 0, 0, 0
                    max_price_30m, min_price_30m, max_liq_30m, min_liq_30m = 0, 0, 0, 0
                    max_price_1h, min_price_1h, max_liq_1h, min_liq_1h = 0, 0, 0, 0
                    max_price_4h, min_price_4h, max_liq_4h, min_liq_4h = 0, 0, 0, 0
                    max_price_8h, min_price_8h, max_liq_8h, min_liq_8h = 0, 0, 0, 0
                    max_price_16h, min_price_16h, max_liq_16h, min_liq_16h = 0, 0, 0, 0
                    max_price_2d, min_price_2d, max_liq_2d, min_liq_2d = 0, 0, 0, 0
                    max_price_5d, min_price_5d, max_liq_5d, min_liq_5d = 0, 0, 0, 0
                    max_price_inf, min_price_inf, max_liq_inf, min_liq_inf = 0, 0, 0, 0
                    for j, dow in enumerate(dfn[i+1:]):
                        _change_price = 100 * dow['close'] / row['close'] - 100
                        _change_liq = dow['liquidity'] - row['liquidity']
                        _minutes_past = (dow['block'] - row['block']) * 12 / 60

                        if _minutes_past <= 5:
                            if max_price_5m < _change_price: max_price_5m = _change_price
                            if min_price_5m > _change_price: min_price_5m = _change_price
                            if max_liq_5m < _change_liq: max_liq_5m = _change_liq
                            if min_liq_5m > _change_liq: min_liq_5m = _change_liq

                        if _minutes_past <= 15:
                            if max_price_15m < _change_price: max_price_15m = _change_price
                            if min_price_15m > _change_price: min_price_15m = _change_price
                            if max_liq_15m < _change_liq: max_liq_15m = _change_liq
                            if min_liq_15m > _change_liq: min_liq_15m = _change_liq

                        if _minutes_past <= 30:
                            if max_price_30m < _change_price: max_price_30m = _change_price
                            if min_price_30m > _change_price: min_price_30m = _change_price
                            if max_liq_30m < _change_liq: max_liq_30m = _change_liq
                            if min_liq_30m > _change_liq: min_liq_30m = _change_liq

                        if _minutes_past <= 60:
                            if max_price_1h < _change_price: max_price_1h = _change_price
                            if min_price_1h > _change_price: min_price_1h = _change_price
                            if max_liq_1h < _change_liq: max_liq_1h = _change_liq
                            if min_liq_1h > _change_liq: min_liq_1h = _change_liq

                        if _minutes_past <= 60 * 4:
                            if max_price_4h < _change_price: max_price_4h = _change_price
                            if min_price_4h > _change_price: min_price_4h = _change_price
                            if max_liq_4h < _change_liq: max_liq_4h = _change_liq
                            if min_liq_4h > _change_liq: min_liq_4h = _change_liq

                        if _minutes_past <= 60 * 8:
                            if max_price_8h < _change_price: max_price_8h = _change_price
                            if min_price_8h > _change_price: min_price_8h = _change_price
                            if max_liq_8h < _change_liq: max_liq_8h = _change_liq
                            if min_liq_8h > _change_liq: min_liq_8h = _change_liq

                        if _minutes_past <= 60 * 16:
                            if max_price_16h < _change_price: max_price_16h = _change_price
                            if min_price_16h > _change_price: min_price_16h = _change_price
                            if max_liq_16h < _change_liq: max_liq_16h = _change_liq
                            if min_liq_16h > _change_liq: min_liq_16h = _change_liq

                        if _minutes_past <= 60 * 24 * 2:
                            if max_price_2d < _change_price: max_price_2d = _change_price
                            if min_price_2d > _change_price: min_price_2d = _change_price
                            if max_liq_2d < _change_liq: max_liq_2d = _change_liq
                            if min_liq_2d > _change_liq: min_liq_2d = _change_liq

                        if _minutes_past <= 60 * 24 * 5:
                            if max_price_5d < _change_price: max_price_5d = _change_price
                            if min_price_5d > _change_price: min_price_5d = _change_price
                            if max_liq_5d < _change_liq: max_liq_5d = _change_liq
                            if min_liq_5d > _change_liq: min_liq_5d = _change_liq

                        if max_price_inf < _change_price: max_price_inf = _change_price
                        if min_price_inf > _change_price: min_price_inf = _change_price
                        if max_liq_inf < _change_liq: max_liq_inf = _change_liq
                        if min_liq_inf > _change_liq: min_liq_inf = _change_liq

                    self.df_analyse.loc[self.df_analyse['block_number'] == row['block'], [
                        '&-max_price_5m',
                        '&-min_price_5m',
                        '&-max_liq_5m',
                        '&-min_liq_5m',
                        '&-max_price_15m',
                        '&-min_price_15m',
                        '&-max_liq_15m',
                        '&-min_liq_15m',
                        '&-max_price_30m',
                        '&-min_price_30m',
                        '&-max_liq_30m',
                        '&-min_liq_30m',
                        '&-max_price_1h',
                        '&-min_price_1h',
                        '&-max_liq_1h',
                        '&-min_liq_1h',
                        '&-max_price_4h',
                        '&-min_price_4h',
                        '&-max_liq_4h',
                        '&-min_liq_4h',
                        '&-max_price_8h',
                        '&-min_price_8h',
                        '&-max_liq_8h',
                        '&-min_liq_8h',
                        '&-max_price_16h',
                        '&-min_price_16h',
                        '&-max_liq_16h',
                        '&-min_liq_16h',
                        '&-max_price_2d',
                        '&-min_price_2d',
                        '&-max_liq_2d',
                        '&-min_liq_2d',
                        '&-max_price_5d',
                        '&-min_price_5d',
                        '&-max_liq_5d',
                        '&-min_liq_5d',
                        '&-max_price_inf',
                        '&-min_price_inf',
                        '&-max_liq_inf',
                        '&-min_liq_inf',
                    ]] = [
                        max_price_5m,
                        min_price_5m,
                        max_liq_5m,
                        min_liq_5m,
                        max_price_15m,
                        min_price_15m,
                        max_liq_15m,
                        min_liq_15m,
                        max_price_30m,
                        min_price_30m,
                        max_liq_30m,
                        min_liq_30m,
                        max_price_1h,
                        min_price_1h,
                        max_liq_1h,
                        min_liq_1h,
                        max_price_4h,
                        min_price_4h,
                        max_liq_4h,
                        min_liq_4h,
                        max_price_8h,
                        min_price_8h,
                        max_liq_8h,
                        min_liq_8h,
                        max_price_16h,
                        min_price_16h,
                        max_liq_16h,
                        min_liq_16h,
                        max_price_2d,
                        min_price_2d,
                        max_liq_2d,
                        min_liq_2d,
                        max_price_5d,
                        min_price_5d,
                        max_liq_5d,
                        min_liq_5d,
                        max_price_inf,
                        min_price_inf,
                        max_liq_inf,
                        min_liq_inf,
                    ]
                    


            self.df_analyse = self.df_analyse.dropna(subset=['&-max_price_5m'])

            if (1 or self.verbose):
                print(f"self.df_analyse for address={self.address}:")
                print(self.df_analyse[[
                    'block_number',
                    'liquidity',
                    'total_trades',
                    '_actual_minutes_past',
                    '_closest_minute',
                    '_closest_trade',
                    # '&-max_price_5m','&-min_price_5m','&-max_liq_5m','&-min_liq_5m',
                    '&-max_price_15m','&-min_price_15m',
                    #'&-max_liq_15m','&-min_liq_15m',
                    # '&-max_price_30m','&-min_price_30m','&-max_liq_30m','&-min_liq_30m',
                    # '&-max_price_1h','&-min_price_1h','&-max_liq_1h','&-min_liq_1h',
                    #'&-max_price_4h','&-min_price_4h','&-max_liq_4h','&-min_liq_4h',
                    # '&-max_price_8h','&-min_price_8h','&-max_liq_8h','&-min_liq_8h',
                    # '&-max_price_16h','&-min_price_16h','&-max_liq_16h','&-min_liq_16h',
                    '&-max_price_2d','&-min_price_2d','&-max_liq_2d','&-min_liq_2d',
                    #'&-max_price_5d','&-min_price_5d','&-max_liq_5d','&-min_liq_5d',
                    #'&-max_price_inf','&-min_price_inf','&-max_liq_inf','&-min_liq_inf'
                ]])

            print(f"{(time.time() - start_time_g):.2f}s &-targets for loop, len self.df_analyse={len(self.df_analyse)}, total_trades={self.total_trades_buy+self.total_trades_sell}")
            self.df_analyse.to_csv(f"/disk_sdc/df_analyse/{self.address}.csv", index=False)






    def update_balances(self, value_usd_per_token_unit, token_supply, last_tx_sql, start_time_block, w3=None):

        # print(f"update_balances(), last_tx_sql['block_number']={last_tx_sql['block_number']}, last_tx_sql['from']={last_tx_sql['from']}, last_tx_sql['hash']={last_tx_sql['hash']}, last_block={self.last_block}")
        since_ = ''
        if self.first_trade_tx_sql != None:
            since_ = f"({last_tx_sql['block_number'] - self.first_trade_tx_sql['block_number']} since first trade, {(last_tx_sql['block_number'] - self.first_trade_tx_sql['block_number'])*12/60:.1f}m)"
        self.print(f"\33[105mupdate_balance()\033[0m on block={last_tx_sql['block_number']} {since_}")





        mev_bot_addresses, mev_volumes, mev_profits = [], [], []
        if 'buy_addresses' in self.blocks_data[last_tx_sql['block_number']]:
            mev_bot_addresses = list(set(self.blocks_data[last_tx_sql['block_number']]['buy_addresses']).intersection(self.blocks_data[last_tx_sql['block_number']]['sell_addresses']))
            if mev_bot_addresses:
                self.print(f"в этом блоке были MEV боты (покупка + продажа): {mev_bot_addresses} => убираем их из buy_addresses и sell_addresses")
                self.blocks_data[last_tx_sql['block_number']]['buy_addresses'] = list(set(self.blocks_data[last_tx_sql['block_number']]['buy_addresses']) - set(mev_bot_addresses))
                self.blocks_data[last_tx_sql['block_number']]['sell_addresses'] = list(set(self.blocks_data[last_tx_sql['block_number']]['sell_addresses']) - set(mev_bot_addresses))

            try:
                for adr in mev_bot_addresses:
                    _invested = 0
                    _withdrew = 0
                    _trades_count = 0
                    for trade in self.blocks_data[last_tx_sql['block_number']]['trades_list']:
                        if trade['address'] == adr:
                            if trade['s'] == 'buy': _invested += trade['invested']
                            if trade['s'] == 'sell': _withdrew += trade['withdrew']
                            _trades_count += 1
                        
                    self.print(f"mev address={adr} (за {_trades_count} трейда): invested={_invested:.2f}$, withdrew={_withdrew:.2f}$ => mev_volume={((_invested + _withdrew)):.2f}$, mev_profit={(_withdrew - _invested):.2f}$")
                    mev_volumes.append((_invested + _withdrew))
                    mev_profits.append(_withdrew - _invested)
            except:
                mev_profits.append(1)
                self.print(f"\x1b[31;1m(!)\x1b[0m mev_bot_addresses error")
                self.print(str(traceback.format_exc()))

            self.print('---')




        
        exclude_addresses = self.pairs_list + [self.address] + [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]
        # сортировка нужна для подсчета minutes_hold_median
        balances_ = sorted(self.balances.items(), key=lambda d: (d[1]['last_trade_block']))


        # if self.first_trade_tx_sql and last_tx_sql['block_number'] - self.first_trade_tx_sql['block_number'] > 10:
        #     for address, item in balances_:
        #         print(item['i'], item['last_p_i'], item['last_trade_block'])
            
        #     exit()


        for window in [99999, 70, 10]:

            _stats = {
                'roi_r': [],
                'roi_r_pos': [],
                'roi_r_neg': [],
                'roi_u': [],
                'roi_u_pos': [],
                'roi_u_neg': [],
                'roi_t': [],
                'roi_t_pos': [],
                'roi_t_neg': [],
                'realized_profit': [],
                'realized_profit_pos': [],
                'realized_profit_neg': [],
                'unrealized_profit': [],
                'unrealized_profit_pos': [],
                'unrealized_profit_neg': [],
                'pnl': [],
                'pnl_pos': [],
                'pnl_neg': [],
                'minutes_hold': [],
            }
            new_holders = 0

            # for address in self.balances.keys():
            for address, item in balances_[-window:]:

                if window == 99999:
                    # (!) пока тестирую 30m стратегию, условие ниже всегда должно выполняться
                    if 1 or item['value'] > 1 or item['value'] < 0:
                        #if address not in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:
                        self.update_balance_address(address, value_usd_per_token_unit, last_tx_sql['block_timestamp'], block_number=last_tx_sql['block_number'], token_supply=token_supply)
                        pass

                if address in exclude_addresses: continue

                # (!) здесь можно улучшить, если стату делать отдельно по buy_addresses и sell_addresses

                # не учитываем кошельки, которые купили на этом блоке
                if item['first_trade_block'] == last_tx_sql['block_number']:
                    new_holders += 1
                    continue

                if item['invested'] != 0 and item['value'] > 1:
                    _stats['roi_r'].append(item['roi_r'])
                    _stats['roi_u'].append(item['roi_u'])
                    _stats['roi_t'].append(item['roi_t'])
                    _stats['realized_profit'].append(item['realized_profit'])
                    _stats['unrealized_profit'].append(item['unrealized_profit'])
                    _stats['pnl'].append(item['pnl'])
                    if item['roi_r'] > 0:
                        _stats['roi_r_pos'].append(item['roi_r'])
                    if item['roi_r'] <= 0:
                        _stats['roi_r_neg'].append(abs(item['roi_r']))
                    if item['roi_u'] > 0:
                        _stats['roi_u_pos'].append(item['roi_u'])
                    if item['roi_u'] <= 0:
                        _stats['roi_u_neg'].append(abs(item['roi_u']))
                    if item['roi_t'] > 0:
                        _stats['roi_t_pos'].append(item['roi_t'])
                    if item['roi_t'] <= 0:
                        _stats['roi_t_neg'].append(abs(item['roi_t']))
                    if item['realized_profit'] > 0:
                        _stats['realized_profit_pos'].append(item['realized_profit'])
                    if item['realized_profit'] <= 0:
                        _stats['realized_profit_neg'].append(abs(item['realized_profit']))
                    if item['unrealized_profit'] > 0:
                        _stats['unrealized_profit_pos'].append(item['unrealized_profit'])
                    if item['unrealized_profit'] <= 0:
                        _stats['unrealized_profit_neg'].append(abs(item['unrealized_profit']))
                    if item['pnl'] > 0:
                        _stats['pnl_pos'].append(item['pnl'])
                    if item['pnl'] <= 0:
                        _stats['pnl_neg'].append(abs(item['pnl']))
                    
                    if item['days'] != None:
                        _stats['minutes_hold'].append(item['days'] * 1440)
                    else:
                        pass
                        #print(f"for wallet_address={address} on address={self.address} item[days]=None")

            for key in ['roi_r_pos', 'roi_r_neg', 'roi_u_pos', 'roi_u_neg', 'roi_t_pos', 'roi_t_neg']:
                _stats[f'{key}_count'] = len(_stats[key])
                _stats[f'{key}_sum'] = sum(_stats[key])
                _stats[f'{key}_max'] = max(_stats[key]) if _stats[key] else None
                _stats[f'{key}_median'] = statistics.median(_stats[key]) if _stats[key] else None
                _stats[f'{key}_std'] = statistics.pstdev(_stats[key]) if _stats[key] else None
                del _stats[key]
            
            for key in ['roi_r','roi_u','roi_t','realized_profit','unrealized_profit','pnl','realized_profit_pos', 'realized_profit_neg', 'unrealized_profit_pos', 'unrealized_profit_neg', 'pnl_pos', 'pnl_neg']:
                _stats[f'{key}_sum'] = sum(_stats[key])
                _stats[f'{key}_max'] = max(_stats[key]) if _stats[key] else None
                _stats[f'{key}_median'] = statistics.median(_stats[key]) if _stats[key] else None
                _stats[f'{key}_std'] = statistics.pstdev(_stats[key]) if _stats[key] else None
                del _stats[key]

            if window == 99999:
                _stats['minutes_hold_sum_window=60'] = sum(_stats['minutes_hold'][-60:]) if _stats['minutes_hold'] else None
                _stats['minutes_hold_sum_window=500'] = sum(_stats['minutes_hold'][-500:]) if _stats['minutes_hold'] else None
                _stats['minutes_hold_median_window=60'] = statistics.median(_stats['minutes_hold'][-60:]) if _stats['minutes_hold'] else None
                _stats['minutes_hold_median_window=500'] = statistics.median(_stats['minutes_hold'][-500:]) if _stats['minutes_hold'] else None
                _stats['minutes_hold_std_window=60'] = statistics.pstdev(_stats['minutes_hold'][-60:]) if _stats['minutes_hold'] else None
                _stats['minutes_hold_std_window=500'] = statistics.pstdev(_stats['minutes_hold'][-500:]) if _stats['minutes_hold'] else None
            
            del _stats['minutes_hold']

            if window != 99999:
                _stats[f'new_holders'] = new_holders

            for key, value in _stats.items():
                self.blocks_data[last_tx_sql['block_number']][f'stats_profit_window={window}|{key}'] = value



        self.blocks_data[last_tx_sql['block_number']] |= {
            'total_volume_buy': self.total_volume_buy,
            'total_volume_sell': self.total_volume_sell,
            'total_volume_transfer': self.total_volume_transfer,
            'total_invested': self.total_invested,
            'total_withdrew': self.total_withdrew,
            'total_trades_buy': self.total_trades_buy,
            'total_trades_sell': self.total_trades_sell,
            'total_trades_transfer': self.total_trades_transfer,
        }


        if 1:
            for what_addresses in ['buy_addresses', 'sell_addresses']:
                if what_addresses in self.blocks_data[last_tx_sql['block_number']]:
                    self.print(f"Начинаю подсчет _stat_common для {what_addresses}, трейдов было {len(self.blocks_data[last_tx_sql['block_number']][what_addresses])}")
                    _stat_common = None
                    erc20_to_1_count = 0
                    #print(f"Адреса, которые делали трейд на {last_tx_sql['block_number']} блоке:")
                    for adr in self.blocks_data[last_tx_sql['block_number']][what_addresses]:

                        #print(f"- {adr} {'YES' if self.balances[adr]['stat_wallet_address'] != None else 'NO'}")

                        if self.balances[adr]['stat_wallet_address'] != None:
                            if _stat_common == None:
                                _stat_common = {}
                                for key in self.balances[adr]['stat_wallet_address'].keys():
                                    if key in ['first_in_transaction_from_address']:
                                        continue
                                    _stat_common[key] = []
                            
                            for key, item in self.balances[adr]['stat_wallet_address'].items():
                                if key in _stat_common:
                                    _stat_common[key].append(item)
                            
                            if self.balances[adr]['stat_wallet_address']['erc20_to'] == 1:
                                erc20_to_1_count += 1

                    if _stat_common:
                        for key, item in _stat_common.copy().items():
                            #_stat_common[key] = sum(item) / len(item) # avg
                            if type(item[0]) in [list, dict, str]:
                                del _stat_common[key]
                            else:
                                # _stat_common[key] = statistics.median(item)
                                _stat_common[key] = item

                        for key, value in _stat_common.items():
                            self.blocks_data[last_tx_sql['block_number']][f'stats_erc20_{what_addresses}|{key}'] = value

                        self.print(f'Итого stats_erc20_{what_addresses}:')
                        for key, value in _stat_common.items():
                            self.print(f"  {key}: {value}")
                    
                    if what_addresses == 'buy_addresses':
                        self.blocks_data[last_tx_sql['block_number']]['erc20_to_1'] = erc20_to_1_count

        if not self.realtime and (self.verbose or len(self.blocks_data.keys()) <= self.full_log_text_limit):
            self.print(self.balance_table())

        if self.first_trade_tx_sql and self.first_trade_tx_sql['block_number'] == last_tx_sql['block_number']:


            # self.show_erc20_stat()
            
            
            if not self.can_be_old_token or self.can_be_old_token and self.owner_addresses:
                self.print(f"Owner {Fore.RED}{self.owner_addresses[0]}{Style.RESET_ALL} stats (all owners {self.owner_addresses}):")
                if self.owner_addresses[0] not in self.balances:
                    self.print(f"(!!!!!) owner={self.owner_addresses[0]} not in self.balances")
                elif self.balances[self.owner_addresses[0]]['stat_wallet_address'] == None:
                    self.print(f"(!!!!!) owner={self.owner_addresses[0]} self.balances[self.owner_addresses[0]]['stat_wallet_address'] == None")
                else:
                    for key, value in self.balances[self.owner_addresses[0]]['stat_wallet_address'].items():
                        if key == 'first_in_transaction_from_address':
                            self.print(f"   {key}: {utils.print_address(value, w3=w3, last_block=self.first_trade_tx_sql['block_number'])}")
                        else:
                            self.print(f"   {key}: {value}")




            erc20_to_1_count = 0
            erc20_hold_time_block_median_list = []

            for address, item in balances_:    

                if address in self.owner_addresses + [self.address] + self.pairs_list:    
                    continue 

                if not item['stat_wallet_address']:
                    self.print(f"(!!!!!) {address} has no stat_wallet_address")
                    continue

                if item['stat_wallet_address']['erc20_to'] == 1:
                    erc20_to_1_count += 1

                erc20_hold_time_block_median_list.append(item['stat_wallet_address']['erc20_hold_time_block_median'])

            if not self.can_be_old_token:
                diff_ft_fm = self.first_trade_tx_sql['block_number'] - self.txs_sql_mints[list(self.txs_sql_mints.keys())[0]]['block_number'] if self.txs_sql_mints else None
                self.print(f"Разница блоков между первым Mint() и первым трейдом: {diff_ft_fm}")
                self.print(f"tx_index транзакции первого Mint(): {self.txs_sql_mints[list(self.txs_sql_mints.keys())[0]]['transactionindex'] if self.txs_sql_mints else None}")

            erc20_hold_time_block_median = statistics.median(erc20_hold_time_block_median_list) if erc20_hold_time_block_median_list else None
            if erc20_hold_time_block_median != None:
                self.print(f"erc20_hold_time_block_median: {erc20_hold_time_block_median:.1f}")

                if erc20_hold_time_block_median > 1000:
                    self.block0_green_flags.append({'erc20_hold_time_block_median': erc20_hold_time_block_median})


            if self.owner_addresses and self.owner_addresses[-1] in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:
                self.print(f"renounced ownership, ft={self.txs_sql_token_owners[-1]['block_number'] - self.first_trade_tx_sql['block_number']} (owners: {', '.join(self.owner_addresses)})")
                self.block0_green_flags.append({'renounce_ownership_ft': self.txs_sql_token_owners[-1]['block_number'] - self.first_trade_tx_sql['block_number']})
            else:
                self.print(f"liq NOT burned (owners: {', '.join(self.owner_addresses)})")


            if erc20_to_1_count >= 7:
                self.block0_green_flags.append({'erc20_to_1_count': erc20_to_1_count})


            self.print('GREEN flags:')
            for flag in self.block0_green_flags:
                self.print('  ', flag)

            self.print('self.pairs:')
            self.print(pformat(self.pairs))
            
            if self.initial_value_native_per_token_unit != 0:
                self.print(f"  self.initial_value_native_per_token_unit={self.initial_value_native_per_token_unit}")


        if self.first_trade_tx_sql and last_tx_sql['block_number'] - self.first_trade_tx_sql['block_number'] >= 1:


            #self.show_erc20_stat(on_block=last_tx_sql['block_number'])

            # blocks_before = [bl for bl in self.blocks_data.keys() if self.first_trade_tx_sql['block_number'] <= bl <= last_tx_sql['block_number']]
            # self.print(f"Торгуемые блоки начиная с нулевого: {blocks_before}")
            pass
            
        

        if self.first_trade_tx_sql:

            # for adr in self.blocks_data[last_tx_sql['block_number']]['buy_addresses']:
            #     self.print(f"address {adr}, erc20_bought_n_not_sold_addresses:")
            #     if self.balances[adr]['stat_wallet_address'] != None:
            #         for erc20_adr in self.balances[adr]['stat_wallet_address']['erc20_bought_n_not_sold_addresses']:
            #             self.print(f"   {erc20_adr}")


            if self.initial_value_native_per_token_unit == 0:
                self.print(f" initial_value_usd_per_token_unit=0, нельзя подсчитать изменение цены")
            elif self.initial_value_native_per_token_unit == None:
                self.print(f" initial_value_usd_per_token_unit=None, нельзя подсчитать изменение цены")
            else:
                if self.blocks_data[last_tx_sql['block_number']]['value_usd_per_token_unit'] != None:
                    self.print(f" На {round(100 * self.blocks_data[last_tx_sql['block_number']]['value_usd_per_token_unit'] * 10 ** self.contract_data_token['token_decimals'] / self.initial_value_usd_per_token_unit - 100, 2)}% цена изменилась в $")
                else:
                    self.print(f"\x1b[31;1m(!)\x1b[0m self.blocks_data[last_tx_sql['block_number']]['value_usd_per_token_unit'] == None, нельзя сказать на сколько цена изменилась")
            

        self.print(f"start_time_block за блок {last_tx_sql['block_number']}: {(time.time() - start_time_block):.3f}s")



        # здесь адреса owners, pairs, native
        exclude_addresses = []
        for adr in [self.address] + self.owner_addresses + self.pairs_list + [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:
            if adr in self.balances: exclude_addresses.append(adr)
        
        # if self.verbose:
        #     print('exclude_addresses:')
        #     pprint(exclude_addresses)

        holders_new = 0
        if 'buy_addresses' in self.blocks_data[last_tx_sql['block_number']]:
            for adr in self.blocks_data[last_tx_sql['block_number']]['buy_addresses']:
                if self.balances[adr]['first_trade_block'] == last_tx_sql['block_number']:
                    holders_new += 1
                    self.print(f"адрес {adr} сделал первую покупку данного токена (holders_new += 1)")

        self.print('---')
        numbers_of_repeated_buys = []
        repeated_buys_volumes = []
        if 'buy_addresses' in self.blocks_data[last_tx_sql['block_number']]:
            for adr in self.blocks_data[last_tx_sql['block_number']]['buy_addresses']:
                #assert self.balances[adr]['trades_buy'] != 0, f"self.balances[adr]['trades_buy']={self.balances[adr]['trades_buy']} != 0, wallet={adr}, address={self.address}"
                if self.balances[adr]['trades_buy'] == 0:
                    self.print(f"\x1b[31;1m(!)\x1b[0m self.balances[adr]['trades_buy']={self.balances[adr]['trades_buy']} != 0, wallet={adr}, address={self.address}", force=True)
                elif self.balances[adr]['trades_buy'] > 1:
                    numbers_of_repeated_buys.append(self.balances[adr]['trades_buy'])

                    _blocks = list(self.balances[adr]['trades_list'].keys())
                    if not _blocks:
                        self.print(f"\x1b[31;1m(!)\x1b[0m assert _blocks wallet={adr}, address={self.address}", force=True)
                        # assert _blocks, f"assert _blocks wallet={adr}, address={self.address}"
                        continue
                    last_trade = self.balances[adr]['trades_list'][_blocks[-1]][-1]
                    if last_trade['s'] != 'buy':
                        self.print(f"\x1b[31;1m(!)\x1b[0m assert last_trade['s'] == 'buy' wallet={adr}, address={self.address}", force=True)
                        #assert last_trade['s'] == 'buy', f"assert last_trade['s'] == 'buy' wallet={adr}, address={self.address}"
                        continue
                    repeated_buys_volumes.append(last_trade['invested'])


        self.print(f"numbers_of_repeated_buys={numbers_of_repeated_buys}")
        self.print(f"numbers_of_repeated_buys_count={len(numbers_of_repeated_buys)}")
        self.print(f"repeated_buys_volumes={repeated_buys_volumes}")

        self.print('---')
        sell_amount_from_previous_balance_perc = []
        amount_left_from_max_balance_who_sold_perc = []
        addresses_that_sold_to_zero_value = 0
        numbers_of_repeated_sells = []
        repeated_sells_volumes = []
        if 'sell_addresses' in self.blocks_data[last_tx_sql['block_number']]:
            for adr in self.blocks_data[last_tx_sql['block_number']]['sell_addresses']:

                amount_was_sold_perc = None
                if self.sort == 'desc' and self.balances[adr]['history_balance'][-1] == 0:
                    self.print(f"address={adr} сделал продажу, но self.sort == 'desc' and self.balances[adr]['history_balance'][-1] == 0 => не можем подсчитать")
                else:
                    if (self.can_be_old_token or self.sort == 'desc') and self.balances[adr]['history_balance'][-1] == 0:
                        self.print(f"address={adr} сделал продажу, но self.can_be_old_token=True и not self.balances[adr]['history_balance'][-1]=0")
                    else:
                        # if adr == '0x72A51d9dA0Ec9c35347de3497aC7eB859b941101':
                        #     print('HEREEE', last_tx_sql['block_number'])
                        #     print(self.balances[adr]['history_balance'])
                        # assert self.balances[adr]['history_balance'][-1] != 0, f"address={self.address}, wallet adr={adr}, history_balance={self.balances[adr]['history_balance']}"
                        if self.balances[adr]['history_balance'][-1] == 0:
                            self.print(f" \x1b[31;1m(!)\x1b[0m address={self.address}, wallet adr={adr} сделал продажу и имеет 0 на self.balances[adr]['history_balance'][-1]", force=True)
                        else:
                            amount_was_sold_perc = 100 - 100 * self.balances[adr]['value'] / self.balances[adr]['history_balance'][-1]
                            sell_amount_from_previous_balance_perc.append(amount_was_sold_perc)
                            self.print(f"address={adr} сделал продажу, имел balance={self.balances[adr]['history_balance'][-1]}, теперь {self.balances[adr]['value']} => amount_was_sold_perc={amount_was_sold_perc:.2f}%")
                            if amount_was_sold_perc < 0:
                                self.print(f" \x1b[31;1m(!)\x1b[0m amount_was_sold_perc={amount_was_sold_perc:.2f}% < 0 для wallet={adr}, address={self.address}, self.initial_value_usd_per_token_unit={self.initial_value_usd_per_token_unit}", force=True)

                _max = max(self.balances[adr]['history_balance'])
                if _max == 0:
                    if self.can_be_old_token or self.sort == 'desc':
                        self.print(f" у него max_balance=0 => не можем подсчитать")
                    else:
                        self.print(f" \x1b[31;1m(!)\x1b[0m address={self.address}, wallet adr={adr}, у него max_balance=0 => не можем подсчитать, не должно быть", force=True)
                else:
                    amount_left_from_max_balance_who_sold_perc.append(100 * self.balances[adr]['value'] / _max)
                    self.print(f" у него max_balance был {_max}, теперь {(100 * self.balances[adr]['value'] / _max):.2f}% осталось ({self.balances[adr]['value']})")
                    if 100 * self.balances[adr]['value'] / _max > 100:
                        self.print(f" \x1b[31;1m(!)\x1b[0m 100 * self.balances[adr]['value'] / _max = {(100 * self.balances[adr]['value'] / _max):.2f}% > 100 для wallet={adr}, address={self.address}", force=True)

                if 0 <= self.balances[adr]['value'] <= 1 or (amount_was_sold_perc != None and amount_was_sold_perc >= 99.98):
                    addresses_that_sold_to_zero_value += 1
                    self.print(f" он продал токен в ноль (addresses_that_sold_to_zero_value += 1)")
                # _all_trades_list = list(itertools.chain.from_iterable(self.balances[adr]['trades_list'].values()))

                if self.balances[adr]['trades_sell'] > 1:
                    numbers_of_repeated_sells.append(self.balances[adr]['trades_sell'])
                    #assert self.balances[adr]['trades_sell'] != 0, self.address
                    if self.balances[adr]['trades_sell'] == 0:
                        self.print(f" \x1b[31;1m(!)\x1b[0m self.balances[adr]['trades_sell'] == 0, address={self.address}", force=True)

                    _blocks = list(self.balances[adr]['trades_list'].keys())
                    #assert _blocks, f"wallet={adr}, address={self.address}"
                    if not _blocks:
                        self.print(f" \x1b[31;1m(!)\x1b[0m not _blocks, address={self.address}", force=True)
                    else:
                        last_trade = self.balances[adr]['trades_list'][_blocks[-1]][-1]
                        # assert last_trade['s'] == 'sell', f"wallet={adr}, address={self.address}"
                        if last_trade['s'] != 'sell':
                            self.print(f" \x1b[31;1m(!)\x1b[0m last_trade['s'] != 'sell', wallet={adr}, address={self.address}", force=True)
                        else:
                            repeated_sells_volumes.append(last_trade['withdrew'])


        self.print(f"numbers_of_repeated_sells={numbers_of_repeated_sells}")
        self.print(f"numbers_of_repeated_sells_count={len(numbers_of_repeated_sells)}")
        self.print(f"repeated_sells_volumes={repeated_sells_volumes}")

        self.print('---')
        self.print(f"erc20_to_1_count={erc20_to_1_count}")
        self.print(f"holders_total={len(self.balances) - len(exclude_addresses)}")
        self.print(f"holders_new={holders_new}")
        self.print(f"holders_new_minus_erc20_to_1={holders_new - self.blocks_data[last_tx_sql['block_number']]['erc20_to_1']}")
        self.print(f"sell_amount_from_previous_balance_perc={sell_amount_from_previous_balance_perc}")
        self.print(f"addresses_that_sold_to_zero_value={addresses_that_sold_to_zero_value}")


        self.blocks_data[last_tx_sql['block_number']] |= {
            'holders_total': len(self.balances) - len(exclude_addresses), # не считая pair, native, owner
            'holders_new': holders_new, # на текущем блоке
            'holders_new_minus_erc20_to_1': holders_new - self.blocks_data[last_tx_sql['block_number']]['erc20_to_1'],
            'numbers_of_repeated_buys': numbers_of_repeated_buys, # какая по счету покупка у каждого кошелька; кол-во эл-ов этого списка есть кол-во повторных покупок
            'numbers_of_repeated_buys_count': len(numbers_of_repeated_buys),
            'repeated_buys_volumes': repeated_buys_volumes, # на какие invested значения купили токены повторные кошельки
            'sell_amount_from_previous_balance_perc': sell_amount_from_previous_balance_perc, # на сколько % продали кошельки токен от предыдущего значения их баланса
            'amount_left_from_max_balance_who_sold_perc': amount_left_from_max_balance_who_sold_perc, # сколько % value на балансе осталось у кошельков после продажи относительно их максимального значения баланса за все время
            'addresses_that_sold_to_zero_value': addresses_that_sold_to_zero_value,
            'numbers_of_repeated_sells': numbers_of_repeated_sells,
            'numbers_of_repeated_sells_count': len(numbers_of_repeated_sells),
            'repeated_sells_volumes': repeated_sells_volumes,
            'mev_bot_addresses': mev_bot_addresses,
            'mev_volumes': mev_volumes,
            'mev_profits': mev_profits,
        }

        self.print(f"")
        self.print(f"финальное значение self.blocks_data[{last_tx_sql['block_number']}]:")
        for key, value in self.blocks_data[last_tx_sql['block_number']].items():
            if 'stats_profit_window=' not in key and 'stats_erc20_' not in key:
                self.print(f"  {key}: {value}")


    def show_erc20_stat(self, on_block=None):



        
        if not self.realtime and (self.verbose or self.full_log_text_limit > 5) and len(self.blocks_data.keys()) < 20:

            # if self.w3_g_eth_block_number - last_block < 50:
            #     w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?ini"))
            # else:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?ini"))

            self.print(f"show_erc20_stat() on_block={on_block}")

            _keys_list = ["adr","index","%","invested","router","balance_eth","balance_usd","total_from","total_to","external_from","external_to","internal_from","internal_to","erc20_from","erc20_to","erc20_bought_n_not_sold","erc20_bought_n_not_sold_perc","erc721_from","erc721_to","erc1155_from","erc1155_to","diff_between_first_and_last","banana_trans","banana_trans_perc","maestro_trans","maestro_trans_perc","first_in_transaction_from_address","nonce","eth_received_external_median","eth_received_internal_median","eth_sent_external_median","eth_sent_internal_median","eth_total_median","erc20_eth_send_median","erc20_eth_received_median","erc20_hold_time_block_median","erc20_value_usd_median","avg_blocks_per_trans","external_from_div_uniq_trans","external_to_div_uniq_trans","internal_to_div_uniq_trans","erc20_from_div_uniq_trans","erc20_to_div_uniq_trans","erc20_uniq_addresses_send","erc20_uniq_addresses_received","unique_received_from_addresses","unique_received_to_addresses","unique_transactions","unique_received_from_addresses_div_uniq_trans","unique_received_to_addresses_div_uniq_trans"]
            _stats_dict_all = []
            addresses_in_list = []
            for address in self.balances.keys():

                if on_block != None and address not in self.blocks_data[on_block]['buy_addresses']:
                    continue

                if self.balances[address]['stat_wallet_address']:
                    # print(f"Адрес {address}" + (" (owner)" if address in self.owner_addresses else ""))
                    # print(f"    {self.balances[address]['%']:.2f} %")
                    # print(f"    ---")
                    _adr_res = {'adr': address[-7:], 'index': self.balances[address]['first_trade_index'], '%': round(self.balances[address]['%'], 2), 'invested': f"{round(self.balances[address]['value'] / 10 ** self.contract_data_token['token_decimals'], 2)}, {round(self.balances[address]['invested'], 2)} $"}

                    _adr_res['router'] = None
                    for f_address, router in FILE_LABELS['ethereum']['routers'].items():
                        if f_address == self.balances[address]['router']:
                            _adr_res['router'] = router.split(' ')[0].rstrip(':').lower()
                            break
                    if _adr_res['router'] == None and self.balances[address]['router'] != None:
                        _adr_res['router'] = self.balances[address]['router'][:5] + '..'

                    for key, value in self.balances[address]['stat_wallet_address'].items():
                        if key in _keys_list:
                            _adr_res[key] = value
                            if type(value) != str:
                                if value != None:
                                    value = round(value, 5)
                                else:
                                    self.print(f"(!) value почему-то == None")
                            # print(f"    {key}: {value}")

                    if address not in self.owner_addresses + [self.address] + self.pairs_list:
                        addresses_in_list.append(address)
                        _stats_dict_all.append(_adr_res)
            
            _tabulate_stats_dict_all = {}
            for ij in range(7):
                _stats_dict_all_x = []
                for st in _stats_dict_all:
                    _dic = {}
                    for key, value in st.items():
                        if ij == 0: _arr = _keys_list[:14]
                        elif ij == 1: _arr = _keys_list[14:21]
                        elif ij == 2: _arr = _keys_list[21:28]
                        elif ij == 3: _arr = _keys_list[28:35]
                        elif ij == 4: _arr = _keys_list[35:42]
                        elif ij == 5: _arr = _keys_list[42:47]
                        else: _arr = _keys_list[47:]
                        if key in _arr or key == 'adr':
                            if key == 'first_in_transaction_from_address':
                                _dic[key] = utils.print_address(value, w3=w3, last_block=self.first_trade_tx_sql['block_number'])
                            else:
                                _dic[key] = value
                    _stats_dict_all_x.append(_dic)
                if _stats_dict_all_x and len(_stats_dict_all_x[0].keys()) == 1 and 'adr' in _stats_dict_all_x[0]:
                    break
                _tabulate_stats_dict_all[ij] = _stats_dict_all_x

            for kk in _tabulate_stats_dict_all.keys():
                if not self.realtime and (self.verbose or len(self.blocks_data.keys()) <= self.full_log_text_limit):
                    self.print(tabulate.tabulate(_tabulate_stats_dict_all[kk], tablefmt='psql', headers='keys', stralign="left"))



            for address in addresses_in_list:
                self.print(address)



    
    def update_balance_address(self, address, value_usd_per_token_unit, block_timestamp, block_number, token_supply):

        if token_supply:
            self.balances[address]['%'] = 100 * self.balances[address]['value'] / token_supply

        if not value_usd_per_token_unit:
            return

        self.balances[address]['value_usd'] = value_usd_per_token_unit * self.balances[address]['value']

        avg_price = None
        _all_trades_list = list(itertools.chain.from_iterable(self.balances[address]['trades_list'].values()))
        _sum_qnt_buy = sum([x['q'] for x in _all_trades_list if x['s'] == 'buy'])
        if _sum_qnt_buy != 0:
            avg_price = sum([x['vut'] * x['q'] for x in _all_trades_list if x['s'] == 'buy']) / _sum_qnt_buy

        if avg_price != None:
            # profit calc https://www.tigerbrokers.nz/help/detail/16947286
            # logs.append(f"avg_price for {contract_data['token_symbol']}: {avg_price} $")
            self.balances[address]['unrealized_profit'] = (value_usd_per_token_unit - avg_price / 10 ** self.contract_data_token['token_decimals']) * (self.balances[address]['value'] )
            if self.balances[address]['invested'] != 0:
                self.balances[address]['roi_r'] = (self.balances[address]['invested'] + self.balances[address]['realized_profit']) / (self.balances[address]['invested']) * 100 - 100
                self.balances[address]['roi_u'] = (self.balances[address]['invested'] + self.balances[address]['unrealized_profit']) / (self.balances[address]['invested']) * 100 - 100
                self.balances[address]['roi_t'] = (self.balances[address]['invested'] + self.balances[address]['realized_profit'] + self.balances[address]['unrealized_profit']) / (self.balances[address]['invested']) * 100 - 100
            else:
                self.balances[address]['roi_r'] = 0
                self.balances[address]['roi_u'] = 0
                self.balances[address]['roi_t'] = 0
        else:
            # здесь подсчет кошельков, у которых invested=0
            self.balances[address]['unrealized_profit'] = value_usd_per_token_unit * self.balances[address]['value']
        
        self.balances[address]['pnl'] = self.balances[address]['realized_profit'] + self.balances[address]['unrealized_profit']

        if self.first_trade_tx_sql and (block_number - self.first_trade_tx_sql['block_number']) * 12 / 60 <= 30: # 30m
            self.balances[address]['roi_by_blocks'][block_number] = {
                'r': self.balances[address]['roi_r'],
                'u': self.balances[address]['roi_u'],
                't': self.balances[address]['roi_t'],
                'rp': self.balances[address]['realized_profit'],
                'urp': self.balances[address]['unrealized_profit'],
                'pnl': self.balances[address]['pnl'],
            }


        #if not v['sell_trades'] or value['value'] != 0: value['sell_trades'].append(utils.get_block_time(w3, self.last_block))
        days_ = []
        sell_trades = [x['t'] for x in _all_trades_list if x['s'] == 'sell']
        if not sell_trades or self.balances[address]['value'] > 1: sell_trades.append(block_timestamp)
        for st in sell_trades:
            days_.append((datetime.fromtimestamp(st) - datetime.fromtimestamp(self.balances[address]['first_trade'])).total_seconds() / 86400)
        if days_:
            self.balances[address]['days'] = sum(days_)/len(days_)



    # (!) получение tx_hash список транзакций, на которых получили Mint() для proceed_mint_logs() нужно переписать
    # сейчас Mint() ищется по get_token_transactions_only_transfers() через логи по pair_created; но в реале Mint() может быть без создания пары
    # поэтому сейчас могут пропускаться Mint'ы

    def proceed_mint_logs(self, w3, tx_hash, tx_eth_price):


        _testnet = 'http://127.0.0.1:8545'


        last_swaped = {} # значение для понимания что есть amount0, amount1 в Mint(); ключ - это пар, value - значение True/False

        self.print('self.txs_sql_pairs:')
        self.print(pformat(self.txs_sql_pairs))


        # этот цикл нужен только для определения last_swaped
        for tx_sql in self.txs_sql_pairs:

            decoded_data = tx_sql['decoded_data']

            # SetPairAddress
            # 0x3e6ab880d904bc8c25d9362a45e29de45203e5ac9a4f98e287bd14bb0f1e3094
            # PairCreated
            # 0xd6c0e7d791551b3c9c5c2d2b8015489fe252a0b44f253f2edbdc289be723d6b6
            # 0xe0a465d1e4a622ca43db886b589122c6f44390b6a1a1715aab0948ac246619c9
            # 0x7e2d49d0bb6c3c325934eb2586088cb456c571ee11452031bd1e6130a82b9e11
            # PoolCreated
            # 0xe2adabc835d5659444859aa285a117ff80c990a21c9791ad4a5847123da3e535
            # 0x73f089f3562602a15034cca283a6c0da2d6d2223d1e48e86cc3c83868d813b5c

            if tx_sql['log_address'] == utils.UNI_V2_FACTORY.lower():
                pair_address = decoded_data['pair']
            if tx_sql['log_address'] == utils.UNI_V3_FACTORY.lower():
                pair_address = decoded_data['pool']
            assert pair_address == to_checksum_address(tx_sql['pair_created']), f"{pair_address}, {tx_sql['pair_created']} {self.address}"
            if decoded_data['token1'] in [utils.WETH, utils.USDC, utils.USDT]:
                token0 = decoded_data['token0']
                token1 = decoded_data['token1']
                last_swaped[pair_address] = False
            elif decoded_data['token0'] in [utils.WETH, utils.USDC, utils.USDT]:
                token0 = decoded_data['token1']
                token1 = decoded_data['token0']
                last_swaped[pair_address] = True
            else:
                # assert 0, f"Неверное распределение token0/token1: {decoded_data}, т.е. пара {pair_address} не записалась в last_swaped"
                self.print(f"\x1b[31;1m(!)\x1b[0m для address={self.address} неверное распределение token0/token1: {decoded_data}, т.е. пара {pair_address} не записалась в last_swaped", force=True)

            # self.pairs[pair_address] = {
            #     'initial_liquidity': None,
            #     'token1_address': token1,
            #     'wallet_token': {'Wei': {'value': 0, 'value_usd': None}},
            # }
            # if log['address'] == utils.UNI_V2_FACTORY:
            #     self.pairs[pair_address]['exchange'] = 'V2'
            # if log['address'] == utils.UNI_V3_FACTORY:
            #     self.pairs[pair_address]['exchange'] = 'V3'
            # self.pairs[pair_address]['block_created_number'] = tx_sql['block_number']
            # self.pairs[pair_address]['block_created_timestamp'] = tx_sql['block_timestamp']
            # self.print(f"      PairCreated: pair address={pair_address}")
            # self.print(f"      PairCreated: exchange={self.pairs[pair_address]['exchange']}")

        

        tx_sql = self.txs_sql_mints[tx_hash]


        mint_number_among_others = None
        i = 1
        for hash_, item in self.txs_sql_mints.items():
            if hash_ == tx_hash:
                mint_number_among_others = i
                break
            i += 1

        if tx_sql['logs_array'] and (len(tx_sql['logs_array']) < 1000 or tx_sql['logs_array'][0]['logIndex'] > tx_sql['logs_array'][-1]['logIndex']):
            tx_sql['logs_array'] = sorted(tx_sql['logs_array'], key=lambda d: d['logIndex'])

        for ii, log in enumerate(tx_sql['logs_array']):

            log['address'] = to_checksum_address(log['address'])

            if not self.realtime and (self.verbose or len(self.blocks_data.keys()) <= self.full_log_text_limit):
                abi = None
                if log["address"] in self.addresses_for_abi:
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
                    ]
                if log['address'] not in self._contracts:
                    _co = w3.eth.contract(address=log["address"], abi=abi)
                    self._contracts[log['address']] = {
                        # 'contract': _co,
                        'data': utils.get_contract_data(w3, _co, block=tx_sql['block_number'])
                    }

                # contract = self._contracts[log['address']]['contract']
                contract_data = self._contracts[log['address']]['data']

                self.print(f" \x1b[34;1m [{ii + 1}/{log['logIndex']}]\x1b[0m address={log['address']} {contract_data}")                
                self.print(f"      \x1b[34;1m{utils.get_event_name_by_sig(log['topics_0']).split('(')[0]}\x1b[0m {log['decoded_data'] or {}}")
                self.print(f"      topic_0={log['topics_0']}")
                if log['topics_1'] != None: self.print(f"      topic_1={log['topics_1']}")
                if log['topics_2'] != None: self.print(f"      topic_2={log['topics_2']}")
                if log['topics_3'] != None: self.print(f"      topic_3={log['topics_3']}")

                
            decoded_data = log['decoded_data']



            # Mint
            # V2 0xe0a465d1e4a622ca43db886b589122c6f44390b6a1a1715aab0948ac246619c9
            # V2 0x7e2d49d0bb6c3c325934eb2586088cb456c571ee11452031bd1e6130a82b9e11
            # V3 0xe2adabc835d5659444859aa285a117ff80c990a21c9791ad4a5847123da3e535
            # V3 0x73f089f3562602a15034cca283a6c0da2d6d2223d1e48e86cc3c83868d813b5c
            if log['topics_0'] in ['0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f', '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde']:
                amount0 = None

                self.print(f"Found Mint()! tx_hash={tx_sql['hash']}, tx_block={tx_sql['block_number']} ({datetime.fromtimestamp(tx_sql['block_timestamp'])})")

                # (!) decoded_data нужно будет брать уже готовую из бд, поэтому ниже код будет удален
                if decoded_data != None and 'amount0' not in decoded_data or decoded_data == None:
                    def _make_decode(w3, contract, abi, log):
                        start_time = time.time()
                        decoded_data, evt_name, logs_dlc = utils.decode_log_contract(w3, contract, abi, log)
                        if 1:
                            for lo in logs_dlc:
                                self.print('Error _make_decode():', lo, 'tx hash:', tx_sql['hash'], force=True)
                        return decoded_data, evt_name
            
                    tx_receipt = requests.post(_testnet, json={"method":"eth_getTransactionReceipt","params":[tx_sql['hash']],"id":2,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']
                    # print('tx_receipt:')
                    # print('hash:', tx_sql['hash'])
                    # print('block:', tx_sql['block_number'])
                    # pprint(requests.post(_testnet, json={"method":"eth_getTransactionReceipt","params":[tx_sql['hash']],"id":2,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json())
                    
                    if tx_receipt == None and (self.can_be_old_token or mint_number_among_others > 1):
                        return

                    assert tx_receipt != None, f"address={self.address}, tx_hash={tx_sql['hash']} mint_number_among_others={mint_number_among_others}"
                    # if tx_receipt == None:
                    #     if tx_hash == self.txs_sql_mints[list(self.txs_sql_mints.keys())[0]]:
                    #         assert tx_receipt != None, f"address={self.address}, tx_hash={tx_sql['hash']} tx_receipt=None"
                    #     else:
                    #         self.print(f"assert tx_receipt != None, address={self.address}, tx_hash={tx_sql['hash']}", force=True)
                    #         break
                    
                    assert log['address'].lower() == tx_receipt['logs'][ii]['address'].lower(), f"hash={tx_sql['hash']} ii={ii} address={self.address.lower()} {log['address'].lower()} {tx_receipt['logs'][ii]['address'].lower()}"
                    abi = [
                        # Mint(address,uint256,uint256) V2
                        json.loads('{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"}'),
                        # Mint(address,address,int24,int24,uint128,uint256,uint256) V3
                        json.loads('{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"}'),
                    ]
                    decoded_data, evt_name = _make_decode(w3, AttributeDict({'address':log['address'],'abi':abi}), abi, tx_receipt['logs'][ii])
                    self.print(f"      decoded_data={decoded_data}")
                    self.print(f"      (!) Mint decode was made again on tx_hash={tx_sql['hash']}, tx_block={tx_sql['block_number']} ({datetime.fromtimestamp(tx_sql['block_timestamp'])})")
                    if self.realtime:
                        print(f"      (!) Mint decode was made again on tx_hash={tx_sql['hash']}, tx_block={tx_sql['block_number']} ({datetime.fromtimestamp(tx_sql['block_timestamp'])})")

                for pair_address, values in self.pairs.items():
                    if log['address'] == pair_address: # 'la' = log_address (в данном случае адрес созданной пары)
                        if pair_address not in last_swaped:
                            if self.pairs[pair_address]['token1_address'] == utils.WETH:
                                amount0 = decoded_data['amount0']
                                amount1 = decoded_data['amount1']
                            else:
                                amount0 = decoded_data['amount1']
                                amount1 = decoded_data['amount0']
                        elif last_swaped[pair_address] == False:
                            amount0 = decoded_data['amount0']
                            amount1 = decoded_data['amount1']
                        elif last_swaped[pair_address] == True:
                            amount0 = decoded_data['amount1']
                            amount1 = decoded_data['amount0']

                        #if len(self.transactions_mint) == 0:
                        # если это первый Mint() токена за все время
                        if tx_sql['hash'] == [x['hash'] for x in self.txs_sql_mints.values()][0]:
                            # if self.sort == 'asc':
                            #     assert self.total_supply == None, self.total_supply
                            assert self.pairs[pair_address]['initial_liquidity'] == None, self.pairs[pair_address]['initial_liquidity']
                            #self.total_supply = amount0
                            self.pairs[pair_address]['token1_liquidity'] = 0
                            self.pairs[pair_address]['token1_liquidity_usd'] = 0
                            self.pairs[pair_address]['balances'] = {}
                            self.pairs[pair_address]['initial_liquidity'] = {'value': amount1}
                            self.pairs[pair_address]['extra_liquidities'] = []

                            # self.print('extra_liquidities created!', pair_address)

                            if self.pairs[pair_address]['token1_address'] == utils.WETH:
                                self.pairs[pair_address]['initial_liquidity']['value_eth'] = float(from_wei(amount1, 'ether'))
                                self.pairs[pair_address]['initial_liquidity']['value_usd'] = amount1 / tx_eth_price
                                self.print(f"      Mint: initial_liquidity={from_wei(amount1, 'ether')} ETH, {round(amount1 / tx_eth_price, 2)} $")
                            elif self.pairs[pair_address]['token1_address'] in [utils.USDC, utils.USDT]:
                                self.pairs[pair_address]['initial_liquidity']['value_eth'] = float(from_wei(amount1 / 10 ** 6 * tx_eth_price, 'ether'))
                                self.pairs[pair_address]['initial_liquidity']['value_usd'] = amount1 / 10 ** 6
                                self.print(f"      Mint: initial_liquidity={round(amount1 / 10 ** 6, 2)} $")

                            # _amount0 = self.total_supply
                            # _amount1 = self.pairs[pair_address]['initial_liquidity']['value']
                            _amount0 = amount0
                            _amount1 = amount1
                            #if not self.initial_value_native_per_token_unit:
                            if self.pairs[pair_address]['token1_address'] == utils.WETH:

                                if _amount0 == 0:
                                    #assert 0, f"_amount0=0, address={self.address}"
                                    self.print(f"\x1b[31;1m(!)\x1b[0m _amount0=0, address={self.address}", force=True)
                                    _amount0 = decoded_data['amount']

                                self.initial_value_native_per_token_unit = (_amount1 / 10 ** 18) / (_amount0 / 10 ** self.contract_data_token['token_decimals'])
                                self.initial_value_usd_per_token_unit = (_amount1 / tx_eth_price) / (_amount0 / 10 ** self.contract_data_token['token_decimals'])
                            else:
                                if _amount0 != 0:
                                    self.initial_value_native_per_token_unit = (_amount1 / 10 ** 6) / (_amount0 / 10 ** self.contract_data_token['token_decimals'])
                                    self.initial_value_usd_per_token_unit = self.initial_value_native_per_token_unit
                                else:
                                    self.print(f"\x1b[31;1m(!)\x1b[0m почему-то _amount0=0, address={self.address}", force=True)

                            
                        
                        else:

                            if self.pairs[pair_address]['token1_address'] == utils.WETH:
                                value_usd = amount1 / tx_eth_price
                                self.print(f"      Mint: extra_liquidity={from_wei(amount1, 'ether')} ETH, {round(amount1 / tx_eth_price, 2)} $")
                            elif self.pairs[pair_address]['token1_address'] in [utils.USDC, utils.USDT]:
                                value_usd = amount1 / 10 ** 6
                                self.print(f"      Mint: extra_liquidity={round(amount1 / 10 ** 6, 2)} $")

                            # if amount0 is None:
                            #     self.print(f"\x1b[31;1m(!)\x1b[0m почему-то amount0=None, address={self.address}", force=True)
                            # elif self.total_supply is None:
                            #     if not self.can_be_old_token:
                            #         self.print(f"\x1b[31;1m(!)\x1b[0m self.total_supply=None, address={self.address}")
                            # else:
                            #     self.total_supply += amount0
                            self.print('now for new extra_liquidities', pair_address)
                            if 'extra_liquidities' not in self.pairs[pair_address]:
                                if self.sort == 'asc':
                                    self.print(f"\x1b[31;1m(!)\x1b[0m 'extra_liquidities' not in self.pairs[{pair_address}], address={self.address}", force=True)
                                self.pairs[pair_address]['extra_liquidities'] = []
                            self.pairs[pair_address]['extra_liquidities'].append({'value': amount1, 'value_usd': value_usd})
                            self.print(f"      len extra_liquidities={len(self.pairs[pair_address]['extra_liquidities'])}")


                        minted = True
                        # (!) это надо раскомментить и убрать эту же строку ниже еще
                        # self.transactions_mint.append(tx_sql)
                        self.print(f"      Mint: total_supply={amount0}")
                        if self.total_supply == None:
                            self.total_supply = amount0
                        





def wrapper_last_trades(address, last_block):

    # try:
    #     with conn.cursor() as cursor:
    #         pass
    # except psycopg2.InterfaceError as e: # connection already closed
    #     print(f"\x1b[31;1m(!)\x1b[0m {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {type(e)}: {e}")
    #     conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)


    # time.sleep(random.randint(1,8))


    for _ in range(5):
        try:
            with psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs) as conn:

                was_error = False
                try:


                    lkwards = {
                        'conn': conn,
                        'address': address,
                        'last_block': last_block,
                        'can_be_old_token': True, # !!!!!!!!!
                        'limit': 60000 if len(list_addresses) == 1 else 60000,
                        'sort': 'desc', # !!!!!!!!!!!
                        'realtime': False,
                        'verbose': 0 if len(list_addresses) == 0 else 0,
                    }

                    # load_object=True
                    if 0:
                        assert lkwards['sort'] == 'asc', f"{address} sort={lkwards['sort']}"

                        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                            cursor.execute(f"""
                                SELECT
                                    count(*) as cnt
                                FROM last_trades_token_status
                                WHERE script='last_trades'
                            """)
                            _count = cursor.fetchone()
                            _count = _count['cnt']

                            print(f"Сейчас выполняется {_count} потоков в LastTrades()")
                            max_processes = 1
                            if 0 and _count >= max_processes:
                                raise Exception(f"max processes reached. _count={_count} >= max_processes={max_processes}, address={address}")
                            
                            try:
                                cursor.execute("""
                                    INSERT INTO last_trades_token_status (
                                        address,
                                        script
                                    ) VALUES %s;
                                """, ((
                                        address.lower(),
                                        "last_trades"
                                ),))
                                conn.commit()
                            except Exception as e:
                                print(f"INSERT INTO last_trades_token_status address={address} error")
                                conn.rollback()
                                raise e

                            if 1:
                                try:
                                    os.remove(f"{DIR_LOAD_OBJECTS}/{to_checksum_address(address)}_last_trades.pkl")
                                except OSError:
                                    pass

                            try:
                                with open(f"{DIR_LOAD_OBJECTS}/{to_checksum_address(address)}_last_trades.pkl", 'rb') as o:
                                    last_trades = pickle.load(o)

                                print('last_trades loaded!')
                                print(f"self.balances len={len(last_trades.balances)}")
                                if '0x72A51d9dA0Ec9c35347de3497aC7eB859b941101' in last_trades.balances:
                                    print(f"self.balances[0x72A51d9dA0Ec9c35347de3497aC7eB859b941101][history_balance]={last_trades.balances['0x72A51d9dA0Ec9c35347de3497aC7eB859b941101']['history_balance']}")
                                else:
                                    print("0x72A51d9dA0Ec9c35347de3497aC7eB859b941101 not in balances")

                                last_trades.reinit(conn=conn, last_block=last_block, from_block=last_trades.last_block_by_last_transaction + 1, limit=lkwards['limit'], realtime=lkwards['realtime'], verbose=lkwards['verbose'])
                            except FileNotFoundError:
                                last_trades = LastTrades(**lkwards)

                    else:
                        last_trades = LastTrades(**lkwards)
                    
                    
                    
                    last_trades.get_trades()
                    start_time = time.time()
                    last_trades.create_dataframe()

                    # print(f"self.balances len={len(last_trades.balances)}")
                    # if '0x72A51d9dA0Ec9c35347de3497aC7eB859b941101' in last_trades.balances:
                    #     print(f"self.balances[0x72A51d9dA0Ec9c35347de3497aC7eB859b941101][history_balance]={last_trades.balances['0x72A51d9dA0Ec9c35347de3497aC7eB859b941101']['history_balance']}")

                    if not lkwards['verbose']:
                        print(f"saving object to {DIR_LOAD_OBJECTS}/{to_checksum_address(address)}.pkl, create_dataframe() in {(time.time() - start_time):.2f}s, df len={len(last_trades.dataframe)}")
                        with open(f"{DIR_LOAD_OBJECTS}/{to_checksum_address(address)}.pkl", 'wb') as o:
                            pickle.dump(last_trades, o)


                        

                    balances_ = sorted(last_trades.balances.items(), key=lambda d: (d[1]['pnl']))
                    if last_trades.verbose:
                        print('-'*50)
                        print('balances addresses list:')
                        for _address, value in balances_:
                            print(_address)

                        print(last_trades.balance_table(order='pnl'))
                    
                    print(f"done success {address}")
                #except:
                except Exception as e:
                    try:
                        conn.rollback()
                    except:
                        pass
                    if str(e) == 'w3_g not connected':
                        print(f'it was w3_g not connected for address={address}')
                        time.sleep(5)
                    print(traceback.format_exc())
                    print(f"done fail {address}")
                    was_error = True


                if 0:
                    if conn.closed:
                        conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)
                    with conn.cursor() as cursor:
                        try:
                            cursor.execute(f"""
                                DELETE
                                FROM last_trades_token_status
                                WHERE script='last_trades'
                                and address='{address.lower()}'
                            """)
                            conn.commit()
                        except:
                            print(f"DELETE FROM last_trades_token_status address={address}, error: {str(traceback.format_exc())}")
                            conn.rollback()
                    
                if was_error:
                    return None

                break
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e: # psycopg2.InterfaceError - connection already closed
            print(f"\x1b[31;1m(!)\x1b[0m {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} address={address} {type(e)}: {e}")
            time.sleep(1)


    # try:
    #     del last_trades.conn
    # except UnboundLocalError:
    #     print(f"(!) UnboundLocalError for last_trades.conn")
    #     return None
    
    try:
        #return last_trades
        return None

    # psycopg2.OperationalError: connection to server at "127.0.0.1", port 60444 failed: FATAL:  remaining connection slots are reserved for non-replication superuser connections
    except UnboundLocalError:# UnboundLocalError: cannot access local variable 'last_trades' where it is not associated with a value
        return None



if __name__ == '__main__':

    testnet = 'http://127.0.0.1:8545/?wp'

    w3 = Web3(Web3.HTTPProvider(testnet))
    if not w3.is_connected():
        raise Exception("w3 not connected")


    # DIR_LOAD_OBJECTS = '/disk_sdc/last_trades_objects'
    DIR_LOAD_OBJECTS = '/disk_sdc/last_trades_objects_can_be_old'
    
    
    path = DIR_LOAD_OBJECTS
    files = [f for f in listdir(path) if isfile(join(path, f)) and f != '.DS_Store']
    addresses_in_folder = []
    for file in files:
        if 0:
            os.remove(f"{path}/{file}")
        else:
            _ff = file.split(',')[0].split('.')[0].lower()
            if ']' in _ff: _ff = _ff.split(']')[1]
            addresses_in_folder.append(_ff)
    
    print(f"found addresses_in_folder={len(addresses_in_folder)} addresses in {path}")
    start_time_e = time.time()


    with psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs) as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("""
                    DELETE
                    FROM last_trades_token_status
                    WHERE script='last_trades'
                """)
                conn.commit()
            except Exception as e:
                print(traceback.format_exc())
                conn.rollback()
                exit()

    """
    токены, где до Mint() была раздача токенов разным кошелькам
    """
    # 0xfefe157c9d0aE025213092ff9a5cB56ab492BaB8

    """
    токены c locked liq, но liq < 1$
    """
    # 0xB962A3A25d3d3D6cc5ae49c9ccd82675d8347DEA

    """
    токен c locked liq и honeypot.is чист, но скам
    """
    # 0x0f3e84fE0a989ca4F8f7B2073e285c13f2feB677

    """
    токены, где очень четко и хорошо ema cross идет
    """
    # 0x36178503ed14ccbe7ee6fa0c3d4ba1f91f2ba3ac

    """
    токен, где в транзакции sell очень много Swap
    и после первых блоков падение токена
    """
    # 0x178221CBc1a669771D53E22725c96CBcF223Ab33 (0x8b13e0f6652089969055b63a53b5d09eef7f06940cddf48faabf2b9bf9acfc6c)
    # 0x9D20731C8cb82cC6264711Cf5a5F83196B9B1CE4 (0x4a0d883405367d57bb0520d21c45276d4c40f0d3b99cb28ecc76ca08559eaba0)
    # 0x81b1a1817215E3aC256932d868b01B06dEd47215
    # 0x0000642d6E4Dd287aB91901f865Ad7Ee3DE0fe6E
    # 0x151f7E063cFFD25Fe5236F255E1cC7f6345096c4 (транзакция 0x0e40368d300c2619bf1c919ca6017a1579b68f01cfd8f0ce7ff92a8011f4c438)

    """
    токены, где владелец/маркетмейкер вначале первый закупил токены и холдят долгое время
    """
    # 0x36C7188D64c44301272Db3293899507eabb8eD43
    # 0x3fFEea07a27Fab7ad1df5297fa75e77a43CB5790




    LABEL_TOKENS = '[SC]'
    #with open(f"{FILES_DIR}/temp/22jun_2jul_not_scam_tokens.txt", "r") as file:
    #with open(f"{FILES_DIR}/temp/22jun_2jul_high_profit_tokens.txt", "r") as file:
    # with open(f"{FILES_DIR}/temp/15aug_31aug_scam_tokens.txt", "r") as file:
    #     list_addresses = json.load(file)
    

    LABEL_TOKENS = None
    with open(f"{FILES_DIR}/temp/13feb_14nov_not_scam_tokens.txt", "r") as file:
        list_addresses = json.load(file)
    # list_addresses = list(map(str.lower, list(list_addresses.keys())))


    LAST_BACKTESTING_BLOCK = w3.eth.block_number
    print(f"w3.eth.block_number={LAST_BACKTESTING_BLOCK}")
    #exit()


    # results_22jun-2jul_block5
    # list_addresses = ["0x9959d38d7dd5a5b92def3468fba4a80038ade4e9","0x9b465aab9e8f325418f222c37de474b1bd38ded2","0x69695c8427c01038cb11091f3820cb457668acba","0x0000721475421300e285d0a33f660717da058c21","0x003099e3456f358b583b0604009f650a4e5442cf","0x6284f7760989e4c29844099ff2c469edb1985977","0x5cfbb606c1591c9e726fa4221844e00db1c671ca","0x667bd45d6911ea696e33b1eea5022a8fbba1fe79","0x91e674cf737e7dc80805d40e853b4a31559e0a47","0x61e9e8b1c45110285c320f5ea36fbf652136dfc2","0x88a4e88d3e5ad70b5c78f9009eddb5816f3cd621","0xde46b6d895d7c821fc886a1890967c6e7f5943cd","0xaed18826655bf9167a377c8647132a937d6a4f36","0x7e71942fe2b24e304abd172e4709139656fa2bbe","0xb9d09bc374577dac1ab853de412a903408204ea8","0xc3f78ecaf481eae4eb32d43233b3a60469d5dfa9","0x82a0dce6956fc709cd602968378110e7ac36b420","0x7445fc922e383bb61da62b7d68c6ec5d164af446","0xd2041a7976f64d612563dd2e02f469b48917ffc3","0x7ddd18b77e390bf7c42ba204b8958d37d5bf7716","0x66c58510e15c25268c1aec890e6210e5bd4433ae","0x039d4f77dd7ae619e09143cb225f2ac47198e774","0xc7e3b4ccb7d9683b85b32ebc636310232b401bb9","0xbd76e5912c5fb7a83e0cdec716468fe49ec71ee4","0xdc8dfd979293302568b82ec0036e815c7ee3953e","0x429b90dab7105ed5a96319449c985925ab0dbddc","0xa1ba34b1906c6706cee904b6d8bb08a6412d94a5","0x5366b56ac001bad6421a9aa46b73b5383f790bcb","0xbb98a11694452488756a540553960a5999cc620a","0x696969e69c81337d18133d245a6b8dbee7533694","0xb69100340a5947e856d873463694ae2186146c43","0xa2f2fcf57249db7c72191768090b9f57e472aef8","0x4abd5745f326932b1b673bfa592a20d7bb6bc455","0x3c639071a384d3cad693b0ac3e853ea4c2701c4e","0x6269093cee4f0a7c8194f69a34104647b03d86d8","0xcd605ac26fa22a316148aca8fe0dae7c747dcb41","0x8392d6783eb0e26fe238b5f42171b9656bfe99a0","0x38b9cdf27192e838e1e549c58ace5387187a0737","0x734be1702fda8a6cd3e68c6e6171e995d7531d65","0x6983a54320c11b4b0511d9110ee6dfc490e3b0d2","0x07049d0aac3424d2a65d96035c73bb38ad89dc24","0xa728f48f9ad6340f478cf2dc24b3d8f315b911bf","0x670176431fd8c1579b2d98ec8a2d3ed2a50bd067","0x8ef2db537ee1f33d109c83e654386f960ef2af69","0xfaded69acbc3adbca0886de811fd079843706a16","0x1d1ce9b1273d967a12ec4572e6a27eb7ac32a2db","0x607be2ea94580de46a33992f15948fc86b33b8f5","0x7ac126efa0758d0b199f07f3d1d66a4f82856df6","0x68ad75469db9181a1144e769c16adf47f2f32cae","0x381491960c37b65862819ced0e35385f04b2c78b","0x7abff417f2c34398216c1a8eb85ca1f30a27214d","0x0c04ff41b11065eed8c9eda4d461ba6611591395"]

    
    # backtest.js
    # list_addresses = ["0x5ef94d6fC090227904771D7c6E90cc8F95F27047","0xf00F0020138f1508008729254E0223bB938d3F66","0x5B943d32281de1099Fc977E9214c9784Fc3D49A3","0x9D20731C8cb82cC6264711Cf5a5F83196B9B1CE4","0x636bd98fC13908e475F56d8a38a6e03616Ec5563","0x8392D6783eb0E26FE238B5f42171b9656BFE99a0","0x8ef2db537ee1f33d109c83e654386f960ef2af69","0x6135177A17E02658dF99A07A2841464deB5B8589","0x7E7fe8F234FCa86a8F1C03E263B7639Aa45deEcB","0x1992cC34B95eD3d11Cd6d505b6608eB5F3136Ab2","0x4704cF8D968aA0AF61ed7Cf8F2DE0d0B31cAb623","0x04D696A2cA34747dd29Da99A58745769213DDdCF","0x0fd5020aa4de6ee5ffbab4956aa6871ceb0b4dae","0x791776A295CDae17367d0a959dd040566b3DdAA1","0xB8A914A00664e9361eaE187468EfF94905dfbC15","0x42069cc15F5BEfb510430d22Ff1c9A1b3ae22CfE","0x4C44A8B7823B80161Eb5E6D80c014024752607F2","0xC3DC991F2E0D39B3F3C4F8aAEdB30F81207974C6","0xBbd91d5cda7085a186b1354e1b0744bb58ad7Cf6","0x69EE720C120EC7c9C52A625C04414459B3185f23","0x683A4ac99E65200921f556A19dADf4b0214B5938","0x36C7188D64c44301272Db3293899507eabb8eD43","0x8802269D1283cdB2a5a329649E5cB4CdcEE91ab6","0x3fFEea07a27Fab7ad1df5297fa75e77a43CB5790"]
    
    # not_scam_22jun-2jul
    # list_addresses = ["0x71e63826a2433180374a718fbe247a91b530ff84", "0x5ef94d6fc090227904771d7c6e90cc8f95f27047", "0xaf659fbb89516a2caf6a4d43b050bc223a28c2f1", "0x48b04fc08aea778c13fcaee02e307043143988e9", "0x1babd2017df5a444252f81bb2be186961dab3d28", "0xd875a85c5fe61a84fe6bdb16644b52118fdb8dfd", "0x0ac9e8d8b3e87ace7938539c49cd7f7c6ceb3f20", "0x6968679aa3030532abdadea4321de041749aea95", "0xf00f0020138f1508008729254e0223bb938d3f66", "0x696969ade0cc455414fc4800ebea505d690b2429", "0x6e9730ecffbed43fd876a264c982e254ef05a0de", "0xe315c322f17fc12d23bebde1acd22854323d3c46", "0xce0ca7a150a441cc52415526f2eac97aa4140d13", "0xfce1baebcd6c05196448465312b68c276651819c", "0x41c10e79dc405513401a93cddbd9b3f0023a85ed", "0x381491960c37b65862819ced0e35385f04b2c78b", "0xb699c99063827a003dde7203678f1cbd4dd2596f", "0xd62bbc1abdd0dfa1f33afffcf3891b9f10b22f31", "0x2a82aee9e7be48a17ef5b95d784e555f1f2c9814", "0x636bd98fc13908e475f56d8a38a6e03616ec5563", "0x56a187c49011bafb01e9c32ce90b5d259bee0c12", "0x5b943d32281de1099fc977e9214c9784fc3d49a3", "0x9d20731c8cb82cc6264711cf5a5f83196b9b1ce4", "0xa5a3aa9046915a34abf5f0852cc7e6010e880f5e", "0xaafb9f72047289af0ef6f3e10d46ab64ad984910", "0x11111c283d87f443e7eaca86cfa360059d6c4216", "0x8279735af191add7721df39bf91dd32882b90e9f", "0x15f34c43b13913577e76f2d872d5373f8b9012a9", "0x0dbb0b1cc1b4bbf414511b2a9be5ab7921189356", "0xc3f78ecaf481eae4eb32d43233b3a60469d5dfa9", "0x683cce3e3a8a57a0fb6eecfbbdcf86a528111e54", "0xa7c33e1754b75d4109177eb3e5178b103c9241cd", "0x8f428eacaa9def715117ad95e5f68d2e5882dbf1", "0x039d4f77dd7ae619e09143cb225f2ac47198e774", "0x2300e6343471d762cf12f46e40c4e2d2ce624d28", "0x1d1ce9b1273d967a12ec4572e6a27eb7ac32a2db", "0x5505fa45da599eab7fcc7b8c579c6d3509646fb7", "0x3b991130eae3cca364406d718da22fa1c3e7c256", "0xabb41328eb3b89465cc1d6638f6b6b828adcc96d", "0x84c80cd3d2ff3ac31d2dad8a40f474c47f6fdb5a", "0x07049d0aac3424d2a65d96035c73bb38ad89dc24", "0x69691c7e9b7c055ccaa795ad4a8c2c1ba5d9af69", "0xd721ea4f9d351fbff4684108282c56cb222ce087", "0x3da56ec4ec7d080c2677c081fbeb47a152c9551f", "0x4206984834ca2001ca4b203d3a7dd3edd6185b1a", "0x945a9f6c5be130190aad5bf436b3052d6a6f098f", "0x6a68dd32846391b053ed80188be669e6b9bd8892", "0x38b9cdf27192e838e1e549c58ace5387187a0737", "0xbe660f8213fa2b07372a42725773c31136ed171d", "0xca84898ab54c7d07cd9e7b179a512f1b462f6fbf", "0xa09a12b298853fc095a44dff40aecab94e5d6e9d", "0xfe14e3138d0e3f74c9f964583113c613a248cc32", "0x3927fb89f34bbee63351a6340558eebf51a19fb8", "0x9b94c7c05a30005f46682c9edd29009210a61937", "0x80486e01dd260e2e838bdd4d408bcf2366154db7", "0x6723643e9d4f3d382aa3b88f5a9262a378f97534", "0x66c58510e15c25268c1aec890e6210e5bd4433ae", "0x99979551aa8cfc7eae6c4fc1651046e0330e4dc0", "0xbbd91d5cda7085a186b1354e1b0744bb58ad7cf6", "0x607be2ea94580de46a33992f15948fc86b33b8f5", "0x99765236c134faf19f4384d95cf2bcdddb14af26", "0xcaf4cb06beec11ff5ce3236c6d21308894e010bc", "0xedd9bcf69ed719b0e403adc41a9d7311af4c3821", "0x3bf544480503f0a5bf56b120dee596aba173f510", "0xcab0a151152c8e30fb2016ce3dcbe5303e8cd6c0", "0x62e3b3c557c792c4a70765b3cdb5b56b1879f82d", "0x0fd5020aa4de6ee5ffbab4956aa6871ceb0b4dae", "0x4ec60512f54a196c5a4792437cd500fa98747dea", "0x0101c9eed0391ee21d9d94259ea09412e97c2ba5", "0x69d094e731cb891ad4b43e620deef19b09a545a4", "0x7d3563a64f7909e47ddae5bf76fe6ce3fd83dc90", "0xfaded69acbc3adbca0886de811fd079843706a16", "0x2a5a6763797921bfa2eaa66699996f72d180281e", "0x0678e012ae6b6d7cb9ea198b9dc54c8c51c632fe", "0x69f7c32cc5b13e545ddb85b8db3e74460071c420", "0xb02a4e2b1e16cfc58fe143cea7a72e018c0b5bd6", "0x0000007e0a82ef9c690646c7f09f2a38baa11ed2", "0xfeac2eae96899709a43e252b6b92971d32f9c0f9", "0xc73abd12f1f0654e51e1f5653fba8566f0c3eeb4", "0xb5838968d26480837740d949420ea15fd4976133", "0x1311275d97ead072384b742718b4110d93e77181", "0xb69100340a5947e856d873463694ae2186146c43", "0xf7d75a5e807b5345eb7aab8cc9a2f9e2175eb88e", "0x6969dd6fd01b42f3b9004ae9482296a67657a1ad", "0x350a1fde11aeaf1759700b5c4c45ce964fc23632", "0xe81d0f58a154cbb358d81af343b6f2d7b1bc44bc", "0x0bc1d189a34a217b2d8a6640cbe4712c773eda06", "0xff45bee3f33ab6f1b446474f86972faec7230dd5", "0x696942331b2b8a5aa7257ac9286e9700e16c1715", "0xde46b6d895d7c821fc886a1890967c6e7f5943cd", "0x4bc19c49435d9a6638962a312f990e3e06d7469b", "0x3e222ab6d7f648c9bb40cbe1c94e7fde920921bd", "0x698af1b6d46fa1b6ce3f2473d4cef37abe1e72ee", "0x69420ddf6c710bcbca7c88c33f2bdcd250f61ed7", "0xb7e8c6d89abc575dd8f0e913be925ae851673b5d", "0x9e073db377266ccda355030e5287e7d0a35662e6", "0x6e2e182c5faab2dec59dc8bf4fbee5fcdbc1bd85", "0xa728f48f9ad6340f478cf2dc24b3d8f315b911bf", "0x444c21aedaf670502136bed85c7ad7d0a018c2a1", "0x120d58409646682e16bfd7028dbdabb632f0c8ad", "0x50b3ab144c9bb8be97a41b88da1c776a075d4538", "0xbcebd4cfe93355f8f004eed3b71f5e2842084570", "0x561732ba7c6359fa4cb7fc73bc6f1834ba6aa4b1", "0x1984e59b6959d960214d9dbecf138c340de8bd4b", "0x4206921bf8b68dd28282206a5c1486c359df46c9", "0x61e9e8b1c45110285c320f5ea36fbf652136dfc2", "0x35f4f6ddf2bf1db3be70dd000c3c48f138b1c60d", "0x5cfbb606c1591c9e726fa4221844e00db1c671ca", "0x5e411a193ae5113cb30b27a3058ef0a6825b29ee", "0xdabf1f85ce444bf0e5c30333ef6412f0f6186564", "0x9f03154d8a93c41996ba8375e3af2c678fe63927", "0x0d3b851326f19c715defb69828deb2762c4a759f", "0x93336255a334fa478a29d3c2047aabc11c60cae0", "0xa4fe23bd8be555736c904e67af192272fc8a953e", "0x1192e281cdc0f0b5f1557c0c4ebb751ca2b9e7ff", "0xbb98a11694452488756a540553960a5999cc620a", "0x2cf5396eb9b0a592684e3c1490bc6219af05f216", "0x6942085e1902ea0bd64a179148f3983154d44cbb", "0x69420dbd873dedb25c86124cf6bfce5c932e46b9", "0xcd605ac26fa22a316148aca8fe0dae7c747dcb41", "0x9b465aab9e8f325418f222c37de474b1bd38ded2", "0x6d5aced2b2fe9e47d0d75b73e1bb63bdcf61a054", "0xfe3b7b0dc6a62436e39436649c5b0fb8a9a8f906", "0x7e71942fe2b24e304abd172e4709139656fa2bbe", "0x69825eb9061da7f82aecba3878f07c162c049d4c", "0xc179e6235a9f9967b7b5aa236b240d4d74e2724a", "0xba53d66ac20637f64b05f29402a899f08f80bb6e", "0x1a7661cff5c31aa8774c53defbd2b632d643a610", "0x951a03ccffb52a7853221a185a6cfd7f505e3e3c", "0x6969550a22a29d1cdc8459fcdf638adbecf409e3", "0x8a8cae90f696b2424ed1467f70b486b50b1cb3cb", "0x91e674cf737e7dc80805d40e853b4a31559e0a47", "0x92c3372ff67adfee195df0d2f014fd9d7e7ea433", "0x956f8918efba9176e39db126cc38b819cffd6851", "0x694209b7612a6219eac9446d5718fe23599a991b", "0x95ad61b69d08169557fffd75bfe4325096212497", "0x9d653c8feaad9062c75d69f8239ef54cb747730d", "0xe334a54bcf21d88932ed88ed2452e0ee0e24f5bf", "0x8888544ae26b17a05be132733a9d79461d6aa500", "0xd2fa2a742156904ee436a354a56f6a50b08dd329", "0x23d17de53aae4a767499a9d8b8c33b5b1c3ebdb0", "0xdc60153930b1434a45b9ecda92e4e9dee4b5bf78", "0x2139b785cf81f2d9eb68341175fd52bbb827a202", "0xf17e4b4dd4150c0b0973d0beb806681d878fbb69", "0x55747be9f9f5beb232ad59fe7af013b81d95fd5e", "0x1da7e97f0b8a2c1ca46abdb0ba7b0046cb5be915", "0xe6f713ef81d3ec5fb1f8cf2966ea5357da61723f", "0xd28c9ba25a4c6b4da851a427800741f866b8f324", "0x79cb59b8096aca6aeba048331c650ef887fec850", "0xf9dd219f3bb9e9dcbde5bdf93e2e6ffb71889d21", "0x99999824fd748110a6ff94e166445635818cc92e", "0x670176431fd8c1579b2d98ec8a2d3ed2a50bd067", "0x7147c11cfc5a26513327c26ff1c4d58ded045d83", "0x0511ffdf861ae1ce16246b6fce226fe66e9c4b1f", "0xc3bd2f8c38593f6ac5c6b7f55ac43ec7b658ca5b", "0xba9e047e7920cac6cd5c992c6eca01f88b173c2c", "0x166769354205bcf47945d989572b6dbeaed791be", "0x0048c0fa6b5c43fbec2290b1f2ab83abadec9ea4", "0xc35bd166e286d021b14118ae8dcbf691b30a8970", "0x4204c341cadde19aeea97d73b75775ec6cd3c60a", "0x6284f7760989e4c29844099ff2c469edb1985977", "0xbad692a9fe0409518a0f719613feb1c42cb8bc36", "0x67e4ad8b3d32744fec0dc00e17b426b612abaedf", "0x7b91268f4ae255c06b2a6011e03f91bc4a426f48", "0xc0f1daf4391e3a6ba3d959297c20d1159d582ec6", "0x57226b3bad476a2247736f8791242a2be02c8024", "0x420698cfdeddea6bc78d59bc17798113ad278f9d", "0x767c24c71ce5e00e413fc30b41aa3074c8b221e4", "0x2c84f0cd5fc2a156021fb3fef04f5e45920a9bc3", "0x51ddb9872bd7ec3f66bf1e1607bef9c81eccea9a", "0x0c04ff41b11065eed8c9eda4d461ba6611591395", "0xd14bcaae5546226012bbf876baa66c90759a6d0d", "0xcd345477007ca5126b8c9265a052d5f4fd43820c", "0x2a33ab3ef0594244e8fed02eb1e95adb45ae5027", "0x09f971dd434a8c052ba58e5c037e1057eff6fa22", "0x42069077a5df8124c5fb3efc4fb229fe3b7a4969", "0xda987c655ebc38c801db64a8608bc1aa56cd9a31", "0x698db71ddc841039688441a82f8c68dd8068b933", "0x0497b60d5fb760260c21f38f533ad3a47677f03d", "0x01d8c80fd334b65707918f0b52ea8a0877187df0", "0xae7e86c227b84930be87fd8b7bed024e3276e26f", "0xf3dc7a576dc0954697712a5b99e299504874abf0", "0x404a2e7c3c2e164da0a2023312cfad8d1ad2d404", "0x31ca3edac60c6b22f62141a274b69fbb9d4587eb", "0x90a66bb8f16a7829807efc3e74900c830f44d4bb", "0x7445fc922e383bb61da62b7d68c6ec5d164af446", "0x4b69926e688195d991da4323a5217851af3caca6", "0x248d6cdf6207af27edd4d03e33f05bde29c232c0", "0x6bec5f1c594af73202cd3e5c1f699d440959954c", "0xb40ddebcd4ad60c9f68b008ebf423cb28d11de62", "0xce176825afc335d9759cb4e323ee8b31891de747", "0x4d8b2febb878346389be70e9c199ef74467b166e", "0x99290b6918c1029ec86fb00b459f6cd9b5d22672", "0x5d2868afae671dd380d67ad296ca49abc01f30fd", "0x7ddd18b77e390bf7c42ba204b8958d37d5bf7716", "0x4f72901b04af8a6a4b3c4a3bd2dad38e1e5c37e6", "0x2cd25b42048ef9e96eb56e6d684fac891537dd8a", "0x87bd97252f98351111ce3f5dd95effcd61c2f055", "0x05745a35cfcb717aa6e6f5d2c6e29598a511332b", "0x506c1432fa1fe4e8d4e4d134953c5ce1cfee5ffe", "0x0fd28e1fc7feea029c77508dc775ac910c54c8ac", "0x772358ef6ed3e18bde1263f7d229601c5fa81875", "0x003099e3456f358b583b0604009f650a4e5442cf", "0x6096b8765eb48cd2193f840a977f3727e7800356", "0xc7e3b4ccb7d9683b85b32ebc636310232b401bb9", "0x4abd5745f326932b1b673bfa592a20d7bb6bc455", "0x472024566281fff51d2e6356f36a1e6239359845", "0x8767bd64c196e39284d3a12d88b0048127356e8b", "0xca530408c3e552b020a2300debc7bd18820fb42f", "0x420b879b0d18cc182e7e82ad16a13877c3a88420", "0xa74fd910a6dcc844ba42839108a7a72aa2ee2c24", "0x698e6976e197a126a507bb79dcdb6ed37f072933", "0x9959d38d7dd5a5b92def3468fba4a80038ade4e9", "0x82a0dce6956fc709cd602968378110e7ac36b420", "0xcf384e0715c6eca371e2fdc368d7cff2240f3cfe", "0xaed18826655bf9167a377c8647132a937d6a4f36", "0x90e0cbe916700ba537332d327c3020b8cf019c3b", "0xa2400e2f0f4f70368fc00f339bd9bfc937e7d271", "0xb3ce7e10de75d88270872af83d57049642df360e", "0x1f5eaac2f7ebca14f86882b7724bcce3fdfea2bd", "0x0fc7bcb6f392bdc8fc90103685359d3f6c8ffcee", "0x68ad75469db9181a1144e769c16adf47f2f32cae", "0x734be1702fda8a6cd3e68c6e6171e995d7531d65", "0xecf77b0c4c9808fd49774f4ca250ba35ff37832d", "0x23593f7fb29cdc71dfb03b467853356d8f22c809", "0xd4f4d0a10bcae123bb6655e8fe93a30d01eebd04", "0x589be19ddedf2d6bbcf31a6b31c3d0b0cb944585", "0xb262c3e4105618b521ee5228d4dbbbd2c4ded6f9", "0xa75ad62a1b6b78eea6c54c22eb697ca0b125f97a", "0xff2693027887a125ded7d89d49c2181883d60f50", "0x8e3f2543f946a955076c137700ead4c9439e7fca", "0xc9e503562d0db0a2629288a5d3b1c94ea7741869", "0x69696937a66defa05529751543ba2ea1393452c2", "0x0c76a22f75622c51397e13b20f60013e0ba005ce", "0x30f68f340b803112357d6807c917f7c7c95e3489", "0x79cfbba249657612d27c58f3b1811ac5d3ff6f89", "0x64489c3389d706b24ac1629deaa54551879de733", "0xa83149a75591839c81cceb60fd5a8da938176dc9", "0x073a3b3e890a3aaeb061e50e92fffdd31197870b", "0x69420ead81514b283d49594a35fcc7dfe0036fa1", "0x27a6fbefd1294818895f506456b9ea5760f6788a", "0x89c4b5a687d5202311455978ad7a4b902bf5dc2f", "0xb1c9d42fa4ba691efe21656a7e6953d999b990c4", "0xdb0238975ce84f89212ffa56c64c0f2b47f8f153", "0xd3f6efc975a47c38668ef98c39e5a0273afb5f18", "0xb98b7ebae3fd5796b60911c857609c1589cc1362", "0x59ff2f2673f86b78a8179048b117aebfeca46349", "0x698258d7cd0ced4c811ee79aeb5f9d5d92c30c40", "0x5b80d10959e58db8922bd6643f5a51023d665d68", "0xedb40d2632b432173b48ee4d828afe10790a25e1", "0x3a13cd38542d0f81db09f57b98c0446346271396", "0xc11158c5da9db1d553ed28f0c2ba1cbedd42cfcb", "0xfa06a1f7eb5efa0751a51c6c4897f7a25b886e6b", "0x9796a74c0e4d84ccb9a237a4c493bf13b98ec09d", "0x051d3c485a4acce2664a21022140069008bcdd8f", "0x3fbabe48e137c9ef4f8cb517a4762add7e6242ae", "0x4b3c534f661c0097b5903ff2754d5531da9e0662", "0xea318541a292ab46e72722b02b3b060403cbbae0", "0xababe51c38843f71e7b4b3139a67c39c909eb47f", "0x47e3014d94401410e308505cb3e8970dea8074a6", "0x69420034e8ea5797087a629e88c3ac1af3edc54e", "0x989436e4194af162546f595afc6336a15b3dca7d", "0xca953ccf0f493b7ce942e68a2b6b87a0c72206c2", "0x342ea8978d64a18af3cce0055072d6178b7b2ad4", "0x69202499d68baa216bf41580c674830257f2ebb1", "0x55932abec6d9bc1b4ea3cf09162d75f9d9ea0b81", "0xa5c931f43a283175b7b3f38755e7d0d9675690dd", "0x181385dafe0d56e297b22c19db27ecb09d836c54", "0x6dce26b25cc024e343039f649694140b76974a1d", "0xd8e75ad90928f81061c4c6f532e75c581023422d", "0x81dd5f165f63b7d05e42c4238b31998df01a4de7", "0xebaf191a4f7ba3ebe7e8fbc3f53a1c037c2a0013", "0x661bdcee4749313e7b4386ed19b6675a1b392f43", "0x69695c8427c01038cb11091f3820cb457668acba", "0x600d6777805f45e960c84726f721757a0bfd3125", "0x6ca6e692f8c66d0456c62220cc7c438c8015185b", "0xe7d33b47c3d0736d7c9de5aac7d68c5d4cd9e2b0", "0x179c5bfb1f9ddd3a6895f4f15b9e52616f13673d", "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e", "0x37d47be72cc16a324533ac1b819fcc0f089cc672", "0x92c35d7822f59564ecdc4a8e257fd18899cbd1a4", "0xa2c0a9e86c61a1dc0832d7997144e89c519aa80e", "0x6942016b8de9d18a5831eeda915e48b27cc8e23d", "0xd05a02e4ecbd6c766a6f9dc96cedbdc1f1b591be", "0x184ce9873b813dcc97b54a21ee2117f1a42fb144", "0xfaff66772de50019b326721dec2ea487e5700f25", "0x867694c2a1fb412a77a5569b429a613d71eee3b5", "0x2093000740d4bac57041bafe9dd83d663482b594", "0xb47bd6bf7913ce10fcca1cdc0cf9ca400d3dc5a6", "0x3fa3cd8c0831be7cc16995c156207a1f702e3ee0", "0x07e6d1a3839c5d037d66dabe4ffc59a1dab77631", "0x888ca98a609e3e9b067259ffb6a9955de26c0068", "0x9c979daa0e8823c6b3d2414d49bccb2866ef710f", "0x44dcf99ab04772146aa366c046425b99e0a0e45f", "0x6983a54320c11b4b0511d9110ee6dfc490e3b0d2", "0xd37120378ab1803832e5a6a639dc9df46229dae0", "0x020b477193839289726d5042ea62b91121f0d5c0", "0xb9d09bc374577dac1ab853de412a903408204ea8", "0x9afec6e20faa8b6642054db06adb5170ab566c6e", "0x5ff70c0ea3bd71b4149e1f047c5fbaf11edc8758", "0x37dd1cc64e0b995f21a682ec9e2c13ccba386aef", "0x3ef128242290460e4fc1afe33837978e32acce90", "0xaa0181e6565346ad64094198ec854405ff54b1cd", "0x076524810db35d719dc4cf134f0de2f4ddfce9d9", "0x2022cad0523b5928bced1f587de03112d5d44324", "0xbea5bc325580d8e649bd8991b62b5caea39d3394", "0x562b9166776938f8a842291d8b886d821ec9c3c2", "0x667bd45d6911ea696e33b1eea5022a8fbba1fe79", "0xa40b07a5f80c59a55d3ed83addd8246014e07cd4", "0xe6705367880b4d5d1aeae948fd620f55ef7413e4", "0xab8d89cb7c1a0da67500e9572828cadd034471cb", "0x3df34aba7fccd784ceb5febf1b8ebddf5992ea90", "0xe6ebb45125c627b132360f81f7ecf574db22945b", "0xa3a8b72062be54979eae334e7c02121254f5d1c7", "0x69420a99859dc334e71be7484cf026215afa228e", "0xa445b79c38c85e074b5f9488593a846c1a552e42", "0x7fee1458c9944fc30f22d539757561c803a3e4ff", "0x3f27505a60978428568c56dc48c30ed9b7b148a1", "0xa1ba34b1906c6706cee904b6d8bb08a6412d94a5", "0x87da227f4bba18beae5b2e67e7e636b8cf8d9f4a", "0x6942002626c17e348e6b61899f45b62441cbd33b", "0xa4f59d5c14567f346047dc944d57c2d12e76bd23", "0x694206517126c1b06e119043adb382ef28a95193", "0x929af435b2fdcaef9eb1c800581dbe6476e94309", "0x5977c50f7e605801fe5009c03c655da1f525b762", "0x7c75b20c0a040e327753394cb00ef207878c4757", "0xba73c4bd568ed00fdba7d9468a54a60c458e40cf", "0xefc3f1ecff8b9e9389323ef610bb9149236e62fd", "0x6f467f88effa90a6ada73a6d2eb931d73a3a4a51", "0x70bb8bcfafcf60446d5071d4427da8e99f0168aa", "0x5bf3e67c44e70568bf37f31d04e667b1eeac943b", "0x6942093d46081b2dd7097aa7879165ca37c6cb92", "0xea1869c3975ab3dacfa124439d017e721186f638", "0x610c04f69f8b3fde16c19c77a7dfed24b9a63853", "0xcef3124cdc31813e4498198d3f503790fa3590bb", "0x2df17b8394d8cb4b8de6a534e27a217f15a91cf4", "0xdc8dfd979293302568b82ec0036e815c7ee3953e", "0x2ac0cb20e0d4e39e4843317be7ce6546614fb85a", "0x3d491f985b980818f27d980d629b8335bb06544a", "0x05dc115b1076e3c656b5d80eeb55de7b4e9ac0a9", "0x90846ee5ca3c06682c03e573601bd072dcdb08c6", "0x6cfa352d24f647cf81ae53fd64d3e7fcb9db01e3", "0xc19fe1a70f71f32784bbd254eac7aec5e4765db8", "0xf754d479a8f1a7f3c455dea64524bd5f75c60156", "0x88a4e88d3e5ad70b5c78f9009eddb5816f3cd621", "0x81085e62f97527ead18ab640be4c6cd67a1a618a", "0x69d4e88aeda2ee5692fe95140e2b922c18eb8bad", "0x0992290e85c4fe98cf27f3b8099b610efb568c84", "0x3682decb1c978f848a4a24153219d1995224ef7c", "0x1ce76c572f84254fc6601f312dddbcc4556a5de5", "0xa2f2fcf57249db7c72191768090b9f57e472aef8", "0x553cf8bedceaa04648ecc19271ab3199e334d7d7", "0x696969e69c81337d18133d245a6b8dbee7533694", "0x67420813ca0887850d9e1d7c3d04f410252c37bd", "0x513c569ef61d0af73ef6dda0d0ecaff8d1bd9f0e", "0xb3cf186d747aba2dc856595b5fdc0bc0757a659c", "0x6e7fe381bab374851097ded410b4c3bc17aaa015", "0xcab1121a6c189852be5ccc73899ac79ba9d4a21d", "0x4e0a23e7230529f8ed372168d17409cb9af8ea70", "0x7bee078e2366eda19f80b3724cb219820195b07d", "0x46f99ebd0496afffa72c2f36218753f8663640b4", "0x1aba49b1724553a56984244b363a8cb48f9e0976", "0x7009f477376cb704309622045f982925989d7c04", "0x967770ed11dc3abb97776ad6fcd7937dbd0fceda", "0xbf20963c84241d7c2bf4daf34c891a683012d941", "0x0666b157e15930b80199b24d98859e99c896696a", "0x69825058d714dcab2b93e3a0c43d4f43aa5f6643", "0x44e1a28d14fb6db2e5ac64931867c64da130a629", "0xf95eeff328e46616111021dc77c5925d3abd01b9", "0x5b34b5032267e5d5a80b99a06b4b85716f404ea2", "0x2588cdb42c7cb543c48354b193d07a0fb40bb12d", "0xa46faf2a322beda28949e81bc910419d57ea702d", "0x6248dc5cce77934615c695a75b0e7d31957a5677", "0x696969205dd1517cf6ec0e5fc2ab43dd46175461", "0xf843e69c7bc9e132d4cb7002dadb357c6ce9a3fb", "0x3ba16930a8ef3b53db4eeeb2f627e4ed0f894a98"]
    
    """
    токены, которые выросли, но не ясно по block 0
    """
    # 0x30303101104100C397C069E0642ACaC518420205
    # 0x683A4ac99E65200921f556A19dADf4b0214B5938

    # 0x1859EF5c77F2A27B8440d2d0083fE89d0F04a7FA - токен анализирую how_much_was_spent 24 jul
    # 0x70d70a716cDF5f5335E62d298A848F5a8228F473 - транзакция не посчитала объем торгуемый со многими Swap 0x75a798da465306c0fd290eee6de70340396819068e354124bcfe4f3aa5034570
    # 0x6b2e5e59d214dE99Bc7BC8040Ec5573479984F8e - block 0 banana покупки с bribe

    """
    токены с bribe по banana
    """
    # 0x6b2e5e59d214dE99Bc7BC8040Ec5573479984F8e
    # 0x52e7af0e1dae83ebedc45aac5ea9c72befde4149
    # 0xa3616b4e8b1c9eb5ebe1a402e78d623ffa852b6e
    # 0xcebad07d7e4fad0284db34d87c021c7aba9f92be
    # 0x8b2130ecbf32818978ed1a006080c2fc5dd5b4a2
    # 0x8bAF5d75CaE25c7dF6d1E0d26C52d19Ee848301a

    """
    токены, которые спустя время начали вновь мощно расти
    """
    # 0x07040971246a73ebDa9Cf29ea1306bB47C7C4e76 USPEPE 292k
    # 0x155788DD4b3ccD955A5b2D461C7D6504F83f71fa HARRIS 324k
    # 0xd43fbA1f38d9B306AEEF9d78aD177d51Ef802B46 GONDOLA 292k

    """
    токены с locked liq < 1$
    """
    # 0x6D32399d43B97018d40d152ECF579872fd7d68C3

    """
    токены, которые мощно выросли
    """
    # 0x8802269D1283cdB2a5a329649E5cB4CdcEE91ab6 FIGHT
    # 0x790814Cd782983FaB4d7B92CF155187a865d9F18 MATT - интересное наблюдение: много Swap() идут сразу после Mint() по tx_index, там 10 и 11
    # 0x42069cc15F5BEfb510430d22Ff1c9A1b3ae22CfE
    # 0xfefe157c9d0aE025213092ff9a5cB56ab492BaB8 FEFE
    # 0xB8A914A00664e9361eaE187468EfF94905dfbC15 DRIP
    # 0x683A4ac99E65200921f556A19dADf4b0214B5938 MAPE
    # 0x636bd98fc13908e475f56d8a38a6e03616ec5563 WAT
    # 0xD875a85C5Fe61a84fe6bdB16644B52118fDb8Dfd TRUMPONOMICS
    # 0xce0CA7A150a441Cc52415526F2EaC97Aa4140D13 BIDENOMICS 13k
    # 0x07040971246a73ebDa9Cf29ea1306bB47C7C4e76 USPEPE 292k
    # 0x4aBD5745F326932B1b673bfa592a20d7BB6bc455 Froglic 500k
    # 0x777BE1c6075c20184C4fd76344b7b0B7c858fe6B BAR 834k
    # 0xcee99db49fe7B6e2d3394D8df2989B564bB613DB BSR 145k
    # 0x5B1543C4EA138eFAE5b0836265dfF75e1Ab6227D ASPIE 135k, здесь erc_20=1 много на block0 и 1-ый трейд спустя много блоков после Mint(), и потом странно идут покупки, как будто маркетмейкеры закупали

    """
    скам токены, но у которых liq burned
    """
    # 0x3582CddCda46A5be75117871deDFeD57C0E8D0d4
    # 0x799b912d2ff72ec4E9e22260b72a44ADe1b8E75F
    # 0x98414d1fb5fD053694387d2eb5947AFF2FeF3EdD
    # 0x222343B74770D9FedF8d3F7Ffd3Ec1b88b78a74A
    # 0xF07D2131d125b397203664716BDB3C7e1C30C15F
    # 0x7B88b016632E2C6950D98537C0C899C86B2dB141 - tokensniffer писал liq locked и тоже самое было написано на dexscreener'e, но через время красная свеча и вытащили в < 1$ всю ликвидность
    # 0x639bdD3aCc7218567A1Ed382dd4344411E7877b5
    
    """
    токены не скам и рост после 0 блока без продаж на первых блоках
    """
    # 0x04C0Dc93Ce5698fA8a380CD8aFF38710E04D4737, predict on block=0 + много bribe sent
    # 0x0bf043B7705a65468Ee74D0F5ce4919EDa3317a0

    """
    токены не скам и падение после 0-1 блока, но потом рост
    """
    # 0xF6D8D3184A10E7013B6236E5054d45E2530954a7, predict on block=0

    """
    токены не скам и падение после 0-1 блока и без роста потом
    """
    # 0x6e9752CC93306930c90142DF623103d3C1D32903
    # 0x6911D0C4De3905af2527193696A639950df8D420
    # 0xAaaAAAb7BCabd9c3997B5Ba86c0937E037B0A6a3
    # 0xE344EaD74a37aBc8df75C8392caC25a55F44F19d

    """
    токены, где были Transfers() перед первым Mint()
    """
    # 0x5ca83216Fae72717332469E6A2eb28c4BF9AF9ec

    """
    стратегии
    """
    # если на dexscreener иконки накручивают, то работает команда. пример 0x7777Cec341E7434126864195adEf9B05DCC3489C
    # искать кошельки по типу find_cptd.py, после которых постоянно на следующем блоке цена растет
    # отобрать кошельки, которые снайпят на 0 блоке и не скам токены, туда заходить на 1 блоке
    # идти в лонг от уровней поддержки; тут в начале запуска токена четко видно 0x921D07059cb0640997251b577735a1bef90c408F
    # анализ крупного капитала https://www.youtube.com/channel/UCKpxNYGWhqWdiiNulVbVyfQ
    # если на предистории после больших красных свечей сразу откуп идет, то заходить после красных свечей; пример 0x6db6fdb5182053eecec778afec95e0814172a474
    # если часто докупают одни и те же, то будет рост; пример 0x6dB6FDb5182053EECeC778aFec95E0814172A474; пример кошельков - все из top traders (0x69B02FAAb1585216000d278Fdb8dcB9B379cc318)
    # все те кошельки, которые получили хорошую прибыль с токена, найти куда они потом переводят эти деньги; как говорит Дмитрий Иванов на 23 минуте https://www.youtube.com/watch?v=41nQokPRevU
    #  эти 100-ни кошельков потом переводят прибыль на 1-2 кошелька
    # bubble
    # если кто-то уже в профите по данному токену и снова докупает токен, то повторять за ним
    # если у токена owner уже запускал не скам, то заходить; пример owner=0x0126da5294537e1c04713f3b6309a081be5821e5, токены: 0x810387d0cee0f693c3df8c25e0282d07a9a7c0a4 и 0x714c1ce091141a83a69c6cf526036809a88232f1
    # на dexscreener есть оплата за молнию рейтинг; смотреть кошельки кто делал оплату на эфире и анализ этих кошельков
    # для снайпинга: чтобы определить контракт может ли менять таксы, можно чексумы контракта смотреть уже известных. "я беру список функций и считаю по ним хэш"

    # (!) добавить
    # erc20 стату о том, сколько токенов заскамились, которые кошелек покупает, сколько у них было trades к концу last_block; сколько без renounce_ownership
    # erc20 стата сколько было куплено и НЕ продано токенов, то есть держатся на балансе (полностью и частично)
    # erc20 сколько держатся на балансе и НЕ были проданы и при этом заскамлены
    # смотреть bribe
    # сколько status=0 транзакций у кошелька
    # какой-то показатель сделать какой % объема erc20 токена кошелек у себя хранит после первой покупки и как долго
    # erc20 стату добавить сколько всего по токену было трейдов, так будет ясно он популярен ли; сколько было трейдов за первый час и тд
    # median duration of erc20 токена с момента первого до последнего трейда
    # находить кошельки, после которых на след блоке всегда покупки идут; значит за ним копи трейдят
    # показатель какой % объема продали уже токен снайпера
    # в bubble maps смотреть кластеры для кошелька для проверки скам ли
    # показатель топ 10 холдеров каким процентом владеют от объема, так же каким процентом владеют по группам разбитые пользователи
    # пример сжигания ликвидности для FARM токена: https://etherscan.io/tx/0x6e9f7ff6374d8b5392aac69d7cae5f7ab36b6801a8c315b3704638cf50ad0e23
    #  потом они renounced contract: https://etherscan.io/tx/0x8f020ce3f2eb9f7dc9a51c30adcd0f2be61773f699ad0c8a671b989c34455a5b
    # добавить показатель есть ли у токена веб-сайт, телегам и твиттер; далее можно анализ аккаунтов твиттера добавить
    #  для bMint стратегии отлично было бы отправить эти данные скрипту ml
    # показатель сколько кошельков повторно делают покупку
    # wallet avg_transaction_index

    # (!) добавить на candle
    # % сколько объема продали снайпера на 0 и 1 блоке
    # смотреть какие кощельки покупают в течении первых часов. например, покупают ли недавно созданные кошельки
    # повторные покупки отпечать на plot
    # выводить median % кошельков, которые купили этот токен и какой % их erc20 не скам и high profit

    # (!) ~ кол-во холдеров у монеты не растет, но кол-во токенов у каждого кошелька, кто холдит, растет; значит идет накопление
    # и тут сделать показатель на сколько изменилось кол-во токенов у кошелька с моментов
    # - размер ETH, который получил кошелек на первой самой транзакции founded
    # - время между founded транзакцией и следующей дальше
    # сделать параметр сколько эмиссии у первых десяти или сколько-то кошельков в течение каждого интервала времени токена
    # то есть правильно описать что идет накопление холдеров и у них токенов; и на какая скорость их изменения в течение времени
    # - какой % supply держит кошелек
    # средняя покупка и продажа токена (avg invested / withdrawal)
    # метрика на какую среднюю сумму покупают нулевые fresh кошельки
    # добавить метрику в кошелек сколько дней назад был предыдущий трейд (не считая текущий сейчас)
    # добавить метрику в какие токены обычно заходит кошелек; например, какой средний market cap токена в момент захода кошелька; на каком блоке в среднем заходит с момента Mint()
    # метрика у кошелька SBRatio: соотношение продаж и покупок, оценивает торговое поведение; более высокие значения могут указывать на фиксацию прибыли или спекуляции.
    # numberOfActiveDays - в дюне. думаю это сколько дней по каждому засветился кошелек
    # метрику сделать соотношение сколько повторных покупок делает адрес и как часто
    # "История взаимодействий с контрактами: какие смарт-контракты вызывались адресом." - или сколько уникальных роутеров вызывалось адресом
    # у CHAT GTP спросить 
    # win_rate_scam - сколько не в скам заходит
    # volume_buy/volume_sell - по токену отношение всех покупок и продаж по $
    # так же trades_buy/trades_sell
    # добавить стату по еще ботам помимо бананы/маэстро
    # сколько токенов общих пересекается у группы кошельков



    LABEL_TOKENS = None
    #LABEL_TOKENS = '[NS]'
    #list_addresses = ['0x52832A7116193786b8c02a97982e6Bd5403ac395','0x59a47B40923C4c2d179e79812215c6a5f0d5BCAa','0x048d07Bd350ba516b84587E147284881B593Eb86','0x5b342F03D126314d925Fa57A45654f92905e6451','0x1FdB29Ad49330B07aE5a87483F598AA6b292039E','0x1001271083C249bd771E1bb76C22D935809a61Ee','0xD09Eb9099faC55eDCbF4965e0A866779ca365a0C','0x155788DD4b3ccD955A5b2D461C7D6504F83f71fa','0x8B68F1D0246320d5CaF8CD9828faaB28D66BA749']
    
    #list_addresses = ['0x0dA2082905583cEdFfFd4847879d0F1Cf3d25C36']


    #list_addresses = list_addresses[-50:]

    # print('addresses_in_folder:')
    # print(addresses_in_folder)

    # tokens for time strategy
    # list_addresses = ['0x3751d747bB6EbDC038f2a609c93C0e4eEAca2e13','0x68AD75469DB9181a1144E769c16Adf47f2F32Cae','0x748509433eF209C4d11adA51347D5724A5da0CA5','0xeD89fc0F41d8Be2C98B13B7e3cd3E876D73f1d30','0xE10adbD5d600b54F516E3197C45a7AcA2CE5A41E','0x937F44dF573fC9779846C255B3f58Eee7266D25A','0x3dc7d520fB61835c12150456eE38c9eF305b94cd','0x8Abf4ED04299306eF2FEa9820101612B3A6b9892','0x08e96f308Eb008B3Db68640aba6B06078625f8cd','0x4754C5c773e8e59c9325B01d110306031f666136']

    # tokens for time strategy v2
    #list_addresses = ['0x56677BBBEeCCc743E08BD7f9Ae58911ecb1c6958', '0x3F9929ffF0911c298B9D607b1623b721eE267a88', '0xAf05Ce8A2Cef336006E933c02fc89887F5B3C726', '0x3806FFd354012BcB752c3dEC1889cafcdf89d24E', '0x553cf8BEDCEAA04648ecC19271Ab3199E334D7d7', '0x3aeaC44894d61E6b7Cbd48dA008490e18c539fd8', '0xe3E5436978450047BF197b5f7b48C98eAe3571dF', '0x465dbC39F46f9D43C581a5d90A43e4a0F2A6fF2d', '0x20485641350a3Ca182D84199B7b3f679F03703Bf']

    #list_addresses = ['0x570c184c9b03b2de238bed486015bd2bcdbb3da5', '0xe001d7e6bf3138cc9564c615b03d120c402fb251', '0xb48c6946fe97a79ae39e8fa5e297a8b9651e00f8', '0xe64ad697799252a7ee61143163c59dfd327445c5', '0x34df29dd880e9fe2cec0f85f7658b75606fb2870', '0xf5e63b4c9db61c35bb66462745f9a5e64604f0a9', '0xf5e63b4c9db61c35bb66462745f9a5e64604f0a9', '0xa120199529228d39cffb62de9dfaddbd4ba9e337', '0xe64ad697799252a7ee61143163c59dfd327445c5','0xf280B16EF293D8e534e370794ef26bF312694126','0xf41A7B7C79840775F70A085C1fC5A762bBc6B180','0x53229609F907be495704fCa99Ad0835C5f3abd3A','0xE81C4A73bfDdb1dDADF7d64734061bE58d4c0b4C','0xFB14434caB5EC006a562EF9Ed7ad8610866Da65A','0xa6180029845469E89C507fE3EAfeDFa242687822','0xd0630a2d243503591c84277342BaFf84854b14F8','0x6862D2F41454c9e10B4DD108A87C703338408b09','0x426742d23e7B29019AA68638CDBF385C9Aa09F0f','0x106Db9A4D0E205ea3f6629573366Bc80C1796Ed7','0x1C74F797c39C454E662800eF09e38637d7D3b1d5','0x6ABc8Bf930d8D37CC816c9630d296580D7fd9707','0x41B1f9dcd5923c9542b6957B9B72169595ACBc5C','0x5a9e54ef2153051e9CB8e40057c6E537c9Ce46Df','0x8AF8cCe77D705B4ca1B0a627D705A33ff475AF5e','0x4466Ce2b0a25b68C241D881b9Be10c700BD21BFe','0xB43484516E71Ab324DA6E3A90cA3F499665467C3','0xb0ba1b6EbADeBa1A63A94445F0DFB249082B5Dc1','0x44E18207b6E98f4A786957954E462ed46B8C95bE','0x023b7FFd5602D839869F4d968ECc7B1d8238a7fF','0xBb376e82B796ec9D5bA68B10B33f79Cbe612dC07','0xDF14ab1bb0061FB4a3309E8A5BDBA3aA640899dC','0x870e184b7fb15a902Dc9A93bEB03C15a65977918','0xcc524200264499d2434E071798a37e846CDd8D01','0x696969205DD1517CF6ec0E5FC2Ab43dd46175461','0xf93434B39F9F50516BAA5B2FB81430a9bdd56EC3','0x8C5859e10829e54B1f9233f09eF8aB271Af97ba8','0xB3912b20b3aBc78C15e85E13EC0bF334fbB924f7','0xC1bF21674a3d782eE552D835863D065b7A89d619','0x937F44dF573fC9779846C255B3f58Eee7266D25A','0xE10adbD5d600b54F516E3197C45a7AcA2CE5A41E','0xA4DdDe6405E6AE8174f0580dABbE6E688Babc987','0x32f411f32Dd14f142C087062335032F89Bdecb0e','0x069F01CDD1E32D7BaB5fc81527df191835136C9D','0xB7cf1Ee2B2A26d22516E9053Cb22D1c7dA37154C','0x4754C5c773e8e59c9325B01d110306031f666136','0x9999999840065a79d3a20575450eAa57b6a515FC','0x13d2f1501E3033cE0c28Df5D7C4D50fcCf11AcA1','0x8A26C07be7fCc7863FD2181cbe354f8d910038CB','0x373cB10042F1f9A6a80614a9394C3D296615979E','0x13a9c74ee3504b58A8138997AC5778CC007c86e8','0x7A55d2222d9138018FfDAd69FFc3eBdB1c22dbAb','0x1524f86D5A1D82ABfC94843700ae12F1d30632F2','0x5A33FeEf95a9CF7BfA9095d6cb4EB11A6C917874','0xc3D2B3e23855001508e460A6dbE9f9e3116201aF','0x786f112c9A6Bc840cdc07CFD840105EfD6EF2d4B','0xb21C1D75E0dea3CA9519139e0eed94b00DFBB495','0x224f261a4eC1c7562CCA756985f05514c4364E3F','0x6968676661Ac9851C38907bDfcC22d5Dd77b564d','0x4216663dDc7BD10eAf44609DF4Dd0f91cD2bE7F2','0x69d21A19Db9f623402A30036bf42CBac8cA32eeB','0xB11176581535022844A8797C0111D808CC7917F3','0x65AB6A3B8a1ED422ed94eabBb9da61bd89B21273','0xD2eBc42175a210d8fD976B2875d10e785f15112e','0x20485641350a3Ca182D84199B7b3f679F03703Bf','0x281D0BBC59f96CbfaB4EbE590c05a4D06b494aD6','0xF5Cda34de70dCbF4bbCcA4fEBfAFc46405dC4Df7','0x7A2Db92624Fc80116Cc209c9662eE2baa8AdBeb1','0xdDfB931Be05ac6fA2853d3e066c210273A8de6C2','0xe9682d02248B68FB4A231cA2592518f7C4E36691','0xaD04C254b62cB90cF0669f9e6753fD4452e1C7f0','0xF9F3c901ccD97e0A9456849333094e7Bf8543F58','0x31d3779b3b3171b0bF78BF6D0C0AF65A7C995D22','0x86B69F38BEA3e02f68fF88534bc61EC60E772b19','0xa247C6d23c8C7d223420700d16d189cFF9357F38','0xA11B499369522864C0d5bFDAF6C84BaFe5CDa608','0x2224F1Ef81A6FFe11C4F2c754F16E46076d66eCB','0xAFeCCc61A15dfDC7DC6f14034A56623D76Bfb914','0x51D4b72E6f7B4f5bE826E8d215be32CE49273cf2','0x984f9f1e215975cb89b5C7AecA5443b766F22fE5','0x51cCBCd853895F0cec1737C4b155d801f030F806','0xda0c0D0a0FF8262F3EE9Ee8A712B988Df897bE65','0xDC90B2079f139AB14d98389DcFD044e360838EAB','0x0281baeb40ff6772BdA48b3aEB0eE82baE27a703','0x1d4FB9BfA1967BE6cA74819e28b98C2aA5AE8B59','0x810387D0CEE0f693c3df8C25E0282d07A9a7C0a4','0xF55917eba7eDb6a88EE57b6e899391e317cbab45','0x27eFFE278D7f3a6634d5294A8a766e04285d3da4','0x39636B82C7d2E703baa093ef195ee5af81535954','0xba2Ae4e0a9c6ECaF172015Aa2cdd70a21F5A290B','0xC117e7CA230f47613818921f497951Dd95d1968d','0xe3967fBa8C5270e3c5224CB788b7820653eE514A','0x7f911119435D8dED9F018194B4b6661331379a3d','0x5408d3883EC28C2dE205064AE9690142B035fEd2','0x8e42FE26FC1697f57076C9f2A8d1FF69Cf7F6Fda','0x88a7133Acf9985b4b59cF9e46865f415bf697b9d','0x9800CBf209bEE9377B496A3d67aBf9B524B7448d','0x08e96f308Eb008B3Db68640aba6B06078625f8cd','0x8e94CBfC24d32B14d23C87071F156e56781b8Ee8','0x5EBD0CDbd5BFA3BcBc101Cc8Fb39c3B6f3594cA3','0xAD4177d1ADeC9CB006186760173d5118B213cA08','0xAb5D6508E4726141D29c6074Ab366aFa03f4Ec8D']
    #+list_addresses = ["0x572DaCA03e207604A23aACC453e727D55e433BAa","0xC9bd9b4F5e96644B9C5C525E99f655Ff31ce5Ce6","0xD5930C307d7395Ff807F2921F12C5EB82131a789","0xF433089366899D83a9f26A773D59ec7eCF30355e","0x1131D427ECd794714ed00733aC0f851E904c8398","0xD533a949740bb3306d119CC777fa900bA034cd52","0xeD89fc0F41d8Be2C98B13B7e3cd3E876D73f1d30","0xeA4170A365952c666A9f34950771E51841732de9","0xfB5c6815cA3AC72Ce9F5006869AE67f18bF77006","0xd43fbA1f38d9B306AEEF9d78aD177d51Ef802B46","0xe3944AB788A60ca266f1eEc3C26925b95f6370aD"]
    
    
    #list_addresses = ['0xa00453052a36d43a99ac1ca145dfe4a952ca33b8']

    #list_addresses = ['0xDF14ab1bb0061FB4a3309E8A5BDBA3aA640899dC']

    # (!) amount_was_sold_perc=-72.73% < 0 для wallet=0x632A1130EF66c8deb5C715E420Bfdb52dd6942bC, address=0xAb5D6508E4726141D29c6074Ab366aFa03f4Ec8D
    # (!) amount_was_sold_perc=-201.79% < 0 для wallet=0xA75652620CBE31e05272958110d64f7440b4F498, address=0xAb5D6508E4726141D29c6074Ab366aFa03f4Ec8D
    # (!) amount_was_sold_perc=-10.35% < 0 для wallet=0x4d9fEF09552f252bCF6B637Dd189eC4b6e2ed0f1, address=0xAb5D6508E4726141D29c6074Ab366aFa03f4Ec8D
    # (!) amount_was_sold_perc=-130.60% < 0 для wallet=0x5F4AE4D51e2a1bA85299356FA17043846F4C80Cb, address=0x15B543e986b8c34074DFc9901136d9355a537e7E
    # (!) amount_was_sold_perc=-31.58% < 0 для wallet=0xa278eEfB6a2A3A005cc2d84373ffAc2c7eb2fcAe, address=0xa0Cc4428FbB652C396F28DcE8868B8743742A71c
    # (!) amount_was_sold_perc=-93.79% < 0 для wallet=0xd09d3dc39e28e4ab8587FCc93F4608943Fc41f39, address=0xac506C7DC601500E997caD42Ea446624ED40c743
    # (!) amount_was_sold_perc=-6.37% < 0 для wallet=0x6d8a0a9f628135E7Cdc94d480cc419eD38651990, address=0x4bceA5E4d0F6eD53cf45e7a28FebB2d3621D7438
    # (!) amount_was_sold_perc=-221.39% < 0 для wallet=0x4D43e75B8b434A2b16E266d616599c8a8A28B325, address=0x76e222b07C53D28b89b0bAc18602810Fc22B49A8


    # на ошибки сейчас проверить:
    # 0xe7b077e5c9d0745ca015503daf5c6932e2e7acbe
    # 0x5c64c984b05638a364de7d82f4db3099c2e70004
    # 0x5b408a33179d7ba861309a512091a5fbd78398ed
    # 0x8721cdb839182ec7fb12dcda348e699c1c2084e3
    # 0xe25880deab1a7f1ae4546261d285db9f2583daff
    # 0x39767c0b270baf7c527e7e9352295fc94b74470c
    # 0x1bbe973bef3a977fc51cbed703e8ffdefe001fed
    # почему на token.balances[list(token.balances.keys())[10]] invested = None и отриц знач проверить

    # (!) ошибку проверить
    # почему-то для address=0xe7b077e5c9d0745ca015503daf5c6932e2e7acbe на транзакции 0x191423c4a9e23af5b13b3a08751b01267c1fae1e78aabb5ef022e03631b1377a
    #     value_usd_per_token_unit=None, хотя Mint был ранее
    # 0x6dB6FDb5182053EECeC778aFec95E0814172A474 FARM - кошелек 0xffffAb07392DbD555c8d46429Fe14018Ec71A5A3 получил 15% от всего total supply, хотя это не так (транзакция 0xa1daa1f91d21c2bf9b6a34ec5818bf6b6848d5969a40045ab761c82c7a4bc686)

    #list_addresses = ['0x39767c0b270baf7c527e7e9352295fc94b74470c']

    # 0x155788DD4b3ccD955A5b2D461C7D6504F83f71fa - здесь баг с MATERIALIZED
    #list_addresses = ["0xce0CA7A150a441Cc52415526F2EaC97Aa4140D13",'0x777BE1c6075c20184C4fd76344b7b0B7c858fe6B']

    # токены, которые мощно выросли
    #list_addresses = ['0xb8D6196D71cdd7D90A053a7769a077772aAac464','0x8802269D1283cdB2a5a329649E5cB4CdcEE91ab6','0x790814Cd782983FaB4d7B92CF155187a865d9F18','0x42069cc15F5BEfb510430d22Ff1c9A1b3ae22CfE','0xfefe157c9d0aE025213092ff9a5cB56ab492BaB8','0xB8A914A00664e9361eaE187468EfF94905dfbC15','0x683A4ac99E65200921f556A19dADf4b0214B5938','0x636bd98fc13908e475f56d8a38a6e03616ec5563','0xD875a85C5Fe61a84fe6bdB16644B52118fDb8Dfd','0xce0CA7A150a441Cc52415526F2EaC97Aa4140D13','0x07040971246a73ebDa9Cf29ea1306bB47C7C4e76','0x4aBD5745F326932B1b673bfa592a20d7BB6bc455','0x777BE1c6075c20184C4fd76344b7b0B7c858fe6B','0xcee99db49fe7B6e2d3394D8df2989B564bB613DB']
    # из них с ошибками завершились:
    #list_addresses = ["0x42069cc15F5BEfb510430d22Ff1c9A1b3ae22CfE","0xcee99db49fe7B6e2d3394D8df2989B564bB613DB","0xfefe157c9d0aE025213092ff9a5cB56ab492BaB8","0x636bd98fc13908e475f56d8a38a6e03616ec5563"]


    #list_addresses = ['0xb8D6196D71cdd7D90A053a7769a077772aAac464'] # MARS
    #list_addresses = ['0x6dB6FDb5182053EECeC778aFec95E0814172A474'] # FARM
    #list_addresses = ['0x5200B34E6a519F289f5258dE4554eBd3dB12E822'] # GOAT
    #list_addresses = ['0x9D09bCF1784eC43f025d3ee071e5b632679a01bA'] # TEE
    # 0xd6203889C22D9fe5e938a9200f50FDFFE9dD8e02 # SBR
    #list_addresses = ['0xE344EaD74a37aBc8df75C8392caC25a55F44F19d'] # NFL упал быстро, 0xd9b9c511fbc384f431ce7c5b6f859643a3ca5ec27d2a73a61be7899952146f95 - Transfer()  NFL

    #list_addresses = ['0xb8D6196D71cdd7D90A053a7769a077772aAac464','0x6dB6FDb5182053EECeC778aFec95E0814172A474','0x5200B34E6a519F289f5258dE4554eBd3dB12E822','0xE344EaD74a37aBc8df75C8392caC25a55F44F19d']


    # list_addresses = ['0x23bbb1b8cc5756e4211c859bdc774f29461478c3','0x1c60dd64ebf1ca2aa5ac62f92a29e6aae6407fc4','0x5200B34E6a519F289f5258dE4554eBd3dB12E822']
    
    
    #list_addresses = ['0x9D09bCF1784eC43f025d3ee071e5b632679a01bA']

    # high profit ~20-30 Oct
    #list_addresses = ['0x6666664b8362444280b7859F40265e5A2e54b661','0x281C2C810c6A483A504c8f45091879f59619d305','0x60881622f6dBB321A3878dA1c13888aD4a00698c','0x9D09bCF1784eC43f025d3ee071e5b632679a01bA','0x8808434a831eFea81170A56a9ddc57Cc9E6De1d8','0xba62856E16CB3283d3B8E670B196B9Bb02902f30','0x42069026EAC8Eee0Fd9b5f7aDFa4f6E6D69a2B39','0x535887989b9EdffB63b1Fd5C6b99a4d45443b49a','0x230EA9AEd5d08AfDb22Cd3c06c47cf24aD501301',]
    
    # high profit from collected
    # list_addresses = ['0x8802269D1283cdB2a5a329649E5cB4CdcEE91ab6','0x790814Cd782983FaB4d7B92CF155187a865d9F18','0x42069cc15F5BEfb510430d22Ff1c9A1b3ae22CfE','0xfefe157c9d0aE025213092ff9a5cB56ab492BaB8','0xB8A914A00664e9361eaE187468EfF94905dfbC15','0x683A4ac99E65200921f556A19dADf4b0214B5938','0x636bd98fc13908e475f56d8a38a6e03616ec5563','0xD875a85C5Fe61a84fe6bdB16644B52118fDb8Dfd','0xce0CA7A150a441Cc52415526F2EaC97Aa4140D13','0x07040971246a73ebDa9Cf29ea1306bB47C7C4e76','0x4aBD5745F326932B1b673bfa592a20d7BB6bc455','0x777BE1c6075c20184C4fd76344b7b0B7c858fe6B','0xcee99db49fe7B6e2d3394D8df2989B564bB613DB','0x5B1543C4EA138eFAE5b0836265dfF75e1Ab6227D','0xb8D6196D71cdd7D90A053a7769a077772aAac464','0x6dB6FDb5182053EECeC778aFec95E0814172A474','0x5200B34E6a519F289f5258dE4554eBd3dB12E822','0x66b5228CfD34d9f4d9f03188d67816286C7c0b74','0x230EA9AEd5d08AfDb22Cd3c06c47cf24aD501301','0x42069026EAC8Eee0Fd9b5f7aDFa4f6E6D69a2B39','0xF63E309818E4EA13782678CE6C31C1234fa61809','0x594DaaD7D77592a2b97b725A7AD59D7E188b5bFa','0x812Ba41e071C7b7fA4EBcFB62dF5F45f6fA853Ee','0x5200B34E6a519F289f5258dE4554eBd3dB12E822','0xb612bFC5cE2FB1337Bd29F5Af24ca85DbB181cE2','0xCeD88f3Ca5034AbFC1F5492a808a39eE2B16F321','0xc56C7A0eAA804f854B536A5F3D5f49D2EC4B12b8','0x1121AcC14c63f3C872BFcA497d10926A6098AAc5','0xD29DA236dd4AAc627346e1bBa06A619E8c22d7C5','0xeBb66a88cEdd12bfE3a289df6DFEe377F2963F12','0xc8F69A9B46B235DE8d0b77c355FFf7994F1B090f','0xEE2a03Aa6Dacf51C18679C516ad5283d8E7C2637','0xe5018913F2fdf33971864804dDB5fcA25C539032','0xb3E41D6e0ea14b43BC5DE3C314A408af171b03dD','0x6E79B51959CF968d87826592f46f819F92466615','0x41D06390b935356b46aD6750bdA30148Ad2044A4','0x391cF4b21F557c935C7f670218Ef42C21bd8d686','0x3fFEea07a27Fab7ad1df5297fa75e77a43CB5790','0x4e6221c07DAE8D3460a46fa01779cF17FdD72ad8','0xd1A6616F56221A9f27eB9476867b5bDB2B2d101d','0x0000e3705d8735ee724a76F3440c0a7ea721eD00','0x289Ff00235D2b98b0145ff5D4435d3e92f9540a6','0xB3912b20b3aBc78C15e85E13EC0bF334fbB924f7','0xCb76314C2540199f4B844D4ebbC7998C604880cA','0x240Cd7b53d364a208eD41f8cEd4965D11F571B7a','0x67466BE17df832165F8C80a5A120CCc652bD7E69','0x1495bc9e44Af1F8BCB62278D2bEC4540cF0C05ea','0x69babE9811CC86dCfC3B8f9a14de6470Dd18EDA4','0xeD89fc0F41d8Be2C98B13B7e3cd3E876D73f1d30','0x5981E98440E41fa993B26912B080922b8Ed023c3','0xfefe157c9d0aE025213092ff9a5cB56ab492BaB8','0x8bAF5d75CaE25c7dF6d1E0d26C52d19Ee848301a','0x28561B8A2360F463011c16b6Cc0B0cbEF8dbBcad','0x6969f3A3754AB674B48B7829A8572360E98132Ba','0x155788DD4b3ccD955A5b2D461C7D6504F83f71fa','0x36C7188D64c44301272Db3293899507eabb8eD43','0xCE176825AFC335d9759cB4e323eE8b31891DE747','0x00F116ac0c304C570daAA68FA6c30a86A04B5C5F','0x95Ed629B028Cf6AADd1408Bb988c6d1DAAbe4767','0xed2D1Ef84a6a07dD49E8cA934908E9De005b7824','0xa00453052A36D43A99Ac1ca145DFe4A952cA33B8','0x4C44A8B7823B80161Eb5E6D80c014024752607F2','0x13E4b8CfFe704d3De6F19E52b201d92c21EC18bD','0x4AdE2b180f65ed752B6F1296d0418AD21eb578C0','0xE81C4A73bfDdb1dDADF7d64734061bE58d4c0b4C','0x71297312753EA7A2570a5a3278eD70D9a75F4f44','0x85bEA4eE627B795A79583FcedE229E198aa57055','0x9D09bCF1784eC43f025d3ee071e5b632679a01bA','0x556c3cbDCA77a7f21AFE15b17e644e0e98e64Df4','0xd6203889C22D9fe5e938a9200f50FDFFE9dD8e02']

    #list_addresses = ["0x391cF4b21F557c935C7f670218Ef42C21bd8d686","0xD29DA236dd4AAc627346e1bBa06A619E8c22d7C5","0xd1A6616F56221A9f27eB9476867b5bDB2B2d101d","0xe5018913F2fdf33971864804dDB5fcA25C539032","0x67466BE17df832165F8C80a5A120CCc652bD7E69","0x9D09bCF1784eC43f025d3ee071e5b632679a01bA","0x28561B8A2360F463011c16b6Cc0B0cbEF8dbBcad","0xb3E41D6e0ea14b43BC5DE3C314A408af171b03dD","0x00F116ac0c304C570daAA68FA6c30a86A04B5C5F","0x4AdE2b180f65ed752B6F1296d0418AD21eb578C0","0x0000e3705d8735ee724a76F3440c0a7ea721eD00","0x07040971246a73ebDa9Cf29ea1306bB47C7C4e76","0x3fFEea07a27Fab7ad1df5297fa75e77a43CB5790","0x69babE9811CC86dCfC3B8f9a14de6470Dd18EDA4","0x66b5228CfD34d9f4d9f03188d67816286C7c0b74","0x8802269D1283cdB2a5a329649E5cB4CdcEE91ab6","0x636bd98fc13908e475f56d8a38a6e03616ec5563","0x1495bc9e44Af1F8BCB62278D2bEC4540cF0C05ea","0x85bEA4eE627B795A79583FcedE229E198aa57055","0xa00453052A36D43A99Ac1ca145DFe4A952cA33B8","0xCb76314C2540199f4B844D4ebbC7998C604880cA","0x8bAF5d75CaE25c7dF6d1E0d26C52d19Ee848301a","0x289Ff00235D2b98b0145ff5D4435d3e92f9540a6","0x36C7188D64c44301272Db3293899507eabb8eD43","0x790814Cd782983FaB4d7B92CF155187a865d9F18","0x556c3cbDCA77a7f21AFE15b17e644e0e98e64Df4"]
    #list_addresses = ['0x391cF4b21F557c935C7f670218Ef42C21bd8d686','0x1495bc9e44Af1F8BCB62278D2bEC4540cF0C05ea']
    
    #  analyse_30m.create_features() error="pair=0xA9350cEe79305D8488C3cDebCbD88309d8f604C7 not in last_trades.pairs"
    #list_addresses = ['0xd1A6616F56221A9f27eB9476867b5bDB2B2d101d','0x1495bc9e44Af1F8BCB62278D2bEC4540cF0C05ea','0xCb76314C2540199f4B844D4ebbC7998C604880cA']
    #list_addresses = ['0xd1A6616F56221A9f27eB9476867b5bDB2B2d101d','0x1495bc9e44Af1F8BCB62278D2bEC4540cF0C05ea']

    # mint:
    #list_addresses = ['0x86B69F38BEA3e02f68fF88534bc61EC60E772b19','0x6666664b8362444280b7859F40265e5A2e54b661']


    #list_addresses = ['0x6666664b8362444280b7859F40265e5A2e54b661','0x281C2C810c6A483A504c8f45091879f59619d305','0x60881622f6dBB321A3878dA1c13888aD4a00698c','0x9D09bCF1784eC43f025d3ee071e5b632679a01bA','0x8808434a831eFea81170A56a9ddc57Cc9E6De1d8','0xba62856E16CB3283d3B8E670B196B9Bb02902f30','0x42069026EAC8Eee0Fd9b5f7aDFa4f6E6D69a2B39','0x535887989b9EdffB63b1Fd5C6b99a4d45443b49a','0x230EA9AEd5d08AfDb22Cd3c06c47cf24aD501301','0x8802269D1283cdB2a5a329649E5cB4CdcEE91ab6','0x790814Cd782983FaB4d7B92CF155187a865d9F18','0x42069cc15F5BEfb510430d22Ff1c9A1b3ae22CfE','0xfefe157c9d0aE025213092ff9a5cB56ab492BaB8','0xB8A914A00664e9361eaE187468EfF94905dfbC15','0x683A4ac99E65200921f556A19dADf4b0214B5938','0x636bd98fc13908e475f56d8a38a6e03616ec5563','0xD875a85C5Fe61a84fe6bdB16644B52118fDb8Dfd','0xce0CA7A150a441Cc52415526F2EaC97Aa4140D13','0x07040971246a73ebDa9Cf29ea1306bB47C7C4e76','0x4aBD5745F326932B1b673bfa592a20d7BB6bc455','0x777BE1c6075c20184C4fd76344b7b0B7c858fe6B','0xcee99db49fe7B6e2d3394D8df2989B564bB613DB','0x5B1543C4EA138eFAE5b0836265dfF75e1Ab6227D','0xb8D6196D71cdd7D90A053a7769a077772aAac464','0x6dB6FDb5182053EECeC778aFec95E0814172A474','0x5200B34E6a519F289f5258dE4554eBd3dB12E822','0x66b5228CfD34d9f4d9f03188d67816286C7c0b74','0x230EA9AEd5d08AfDb22Cd3c06c47cf24aD501301','0x42069026EAC8Eee0Fd9b5f7aDFa4f6E6D69a2B39','0xF63E309818E4EA13782678CE6C31C1234fa61809','0x594DaaD7D77592a2b97b725A7AD59D7E188b5bFa','0x812Ba41e071C7b7fA4EBcFB62dF5F45f6fA853Ee','0x5200B34E6a519F289f5258dE4554eBd3dB12E822','0xb612bFC5cE2FB1337Bd29F5Af24ca85DbB181cE2','0xCeD88f3Ca5034AbFC1F5492a808a39eE2B16F321','0xc56C7A0eAA804f854B536A5F3D5f49D2EC4B12b8','0x1121AcC14c63f3C872BFcA497d10926A6098AAc5','0xD29DA236dd4AAc627346e1bBa06A619E8c22d7C5','0xeBb66a88cEdd12bfE3a289df6DFEe377F2963F12','0xc8F69A9B46B235DE8d0b77c355FFf7994F1B090f','0xEE2a03Aa6Dacf51C18679C516ad5283d8E7C2637','0xe5018913F2fdf33971864804dDB5fcA25C539032','0xb3E41D6e0ea14b43BC5DE3C314A408af171b03dD','0x6E79B51959CF968d87826592f46f819F92466615','0x41D06390b935356b46aD6750bdA30148Ad2044A4','0x391cF4b21F557c935C7f670218Ef42C21bd8d686','0x3fFEea07a27Fab7ad1df5297fa75e77a43CB5790','0x4e6221c07DAE8D3460a46fa01779cF17FdD72ad8','0xd1A6616F56221A9f27eB9476867b5bDB2B2d101d','0x0000e3705d8735ee724a76F3440c0a7ea721eD00','0x289Ff00235D2b98b0145ff5D4435d3e92f9540a6','0xB3912b20b3aBc78C15e85E13EC0bF334fbB924f7','0xCb76314C2540199f4B844D4ebbC7998C604880cA','0x240Cd7b53d364a208eD41f8cEd4965D11F571B7a','0x67466BE17df832165F8C80a5A120CCc652bD7E69','0x1495bc9e44Af1F8BCB62278D2bEC4540cF0C05ea','0x69babE9811CC86dCfC3B8f9a14de6470Dd18EDA4','0xeD89fc0F41d8Be2C98B13B7e3cd3E876D73f1d30','0x5981E98440E41fa993B26912B080922b8Ed023c3','0xfefe157c9d0aE025213092ff9a5cB56ab492BaB8','0x8bAF5d75CaE25c7dF6d1E0d26C52d19Ee848301a','0x28561B8A2360F463011c16b6Cc0B0cbEF8dbBcad','0x6969f3A3754AB674B48B7829A8572360E98132Ba','0x155788DD4b3ccD955A5b2D461C7D6504F83f71fa','0x36C7188D64c44301272Db3293899507eabb8eD43','0xCE176825AFC335d9759cB4e323eE8b31891DE747','0x00F116ac0c304C570daAA68FA6c30a86A04B5C5F','0x95Ed629B028Cf6AADd1408Bb988c6d1DAAbe4767','0xed2D1Ef84a6a07dD49E8cA934908E9De005b7824','0xa00453052A36D43A99Ac1ca145DFe4A952cA33B8','0x4C44A8B7823B80161Eb5E6D80c014024752607F2','0x13E4b8CfFe704d3De6F19E52b201d92c21EC18bD','0x4AdE2b180f65ed752B6F1296d0418AD21eb578C0','0xE81C4A73bfDdb1dDADF7d64734061bE58d4c0b4C','0x71297312753EA7A2570a5a3278eD70D9a75F4f44','0x85bEA4eE627B795A79583FcedE229E198aa57055','0x9D09bCF1784eC43f025d3ee071e5b632679a01bA','0x556c3cbDCA77a7f21AFE15b17e644e0e98e64Df4']

    #list_addresses = ['0xba531467411155051b8628c2256c42dcb42bf968','0x7e78011b2bf4628f4457cc1983f0cff1414761ae']

    # (!) analyse_30m.create_features() error="pair=0xA9350cEe79305D8488C3cDebCbD88309d8f604C7 not in last_trades.pairs", _minutes_past=307.2, total_trades=513, traded_blocks=189, len blocks_data=191, address=0xd1A6616F56221A9f27eB9476867b5bDB2B2d101d
    # AssertionError: exit stat analyse_30m.create_features(), address=0xd1A6616F56221A9f27eB9476867b5bDB2B2d101d
    # list_addresses = ['0xd1A6616F56221A9f27eB9476867b5bDB2B2d101d']

    #   File "/home/sirjay/python/ethereum/lib/last_trades.py", line 1503, in get_trades
    # AssertionError: type(self.df_analyse) == type(None), total_trades=6438, address=0x66b5228CfD34d9f4d9f03188d67816286C7c0b74
    # done fail 0x66b5228CfD34d9f4d9f03188d67816286C7c0b74
    # done fail 0x8f88e2f597c462d8ab16b0cc45e3a7481569fb13

    # (!) analyse_30m.create_features() error="last_trades.blocks_data[backtest_block]['value_usd_per_token_unit'] == None, backtest_block=19510317", _minutes_past=25309.8, total_trades=1128, traded_blocks=1673, len blocks_data=1688, address=0x000000000503BE77A5ED27bef2C19943A8B5AE73

    # REEF, XYO, SNX, ENS, INJ, DODO, COMP, ELON, ARC, TRUMP, JOE, LMEOW
    # list_addresses = ['0xFE3E6a25e6b192A42a44ecDDCd13796471735ACf','0x55296f69f40Ea6d20E478533C15A6B08B654E758','0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F','0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72','0xe28b3B32B6c345A34Ff64674606124Dd5Aceca30','0x747E550a7B848acE786C3CFe754Aa78feBC8A022','0xc00e94Cb662C3520282E6f5717214004A7f26888','0x761D38e5ddf6ccf6Cf7c55759d5210750B5D60F3','0xC82E3dB60A52CF7529253b4eC688f631aad9e7c2','0x576e2BeD8F7b46D34016198911Cdf9886f78bea7','0x76e222b07C53D28b89b0bAc18602810Fc22B49A8','0x1aE7e1d0ce06364CED9aD58225a1705b3e5DB92b']

    # REEF
    #list_addresses = ['0xfe3e6a25e6b192a42a44ecddcd13796471735acf']

    # REEF, KP3R, ELON, XYO, VIRTUAL, LORDS
    list_addresses = ['0xfe3e6a25e6b192a42a44ecddcd13796471735acf','0x1ceb5cb57c4d4e2b2433641b95dd330a33185a44','0x761D38e5ddf6ccf6Cf7c55759d5210750B5D60F3', '0x55296f69f40Ea6d20E478533C15A6B08B654E758', '0x44ff8620b8cA30902395A7bD3F2407e1A091BF73', '0x686f2404e77Ab0d9070a46cdfb0B7feCDD2318b0']


    list_addresses = list(set(list_addresses))
    print(f"list_addresses len={len(list_addresses)}")


    # if addresses_in_folder:
    #     list_addresses = list(set(list_addresses) - set(addresses_in_folder))
    #     print(f"после вычитания addresses_in_folder у list_addresses len={len(list_addresses)}")


    random.shuffle(list_addresses)


    params = []
    for ia, address in enumerate(list_addresses):
        params.append((address,LAST_BACKTESTING_BLOCK,))

    with utils.NestablePool(min(80, len(list_addresses))) as pool:
        token_objects = pool.starmap_async(wrapper_last_trades, params).get()

    print('~'*50)

    print('results for green flags:')
    cnt = 0
    first_in_transaction_from_address_rating = {}
    no_errors_tokens = []
    for token in token_objects:
        if token:
            if 1 or token.block0_green_flags:
                print(f"{token.address} {token.block0_green_flags}")
                if token.txs_sql_mints:
                    print(f"   block0 and first mint diff: {token.first_trade_tx_sql['block_number'] - token.txs_sql_mints[list(token.txs_sql_mints.keys())[0]]['block_number']}")
                    print(f"   trades: {token.blocks_data[token.first_trade_tx_sql['block_number']]['trades_buy']} / {token.blocks_data[token.first_trade_tx_sql['block_number']]['trades_sell']}")
                    if token.pairs[list(token.pairs_dict.keys())[0]]['initial_liquidity'] != None:
                        print(f"   initial liquidity: {token.pairs[list(token.pairs_dict.keys())[0]]['initial_liquidity']['value_usd']:.2f} $")
                    else:
                        print(f"   initial liquidity: None")
                    print(f"   liquidity: {token.blocks_data[token.first_trade_tx_sql['block_number']]['liquidity']:.2f} $")
                    print(f"   invested: {token.blocks_data[token.first_trade_tx_sql['block_number']]['invested']:.2f} $")
                    print(f"   volume_buy: {token.blocks_data[token.first_trade_tx_sql['block_number']]['volume_buy']:.2f} $")
                    print(f"   tx_index первого Mint(): {token.txs_sql_mints[list(token.txs_sql_mints.keys())[0]]['transactionindex']}")
                    print('type(token.initial_value_usd_per_token_unit):', type(token.initial_value_usd_per_token_unit))
                    if token.initial_value_usd_per_token_unit not in [0, None]:
                        print(f"   на {round(100 * token.blocks_data[token.first_trade_tx_sql['block_number']]['value_usd_per_token_unit'] * 10 ** token.contract_data_token['token_decimals'] / token.initial_value_usd_per_token_unit - 100, 2)}% цена изменилась в $")
                    else:
                        print(f"   token.initial_value_usd_per_token_unit={token.initial_value_usd_per_token_unit}, нельзя определить на сколько % цена изменилась")
                else:
                    print(f"   у токена нет txs_sql_mints")
                cnt += 1
        
            for adr, val in token.first_in_transaction_from_address_rating.items():
                if adr not in first_in_transaction_from_address_rating: first_in_transaction_from_address_rating[adr] = 0
                first_in_transaction_from_address_rating[adr] += val
            
            no_errors_tokens.append(token.address)

    print(f"list_addresses len={len(list_addresses)}, green flag tokens={cnt}")

    first_in_transaction_from_address_rating = sorted(first_in_transaction_from_address_rating.items(), key=lambda d: d[1])
    print('first_in_transaction_from_address_rating:')
    for adr, val in first_in_transaction_from_address_rating:
        if val >= 4:
            print(f"   {utils.print_address(adr)}: {val}")

    print(f"no_errors_tokens:")
    for adr in no_errors_tokens:
        print(f'"{adr}",')

    print(f"error tokens:")
    for adr in list(set(list_addresses) - set(no_errors_tokens)):
        print(f'"{adr}",')
    
    print(f"{(time.time() - start_time_e):.2f}s total exec")

    # print(f"token_object for [0]:")
    # print(token_objects[0].full_log_text)