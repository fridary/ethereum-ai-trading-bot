import os
import sys
from web3 import Web3
from web3.logs import DISCARD
from web3.exceptions import BadFunctionCallOutput, ContractLogicError
from web3._utils.events import get_event_data
import requests
import json
import traceback
from attributedict.collections import AttributeDict
from pprint import pprint
from colorama import Fore, Back, Style
import statistics
import pandas as pd
import time
import random
import pickle
import string
import copy
from datetime import datetime
from eth_utils import to_checksum_address, from_wei, to_wei
from hexbytes import HexBytes
import tabulate
from multiprocessing import Pool

import psycopg2
from psycopg2.extras import Json, DictCursor, RealDictCursor

import utils
from utils import FILES_DIR, FILE_LABELS



class Token():


    
    def __init__(self, address, last_block, realtime=False, w3_g_eth_block_number=None, realtime_bMint=False, ia=None, verbose=1):

        self.address = to_checksum_address(address)
        self.last_block = last_block
        self.realtime = realtime
        self.w3_g_eth_block_number = w3_g_eth_block_number
        self.realtime_bMint = realtime_bMint
        self.ia = ia
        self.verbose = verbose

        self.balances = {}
        """
        {'%': 44.25564618159304,
        'days': 0.0,
        'first_trade': 1717027043,
        'first_trade_block': ...,
        'gas_usd': 6.019402890115204,
        'i': 4, # номер покупателя, начиная с 0
        'invested': 3763.29,
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
        'trades_list': [{'q': 429279767.96145254,'s': 'buy','t': 1717027043,'vut': 8.76652076539961e-06}],
        'trades_sell': 0,
        'unrealized_profit': 2000.2845081898126,
        'value': 429279767961452528960879884,
        'value_usd': 5763.574508189812,
        'withdrew': 0}
        """

        self.owner_addresses = [] # последний эл-т - это последний owner
        self.wallet_token = {'Wei': {'value': 0, 'value_usd': None}} # токены на адресе контракта
        self.transactions = [] # только транзакции Transfer/Swap токена
        self.pairs = {}
        # self.pairs = {
        #     '..pair_address..': {
        #         'block_created_number': ..., # на каком блоке создалась пара
        #         'block_created_timestamp': ..., # timestamp этого блока
        #         'exchange': .., # V2, V3
        #         'initial_liquidity': {'value': .., 'value_usd': ..}, # value в валюте token1 (WETH/USDC/USDT)
        #         'extra_liquidities': [{'value': .., 'value_usd': ..}, ...] # массив сколько новых Mint() было сделано
        #         'token1_address': .., # WETH, USDC, USDT
        #         'token1_liquidity': ..,
        #         'token1_liquidity_usd': ..,
        #         'balances': [],
        #         'wallet_token': {'Wei': {'value': 0, 'value_usd': None}}, # токены на адресе контракта
        #     }
        # }
        self.total_supply = None # без деления на token_decimals
        self.traded_volume_buy = 0 # $
        self.traded_volume_sell = 0 # $
        self.trades_buy = 0
        self.trades_sell = 0
        self.block_first_trade = None # на каком блоке первая сделка
        self.last_transaction_block = None # последний номер block'a, на котором была транзакция
        # (!) ... получить значение цены usd inited перед началом торгов 
        self.transaction_absolute_first = None
        self.transaction_first_ownershiptransferred = None # на какой транзакции был первый OwnershipTransferred
        self.transactions_paircreated = [] # список транзакций с PairCreated()/PoolCreated() токена
        self.transactions_mint = [] # список транзакций с Mint() токена
        self.transaction_blocks_before_mint = [] # список блоков, где выполнялись транзакции (все подряд, включая Approve)
        self.transaction_blocks_after_mint = []
        self.how_many_holders_before_first_mint = None # без pairs, native
        self.how_many_perc_holders_before_first_mint = 0 # суммарно % от supply объема
        self.holders_before_mint_values = [] # value без деления на decimal всех холдеров до Mint()
        self.holders_before_mint_percentages = [] # %-ы у каждого холдера до Mint() (из holders_before_mint_values берется)
        self.initial_value_usd_per_token_unit = None # цена токена в момент Mint()
        self.initial_value_native_per_token_unit = None # значение в WETH или USDC/USDT
        self.ownership_renounce_before_paircreated = False
        self.jaredfromsubway_appeared = False
        self.mevbot_appeared_times = 0
        self.transaction_ownership_burned = None
        self.p_i = None
        self.red_flags = []
        self.bribe_sent = [] # массив из ETH отправок bribe
        self.blocks_data = {} # данные, где ключ есть номер блока
        # self.blocks_data[<номер_блока>] = {
        #    '<pair_address>': {
        #       'token1_liquidity': ..
        #       'value_native_per_token_unit': ..
        #       'value_usd_per_token_unit': ..
        #       'price_change': ..
        #     },
        #     'timestamp',
        #     'traded_volume_buy': ..
        #     'traded_volume_sell': ..
        #     'trades_buy': ..
        #     'trades_sell': ..
        #     'transactions_count': ..
        #     'prediction': [0/1, <prob>] # предикт на данном блоке по _b0/1/2/..
        #     'prediction_all': [0/1, <prob>] # предикт по _all
        #     'stat_base': ..
        #     'stat_last_transactions': ..
        #     'stat_wallets': ..
        # }
        self.steps_data = {} # данные, где ключ есть номер шага
        # self.steps_data["step1"] = {
        #      'prediction': [0/1, <prob>] # предикт на данном step
        #      'prediction_all': [0/1, <prob>] # предикт по _all
        # }

        # abi = utils.get_abi(self.address)
        # if 'error' in abi and abi['error'] == 'Contract source code not verified':
        #     abi = utils.get_abi('0x69420E3A3aa9E17Dea102Bb3a9b3B73dcDDB9528') # ELON
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
        self.value_usd_per_token_unit = None

        self.addresses_for_abi = [utils.UNI_V2_FACTORY, utils.UNI_V3_FACTORY, utils.WETH, utils.USDT, utils.USDC]
        for _, item in FILE_LABELS['ethereum'].items():
            self.addresses_for_abi += item.keys()

        self._contracts = {}

        self._stat_base = {}
        self._stat_last_transactions = {}
        self._stat_wallets = {}

        self.last_swaped = None # значение для понимания что есть amount0, amount1 в Mint()


    @property
    def stats(self):
        return self._stat_base | self._stat_last_transactions | self._stat_wallets

    @property
    def stat_base(self):
        return self._stat_base

    @property
    def stat_last_transactions(self):
        return self._stat_last_transactions

    @property
    def stat_wallets(self):
        return self._stat_wallets

    @property
    def trades_total(self):
        return self.trades_buy + self.trades_sell
    
    @property
    def transactions_total(self):
        return len(self.transaction_blocks_before_mint) + len(self.transaction_blocks_after_mint)
    

    def print(self, *args, **kwargs):
        if self.verbose:
            print(*args)


    def balance_table(self):

        balances_ = sorted(self.balances.items(), key=lambda d: (d[1]['last_trade_block'], d[1]['last_p_i']))
        #balances_ = [value | {'address': address} for address, value in balances_]

        modified_w = []
        for address, value in balances_:

            #value = {'date': datetime.fromtimestamp(value['block_timestamp']).strftime("%d-%m-%Y"), 'address': address} | value
            # value = value | {'address': address}
            # print('value:')
            # print(value)

            a_c = {}
            a_c['i'] = value['i']
            a_c['l_block'] = value['last_trade_block']
            a_c['value'] = round(value['value'] / 10 ** self.contract_data_token['token_decimals'], 2)
            if value['value_usd']: a_c['value'] = f"{a_c['value']}, {round(value['value_usd'], 2)} $"
            if self.total_supply and address != utils.ADDRESS_0X0000:
                a_c['%'] = round(100 * value['value'] / self.total_supply, 2)
            else:
                a_c['%'] = None
            a_c['invested'] = f"{round(value['invested'], 2)} $"
            a_c['withdrew'] = f"{round(value['withdrew'], 2)} $"
            a_c['gas'] = f"{round(value['gas_usd'], 2)} $"

            a_c['liq_first'] = None
            if value['liquidity_first']:
                if value['liquidity_first']['t'] == value['liquidity_first']['p']:
                    a_c['liq_first'] = f"{utils.human_format(round(2 * value['liquidity_first']['t'], 2))} $"
                else:
                    a_c['liq_first'] = f"{utils.human_format(round(value['liquidity_first']['t'], 2)) if value['liquidity_first']['t'] else 'N/A'}/{utils.human_format(round(value['liquidity_first']['p'], 2)) if value['liquidity_first']['p'] else 'N/A'} $"

            a_c['router'] = value['router'].split(' ')[0].rstrip(':').lower() if value['router'] else None

            a_c['r_prof'] = f"{round(value['realized_profit'], 2)} $" if value['realized_profit'] != 0 else 0
            a_c['ur_prof'] = f"{round(value['unrealized_profit'], 2)} $" if value['unrealized_profit'] != 0 else 0
            a_c['roi_r'] = f"{round(value['roi_r'])}%" if value['roi_r'] != 0 else 0
            a_c['roi_u'] = f"{round(value['roi_u'])}%" if value['roi_u'] != 0 else 0
            a_c['roi_t'] = f"{round(value['roi_t'])}%" if value['roi_t'] != 0 else 0

            a_c['days'] = round(value['days'], 2) if value['days'] else None
            a_c['trades'] = f"{value['trades_buy']}/{value['trades_sell']}"
            a_c['pnl'] = f"{round(value['pnl'], 2)} $" if value['pnl'] != 0 else 0

            a_c['address'] = address[-7:]
            if address in self.owner_addresses:
                a_c['address'] = f"{Fore.RED}{a_c['address']}{Style.RESET_ALL}"
            if address == self.address:
                a_c['address'] = f"{Fore.GREEN}{a_c['address']}{Style.RESET_ALL}"
            if address in list(self.pairs.keys()):
                a_c['address'] = f"{Fore.BLUE}{a_c['address']}{Style.RESET_ALL}"

            
            modified_w.append(a_c)


        # order_ = ['i','value','%','invested','withdrew','last_trade_block','address']
        # for j in range(len(modified_w)):
        #     temp_w_ = {}
        #     for or_ in order_:
        #         if or_ in modified_w[j]:
        #             temp_w_[or_] = modified_w[j][or_]
        #         else:
        #             temp_w_[or_] = None
        #     pprint(temp_w_)
        #     modified_w[j] = temp_w_


        return tabulate.tabulate(modified_w[:], tablefmt='psql', headers='keys', stralign="right")
    


    def balance_pair_table(self, pair_address):

        balances_ = sorted(self.pairs[pair_address]['balances'].items(), key=lambda d: d[1]['last_trade_block'])
        balances_ = [value | {'address': address} for address, value in balances_]


        return tabulate.tabulate(balances_[:], tablefmt='psql', headers='keys', stralign="right")



    def calculate_liquidity(self, p_i, tx_sql, tx_eth_price, next_tx_block):
        """
        :next_tx_block: какой blockNumber у следующей итерации (None, если последняя)
        """

        if self.w3_g_eth_block_number - tx_sql['block_number'] < 50:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
            _testnet = f'http://127.0.0.1:{utils.GETH_PORT}'
        else:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
            _testnet = 'http://127.0.0.1:8545'

        if tx_sql['to'] == None:
            tx_sql['to'] = tx_sql['contractaddress']

        tx_sql['from'] = to_checksum_address(tx_sql['from'])
        tx_sql['to'] = to_checksum_address(tx_sql['to'])

        self.print(f" \x1b[6;30;42m({p_i})\x1b[0m tx_hash={tx_sql['hash']}, tx_block={tx_sql['block_number']} ({datetime.fromtimestamp(tx_sql['block_timestamp'])}), tx index={tx_sql['transactionindex']}")
        self.print(f"     from: {utils.print_address(tx_sql['from'])}, to: {utils.print_address(tx_sql['to'])}, value: {tx_sql['value']}, gas: {round(tx_sql['gasused'] * tx_sql['gasprice'] / tx_eth_price, 2) if tx_sql['gasused'] != None else None} $")

        self.last_transaction_block = tx_sql['block_number']
        if tx_sql['logs_array'] == None: tx_sql['logs_array'] = []

        if not self.transactions_mint:
            self.transaction_blocks_before_mint.append(tx_sql['block_number'])
        else:
            self.transaction_blocks_after_mint.append(tx_sql['block_number'])

        # (!) здесь потом gas_usd надо брать из бд
        tx_sql['gas_usd'] = tx_sql['gasused'] * tx_sql['gasprice'] / tx_eth_price if tx_sql['gasused'] != None else 0

        if tx_sql['gasused'] == None:
            print(f"(!) tx_sql['gasused']=None, address={self.address}, block={tx_sql['block_number']}")

        if tx_sql['from'] == '0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13' or tx_sql['to'] in ['0x6b75d8AF000000e20B7a7DDf000Ba900b4009A80', '0x1f2F10D1C40777AE1Da742455c65828FF36Df387']: # jaredfromsubway
            self.jaredfromsubway_appeared = True
        
        if tx_sql['from'] in utils.MEV_BOTS_LIST or tx_sql['to'] in utils.MEV_BOTS_LIST:
            self.mevbot_appeared_times += 1
        
        if str(tx_sql['block_number']) not in self.blocks_data:
            self.blocks_data[str(tx_sql['block_number'])] = {
                'prediction': None,
                'prediction_all': None,
            }

        self.p_i = p_i
        tx_sql['p_i'] = p_i
        block_timestamp = tx_sql['block_number']
        paircreated = False
        minted = False
        owner_added = False
        owner_addresses_before = copy.deepcopy(self.owner_addresses)

        if tx_sql['logs_array'] and (len(tx_sql['logs_array']) < 1000 or tx_sql['logs_array'][0]['logIndex'] > tx_sql['logs_array'][-1]['logIndex']):
            tx_sql['logs_array'] = sorted(tx_sql['logs_array'], key=lambda d: d['logIndex'])

        for ii, log in enumerate(tx_sql['logs_array']):

            log['address'] = to_checksum_address(log['address'])

            if self.verbose:
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

                print(f" \x1b[34;1m [{ii + 1}/{log['logIndex']}]\x1b[0m address={log['address']} {contract_data}")                
                print(f"      \x1b[34;1m{utils.get_event_name_by_sig(log['topics_0']).split('(')[0]}\x1b[0m {log['decoded_data'] or {}}")
                print(f"      topic_0={log['topics_0']}")
                if log['topics_1'] != None: print(f"      topic_1={log['topics_1']}")
                if log['topics_2'] != None: print(f"      topic_2={log['topics_2']}")
                if log['topics_3'] != None: print(f"      topic_3={log['topics_3']}")

                
            decoded_data = log['decoded_data']

            # (!) на этих адресах не ясно как подтвердили Ownership
            # 0x527A67d31cDadAde333A1e9E9b417055AAACB86b
            # 0x1122A64Bcd9497370954Da7dA1825EE0F7185c71

            # OwnershipTransferred
            # 0xb980ebd41dcef12a4248d9ad98edcb1755661b59ee4527a6bacba38f792cdc35
            # 0xbbcbcd4038f93441663481df50476675f02559e97bb5bf753379c09c4a1aa5c4
            # renounceOwnership() 0xcc3da40248d8e102e8e5017154d994bc2ba50cadddca2abb939e482819616878
            if log['topics_0'] == '0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0':
                # log['topics_1'] - previousOwner
                # log['topics_2'] - newOwner
                owner_address = to_checksum_address(log['topics_2'])

                if not self.owner_addresses and p_i == 0:
                    assert self.transaction_first_ownershiptransferred == None
                    self.transaction_first_ownershiptransferred = tx_sql

                assert self.owner_addresses or not self.owner_addresses and p_i == 0
                if not self.owner_addresses or self.owner_addresses[-1] != owner_address:
                    self.owner_addresses.append(owner_address)
                owner_added = True
                self.print(f"      OwnershipTransferred(): address={owner_address}")
                if len(self.owner_addresses) >= 2 and owner_address in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:
                    if not self.pairs:
                        self.ownership_renounce_before_paircreated = True
                    self.transaction_ownership_burned = tx_sql
            
            # RolesAdded
            if log['topics_0'] == '0x34e73c57659d4b6809b53db4feee9b007b892e978114eda420d2991aba150143':
                # (!) здесь не уверен, пример 0xbf015d96a36b70b922a2f920dc747799bf9f98d14822a1b8c81feb633674831b
                assert not owner_addresses_before, owner_addresses_before
                if tx_sql['from'] not in self.owner_addresses:
                    assert self.owner_addresses or not self.owner_addresses and p_i == 0
                    self.owner_addresses.append(tx_sql['from'])
                    owner_added = True
                    self.print(f"      OwnershipTransferred() через RolesAdded(): address={tx_sql['from']}")

            # SetPairAddress
            # 0x3e6ab880d904bc8c25d9362a45e29de45203e5ac9a4f98e287bd14bb0f1e3094
            # PairCreated
            # 0xd6c0e7d791551b3c9c5c2d2b8015489fe252a0b44f253f2edbdc289be723d6b6
            # 0xe0a465d1e4a622ca43db886b589122c6f44390b6a1a1715aab0948ac246619c9
            # 0x7e2d49d0bb6c3c325934eb2586088cb456c571ee11452031bd1e6130a82b9e11
            # PoolCreated
            # 0xe2adabc835d5659444859aa285a117ff80c990a21c9791ad4a5847123da3e535
            # 0x73f089f3562602a15034cca283a6c0da2d6d2223d1e48e86cc3c83868d813b5c
            if (log['address'] == utils.UNI_V2_FACTORY and log['topics_0'] == '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' \
                or log['address'] == utils.UNI_V3_FACTORY and log['topics_0'] == '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118') \
                and self.address in [decoded_data['token0'], decoded_data['token1']]:
                if decoded_data['token1'] in [utils.WETH, utils.USDC, utils.USDT]:
                    token0 = decoded_data['token0']
                    token1 = decoded_data['token1']
                    self.last_swaped = False
                elif decoded_data['token0'] in [utils.WETH, utils.USDC, utils.USDT]:
                    token0 = decoded_data['token1']
                    token1 = decoded_data['token0']
                    self.last_swaped = True
                else:
                    assert 0, f"Неверное распределение token0/token1: {decoded_data}"
                assert log['address'] in [utils.UNI_V2_FACTORY, utils.UNI_V3_FACTORY], log['address']
                if log['address'] == utils.UNI_V2_FACTORY:
                    pair_address = decoded_data['pair']
                if log['address'] == utils.UNI_V3_FACTORY:
                    pair_address = decoded_data['pool']
                assert pair_address not in self.pairs, self.pairs[pair_address]

                self.pairs[pair_address] = {
                    'initial_liquidity': None,
                    'token1_address': token1,
                    'wallet_token': {'Wei': {'value': 0, 'value_usd': None}},
                }
                if log['address'] == utils.UNI_V2_FACTORY:
                    self.pairs[pair_address]['exchange'] = 'V2'
                if log['address'] == utils.UNI_V3_FACTORY:
                    self.pairs[pair_address]['exchange'] = 'V3'
                self.pairs[pair_address]['block_created_number'] = tx_sql['block_number']
                self.pairs[pair_address]['block_created_timestamp'] = tx_sql['block_timestamp']
                self.print(f"      PairCreated: pair address={pair_address}")
                self.print(f"      PairCreated: exchange={self.pairs[pair_address]['exchange']}")
                paircreated = True





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
                                print('Error _make_decode():', lo, 'tx hash:', tx_sql['hash'])
                        return decoded_data, evt_name
            
                    tx_receipt = requests.post(_testnet, json={"method":"eth_getTransactionReceipt","params":[tx_sql['hash']],"id":2,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']
                    # print('tx_receipt:')
                    # print('hash:', tx_sql['hash'])
                    # print('block:', tx_sql['block_number'])
                    # pprint(requests.post(_testnet, json={"method":"eth_getTransactionReceipt","params":[tx_sql['hash']],"id":2,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json())
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
                        if self.last_swaped == False:
                            amount0 = decoded_data['amount0']
                            amount1 = decoded_data['amount1']
                        elif self.last_swaped == True:
                            amount0 = decoded_data['amount1']
                            amount1 = decoded_data['amount0']
                        elif self.last_swaped == None:
                            if self.pairs[pair_address]['token1_address'] == utils.WETH:
                                amount0 = decoded_data['amount0']
                                amount1 = decoded_data['amount1']
                            else:
                                amount0 = decoded_data['amount1']
                                amount1 = decoded_data['amount0']

                        #if len(self.transactions_mint) == 0:
                        if len(self.transactions_mint) == 0:
                            assert self.total_supply == None, self.total_supply
                            assert self.pairs[pair_address]['initial_liquidity'] == None, self.pairs[pair_address]['initial_liquidity']
                            self.total_supply = amount0
                            self.pairs[pair_address]['token1_liquidity'] = 0
                            self.pairs[pair_address]['token1_liquidity_usd'] = 0
                            self.pairs[pair_address]['balances'] = {}
                            self.pairs[pair_address]['initial_liquidity'] = {'value': amount1}
                            self.pairs[pair_address]['extra_liquidities'] = []

                            # self.print('extra_liquidities created!', pair_address)

                            if self.pairs[pair_address]['token1_address'] == utils.WETH:
                                self.pairs[pair_address]['initial_liquidity']['value_usd'] = amount1 / tx_eth_price
                                self.print(f"      Mint: initial_liquidity={from_wei(amount1, 'ether')} ETH, {round(amount1 / tx_eth_price, 2)} $")
                            elif self.pairs[pair_address]['token1_address'] in [utils.USDC, utils.USDT]:
                                self.pairs[pair_address]['initial_liquidity']['value_usd'] = amount1 / 10 ** 6
                                self.print(f"      Mint: initial_liquidity={round(amount1 / 10 ** 6, 2)} $")

                            # _amount0 = self.total_supply
                            # _amount1 = self.pairs[pair_address]['initial_liquidity']['value']
                            _amount0 = amount0
                            _amount1 = amount1
                            #if not self.initial_value_native_per_token_unit:
                            if self.pairs[pair_address]['token1_address'] == utils.WETH:

                                if _amount0 == 0:
                                    assert 0, f"_amount0=0, address={self.address}"

                                self.initial_value_native_per_token_unit = (_amount1 / 10 ** 18) / (_amount0 / 10 ** self.contract_data_token['token_decimals'])
                                self.initial_value_usd_per_token_unit = (_amount1 / tx_eth_price) / (_amount0 / 10 ** self.contract_data_token['token_decimals'])
                            else:
                                self.initial_value_native_per_token_unit = (_amount1 / 10 ** 6) / (_amount0 / 10 ** self.contract_data_token['token_decimals'])
                                self.initial_value_usd_per_token_unit = self.initial_value_native_per_token_unit
                            
                        
                        else:

                            if self.pairs[pair_address]['token1_address'] == utils.WETH:
                                value_usd = amount1 / tx_eth_price
                                self.print(f"      Mint: extra_liquidity={from_wei(amount1, 'ether')} ETH, {round(amount1 / tx_eth_price, 2)} $")
                            elif self.pairs[pair_address]['token1_address'] in [utils.USDC, utils.USDT]:
                                value_usd = amount1 / 10 ** 6
                                self.print(f"      Mint: extra_liquidity={round(amount1 / 10 ** 6, 2)} $")

                            self.total_supply += amount0
                            print('now for new extra_liquidities', pair_address)
                            self.pairs[pair_address]['extra_liquidities'].append({'value': amount1, 'value_usd': value_usd})
                            self.print(f"      len extra_liquidities={len(self.pairs[pair_address]['extra_liquidities'])}")


                        minted = True
                        # (!) это надо раскомментить и убрать эту же строку ниже еще
                        # self.transactions_mint.append(tx_sql)
                        self.print(f"      Mint: total_supply={amount0}")



        if self.verbose:
            print('transaction_transfers:')
            pprint(tx_sql['transfers_array'])

        transaction_transfers = tx_sql['transfers_array']

        if transaction_transfers == None:
            self.print(f"\x1b[31;1m(!)\x1b[0m почему-то transaction_transfers=None, хотя у остальных транзакций не так")
            transaction_transfers = []

        # (!) перепроверить эту транзакцию 0x2730b2bafa205b60d112a9eb73c9886ae8f45f96f3ae14f98306ddb051d46230
        # здесь event_type='-' почему-то, хотя должен быть trade

        has_in_token, has_out_token = False, False
        has_in_other, has_out_other = False, False
        how_much_usd_was_spent = {} # здесь заложено сколько $ было потрачено без учета self.address
        how_much_usd_was_gained = {} # здесь заложено сколько $ было получено без учета self.address
        how_much_native_spent, how_much_native_gained = {}, {} # значение в value токена
        action_with_token = None
        what_pair_was_traded = None

        liquidities_before_trade = {} # $
        for pair_address, value in self.pairs.items():
            if 'token1_liquidity_usd' in value:
                liquidities_before_trade[pair_address] = value['token1_liquidity_usd']

        for i, trf in enumerate(transaction_transfers):
            # transaction_transfers[i] = {'address': trf['a'], 'from': trf['f'], 'to': trf['t'], 'value': trf['v'], 'value_usd': trf['vu'], 'value_usd_per_token_unit': trf['vut']}
            # if 'rt' in trf and trf['rt']: transaction_transfers[i]['reserves_token_usd'] = trf['rt']
            # if 'rp' in trf and trf['rp']: transaction_transfers[i]['reserves_pair_usd'] = trf['rp']
            # trf = transaction_transfers[i]

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

            if trf['from'] and trf['from'] not in how_much_usd_was_spent: how_much_usd_was_spent[trf['from']] = 0
            if trf['to'] and trf['to'] not in how_much_usd_was_gained: how_much_usd_was_gained[trf['to']] = 0

            if self.address in [trf['from'], trf['to']]:
                if trf['address'] not in self.wallet_token:
                    self.wallet_token[trf['address']] = {'value': 0, 'value_usd': None}

            if trf['address'] == None: # Wei
                found_ = False
                if trf['to'] == self.address:
                    self.wallet_token['Wei']['value'] += trf['value']
                    if not self.realtime_bMint:
                        self.wallet_token['Wei']['value_usd'] = self.wallet_token['Wei']['value'] / trf['value_usd_per_token_unit']
                    else:
                        self.wallet_token['Wei']['value_usd'] = self.wallet_token['Wei']['value'] / tx_eth_price
                    self.print(f"      пополнение address +{trf['value']} Wei ({from_wei(trf['value'], 'ether')} ETH, {round(trf['value'] / trf['value_usd_per_token_unit'], 2)} $)")
                    found_ = True
                elif trf['from'] == self.address:
                    self.wallet_token['Wei']['value'] -= trf['value']
                    if not self.realtime_bMint:
                        self.wallet_token['Wei']['value_usd'] = self.wallet_token['Wei']['value'] / trf['value_usd_per_token_unit']
                    else:
                        self.wallet_token['Wei']['value_usd'] = self.wallet_token['Wei']['value'] / tx_eth_price
                    self.print(f"      списание address -{trf['value']} Wei ({from_wei(trf['value'], 'ether')} ETH, {round(trf['value'] / trf['value_usd_per_token_unit'], 2)} $)")
                    found_ = True
                else:

                    
                    # пример адреса, где есть операции ниже 0x5806c0d97B2443a431465555647E0240eA675053, 0xf51e51cf61e2cc6770b18862f81e442a5826659a
                    for pair_address in self.pairs.keys():
                        if trf['to'] == pair_address:
                            self.pairs[pair_address]['wallet_token']['Wei']['value'] += trf['value']
                            self.pairs[pair_address]['wallet_token']['Wei']['value_usd'] = self.pairs[pair_address]['wallet_token']['Wei']['value'] * tx_eth_price
                            self.print(f"      пополнение pair_address={pair_address} +{trf['value']} Wei ({from_wei(trf['value'], 'ether')} ETH, {round(trf['value'] * tx_eth_price, 2)} $), баланс Wei: {self.pairs[pair_address]['wallet_token']['Wei']['value_usd']:.2f} $")
                            found_ = True
                        elif trf['from'] == pair_address:
                            self.pairs[pair_address]['wallet_token']['Wei']['value'] -= trf['value']
                            self.pairs[pair_address]['wallet_token']['Wei']['value_usd'] = self.pairs[pair_address]['wallet_token']['Wei']['value'] * tx_eth_price
                            self.print(f"      списание pair_address={pair_address} -{trf['value']} Wei ({from_wei(trf['value'], 'ether')} ETH, {round(trf['value'] * tx_eth_price, 2)} $), баланс Wei: {self.pairs[pair_address]['wallet_token']['Wei']['value_usd']:.2f} $")
                            found_ = True
                
                if trf['from'] == tx_sql['from']:
                    has_out_other = True
                if trf['to'] == tx_sql['from']:
                    has_in_other = True

                if not self.realtime_bMint:
                    if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                    if trf['to']: how_much_usd_was_gained[trf['to']] += trf['value_usd']

                # Titan Builder
                if trf['to'] == '0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97':
                    self.bribe_sent.append(float(from_wei(trf['value'], 'ether')))
                    self.print(f"Titan Builder bribe: {float(from_wei(trf['value'], 'ether'))} ETH")

                if not found_:
                    self.print(f"      \x1b[31;1m(!)\x1b[0m какой-то перевод {trf['value']} Wei ({from_wei(trf['value'], 'ether')} ETH, {round(trf['value'] / tx_eth_price, 2)} $), from={utils.print_address(trf['from'])}, to={utils.print_address(trf['to'])}")


            # здесь изменяется пул ликвидности WETH, USDC, USDT
            elif trf['address'] in [utils.WETH, utils.USDC, utils.USDT]:


                if not self.realtime_bMint:
                    # (!)
                    # вот тут надо для WETH поставить value_usd_per_token_unit = trf['value_usd_per_token_unit']
                    if trf['address'] == utils.WETH:
                        value_usd_per_token_unit = 1 / trf['value_usd_per_token_unit']
                    else:
                        value_usd_per_token_unit = trf['value_usd_per_token_unit']

                if trf['from'] == self.address:
                    self.wallet_token[trf['address']]['value'] -= trf['value']
                    if not self.realtime_bMint:
                        self.wallet_token[trf['address']]['value_usd'] = self.wallet_token[trf['address']]['value'] * value_usd_per_token_unit
                if trf['to'] == self.address:
                    self.wallet_token[trf['address']]['value'] += trf['value']
                    if not self.realtime_bMint:
                        self.wallet_token[trf['address']]['value_usd'] = self.wallet_token[trf['address']]['value'] * value_usd_per_token_unit
                
                if trf['from'] == tx_sql['from']:
                    has_out_other = True
                if trf['to'] == tx_sql['from']:
                    has_in_other = True
                
                if not self.realtime_bMint:
                    if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                    if trf['to']: how_much_usd_was_gained[trf['to']] += trf['value_usd']

                for pair_address in self.pairs.keys():
                    if trf['from'] == pair_address:
                        assert self.pairs[pair_address]['token1_address'] == trf['address'], f"{self.pairs[pair_address]['token1_address']}, {trf['address']}"
                        self.pairs[pair_address]['token1_liquidity'] -= trf['value']
                        self.pairs[pair_address]['token1_liquidity_usd'] = self.pairs[pair_address]['token1_liquidity'] / value_usd_per_token_unit
                    if trf['to'] == pair_address:
                        assert self.pairs[pair_address]['token1_address'] == trf['address'], f"{self.pairs[pair_address]['token1_address']}, {trf['address']}"
                        self.pairs[pair_address]['token1_liquidity'] += trf['value']
                        self.pairs[pair_address]['token1_liquidity_usd'] = self.pairs[pair_address]['token1_liquidity'] / value_usd_per_token_unit
                


            # здесь изменяется пул ликвидности токена (пары)
            elif trf["address"] == self.address:

                if p_i == 0 and not owner_added and trf['from'] == utils.ADDRESS_0X0000:
                    assert self.transaction_first_ownershiptransferred == None
                    self.transaction_first_ownershiptransferred = tx_sql

                    self.owner_addresses.append(trf['to'])
                    owner_added = True
                    self.print(f"      Transfer() (OwnershipTransferred): address={trf['to']}")

                new_item = {
                    'value': 0,
                    'value_usd': None,
                    'invested': 0,
                    'withdrew': 0,
                    'trades_buy': 0,
                    'trades_sell': 0,
                    'gas_usd': 0,  # total gas used $
                    'liquidity_first': None, # ликвидность usd в момент первой покупки
                    'realized_profit': 0, # $
                    'unrealized_profit': 0, # $
                    'pnl': 0, # $
                    'roi_r': 0,
                    'roi_u': 0,
                    'roi_t': 0,
                    'trades_list': [], # нужно для вычисления avg_price для profits
                    'first_trade': tx_sql['block_timestamp'], # timestamp
                    'first_trade_block': tx_sql['block_number'],
                    'last_trade': tx_sql['block_timestamp'], # timestamp
                    'last_trade_block': tx_sql['block_number'],
                    'days': None, # days holding token
                    'router': None,
                }
                if trf['from'] and trf['from'] not in self.balances:
                    if not(len(self.balances) == 0 or trf['from'] in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]):
                        self.balances[trf['from']] = {'i': len(self.balances)} | copy.deepcopy(new_item)
                if trf['to'] and trf['to'] not in self.balances:
                    self.balances[trf['to']] = {'i': len(self.balances)} | copy.deepcopy(new_item)

                if trf['from'] == tx_sql['from']:
                    has_out_token = True
                if trf['to'] == tx_sql['from']:
                    has_in_token = True

                # if trf['value_usd_per_token_unit'] != None:
                #     if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                #     if trf['to']: how_much_usd_was_gained[trf['to']] += trf['value_usd']

                # if trf['value_usd_per_token_unit'] != None:
                #     print('trf address:', trf['address'])
                #     print('{:.20f}'.format(trf['value_usd_per_token_unit']), 'value_usd_per_token_unit')
                #     latest_eth_price_wei = utils.get_eth_price(w3=w3, block=tx_sql['block_number'])
                #     # token_price_usd = (reserves[1] / latest_eth_price_wei) / (reserves[0]/ 10 ** token_dec) # WETH
                #     pair_address = list(self.pairs.keys())[0]
                #     if pair_address in self.balances:
                #         if self.balances[pair_address]['value'] == 0:
                #             print('THIS is 0')
                #         else:
                #             token_price_usd = (self.pairs[pair_address]['token1_liquidity'] / latest_eth_price_wei) / (self.balances[pair_address]['value'] / 10 ** self.contract_data_token['token_decimals']) # WETH
                #             print('{:.20f}'.format(token_price_usd), 'token_price_usd')
                #             #exit()
                #             transaction_transfers[i]['value_usd_per_token_unit'] = token_price_usd
                #             trf = transaction_transfers[i]

                # from
                if trf['from'] and trf['from'] in self.balances:
                    self.balances[trf['from']]['value'] -= trf['value']
                    self.balances[trf['from']]['last_trade_block'] = tx_sql['block_number']
                    self.balances[trf['from']]['last_p_i'] = p_i
                    self.balances[trf['from']]['last_trade'] = tx_sql['block_timestamp']
                    if self.balances[trf['from']]['value'] < 0:
                        self.print(f"      \x1b[31;1m(!)\x1b[0m balance for {trf['from']}={self.balances[trf['from']]['value']} < 0 on address={trf['address']}")

                # to
                if trf['to']:
                    self.balances[trf['to']]['value'] += trf['value']
                    self.balances[trf['to']]['last_trade_block'] = tx_sql['block_number']
                    self.balances[trf['to']]['last_p_i'] = p_i
                    self.balances[trf['to']]['last_trade'] = tx_sql['block_timestamp']

                    if trf['to'] not in how_much_native_gained: how_much_native_gained[trf['to']] = 0
                    how_much_native_gained[trf['to']] += trf['value']
                
                if trf['value_usd_per_token_unit'] != None:
                    self.value_usd_per_token_unit = trf['value_usd_per_token_unit']
                    if trf['from'] and trf['from'] != utils.ADDRESS_0X0000: self.balances[trf['from']]['value_usd'] = trf['value_usd_per_token_unit'] / 10 ** self.contract_data_token['token_decimals'] * self.balances[trf['from']]['value']
                    if trf['to'] and trf['to'] != utils.ADDRESS_0X0000: self.balances[trf['to']]['value_usd'] = trf['value_usd_per_token_unit'] / 10 ** self.contract_data_token['token_decimals'] * self.balances[trf['to']]['value']
                
                __what_pair_was_traded = None
                if trf['from'] in list(self.pairs.keys()):
                    __what_pair_was_traded = trf['from']
                elif trf['to'] in list(self.pairs.keys()):
                    __what_pair_was_traded = trf['to']
                elif self.pairs:
                    __what_pair_was_traded = list(self.pairs.keys())[0]
                assert what_pair_was_traded == None or what_pair_was_traded == __what_pair_was_traded, what_pair_was_traded
                what_pair_was_traded = __what_pair_was_traded


            else:

                contract = w3.eth.contract(address=trf["address"], abi=utils.BASIC_ERC20_ABI)
                contract_data = utils.get_contract_data(w3, contract, block=tx_sql['block_number'])

                for pair_address in self.pairs.keys():
                    if trf["address"] in [pair_address]:

                        # from
                        if trf['from'] and trf['from'] not in self.pairs[pair_address]['balances']:
                            self.pairs[pair_address]['balances'][trf['from']] = {'i': len(self.pairs[pair_address]['balances']), 'value': 0, 'value_usd': None, 'trades_list': []}
                        self.pairs[pair_address]['balances'][trf['from']]['value'] -= trf['value']
                        if self.pairs[pair_address]['balances'][trf['from']]['value'] < 0:
                            self.print(f"      \x1b[31;1m(!)\x1b[0m balance for {trf['from']}={self.pairs[pair_address]['balances'][trf['from']]['value']} < 0 on address={trf['address']}")
                        self.pairs[pair_address]['balances'][trf['from']]['last_trade_block'] = tx_sql['block_number']

                        # to
                        if trf['to'] and trf['to'] not in self.pairs[pair_address]['balances']:
                            self.pairs[pair_address]['balances'][trf['to']] = {'i': len(self.pairs[pair_address]['balances']), 'value': 0, 'value_usd': None, 'trades_list': []}
                        self.pairs[pair_address]['balances'][trf['to']]['value'] += trf['value']
                        self.pairs[pair_address]['balances'][trf['to']]['last_trade_block'] = tx_sql['block_number']
                        
                        if trf['value_usd_per_token_unit'] != None:
                            self.pairs[pair_address]['balances'][trf['from']]['value_usd'] = trf['value_usd_per_token_unit'] / 10 ** contract_data['token_decimals'] * self.pairs[pair_address]['balances'][trf['from']]['value']
                            self.pairs[pair_address]['balances'][trf['to']]['value_usd'] = trf['value_usd_per_token_unit'] / 10 ** contract_data['token_decimals'] * self.pairs[pair_address]['balances'][trf['to']]['value']


                if trf['from'] == tx_sql['from']:
                    has_out_other = True
                if trf['to'] == tx_sql['from']:
                    has_in_other = True
                
                if trf['value_usd_per_token_unit'] != None:
                    if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                    if trf['to']: how_much_usd_was_gained[trf['to']] += trf['value_usd']
                

                # (!) здесь self.wallet_token[trf['address']]['value_usd'] может неверно считаться (пример контракта 0x2541A36BE4cD39286ED61a3E6AFC2307602489d6 V3 WETH)
                if trf['from'] == self.address:
                    self.wallet_token[trf['address']]['value'] -= trf['value']
                    if trf['value_usd_per_token_unit'] != None:
                        self.wallet_token[trf['address']]['value_usd'] = self.wallet_token[trf['address']]['value'] / trf['value_usd_per_token_unit']
                if trf['to'] == self.address:
                    self.wallet_token[trf['address']]['value'] += trf['value']
                    if trf['value_usd_per_token_unit'] != None:
                        self.wallet_token[trf['address']]['value_usd'] = self.wallet_token[trf['address']]['value'] / trf['value_usd_per_token_unit']



        # -----

        if self.verbose:
            self.print('how_much_usd_was_spent $:')
            pprint(how_much_usd_was_spent)
            self.print('how_much_usd_was_gained $:')
            pprint(how_much_usd_was_gained)


        if has_in_token and has_out_other or has_out_token and has_in_other: tx_sql['event'] = 'trade'
        elif has_in_token and not has_out_other: tx_sql['event'] = 'receive'
        elif has_out_token and not has_in_other: tx_sql['event'] = 'send'
        else: tx_sql['event'] = '-'
        self.print(f"transaction event type: {tx_sql['event']}")

        if tx_sql['from'] in how_much_usd_was_spent:
            if has_in_token:
                if how_much_usd_was_spent[tx_sql['from']] - how_much_usd_was_gained[tx_sql['from']] == 0:
                    self.print(f"\x1b[31;1m(!)\x1b[0m how_much_usd_was_spent[tx_sql['from']] - how_much_usd_was_gained[tx_sql['from']] == 0")
                    #exit("op")
                self.balances[tx_sql['from']]['invested'] += how_much_usd_was_spent[tx_sql['from']] - how_much_usd_was_gained[tx_sql['from']]
                self.balances[tx_sql['from']]['trades_buy'] += 1
        if tx_sql['from'] in how_much_usd_was_gained:
            if has_out_token:
                if how_much_usd_was_gained[tx_sql['from']] - how_much_usd_was_spent[tx_sql['from']] == 0:
                    self.print(f"\x1b[31;1m(!)\x1b[0m how_much_usd_was_gained[tx_sql['from']] - how_much_usd_was_spent[tx_sql['from']] == 0")
                    #exit('op2')
                self.balances[tx_sql['from']]['withdrew'] += how_much_usd_was_gained[tx_sql['from']] - how_much_usd_was_spent[tx_sql['from']]
                self.balances[tx_sql['from']]['trades_sell'] += 1

        if self.value_usd_per_token_unit:
            if tx_sql['from'] in self.balances:
                # reserves_token_usd_first
                if what_pair_was_traded and not self.balances[tx_sql['from']]['liquidity_first']:
                    #self.balances[tx_sql['from']]['liquidity_first'] = {'t': self.balances[what_pair_was_traded]['value_usd'], 'p': self.pairs[what_pair_was_traded]['token1_liquidity_usd']}
                    self.balances[tx_sql['from']]['liquidity_first'] = {'t': self.value_usd_per_token_unit * utils.contract_balanceOf(w3, token_address=self.address, wallet_address=what_pair_was_traded, block_identifier=tx_sql['block_number']) / 10 ** self.contract_data_token['token_decimals']}
                    if self.pairs[what_pair_was_traded]['token1_address'] == utils.WETH:
                        self.balances[tx_sql['from']]['liquidity_first']['p'] = utils.contract_balanceOf(w3, token_address=self.pairs[what_pair_was_traded]['token1_address'], wallet_address=what_pair_was_traded, block_identifier=tx_sql['block_number']) / tx_eth_price
                    else:
                        self.balances[tx_sql['from']]['liquidity_first']['p'] = utils.contract_balanceOf(w3, token_address=self.pairs[what_pair_was_traded]['token1_address'], wallet_address=what_pair_was_traded, block_identifier=tx_sql['block_number']) / 10 ** 6


                self.balances[tx_sql['from']]['gas_usd'] += tx_sql['gas_usd']

                for address, router in FILE_LABELS['ethereum']['routers'].items():
                    if address == tx_sql['to']:
                        self.balances[tx_sql['from']]['router'] = router
                        break

            if what_pair_was_traded and 'token1_liquidity_usd' in self.pairs[what_pair_was_traded]:
                # если tx_sql['traded_volume'] > 0, то покупка, если < 0, то продажа
                # print('{:.25f}'.format(self.pairs[what_pair_was_traded]['token1_liquidity_usd']), "self.pairs[what_pair_was_traded]['token1_liquidity_usd']")
                # print('{:.25f}'.format(liquidities_before_trade[what_pair_was_traded]), "liquidities_before_trade[what_pair_was_traded]")
                self.print(f"liqudity of traded pair token1 change: {round(self.pairs[what_pair_was_traded]['token1_liquidity_usd'] - liquidities_before_trade[what_pair_was_traded], 2)} $ (traded pair {what_pair_was_traded})")
                # if self.traded_volume_buy == None:
                #     # если самая первая транзакция на PairCreated(), пропускаем прибавление initial ликвидности
                #     self.print('самая первая транзакция на PairCreated(), пропускаем прибавление initial ликвидности')
                #     self.traded_volume_buy = 0
                if not minted:
                    # 'traded_volume' здесь > 0, если покупка, и < 0, если продажа
                    tx_sql['traded_volume'] = self.pairs[what_pair_was_traded]['token1_liquidity_usd'] - liquidities_before_trade[what_pair_was_traded]
                    if tx_sql['traded_volume'] > 0:
                        self.traded_volume_buy += tx_sql['traded_volume']
                        self.trades_buy += 1
                    else:
                        self.traded_volume_sell += abs(tx_sql['traded_volume'])
                        self.trades_sell += 1
                    self.print(f"В этой транзакции traded_volume={tx_sql['traded_volume']:.2f} $ (> 0 покупка, < 0 продажа)")
                    self.print(f"trades_buy: {self.trades_buy}, traded_volume_buy total: {round(self.traded_volume_buy, 2)} $")
                    self.print(f"trades_sell: {self.trades_sell}, traded_volume_sell total: {round(self.traded_volume_sell, 2)} $")
            

        if p_i > 0:
            if not len(self.owner_addresses):
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} Unknown owner confirmation for address {self.address}")
                raise Exception("Unknown owner confirmation")

        # 559803 gas
        # 8708652488 gasPrice
        # 13681421293 maxFeePerGas
        # 27231195 maxPriorityFeePerGas
        # 7671241 cumulativeGasUsed
        # 186084 gasUsed
        # 8681421293 baseFeePerGas
        # 29989759 gasUsed block
        # 30000000 gasLimit block

        if p_i == 0:
            self.transaction_absolute_first = tx_sql
        if paircreated:
            self.transactions_paircreated.append(tx_sql)
        if minted:
            self.transactions_mint.append(tx_sql)
        
        assert not owner_added or owner_added and len(self.owner_addresses) == 1 and p_i == 0 or owner_added and len(self.owner_addresses) > 1


        if minted:
            if not self.how_many_holders_before_first_mint:
                self.how_many_holders_before_first_mint = len(set(self.balances.keys()) - set([self.address]) - set(self.pairs.keys()))
                self.print(f"how_many_holders_before_first_mint: {self.how_many_holders_before_first_mint}")
                self.holders_before_mint_values += [value['value'] for address, value in self.balances.items() if value['value'] > 0 and address not in [self.address] + list(self.pairs.keys())]
                self.how_many_perc_holders_before_first_mint = 100 * sum(self.holders_before_mint_values) / self.total_supply
                self.print(f"how_many_perc_holders_before_first_mint={self.how_many_perc_holders_before_first_mint}")

                self.holders_before_mint_percentages = []
                for i, hb in enumerate(self.holders_before_mint_values):
                    self.holders_before_mint_percentages.append(100 * hb / self.total_supply)

                # print('how_many_holders_before_first_mint:')
                # print(self.how_many_holders_before_first_mint)
                # print('holders_before_mint_percentages:')
                # print(self.holders_before_mint_percentages)


        # -----

        transactions_added = 0
        for i, tt in enumerate(transaction_transfers):


            if tt['address'] == self.address:

                avg_price = None

                if tt['to'] == tx_sql['from']:

                    if tt['value_usd_per_token_unit'] != None:

                        if tx_sql['from'] in how_much_usd_was_spent and tt['value'] != 0:
                            new_value_usd_per_token_unit = (how_much_usd_was_spent[tx_sql['from']] - how_much_usd_was_gained[tx_sql['from']]) / (tt['value'] / 10 ** self.contract_data_token['token_decimals'])
                            # print('{:.20f}'.format(tt['value_usd_per_token_unit']), "tt['value_usd_per_token_unit']")
                            # print('{:.20f}'.format(new_value_usd_per_token_unit), "new_value_usd_per_token_unit")
                        else:
                            new_value_usd_per_token_unit = tt['value_usd_per_token_unit']
                        

                        self.balances[tx_sql['from']]['trades_list'].append({
                            'q': tt['value'] / 10 ** self.contract_data_token['token_decimals'], # qnt
                            'vut': new_value_usd_per_token_unit,
                            't': tx_sql['block_timestamp'], # timestamp
                            's': 'buy', # side
                        })
                        # _sum_qnt_buy = sum([x['q'] for x in self.balances[tx_sql['from']]['trades_list'] if x['s'] == 'buy'])
                        # if _sum_qnt_buy != 0:
                        #     avg_price = sum([x['vut'] * x['q'] for x in self.balances[tx_sql['from']]['trades_list'] if x['s'] == 'buy']) / _sum_qnt_buy


                if tt['from'] == tx_sql['from']:
                    if tt['value_usd_per_token_unit'] != None:

                        if tx_sql['from'] in how_much_usd_was_gained and tt['value'] != 0:
                            new_value_usd_per_token_unit = (how_much_usd_was_gained[tx_sql['from']] - how_much_usd_was_spent[tx_sql['from']]) / (tt['value'] / 10 ** self.contract_data_token['token_decimals'])
                        else:
                            new_value_usd_per_token_unit = tt['value_usd_per_token_unit']

                        # new_value_usd_per_token_unit = tt['value_usd_per_token_unit']
                        # print('..:', new_value_usd_per_token_unit)

                        self.balances[tx_sql['from']]['trades_list'].append({
                            'q': tt['value'] / 10 ** self.contract_data_token['token_decimals'],
                            'vut': new_value_usd_per_token_unit,
                            't': tx_sql['block_timestamp'],
                            's': 'sell',
                        })
                        _sum_qnt_buy = sum([x['q'] for x in self.balances[tx_sql['from']]['trades_list'] if x['s'] == 'buy'])
                        if _sum_qnt_buy != 0:
                            avg_price = sum([x['vut'] * x['q'] for x in self.balances[tx_sql['from']]['trades_list'] if x['s'] == 'buy']) / _sum_qnt_buy
                            profit_from_current_trade = (new_value_usd_per_token_unit - avg_price) * (tt['value'] / 10 ** self.contract_data_token['token_decimals'])
                            self.balances[tx_sql['from']]['realized_profit'] += profit_from_current_trade
                            #invested_sum = sum([x['price_usd_per_unit'] * x['qnt'] for x in self.wallet[log['address']]['trades_list'] if x['side'] == 'sell'])
                        else:
                            tx_sql['critical_error'] = '_sum_qnt_buy = 0'


                if tx_sql['from'] in self.balances and tt['value_usd_per_token_unit'] != None:

                    self.update_balance_address(tx_sql['from'], tt['value_usd_per_token_unit'], block_timestamp)

                    if tt['from'] != tt['to'] and not(what_pair_was_traded == tt['from'] and self.address == tt['to']):
                        self.transactions.append(tx_sql)
                        self.print('Транзакция добавлена в self.transactions')
                        transactions_added += 1
                        if self.block_first_trade == None and tx_sql['from'] not in self.owner_addresses:
                            self.block_first_trade = tx_sql['block_number']
                            self.print(f"Случился первый трейд (блок {tx_sql['block_number']})")
                            if not self.transaction_blocks_after_mint:
                                self.transaction_blocks_after_mint.append(tx_sql['block_number'])

        # assert transactions_added <= 1, transactions_added


        if self.block_first_trade != None and tx_sql['block_number'] - self.block_first_trade <= 0 and len(how_much_native_gained) > 2:
            self.red_flags.append({'how_much_native_gained': len(how_much_native_gained)})

        self.print('Token wallet:')
        for address, value in self.wallet_token.items():
            if address == None: # Wei
                if self.wallet_token['Wei']['value'] >= 0:
                    self.print(f" Wei value: {from_wei(self.wallet_token['Wei']['value'], 'ether')} ETH, {round(self.wallet_token['Wei']['value_usd'], 2) if self.wallet_token['Wei']['value_usd'] else self.wallet_token['Wei']['value_usd']} $ (balance {from_wei(utils.get_balance(w3, self.address, block_identifier=tx_sql['block_number']), 'ether')} ETH)")
                else:
                    self.print(f" \x1b[31;1m(!)\x1b[0m Wei value: {self.wallet_token['Wei']['value']} < 0, (balance {from_wei(utils.get_balance(w3, self.address, block_identifier=tx_sql['block_number']), 'ether')} ETH)")
            else:
                self.print(f" {address} value: {value['value']}, {round(value['value_usd'], 2) if 'value_usd' in value and value['value_usd'] else 'N/A'} $")
        balance_token_native = utils.contract_balanceOf(w3, token_address=self.address, wallet_address=self.address, block_identifier=tx_sql['block_number'])
        if self.address in self.balances:
            self.print(f" liquidity={self.balances[self.address]['value']}")
            if balance_token_native == None:
                self.print(f" \x1b[31;1m(!)\x1b[0m balance_token_native=None")
            else:
                self.print((' \x1b[32;1m(+)\x1b[0m' if self.balances[self.address]['value'] == balance_token_native else '') + f" balance liquidity={balance_token_native / 10 ** self.contract_data_token['token_decimals']} ({balance_token_native})")
                if tx_sql['block_number'] != next_tx_block and next_tx_block:
                    # следующая транзакция будет в новом блоке
                    if self.balances[self.address]['value'] != balance_token_native:
                        print(f"\x1b[31;1m(!)\x1b[0m Где-то ошибка вычисления баланса на {round(100 - 100 * self.balances[self.address]['value'] / balance_token_native, 2) if balance_token_native != 0 else 100}%, изменяем на настоящий")
                        if self.realtime_bMint:
                            print('Отмена изменения, т.к. realtime_bMint')
                        else:
                            self.balances[self.address]['value'] = balance_token_native
                        # exit('Ошибка вычисления баланса')
        # balance_token1 = utils.contract_balanceOf(w3, token_address=utils.WETH, wallet_address=self.address, block_identifier=tx_sql['block_number'])
        # self.print(f" balance WETH={from_wei(balance_token1, 'ether')}")
        if self.verbose:
            self.print(self.balance_table())

        if tx_sql['block_number'] != next_tx_block and next_tx_block:
            self.print("это последняя транзакция в данном блоке")

            # пример адреса с ошибкой на самой 1-ой транзакции 0xc35bd166e286d021b14118AE8dcBf691B30A8970
            # и здесь ошибка TypeError: 'NoneType' object is not subscriptable 0x39Af68d946EcDEf73BbC1a29e10e8F2cE7AE6475
            if self.transaction_first_ownershiptransferred != None and tx_sql['block_number'] == self.transaction_first_ownershiptransferred['block_number']:
                # assert len(self.owner_addresses) == 1, self.owner_addresses
                if self.owner_addresses[0] in self.balances:
                    balance_owner = utils.contract_balanceOf(w3, token_address=self.address, wallet_address=self.owner_addresses[0], block_identifier=tx_sql['block_number'])
                    if balance_owner != self.balances[self.owner_addresses[0]]['value']:
                        print(f"owner address: {self.owner_addresses[0]}")
                        # print(f"{round(balance_owner / 10 ** contract_data['token_decimals'], 3)} balance owner decimals")
                        print(f"{balance_owner} balance_owner")
                        print(f"{self.balances[self.owner_addresses[0]]['value']} balance by self.balances")
                        msg = f"\x1b[31;1m(!)\x1b[0m значения не совпадают на {round(100-100*balance_owner/self.balances[self.owner_addresses[0]]['value'])}%, изменяем. address={self.address}"
                        print(msg)
                        # assert 0, msg
                        self.balances[self.owner_addresses[0]]['value'] = balance_owner
                    

                # пример адреса с ошибкой на самой 1-ой транзакции 0xc35bd166e286d021b14118AE8dcBf691B30A8970
                balance_address = utils.contract_balanceOf(w3, token_address=self.address, wallet_address=self.address, block_identifier=tx_sql['block_number'])
                if balance_address not in [None, 0] and self.address not in self.balances:
                    print(f"{balance_address} balance_address")
                    print(f"\x1b[31;1m(!)\x1b[0m self.address not in self.balances, but it's balance={balance_address}, address={self.address}")
                    assert 0, f"self.address not in self.balances, but it's balance={balance_address}, address={self.address}"




        for pair_address in self.pairs.keys():
            if pair_address in self.balances:
                self.print(f"pair={pair_address}, token1={self.pairs[pair_address]['token1_address']}:")
                self.print(f" liquidity={self.balances[pair_address]['value']}, liquidity_usd={round(self.balances[pair_address]['value_usd'], 2) if self.balances[pair_address]['value_usd'] else self.balances[pair_address]['value_usd']} $")
                balance_token_native = utils.contract_balanceOf(w3, token_address=self.address, wallet_address=pair_address, block_identifier=tx_sql['block_number'])
                self.print((' \x1b[32;1m(+)\x1b[0m' if self.balances[pair_address]['value'] == balance_token_native else '') + f" balance liquidity={balance_token_native}")
                self.print(f" liquidity/total_supply={round(100 * self.balances[pair_address]['value'] / self.total_supply, 2)}%")

                self.print(' -')
                assert self.pairs[pair_address]['token1_address'] in [utils.WETH, utils.USDC, utils.USDT], self.pairs[pair_address]['token1_address']
                balance_token1 = utils.contract_balanceOf(w3, token_address=self.pairs[pair_address]['token1_address'], wallet_address=pair_address, block_identifier=tx_sql['block_number'])
                self.print(f" token1 liquidity={self.pairs[pair_address]['token1_liquidity']}, token1_liquidity_usd={round(self.pairs[pair_address]['token1_liquidity_usd'], 2) if self.pairs[pair_address]['token1_liquidity_usd'] else self.pairs[pair_address]['token1_liquidity_usd']} $")
                if self.pairs[pair_address]['token1_address'] == utils.WETH:
                    _extr = f"{round(from_wei(self.pairs[pair_address]['token1_liquidity'], 'ether'), 5)} ETH"
                else:
                    _extr = f"{round(self.pairs[pair_address]['token1_liquidity'] / 10 ** 6, 2)} $"
                self.print((' \x1b[32;1m(+)\x1b[0m' if self.pairs[pair_address]['token1_liquidity'] == balance_token1 else '') + f" balance token1 liquidity={balance_token1} ({_extr})")

                # следующая транзакция будет в новом блоке или это последняя
                if tx_sql['block_number'] != next_tx_block or next_tx_block == None:
                    if self.balances[pair_address]['value'] != balance_token_native:
                        print(f"\x1b[31;1m(!)\x1b[0m Где-то ошибка вычисления баланса native для пары на {round(100 - 100 * self.balances[pair_address]['value'] / balance_token_native, 2) if balance_token_native != 0 else 100}%, изменяем на настоящий {balance_token_native}")
                        if self.realtime_bMint:
                            print('Отмена изменения, т.к. realtime_bMint')
                        else:
                            self.balances[pair_address]['value'] = balance_token_native
                        #exit('Ошибка вычисления баланса native пары')
                    if self.pairs[pair_address]['token1_liquidity'] != balance_token1:
                        print(f"\x1b[31;1m(!)\x1b[0m Где-то ошибка вычисления баланса token1 для пары на {round(100 - 100 * self.pairs[pair_address]['token1_liquidity'] / balance_token1, 2) if balance_token1 != 0 else 100}%, изменяем на настоящий {balance_token1}")
                        if self.realtime_bMint:
                            print('Отмена изменения, т.к. realtime_bMint')
                        else:
                            self.pairs[pair_address]['token1_liquidity'] = balance_token1
                        # exit('Ошибка вычисления баланса token1 пары')
                

                # (!) если позже подсчет calculate_liqudity() будет в многопотоке, то здесь важно сделать проверку на последнюю транзакцию в блоке
                if pair_address not in self.blocks_data[str(tx_sql['block_number'])]:
                    self.blocks_data[str(tx_sql['block_number'])][pair_address] = {}
    
                self.blocks_data[str(tx_sql['block_number'])][pair_address]['token1_liquidity'] = balance_token1


                if self.pairs[pair_address]['token1_address'] == utils.WETH:
                    value_native_per_token_unit = (self.pairs[pair_address]['token1_liquidity'] / 10 ** 18) / (self.balances[pair_address]['value'] / 10 ** self.contract_data_token['token_decimals'])
                    value_usd_per_token_unit = (self.pairs[pair_address]['token1_liquidity'] / tx_eth_price) / (self.balances[pair_address]['value'] / 10 ** self.contract_data_token['token_decimals'])
                else:
                    value_native_per_token_unit = (self.pairs[pair_address]['token1_liquidity'] / 10 ** 6) / (self.balances[pair_address]['value'] / 10 ** self.contract_data_token['token_decimals'])
                    value_usd_per_token_unit = value_native_per_token_unit
                
                self.blocks_data[str(tx_sql['block_number'])][pair_address]['value_native_per_token_unit'] = value_native_per_token_unit
                self.blocks_data[str(tx_sql['block_number'])][pair_address]['value_usd_per_token_unit'] = value_usd_per_token_unit
                # (!) может появиться новые пары, тогда self.initial_value_native_per_token_unit должно быть исходя из той пары, на которой for loop
                self.blocks_data[str(tx_sql['block_number'])][pair_address]['price_change'] = 100 * value_native_per_token_unit / self.initial_value_native_per_token_unit - 100 if self.initial_value_native_per_token_unit != 0 else 100
                
                self.print(' -')
                if self.initial_value_native_per_token_unit != 0:
                    utils.save_log(f" На {round(100 * value_native_per_token_unit / self.initial_value_native_per_token_unit - 100, 2)}% цена изменилась native валюте {self.pairs[pair_address]['token1_address']}", self.address, stdout=self.verbose)
                    utils.save_log(f" На {round(100 * value_usd_per_token_unit / self.initial_value_usd_per_token_unit - 100, 2)}% цена изменилась в $", self.address, stdout=self.verbose)
                else:
                    utils.save_log(f" initial_value_usd_per_token_unit=0, нельзя подсчитать изменение цены", self.address, stdout=self.verbose)
                utils.save_log(f" {self.initial_value_native_per_token_unit:.20f}=self.initial_value_native_per_token_unit", self.address, stdout=self.verbose)
                utils.save_log(f" {value_native_per_token_unit:.20f}=value_native_per_token_unit", self.address, stdout=self.verbose)
                utils.save_log(f" {value_usd_per_token_unit:.20f}=value_usd_per_token_unit", self.address, stdout=self.verbose)



                if self.verbose:
                    self.print(self.balance_pair_table(pair_address))

        self.blocks_data[str(tx_sql['block_number'])]['timestamp'] = tx_sql['block_timestamp']
        self.blocks_data[str(tx_sql['block_number'])]['transactions_count'] = len(self.transactions)
        self.blocks_data[str(tx_sql['block_number'])]['traded_volume_buy'] = self.traded_volume_buy
        self.blocks_data[str(tx_sql['block_number'])]['traded_volume_sell'] = self.traded_volume_sell
        self.blocks_data[str(tx_sql['block_number'])]['trades_buy'] = self.trades_buy
        self.blocks_data[str(tx_sql['block_number'])]['trades_sell'] = self.trades_sell
        if tx_sql['block_number'] == self.block_first_trade:
            self.blocks_data[str(tx_sql['block_number'])]['bribe_sent'] = copy.deepcopy(self.bribe_sent)
        

        return True


    def update_balances(self, tx_block, tx_eth_price, vuptu_inited=False):

        if self.w3_g_eth_block_number - tx_block < 50:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
        else:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))

        if vuptu_inited:
            value_usd_per_token_unit = self.initial_value_usd_per_token_unit
        else:
            if self.pairs[list(self.pairs.keys())[0]]['token1_address'] == utils.WETH: token1_currency = 'WETH'
            elif self.pairs[list(self.pairs.keys())[0]]['token1_address'] == utils.USDC: token1_currency = 'USDC'
            elif self.pairs[list(self.pairs.keys())[0]]['token1_address'] == utils.USDT: token1_currency = 'USDT'
            else: assert 0, f"{self.address}, {self.pairs[list(self.pairs.keys())[0]]['token1_address']}"
            print('checking price', self.address, tx_block, list(self.pairs.keys())[0], token1_currency, tx_eth_price)
            # 0xD1B89856D82F978D049116eBA8b7F9Df2f342fF3 20002461 0x76860Df4F522D29d86d1583cAAAA596C60061413 WETH 262817331888433.72
            _kwards = utils.usd_per_one_token(w3=w3, address=self.address, block=tx_block, pair_address=list(self.pairs.keys())[0], token1_currency=token1_currency, tx_eth_price=tx_eth_price)
            value_usd_per_token_unit = _kwards['value_usd_per_token_unit']
            reserves_token = _kwards['reserves_token']
            reserves_token_usd = _kwards['reserves_token_usd']
            reserves_pair = _kwards['reserves_pair']
            reserves_pair_usd = _kwards['reserves_pair_usd']
            pair_address = _kwards['pair_address']
            token1_currency = _kwards['token1_currency']
            exchange = _kwards['exchange']

            self.value_usd_per_token_unit = value_usd_per_token_unit
            print('value_usd_per_token_unit:', value_usd_per_token_unit)
        

        if tx_block == self.block_first_trade:
            print(f"self.bribe_sent: {self.bribe_sent}")
            print(f"ETH bribe sum: {sum(self.bribe_sent)}, {(sum(self.bribe_sent) / tx_eth_price):.2f} $")
        

        block_timestamp = utils.get_block_time(w3, tx_block)

        since_ = ''
        if self.block_first_trade != None:
            since_ = f"({tx_block - self.block_first_trade} since first trade, {(tx_block - self.block_first_trade)*12/60:.1f}m)"
        self.print(f"\33[105mupdate_balance()\033[0m on block={tx_block} {since_}")

        for address in self.balances.keys():

            #if address not in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:

            if self.balances[address]['value'] > 1 or self.balances[address]['value'] < 0:
                self.update_balance_address(address, value_usd_per_token_unit, block_timestamp)

            if self.realtime_bMint:
                if self.balances[address]['value'] == 0:
                    self.balances[address]['%'] = 0
                    self.balances[address]['value_usd'] = 0

        if self.verbose:
            self.print(self.balance_table())



    def update_balance_address(self, address, value_usd_per_token_unit, block_timestamp):

        if self.total_supply:
            self.balances[address]['%'] = 100 * self.balances[address]['value'] / self.total_supply

        if not value_usd_per_token_unit:
            return

        self.balances[address]['value_usd'] = value_usd_per_token_unit * self.balances[address]['value'] / 10 ** self.contract_data_token['token_decimals']

        avg_price = None
        _sum_qnt_buy = sum([x['q'] for x in self.balances[address]['trades_list'] if x['s'] == 'buy'])
        if _sum_qnt_buy != 0:
            avg_price = sum([x['vut'] * x['q'] for x in self.balances[address]['trades_list'] if x['s'] == 'buy']) / _sum_qnt_buy

        if avg_price != None:
            # profit calc https://www.tigerbrokers.nz/help/detail/16947286
            # logs.append(f"avg_price for {contract_data['token_symbol']}: {avg_price} $")
            self.balances[address]['unrealized_profit'] = (value_usd_per_token_unit - avg_price) * (self.balances[address]['value'] / 10 ** self.contract_data_token['token_decimals'])
            if self.balances[address]['invested'] != 0:
                self.balances[address]['roi_r'] = (self.balances[address]['invested'] + self.balances[address]['realized_profit']) / (self.balances[address]['invested']) * 100 - 100
                self.balances[address]['roi_u'] = (self.balances[address]['invested'] + self.balances[address]['unrealized_profit']) / (self.balances[address]['invested']) * 100 - 100
                self.balances[address]['roi_t'] = (self.balances[address]['invested'] + self.balances[address]['realized_profit'] + self.balances[address]['unrealized_profit']) / (self.balances[address]['invested']) * 100 - 100
            else:
                self.balances[address]['roi_r'] = 0
                self.balances[address]['roi_u'] = 0
                self.balances[address]['roi_t'] = 0
            self.balances[address]['pnl'] = self.balances[address]['realized_profit'] + self.balances[address]['unrealized_profit']


        #if not v['sell_trades'] or value['value'] != 0: value['sell_trades'].append(utils.get_block_time(w3, self.last_block))
        days_ = []
        sell_trades = [x['t'] for x in self.balances[address]['trades_list'] if x['s'] == 'sell']
        if not sell_trades or self.balances[address]['value'] > 1: sell_trades.append(block_timestamp)
        for st in sell_trades:
            days_.append((datetime.fromtimestamp(st) - datetime.fromtimestamp(self.balances[address]['first_trade'])).total_seconds() / 86400)
        if days_:
            self.balances[address]['days'] = sum(days_)/len(days_)



    def update_stat_base(self, last_block, tx_eth_price):


        if self.w3_g_eth_block_number - last_block < 50:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
        else:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))


        # здесь адреса owners, pairs, native
        exclude_addresses = [self.address] + self.owner_addresses + list(self.pairs.keys()) + [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]



        stat = {}

        assert len(self.pairs) == 1, self.pairs.keys()


        if self.pairs[list(self.pairs.keys())[0]]['token1_address'] == utils.WETH:
            initial_liquidity = from_wei(self.pairs[list(self.pairs.keys())[0]]['initial_liquidity']['value'], 'ether')
        else:
            initial_liquidity = from_wei(self.pairs[list(self.pairs.keys())[0]]['initial_liquidity']['value'] / 10 ** 6 * tx_eth_price, 'ether')

        transaction_absolute_first_maxFeePerGas = self.transaction_absolute_first['maxfeepergas'] if self.transaction_absolute_first['maxfeepergas'] != None else self.transaction_absolute_first['block_basefeepergas']
        transaction_absolute_first_maxPriorityFeePerGas = self.transaction_absolute_first['maxpriorityfeepergas'] if self.transaction_absolute_first['maxpriorityfeepergas'] != None else 0
        transaction_paircreated_maxFeePerGas = self.transactions_paircreated[0]['maxfeepergas'] if self.transactions_paircreated[0]['maxfeepergas'] != None else self.transactions_paircreated[0]['block_basefeepergas']
        transaction_paircreated_maxPriorityFeePerGas = self.transactions_paircreated[0]['maxpriorityfeepergas'] if self.transactions_paircreated[0]['maxpriorityfeepergas'] != None else 0

        stat |= {
            'initial_liquidity': float(initial_liquidity), # ETH
            'initial_liquidity_usd': self.pairs[list(self.pairs.keys())[0]]['initial_liquidity']['value_usd'],
            'token1_address': self.pairs[list(self.pairs.keys())[0]]['token1_address'],
            'exchange': self.pairs[list(self.pairs.keys())[0]]['exchange'],
            'diff_paircreated_absfirst': self.transactions_paircreated[0]['p_i'] - self.transaction_absolute_first['p_i'],
            'diff_paircreated_blockcreated': self.pairs[list(self.pairs.keys())[0]]['block_created_number'] - self.transaction_absolute_first['block_number'],
            'diff_firsttrade_minted': self.block_first_trade - self.transactions_mint[0]['block_number'] if self.block_first_trade else 0,
            'diff_minted_paircreated': self.transactions_mint[0]['block_number'] - self.pairs[list(self.pairs.keys())[0]]['block_created_number'],
            'how_many_holders_bfm': self.how_many_holders_before_first_mint,
            'how_many_perc_holders_bfm': self.how_many_perc_holders_before_first_mint,
            'holders_before_mint_percentages_max': max(self.holders_before_mint_percentages) if self.holders_before_mint_percentages else 0,
            'holders_before_mint_percentages_min': min(self.holders_before_mint_percentages) if self.holders_before_mint_percentages else 0,
            'holders_before_mint_percentages_median': statistics.median(self.holders_before_mint_percentages) if self.holders_before_mint_percentages else 0,
            'holders_before_mint_percentages_std': statistics.pstdev(self.holders_before_mint_percentages) if self.holders_before_mint_percentages else 0,
            'paircreated_hour': datetime.fromtimestamp(self.pairs[list(self.pairs.keys())[0]]['block_created_timestamp']).hour,
            'paircreated_weekday': datetime.fromtimestamp(self.pairs[list(self.pairs.keys())[0]]['block_created_timestamp']).weekday(),
            'paircreated_index': self.transactions_paircreated[0]['transactionindex'],
            'transaction_absolute_first_hour': datetime.fromtimestamp(self.transaction_absolute_first['block_timestamp']).hour,
            'transaction_absolute_first_weekday': datetime.fromtimestamp(self.transaction_absolute_first['block_timestamp']).weekday(),
            'transaction_absolute_first_index': self.transaction_absolute_first['transactionindex'],
            'transaction_first_mint_hour': datetime.fromtimestamp(self.transactions_mint[0]['block_timestamp']).hour,
            'transaction_first_mint_weekday': datetime.fromtimestamp(self.transactions_mint[0]['block_timestamp']).weekday(),
            'transaction_first_mint_index': self.transactions_mint[0]['transactionindex'],
            'ownership_renounce_before_paircreated': self.ownership_renounce_before_paircreated,
            'gasx_1': transaction_absolute_first_maxFeePerGas / self.transaction_absolute_first['block_basefeepergas'],
            'gasx_2': transaction_absolute_first_maxPriorityFeePerGas / self.transaction_absolute_first['block_basefeepergas'],
            'gasx_3': transaction_absolute_first_maxFeePerGas / transaction_absolute_first_maxPriorityFeePerGas if transaction_absolute_first_maxPriorityFeePerGas != 0 else transaction_absolute_first_maxFeePerGas,
            'gasx_4': transaction_absolute_first_maxFeePerGas / self.transaction_absolute_first['gasprice'],
            'gasx_5': transaction_absolute_first_maxFeePerGas % 1000 == 0,
            'gasx_6': transaction_absolute_first_maxPriorityFeePerGas % 1000 == 0,
            'gasx_7': self.transaction_absolute_first['gas_usd'],
            'gasx_8': self.transaction_absolute_first['block_gaslimit'] / self.transaction_absolute_first['block_gasused'],
            'gasy_1': transaction_paircreated_maxFeePerGas / self.transactions_paircreated[0]['block_basefeepergas'],
            'gasy_2': transaction_paircreated_maxPriorityFeePerGas / self.transactions_paircreated[0]['block_basefeepergas'],
            'gasy_3': transaction_paircreated_maxFeePerGas / transaction_paircreated_maxPriorityFeePerGas if transaction_paircreated_maxPriorityFeePerGas != 0 else transaction_paircreated_maxFeePerGas,
            'gasy_4': transaction_paircreated_maxFeePerGas / self.transactions_paircreated[0]['gasprice'],
            'gasy_5': transaction_paircreated_maxFeePerGas % 1000 == 0,
            'gasy_6': transaction_paircreated_maxPriorityFeePerGas % 1000 == 0,
            'gasy_7': self.transactions_paircreated[0]['gas_usd'],
            'gasy_8': self.transactions_paircreated[0]['block_gaslimit'] / self.transactions_paircreated[0]['block_gasused'],
        }

        if self.realtime_bMint:
            if stat['diff_minted_paircreated'] != 0: stat['diff_minted_paircreated'] += 1
            if stat['diff_paircreated_blockcreated'] != 0: stat['diff_paircreated_blockcreated'] += 1

        self._stat_base = stat

        return



    def update_stat_last_transactions(self, N, last_block, tx_eth_price):

        if self.w3_g_eth_block_number - last_block < 50:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
        else:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))


        stat = {}


        if '%' not in self.balances[list(self.pairs.keys())[0]]:
            print("if '%' not in self.balances[list(self.pairs.keys())[0]]", list(self.pairs.keys())[0])
            pprint(self.balances[list(self.pairs.keys())[0]])
            assert 0, list(self.pairs.keys())[0]
        for owner in list(set(self.owner_addresses) - set([utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD])):
            if owner in self.balances:
                if '%' not in self.balances[owner]:
                    print("\x1b[31;1m(!)\x1b[0m if '%' not in self.balances[owner]", f"address={self.address}, owner={owner}")
                    #assert 0, self.address

                    # (!) строку ниже надо убрать, % у owner'a должен был ранее подсчитаться
                    self.balances[owner]['%'] = 0
            else:
                self.print(f"\x1b[31;1m(!)\x1b[0m Почему-то у кошелька owner'a={owner} нет трансферов токена")
        
        if self.address in self.balances and '%' not in self.balances[self.address]:
            print(f"\x1b[31;1m(!)\x1b[0m почему-то self.balances and '%' not in self.balances[self.address], self.address={self.address}")

        stat['tokens_on_pair_perc'] = self.balances[list(self.pairs.keys())[0]]['%']
        stat['tokens_on_native_perc'] = self.balances[self.address]['%'] if self.address in self.balances and '%' in self.balances[self.address] else 0 # % сколько токенов имеет сам контракт
        stat['tokens_on_owner_perc'] = sum([self.balances[owner]['%'] for owner in self.owner_addresses if owner in self.balances]) # % сколько токенов у владельца
        
        stat['tokens_on_owner_perc_div_native'] = round(stat['tokens_on_owner_perc']) / round(stat['tokens_on_native_perc']) if round(stat['tokens_on_native_perc']) else round(stat['tokens_on_owner_perc'])
        stat['tokens_on_owner_perc_div_pair'] = round(stat['tokens_on_owner_perc']) / round(stat['tokens_on_pair_perc']) if round(stat['tokens_on_pair_perc']) else round(stat['tokens_on_owner_perc'])
        stat['tokens_on_owner_perc_div_native_n_pair'] = round(stat['tokens_on_owner_perc']) / round(stat['tokens_on_native_perc'] + stat['tokens_on_pair_perc']) if round(stat['tokens_on_native_perc'] + stat['tokens_on_pair_perc']) else round(stat['tokens_on_owner_perc']) 
        stat['tokens_on_owner_perc_n_native_div_pair'] = round(stat['tokens_on_owner_perc'] + stat['tokens_on_native_perc']) / round(stat['tokens_on_pair_perc']) if round(stat['tokens_on_pair_perc']) else round(stat['tokens_on_owner_perc'] + stat['tokens_on_native_perc'])

        stat['tokens_on_owner_perc_rel_native'] = round(stat['tokens_on_owner_perc']) / (round(stat['tokens_on_native_perc'] + stat['tokens_on_owner_perc'])) if round(stat['tokens_on_native_perc'] + stat['tokens_on_owner_perc']) != 0 else None
        stat['tokens_on_owner_perc_rel_pair'] = round(stat['tokens_on_owner_perc']) / (round(stat['tokens_on_pair_perc'] + stat['tokens_on_owner_perc'])) if round(stat['tokens_on_pair_perc'] + stat['tokens_on_owner_perc']) != 0 else None
        stat['tokens_on_owner_perc_rel_native_n_pair'] = round(stat['tokens_on_owner_perc']) / round(stat['tokens_on_native_perc'] + stat['tokens_on_pair_perc'] + stat['tokens_on_owner_perc']) if round(stat['tokens_on_native_perc'] + stat['tokens_on_pair_perc'] + stat['tokens_on_owner_perc']) != 0 else None 
        stat['tokens_on_owner_perc_n_native_rel_pair'] = round(stat['tokens_on_owner_perc'] + stat['tokens_on_native_perc']) / round(stat['tokens_on_pair_perc'] + stat['tokens_on_owner_perc'] + stat['tokens_on_native_perc']) if round(stat['tokens_on_pair_perc'] + stat['tokens_on_owner_perc'] + stat['tokens_on_native_perc']) != 0 else None
        
        stat['value_usd_pair'] = self.balances[list(self.pairs.keys())[0]]['value_usd']
        stat['value_usd_native'] = self.balances[self.address]['value_usd'] if self.address in self.balances else 0
        stat['value_usd_owner'] = sum([self.balances[owner]['value_usd'] for owner in self.owner_addresses if owner in self.balances])
        stat['owner_count'] = len(set(self.owner_addresses) - set([utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]))
        stat['ownership_burned'] = True if self.transaction_ownership_burned != None else False
        stat['ownership_burned_after_blocks'] = self.transaction_ownership_burned['block_number'] - self.transactions_mint[0]['block_number'] if self.transaction_ownership_burned != None else None
        




        # здесь адреса owners, pairs, native
        # (!) надо поправить и сделать провурку, чтобы эти адреса были в self.balances
        exclude_addresses = [self.address] + self.owner_addresses + list(self.pairs.keys()) + [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]

        if self.realtime: # или если бэктести и ф-ция текущая была вызвана не для подсчета bMint
            assert last_block == self.last_transaction_block

        # if self.pairs[list(self.pairs.keys())[0]]['token1_address'] == utils.WETH:
        #     balance_token1 = utils.contract_balanceOf(w3, token_address=utils.WETH, wallet_address=list(self.pairs.keys())[0], block_identifier=last_block)
        #     liquidity_on_first_pair_usd = balance_token1 / tx_eth_price
        # else:
        #     balance_token1 = utils.contract_balanceOf(w3, token_address=self.pairs[list(self.pairs.keys())[0]]['token1_address'], wallet_address=list(self.pairs.keys())[0], block_identifier=last_block)
        #     liquidity_on_first_pair_usd = balance_token1 / 10 ** 6


        if self.pairs[list(self.pairs.keys())[0]]['token1_address'] == utils.WETH:
            value_native_per_token_unit = (self.pairs[list(self.pairs.keys())[0]]['token1_liquidity'] / 10 ** 18) / (self.balances[list(self.pairs.keys())[0]]['value'] / 10 ** self.contract_data_token['token_decimals'])
            value_usd_per_token_unit = (self.pairs[list(self.pairs.keys())[0]]['token1_liquidity'] / tx_eth_price) / (self.balances[list(self.pairs.keys())[0]]['value'] / 10 ** self.contract_data_token['token_decimals'])
        else:
            value_native_per_token_unit = (self.pairs[list(self.pairs.keys())[0]]['token1_liquidity'] / 10 ** 6) / (self.balances[list(self.pairs.keys())[0]]['value'] / 10 ** self.contract_data_token['token_decimals'])
            value_usd_per_token_unit = value_native_per_token_unit
        
        if self.initial_value_native_per_token_unit  != 0:
            price_change_from_inited = 100 * value_native_per_token_unit / self.initial_value_native_per_token_unit - 100
        else:
            price_change_from_inited = 100

        # assert len(self.transactions) == self.trades_buy + self.trades_sell, f"{len(self.transactions)} == {self.trades_buy + self.trades_sell}"
        self.print(f"len self.transaction={len(self.transactions)}, trades={self.trades_buy + self.trades_sell}, self.trades_buy={self.trades_buy}, self.trades_sell={self.trades_sell}")

        if self.address in self.balances:
            if not self.realtime_bMint:
                assert self.balances[self.address]['value_usd'] != None, self.address

        stat |= {
            'transactions_total': self.transactions_total,
            'transactions_before_mint': len(self.transaction_blocks_before_mint),
            'transactions_per_block_before_mint': len(self.transaction_blocks_before_mint) / (self.transaction_blocks_before_mint[-1] - self.transaction_blocks_before_mint[0] + 1),
            'diff_pi_absfirst': self.p_i - self.transaction_absolute_first['p_i'],
            'diff_lastblock_absfirst': last_block - self.transaction_absolute_first['block_number'],
            'diff_lastblock_minted': last_block - self.transactions_mint[0]['block_number'],
            'diff_lastblock_paircreated': last_block - self.pairs[list(self.pairs.keys())[0]]['block_created_number'],
            'owner_roi_r': sum([self.balances[owner]['roi_r'] for owner in self.owner_addresses if owner in self.balances]),
            'owner_roi_u': sum([self.balances[owner]['roi_u'] for owner in self.owner_addresses if owner in self.balances]),
            'owner_roi_t': sum([self.balances[owner]['roi_t'] for owner in self.owner_addresses if owner in self.balances]),
            'owner_realized_profit': sum([self.balances[owner]['realized_profit'] for owner in self.owner_addresses if owner in self.balances]),
            'owner_unrealized_profit': sum([self.balances[owner]['unrealized_profit'] for owner in self.owner_addresses if owner in self.balances]),
            'owner_pnl': sum([self.balances[owner]['pnl'] for owner in self.owner_addresses if owner in self.balances]),
            'owner_value_usd': sum([self.balances[owner]['value_usd'] for owner in self.owner_addresses if owner in self.balances]),
            'owner_perc': sum([self.balances[owner]['%'] for owner in self.owner_addresses if owner in self.balances]),
            'holders_count': len(self.balances) - len(exclude_addresses), # не считая pair, native, owner
            'liquidity_on_address_usd': self.balances[self.address]['value_usd'] if self.address in self.balances else 0,
            'liquidity_on_first_pair_usd': self.balances[list(self.pairs.keys())[0]]['value_usd'],
            'liquidity_on_address_n_liquidity_on_first_pair_usd': self.balances[self.address]['value_usd'] + self.balances[list(self.pairs.keys())[0]]['value_usd'] if self.address in self.balances else 0,
            'liquidity_on_address_rel_liquidity_on_first_pair': self.balances[self.address]['value_usd'] / (self.balances[self.address]['value_usd'] + self.balances[list(self.pairs.keys())[0]]['value_usd']) if self.address in self.balances and self.balances[self.address]['value_usd'] + self.balances[list(self.pairs.keys())[0]]['value_usd'] != 0 else None,
            'price_change_from_inited': price_change_from_inited,
        }

        if self.realtime_bMint:
            stat['transactions_per_block_before_mint'] = len(self.transaction_blocks_before_mint) / (self.transaction_blocks_before_mint[-1] + 1 - self.transaction_blocks_before_mint[0] + 1)

        if self.block_first_trade:

            assert len(self.transaction_blocks_after_mint), f"{self.address}, block_first_trade={self.block_first_trade}"

            stat |= {
                'transactions_after_mint': len(self.transaction_blocks_after_mint),
                'transactions_per_block_after_mint': len(self.transaction_blocks_after_mint) / (self.transaction_blocks_after_mint[-1] - self.transaction_blocks_after_mint[0] + 1),
                'traded_volume_buy': self.traded_volume_buy, # $
                'traded_volume_sell': self.traded_volume_sell, # $
                'traded_volume': self.traded_volume_buy + self.traded_volume_sell,
                'trades_buy': self.trades_buy,
                'trades_sell': self.trades_sell,
                'trades_buy_rel_trades_sell': self.trades_buy / (self.trades_buy + self.trades_sell) if self.trades_buy + self.trades_sell != 0 else None,
                'trades_total': self.trades_buy + self.trades_sell,
                'trades_per_block': (self.trades_buy + self.trades_sell) / (last_block - self.block_first_trade + 1),
                'trades_buy_per_block': self.trades_buy / (last_block - self.block_first_trade + 1),
                'trades_sell_per_block': self.trades_sell / (last_block - self.block_first_trade + 1),
                'traded_volume_buy_per_block': self.traded_volume_buy / (last_block - self.block_first_trade + 1),
                'traded_volume_sell_per_block': self.traded_volume_sell / (last_block - self.block_first_trade + 1),
                'traded_volume_buy_div_trades_buy': self.traded_volume_buy / self.trades_buy if self.trades_buy else None,
                'traded_volume_sell_div_trades_sell': self.traded_volume_sell / self.trades_sell if self.trades_sell else None,
                'traded_volume_div_trades_total': (self.traded_volume_buy + self.traded_volume_sell) / (self.trades_buy + self.trades_sell) if (self.trades_buy + self.trades_sell) else None,
                'diff_lastblock_firsttrade': last_block - self.block_first_trade,
                'jaredfromsubway_appeared': self.jaredfromsubway_appeared,
                'mevbot_appeared_times': self.mevbot_appeared_times,
                'extra_liquidities_len': len(self.pairs[list(self.pairs.keys())[0]]['extra_liquidities']),

                'traded_volume_buy_N': 0, # $, for last N trades
                'traded_volume_sell_N': 0, # $

                # 'roi_r_sum': 0,
                # 'roi_u_sum': 0,
                # 'roi_t_sum': 0,
                # 'pnl_sum': 0,
                # 'realized_profit_sum': 0,
                # 'unrealized_profit_sum': 0,
                # 'invested_sum': 0,
                # 'withdrew_sum': 0,
                # 'value_usd_sum': 0,
                # 'gas_usd_sum': 0,
                # 'gas_1_sum': 0,
                # 'gas_2_sum': 0,
                # 'gas_3_sum': 0,
                # 'gas_4_sum': 0,
                # 'holders_afm_perc_sum': 0, # %, не считая pair, native, owner

                'banana_count': 0, # сколько было сделок banana
                'maestro_count': 0, # сколько было сделок maestro
            }


        for k in ['roi_r','roi_u','roi_t','pnl','realized_profit','unrealized_profit','invested','withdrew','value_usd','gas_usd','gas_1','gas_2','gas_3','gas_4','holders_afm_perc']:
            for k2 in ['max','min','sum','std','median']:
                stat[f"{k}_{k2}"] = 0
                if k not in ['roi_r','roi_u','roi_t','invested']:
                    stat[f"inv0_{k}_{k2}"] = 0
            stat[f"{k}_avg"] = []
            if k not in ['roi_r','roi_u','roi_t','invested']:
                stat[f"inv0_{k}_avg"] = []


        # здесь адреса owners, pairs, native
        exclude_addresses = [self.address] + self.owner_addresses + list(self.pairs.keys()) + [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]


        def make_min_max_avg_sum(a, value):
            assert value != None, f"{a} {self.address}"
            if f'{a}_max' not in stat or value > stat[f'{a}_max']:
                stat[f'{a}_max'] = value
            if f'{a}_min' not in stat or value < stat[f'{a}_min']:
                stat[f'{a}_min'] = value

            if f'{a}_avg' not in stat: stat[f'{a}_avg'] = []
            stat[f'{a}_avg'].append(value)

            if f'{a}_sum' not in stat: stat[f'{a}_sum'] = 0
            stat[f'{a}_sum'] += value


        
        if self.pairs[list(self.pairs.keys())[0]]['token1_address'] == utils.WETH:
            for el in self.pairs[list(self.pairs.keys())[0]]['extra_liquidities']:
                make_min_max_avg_sum('extra_liquidities', float(from_wei(el['value'], 'ether')))
                make_min_max_avg_sum('extra_liquidities_usd', el['value_usd'])
        else:
            for el in self.pairs[list(self.pairs.keys())[0]]['extra_liquidities']:
                make_min_max_avg_sum('extra_liquidities', float(from_wei(el['value'] / 10 ** 6 * tx_eth_price, 'ether')))
                make_min_max_avg_sum('extra_liquidities_usd', el['value_usd'])
        



        stat_blocks_trades = {}
        MAX_STAT_BLOCKS_TRADES = 7
        if self.block_first_trade and last_block - self.block_first_trade > 0:
            for i in range(1, min(MAX_STAT_BLOCKS_TRADES + 1, last_block - self.block_first_trade + 1)):
                stat_blocks_trades[str(i)] = {'trades_buy': 0, 'trades_sell': 0}
        

        # print('exclude_addresses:')
        # for ex in exclude_addresses:
        #     print(' ', ex)



        for trans in self.transactions[-N:]:
            if trans['from'] in exclude_addresses:
                continue

            bfft = trans['block_number'] - self.block_first_trade # blocks_from_first_trade
            if 'traded_volume' in trans:
                if trans['traded_volume'] > 0:
                    if bfft > 0 and str(bfft) in stat_blocks_trades:
                        stat_blocks_trades[str(bfft)]['trades_buy'] += 1
                    stat['traded_volume_buy_N'] += trans['traded_volume']
                elif trans['traded_volume'] < 0:
                    if bfft > 0 and str(bfft) in stat_blocks_trades:
                        stat_blocks_trades[str(bfft)]['trades_sell'] += 1
                    stat['traded_volume_sell_N'] += abs(trans['traded_volume'])

            if trans['to'] == '0x3328F7f4A1D1C57c35df56bBf0c9dCAFCA309C49':
                stat['banana_count'] += 1
            if trans['to'] == '0x80a64c6D7f12C47B7c66c5B4E20E72bc1FCd5d9e':
                stat['maestro_count'] += 1

            # print(f"смотрим trans[from]={address}, invested={self.balances[address]['invested']}")

            address = trans['from']

            for ii in range(2):
                if ii == 0:
                    key = ''
                    if self.balances[address]['invested'] == 0:
                        continue
                elif ii == 1:
                    key = 'inv0_'
                    if self.balances[address]['invested'] != 0:
                        continue

                # https://ethereum.stackexchange.com/questions/152311/i-found-a-transaction-that-the-maxfeepergas-is-equal-to-maxpriorityfeepergas-on
                maxFeePerGas = trans['maxfeepergas'] if trans['maxfeepergas'] != None else trans['block_basefeepergas']
                maxPriorityFeePerGas = trans['maxpriorityfeepergas'] if trans['maxpriorityfeepergas'] != None else 0
                make_min_max_avg_sum(f'{key}gas_1', maxFeePerGas / trans['block_basefeepergas'])
                make_min_max_avg_sum(f'{key}gas_2', maxPriorityFeePerGas / trans['block_basefeepergas'])
                make_min_max_avg_sum(f'{key}gas_3', maxFeePerGas / maxPriorityFeePerGas if maxPriorityFeePerGas != 0 else maxFeePerGas)
                make_min_max_avg_sum(f'{key}gas_4', maxFeePerGas / trans['gasprice'])


        holders_with_invested_0 = 0
        holders_with_invested_not_0 = 0
        # (!) на production здесь стоит взять последние N элементов
        balances_ = sorted(self.balances.items(), key=lambda d: (d[1]['last_trade_block'], d[1]['last_p_i']))
        for address, value in balances_:
            if address in exclude_addresses:
                continue

            # print(f"смотрим trans[from]={address}, invested={self.balances[address]['invested']}")

            for ii in range(2):
                if ii == 0:
                    key = ''
                    if self.balances[address]['invested'] == 0:
                        continue
                    holders_with_invested_not_0 += 1
                elif ii == 1:
                    key = 'inv0_'
                    if self.balances[address]['invested'] != 0:
                        continue
                    holders_with_invested_0 += 1

                if self.balances[address]['invested'] != 0:
                    make_min_max_avg_sum('invested', self.balances[address]['invested'])
                    # для invested=0 убираем roi, т.к. для них всегда стоит значение 0
                    make_min_max_avg_sum(f'{key}roi_r', self.balances[address]['roi_r'])
                    make_min_max_avg_sum(f'{key}roi_u', self.balances[address]['roi_u'])
                    make_min_max_avg_sum(f'{key}roi_t', self.balances[address]['roi_t'])
                make_min_max_avg_sum(f'{key}realized_profit', self.balances[address]['realized_profit'])
                make_min_max_avg_sum(f'{key}unrealized_profit', self.balances[address]['unrealized_profit'])
                make_min_max_avg_sum(f'{key}pnl', self.balances[address]['pnl'])
                #if self.balances[address]['value_usd'] >= 0:
                if self.balances[address]['value_usd'] != None:
                    make_min_max_avg_sum(f'{key}value_usd', self.balances[address]['value_usd'])
                make_min_max_avg_sum(f'{key}withdrew', abs(self.balances[address]['withdrew']))
                make_min_max_avg_sum(f'{key}gas_usd', self.balances[address]['gas_usd'])


                if '%' not in self.balances[address]:
                    pprint(self.balances[address])
                    print(address)
                    print("\x1b[31;1m(!)\x1b[0m почему-то '%' not in self.balances[address]")
                    #assert 0
                else:
                    if self.balances[address]['%'] >= 0:
                        make_min_max_avg_sum(f'{key}holders_afm_perc', self.balances[address]['%'])
            

        # если ни одного inv0_ не было, заполним нулями
        if 'inv0_holders_afm_perc_sum' not in stat:
            key = 'inv0_'
            make_min_max_avg_sum(f'{key}realized_profit', 0)
            make_min_max_avg_sum(f'{key}unrealized_profit', 0)
            make_min_max_avg_sum(f'{key}pnl', 0)
            make_min_max_avg_sum(f'{key}value_usd', 0)
            make_min_max_avg_sum(f'{key}withdrew', 0)
            make_min_max_avg_sum(f'{key}gas_usd', 0)
            make_min_max_avg_sum(f'{key}gas_1', 0)
            make_min_max_avg_sum(f'{key}gas_2', 0)
            make_min_max_avg_sum(f'{key}gas_3', 0)
            make_min_max_avg_sum(f'{key}gas_4', 0)
            make_min_max_avg_sum(f'{key}holders_afm_perc', 0)
        
        
        # подсчитываем bribe только относительно блока первого трейда
        if self.block_first_trade:
            for bribe in self.blocks_data[str(self.block_first_trade)]['bribe_sent']:
                make_min_max_avg_sum(f'bribe_eth', bribe)
            # (!) это надо раскомментить, пример с ошибкой 0x80Aa87B47Da26b2dBa301708046fe467575fC355
            # assert self.blocks_data[str(self.block_first_trade)]['trades_buy'] + self.blocks_data[str(self.block_first_trade)]['trades_sell'] != 0, self.address
            # а этот if, соответственно, убрать
            if self.blocks_data[str(self.block_first_trade)]['trades_buy'] + self.blocks_data[str(self.block_first_trade)]['trades_sell'] != 0:
                stat |= {
                    'bribe_eth_len': len(self.blocks_data[str(self.block_first_trade)]['bribe_sent']),
                    'bribe_eth_rel_trades': len(self.blocks_data[str(self.block_first_trade)]['bribe_sent']) / (self.blocks_data[str(self.block_first_trade)]['trades_buy'] + self.blocks_data[str(self.block_first_trade)]['trades_sell']),
                }



        if self.block_first_trade and last_block - self.block_first_trade > 0:
            trades_buy, trades_sell = 0, 0
            buy_blocks_strength, sell_blocks_strength = 0, 0
            for i in range(1, min(MAX_STAT_BLOCKS_TRADES + 1, last_block - self.block_first_trade + 1)):
                trades_buy += stat_blocks_trades[str(i)]['trades_buy']
                trades_sell += stat_blocks_trades[str(i)]['trades_sell']
                buy_blocks_strength += trades_buy / i
                sell_blocks_strength += trades_sell / i
                stat |= {
                    f'block{i}_trades_buy': stat_blocks_trades[str(i)]['trades_buy'],
                    f'block{i}_trades_sell': stat_blocks_trades[str(i)]['trades_sell'],

                    # (!) это надо проработать
                    f'block{i}_trades_buy_div_bfft': stat_blocks_trades[str(i)]['trades_buy'] / (trans['block_number'] - self.block_first_trade + 1), # blocks_from_first_trade
                    f'block{i}_trades_sell_div_bfft': stat_blocks_trades[str(i)]['trades_sell'] / (trans['block_number'] - self.block_first_trade + 1),
                    
                    f'block{i}_trades': stat_blocks_trades[str(i)]['trades_buy'] + stat_blocks_trades[str(i)]['trades_buy'],
                    f'block{i}_trades_buy_rel_trades_sell': stat_blocks_trades[str(i)]['trades_buy'] / (stat_blocks_trades[str(i)]['trades_buy'] + stat_blocks_trades[str(i)]['trades_sell']) if stat_blocks_trades[str(i)]['trades_buy'] + stat_blocks_trades[str(i)]['trades_sell'] != 0 else None,
                
                    f'block{i}_buy_blocks_strength': buy_blocks_strength,
                    f'block{i}_sell_blocks_strength': sell_blocks_strength,
                }


        tokens_on_pair_perc = self.balances[list(self.pairs.keys())[0]]['%']
        tokens_on_native_perc = self.balances[self.address]['%'] if self.address in self.balances and '%' in self.balances[self.address] else 0
        tokens_on_owner_perc = sum([self.balances[owner]['%'] for owner in self.owner_addresses if owner in self.balances])
        if self.block_first_trade:
            stat['holders_afm_perc_div_others_perc'] = stat['holders_afm_perc_sum'] / (tokens_on_pair_perc + tokens_on_native_perc + tokens_on_owner_perc)
            stat['banana_perc'] = stat['banana_count'] / min(N, len(self.transactions)) if self.transactions else 0
            stat['maestro_perc'] = stat['maestro_count'] / min(N, len(self.transactions)) if self.transactions else 0
            # (!) тут внизу поправить, stat['holders_afm_perc_sum'] не может быть 0. пример на 0x482702745260Ffd69FC19943f70cFFE2caCd70e9
            stat['how_many_perc_holders_bfm_rel_holders_afm_perc_sum'] = self.how_many_perc_holders_before_first_mint / (self.how_many_perc_holders_before_first_mint + stat['holders_afm_perc_sum']) if self.how_many_perc_holders_before_first_mint + stat['holders_afm_perc_sum'] != 0 else None

        for key in ['extra_liquidities','extra_liquidities_usd','bribe_eth',\
            'roi_r','roi_u','roi_t','realized_profit','unrealized_profit','pnl','invested','withdrew','value_usd','gas_usd','gas_1','gas_2','gas_3','gas_4','holders_afm_perc',\
            'inv0_realized_profit','inv0_unrealized_profit','inv0_pnl','inv0_withdrew','inv0_value_usd','inv0_gas_usd','inv0_gas_1','inv0_gas_2','inv0_gas_3','inv0_gas_4','inv0_holders_afm_perc']:
            if f'{key}_avg' in stat:
                stat[f'{key}_std'] = statistics.pstdev(stat[f'{key}_avg']) if stat[f'{key}_avg'] else 0
                stat[f'{key}_median'] = statistics.median(stat[f'{key}_avg']) if stat[f'{key}_avg'] else 0
                stat[f'{key}_avg'] = sum(stat[f'{key}_avg']) / len(stat[f'{key}_avg']) if stat[f'{key}_avg'] else 0


        if self.block_first_trade:
            stat |= {
                'inv0_holders_with_invested_0': holders_with_invested_0,
                'inv0_holders_with_invested_not_0': holders_with_invested_not_0,
                'inv0_holders_with_invested_0_rl_not_0': holders_with_invested_0 / (holders_with_invested_0 + holders_with_invested_not_0),
                'inv0_holders_afm_perc_sum_rl_holders_afm_perc_sum': stat['inv0_holders_afm_perc_sum'] / (stat['inv0_holders_afm_perc_sum'] + stat['holders_afm_perc_sum']) if stat['inv0_holders_afm_perc_sum'] + stat['holders_afm_perc_sum'] != 0 else 0,
                'inv0_holders_afm_perc_sum_div_pair_perc_plus_native_perc': stat['inv0_holders_afm_perc_sum'] / (tokens_on_pair_perc + tokens_on_native_perc),
            }


        # for trans in self.transactions[-N:]:
        #     print(trans['from'])
        # print("len(self.transactions[-N:]):", len(self.transactions[-N:]))
        # print("len(exclude_addresses):", len(exclude_addresses), exclude_addresses)


    
        self._stat_last_transactions = stat

        return


    # @staticmethod
    def update_stat_wallets(self, conn, last_block, with_owners=False, only_last_block=False):



        if self.w3_g_eth_block_number - last_block < 50:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
        else:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))



        # (!) проверить баг почему Uniswap v2 universal router (0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD) в addresses ниже
        # токен 0x3772c33fEDE7d7CD5fb587a96707736A38651D2a

        self.print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} START stat_wallets()")


        exclude_addresses = [self.address] + list(self.pairs.keys()) + [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]

        if only_last_block:
            for adr, item in self.balances.items():
                if item['first_trade_block'] != last_block:
                    self.print(f"Пропускаем address={adr} в stat_wallets(), так как first_trade_block={item['first_trade_block']} не равен last_block={last_block}")
                    exclude_addresses += [adr]


        addresses = list(set(list(set(self.balances.keys()) - set(exclude_addresses)) + list(set(self.owner_addresses) - set([utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]))))
        addresses_without_owners = list(set(addresses) - set(self.owner_addresses))

        if not with_owners:
            addresses = addresses_without_owners

        # print('owner adresses:')
        # pprint(self.owner_addresses)
        # print('all addresses:')
        # pprint(addresses)


        all_stats = {}
        # wallets_stat = {}
        # times_calculate_wallet_profit = []
        # times_calculate_wallet_live = []

        # # (!)
        #addresses = ['0xc09Ed35A4D4Da16Ed4b0B39d76a1399F97811F15']


        self.print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} len addresses = {len(addresses)}")

        # total_time_sql_get_address_transfers = 0


        try:
            with conn.cursor() as cursor:
                pass
        except psycopg2.InterfaceError as e: # connection already closed
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {type(e)}: {e}", file=sys.stderr)
            conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)



        with conn.cursor(cursor_factory=RealDictCursor) as cursor:


            try:
                # (!) здесь можно улучшить скорость, если брать stat недавно созданных в бд от других token_address
                cursor.execute("""
                    SELECT
                        address,
                        last_block,
                        stat
                    FROM stat_wallets_address
                    WHERE token_address=%s
                """, (self.address.lower(),))
                sql_stat_wallets_addresses = cursor.fetchall()
            except:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} EXCEPTION SELECT * FROM stat_wallets_address rollback(): {sys.exc_info()}")
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} EXCEPTION SELECT * FROM stat_wallets_address rollback(): {sys.exc_info()}", file=sys.stderr)
                print(traceback.format_exc(), file=sys.stderr)
                utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} EXCEPTION SELECT * FROM stat_wallets_address rollback(): {sys.exc_info()}", self.address)
                conn.rollback()
            
            if sql_stat_wallets_addresses:
                for ssw in sql_stat_wallets_addresses:
                    ssw['address'] = to_checksum_address(ssw['address'])
                    if ssw['address'] in addresses:
                        all_stats[ssw['address']] = ssw['stat']
                        addresses.remove(ssw['address'])

            # (!) для улучшения точности можно выборочно брать старые значения stat_wallets_address, если они > last_block, и заново прогонять


            params = []
            for ia, address in enumerate(addresses):
                params.append((address, last_block,))

            start_time = time.time()
            if not self.realtime and self.ia != None:
                if self.ia != None:
                    M = 2
                else:
                    M = 5
            else:
                M = min(5, max(len(addresses), 1))

            with Pool(M) as pool:
                self.print(f"{(time.time() - start_time):.3f}s initing Pool({M})")

                start_time = time.time()
                self.print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} starting pool for addresses..")
                results = pool.starmap_async(self.stat_wallets_address, params).get()
                self.print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} pool ended in {(time.time() - start_time):.3f}s")

            
            _max_ex = 0
            for i, address in enumerate(addresses):
                # (!) тут перепроверить что делать если stat_wallets_address вернул None, нужно ли это в финальной stat учитывать
                if results[i] != None:
                    #self.print(f"address executed in {results[i]['executed_in']:.3f}s")
                    # if results[i]['executed_in'] > _max_ex:
                    #     _max_ex = results[i]['executed_in']
                    if 'executed_in' in results[i]:
                        del results[i]['executed_in']
                    all_stats[address] = results[i]

                    if to_checksum_address(address) not in self.owner_addresses:
                        try:
                            cursor.execute("""
                                INSERT INTO stat_wallets_address (
                                    address,
                                    token_address,
                                    last_block,
                                    stat
                                ) VALUES %s;
                            """, ((
                                    address.lower(),
                                    self.address.lower(),
                                    last_block,
                                    json.dumps(results[i])
                            ),))
                            conn.commit()
                        except:
                            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} INSERT INTO stat_wallets_address rollback(): {sys.exc_info()}")
                            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} INSERT INTO stat_wallets_address rollback(): {sys.exc_info()}", file=sys.stderr)
                            print(traceback.format_exc(), file=sys.stderr)
                            utils.save_log(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} INSERT INTO stat_wallets_address rollback(): {sys.exc_info()}", self.address)
                            conn.rollback()
                            

            self.print(f"len addresses={len(addresses)}, max time={_max_ex:.3f}s")
            


        # (!) важно ниже то что убрать проверку если first_in_transaction_from_address равен адресу бирж

        final_stat = {}
        owners_stat = {}
        min_max_sum_avg_stats = {}

        start_time_a = time.time()
        if all_stats:
            keys_to_calc_ = list(set(all_stats[list(all_stats.keys())[0]].keys()) - set(['first_in_transaction_from_address']))
            list_first_in_transaction_from_address = []
            for address, item in all_stats.items():
                if address in self.owner_addresses:
                    for key, value in item.items():
                        if key not in keys_to_calc_: continue
                        if key not in owners_stat: owners_stat[f'o_{key}'] = []
                        owners_stat[f'o_{key}'].append(value)
                else:
                    list_first_in_transaction_from_address.append(item['first_in_transaction_from_address'])
                    for key, value in item.items():
                        if key not in keys_to_calc_: continue
                        if key not in min_max_sum_avg_stats: min_max_sum_avg_stats[key] = []
                        min_max_sum_avg_stats[key].append(value)
            
            # если owners > 1, то из всех значений берем max()
            for key, value in owners_stat.items():
                owners_stat[key] = max(value)

            for key, value in min_max_sum_avg_stats.copy().items():
                if type(value[0]) == list:
                    # удаляем если значения массива не числа (например для erc20_bought_n_not_sold_addresses)
                    del min_max_sum_avg_stats[key]
                    continue
                final_stat[f'{key}_max'] = max(value)
                if key != 'erc20_eth_received_min':
                    final_stat[f'{key}_min'] = min(value)
                final_stat[f'{key}_sum'] = sum(value)
                final_stat[f'{key}_std'] = statistics.pstdev(value)
                final_stat[f'{key}_median'] = statistics.median(value)
                final_stat[f'{key}_avg'] = statistics.mean(value)
            
            if self.verbose:
                self.print('list_first_in_transaction_from_address:')
                for adr in list_first_in_transaction_from_address:
                    self.print('-', utils.print_address(adr))
        
            final_stat['first_in_transaction_from_address_uniq'] = 100 * len(set(list_first_in_transaction_from_address)) / len(list_first_in_transaction_from_address) if list_first_in_transaction_from_address else 0
            final_stat |= owners_stat
        else:
            self.print(f'all_stats пуст на last_block={last_block}')
        
        
        self.print(f"{(time.time() - start_time_a):.3f}s all_stats")

        #print(f"{total_time_sql_get_address_transfers:.3f}s total_time_sql_get_address_transfers")

        self.print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} END stat_wallets()")

        # print('final_stat:')
        # pprint(final_stat)
        # print('owners_stat:')
        # pprint(owners_stat)



        self._stat_wallets = final_stat

        return



    def stat_wallets_address(self, address, last_block):


        if self.w3_g_eth_block_number - last_block < 50:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:{utils.GETH_PORT}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
        else:
            w3 = Web3(Web3.HTTPProvider(f"http://127.0.0.1:8545/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))



        self.print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} begin stat_wallets_address()")


        stat = utils.get_stat_wallets_address(w3=w3, address=address, last_block=last_block, verbose=0)


        self.print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} end stat_wallets_address()")

        return stat




if __name__ == '__main__':


    pass