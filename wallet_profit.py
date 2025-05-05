import os
import requests
import json
import traceback
import time
import pickle
import random
import string
from decimal import Decimal
from web3 import Web3
from web3.logs import DISCARD
from web3._utils.events import get_event_data
#from web3_input_decoder import decode_constructor, decode_function
from pprint import pprint
import statistics
# import pandas as pd
# from colorama import Fore, Style
from eth_utils import to_checksum_address, from_wei
from hexbytes import HexBytes
from datetime import datetime
from attributedict.collections import AttributeDict
from functools import cached_property
from multiprocessing.pool import ThreadPool
import copy
import tabulate
tabulate.PRESERVE_WHITESPACE = True

import psycopg2
import psycopg2.extras
from psycopg2.extras import Json, DictCursor, RealDictCursor
# psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extras.register_uuid()

import utils
from utils import FILES_DIR



class Wallet():



    def __init__(self, testnet, conn, address, limit, last_block, verbose=1, debug=0):

        self.wallet = {}
        self.wallet_transactions = []

        self.testnet = testnet
        w3 = Web3(Web3.HTTPProvider(f"{testnet}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
        if not w3.is_connected():
            exit("w3 not connected")

        self.conn = conn
        self.address = to_checksum_address(address)
        self.limit = limit
        self.last_block = last_block # last backtesting block
        self.verbose = verbose
        self.debug = debug

        # common.set_setting('autogenerate_session_id', False)

        self.wallet = {'Wei': {'value': 0, 'value_usd': 0, 'symbol': 'Wei', 'total_invested_usd': 0, 'total_gained_usd': 0, 'total_withdrawal_usd': 0, 'total_gas_usd': 0}}
        self.wallet_transactions = []
        self.wallet_table_live = []
        self.pbar = None
        self._stats = {}
        self._wallet_stats_found = None




    def create_wallet(self, contract_data):
        return {
            'value': 0,
            'value_usd': 0,
            'symbol': contract_data['token_symbol'] if 'token_symbol' in  contract_data else None,
            'token_decimals': contract_data['token_decimals'] if 'token_decimals' in  contract_data else None,
            'total_invested_usd': 0, # (не включает gas)
            'total_gained_usd': 0,
            'total_withdrawal_usd': 0,
            'trades_buy': 0,
            'trades_sell': 0,
            'realized_profit': 0, # usd
            'unrealized_profit': 0, # usd
            'roi_r': 0, # roi realized profit
            'roi_u': 0, # roi unrealized profit
            'roi_t': 0, # roi total
            'trades_list': [], # нужно для вычисления avg_price для profits
            'first_trade': None, # timestamp
            'last_trade': None, # timestamp
            'sell_trades': [], # timestamps
            'total_gas_usd': 0, # total gas used usd
            'reserves_token_usd_first': None, # ликвидность в момент покупки $
            'reserves_pair_usd_first': None,
            'reserves_token_usd_last': None, # ликвидность в LAST_BACKTEST_BLOCK $
            'reserves_pair_usd_last': None,
            'first_mint_tx_sql': None, # tx_sql транзакции первого Mint()
        }
    

    def print(self, *args, **kwargs):
        if self.verbose == 1:
            print(*args)


    @property
    def stats(self):
        return self._stats
    
    @stats.setter
    def stats(self, value):
        print('changing stats')
        self._stats = value

    
    def calculate_stats(self):
        w3 = Web3(Web3.HTTPProvider(f"{self.testnet}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))
        x = {
            'win': 0,
            'lose': 0,
            'tokens': 0,
            'incorrect_balancesof': 0,
            'roi_win_median': [],
            'roi_lose_median': [],
            'roi': 0,
            'roi_median': [],
            'realized_profit': 0,
            'unrealized_profit': 0,
            'pnl': 0,
            'pnl_median': [],
            'days_median': [],
            'token_trades_avg': [],
            'invested_usd': 0,
            'invested_usd_median': [],
            'total_withdrawal_usd': 0,
            'total_withdrawal_usd_median': [],
            'rating': [],
            'w3_trades': utils.get_transaction_count(w3, address=self.address, block_identifier=self.last_block),
        }
        for address, v in self.wallet.items():
            if address in ['Wei', utils.WETH, utils.USDT, utils.USDC]:
                continue
            if v['balanceOf'] != v['value']:
                x['incorrect_balancesof'] += 1
                continue
            if not v['reserves_token_usd_first'] or v['reserves_token_usd_first'] + v['reserves_pair_usd_first'] < 5000: # $
                continue
            if not v['reserves_token_usd_last'] or v['reserves_token_usd_last'] + v['reserves_pair_usd_last'] < 2000: # $
                continue
            if v['unrealized_profit'] > 500000:
                continue

            if v['roi_t'] > 200:
                v['roi_t'] = 200
                x['rating'].append(200)
            elif v['roi_t'] < -200:
                v['roi_t'] = -200
                x['rating'].append(-200)
            else:
                x['rating'].append(v['roi_t'])

            if v['realized_profit'] + v['unrealized_profit'] >= 0: x['win'] += 1
            if v['realized_profit'] + v['unrealized_profit'] < 0: x['lose'] += 1
            if v['roi_t'] >= 0:
                x['roi_win_median'].append(v['roi_t'])
            if v['roi_t'] < 0:
                x['roi_lose_median'].append(v['roi_t'])

            x['roi'] += v['roi_t']
            x['roi_median'].append(v['roi_t'])

            x['tokens'] += 1
            x['realized_profit'] += v['realized_profit']
            x['unrealized_profit'] += v['unrealized_profit']
            x['pnl'] += v['realized_profit'] + v['unrealized_profit']
            x['pnl_median'].append(v['realized_profit'] + v['unrealized_profit'])
            x['invested_usd'] += v['total_invested_usd']
            x['invested_usd_median'].append(v['total_invested_usd'])
            x['total_withdrawal_usd'] += v['total_withdrawal_usd']
            x['total_withdrawal_usd_median'].append(v['total_withdrawal_usd'])

            x['days_median'].append(v['days'])
            x['token_trades_avg'].append(v['trades_buy'] + v['trades_buy'])


        x['roi_win_median'] = statistics.median(x['roi_win_median']) if len(x['roi_win_median']) else 0
        x['roi_lose_median'] = statistics.median(x['roi_lose_median']) if len(x['roi_lose_median']) else 0
        x['roi_median'] = statistics.median(x['roi_median']) if len(x['roi_median']) else 0
        x['days_median'] = statistics.median(x['days_median']) if len(x['days_median']) else 0
        x['minutes_median'] = x['days_median'] * 1440
        x['pnl_median'] = statistics.median(x['pnl_median']) if len(x['pnl_median']) else 0
        x['invested_usd_median'] = statistics.median(x['invested_usd_median']) if len(x['invested_usd_median']) else 0
        x['total_withdrawal_usd_median'] = statistics.median(x['total_withdrawal_usd_median']) if len(x['total_withdrawal_usd_median']) else 0
        x['token_trades_avg'] = sum(x['token_trades_avg']) / len(x['token_trades_avg']) if len(x['token_trades_avg']) else 0
        x['win_rate'] = 100 - 100 * x['lose'] / (x['win'] + x['lose']) if x['win'] + x['lose'] != 0 else 0
        x['rating'] = sum(x['rating']) / len(x['rating']) / 2 if len(x['rating']) else 0
        perc_add_ = x['rating'] * 2 / x['days_median'] * 2 if x['days_median'] != 0 else 0
        x['rating_with_days'] = x['rating'] + x['rating'] * perc_add_ / 100

        self._stats = x
    


    def modify_wallet_transactions(self):

        modified_w = []
        for a in self.wallet_transactions:

            a = {'date': datetime.fromtimestamp(a['block_timestamp']).strftime("%d-%m-%Y %H:%M:%S")} | a

            a_c = a.copy()
            #self.print(a)


            for k in list(a.keys()):
                if k == 'event':
                    a_c['event'] = f"{a['event']}"
                    if a['standard'] == 'ERC-721': a_c['event'] += ', NFT'
                    if a['event_obj']: a_c['event'] += f" [{a['event_obj'] if len(a['event_obj']) <= 12 else a['event_obj'][:12] + '..'}]"
                if k in ['from', 'to']:
                    a_c[k] = utils.print_address(address=a[k], crop=True)
                    #a[k] = a[k][:5] + '..' + a[k][-4:]
                if k  == 'wallet':
                    output = ''
                    for adr in a['addresses_traded']:
                        if adr == 'Wei':
                            if a['wallet']['Wei']['value'] != 0:
                                output += f"{round(a['wallet']['Wei']['value']/10**18, 4)} ETH, {round(a['wallet']['Wei']['value_usd'], 2)} $"
                        else:
                            tr_contract_data = a['contract_datas'][adr]
                            if a['wallet'][adr]['value_usd'] == None:
                                value_usd_print = 'N/A $'
                            else:
                                value_usd_print = f"{round(a['wallet'][adr]['value_usd'], 2)} $"
                                #value_usd_print = "{:.2f}".format(a['wallet'][adr]['value_usd']) + ' $'
                                #value_usd_print = "{:.4g}".format(round(a['wallet'][adr]['value_usd'], 3)) + ' $'
                            if 'token_symbol' in contract_data:
                                symbol_ = tr_contract_data['token_symbol'] if len(tr_contract_data['token_symbol']) <= 6 else tr_contract_data['token_symbol'][:6] + '..'
                            else:
                                symbol_ = adr[:6]+'..'
                            if 'token_decimals' in contract_data:
                                value_ = '{:.4g}'.format(round(a['wallet'][adr]['value'] / 10 ** tr_contract_data['token_decimals'], 3))
                            else:
                                value_ = str(a['wallet'][adr]['value'])[:6]+'..'
                            output += f"{value_} {symbol_}, {value_usd_print}"
                        if a['wallet_error'] != False and a['wallet_error']['address'] == adr:
                            output += f" \x1b[31;1m(!)\x1b[0m"
                        output += '\n'
                    output = output.rstrip('\n')
                    a_c['wallet'] = output
                if k  == 'transfers':
                    output = ''
                    for t in a['transfers']:
                        if t['address'] == 'Wei' and t['value'] == 0: continue
                        if t['to'] == self.address: output += '+'
                        elif t['from'] == self.address: output += '-'
                        if t['address'] == 'Wei':
                            output += f"{round(from_wei(abs(t['value']), 'ether'), 4)} ETH, {round(t['value_usd'], 2)} $\n"
                        else:
                            if t['value_usd'] == None:
                                value_usd_print = 'N/A $'
                            else:
                                value_usd_print = f"{round(t['value_usd'], 2)} $"
                                #value_usd_print = "{:.4g}".format(round(t['value_usd'], 2)) + ' $'
                            contract_data = a['contract_datas'][t['address']]
                            if 'token_symbol' in contract_data:
                                symbol_ = contract_data['token_symbol'] if len(contract_data['token_symbol']) <= 5 else contract_data['token_symbol'][:5] + '..'
                            else:
                                symbol_ = t['address'][:6]+'..'
                            if t['value'] == None:
                                value_ = 'None'
                            elif 'token_decimals' in contract_data:
                                value_ = '{:.4g}'.format(round(t['value'] / 10 ** contract_data['token_decimals'], 3))
                            else:
                                value_ = str(t['value'])[:6]+'..'
                            output += f"{value_} {symbol_}, {value_usd_print}"
                            output += '\n'
                    output = output.rstrip('\n')
                    a_c['transfers'] = output

                    # invested
                    output = ''
                    for t in a['transfers']:
                        if t['address'] == 'Wei' and t['value'] == 0: continue
                        if t['address'] == 'Wei':
                            output += f"\n"
                        else:
                            output += f"{round(self.wallet[t['address']]['total_invested_usd'], 2)} $"
                            output += '\n'
                    output = output.rstrip('\n')
                    a_c['invested'] = output

                    # withdrew
                    output = ''
                    for t in a['transfers']:
                        if t['address'] == 'Wei' and t['value'] == 0: continue
                        if t['address'] == 'Wei':
                            output += f"\n"
                        else:
                            output += f"{round(self.wallet[t['address']]['total_withdrawal_usd'], 2)} $"
                            output += '\n'
                    output = output.rstrip('\n')
                    a_c['withdrew'] = output


                if k not in ['date','event','wallet','transfers','rlz_profit','unrlz_profit']:
                    a_c.pop(k)



            if a['status'] == 0:
                a_c['event'] = f"\x1b[31;1m{a_c['event']} (fail)\x1b[0m"
            if a['critical_error'] != False:
                a_c['event'] = f"{a_c['event']} \x1b[31;1m(!)\x1b[0m"


            if a['gas_usd'] == 0:
                a_c['gas'] = ''
            else:
                a_c['gas'] = f"{round(abs(a['gas_usd']), 2)} $"
                #a_c['gas'] = f"{round(from_wei(a['gasUsed'] * a['gasPrice'], 'ether'), 4)} ETH"
            
            # liq
            if a['status'] != 0:
                output = ''
                for t in a['transfers']:
                    if t['address'] == 'Wei': output += f"\n"
                    else:
                        if t['extra_data'] and 'rt' in t['extra_data'] and t['extra_data']['rt']:
                            if t['extra_data']['rt'] == t['extra_data']['rp']:
                                output += f"{utils.human_format(round(2 * t['extra_data']['rt'], 2))} $\n"
                            else:
                                output += f"{utils.human_format(round(t['extra_data']['rt'], 2))}/{utils.human_format(round(t['extra_data']['rp'], 2))} $\n"
                        else:
                            output += f"\n"
                output = output.rstrip('\n')
                a_c['liq'] = output
        

            for k in ['rlz_profit', 'unrlz_profit']:
                if a['status'] != 0:
                    output = ''
                    for t in a['transfers']:
                        if t['address'] == 'Wei': output += f"\n"
                        else:
                            if k == 'rlz_profit': name = 'realized_profit'
                            if k == 'unrlz_profit': name = 'unrealized_profit'
                            if a['wallet'][t['address']][name] == 0:
                                output += '0\n'
                            else:
                                output += f"{round(a['wallet'][t['address']][name], 2)} $\n"
                    output = output.rstrip('\n')
                    a_c[k] = output
            
            for k in ['roi_r', 'roi_u']:
                if a['status'] != 0:
                    output = ''
                    for t in a['transfers']:
                        if t['address'] == 'Wei': output += f"\n"
                        else: output += f"{round(a['wallet'][t['address']][k])}%\n"
                    output = output.rstrip('\n')
                    a_c[k] = output

            modified_w.append(a_c)
        return modified_w


    def calculate_wallet_profit(self):

        w3 = Web3(Web3.HTTPProvider(f"{self.testnet}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))


        self.print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} begin calculate_wallet_profit()")



        # start_time = time.time()
        # transfers_sql = utils.get_address_transfers(conn=self.conn, address=self.address, N=2000, last_block=self.last_block)
        # self.print("%.3fs get_address_transfers()" % (time.time() - start_time))


        # start_time = time.time()
        # stat_erc = utils.get_stat_wallets_address(w3=w3, transfers_sql=transfers_sql, address=self.address, last_block=self.last_block, verbose=1)
        # self.print("%.3fs get_stat_wallets_address()" % (time.time() - start_time))

        #pprint(stat_erc)


        start_time = time.time()
        transactions_tupl = utils.get_wallet_transactions_only_transfers(conn=self.conn, address=self.address, last_block=self.last_block, limit=self.limit)
        if 1 or self.verbose:
            print(f"{(time.time() - start_time):.3f}s select transactions_tupl get_wallet_transactions_only_transfers(), len(_rows): {len(transactions_tupl)}")
            print(f"first trade: {datetime.fromtimestamp(transactions_tupl[0]['block_timestamp'])}, last_trade: {datetime.fromtimestamp(transactions_tupl[-1]['block_timestamp'])}")
        self.transactions_tupl = transactions_tupl


        total_time = 0
        total_address_transfers = 0


        for p_i, tx_sql in enumerate(transactions_tupl):

            
            # if p_i == 10:
            #     exit()



            if tx_sql['to'] == None:
                tx_sql['to'] = tx_sql['contractaddress']

            tx_sql['from'] = to_checksum_address(tx_sql['from'])
            tx_sql['to'] = to_checksum_address(tx_sql['to'])
            tx_sql['value'] = int(tx_sql['value']) # Decimal convert

            tx_eth_price = tx_sql['eth_price']
            if tx_eth_price in [0, None]:
                tx_eth_price = utils.get_eth_price(w3=w3, block=tx_sql['block_number'])


            self.print(f"\x1b[32;1m [{p_i}]\x1b[0m tx_hash={tx_sql['hash']}, tx_block={tx_sql['block_number']} ({datetime.fromtimestamp(tx_sql['block_timestamp'])}), tx_index={tx_sql['transactionindex']}")
            self.print(f"     from: {utils.print_address(tx_sql['from'])}, to: {utils.print_address(tx_sql['to'])}, value: {tx_sql['value']}, gas: {round(tx_sql['gasused'] * tx_sql['gasprice'] / tx_eth_price, 2) if tx_sql['gasused'] != None else None} $")

            if self.verbose == 1:
                pprint(dict(tx_sql))


            # print('transaction_transfers:')
            # pprint(tx_sql['transfers_array'])
            
            transaction_transfers = tx_sql['transfers_array']
            total_address_transfers += len(transaction_transfers)

            has_in, has_out = False, False
            how_much_usd_was_spent = {} # здесь заложено сколько $ было потрачено без учета address
            how_much_usd_was_gained = {} # здесь заложено сколько $ было получено без учета address
            how_much_from_spent_native, how_much_from_gained_native = 0, 0 # значение в value токена
            how_much_token_spent, how_much_token_gained = {}, {} # key - адрес кошелька, value - dict из key - токен, value - value
            this_trans_value_usd_per_token_unit = {} # key - токен, value - value
            trans_from_n_to_addresses = []
            tokens_received, tokens_sent = [], []
            contract_datas = {}
            b_transaction = {
                'blockNumber': tx_sql['block_number'],
                'block_timestamp': tx_sql['block_timestamp'],
                'from': tx_sql['from'],
                'to': tx_sql['to'],
                'value': tx_sql['value'],
                'status': tx_sql['status'],
                'gas_usd': float(tx_sql['gasused'] * tx_sql['gasprice'] / tx_eth_price) if tx_sql['from'] == self.address else 0,
                'standard': None,
                'event_obj': None,
                'critical_error': False,
                'wallet_error': False,
                'addresses_traded': [],
            }

            for i, trf in enumerate(transaction_transfers):
                if transaction_transfers[i]['address'] != None:
                    transaction_transfers[i]['address'] = to_checksum_address(transaction_transfers[i]['address'])
                if transaction_transfers[i]['from'] != None:
                    transaction_transfers[i]['from'] = to_checksum_address(transaction_transfers[i]['from'])
                if transaction_transfers[i]['to'] != None:
                    transaction_transfers[i]['to'] = to_checksum_address(transaction_transfers[i]['to'])
                trf = transaction_transfers[i]

                if trf['from'] not in trans_from_n_to_addresses:
                    trans_from_n_to_addresses.append(trf['from'])
                if trf['to'] not in trans_from_n_to_addresses:
                    trans_from_n_to_addresses.append(trf['to'])
                
                # if 'is_gas' in tt and trf['is_gas']:
                #     b_transaction['gas_usd'] = abs(trf['value_usd'])
                #     self.wallet[trf['address']]['value'] += trf['value']
                #     continue


                if trf['address'] == None: trf['address'] = 'Wei'


                # Wei failed transaction
                if trf['address'] == 'Wei' and trf['status'] == 0:
                    continue
                if trf['address'] == 'Wei' and trf['value'] == 0:
                    continue

                if trf['address'] not in contract_datas:
                    contract_data = utils.get_contract_data(w3=w3, contract=AttributeDict({'address': trf['address']}), block=tx_sql['block_number'], debug=0)
                    contract_datas[trf['address']] = contract_data
                    # print(f"address: {trf['address']}, contract_data: {contract_data}")
                else:
                    contract_data = contract_datas[trf['address']]
                # print(f"    {trf['address']} contract_data={contract_data}")

                if trf['address'] not in self.wallet and self.address in [trf['from'], trf['to']]:
                    self.wallet[trf['address']] = self.create_wallet(contract_data)
                    if trf['extra_data']:
                        self.wallet[trf['address']]['reserves_token_usd_first'] = trf['extra_data']['rt']
                        self.wallet[trf['address']]['reserves_pair_usd_first'] = trf['extra_data']['rp']


                if trf['from'] and trf['from'] not in how_much_usd_was_spent: how_much_usd_was_spent[trf['from']] = 0
                if trf['from'] and trf['from'] not in how_much_usd_was_gained: how_much_usd_was_gained[trf['from']] = 0
                if trf['to'] and trf['to'] not in how_much_usd_was_spent: how_much_usd_was_spent[trf['to']] = 0
                if trf['to'] and trf['to'] not in how_much_usd_was_gained: how_much_usd_was_gained[trf['to']] = 0

                if trf['value'] == None:
                    assert trf['value_usd'] == None
                    continue

                if trf['from'] == self.address:
                    trade_sign = '-'
                    self.wallet[trf['address']]['value'] -= trf['value']
                if trf['to'] == self.address:
                    trade_sign = '+'
                    self.wallet[trf['address']]['value'] += trf['value']
                if trf['value_usd']:
                    self.wallet[trf['address']]['value_usd'] = self.wallet[trf['address']]['value'] * trf['value_usd_per_token_unit']


                if trf['value_usd']:
                    value_usd_print = f"{round(trf['value_usd'], 2)} $"
                else:
                    value_usd_print = 'N/A $'
                value_in_dec = round(trf['value'] / 10 ** contract_data['token_decimals'], 3) if 'token_decimals' in contract_data else None
                self.print(f"      {trade_sign}{value_in_dec} {contract_data['token_symbol'] if 'token_symbol' in contract_data else None} ({trf['value']}), {value_usd_print}")


                if trf['extra_data']:
                    self.wallet[trf['address']]['reserves_token_usd_last'] = trf['extra_data']['rt']
                    self.wallet[trf['address']]['reserves_pair_usd_last'] = trf['extra_data']['rp']


                if trf['to'] == self.address:
                    has_in = True
                    if trf['address'] not in tokens_received:
                        tokens_received.append(trf['address'])
                    # if trf['value_usd']:
                    #     how_much_usd_was_gained += trf['value_usd']
                elif trf['from'] == self.address:
                    has_out = True
                    if trf['address'] not in tokens_sent:
                        tokens_sent.append(trf['address'])
                    if trf['value_usd']:
                        pass
                        # self.wallet[trf['address']]['total_withdrawal_usd'] += trf['value_usd']
                        # how_much_usd_was_spent += abs(trf['value_usd'])
                    if self.wallet[trf['address']]['value'] < 0:
                        msg = f"{trf['address']} {contract_data['token_symbol'] if 'token_symbol' in contract_data else None} wallet value {self.wallet[trf['address']]['value']} < 0"
                        self.print(f"      \x1b[31;1m(!)\x1b[0m {msg}")
                        b_transaction['wallet_error'] = {'address': trf['address'], 'error': msg}
                


                if trf['address'] not in b_transaction['addresses_traded']:
                    if trf['address'] != 'Wei':
                        if trf['to'] == self.address:
                            self.wallet[trf['address']]['trades_buy'] += 1
                        if trf['from'] == self.address:
                            self.wallet[trf['address']]['trades_sell'] += 1
                    b_transaction['addresses_traded'].append(trf['address'])


                if trf['address'] in ['Wei', utils.WETH, utils.USDC, utils.USDT]:

                    if trf['value_usd'] != None:
                        if trf['from']: how_much_usd_was_spent[trf['from']] += trf['value_usd']
                        if trf['to']: how_much_usd_was_gained[trf['to']] += trf['value_usd']



                if trf['address'] == 'Wei':
                    # Titan Builder
                    if trf['to'] == '0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97':
                        # self.bribe_sent.append(float(from_wei(trf['value'], 'ether')))
                        pass
                

                if trf['address'] not in ['Wei', utils.WETH, utils.USDC, utils.USDT]:

                    if trf['value_usd_per_token_unit'] != None:

                        if trf['address'] in this_trans_value_usd_per_token_unit:
                            assert this_trans_value_usd_per_token_unit[trf['address']] == trf['value_usd_per_token_unit'], f"{self.address}, {this_trans_value_usd_per_token_unit[trf['address']]}, {trf['value_usd_per_token_unit']}"
                        this_trans_value_usd_per_token_unit[trf['address']] = trf['value_usd_per_token_unit']

                        if trf['from'] not in how_much_token_spent: how_much_token_spent[trf['from']] = {}
                        if trf['address'] not in how_much_token_spent[trf['from']]: how_much_token_spent[trf['from']][trf['address']] = 0
                        how_much_token_spent[trf['from']][trf['address']] += trf['value']

                        if trf['to'] not in how_much_token_gained: how_much_token_gained[trf['to']] = {}
                        if trf['address'] not in how_much_token_gained[trf['to']]: how_much_token_gained[trf['to']][trf['address']] = 0
                        how_much_token_gained[trf['to']][trf['address']] += trf['value']
                    





                if trf['from'] == self.address and 'first_trade' in self.wallet[trf['address']] and self.wallet[trf['address']]['first_trade']:
                    if not self.wallet[trf['address']]['sell_trades'] or self.wallet[trf['address']]['sell_trades'][-1] != b_transaction['block_timestamp']:
                        self.wallet[trf['address']]['sell_trades'].append(b_transaction['block_timestamp'])


                if 'first_trade' in self.wallet[trf['address']] and not self.wallet[trf['address']]['first_trade']:
                    self.wallet[trf['address']]['first_trade'] = b_transaction['block_timestamp']
                                    
                self.wallet[trf['address']]['last_trade'] = b_transaction['block_timestamp']
                    

                # if trf['address'] not in ['Wei', utils.WETH, utils.USDC, utils.USDT]:
                #     if trf['value_usd_per_token_unit'] != None:

                #         if trf['to'] == self.address:
                #             self.balances[self.address]['volume_buy'] += trf['value_usd']
                #             how_much_from_gained_native += trf['value']
                #             if not is_transaction_mint:
                #                 trans_volume_buy += trf['value_usd']
                #             self.print(f"покупка токена адресом {self.address} value={trf['value'] / 10 ** self.contract_data_token['token_decimals']} на сумму {trf['value_usd']:.2f} $ (self.traded_volume_buy={self.traded_volume_buy:.2f} $)")
                #         if trf['from'] == self.address:
                #             self.balances[self.address]['volume_sell'] += trf['value_usd']
                #             how_much_from_spent_native += trf['value']
                #             if not is_transaction_mint:
                #                 trans_volume_sell += trf['value_usd']
                #             self.print(f"продажа/трансфер токена адресом {self.address} value={trf['value'] / 10 ** self.contract_data_token['token_decimals']} на сумму {trf['value_usd']:.2f} $ (self.traded_volume_sell={self.traded_volume_sell:.2f} $)")




            # -----

            
            start_time_tt = time.time()

            if self.verbose == 1:
                self.print('how_much_usd_was_spent $:')
                pprint(how_much_usd_was_spent)
                self.print('how_much_usd_was_gained $:')
                pprint(how_much_usd_was_gained)
                self.print('trans_from_n_to_addresses:', trans_from_n_to_addresses)
                self.print('how_much_token_spent (key - адрес кошелька, value - dict из erc20 и его value):')
                pprint(how_much_token_spent)
                self.print('how_much_token_gained:')
                pprint(how_much_token_gained)


            # _balance = utils.contract_balanceOf(w3, token_address=self.address, wallet_address=tx_sql['from'], block_identifier=tx_sql['block_number'])

            # print('from', utils.contract_balanceOf(w3, token_address=self.address, wallet_address=tx_sql['from'], block_identifier=tx_sql['block_number']))
            # print('to', utils.contract_balanceOf(w3, token_address=self.address, wallet_address=tx_sql['to'], block_identifier=tx_sql['block_number']))




            if has_in and has_out: b_transaction['event'] = 'trade'
            elif has_in and not has_out: b_transaction['event'] = 'receive'
            elif not has_in and has_out: b_transaction['event'] = 'send'
            else: b_transaction['event'] = '-'
            self.print(f"transaction event type: {b_transaction['event']}")


            if 'Wei' in tokens_received:
                tokens_received.remove('Wei')
            if 'Wei' in tokens_sent:
                tokens_sent.remove('Wei')

            if len(tokens_received) == 0:
                pass
            elif len(tokens_received) == 1:
                self.wallet[tokens_received[0]]['total_invested_usd'] += how_much_usd_was_spent[self.address] - how_much_usd_was_gained[self.address]
                self.wallet[tokens_received[0]]['total_gained_usd'] += how_much_usd_was_gained[self.address] - how_much_usd_was_spent[self.address]
                self.wallet[tokens_received[0]]['total_gas_usd'] += b_transaction['gas_usd']

                self.print('tokens_received[0]:', tokens_received[0])
                self.print("b_transaction['gas_usd']:", b_transaction['gas_usd'])

                if self.address in how_much_usd_was_gained:
                    if has_out:
                        if how_much_usd_was_gained[self.address] - how_much_usd_was_spent[self.address] == 0:
                            self.print(f"\x1b[31;1m(!)\x1b[0m how_much_usd_was_gained[self.address] - how_much_usd_was_spent[self.address] == 0")
                            #exit('op2')
                        self.wallet[tokens_received[0]]['total_withdrawal_usd'] += how_much_usd_was_gained[self.address] - how_much_usd_was_spent[self.address]
            else:
                # (!) здесь правильно будет по соотношениям раскинуть проданных token'ов
                self.print(f"\x1b[31;1m(!)\x1b[0m tokens_received > 1: {', '.join(tokens_received)}")
                for at in tokens_received:
                    self.wallet[at]['total_invested_usd'] += (how_much_usd_was_spent[self.address] - how_much_usd_was_gained[self.address]) / len(tokens_received)
                    self.wallet[at]['total_gained_usd'] += (how_much_usd_was_gained[self.address] - how_much_usd_was_spent[self.address]) / len(tokens_received)
                    self.wallet[at]['total_gas_usd'] += b_transaction['gas_usd'] / len(tokens_received)

                    if self.address in how_much_usd_was_gained:
                        if has_out:
                            self.wallet[at]['total_withdrawal_usd'] += (how_much_usd_was_gained[self.address] - how_much_usd_was_spent[self.address]) / len(tokens_received)



            for at in tokens_sent:
                self.wallet[at]['total_gas_usd'] += b_transaction['gas_usd'] / len(tokens_sent)



            # -----


            # (!) здесь ошибка подсчета, если в одной транзакции 2 swap'a было на 1 адрес
            # how_much_usd_was_gained здесь дублируется неверно
            # лучше убрать этот цикл и сделать как в last_trades.py
            # пример 0x8136f05c5ea109f85711740fc5faf56131b2fb22795670de8a8272517130fda3



            # (!) здесь вероятно будет ошибка подсчета, если в how_much_token_gained или spent были операции более чем с 2 токенами, тогда не понятно как между ними делить how_much_usd_gained/spent


            trf = None

            if 1:

            
                if 1:

                    avg_price = None

                    #trf['value_usd_per_token_unit'] = trf['value_usd_per_token_unit'] * 10 ** contract_data['token_decimals']

                    if self.address in how_much_token_gained:


                        for token, value in how_much_token_gained[self.address].items():

                            
                            if token not in this_trans_value_usd_per_token_unit:
                                # (!) могут быть случаи, что цена токена не определилась, а спустя пару минут на другой итерации да
                                # пример токена GPT кошелька 0xbc3928fbc651cfdc015e2edada4971a5f74d9261
                                # self.wallet[log['address']]['total_invested_usd'] = None
                                pass
                            else:

                                # подсчет цены единицы токена относительно значений сколько пришло и ушло $ за транзакцию
                                if self.address in how_much_usd_was_spent and value != 0:
                                    #assert how_much_usd_was_spent[self.address] - how_much_usd_was_gained[self.address] > 0, f"{self.address} {how_much_usd_was_spent[self.address] - how_much_usd_was_gained[self.address]}"
                                    new_value_usd_per_token_unit = (how_much_usd_was_spent[self.address] - how_much_usd_was_gained[self.address]) / value
                                    # print(f"diff $ after buy: {how_much_usd_was_spent[self.address] - how_much_usd_was_gained[self.address]}")
                                    # print('{:.20f}'.format(tt['value_usd_per_token_unit']), "tt['value_usd_per_token_unit']")
                                    # print('{:.20f}'.format(new_value_usd_per_token_unit), "new_value_usd_per_token_unit")
                                else:
                                    # assert 0, 'no1'
                                    print('!!!!!!!!!!!!!!!!!! no1')
                                    new_value_usd_per_token_unit = this_trans_value_usd_per_token_unit[token]
                                

                                # self.print(f"покупка {trf['address']}")
                                # self.print(f'{new_value_usd_per_token_unit} new_value_usd_per_token_unit')
                                # self.print(f"{trf['value_usd_per_token_unit']} trf['value_usd_per_token_unit']")

                                
                                self.wallet[token]['trades_list'].append({
                                    'qnt': value / 10 ** contract_datas[token]['token_decimals'],
                                    'price_usd_per_unit': new_value_usd_per_token_unit,
                                    'side': 'buy',
                                })
                                # self.wallet[log['address']]['total_invested_usd'] += value_usd
                                # self.wallet[log['address']]['total_gas_usd'] += transaction['gas_usd']
                                # transaction_gas_saved = True
                                _sum_qnt_buy = sum([x['qnt'] for x in self.wallet[token]['trades_list'] if x['side'] == 'buy'])
                                if _sum_qnt_buy != 0:
                                    avg_price = sum([x['price_usd_per_unit'] * x['qnt'] for x in self.wallet[token]['trades_list'] if x['side'] == 'buy']) / _sum_qnt_buy



                                if self.wallet[token]['total_invested_usd'] != 0 and avg_price != None:
                                    # profit calc https://www.tigerbrokers.nz/help/detail/16947286
                                    # logs.append(f"avg_price for {contract_data['token_symbol']}: {avg_price} $")
                                    self.wallet[token]['unrealized_profit'] = (new_value_usd_per_token_unit - avg_price) * (self.wallet[token]['value'])
                                        
                                    self.wallet[token]['roi_r'] = (self.wallet[token]['total_invested_usd'] + self.wallet[token]['realized_profit']) / (self.wallet[token]['total_invested_usd']) * 100 - 100
                                    self.wallet[token]['roi_u'] = (self.wallet[token]['total_invested_usd'] + self.wallet[token]['unrealized_profit']) / (self.wallet[token]['total_invested_usd']) * 100 - 100
                                    self.wallet[token]['roi_t'] = (self.wallet[token]['total_invested_usd'] + self.wallet[token]['realized_profit'] + self.wallet[token]['unrealized_profit']) / (self.wallet[token]['total_invested_usd']) * 100 - 100
                                else:
                                    b_transaction['critical_error'] = 'total_invested_usd=0 or avg_price=None'



                    avg_price = None

                    if self.address in how_much_token_spent:


                        for token, value in how_much_token_spent[self.address].items():

                            if token in this_trans_value_usd_per_token_unit:

                                if self.address in how_much_usd_was_gained and value != 0:
                                    # assert how_much_usd_was_gained[self.address] - how_much_usd_was_spent[self.address] > 0, how_much_usd_was_gained[self.address] - how_much_usd_was_spent[self.address]
                                    new_value_usd_per_token_unit = (how_much_usd_was_gained[self.address] - how_much_usd_was_spent[self.address]) / value
                                    # print(f"diff $ after sell: {how_much_usd_was_gained[self.address] - how_much_usd_was_spent[self.address]}")
                                else:
                                    #assert 0, 'no2'
                                    print('!!!!!!!!!!!!!!!!!! no2')
                                    new_value_usd_per_token_unit = this_trans_value_usd_per_token_unit[token]


                                self.wallet[token]['trades_list'].append({
                                    'qnt': value / 10 ** contract_datas[token]['token_decimals'],
                                    'price_usd_per_unit': new_value_usd_per_token_unit,
                                    'side': 'sell',
                                })
                                _sum_qnt_buy = sum([x['qnt'] for x in self.wallet[token]['trades_list'] if x['side'] == 'buy'])
                                if _sum_qnt_buy != 0:
                                    avg_price = sum([x['price_usd_per_unit'] * x['qnt'] for x in self.wallet[token]['trades_list'] if x['side'] == 'buy']) / _sum_qnt_buy
                                    profit_from_current_trade = (new_value_usd_per_token_unit - avg_price) * value
                                    self.print(f"profit_from_current_trade for {contract_datas[token]['token_symbol'] if 'token_symbol' in contract_datas[token] else contract_datas[token]}: {profit_from_current_trade} $")
                                    self.wallet[token]['realized_profit'] += profit_from_current_trade
                                    #invested_sum = sum([x['price_usd_per_unit'] * x['qnt'] for x in self.wallet[log['address']]['trades_list'] if x['side'] == 'sell'])
                                else:
                                    b_transaction['critical_error'] = '_sum_qnt_buy = 0'



                                if self.wallet[token]['total_invested_usd'] != 0 and avg_price != None:
                                    # profit calc https://www.tigerbrokers.nz/help/detail/16947286
                                    # logs.append(f"avg_price for {contract_data['token_symbol']}: {avg_price} $")
                                    self.wallet[token]['unrealized_profit'] = (new_value_usd_per_token_unit - avg_price) * (self.wallet[token]['value'])
                                        
                                    self.wallet[token]['roi_r'] = (self.wallet[token]['total_invested_usd'] + self.wallet[token]['realized_profit']) / (self.wallet[token]['total_invested_usd']) * 100 - 100
                                    self.wallet[token]['roi_u'] = (self.wallet[token]['total_invested_usd'] + self.wallet[token]['unrealized_profit']) / (self.wallet[token]['total_invested_usd']) * 100 - 100
                                    self.wallet[token]['roi_t'] = (self.wallet[token]['total_invested_usd'] + self.wallet[token]['realized_profit'] + self.wallet[token]['unrealized_profit']) / (self.wallet[token]['total_invested_usd']) * 100 - 100
                                else:
                                    b_transaction['critical_error'] = 'total_invested_usd=0 or avg_price=None'





            if tx_sql['from'] == self.address:
                self.wallet['Wei']['value'] -= tx_sql['gasused'] * tx_sql['gasprice']
                self.wallet['Wei']['value_usd'] /= float(tx_eth_price)


            # self.print('wallet balances:')
            # for addr, item in self.wallet.items():
            #     self.print(f"  {addr} {item['value_usd']:.2f} $, {item['value']}")


            self.wallet_transactions.append(b_transaction | {'contract_datas': contract_datas, 'transfers': transaction_transfers, 'wallet': copy.deepcopy(self.wallet)})
            #pprint(self.wallet_transactions[-1])
            if self.verbose == 1:
                modified_w = self.modify_wallet_transactions()
                print(tabulate.tabulate(modified_w, tablefmt='psql', headers='keys', stralign="right", showindex="always"))



            total_time += time.time() - start_time_tt




        print(f"total_time for p_i, tx_sql in enumerate(transactions_tupl): {total_time:.3f}s")
        print(f"total_address_transfers={total_address_transfers}")



        token_pairs = []
        start_time_p = time.time()
    
        # получение пар токенов в self.wallet, транзакций их первых Mint() для вычисления через сколько блоков после первого Mint() был трейд
        if 1:
            _addresses = list(self.wallet.keys())
            if 'Wei' in _addresses: _addresses.remove('Wei')
            if utils.WETH in _addresses: _addresses.remove(utils.WETH)
            if utils.USDC in _addresses: _addresses.remove(utils.USDC)
            if utils.USDT in _addresses: _addresses.remove(utils.USDT)

            # key - token address, value - list of pairs
            addresses_pairs = utils.get_tokens_pairs(conn=self.conn, addresses=_addresses, last_block=self.last_block, verbose=self.verbose)
            pairs_to_address = {} # key - pair address, value - token address
            for adr, pairs in addresses_pairs.items():
                token_pairs += pairs[:50] # на всякий случай обрезаем массив, если вдруг там найдено слишком много пар, иначе долго будет грузить utils.get_mints_txs()
                for pa in pairs:
                    assert pa not in pairs_to_address or pairs_to_address[pa] != adr, f"{self.address}, {pa} {pairs_to_address}"
                    pairs_to_address[pa] = adr
            

        print(f"{(time.time() - start_time_p):.3f}s получение token_pairs")
        # print('addresses_pairs:')
        # pprint(addresses_pairs)

        start_time_p = time.time()

        if token_pairs:


            start_time = time.time()
            # print(f"start get_mints_txs() for address={self.address}")
            # print(f'token_pairs (len={len(token_pairs)}, uniq={len(set(token_pairs))}):')
            # print(token_pairs)

            # для производительности эту ф-цию можно отредактировать, получая не все Mint'ы каждой пары, а только первое совпадение
            _txs_sql_mints = utils.get_mints_txs(self.conn, pairs=token_pairs, last_block=self.last_block, verbose=self.verbose)
            for tx_mint in _txs_sql_mints:
                # self.print(tx_mint['log_address'], tx_mint['block_number'], tx_mint['status'])
                if self.wallet[pairs_to_address[to_checksum_address(tx_mint['log_address'])]]['first_mint_tx_sql'] == None and tx_mint['status'] == 1:
                    tx_mint = dict(tx_mint)
                    for key, value in tx_mint.items():
                        if type(value) == Decimal: tx_mint[key] = float(tx_mint[key])
                    self.wallet[pairs_to_address[to_checksum_address(tx_mint['log_address'])]]['first_mint_tx_sql'] = tx_mint
                    # self.print(f"  saved to address={pairs_to_address[to_checksum_address(tx_mint['log_address'])]}")
            
            # print(f"ended mint in {(time.time() - start_time):.2f}s for address={self.address}")

        print(f"{(time.time() - start_time_p):.3f}s получение get_mints_txs и проход в цикле")





        self.print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} end calculate_wallet_profit()")




    def print_table_last_trade(self):

        if not self.verbose:
            return

        for address, v in self.wallet.items():
            print(f"{address} {v['symbol']} first trade: {datetime.fromtimestamp(v['first_trade']) if 'first_trade' in v and v['first_trade'] != None else None}, last trade: {datetime.fromtimestamp(v['last_trade']) if 'last_trade' in v and 'first_trade' in v and v['last_trade'] != v['first_trade'] and v['last_trade'] != None else None}")

        self.wallet_table_last_trade = []
        #pprint(wallet)
        stats = {'total_realized_profit': 0, 'total_unrealized_profit': 0}
        for address, v in self.wallet.items():
            z = v.copy()
            # if v['symbol'] == 'Wei': z['address'] = ''
            # else: z['address'] = utils.print_address(address=k, crop=True)
            for kk, vv in v.items():
                #if kk in ['value'] and v[kk] != 0: v[kk] = str(v[kk])[:8] + '...'
                if kk == 'realized_profit': stats['total_realized_profit'] += v[kk]
                if kk == 'unrealized_profit': stats['total_unrealized_profit'] += v[kk]
                if kk in ['realized_profit', 'unrealized_profit']: z[kk] = f"{round(v[kk], 2)} $"
                if kk == 'value' and v['value'] != 0:
                    if v['symbol'] == 'Wei':
                        if v['value'] >= 0:
                            z['value'] = f"{round(from_wei(v['value'], 'ether'), 3)} ETH, {round(v['value_usd'], 2)} $"
                        else:
                            z['value'] = f"-{round(from_wei(abs(v['value']), 'ether'), 3)} ETH, {round(v['value_usd'], 2)} $ \x1b[31;1m(!)\x1b[0m"
                    else:
                        if v['value_usd'] != None:
                            value_usd_print = f"{round(v['value_usd'], 2)} $"
                        else:
                            value_usd_print = f"N/A \x1b[31;1m(!)\x1b[0m"
                        if v['token_decimals'] != None:
                            z['value'] = f"{round(v['value'] / 10 ** v['token_decimals'], 3)}, {value_usd_print}"
                            if v['value'] < 0 and v['value_usd']:
                                z['value'] += " \x1b[31;1m(!)\x1b[0m"
                        else:
                            z['value'] = str(v['value'])[:6]+'.. \x1b[31;1m(no tok_dec)\x1b[0m'
                if kk == 'total_invested_usd':
                    if v[kk] != None: z[kk] = f"{round(v[kk], 2)} $"
                    else: z[kk] = 'N/A'
                if kk == 'total_gas_usd':
                    z[kk] = f"{round(v[kk], 2)} $"
                if kk in ['roi_r', 'roi_u', 'roi_t']:
                    if v[kk] == 0: z[kk] = 0
                    else: z[kk] = f"{round(v[kk])}%"
                if kk == 'symbol':
                    if v['symbol'] != None:
                        z[kk] = v['symbol'] if len(v['symbol']) <= 6 else v['symbol'][:6] + '..'
                    else:
                        z[kk] = address[:6]+'..'
                if kk not in ['value','total_invested_usd','total_gas_usd','symbol','realized_profit','unrealized_profit''pnl','roi_r','roi_u','roi_t']:
                    z.pop(kk)
            
            if address != 'Wei':
                z['trades'] = f"{v['trades_buy']}/{v['trades_sell']}"
            
            # liq
            for numb in ['first', 'last']:
                output = ''
                if address != 'Wei':
                    if f'reserves_token_usd_{numb}' in v and v[f'reserves_token_usd_{numb}']:
                        if v[f'reserves_token_usd_{numb}'] == v[f'reserves_pair_usd_{numb}'] and v[f'reserves_token_usd_{numb}'] != None:
                            output += f"{utils.human_format(round(2 * v[f'reserves_token_usd_{numb}'], 2))} $\n"
                        else:
                            output += f"{utils.human_format(round(v[f'reserves_token_usd_{numb}'], 2)) if v[f'reserves_token_usd_{numb}'] else 'N/A'}/{utils.human_format(round(v[f'reserves_pair_usd_{numb}'], 2)) if v[f'reserves_pair_usd_{numb}'] else 'N/A'} $\n"
                    else:
                        output += f"\n"
                output = output.rstrip('\n')
                if numb == 'first':
                    z['liq_f'] = output
                if numb == 'last':
                    z['liq_l'] = output

            if 'realized_profit' in v and 'unrealized_profit' in v:
                if v['realized_profit'] + v['unrealized_profit'] == 0:
                    z['pnl'] = 0
                    z['roi_r'] = 0
                    z['roi_u'] = 0
                    z['roi_t'] = 0
                else:
                    z['pnl'] = f"{round(v['realized_profit'] + v['unrealized_profit'], 2)} $"
                    # if v['total_invested'] == 0: z['roi'] = '?'
                    # else: z['roi'] = f"{round(100 * (v['value_usd'] + v['realized_profit']) / v['total_invested_usd'] - 100)} %"
            self.wallet_table_last_trade.append(z)

        print('On the moment of last token trade:')
        print(tabulate.tabulate(self.wallet_table_last_trade, tablefmt='psql', headers='keys', stralign="right"))

        print(f"Total realized profit: {round(stats['total_realized_profit'], 2)} $, unrealized profit: {round(stats['total_unrealized_profit'], 2)} $, pnl: {round(stats['total_realized_profit'] + stats['total_unrealized_profit'], 2)} $")
        print(f"ETH total invested: {round(self.wallet['Wei']['total_invested_usd'], 2)} $, withdrawal: {round(self.wallet['Wei']['total_withdrawal_usd'], 2)} $")

        total_gas_used_usd = sum([x['gas_usd'] for x in self.wallet_transactions])
        print(f"total_gas_used {round(total_gas_used_usd, 2)} $")



    def get_balance(self, w3, token, wallet):

        if token == 'Wei':
            balance_token = utils.get_balance(w3, to_checksum_address(wallet), block_identifier=self.last_block)
        else:
            try:
                balance_token = utils.contract_balanceOf(w3, token_address=token, wallet_address=wallet, block_identifier=self.last_block)
            except Exception as e:
                self.print(f"      \x1b[31;1m(!)\x1b[0m {token} error balanceOf():", e)
                balance_token = False

        return {'token': token, 'balance': balance_token}


    def calculate_wallet_live(self):
        

        if 0 or self.debug or not self._wallet_stats_found:

            w3 = Web3(Web3.HTTPProvider(f"{self.testnet}/?{''.join(random.choices(string.ascii_letters + string.digits, k=20))}"))

            self.print('Live real-time data table:')
            latest_eth_price_wei = utils.get_eth_price(w3=w3, block=self.last_block)
            self.print(f"1 ETH = {round(1 * 10**18 / latest_eth_price_wei, 6)} $, last_block={self.last_block}")
            last_block_timestamp = utils.get_block_time(w3, self.last_block)

            # balances_ = {}
            # start_time = time.time()
            # with ThreadPool(10) as pool:
            #     params = [(w3, token, self.address) for token in self.wallet.keys()]
            #     for result in pool.starmap(self.get_balance, params):
            #         balances_[result['token']] = result['balance']
            # if self.verbose:
            #     print("%.3fs collected balances_" % (time.time() - start_time))
            

            for address, v in self.wallet.items():
                #z = {}
                z = v.copy()
                if v['symbol'] != 'Wei':
                    start_time = time.time()
                    
                    _kwards = utils.usd_per_one_token(w3=w3, address=address, block=self.last_block, save=False)
                    value_usd_per_token_unit = _kwards['value_usd_per_token_unit']
                    reserves_token = _kwards['reserves_token']
                    reserves_token_usd = _kwards['reserves_token_usd']
                    reserves_pair = _kwards['reserves_pair']
                    reserves_pair_usd = _kwards['reserves_pair_usd']
                    pair_address = _kwards['pair_address']
                    token1_currency = _kwards['token1_currency']
                    exchange = _kwards['exchange']
                            
                    #value_usd_per_token_unit, reserves_token, reserves_token_usd, reserves_pair, reserves_pair_usd = 1,1,1,1,1
                    #print("%.3fs usd_per_one_token()" % (time.time() - start_time))
                    # assert reserves_token_usd == reserves_pair_usd, f"{reserves_token_usd} {reserves_pair_usd}"
                    # print(f"{v['symbol']} reserves_token_usd={reserves_token_usd}, reserves_pair_usd={reserves_pair_usd}")
                    if value_usd_per_token_unit != None:
                        value_usd_per_token_unit /= 10 ** v['token_decimals']
                else:
                    value_usd_per_token_unit, reserves_token_usd, reserves_pair_usd = None, None, None


                # ['value', 'symbol', 'balanceOf', 'invested', 'gas', 'liq_f', 'liq_l', 'r_prof', 'ur_prof', 'roi_r', 'roi_u', 'roi_t', 'days', 'trds', 'pnl']


                for kk, vv in v.items():
                    #if kk == 'address': z['address'] = utils.print_address(address=k, crop=True)

                    if kk in ['realized_profit', 'unrealized_profit']: z[kk] = f"{round(v[kk], 2)} $"

                    if kk == 'unrealized_profit' and v['symbol'] != 'Wei':
                        if v['value'] == 0 or not len(self.wallet[address]['trades_list']):
                            z['unrealized_profit'] = 0
                        else:
                            #pprint(self.wallet[address]['trades_list'])
                            _sum_qnt_buy = sum([x['qnt'] for x in self.wallet[address]['trades_list'] if x['side'] == 'buy'])
                            if value_usd_per_token_unit and _sum_qnt_buy != 0:
                                avg_price = sum([x['price_usd_per_unit'] * x['qnt'] for x in self.wallet[address]['trades_list'] if x['side'] == 'buy']) / _sum_qnt_buy
                                # print('---')
                                # print(f"address {address} {v['symbol']}")
                                # print(self.wallet[address]['value'] / 10 ** v['token_decimals'], 'value')
                                # print(value_usd_per_token_unit, 'value_usd_per_token_unit')
                                # print(avg_price, 'avg_price')
                                self.wallet[address]['unrealized_profit'] = (value_usd_per_token_unit - avg_price) * self.wallet[address]['value']
                                if self.wallet[address]['unrealized_profit'] > 1000000:
                                    z['unrealized_profit'] = f"1m+ $"
                                elif self.wallet[address]['unrealized_profit'] < -1000000:
                                    z['unrealized_profit'] = f"-1m+ $"
                                else:
                                    z['unrealized_profit'] = f"{round(self.wallet[address]['unrealized_profit'], 2)} $"
                                if self.wallet[address]['unrealized_profit'] > 500000:
                                    z['mark'] = 'red'
                            else:
                                self.print(f"      \x1b[31;1m(!)\x1b[0m value_usd_per_token_unit=None or _sum_qnt_buy=0 on {v['symbol']}")
                                z['unrealized_profit'] = 'N/A \x1b[31;1m(!)\x1b[0m'
                                z['mark'] = 'red'
                    
                    if kk == 'roi_r':
                        if v['roi_r'] == 0: z['roi_r'] = 0
                        else: z['roi_r'] = f"{round(v['roi_r'])}%"  

                    if kk == 'roi_u':
                        if v['roi_u'] == 0: z['roi_u'] = 0
                        elif v['roi_u'] <= -1000000: z['roi_u'] = '-1m+%'
                        elif v['roi_u'] >= 1000000: z['roi_u'] = '1m+%'
                        else: z['roi_u'] = f"{round(v['roi_u'])}%"

                    if kk == 'value' and v['value'] != 0:
                        if v['symbol'] == 'Wei':
                            if v['value'] >= 0:
                                z[kk] = f"{round(from_wei(v['value'], 'ether'), 3)} ETH, {round(v['value'] / latest_eth_price_wei, 2)} $"
                            else:
                                z[kk] = f"-{round(from_wei(abs(v['value']), 'ether'), 3)} ETH, {round(v['value'] / latest_eth_price_wei, 2)} $ \x1b[31;1m(!)\x1b[0m"
                                z['mark'] = 'red'
                        elif 'token_decimals' in v:
                            self.print(f"{v['symbol']}, address={address}, value_usd_per_token_unit={value_usd_per_token_unit}")
                            if value_usd_per_token_unit != None:
                                if v['token_decimals'] != None:
                                    self.wallet[address]['value_usd'] = value_usd_per_token_unit * v['value']
                                    self.print(f"\x1b[31;1m(!)\x1b[0m {address} has None token_decimals")
                                value_usd_print = f"{round(self.wallet[address]['value_usd'], 2)} $"
                            else:
                                self.wallet[address]['value_usd'] = None
                                value_usd_print = 'N/A \x1b[31;1m(!)\x1b[0m'
                                z['mark'] = 'red'
                            if v['token_decimals'] != None:
                                z[kk] = f"{round(v['value'] / 10 ** v['token_decimals'], 3)}, {value_usd_print}"
                            else:
                                z[kk] = str(v['value'])[:6]+'.. \x1b[31;1m(no tok_dec)\x1b[0m'
                                z['mark'] = 'red'

                    if kk == 'symbol':
                        if v['symbol'] != None:
                            z[kk] = v['symbol'] if len(v['symbol']) <= 6 else v['symbol'][:6] + '..'
                        else:
                            z[kk] = address[:8]+'..'
                    
                    if kk == 'total_invested_usd':
                        if v['total_invested_usd'] != None: z['invested'] = f"{round(v[kk], 2)} $"
                        else: z['invested'] = 'N/A'
                    
                    if kk == 'total_gas_usd':
                        z['gas'] = f"{round(v[kk], 2)} $"

                    if kk not in ['value','symbol','realized_profit','unrealized_profit','pnl','roi_r','roi_u','roi_t']:
                        z.pop(kk)
                

                if address != 'Wei':
                    z['trades'] = f"{v['trades_buy']}/{v['trades_sell']}"
                
                # balanceOf
                if v['symbol'] == 'Wei':
                    balance_token = utils.get_balance(w3, to_checksum_address(self.address), block_identifier=self.last_block)
                    #balance_token = balances_['Wei']
                    self.wallet[address]['balanceOf'] = balance_token
                    # z['balanceOf'] = f"{round(from_wei(balance_token, 'ether'), 3)} ETH, {round(balance_token / latest_eth_price_wei, 2)} $"
                    z['balanceOf'] = f"{round(from_wei(balance_token, 'ether'), 3)} ETH"
                else:
                    try:
                        balance_token = utils.contract_balanceOf(w3, token_address=address, wallet_address=self.address, block_identifier=self.last_block)
                    except:
                        balance_token = None

                    #balance_token = balances_[address]
                    #balance_token = 1
                    if balance_token != None:
                        # print(f"{v['symbol']} balance_token: {balance_token}")
                        self.wallet[address]['balanceOf'] = balance_token
                        if v['token_decimals'] != None:
                            z['balanceOf'] = f"{round(balance_token / 10 ** v['token_decimals'], 3)}"
                        else:
                            z['balanceOf'] = '\x1b[31;1m(no tok_dec)\x1b[0m'
                            z['mark'] = 'red'
                    else:
                        self.wallet[address]['balanceOf'] = None
                        z['balanceOf'] = '?'
                        z['mark'] = 'red'
                if balance_token != v['value']:
                    z['balanceOf'] += f" \x1b[31;1m(!)\x1b[0m"
                    z['mark'] = 'red'
                    self.print(f"(!) для {v['symbol']} {balance_token} != {v['value']}")


                # liq_f
                output = ''
                if v['symbol'] != 'Wei' and self.wallet[address]['reserves_token_usd_first']:
                    if self.wallet[address]['reserves_token_usd_first'] == self.wallet[address]['reserves_pair_usd_first']:
                        output = f"{utils.human_format(round(2 * self.wallet[address]['reserves_token_usd_first'], 2))} $"
                    else:
                        output = f"{utils.human_format(round(self.wallet[address]['reserves_token_usd_first'], 2)) if self.wallet[address]['reserves_token_usd_first'] else 'N/A'}/{utils.human_format(round(self.wallet[address]['reserves_pair_usd_first'], 2)) if self.wallet[address]['reserves_pair_usd_first'] else 'N/A'} $"
                z['liq_f'] = output

                # liq_l
                output = ''
                if v['symbol'] != 'Wei' and reserves_token_usd:
                    if reserves_token_usd == reserves_pair_usd:
                        output = f"{utils.human_format(round(2 * reserves_token_usd, 2))} $"
                    else:
                        output = f"{utils.human_format(round(reserves_token_usd, 2)) if reserves_token_usd else 'N/A'}/{utils.human_format(round(reserves_pair_usd, 2)) if reserves_pair_usd else 'N/A'} $"
                    self.wallet[address]['reserves_token_usd_last'] = reserves_token_usd
                    self.wallet[address]['reserves_pair_usd_last'] = reserves_pair_usd

                    if self.wallet[address]['reserves_token_usd_last'] + self.wallet[address]['reserves_pair_usd_last'] < 2000: # $
                        z['mark'] = 'red'
                z['liq_l'] = output

                if address in [utils.WETH, utils.USDT, utils.USDC]:
                    z['mark'] = 'red'


                if v['symbol'] != 'Wei':
                    if self.wallet[address]['total_invested_usd']:
                        # roi_u
                        self.wallet[address]['roi_u'] = (self.wallet[address]['total_invested_usd'] + self.wallet[address]['unrealized_profit']) / self.wallet[address]['total_invested_usd'] * 100 - 100
                        if self.wallet[address]['roi_u'] == 0: z['roi_u'] = 0
                        else: z['roi_u'] = f"{round(self.wallet[address]['roi_u'])}%" 

                        # roi_t
                        self.wallet[address]['roi_t'] = (self.wallet[address]['total_invested_usd'] + self.wallet[address]['realized_profit'] + self.wallet[address]['unrealized_profit']) / self.wallet[address]['total_invested_usd'] * 100 - 100
                        if self.wallet[address]['roi_t'] == 0: z['roi_t'] = 0
                        else: z['roi_t'] = f"{round(self.wallet[address]['roi_t'])}%" 

                        # pnl
                        if v['realized_profit'] + v['unrealized_profit'] == 0: z['pnl'] = 0
                        else: z['pnl'] = f"{round(v['realized_profit'] + v['unrealized_profit'], 2)} $"
                    else:
                        z['roi_u'] = 'N/A'
                        z['roi_t'] = 'N/A'
                        z['pnl'] = 'N/A'


                z['bfm'] = ''
                z['ft'] = ''

                if v['symbol'] == 'Wei':
                    z['trades'] = ''
                    z['realized_profit'] = ''
                    z['unrealized_profit'] = ''
                    z['roi_r'] = ''
                    z['roi_u'] = ''
                    z['roi_t'] = ''
                    z['days'] = ''
                    z['pnl'] = ''
                else:
                    # v['value'] > 1, т.к иногда value=1 после полной продажи остается
                    if not v['sell_trades'] or v['value'] > 1: v['sell_trades'].append(last_block_timestamp)
                    days_ = []
                    # print(f"days for {v['symbol']}, first trade: {datetime.fromtimestamp(v['first_trade'])}, v['value']: {v['value']}, len(v['sell_trades']): {len(v['sell_trades'])}")
                    if v['first_trade']:
                        for st in v['sell_trades']:
                            # print(f" sell trade: {datetime.fromtimestamp(st)}")
                            days_.append((datetime.fromtimestamp(st) - datetime.fromtimestamp(v['first_trade'])).total_seconds() / 86400)
                        # print(f" days=sum(days_)/len(days_)={sum(days_)}/{len(days_)}={sum(days_)/len(days_)}")
                        self.wallet[address]['days'] = sum(days_)/len(days_)
                        z['days'] = f"{round(sum(days_)/len(days_), 2)}"

                        if self.wallet[address]['first_mint_tx_sql']:
                            z['bfm'] = (v['first_trade'] - self.wallet[address]['first_mint_tx_sql']['block_timestamp']) // 12
                    
                        z['ft'] = datetime.fromtimestamp(v['first_trade']).strftime("%Y-%m-%d")



                self.wallet_table_live.append(z)


            # order_ = ['value','symbol','balanceOf','invested','gas','liq_f','liq_l','realized_profit','unrealized_profit','roi_r','roi_u','roi_t','days','trades','pnl','mark']
            order_ = ['value','symbol','invested','gas','balanceOf','liq_f','liq_l','trades','realized_profit','unrealized_profit','roi_r','roi_u','roi_t','days','pnl','bfm','ft','mark']
            for j in range(len(self.wallet_table_live)):
                temp_w_ = {}
                for or_ in order_:
                    if or_ in self.wallet_table_live[j]:
                        temp_w_[or_] = self.wallet_table_live[j][or_]
                self.wallet_table_live[j] = temp_w_
                if 'mark' in self.wallet_table_live[j] and self.wallet_table_live[j]['mark'] == 'red':
                    for it, vv in self.wallet_table_live[j].items():
                        # self.wallet_table_live[i][it] = f"{Fore.RED}{str(vv)}{Fore.RESET}"
                        self.wallet_table_live[j][it] = f"\x1b[31;1m{str(vv)}\x1b[0m"
                    del self.wallet_table_live[j]['mark']

            _s_wtl = []
            for wtl in self.wallet_table_live:
                #assert len(self.wallet_table_live[0].values()) == len(wtl.values()), f"{len(self.wallet_table_live[0].values())} {len(wtl.values())}"
                _s_wtl.append(list(wtl.values()))
            self.wallet_table_live = _s_wtl

            
            # calculating updated self.wallet with live data
            self.calculate_stats()

            

            if 0 and not self._wallet_stats_found:
                
                wst = self._stats



                # print(round(wst['rating'], 2))
                # print(round(wst['rating_with_days'], 2))
                # print(round(wst['roi'], 2))
                # print(round(wst['roi_median'], 2))
                # print(round(wst['roi_lose'], 2))
                # print(round(wst['roi_lose_avg'], 2))
                # print(round(wst['roi_win'], 2))
                # print(round(wst['roi_win_avg'], 2))
                # print(round(wst['token_trades_avg'], 2))

                print(
                    type(wst['days_avg']),
                    type(wst['incorrect_balancesof']),
                    type(wst['invested_usd']),
                    type(wst['invested_usd_avg']),
                    type(wst['lose']),
                    type(wst['pnl']),
                    type(wst['pnl_avg']),
                    round(wst['rating'], 2),
                    round(wst['rating_with_days'], 2),
                    type(wst['realized_profit']),
                    round(wst['roi'], 2), 
                    round(wst['roi_median'], 2),
                    round(wst['roi_lose'], 2),
                    round(wst['roi_lose_avg'], 2),
                    type(round(wst['roi_win'], 2)),
                    type(round(wst['roi_win_avg'], 2)),
                    round(wst['token_trades_avg'], 2),
                    type(wst['tokens']),
                    type(wst['unrealized_profit']),
                    type(wst['win']),
                    type(wst['win_rate']),
                    type(wst['w3_trades'])
                )

                conn = psycopg2.connect(**utils.db_config_psql)
                cursor = conn.cursor()
                cursor.execute("""insert into \"wallets\" (
                    address,
                    first_transaction_block_number,
                    first_transaction_hash,
                    last_transaction_block_number,
                    last_transaction_hash,
                    last_block,
                    wallet_table_live,
                    days_avg,
                    incorrect_balancesof,
                    invested_usd,
                    invested_usd_avg,
                    lose,
                    pnl,
                    pnl_avg,
                    rating,
                    rating_with_days,
                    realized_profit,
                    roi,
                    roi_median,
                    roi_lose,
                    roi_lose_avg,
                    roi_win,
                    roi_win_avg,
                    token_trades_avg,
                    tokens,
                    unrealized_profit,
                    win,
                    win_rate,
                    w3_trades
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
                    self.address, self.transactions_tupl[0]['block_number'], self.transactions_tupl[0]['hash'], self.transactions_tupl[-1]['block_number'], self.transactions_tupl[-1]['hash'], self.last_block, json.dumps(self.wallet_table_live).replace('\\u0000', ''), wst['days_avg'], wst['incorrect_balancesof'], wst['invested_usd'], wst['invested_usd_avg'], wst['lose'], wst['pnl'], wst['pnl_avg'], round(wst['rating'], 2), round(wst['rating_with_days'], 2), wst['realized_profit'], round(wst['roi'], 2), round(wst['roi_median'], 2), round(wst['roi_lose'], 2), round(wst['roi_lose_avg'], 2), round(wst['roi_win'], 2), round(wst['roi_win_avg'], 2), round(wst['token_trades_avg'], 2), wst['tokens'], wst['unrealized_profit'], wst['win'], wst['win_rate'], wst['w3_trades']))
                

                # cursor.execute("""UPDATE \"wallet_transactions\" SET b_transaction=%s, transfers=%s, executed_in=%s WHERE wallet_address=%s AND hash=%s""", (json.dumps(b_tra), json.dumps(transaction_transfers), time.time() - start_time_e, self.address, hash_))


                conn.commit()
                cursor.close()
                conn.close()



def main():

    # testnet = 'https://eth.merkle.io' # нет forbidden блока
    testnet = 'https://eth-pokt.nodies.app'
    #testnet = 'http://127.0.0.1:8545/?wp'

    w3 = Web3(Web3.HTTPProvider(testnet))
    if not w3.is_connected():
        raise Exception("w3 not connected")


    utils.init_workspace()



    ADDRESS = "0xd3eef0f3456f6e7e78ad140341885a6869f14ee6"



    # LAST_BACKTESTING_BLOCK = 19310000 # 26 фев
    LAST_BACKTESTING_BLOCK = w3.eth.block_number

    ADDRESS = to_checksum_address(ADDRESS)
    print('wallet:', ADDRESS)
    print('transactions by w3:', w3.eth.get_transaction_count(to_checksum_address(ADDRESS), block_identifier=LAST_BACKTESTING_BLOCK))
    print(f'LAST_BACKTESTING_BLOCK: {LAST_BACKTESTING_BLOCK}')





    with psycopg2.connect(**utils.db_config_psql) as conn:
        w = Wallet(testnet=testnet, conn=conn, address=ADDRESS, limit=500, last_block=LAST_BACKTESTING_BLOCK, verbose=0, debug=1)
        start_time_e = time.time()
        w.calculate_wallet_profit()
        print("%.3fs calculate_wallet_profit()" % (time.time() - start_time_e))

        for adr, item in w.wallet.items():
            print(f"{adr} {item['symbol']}, first trade: {datetime.fromtimestamp(w.wallet[adr]['first_trade']) if 'first_trade' in w.wallet[adr] and w.wallet[adr]['first_trade'] else '-'}")

        w.print_table_last_trade()
        start_time = time.time()
        w.calculate_wallet_live()
        #print(tabulate.tabulate(w.wallet_table_live, tablefmt='psql', headers=['value', 'symbol', 'balanceOf', 'invested', 'gas', 'liq_f', 'liq_l', 'r_prof', 'ur_prof', 'roi_r', 'roi_u', 'roi_t', 'days', 'trds', 'pnl'], stralign="right"))
        print(tabulate.tabulate(w.wallet_table_live, tablefmt='psql', headers=['value','symbol','invested','gas','balanceOf','liq_f','liq_l','trades','r_prof','ur_prof','roi_r','roi_u','roi_t','days','pnl','bfm','ft'], stralign="right"))
        print("%.3fs calculate_wallet_live()" % (time.time() - start_time))
        pprint(w.stats)

        print("script executed in %.2fs" % (time.time() - start_time_e))

    start_time = time.time()
    wallet_erc20_stat = utils.get_stat_wallets_address(w3=w3, address=ADDRESS, last_block=LAST_BACKTESTING_BLOCK, N=2000, verbose=1)
    pprint(wallet_erc20_stat)
    print("%.3fs get_stat_wallets_address()" % (time.time() - start_time))



if __name__ == '__main__':
    main()