
from pprint import pprint, pformat
import time
import pickle
import statistics
from datetime import datetime
import tabulate
from web3 import Web3
from eth_utils import to_checksum_address

import utils
from utils import DIR_LOAD_OBJECTS, FILE_LABELS, FILES_DIR

from last_trades import LastTrades


DIR_LOAD_OBJECTS = '/disk_sdc/last_trades_objects_can_be_old'




def balance_table(token, balances, order=None, sort=None, limit=99999, extra_columns=[], skip_invested_0=False):

    balances = balances.copy()

    for adr in [token.address] + token.owner_addresses + token.pairs_list + [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:
        if adr in balances:
            del balances[adr]

    if skip_invested_0:
        for address, value in balances.copy().items():
            if value['invested'] < 0.01:
                del balances[address]

    if order == None:
        balances_ = sorted(balances.copy().items(), key=lambda d: (d[1]['last_trade_block'], d[1]['last_p_i']))
        #balances_ = [value | {'address': address} for address, value in balances_]
    else:
        balances_ = sorted(balances.copy().items(), key=lambda d: (d[1][order], d[1]['pnl']), reverse=True if sort == 'desc' else False)



    modified_w = []
    total_supply = token.blocks_data[max(token.blocks_data.keys())]['total_supply'] if token.blocks_data else None
    for address, value in balances_[:limit]:

        #value = {'date': datetime.fromtimestamp(value['block_timestamp']).strftime("%d-%m-%Y"), 'address': address} | value
        # value = value | {'address': address}
        # print('value:')
        # print(value)

        a_c = {}

        for col in extra_columns:
            if col == 'repeated_trades':
                a_c['repeated_trades'] = f"{value['repeated_buys']}/{value['repeated_sells']}"
            elif col == 'repeated_volume':
                a_c['repeated_volume'] = f"{round(value['repeated_buys_volume'],2)} $ / {round(value['repeated_sells_volume'],2)} $"
            elif col == 'transfers_usd':
                a_c['transfers_usd'] = f"{utils.human_format(value['transfers_sent_list_usd_total'])} $"
            elif col == 'transfers_perc':
                a_c['transfers_perc'] = f"{round(value['transfers_sent_list_perc_total'],2)} %"
            else:
                a_c[col] = value[col]

        _value = round(value['value'] / 10 ** token.contract_data_token['token_decimals'], 2)
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
        a_c['invested'] = f"{utils.human_format(value['invested'])} $" if value['invested'] != 0 else 0
        #a_c['vol_sell'] = f"{round(value['volume_sell'], 2)} $"
        #a_c['withdrew / vol_sell'] = f"{round(value['withdrew'], 2)} $ / {round(value['volume_sell'], 2)} $"
        a_c['withdrew'] = f"{utils.human_format(value['withdrew'])} $" if value['withdrew'] != 0 else 0
        a_c['gas'] = f"{round(value['gas_usd'], 2)} $"

        a_c['r_prof'] = f"{utils.human_format(value['realized_profit'])} $" if value['realized_profit'] != 0 else 0
        a_c['ur_prof'] = f"{utils.human_format(value['unrealized_profit'])} $" if value['unrealized_profit'] != 0 else 0
        a_c['roi_r'] = f"{round(value['roi_r'])}%" if value['roi_r'] != 0 else 0
        a_c['roi_u'] = f"{round(value['roi_u'])}%" if value['roi_u'] != 0 else 0
        a_c['roi_t'] = f"{round(value['roi_t'])}%" if value['roi_t'] != 0 else 0

        a_c['days'] = round(value['days'], 2) if value['days'] else None

        a_c['trades'] = f"{value['trades_buy']}/{value['trades_sell']}"

        if not(address == token.address or address in token.pairs_list):
            if value['transfers_sent'] != 0 or value['transfers_received'] != 0:
                a_c['trades'] += f"/{value['transfers_sent']}/{value['transfers_received']}"

        a_c['pnl'] = f"{utils.human_format(value['pnl'])} $" if value['pnl'] != 0 else 0

        a_c['address'] = address
        if address in token.owner_addresses:
            a_c['address'] = f"{Fore.RED}{a_c['address']}{Style.RESET_ALL}"
        if address == token.address:
            a_c['address'] = f"{Fore.GREEN}{a_c['address']}{Style.RESET_ALL}"
        if address in token.pairs_list:
            a_c['address'] = f"{Fore.BLUE}{a_c['address']}{Style.RESET_ALL}"
        if address in [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]:
            a_c['address'] = f"{Fore.MAGENTA}{a_c['address']}{Style.RESET_ALL}"


        
        modified_w.append(a_c)



    return tabulate.tabulate(modified_w[:], tablefmt='psql', headers='keys', stralign="right")




if __name__ == '__main__':


    testnet = 'http://127.0.0.1:8545/?wp'

    w3 = Web3(Web3.HTTPProvider(testnet))
    if not w3.is_connected():
        raise Exception("w3 not connected")


    # REEF, XYO, SNX, ENS, INJ, DODO, COMP, ELON, ARC, TRUMP, JOE, LMEOW
    # list_addresses = ['0xFE3E6a25e6b192A42a44ecDDCd13796471735ACf','0x55296f69f40Ea6d20E478533C15A6B08B654E758','0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F','0xC18360217D8F7Ab5e7c516566761Ea12Ce7F9D72','0xe28b3B32B6c345A34Ff64674606124Dd5Aceca30','0x747E550a7B848acE786C3CFe754Aa78feBC8A022','0xc00e94Cb662C3520282E6f5717214004A7f26888','0x761D38e5ddf6ccf6Cf7c55759d5210750B5D60F3','0xC82E3dB60A52CF7529253b4eC688f631aad9e7c2','0x576e2BeD8F7b46D34016198911Cdf9886f78bea7','0x76e222b07C53D28b89b0bAc18602810Fc22B49A8','0x1aE7e1d0ce06364CED9aD58225a1705b3e5DB92b']

    # '0xD3F4E2eE5f54694290512b761c111249E4C53483' # MARS

    address = '0x55296f69f40Ea6d20E478533C15A6B08B654E758'

    start_time = time.time()
    with open(f"{DIR_LOAD_OBJECTS}/{to_checksum_address(address)}.pkl", 'rb') as o:
        last_trades = pickle.load(o)
    print(f"object {address} loaded in {(time.time() - start_time):.2f}s")

    last_block = list(last_trades.blocks_data.keys())[-1]


    buy_trans_indexes, sell_trans_indexes = [], []
    mev_bots_count = 0

    timerange_started_on_block = None
    timerange_trades_buy, timerange_trades_sell, timerange_trades_buy_volume, timerange_trades_sell_volume, timerange_new_holders, timerange_last_liq = [], [], [], [], [], []
    local_buy, local_sell, local_buy_volume, local_sell_volume, local_new_holders, local_last_liq = 0, 0, 0, 0, 0, 0

    for _block, block_data in last_trades.blocks_data.items():

        buy_trans_indexes += block_data['buy_trans_indexes']
        sell_trans_indexes += block_data['sell_trans_indexes']
        mev_bots_count += len(block_data['mev_bot_addresses'])

        if not timerange_started_on_block:
            timerange_started_on_block = _block
        
        if (_block - timerange_started_on_block) * 12 / 60 / 60 / 24 > 1: # days
            timerange_trades_buy.append(local_buy)
            timerange_trades_sell.append(local_sell)
            timerange_trades_buy_volume.append(local_buy_volume)
            timerange_trades_sell_volume.append(local_sell_volume)
            timerange_new_holders.append(local_new_holders)
            timerange_last_liq.append(local_last_liq)

            timerange_started_on_block = _block
            local_buy, local_sell, local_buy_volume, local_sell_volume, local_new_holders, local_last_liq = 0, 0, 0, 0, 0, 0

        local_buy += block_data['trades_buy']
        local_sell += block_data['trades_sell']
        local_buy_volume += block_data['invested']
        local_sell_volume += block_data['withdrew']
        local_new_holders += block_data['holders_new']
        local_last_liq = block_data['liquidity']
    

    total_repeated_buys, total_repeated_sells, total_repeated_buys_volume, total_repeated_sells_volume, total_gas_usd = 0, 0, 0, 0, 0
    traders_that_sold_to_zero_value, traders_never_sold, traders_sold_part = 0, 0, 0
    traders_that_sold_to_zero_volume, traders_sold_part_volume, traders_never_sold_total_invested = 0, 0, 0
    total_bribe_buy_trans, total_bribe_buy_eth, total_bribe_sell_trans, total_bribe_sell_eth = 0, 0, 0, 0
    invested_list, withdrew_list, transfers_received_list, pnl_list, realized_profit_list, unrealized_profit_list, roi_r_list, roi_u_list, roi_t_list = [], [], [], [], [], [], [], [] ,[]
    erc20_to_1_count, erc20_to_1_buys, erc20_to_1_sells, erc20_to_1_buys_volume, erc20_to_1_sells_volume = 0, 0, 0, 0, 0
    _stat_erc20 = {}

    for adr, item in last_trades.balances.items():

        ii = 0
        last_trades.balances[adr]['repeated_buys'] = 0
        last_trades.balances[adr]['repeated_buys_volume'] = 0
        last_trades.balances[adr]['repeated_sells'] = 0
        last_trades.balances[adr]['repeated_sells_volume'] = 0
        first_withdrew_done = False
        trader_total_invested = 0
        for _block, trades in item['trades_list'].items():
            for trade in trades:
                total_gas_usd += trade['gas_usd']
                if trade['s'] == 'buy':
                    if trade['invested'] != None:
                        invested_list.append(trade['invested'])
                    trader_total_invested += trade['invested'] if trade['invested'] != None else 0
                    if trade['bribe'] and trade['bribe'] > 0:
                        total_bribe_buy_trans += 1
                        total_bribe_buy_eth += trade['bribe']
                    if ii > 0:
                        last_trades.balances[adr]['repeated_buys'] += 1
                        last_trades.balances[adr]['repeated_buys_volume'] += trade['invested'] if trade['invested'] != None else 0
                if trade['s'] == 'sell':
                    if trade['withdrew'] > 0:
                        withdrew_list.append(trade['withdrew'])
                    if not first_withdrew_done:
                        first_withdrew_done = True
                    else:
                        if trade['withdrew'] > 0:
                            last_trades.balances[adr]['repeated_sells'] += 1
                            last_trades.balances[adr]['repeated_sells_volume'] += trade['withdrew']
                ii += 1
        
        transfers_received_list += [y for x in item['transfers_received_list_usd'].values() for y in x]
        pnl_list.append(item['pnl'])
        realized_profit_list.append(item['realized_profit'])
        unrealized_profit_list.append(item['unrealized_profit'])
        if item['invested'] > 0.01:
            roi_r_list.append(item['roi_r'])
            roi_u_list.append(item['roi_u'])
            roi_t_list.append(item['roi_t'])
        total_repeated_buys += last_trades.balances[adr]['repeated_buys']
        total_repeated_sells += last_trades.balances[adr]['repeated_sells']
        total_repeated_buys_volume += last_trades.balances[adr]['repeated_buys_volume']
        total_repeated_sells_volume += last_trades.balances[adr]['repeated_sells_volume']

        if 0 <= item['value'] <= 1:
            traders_that_sold_to_zero_value += 1
            traders_that_sold_to_zero_volume += item['withdrew']
        if item['invested'] > 0 and item['withdrew'] == 0:
            traders_never_sold += 1
            traders_never_sold_total_invested += item['invested']
        if item['value'] > 1 and item['withdrew'] > 1:
            traders_sold_part += 1
            traders_sold_part_volume += item['withdrew']
        
        #first_in_transactions_addresses = {}

        if 'stat_wallet_address_first' in last_trades.balances[adr] and last_trades.balances[adr]['stat_wallet_address_first'] != None:
            if _stat_erc20 == {}:
                for key in last_trades.balances[adr]['stat_wallet_address_first'].keys():
                    # if key == 'first_in_transaction_from_address':
                    #     if last_trades.balances[adr]['stat_wallet_address_first']['first_in_transaction_from_address'] not in first_in_transactions_addresses:
                    #         first_in_transactions_addresses[last_trades.balances[adr]['stat_wallet_address_first']['first_in_transaction_from_address']] = 0
                    #     first_in_transactions_addresses[last_trades.balances[adr]['stat_wallet_address_first']['first_in_transaction_from_address']] += 1
                    if key in ['first_in_transaction_block','first_in_transaction_from_address','erc20_bought_n_not_sold_addresses']:
                        continue
                    _stat_erc20[key] = []
        
            for key, item in last_trades.balances[adr]['stat_wallet_address_first'].items():
                if key in _stat_erc20:
                    if key in ['banana_trans_perc','maestro_trans_perc']:
                        _stat_erc20[key].append(100*item)
                    elif key == 'erc20_hold_time_block_median':
                        _stat_erc20[key].append(item*12/60/60/24)
                    elif key == 'avg_blocks_per_trans':
                        _stat_erc20[key].append(item*12/60/60/24)
                    else:
                     _stat_erc20[key].append(item)

            if last_trades.balances[adr]['stat_wallet_address_first']['erc20_to'] == 1:
                erc20_to_1_count += 1
                erc20_to_1_buys += last_trades.balances[adr]['trades_buy']
                erc20_to_1_buys_volume += last_trades.balances[adr]['invested']
                erc20_to_1_sells += last_trades.balances[adr]['trades_sell']
                erc20_to_1_sells_volume += last_trades.balances[adr]['withdrew']



    """

    по erc20_to_1
    - какой объем $/% держат сейчас холдеры erc20_to_1
    - сколько trades buy/sell, invested/withdrew
    - искать схожие адреса, с которых были первые пополнения кошельков

    - рейтинг адресов, на которые вводились и выводился ETH потом external транзакции 

    ~ употребять on-china анализ и кластерный анализ, групп, поведенческие признаки
    ~ на какие группы поделить 5:12 https://www.youtube.com/watch?v=dwOCM3HttZw
        - киты
        - основатели/команда/разработчики
        - маркетмэйкеры
        - крупный инвестор
        - остальные/толпа

    - таблица топ 100 холдеров, можно было бы добавить группу разработчиков, если бы были данные раньше


    статы, которых не будет
    - avg win_rate кошельков, roi, profit

    """

    """

    - erc20 стата

    """




    stat_base = [
        {
            'var': 'new_holders',
            'value': f"{last_trades.blocks_data[last_block]['holders_total']}",
            'desc': "новых холдеров",
        },
        {
            'var': 'liquidity',
            'value': f"{utils.human_format(last_trades.blocks_data[last_block]['liquidity'])} $",
            'desc': "текущая ликвидность",
        },
        {
            'var': 'volume_buy',
            'value': f"{utils.human_format(last_trades.total_invested)} $",
            'desc': "сколько всего было вложено денег",
        },
        {
            'var': 'volume_sell',
            'value': f"{utils.human_format(last_trades.total_withdrew)} $",
            'desc': "сколько всего было снято денег",
        },
        {
            'var': 'volume_transfer',
            'value': f"{utils.human_format(last_trades.total_volume_transfer)} $",
            'desc': "какой объем был отправлен переводом Transfer() на другой адрес\nобычно делается между биржами/мостами для арбитража",
        },
        {
            'var': 'volume_buy_div_volume_sell',
            'value': f"{round(100 * last_trades.total_invested / (last_trades.total_invested + last_trades.total_withdrew), 1)}%",
            'desc': "соотношение volume_buy / volume_sell",
        },
        {
            'var': 'trades_buy',
            'value': f"{utils.human_format(last_trades.total_trades_buy)}",
            'desc': "кол-во покупок",
        },
        {
            'var': 'trades_sell',
            'value': f"{utils.human_format(last_trades.total_trades_sell)}",
            'desc': "кол-во продаж",
        },
        {
            'var': 'trades_transfer',
            'value': f"{utils.human_format(last_trades.total_trades_transfer)}",
            'desc': "кол-во обычных переводов",
        },
        {
            'var': 'buy_trans_index_avg',
            'value': round(statistics.median(buy_trans_indexes)),
            'desc': "средняя позиция транзакций на покупку в блоке",
        },
        {
            'var': 'sell_trans_index_avg',
            'value': round(statistics.median(sell_trans_indexes)),
            'desc': "средняя позиция транзакций на продажу в блоке",
        },
        {
            'var': 'repeated_buys',
            'value': total_repeated_buys,
            'desc': 'кол-во повторных покупок одним и тем же адресом и сумма по всем адресам',
        },
        {
            'var': 'repeated_buys_volume',
            'value': f"{utils.human_format(total_repeated_buys_volume)} $",
            'desc': 'объем повторных покупок',
        },
        {
            'var': 'repeated_sells',
            'value': total_repeated_sells,
            'desc': 'кол-во повторных продаж',
        },
        {
            'var': 'repeated_sells_volume',
            'value': f"{utils.human_format(total_repeated_sells_volume)} $",
            'desc': 'объем повторных продаж',
        },
        {
            'var': 'traders_sold_to_zero',
            'value': traders_that_sold_to_zero_value,
            'desc': 'кол-во трейдеров, которые продали токен в 0',
        },
        {
            'var': 'traders_sold_to_zero_perc',
            'value': f"{round(100 * traders_that_sold_to_zero_value / last_trades.blocks_data[last_block]['holders_total'], 1)}%",
            'desc': 'кол-во трейдеров, которые продали токен в 0 относительно всех трейдеров',
        },
        {
            'var': 'traders_sold_to_zero_volume',
            'value': f"{utils.human_format(traders_that_sold_to_zero_volume)} $",
            'desc': 'суммарный объем продаж трейдерами, которые продали токен в 0',
        },
        {
            'var': 'traders_sold_part',
            'value': traders_sold_part,
            'desc': 'кол-во трейдеров, которые продали токен частично',
        },
        {
            'var': 'traders_sold_part_perc',
            'value': f"{round(100 * traders_sold_part / last_trades.blocks_data[last_block]['holders_total'], 1)}%",
            'desc': 'кол-во трейдеров, которые продали токен частично относительно всех трейдеров',
        },
        {
            'var': 'traders_sold_part_volume',
            'value': f"{utils.human_format(traders_sold_part_volume)} $",
            'desc': 'суммарный объем продаж трейдерами, которые продали токен частично',
        },
        {
            'var': 'traders_never_sold',
            'value': traders_never_sold,
            'desc': 'кол-во трейдеров, которые ни разу не продавали',
        },
        {
            'var': 'traders_never_sold_perc',
            'value': f"{round(100 * traders_never_sold / last_trades.blocks_data[last_block]['holders_total'], 1)}%",
            'desc': 'кол-во трейдеров, которые ни разу не продавали относительно всех трейдеров'
        },
        {
            'var': 'traders_never_sold_total_invested',
            'value': f"{utils.human_format(traders_never_sold_total_invested)} $",
            'desc': 'суммарный объем покупок трейдерами, которые ни разу не продавали',
        },
        {
            'var': 'total_gas_usd',
            'value': f"{utils.human_format(total_gas_usd)} $",
            'desc': "сумма всего потраченного gas'a на все транзакции токена на покупки, продажи или transfer",
        },
        {
            'var': 'bribe_buy',
            'value': total_bribe_buy_trans,
            'desc': 'кол-во сделок на покупку, у которых был bribe (взятка) в транзакции\nэто дополнительная комиссия, которая отправляется валидатору для выполениния транзакций одной из первых в блоке при его формировании\nможет сигнализировать о том, что профессиональный покупатель совершает сделку',
        },
        {
            'var': 'bribe_buy_eth',
            'value': f"{round(total_bribe_buy_eth,3)} ETH",
            'desc': 'общая сумма в эфире, потраченная на дополнительный bribe при покупках',
        },
        {
            'var': 'bribe_sell',
            'value': total_bribe_sell_trans,
            'desc': 'кол-во сделок на продажу, у которых был bribe (взятка) в транзакции',
        },
        {
            'var': 'bribe_sell_eth',
            'value': f"{round(total_bribe_sell_eth,3)} ETH",
            'desc': 'общая сумма в эфире, потраченная на дополнительный bribe при продажах',
        },
        {
            'var': 'avg_buy',
            'value': f"{round(statistics.median(invested_list),2)} $",
            'desc': 'средняя сумма покупки',
        },
        {
            'var': 'avg_sell',
            'value': f"{round(statistics.median(withdrew_list),2)} $",
            'desc': 'средняя сумма продажи',
        },
        {
            'var': 'avg_transfer',
            'value': f"{round(statistics.median(transfers_received_list),2)} $",
            'desc': 'средняя сумма обычного перевода Transfer()',
        },
        {
            'var': 'avg_pnl',
            'value': f"{round(statistics.mean(pnl_list),2)} $",
            'desc': 'средний pnl',
        },
        {
            'var': 'avg_realized_profit',
            'value': f"{round(statistics.mean(realized_profit_list),2)} $",
            'desc': 'средний realized_profit',
        },
        {
            'var': 'avg_unrealized_profit',
            'value': f"{round(statistics.mean(unrealized_profit_list),2)} $",
            'desc': 'средний unrealized_profit',
        },
        {
            'var': 'avg_roi_r',
            'value': f"{round(statistics.mean(roi_r_list),1)}%",
            'desc': 'средний roi по realized_profit',
        },
        {
            'var': 'avg_roi_u',
            'value': f"{round(statistics.mean(roi_u_list),1)}%",
            'desc': 'средний roi по unrealized_profit',
        },
        {
            'var': 'avg_roi_t',
            'value': f"{round(statistics.mean(roi_t_list),1)}%",
            'desc': 'суммарный roi по realized_profit и unrealized_profit',
        },
        {
            'var': 'avg_trades_buy_per_day',
            'value': f"{round(statistics.mean(timerange_trades_buy),1)}",
            'desc': 'среднее кол-во прокупок в день',
        },
        {
            'var': 'avg_trades_buy_volume_per_day',
            'value': f"{utils.human_format(statistics.mean(timerange_trades_buy_volume))} $",
            'desc': 'средний суммарный объем покупок в день',
        },
        {
            'var': 'avg_trades_sell_per_day',
            'value': f"{round(statistics.mean(timerange_trades_sell),1)}",
            'desc': 'среднее кол-во продаж в день',
        },
        {
            'var': 'avg_trades_sell_volume_per_day',
            'value': f"{utils.human_format(statistics.mean(timerange_trades_sell_volume))} $",
            'desc': 'средний суммарный объем продаж в день',
        },
        {
            'var': 'avg_new_holders_per_day',
            'value': f"{round(statistics.mean(timerange_new_holders),1)}",
            'desc': 'среднее кол-во новых холдеров в день',
        },
        {
            'var': 'avg_liquidity_change_per_day',
            'value': f"{utils.human_format(statistics.mean(timerange_last_liq))} $",
            'desc': 'среднее значение пула ликвидности по дням',
        },
        {
            'var': 'fresh_wallets',
            'value': erc20_to_1_count,
            'desc': 'кол-во свежих адресов (по которым ранее не было ни одной erc20 транзакции)',
        },
        {
            'var': 'fresh_wallets_perc',
            'value': f"{round(100 * erc20_to_1_count / last_trades.blocks_data[last_block]['holders_total'], 1)}%",
            'desc': 'кол-во свежих адресов относительно всех холдеров',
        },
        {
            'var': 'fresh_wallets_buys',
            'value': erc20_to_1_buys,
            'desc': 'кол-во покупок свежих адресов',
        },
        {
            'var': 'fresh_wallets_buys_perc',
            'value': f"{round(100 * erc20_to_1_buys / last_trades.total_trades_buy, 1)}%",
            'desc': 'кол-во покупок свежих адресов относительно всех покупок',
        },
        {
            'var': 'fresh_wallets_buys_volume',
            'value': f"{utils.human_format(erc20_to_1_buys_volume)} $",
            'desc': 'объем покупок свежих адресов',
        },
        {
            'var': 'fresh_wallets_buys_volume_perc',
            'value': f"{round(100 * erc20_to_1_buys_volume / last_trades.total_invested, 1)}%",
            'desc': 'объем покупок свежих адресов относительно всего объема покупок',
        },
        {
            'var': 'fresh_wallets_sells',
            'value': erc20_to_1_sells,
            'desc': 'кол-во продаж свежих адресов',
        },
        {
            'var': 'fresh_wallets_sells_perc',
            'value': f"{round(100 * erc20_to_1_sells / last_trades.total_trades_sell, 1)}%",
            'desc': 'кол-во продаж свежих адресов относительно всех продаж',
        },
        {
            'var': 'fresh_wallets_sells_volume',
            'value': f"{utils.human_format(erc20_to_1_sells_volume)} $",
            'desc': 'объем продаж свежих адресов',
        },
        {
            'var': 'fresh_wallets_sells_volume_perc',
            'value': f"{round(100 * erc20_to_1_sells_volume / last_trades.total_withdrew, 1)}%",
            'desc': 'объем продаж свежих адресов относительно всего объема продаж',
        },
        # {
        #     'var': '',
        #     'value': '',
        #     'desc': '',
        # },
        {
            'var': 'mev_bots_count',
            'value': mev_bots_count,
            'desc': "кол-во MEV ботов",
        },
    ]
    print(f'Общая on-chain статистика с {list(last_trades.blocks_data.keys())[0]} блока по токену {address}')
    print('Что здесь важно понимать: эти данные можно агрегировать как угодно и сделать срезы за любой интервал времени и по любой группе холдеров')
    print('Получить эти данные за, например, период накопления и период пампа. Затем сравнить для поиска аномалий')
    print('Или поделить на кластеры по поведенческим факторам: только свежие кошельки, у которых ранее не было erc20 покупок; крупные инвестора; кому supply был раскинут до листинга; снайпера; основатели/команда/разработчики; маркетмэйкеры; обычные трейдеры и тд.')
    print('fresh_wallets - очень важная метрика; при активных покупках такими адресами может означать период накопления токена перед пампом')
    print(tabulate.tabulate(stat_base, tablefmt='psql', headers='keys', stralign="left"))


    _descriptions = {
        'transfers_sql_len': 'кол-во транзакций',
        'balance_eth': 'баланс адреса в эфире',
        'balance_usd': 'баланс адреса в USDC/USDT',
        'total_from': 'кол-во транзакций отправок эфира на сторонний адрес',
        'total_to': 'кол-во тразакций на получения эфира на текущий адрес',
        'erc20_from': 'кол-во erc20 токенов, отправленных с адреса',
        'erc20_to': 'кол-во erc20 токенов, полученных на адрес',
        'erc721_from': 'кол-во erc721 токенов, отправленных с адреса',
        'erc721_to': 'кол-во erc721 токенов, полученных на адрес',
        'erc1155_from': 'кол-во erc1155 токенов, отправленных с адреса',
        'erc1155_to': 'кол-во erc1155 токенов, полученных на адрес',
        'banana_trans': 'кол-во транзакий через роутер banana',
        'maestro_trans': 'кол-во транзакций через роутер maestro\nmaestro и banana сигнализирует о том, что адрес относится к категории обычных трейдеров',
        'eth_received_internal_median': 'среднее значение получения эфира internal транзакцией',
        'eth_received_external_median': 'среднее значение получения эфира external транзакцией',
        'eth_received_external_max': 'максимальное значение получения эфира external транзакцией',
        'eth_sent_external_median': 'среднее значение отправки эфира external транзакцией',
        'eth_sent_external_max': 'максимальное значение отправки эфира external транзакцией',
        # 'erc20_eth_send_max                            '     1.30134     '       |
        # 'erc20_eth_send_min                            '     0.0603438   '       |
        # 'erc20_eth_send_sum                            '    19.9593      '       |
        # 'erc20_eth_send_std                            '     0.228496    '       |
        # 'erc20_eth_send_median                         '     0.187321    '       |
        # 'erc20_eth_send_avg                            '     0.258993    '       |
        # 'erc20_eth_received_max                        '     1.435       '       |
        # 'erc20_eth_received_min                        '     0.0416859   '       |
        # 'erc20_eth_received_sum                        '    20.1511      '       |
        # 'erc20_eth_received_std                        '     0.261919    '       |
        # 'erc20_eth_received_median                     '     0.195964    '       |
        # 'erc20_eth_received_avg                        '     0.274327    '       |
        'erc20_hold_time_block_median': 'среднее время удерживания erc20 токенов в днях с момента покупки до первой продажи\nважная метрика, если адрес с большим значением данного показателя совершил покупку,\nто высокая вероятноть он не скоро продаст данный токен',
        'erc20_value_usd_median': 'среднее значение объема покупок erc20 токенов',
        'avg_blocks_per_trans': 'среднее кол-во дней на 1 совершенную транзакцию адресом',
        'banana_trans_perc': 'соотношение транзакций с роутером banana к общему кол-ву',
        'maestro_trans_perc': 'соотношение транзакций с роутером maestro к общему кол-ву',
        'erc20_uniq_addresses_send': 'кол-во уникальных адресов, на которые были отправлены erc20 токены',
        'erc20_uniq_addresses_received': 'кол-во уникальных адресов, которые прислали erc20 токены',
        'unique_received_from_addresses': 'кол-во уникальных адресов, которые прислали эфир на адрес',
        'unique_received_to_addresses': 'кол-во уникальных адресов, на которые адрес отправил эфир',
        'erc20_bought_n_not_sold': 'кол-во erc20 токенов, которые есть на балансе кошелька и ни разу не было трейда на продажу\nозначает, что адрес держит токен',
        'erc20_bought_n_not_sold_perc': 'соотношение кол-ва erc20 токенов, по которым ни разу не было продаж, относительно истории наличия всех erc20 токенов у адреса',
        'age_days': 'время жизни адреса (кошелька) в днях с момента первой транзакции',
    }
    stat_erc20_for_tab = []
    for key, value in _stat_erc20.items():
        if key in _descriptions:
            if key in ['balance_eth']:
                _val = f"{round(statistics.median(value),3)} ETH"
            elif key in ['balance_usd','erc20_value_usd_median']:
                _val = f"{round(statistics.median(value),2)} $"
            elif key in ['banana_trans_perc','maestro_trans_perc','erc20_bought_n_not_sold_perc']:
                _val = f"{round(statistics.mean(value),1)}%"
            elif key in ['total_to','erc20_from','erc20_to']:
                _val = f"{round(statistics.median(value),2)}"
            else:
                _val = f"{round(statistics.mean(value),2)}"
            stat_erc20_for_tab.append({
                'var': f'avg_{key}',
                'value': _val,
                'desc': _descriptions[key],
            })
    stat_erc20_for_tab += [
        {
            'var': 'avg_win_rate',
            'value': '-',
            'desc': 'win rate адреса; трейд считается прибыльным, если его pnl > 0',
        },
    ]
    print()
    print()
    print('Ниже усредненная статистика по всем адресам до момента совершения трейда по токену и никак не связанной с активностью данного токена')
    print('В чем суть: после совершения первой активности по токену можно получить все исторические данные трейдера на блокчейне и использовать эти данные для дальнейшего анализа')
    print('Здесь приведены метрики, полученные для каждого адреса отдельно, а затем вычислено среднее значение')
    # print('Например, если transfers_sql_len высокий, то значит текущий токен купили адреса с обильной историей в прошлом')
    # print('Или если erc20_value_usd_median большое значение, то данный адрес можно отнести к группе инвесторов')
    print(tabulate.tabulate(stat_erc20_for_tab, tablefmt='psql', headers='keys', stralign="left"))






    first_in_transactions_addresses = dict(sorted(last_trades.first_in_transaction_from_address_rating.items(), key=lambda x: x[1], reverse=True))
    list_first_in_transactions_addresses = []
    for key, value in first_in_transactions_addresses.items():
        if key != None:
            list_first_in_transactions_addresses.append({'address': utils.print_address(key), 'holders': value, 'is_contract': utils.is_address_contract(w3, to_checksum_address(key))})
        if len(list_first_in_transactions_addresses) == 30:
            break
    print()
    print('Таблица топ 30 адресов, с которых была самая первая external транзакция пополнения баланса эфиром на адрес холдера')
    print('Если с одного адреса, не являющимся биржей, обменником или публичным роутером, было много пополнений на адреса холдеров, то можно отнести их в единую группу')
    print(tabulate.tabulate(list_first_in_transactions_addresses, tablefmt='psql', headers='keys', stralign="left"))




    exclude_addresses = last_trades.pairs_list + [last_trades.address] + [utils.ADDRESS_0X0000, utils.ADDRESS_0XDEAD]
    #find_addresses = list(set(last_trades.balances.keys()) - set(exclude_addresses))
    #print('find_addresses:', len(find_addresses))

    start_time = time.time()

    # def quick_get_stats(adr):
    #     return utils.get_stat_wallets_address(w3=None, address=adr, last_block=21174942, N=500)

    # params = []
    # for i, adr in enumerate(find_addresses[:8]):
    #     params.append((adr,))
    # with Pool(min(8, len(params))) as pool:
    #     _stats_d = pool.starmap_async(quick_get_stats, params).get()

    external_from_addresses_count = {}
    external_from_addresses_eth = {}
    external_from_addresses_holders = {}
    external_from_addresses_holders_supply_perc = {}
    external_from_addresses_holders_value_usd = {}
    external_from_addresses_holders_list = {}
    for adr in last_trades.balances.keys():
        if adr in exclude_addresses:
            continue
        if 'stat_wallet_address' in last_trades.balances[adr] and last_trades.balances[adr]['stat_wallet_address']:
            for adr_external, value in last_trades.balances[adr]['stat_wallet_address']['external_from_addresses_n_values'].items():
                if adr_external not in external_from_addresses_count:
                    external_from_addresses_count[adr_external] = 0
                    external_from_addresses_eth[adr_external] = 0
                    external_from_addresses_holders[adr_external] = 0
                    external_from_addresses_holders_supply_perc[adr_external] = 0
                    external_from_addresses_holders_value_usd[adr_external] = 0
                    external_from_addresses_holders_list[adr_external] = []
                external_from_addresses_count[adr_external] += len(value)
                external_from_addresses_eth[adr_external] += sum(value)
                external_from_addresses_holders[adr_external] += 1
                external_from_addresses_holders_supply_perc[adr_external] += last_trades.balances[adr]['%']
                # if adr == '0xfC27C1527266Af0Be2afF4078979faF283267e62':
                #     print(last_trades.balances[adr]['%'])
                #     exit()
                external_from_addresses_holders_value_usd[adr_external] += last_trades.balances[adr]['value_usd']
                external_from_addresses_holders_list[adr_external].append(adr)

            # print(f"-> adr: {adr}, erc20 adrs: {last_trades.balances[adr]['stat_wallet_address']['erc20_addresses']}")



    __external_from_addresses = dict(sorted(external_from_addresses_holders_supply_perc.items(), key=lambda x: x[1], reverse=True))
    list_external_from_addresses = []
    for key, value in __external_from_addresses.items():
        if external_from_addresses_holders[key] > 1:
            list_external_from_addresses.append({
                'address': utils.print_address(key),
                'holders': external_from_addresses_holders[key],
                'count': external_from_addresses_count[key],
                'supply_perc': f"{round(external_from_addresses_holders_supply_perc[key], 2)} %",
                'value_usd': f"{utils.human_format(external_from_addresses_holders_value_usd[key])} $",
                'eth_sum': f"{round(external_from_addresses_eth[key],3)} ETH",
                'is_contract': utils.is_address_contract(w3, to_checksum_address(key)),
                'holders_list': '\n'.join(external_from_addresses_holders_list[key][:10])
            })
        if len(list_external_from_addresses) == 50:
            break
    print()
    print('Таблица топ 50 адресов, на которые были переводы эфира external транзакцией холдерами токена')
    print('Если много холдеров переводят эфир на некий единый глобальный кошелек, причем эти холдеры закрыли сделки в плюс, то можно отнести эти адреса к группе команды разработчиков токена')
    print('Такие адреса особенно легко вычислить, если ранее был памп, эти адреса закрыли сделки в плюс, вывели деньги на 1 или более основных кошельков, а потом происходит новый период накопления перед очередным пампом')
    print(tabulate.tabulate(list_external_from_addresses, tablefmt='psql', headers='keys', stralign="left"))
    #exit()






    desc = [
        {'value': 'value','desc': "текущий баланс токена на данном адресе и его эквивалент в $"},
        {'value': '%','desc': "процент value от общего supply токена"},
        {'value': 'invested','desc': "сколько всего было вложено денег"},
        {'value': 'withdrew','desc': "сколько всего было снято денег"},
        {'value': 'gas','desc': "сколько всего было потрачено на gas"},
        {'value': 'r_prof','desc': "realized profit, реализованная прибыль закрытых сделок (без учета gas)"},
        {'value': 'ur_prof','desc': "unrealized profit, нереализованная прибыль открытых сделок (без учета gas)"},
        {'value': 'roi_r','desc': "roi реализованной прибыли в %"},
        {'value': 'roi_u','desc': "roi нереализованной прибыли в %"},
        {'value': 'roi_t','desc': "roi суммарный в %"},
        {'value': 'days','desc': "кол-во дней держания токена относительно первой покупки и последней продажи (если была)"},
        {'value': 'trades','desc': "buy/sell/send/receive"},
        {'value': 'pnl','desc': "profit and loss, суммарная прибыль (realized profit + unrealized profit)"},
    ]
    print()
    print('Ниже таблицы с сортировкой некоторых параметров по активности трейдеров')
    print()
    print(tabulate.tabulate(desc, tablefmt='psql', headers='keys', stralign="left"))

    print()
    print('Топ 15 трейдеров по худшему pnl')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='pnl', limit=15, sort='asc', skip_invested_0=True))

    print()
    print('Топ 15 трейдеров по лучшему r_prof (реализованная прибыль)')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='realized_profit', limit=15, sort='desc', skip_invested_0=True))

    print()
    print('Топ 15 трейдеров по лучшему roi_r (реализованная прибыль в %)')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='roi_r', limit=15, sort='desc', skip_invested_0=True))

    print()
    print('Топ 15 трейдеров с наибольшим кол-вом supply токенов')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='%', limit=15, sort='desc'))

    print()
    print('Топ 15 трейдеров с наибольшим кол-вом покупок (включая MEV ботов)')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='trades_buy', limit=15, sort='desc'))

    print()
    print('Топ 15 трейдеров (киты) с наибольшим invested (сколько было вложено денег)')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='invested', limit=15, sort='desc'))

    print()
    print('Топ 15 трейдеров с наибольшим withdrew (сколько было снято денег)')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='withdrew', limit=15, sort='desc'))

    print()
    print('Топ 15 трейдеров с наибольшим кол-вом повторных покупок. Очень важный отчет, большое кол-во повторных покупок может сигнализировать о том, что есть интерес к токену и идет период накопления перед пампом')
    print('repeated_trades - buy/sell')
    print('repeated_volume - суммарный buy volume/sell volume (только повторных трейдов)')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='repeated_buys', limit=15, sort='desc', extra_columns=['repeated_trades', 'repeated_volume']))

    print()
    print('Топ 15 трейдеров с наибольшими кол-вом повторных продаж')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='repeated_sells', limit=15, sort='desc', extra_columns=['repeated_trades', 'repeated_volume']))

    print()
    print('Топ 15 трейдеров с наибольшими transfers в кол-ве отправок')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='transfers_sent', limit=15, sort='desc', extra_columns=['transfers_usd']))
    
    print()
    print('Топ 15 трейдеров с наибольшими transfers в usd')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='transfers_sent_list_usd_total', limit=15, sort='desc', extra_columns=['transfers_usd']))
    
    """
    print()
    print('Топ 15 трейдеров с наибольшими transfers в %')
    print(balance_table(token=last_trades, balances=last_trades.balances, order='transfers_sent_list_perc_total', limit=15, sort='desc', extra_columns=['transfers_usd']))
    """


    # print()
    # print()

    # print(f"balances len: {len(last_trades.balances.keys())}")
    # # def last_trades.balances['0xDEd42a28cb1e01a49eeB9693Fb0763c3547461c0']['stat_wallet_address']
    # # def last_trades.balances['0xDEd42a28cb1e01a49eeB9693Fb0763c3547461c0']['stat_wallet_address_first']
    # # pprint(last_trades.balances['0xDEd42a28cb1e01a49eeB9693Fb0763c3547461c0'])
    # for adr, item in last_trades.balances.items():

    #     if item['trades_list'] == {} and item['transfers_list']:
    #         print(adr)
    #         pprint(item['transfers_list'])
    # exit()