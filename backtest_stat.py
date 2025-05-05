
import json
import traceback
import time
from web3 import Web3
from web3.logs import DISCARD
from web3._utils.events import get_event_data
from web3_input_decoder import decode_constructor, decode_function
from pprint import pprint
from datetime import datetime
from eth_utils import to_checksum_address, from_wei, to_wei

import psycopg2
import psycopg2.extras
from psycopg2.extras import Json, DictCursor, RealDictCursor
psycopg2.extras.register_uuid()

import utils
from utils import FILES_DIR



BACKTEST_MODE = 1


if __name__ == '__main__':


    testnet = 'http://127.0.0.1:8545'

    w3 = Web3(Web3.HTTPProvider(f"{testnet}/?t"))
    if not w3.is_connected():
        raise Exception("w3 not connected")


    last_block = w3.eth.block_number


    trades_list = {} # index - token address

    with psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:

            strategies = 'Time'

            start_time = time.time()
            cursor.execute(f"SELECT * FROM trades WHERE strategy='{strategies}' and backtest={BACKTEST_MODE} ORDER BY trans_block_number ASC")
            _db_trades = cursor.fetchall()
            print(f"{(time.time() - start_time):.3f}s found {len(_db_trades)} trades in {strategies} strategy, backtest={BACKTEST_MODE}")

            cursor.execute(f"SELECT eth_price FROM _blocks WHERE number={last_block - 1} LIMIT 1")
            #cursor.execute(f"SELECT eth_price FROM _blocks WHERE number=20870835 LIMIT 1")
            tx_eth_price = float(cursor.fetchone()['eth_price'])

    for trade in _db_trades:

        """
        trade

        RealDictRow([('id', 360),
        ('address', '0x978ab1d3663c78742f734ac056853480d6661e9c'),
        ('pair', '0x35c60ab301c7f390046bfa323687820237c4cc49'),
        ('token1_address', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'),
        ('trans_block_number', 20718980),
        ('trans_value_usd_per_token_unit', sDecimal('8.340700815109124E-22')),
        ('block_number', None),
        ('value', Decimal('126668309599750570000000')),
        ('invested_usd', None),
        ('gas_usd', Decimal('0')),
        ('realized_profit', None),
        ('http_data', None),
        ('side', 's'),
        ('status', 1),
        ('hash', 'Hrk3yjjXqyGMFtYXuD4B'),
        ('strategy', 'BBands'),
        ('tag', None),
        ('backtest', 1),
        ('exchange', 'V2'),
        ('withdrew_usd', Decimal('105.65024731271345'))])
        """

        if trade['address'] not in trades_list:
            trades_list[trade['address']] = []
        
        if trades_list[trade['address']] and (trade['side'] == 'b' and trades_list[trade['address']][-1]['side'] == 'b' \
            or trade['side'] == 's' and trades_list[trade['address']][-1]['side'] == 's'):
            print("trades_list[trade['address']]:")
            pprint(trades_list[trade['address']])
            assert 0, f"Ошибка в side, address={trade['address']}, trade side={trade['side']}"

        # (!) должно быть включено
        assert trades_list[trade['address']] or not trades_list[trade['address']] and trade['side'] == 'b', f"{trade['address']}, {trade}"

        trades_list[trade['address']].append(trade)
    
    
    closed_profits_list = []
    opened_profits_list = []
    for address, _trades_li in trades_list.items():


        print(f"address={address} ({len(_trades_li)} total trades)")

        i = -1
        for trade in _trades_li:
            i += 1
            if trade['side'] == 'b':
                # print(f" side: {trade['side']}, block: {trade['trans_block_number']}, date: {datetime.fromtimestamp(last_trades.blocks_data[last_block]['timestamp'])}")
                
                print(f" side: {trade['side']}, block: {trade['trans_block_number']}")
                if i + 1 == len(_trades_li):

                    token1_currency = None
                    if trade['token1_address'] == utils.WETH.lower(): token1_currency = 'WETH'
                    if trade['token1_address'] == utils.USDC.lower(): token1_currency = 'USDC'
                    if trade['token1_address'] == utils.USDT.lower(): token1_currency = 'USDT'
                    _kwards = utils.usd_per_one_token(w3=w3, address=to_checksum_address(trade['address']), block=last_block, pair_address=to_checksum_address(trade['pair']), token1_currency=token1_currency, tx_eth_price=tx_eth_price)

                    profit = 100 * _kwards['value_usd_per_token_unit'] / 10 ** _kwards['token_dec'] / float(_trades_li[i - 1]['trans_value_usd_per_token_unit']) - 100
                    opened_profits_list.append(round(float(profit), 3))

                    print(f"  already {round((last_block - trade['trans_block_number']) * 12 / 60, 1)}m in trade, profit: {round(profit, 2)}%, liq: {utils.human_format(round(_kwards['reserves_pair_usd'], 2))}$")
            else:
                profit = 100 * trade['trans_value_usd_per_token_unit'] / _trades_li[i - 1]['trans_value_usd_per_token_unit'] - 100
                closed_profits_list.append(round(float(profit), 3))
                print(f" side: {trade['side']}, block: {trade['trans_block_number']}, time: {round((trade['trans_block_number'] - _trades_li[i - 1]['trans_block_number']) * 12 / 60, 1)}m, profit: {round(profit, 2)}% ({trade['tag']})")

    print('-'*30)

    print(f'opened_profits_list ({len(opened_profits_list)}):', sorted(opened_profits_list))
    print(f'closed_profits_list ({len(closed_profits_list)}):', sorted(closed_profits_list))
    print(f"opened profit: {round(sum(opened_profits_list), 2)}%, avg profit: {round((sum(opened_profits_list)) / (len(opened_profits_list)), 2) if len(opened_profits_list) else '-'}%")
    print(f"closed profit: {round(sum(closed_profits_list), 2)}%, avg profit: {round((sum(closed_profits_list)) / (len(closed_profits_list)), 2)}%")
    print(f"uniq addresses={len(trades_list)}, total profit: {round(sum(opened_profits_list) + sum(closed_profits_list), 2)}%, avg profit: {round((sum(opened_profits_list) + sum(closed_profits_list)) / (len(opened_profits_list) + len(closed_profits_list)), 2)}%")
    print(f"traded tokens:")
    print(trades_list.keys())