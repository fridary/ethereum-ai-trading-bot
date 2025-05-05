



# pip install -U web3==7.0.0-beta.6


import time
import requests
import asyncio
from web3 import AsyncWeb3, WebSocketProvider
from datetime import datetime
from pprint import pprint
from attributedict.collections import AttributeDict
import json
import traceback

import psycopg2
from psycopg2.extras import Json, DictCursor, RealDictCursor

import ml
import utils


import logging
from logging.handlers import RotatingFileHandler
formatter = logging.Formatter('%(asctime)s %(levelname)s %(filename)s %(message)s')
# logging.basicConfig(filename='logs/task/db.log', format='%(asctime)s %(levelname)s %(message)s')
# logger = logging.getLogger()

mints_handler = RotatingFileHandler('/home/sirjay/python/ethereum/lib/logs/bMints.log', mode='a',
    maxBytes=10*1024*1024,
    backupCount=0, encoding=None, delay=0)
mints_handler.setFormatter(formatter)
mints_handler.setLevel(logging.INFO)
mints_logger = logging.getLogger('any-mints') # root
mints_logger.setLevel(logging.INFO)
if (mints_logger.hasHandlers()):
    mints_logger.handlers.clear()
mints_logger.addHandler(mints_handler)





HTTP = "http://127.0.0.1:9545"
WSS = "ws://127.0.0.1:9546"

conn = psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs)

abi_weth = utils.get_abi(utils.WETH)
abi_usdc = utils.get_abi(utils.USDC)
abi_usdt = utils.get_abi(utils.USDT)


async def get_event():
    while True:
        print('starting get_event()')
        try:
            async with AsyncWeb3(WebSocketProvider(WSS)) as w3:
                subscription_id = await w3.eth.subscribe("newPendingTransactions", True)
                print('subscription_id:', subscription_id)

                async for message in w3.socket.process_subscriptions():
                    asyncio.create_task(on_message(w3, message["result"]))
        except:
            print(sys.exc_info())
            print(traceback.format_exc(), file=sys.stderr)
            await asyncio.sleep(5)



async def on_message(w3, tx):

    # hash_ = '0x'+tx['hash'].hex()
    # print('hash_:', hash_)
    # tx = requests.post(HTTP, json={"method":"eth_getTransactionByHash","params":[hash_],"id":1,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()
    # pprint(tx)
    # tx = tx['result']

    start_time_e = time.time()

    # _tx_u = {'to': tx['to'], 'gas': tx['gas'], 'gasPrice': tx['gasPrice'], 'value': tx['value'], 'data': tx['input'], 'from': tx['from']}
    _tx_u = {'to': tx['to'], 'gas': hex(tx['gas']), 'gasPrice': hex(tx['gasPrice']), 'value': hex(tx['value']), 'data': '0x'+tx['input'].hex(), 'from': tx['from']}
    # print('_tx_u:', _tx_u)

    payload = {
        'id': 1,
        'method': 'debug_traceCall',
        'jsonrpc': '2.0',
        'params': [
            _tx_u,
            'latest',
            {
                "tracer": "callTracer", # prestateTracer
                # "timeout": "120s",
                "tracerConfig": {
                    "withLog": True,
                    "onlyTopCall": False
                },
                # "blockOverrides": {
                #     # "number": hex(block_number) if '9545' in testnet else block_number,
                #     # "blockNumber": hex(block_number) if '9545' in testnet else block_number,
                #     # "gasLimit": 0,
                #     "baseFee": hex(0) if '9545' in HTTP else 0,
                #     "blobBaseFee": hex(0) if '9545' in HTTP else 0,
                # },
            },
        ]
    }

    #start_time = time.time()
    trace = requests.post(HTTP, json=payload, headers={"Content-Type": "application/json"}).json()
    #print("trace in %.3fs" % (time.time() - start_time))

    if 'result' in trace:
        trace = trace['result']
    else:
        print(f"\x1b[31;1m(!)\x1b[0m 'result' not in trace:")
        pprint(trace)
        print(f"tx_hash={'0x'+tx['hash'].hex()}")
        return


    def check_calls(level, tx_internal):
        if level > 1000:
            return
        if 'logs' in tx_internal:
            for lo in tx_internal['logs']:
                logs.append(lo)
        if len(logs) > 40:
            return
        if 'calls' in tx_internal:
            for t_i in tx_internal['calls']:
                check_calls(level + 1, t_i)
    logs = []
    check_calls(1, trace)


    def convert_hex(string):
        if string.startswith('0x'):
            if len(string) <= 42: return string
            if len(string) == 66:
                string = string.replace('0x000000000000000000000000', '0x')
                return string
            else:
                return string

        return string

    def _make_decode(w3, contract, abi, log):
        start_time = time.time()
        decoded_data, evt_name, logs_dlc = utils.decode_log_contract(w3, contract, abi, log)
        if 1:
            for lo in logs_dlc:
                print('Error _make_decode():', lo)
        return decoded_data, evt_name


    abi = [
        # OwnershipTransferred(address,address)
        json.loads('{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"}'),
        # Transfer(address,address,uint256) 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
        json.loads('{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}'),
        # Transfer(address,address,uint256,bytes) 0xe19260aff97b920c7df27010903aeb9c8d2be5d310a2c67824cf3f15396e4c16
        json.loads('{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"},{"indexed":false,"name":"data","type":"bytes"}],"name":"Transfer","type":"event"}'),
        # Mint(address,uint256,uint256) V2
        json.loads('{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"}'),
        # Mint(address,address,int24,int24,uint128,uint256,uint256) V3
        json.loads('{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"}'),
        # PairCreated(address,address,address,uint256)
        json.loads('{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":false,"internalType":"address","name":"pair","type":"address"},{"indexed":false,"internalType":"uint256","name":"","type":"uint256"}],"name":"PairCreated","type":"event"}'),
        # PoolCreated(address,address,uint24,int24,address)
        json.loads('{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":true,"internalType":"uint24","name":"fee","type":"uint24"},{"indexed":false,"internalType":"int24","name":"tickSpacing","type":"int24"},{"indexed":false,"internalType":"address","name":"pool","type":"address"}],"name":"PoolCreated","type":"event"}'),
    ]

    minted_pair = None
    logs_array = []

    for log in logs:

        decoded_data = None

        # Transfer
        if log['topics'][0] in ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0xe19260aff97b920c7df27010903aeb9c8d2be5d310a2c67824cf3f15396e4c16']:
            decoded_data, evt_name = _make_decode(w3, AttributeDict({'address':log['address'],'abi':abi}), abi, log)

        # OwnershipTransferred
        if log['topics'][0] == '0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0':
            decoded_data, evt_name = _make_decode(w3, AttributeDict({'address':log['address'],'abi':abi}), abi, log)
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'OwnershipTransferred', 'hash:', '0x'+tx['hash'].hex(), decoded_data)

        # Mint
        if log['topics'][0] in ['0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f', '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde']:
            if len(log['topics']) >= 2 and convert_hex(log['topics'][1]) in [utils.UNISWAP_V2_ROUTER.lower(), utils.UNISWAP_V3_POSITIONS_NFT.lower()]:
                decoded_data, evt_name = _make_decode(w3, AttributeDict({'address':log['address'],'abi':abi}), abi, log)
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'Mint', 'hash:', '0x'+tx['hash'].hex(), f"address: {log['address']}, {decoded_data}")
                minted_pair = log['address']


        # пример лога
        # {'address': '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        # 'data': '0x00000000000000000000000000000000000000000000000000000000bfa0d4bd',
        # 'position': '0x0',
        # 'topics': ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
        #             '0x000000000000000000000000b4e16d0168e52d35cacd2c6185b44281ec28c9dc',
        #             '0x0000000000000000000000003fc91a3afd70395cd496c647d5a6cc9d4b2b7fad']},


        # PairCreated
        if log['address'] == utils.UNI_V2_FACTORY.lower() and log['topics'][0] == '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9':
            decoded_data, evt_name = _make_decode(w3, AttributeDict({'address':log['address'],'abi':abi}), abi, log)
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'PairCreated', 'hash:', '0x'+tx['hash'].hex(), 'pair:', decoded_data['pair'].lower(), decoded_data)

        # PoolCreated
        if log['address'] == utils.UNI_V3_FACTORY.lower() and log['topics'][0] == '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118':
            decoded_data, evt_name = _make_decode(w3, AttributeDict({'address':log['address'],'abi':abi}), abi, log)
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'PoolCreated', 'hash:', '0x'+tx['hash'].hex(), 'pair:', decoded_data['pool'].lower(), decoded_data)


        logs_array.append({
            'address': log['address'],
            'decoded_data': decoded_data,
            'logIndex': 99, # любой, так как не знаем реал
            'topics_0': log['topics'][0],
            'topics_1': convert_hex(log['topics'][1]) if len(log['topics']) > 1 else None,
            'topics_2': convert_hex(log['topics'][2]) if len(log['topics']) > 2 else None,
            'topics_3': convert_hex(log['topics'][3]) if len(log['topics']) > 3 else None,
        })



    if minted_pair != None:


        print(f"{(time.time() - start_time_e):.3f}s time past since getting tx until calcing logs Mint()")

        start_time = time.time()
        latest_block = requests.post(HTTP, json={"method":"eth_getBlockByNumber","params":["latest",False],"id":1,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']
        print(f"{(time.time() - start_time):.3f}s latest_block ping, block_number: {int(latest_block['number'], 16)}, date: {datetime.fromtimestamp(int(latest_block['timestamp'], 16)).strftime('%Y-%m-%d %H:%M:%S.%f')}, ago: {(time.time() - int(latest_block['timestamp'], 16)):.3f}s")



        address = None
        transfers_array = []

        if tx['value'] != 0:
            transfers_array.append({
                'address': None,
                'from': tx['from'],
                'to': tx['to'],
                'value': tx['value'],
                'value_usd': None,
                'value_usd_per_token_unit': None,
                'category': 'external',
                'status': 1,
            })

        # (!) здесь не хватает добавление internal транзакций

        for log in logs:


            if log['address'] == utils.WETH.lower():

                if log['topics'][0] in ['0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c', '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65']:
                    decoded_data, evt_name = _make_decode(w3, AttributeDict({'address':log['address'],'abi':abi_weth}), abi_weth, log)

                    # Deposit
                    if log['topics'][0] == '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c':
                        transfers_array.append({
                            'address': log['address'],
                            'from': None,
                            'to': decoded_data['dst'],
                            'value': decoded_data['wad'],
                            'value_usd': None,
                            'value_usd_per_token_unit': None,
                            'category': 'erc20',
                            'status': None,
                        })

                    # Withdrawal
                    if log['topics'][0] == '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65':
                        transfers_array.append({
                            'address': log['address'],
                            'from': decoded_data['src'],
                            'to': None,
                            'value': decoded_data['wad'],
                            'value_usd': None,
                            'value_usd_per_token_unit': None,
                            'category': 'erc20',
                            'status': None,
                        })


            # Transfer
            if log['topics'][0] in ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0xe19260aff97b920c7df27010903aeb9c8d2be5d310a2c67824cf3f15396e4c16']:
                
                if minted_pair == convert_hex(log['topics'][2]):
                    # log['topics'][1] = from
                    # log['topics'][2] = to
                    if convert_hex(log['topics'][1]) in [utils.UNISWAP_V2_ROUTER.lower(), utils.UNISWAP_V3_POSITIONS_NFT.lower()]:
                        continue
                    if address != None:
                        print(f"(!) address={address} уже найден через Transfer(), теперь найден второй {convert_hex(log['address'])}")
                        address = None
                    else:
                        address = convert_hex(log['address'])


                _abi = abi
                if log["address"] == utils.WETH.lower(): _abi = abi_weth
                if log["address"] == utils.USDC.lower(): _abi = abi_usdc
                if log["address"] == utils.USDT.lower(): _abi = abi_usdt
                decoded_data, evt_name = _make_decode(w3, AttributeDict({'address':log['address'],'abi':_abi}), _abi, log)

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
                
                if value == None:
                    print(f" \x1b[31;1m(!)\x1b[0m value == None, {decoded_data}")

                if log["address"] == utils.WETH.lower():
                    assert 'wad' in decoded_data, decoded_data
                    transfers_array.append({
                        'address': log['address'],
                        'from': decoded_data['src'],
                        'to': decoded_data['dst'],
                        'value': value,
                        'value_usd': None,
                        'value_usd_per_token_unit': None,
                        'category': 'erc20',
                    })

                elif log["address"] in [utils.USDC.lower(), utils.USDT.lower()]:
                    transfers_array.append( {
                        'address': log['address'],
                        'from': decoded_data['from'],
                        'to': decoded_data['to'],
                        'value': value,
                        'value_usd': None,
                        'value_usd_per_token_unit': None,
                        'category': 'erc20',
                    })

                else:

                    _from = None
                    _to = None
                    if 'from' in decoded_data: _from = decoded_data['from']
                    if 'src' in decoded_data: _from = decoded_data['src']
                    if 'to' in decoded_data: _to = decoded_data['to']
                    if 'dst' in decoded_data: _to = decoded_data['dst']

                    if value != None: # из-за бага на log #2 0x7b826d84f34595cc530a1616792b96118822677e363685d532708e42fb7072bf
                        assert _from != None and _to != None, decoded_data

                    transfers_array.append({
                        'address': log['address'],
                        'from': _from,
                        'to': _to,
                        'value': value,
                        'value_usd': None,
                        'value_usd_per_token_unit': None,
                        'category': 'erc20',
                        # 'extra_data': None,
                        # 'pair_address': None,
                        # 'token1_currency': None,
                        # 'exchange': None,
                    })


        print(f"После Mint() найдена address={address}, pair={minted_pair}")

        if address != None:

            with conn.cursor() as cursor:

                start_time = time.time()
                have_mints_before = utils.have_mints_before(conn, pair=minted_pair, until_block=999999999)
                print(f"{(time.time() - start_time):.3f}s have_mints_before()={have_mints_before}")

                if have_mints_before:
                    return
            

            thread_ether = utils.ThreadWithReturnValue(target=utils.http_request, args=('ether', address))
            thread_ether.start()
            
            print('tx:')
            pprint(dict(tx))
            print('logs:')
            pprint(logs)
            # print('logs_array:')
            # pprint(logs_array)

            
            print('latest_block:')
            pprint(latest_block)



            pending_block = requests.post(HTTP, json={"method":"eth_getBlockByNumber","params":["pending",True],"id":1,"jsonrpc":"2.0"}, headers={"Content-Type": "application/json"}).json()['result']
            # w3.eth.get_block('latest')['baseFeePerGas']

            print(f"pending_block (number={int(pending_block['number'], 16)})")
            #pprint(pending_block)

            # timestamp'], 16),
            #         'baseFeePerGas': int(blocks[0]['baseFeePerGas'], 16),
            #         'gasUsed': int(blocks[0]['gasUsed'], 16),
            #         'gasLimit': int(blocks[0]['gasLimit'], 16),

            transactionIndex = None
            for trans in pending_block['transactions']:
                if trans['hash'] == '0x'+tx['hash'].hex():
                    transactionIndex = int(trans['transactionIndex'], 16)
                    break

            if transactionIndex == None:
                print(f"\x1b[31;1m(!)\x1b[0m transactionIndex = None, {tx}")

            transaction = {
                'from': tx['from'],
                'to': tx['to'],
                'gas': tx['gas'],
                'gasprice': tx['gasPrice'],
                'hash': '0x'+tx['hash'].hex(),
                'maxfeepergas': tx['maxFeePerGas'] if 'maxFeePerGas' in tx else None,
                'maxpriorityfeepergas': tx['maxPriorityFeePerGas'] if 'maxPriorityFeePerGas' in tx else None,
                'nonce': tx['nonce'],
                'value': tx['value'],
                #'contractaddress'
                #'cumulativegasused', 10583983),
                'gasused': None,
                #'status', 1),
                'transactionindex': transactionIndex, # в реале из-за bribe будет другое значение
                'transfers_array': transfers_array,
                'logs_array': logs_array,
                'block_number': int(pending_block['number'], 16) - 1,
                'block_timestamp': int(pending_block['timestamp'], 16),
                'block_basefeepergas': int(pending_block['baseFeePerGas'], 16),
                'block_gasused': int(pending_block['gasUsed'], 16),
                'block_gaslimit': int(pending_block['gasLimit'], 16),
            }

            print('transaction:')
            pprint(transaction)


            start_time = time.time()
            try:
                token = ml.get_token_stat_for_ml(ia=None, address=address, pair=minted_pair, last_block=999999999, realtime=True, bMint_data={'transaction':transaction})
                if token != None and not(type(token) == dict and 'error' in token):
                    pass
            except Exception as e:
                token = None
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} token Exception: {e}, address={address}")
                print(traceback.format_exc())

            if token:
                print(token.blocks_data[list(token.blocks_data.keys())[-1]]['prediction_bMint'])


            start_time = time.time()
            contract_verified = thread_ether.join()
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {(time.time() - start_time):.3f}s доп время потребовалось на thread_ether.join(), contract_verified={contract_verified}")

        
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} time received tx: {datetime.fromtimestamp(start_time_e).strftime('%Y-%m-%d %H:%M:%S.%f')}")
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} tx_hash={transaction['hash']}, address={address}, minted_pair={minted_pair}")
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} {(time.time() - start_time_e):.3f}s time past since getting tx until calcing ml.get_token_stat_for_ml(), latest block number: {int(latest_block['number'], 16)}, ago latest_block: {(time.time() - int(latest_block['timestamp'], 16)):.3f}s, pending block number: {int(pending_block['number'], 16)}")
            date_pending_block = datetime.fromtimestamp(int(latest_block['timestamp'], 16) + 12.0)
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} pending block must be at {date_pending_block.strftime('%Y-%m-%d %H:%M:%S.%f')}, time left: {((date_pending_block - datetime.now()).total_seconds()):.3f}s")
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} transactionIndex={transactionIndex} in pending block")
            
            mints_logger.info(f"----")
            mints_logger.info(f"time received tx: {datetime.fromtimestamp(start_time_e).strftime('%Y-%m-%d %H:%M:%S.%f')}")
            mints_logger.info(f"tx_hash={transaction['hash']}, address={address}, minted_pair={minted_pair}")
            mints_logger.info(f"{(time.time() - start_time_e):.3f}s time past since getting tx until calcing ml.get_token_stat_for_ml(), latest block number: {int(latest_block['number'], 16)}, ago latest_block: {(time.time() - int(latest_block['timestamp'], 16)):.3f}s, pending block number: {int(pending_block['number'], 16)}")
            assert int(latest_block['number'], 16) + 1 == int(pending_block['number'], 16)
            mints_logger.info(f"pending block must be at {datetime.fromtimestamp(int(latest_block['timestamp'], 16) + 12.0).strftime('%Y-%m-%d %H:%M:%S.%f')}, time left: {((date_pending_block - datetime.now()).total_seconds()):.3f}s")
            mints_logger.info(f"transactionIndex={transactionIndex} in pending block")
            if token:
                mints_logger.info(f"prediction: {token.blocks_data[list(token.blocks_data.keys())[-1]]['prediction_bMint']}, contract_verified={contract_verified}")



        # (!) не забывать проверять verified contract по etherscan


        print('----')

        




if __name__ == "__main__":

    try:
        asyncio.run(get_event())
    finally:
        conn.close()



