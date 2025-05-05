import websockets
import asyncio
import json
import os
import sys
import traceback
import time
import random
import string
import socket
from datetime import datetime
from os import listdir
from os.path import isfile, join

from multiprocessing import Pool
import multiprocessing.pool
from threading import Thread

from web3 import Web3

import sync_blocks

import utils
from utils import FILES_DIR


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


from cysystemd import journal



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





testnet = f'http://127.0.0.1:{utils.GETH_PORT}'


async def get_event():


    with sync_blocks.conn.cursor() as cursor:
        try:
            cursor.execute("""
                DELETE
                FROM last_trades_token_status
                WHERE script='realtime'
            """)
            sync_blocks.conn.commit()
        except Exception as e:
            print(traceback.format_exc())
            sync_blocks.conn.rollback()
            exit()


    if 1:
        path = f"{FILES_DIR}/lib/logs_strg"
        files = [f for f in listdir(path) if isfile(join(path, f))]
        for file in files:
            os.remove(f"{path}/{file}")

    if 0:
        path = f"/disk_sdc/last_trades_objects"
        files = [f for f in listdir(path) if isfile(join(path, f))]
        for file in files:
            os.remove(f"{path}/{file}")
        
    if 0:
        path = f"/disk_sdc/df_features_catboost"
        files = [f for f in listdir(path) if isfile(join(path, f))]
        for file in files:
            os.remove(f"{path}/{file}")

    if 1:
        open("/home/root/python/ethereum/lib/logs/last_trades.log", 'w').close()
        # open("/home/root/python/ethereum/lib/logs/service_new_blocks_error.log", 'w').close()
        # open("/home/root/python/ethereum/lib/logs/service_new_blocks_output.log", 'w').close()
        open("/home/root/python/ethereum/lib/logs/strategy_errors.log", 'w').close()
        # os.remove("/home/root/python/ethereum/lib/logs/last_trades.log")
        # os.remove("/home/root/python/ethereum/lib/logs/service_new_blocks_error.log")
        # os.remove("/home/root/python/ethereum/lib/logs/service_new_blocks_output.log")
        # os.remove("/home/root/python/ethereum/lib/logs/strategy_errors.log")


    sb = sync_blocks.SyncBlock(testnet=testnet, realtime=True, verbose=0)

    with NestablePool(100) as pool:
        while True:
            try:
                journal.write('делаю коннект async websockets.connect()')
                async with websockets.connect(f"ws://127.0.0.1:{utils.GETH_PORT + 1}/") as ws:
                    await ws.send('{"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}')
                    subscription_response = json.loads(await ws.recv())
                    print(subscription_response)
                    info_logger.info(subscription_response)
                    journal.write(subscription_response)

                    try:
                        while True:
                            message = json.loads(await ws.recv())
                            block_number = int(message['params']['result']['number'], 16)
                            journal.write(f"[Block] {block_number} socket received")

                            thread = Thread(target=on_message, args=(sb, pool, block_number))  # create the thread
                            thread.start() # start the thread


                    except:
                        journal.write('eth_newHeads: ошибка while True')
                        journal.write(sys.exc_info())
                        journal.write(traceback.format_exc())
                        print(sys.exc_info())
                        print('~'*60, file=sys.stderr)
                        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} Exception eth_newHeads.py", file=sys.stderr)
                        print(traceback.format_exc(), file=sys.stderr)
                        try:
                            await ws.send('{"jsonrpc": "2.0", "id": 1, "method": "eth_unsubscribe", "params": ["' + subscription_response['result'] + '"]}')
                        except:
                            journal.write(f'eth_newHeads: ошибка eth_unsubscribe {sys.exc_info()[0]}')


                await asyncio.sleep(1)
            except ConnectionRefusedError as e:
                journal.write(f'eth_newHeads: ошибка ConnectionRefusedError {e}')
                await asyncio.sleep(4)
            except:
                journal.write(f'eth_newHeads: ошибка общая {sys.exc_info()[0]}')
                await asyncio.sleep(4)




def on_message(sb, pool, block_number):

    try:
        sb.exec_block(pool, block_number)
    except:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} EXCEPTION on_message() {sys.exc_info()}")
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} on_message() Exception {sys.exc_info()}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)



if __name__ == "__main__":

    os.makedirs(f"{FILES_DIR}/lib/logs_strg", exist_ok=True)

    try:
        asyncio.run(get_event())
    finally:
        sync_blocks.conn.close()
        print('all closed')