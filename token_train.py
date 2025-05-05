import os
import sys
from web3 import Web3
from web3._utils.events import get_event_data
import requests
import json
import traceback
from pprint import pprint
import pandas as pd
import time
import random
import pickle
import string
import copy
from datetime import datetime
from eth_utils import to_checksum_address, from_wei
from hexbytes import HexBytes
import tabulate
from multiprocessing import Pool, cpu_count

import ml
from token_stat import Token

import psycopg2
from psycopg2.extras import Json, DictCursor, RealDictCursor

import utils
from utils import FILES_DIR, FILE_LABELS





def wrap_get_token_stat_for_ml(*kwards):

    print('kwards:', kwards)

    try:
        ml.get_token_stat_for_ml(*kwards)
    except Exception as e:
        print(f"Exception address={kwards[1]}: {e}")
        print(traceback.format_exc())
    
    return


def clean_db_stat(list_addresses):
    with psycopg2.connect(**utils.db_config_psql, **utils.keepalive_kwargs) as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute("""
                    DELETE
                    FROM stat_wallets_address
                    WHERE token_address in %s
                """, (tuple(map(str.lower, list_addresses)),))
                conn.commit()
            except:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} DELETE FROM stat_wallets_address rollback(): {sys.exc_info()}")
                print(traceback.format_exc())
                conn.rollback()


if __name__ == '__main__':



    testnet = 'http://127.0.0.1:8545'

    w3 = Web3(Web3.HTTPProvider(f"{testnet}/?t"))
    if not w3.is_connected():
        raise Exception("w3 not connected")


    
    os.makedirs(f"{ml.TOKEN_TRAIN_SAVE_DIR}_all", exist_ok=True)
    os.makedirs(f"{ml.TOKEN_TRAIN_SAVE_DIR}_bMint", exist_ok=True)
    for i in range(0, ml.STOP_SAVE_BLOCKS_ON + 1):
        os.makedirs(f"{ml.TOKEN_TRAIN_SAVE_DIR}_b{i}", exist_ok=True)
    for i in range(1, ml.STOP_TRAIN_ON_TRADES // ml.TRADES_STEP_STAT + 2):
        os.makedirs(f"{ml.TOKEN_TRAIN_SAVE_DIR}_step{i}", exist_ok=True)

    LAST_BACKTESTING_BLOCK = w3.eth.block_number



    start_time_e = time.time()


    df = pd.read_csv(f"{FILES_DIR}/temp/target-column_22jun_2jul.csv")
    list_addresses = df['token_address'].tolist()
    print(df)


    clean_db_stat(list_addresses)

    print(f"len list_addresses={len(list_addresses)}")

    params = []
    for ia, address in enumerate(list_addresses):
        params.append((ia, address, None, LAST_BACKTESTING_BLOCK, False,))


    with utils.NestablePool(20) as pool:
        pool.starmap_async(wrap_get_token_stat_for_ml, params).get()



    clean_db_stat(list_addresses)



        
    print("executied in %.3fs" % (time.time() - start_time_e))