import os
import sys
import requests
import json
import traceback
from pprint import pprint
import numpy as np
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
from os import listdir
from os.path import isfile, join


import utils
from utils import FILES_DIR, FILE_LABELS




if __name__ == '__main__':



    start_time_e = time.time()
    print('start training..')

    TRAIN_FOLDER_CAT = "_step4"
    path_stats = f"{FILES_DIR}/temp/stats_24.06/{TRAIN_FOLDER_CAT}"
    files = [f for f in listdir(path_stats) if isfile(join(path_stats, f))]
    data_stat = []
    for file in files:
        with open(f"{path_stats}/{file}", "r") as f:
            # print('file:', file)
            try:
                data_stat.append({'token_address': file.split('.')[0].split('_')[0].lower()} | json.load(f))
            except json.decoder.JSONDecodeError:
                print(f"file {file} loaded with JSONDecodeError")

    print(f"len files={len(files)}, TRAIN_FOLDER_CAT={TRAIN_FOLDER_CAT}")


    df = pd.DataFrame.from_records(data_stat)



    equal_cols = []
    for col in df.columns:
        a = df[col].to_numpy() # s.values (pandas<0.24)
        if (a[0] == a).all():
            equal_cols.append(col)
    print('equal_cols:', equal_cols)
    df.drop(equal_cols, axis=1, inplace=True)
 
    print(df)
    print(df.info())



    df_target = pd.read_csv(f"{FILES_DIR}/temp/target-column_v24.06.csv", header=0)
    print(f"len target=1 is {len(df[df_target['TARGET'] == 1])}, target=0 is {len(df[df_target['TARGET'] == 0])}")
    target_dict = dict(zip(df_target['token_address'], df_target['TARGET']))


    correct_1 = 0
    not_correct_0 = 0
    for stat in data_stat:

        #if 'block1_trades_buy_div_bfft' in stat and stat['block1_trades_buy_div_bfft'] >= 5.0:
        if 'banana_trans_perc_median' in stat and stat['banana_trans_perc_median'] >= 0.50:

            if target_dict[stat['token_address']] == 1:
                correct_1 += 1
            else:
                not_correct_0 += 1
                print(f"{stat['token_address']} - target=0, but predicted")

    print(f"correct_1={correct_1}, not_correct_0={not_correct_0} ({round(100*not_correct_0/(correct_1+not_correct_0))}%)")