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

# from keras.models import Sequential
# from keras.preprocessing.sequence import pad_sequences
# from keras.layers import Embedding,Dense,LSTM,Dropout,Flatten,BatchNormalization,Conv1D,GlobalMaxPooling1D,MaxPooling1D
# from keras.optimizers import SGD, Adam
# from keras.regularizers import l2
# from keras.optimizers import Adam
# from keras import regularizers
# from keras.callbacks import EarlyStopping
# from sklearn.preprocessing import OneHotEncoder
# from keras.preprocessing import sequence

import optuna

from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import log_loss, roc_auc_score, accuracy_score, mean_squared_error, classification_report, confusion_matrix, recall_score
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier, ExtraTreesClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from catboost import CatBoostClassifier


import utils
from utils import FILES_DIR, FILE_LABELS



def objective(trial):

    param = {
        "iterations": 100,
        "objective": trial.suggest_categorical("objective", ["Logloss", "CrossEntropy"]),
        "colsample_bylevel": trial.suggest_float("colsample_bylevel", 0.01, 0.1),
        "depth": trial.suggest_int("depth", 1, 13),
        "boosting_type": trial.suggest_categorical("boosting_type", ["Ordered", "Plain"]),
        "bootstrap_type": trial.suggest_categorical(
            "bootstrap_type", ["Bayesian", "Bernoulli", "MVS"]
        ),

        "learning_rate": trial.suggest_float("learning_rate", 1e-3, 1e-1, log=True),
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1e-8, 100.0, log=True),
        "random_strength": trial.suggest_float("random_strength", 1e-8, 10.0, log=True),
        "od_type": trial.suggest_categorical("od_type", ["IncToDec", "Iter"]),
        "od_wait": trial.suggest_int("od_wait", 10, 50),
    }

    if param["bootstrap_type"] == "Bayesian":
        param["bagging_temperature"] = trial.suggest_float("bagging_temperature", 0, 10)
    elif param["bootstrap_type"] == "Bernoulli":
        param["subsample"] = trial.suggest_float("subsample", 0.1, 1)

    gbm = CatBoostClassifier(**param)

    gbm.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=1, early_stopping_rounds=30)

    preds = gbm.predict(X_test)
    pred_labels = np.rint(preds)
    accuracy = accuracy_score(y_test, pred_labels)
    recall = recall_score(y_test, pred_labels)
    # return accuracy
    return recall



if __name__ == '__main__':



    start_time_e = time.time()
    print('start training..')

    TARGET_FILE = "target-column_1apr_22jun"
    TRAIN_FOLDER_MAIN = "stats_1apr_22jun"
    TRAIN_FOLDER_CAT = "_bMint"
    path_stats = f"{FILES_DIR}/temp/{TRAIN_FOLDER_MAIN}/{TRAIN_FOLDER_CAT}"
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


    # if df[f'holders_before_mint_percentages_max'].isnull().values.any():
    #     print('NaN values in dataframe:')
    #     #print(dataframe[dataframe.isna().any(axis=1)])
    #     print(df.loc[dataframe[f'holders_before_mint_percentages_max'].isnull()])


    if f'holders_before_mint_percentages_max' in df.columns: df[f'holders_before_mint_percentages_max'] = df[f'holders_before_mint_percentages_max'].astype(float)
    if f'holders_before_mint_percentages_min' in df.columns: df[f'holders_before_mint_percentages_min'] = df[f'holders_before_mint_percentages_min'].astype(float)
    if f'holders_before_mint_percentages_median' in df.columns: df[f'holders_before_mint_percentages_median'] = df[f'holders_before_mint_percentages_median'].astype(float)

    # if f'b{i}_token1_address' in df.columns:
    #     df.loc[df[f'b{i}_token1_address'] == utils.WETH, f'b{i}_token1_address'] = "WETH"
    #     df.loc[df[f'b{i}_token1_address'] == utils.USDC, f'b{i}_token1_address'] = "USDC"
    #     df.loc[df[f'b{i}_token1_address'] == utils.USDT, f'b{i}_token1_address'] = "USDT"

    for var in ['']:
        df.loc[df[f'{var}token1_address'] == utils.WETH, f'{var}token1_address'] = "WETH"
        df.loc[df[f'{var}token1_address'] == utils.USDC, f'{var}token1_address'] = "USDC"
        df.loc[df[f'{var}token1_address'] == utils.USDT, f'{var}token1_address'] = "USDT"

        df_token1_address = pd.get_dummies(df[f'{var}token1_address'])
        df = pd.concat([df, df_token1_address], axis=1).reindex(df.index)
        df.drop(f'{var}token1_address', axis=1, inplace=True)

        df_exchange = pd.get_dummies(df[f'{var}exchange'])
        df = pd.concat([df, df_exchange], axis=1).reindex(df.index)
        df.drop(f'{var}exchange', axis=1, inplace=True)
    
    # if 'o_moralis_perc_relat_tot_supply_avg' in df.columns:
    # df['o_moralis_perc_relat_tot_supply_avg'] = df['o_moralis_perc_relat_tot_supply_avg'].astype(float)
    # df['o_moralis_perc_relat_tot_supply_median'] = df['o_moralis_perc_relat_tot_supply_median'].astype(float)

    equal_cols = []
    for col in df.columns:
        a = df[col].to_numpy() # s.values (pandas<0.24)
        if (a[0] == a).all():
            equal_cols.append(col)
    print('equal_cols:', equal_cols)
    df.drop(equal_cols, axis=1, inplace=True)
 
    print(df)
    print(df.info())
    # print(list(df.columns))
    print(*df.select_dtypes('object').columns.to_list(), sep='\n')


    # missing values in catboost https://www.geeksforgeeks.org/handling-missing-values-with-catboost/




    #df_atl = pd.read_csv(f"{FILES_DIR}/temp/learn-data_2024-05-07_10_06.csv", header=0)
    df_atl = pd.read_csv(f"{FILES_DIR}/temp/{TARGET_FILE}.csv", header=0)
    target_dict = dict(zip(df_atl['token_address'], df_atl['TARGET']))
    # df_atl = df_atl.select_dtypes(exclude=['object'])
    # df_atl_columns = list(set(df_atl.columns) - set(['TARGET']))
    print('df_atl:')
    print(df_atl)
    print(df_atl.info())

    df = pd.merge(df, df_atl[['token_address', 'TARGET']], how='left', on='token_address')
    df = df.dropna(subset = ['TARGET'])
    df['TARGET'] = df['TARGET'].astype(int)
    # df = df.select_dtypes(exclude=['object'])

    # df[df.select_dtypes(np.float64).columns] = df.select_dtypes(np.float64).astype(np.float32)

    for col in df.columns:
        if 'moralis' in col:
            df.drop(col, axis=1, inplace=True)
    
    # df = df.dropna(axis=0, how='any')

    # df.dropna(inplace=True)
    df.replace(np.nan, 0, inplace=True)

    print('df after merge:')
    print(df)
    print(df.info())


    print('df nan columns:')
    print(df.columns[df.isna().any()].tolist())



    for col in df:
        # get the maximum value in column
        # check if it is less than or equal to the defined threshold
        threshold = 10 ** 30
        if df[col].dtype != object and df[col].max() >= threshold:
            _len_found = len(df[(df[col] >= threshold)])
            print(f"у {col} (их {_len_found}) max значение {df[col].max()}")
            if _len_found > 100:
                df = df.drop([col], axis=1)
            else:
                df.drop(df[df[col] >= threshold].index, inplace=True)

    print("DF CROPPED...")
    print(df)


    # print("np.isnan(X):", np.isnan(df))
    # print(np.where(np.isnan(df)))
    # exit()

    #print(np.where(df.values >= np.finfo(np.float32).max))
    #exit()



    X = df.drop('TARGET', axis=1)
    y = df['TARGET']
    y = y.astype(int)

    #X.fillna(X.mean(), inplace=True)

    # scaler = MinMaxScaler(feature_range=(-1, 1))
    # X_scaled_data = scaler.fit_transform(X)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, random_state=42) # 0.2
    X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.01, random_state=1) # 0.25

    # print(f"X_test token_address ({len(X_test['token_address'])}):")
    # print(X_test['token_address'].to_list())

    print(f"X_val token_address ({len(X_val['token_address'])}):")
    X_val_addresses = X_val['token_address'].to_list()
    print(X_val_addresses)

    X_train = X_train.select_dtypes(exclude=['object'])
    X_test = X_test.select_dtypes(exclude=['object'])
    X_val = X_val.select_dtypes(exclude=['object'])

    print(f"lens: X={len(X)} df_atl={len(df_atl)}")
    print(f"X_train={len(X_train)} ({round(100*len(X_train)/len(X))}%), X_test={len(X_test)} ({round(100*len(X_test)/len(X))}%), X_val={len(X_val)} ({round(100*len(X_val)/len(X))}%)")

    print('X_train:')
    print(type(X_train))
    print(X_train)
    print(list(X_train.columns.values))

    if 0:

        study = optuna.create_study(direction="maximize", pruner=optuna.pruners.MedianPruner())
        study.optimize(objective, n_trials=250, timeout=120*60) # timeout=600

        print("Number of finished trials: {}".format(len(study.trials)))

        print("Best trial:")
        trial = study.best_trial

        print("  Value: {}".format(trial.value))

        print("  Params: ")
        for key, value in trial.params.items():
            print("    {}: {}".format(key, value))
            

        exit()

    if 1:

        cat_features = X_train.select_dtypes(include=['object']).columns.tolist()
        
        model = CatBoostClassifier(cat_features=cat_features, iterations=1500, verbose=25)
        
        # Trial 9 finished with value: 0.9140401146131805 and parameters: {'objective': 'CrossEntropy', 'colsample_bylevel': 0.04978010914140293, 'depth': 12, 'boosting_type': 'Ordered', 'bootstrap_type': 'MVS', 'learning_rate': 0.031309633434542204, 'l2_leaf_reg': 5.705903360654442, 'random_strength': 0.002159609717342481, 'od_type': 'IncToDec', 'od_wait': 44}
        #params = {'objective': 'CrossEntropy', 'colsample_bylevel': 0.04978010914140293, 'depth': 12, 'boosting_type': 'Ordered', 'bootstrap_type': 'MVS', 'learning_rate': 0.031309633434542204, 'l2_leaf_reg': 5.705903360654442, 'random_strength': 0.002159609717342481, 'od_type': 'IncToDec', 'od_wait': 44}
        #model = CatBoostClassifier(cat_features=cat_features, iterations=500, verbose=5, **params)

        #Trial 199 finished with value: 0.9054441260744985 and parameters: {'objective': 'CrossEntropy', 'colsample_bylevel': 0.07406824844373386, 'depth': 6, 'boosting_type': 'Ordered', 'bootstrap_type': 'Bernoulli', 'learning_rate': 0.020078551561591593, 'l2_leaf_reg': 0.9544373807946481, 'random_strength': 1.0714448379656006e-06, 'od_type': 'IncToDec', 'od_wait': 11, 'subsample': 0.25307851267597753}
        # params = {'objective': 'CrossEntropy', 'colsample_bylevel': 0.07406824844373386, 'depth': 6, 'boosting_type': 'Ordered', 'bootstrap_type': 'Bernoulli', 'learning_rate': 0.020078551561591593, 'l2_leaf_reg': 0.9544373807946481, 'random_strength': 1.0714448379656006e-06, 'od_type': 'IncToDec', 'od_wait': 11, 'subsample': 0.25307851267597753}
        # model = CatBoostClassifier(cat_features=cat_features, iterations=4000, verbose=5, **params)

        # [I 2024-07-04 16:59:29,799] Trial 0 finished with value: 0.36046511627906974 and parameters:
        # params = {'objective': 'CrossEntropy', 'colsample_bylevel': 0.03902474967841774, 'depth': 4, 'boosting_type': 'Ordered', 'bootstrap_type': 'Bayesian', 'learning_rate': 0.020117756813521067, 'l2_leaf_reg': 15.688929328115798, 'random_strength': 2.033570406566354, 'od_type': 'IncToDec', 'od_wait': 18, 'bagging_temperature': 0.7604533421644777}

        # Trial 23 finished with value: 0.4011627906976744 and parameters:
        # params = {'objective': 'Logloss', 'colsample_bylevel': 0.05894583933360985, 'depth': 4, 'boosting_type': 'Ordered', 'bootstrap_type': 'Bernoulli', 'learning_rate': 0.040869754041051254, 'l2_leaf_reg': 88.15204326761669, 'random_strength': 1.3327243607235887e-06, 'od_type': 'Iter', 'od_wait': 29, 'subsample': 0.20409631017376867}
        # model = CatBoostClassifier(cat_features=cat_features, iterations=3600, verbose=25, **params)

        # Trial 186 finished with value: 0.436046511627907 and parameters:
        # params = {'objective': 'Logloss', 'colsample_bylevel': 0.04275526360790738, 'depth': 5, 'boosting_type': 'Ordered', 'bootstrap_type': 'Bernoulli', 'learning_rate': 0.08304747530009979, 'l2_leaf_reg': 98.58544052977169, 'random_strength': 1.5282240628730382e-07, 'od_type': 'IncToDec', 'od_wait': 31, 'subsample': 0.5910233045384393}
        # model = CatBoostClassifier(cat_features=cat_features, iterations=1500, verbose=25, **params)

        model.fit(X_train, y_train)
        class_predictions = model.predict(X_test)
        probability_predictions = model.predict_proba(X_test)

        print('get_best_score():', model.get_best_score())

        log_loss_value = log_loss(y_test, probability_predictions[:,1])
        print(f'Log Loss: {log_loss_value}')

        roc_auc = roc_auc_score(y_test, probability_predictions[:,1])
        print(f'ROC AUC: {roc_auc}')

        class_report = classification_report(y_test, class_predictions)
        print(f'Classification Report:\n {class_report}')

        print(f"accuracy_score: {accuracy_score(y_test, class_predictions)}")

        correct_0 = 0
        total_0 = 0
        correct_1 = 0
        total_1 = 0
        y_test_list = y_test.values.tolist()
        for i, _ in enumerate(y_test_list):
            if y_test_list[i] == 0: total_0 += 1
            else: total_1 += 1
            if y_test_list[i] == 0 and class_predictions[i] == 0: correct_0 += 1
            if y_test_list[i] == 1 and class_predictions[i] == 1: correct_1 += 1
        print(f"correct_0: {correct_0} / {total_0} ({round(100*correct_0/total_0)}%) {total_0 - correct_0}")
        print(f"correct_1: {correct_1} / {total_1} ({round(100*correct_1/total_1)}%)")
        

        print('---')

        class_predictions_val = model.predict(X_val)
        print(f"accuracy_score val: {accuracy_score(y_val, class_predictions_val)}")

        correct_0 = 0
        total_0 = 0
        correct_1 = 0
        total_1 = 0
        y_val_list = y_val.values.tolist()
        for i, _ in enumerate(y_val_list):
            if y_val_list[i] == 0: total_0 += 1
            else: total_1 += 1
            if y_val_list[i] == 0 and class_predictions_val[i] == 0: correct_0 += 1
            if y_val_list[i] == 1 and class_predictions_val[i] == 1: correct_1 += 1



        print(f"correct_0 val: {correct_0} / {total_0} ({round(100*correct_0/total_0)}%) {total_0 - correct_0}")
        print(f"correct_1 val: {correct_1} / {total_1} ({round(100*correct_1/total_1)}%)")

        class_report = classification_report(y_val, class_predictions_val)
        print(f'Classification Report val:\n {class_report}')




        # print('class_predictions:')
        # print(class_predictions)

        # print('probability_predictions:')
        # print(probability_predictions)

        # print(f'rmse: {np.sqrt(mean_squared_error(y_test, class_predictions))}')


        print()
        feature_importance = model.feature_importances_
        sorted_idx = np.argsort(feature_importance)
        feat_list = []
        for si in reversed(sorted_idx[-30:]):
            print(f"{np.array(X_test.columns)[si]} {feature_importance[si]}")
        for si in reversed(sorted_idx):
            feat_list.append({'feature': np.array(X_test.columns)[si], 'importance': feature_importance[si]})
        print()
        pd.DataFrame.from_records(feat_list).to_csv('feature_importance.csv', index=False)

        model.save_model(f'{FILES_DIR}/lib/models/{TRAIN_FOLDER_MAIN}/{TRAIN_FOLDER_CAT}')



        if 1 or TRAIN_FOLDER_CAT == "_all":

            print('-'*30)

            altantes_tokens = ['0x716bb5e0839451068885250442a5b8377f582933','0xa0bbbe391b0d0957f1d013381b643041d2ca4022','0xca530408c3e552b020a2300debc7bd18820fb42f','0x2b8aac1630f7bc0c4b1ed8036c0fe0d71cb44709','0xaa95f26e30001251fb905d264aa7b00ee9df6c18','0x14fee680690900ba0cccfc76ad70fd1b95d10e16','0x0b61c4f33bcdef83359ab97673cb5961c6435f4e','0x44971abf0251958492fee97da3e5c5ada88b9185','0xc6221ac4e99066ea5443acd67d6108f874e2533d','0x0a2c375553e6965b42c135bb8b15a8914b08de0c','0x67c4d14861f9c975d004cfb3ac305bee673e996e','0x3927fb89f34bbee63351a6340558eebf51a19fb8','0x9fd9278f04f01c6a39a9d1c1cd79f7782c6ade08','0x5888641e3e6cbea6d84ba81edb217bd691d3be38','0x3ffeea07a27fab7ad1df5297fa75e77a43cb5790','0x292fcdd1b104de5a00250febba9bc6a5092a0076','0x6942806d1b2d5886d95ce2f04314ece8eb825833','0x32f0d04b48427a14fb3cbc73db869e691a9fec6f','0xe1a7099f22455b02d226cad9c3510aa169c34b28','0x9ebb0895bd9c7c9dfab0d8d877c66ba613ac98ea','0x8390a1da07e376ef7add4be859ba74fb83aa02d5','0x68bbed6a47194eff1cf514b50ea91895597fc91e','0x40e9187078032afe1a30cfcf76e4fe3d7ab5c6c5','0x0fe13ffe64b28a172c58505e24c0c111d149bd47','0x6d68015171eaa7af9a5a0a103664cf1e506ff699','0x1258d60b224c0c5cd888d37bbf31aa5fcfb7e870','0xc06bf3589345a81f0c2845e4db76bdb64bbbbc9d','0x6982508145454ce325ddbe47a25d4ec3d2311933','0x482702745260ffd69fc19943f70cffe2cacd70e9','0x571e21a545842c6ce596663cda5caa8196ac1c7a','0x1fdd61ef9a5c31b9a2abc7d39c139c779e8412af','0x85f7cfe910393fb5593c65230622aa597e4223f1','0x4c11249814f11b9346808179cf06e71ac328c1b5','0x9cf0ed013e67db12ca3af8e7506fe401aa14dad6','0x7039cd6d7966672f194e8139074c3d5c4e6dcf65','0x067def80d66fb69c276e53b641f37ff7525162f6','0x38e68a37e401f7271568cecaac63c6b1e19130b4','0xb549116ac57b47c1b365a890e1d04fd547dfff97','0xd13cfd3133239a3c73a9e535a5c4dadee36b395c','0x549020a9cb845220d66d3e9c6d9f9ef61c981102','0x1131d427ecd794714ed00733ac0f851e904c8398','0xebb1afb0a4ddc9b1f84d9aa72ff956cd1c1eb4be','0x1946d29fc571a482059a939241e19053bdd6e49b','0x6e79b51959cf968d87826592f46f819f92466615','0x6968676661ac9851c38907bdfcc22d5dd77b564d','0x1ce270557c1f68cfb577b856766310bf8b47fd9c','0x1cc7047e15825f639e0752eb1b89e4225f5327f2','0x7d4a23832fad83258b32ce4fd3109ceef4332af4','0x66bff695f3b16a824869a8018a3a6e3685241269','0xf629cbd94d3791c9250152bd8dfbdf380e2a3b9c','0x6b1a8f210ec6b7b6643cea3583fb0c079f367898','0xe85ebdd58d713ab1a8d6a0db1e8d7be54f1c332f','0xac6379d45eb659822a68a3db81486372433143b1','0x7c7f3385464a21eee2884a0cf6411f5953b9f408','0xdead61337552d8572f3b229284a76086a293dead','0x46c921980d35f31d0a615f33335626f7e6396b88','0xa46e1e0cf3df66e3eb106ea831db67c582a616bd','0x6a698f5f692b6ef238793d402e25c2f313137138','0x604555a88d68e156aa52365080b8d8cba1987de3','0xcb07732abf1d551b6899087448b2b5c0ed3d4fa7','0x516f151a5dea4066cf7281011499c4ab12695b74','0x6969692088bff068d6f3e2c5eddc3dda8182870c','0x56fbbf5b6a0b7118dbc228fe5d3f4d63c2f9e6ae','0x1086acd443b25119e05a49b44010f800bc556d75','0xb27845fcf60f45a83ff73cf13c0023ad5aa18f1c','0xff681856005b25298c0af2499ac3c163f1be3922','0xfd9cf74750d06a76983cc3e94588843fa13b128e','0xf68bc2652d44d66e7ac864fedbf51bfd8179f757','0x1b80eb9afbf62499cc224c118e159aacc64f1f11','0xedc3343de83c35af2dc8ea284836c490f476372c','0x7c6f42c62f80d677c2a77f9e9ad02992c0bd99a6','0x894f8c4258bd09f3973c275c0df71775173437e3','0x69477b93b13f798e2cdcfd7e1cfc9b32db38e78c','0xd4d6af40329ccc59fe661283d0ed707e2761fdbc','0x2d2f2ccbbcc23d142e4a59154b3f5d8d0ea7cd55','0xc7d899ed34f506a76a2a25f23cee2f4a67db53ec','0x7d5d4fff14c285c96810336fc661df1bca2dd15c','0x2b7c0fa747611d4412b54076c62119926474edb3','0x286675fed193e3cfb20bfa3ba12db56e9231a0f7','0xa8e9d8fa0e0f0bc822383c2ab81d1dcefb1d74cd','0xc125ec4827620e2332046f9e0d30ec23f6084fd6','0x2005396cdf6de574d582cfb9e6b3e41f176014fc','0x1ba6b8144d471b7decc638fe794c0bf9ad278297','0xcb97c0c6998f365e096a2b8494704d6b455ad5a6','0x3c91cc527de238222496e95fed714b5de5726f10','0x1ab46d323d200eaa372d4860cf242c82b8301a96','0xa2b8e02ce95b54362f8db7273015478dd725d7e7','0xc3c3566a5b76958ebdd7b0a5c9daf92e362d1751','0x69420312905cf6ee33f3d0dea4aeef72a13f8473','0xeacad96537c0c5647f6b9e2f1ebc0ba9731a57f6','0xb033793679f43777b638bdb1f4cf50fe1d55c9f0','0x40bb1c8933b0cfef0ab5670814446a1ec1834d6f','0xc7215965beb89785b351c62761a1ffdee0ec31da','0x7d702984a9577472941756256595639d643db2c6','0xf373af20eddce50573d6ecc3bc207f87511bdfb4','0x7b28af7e9519684839bf2d804e50aa0c1ff43679','0xdb513a25d5e8342da083a0162e1cf1bcc760c353','0x77146a56bde1bbbdf7aea71b24f63362ff423e3e','0x3a074f5186c057047aea78d8a58bda8d88eee9a2','0xce813ee7d2542e22e2a3556e994ef81f13177156','0xafb698e325bb77e01dd4b74f9bbac561e55cf06f','0x53288aa471511fb0de8bfe17923f8eca64607a54','0x63b7ed40185a3e925b172e9be73f1549322b4d34','0xb6fe105ab68f9d38d61248f7b9042879a9e606c7','0xd966f70fa6f9e3b3b1c159f45c6e795054d034cc','0xead368e880156a37dec707dbf571567945f0a00d','0xbd2466046d22305dcb6dfe37a1ef9b4901440c82','0x2ae61ed5d4da30b40c8ebad2f88c5a2f9786d0ab','0x81725c66de2d405dce99c1c6c473fd8e64f1581f','0x039d4f77dd7ae619e09143cb225f2ac47198e774','0xe14b7a963925e451761166ad08dc6118eceddb9a','0x7366d7fe757e812a3136e39d590d1d6225113e0f','0xedd90e025b390442e1b6450f84031580c9dba53e','0xbaaa693046075ffc31f23637968b95a426c54006','0x420698cfdeddea6bc78d59bc17798113ad278f9d','0x5108f417288f87e8bd39786029b23f9f245f315a','0x4a4cd9c3f86e61ee87c64b63fdb4921564765ed1','0x78b9982f394bf699c3dcf07a693d0f1a0110ce9a','0xdf0669651b53c8e0f39e9a103eec23f8ffc1d4fb','0x9ce654b2046a3560da09238cee6effc97a3017f8','0xf0c59fd9d0c4d73f31f49a085442ec177952a317','0x8d661a27e0ed280f3870da12884e9fc359b2c9d8','0x60dc196cf14f802f25d7456ea3c8a6846943dfc4','0xd8c108d8b8076afcabc54399aad206148a81d68b','0x0ac9e8d8b3e87ace7938539c49cd7f7c6ceb3f20','0xe315c322f17fc12d23bebde1acd22854323d3c46','0x521af22156ad79547a2fd2bb5a00014526e7cf80','0x42102e43ac23a5d5a0c0044371228a577add0a47','0xb699c99063827a003dde7203678f1cbd4dd2596f','0xd62bbc1abdd0dfa1f33afffcf3891b9f10b22f31','0x56a187c49011bafb01e9c32ce90b5d259bee0c12','0x0000721475421300e285d0a33f660717da058c21','0x9d20731c8cb82cc6264711cf5a5f83196b9b1ce4','0x15f34c43b13913577e76f2d872d5373f8b9012a9','0x0dbb0b1cc1b4bbf414511b2a9be5ab7921189356','0x959440d319304bbb793dfe59b01b65184bcce7e3','0xc3f78ecaf481eae4eb32d43233b3a60469d5dfa9','0xe0ca9e4b62e4ae4d80f49315ae8479a5a76184ac','0x1d1ce9b1273d967a12ec4572e6a27eb7ac32a2db','0x5505fa45da599eab7fcc7b8c579c6d3509646fb7','0xda5a9d2267be1aaee97ea7045be4c297adda6224','0x1fb43088cd99908d49408ef6f2af8dba30c60595','0x3b991130eae3cca364406d718da22fa1c3e7c256','0xabb41328eb3b89465cc1d6638f6b6b828adcc96d','0x641cf8d6d42304eeb7fe4dcfbb18bb8a539389d1','0x7777770e1a291207c6fc886baa9ea85500767914','0x69691c7e9b7c055ccaa795ad4a8c2c1ba5d9af69','0x4206984834ca2001ca4b203d3a7dd3edd6185b1a','0x38b9cdf27192e838e1e549c58ace5387187a0737','0xca84898ab54c7d07cd9e7b179a512f1b462f6fbf','0x9b94c7c05a30005f46682c9edd29009210a61937','0x66c58510e15c25268c1aec890e6210e5bd4433ae','0x99979551aa8cfc7eae6c4fc1651046e0330e4dc0','0xb35dc8cc03bb0c240b66e2c17c3cfd08c972d4b1','0x99765236c134faf19f4384d95cf2bcdddb14af26','0xcaf4cb06beec11ff5ce3236c6d21308894e010bc','0xcab0a151152c8e30fb2016ce3dcbe5303e8cd6c0','0xf51b8f01d42736d9e622e6c0cc95e1f6faf80d26','0x69d094e731cb891ad4b43e620deef19b09a545a4','0x7a93edc5ad1ab34589c15dcf80bb842ba4bebf7b','0x7d3563a64f7909e47ddae5bf76fe6ce3fd83dc90','0x896da023ea3fca391f0180b10ceb1187f54ec0e9','0x08103879459ecd8b437d7f8adf80266d1a5ad1e7','0x9b8b742ef077d5a4c85742b3e9cd8169bb4ed9f7','0x5e67769ce24e950afa8830b9e06167280cff5f07','0xede2b2fdf0462d85a95713e05a5f90c225ff9c50','0x99999999900dc26b068b3c2ee971b39d1ccd69a0','0x7f301c0f0bb7ae145ac4c13f0cd29e85f5ab9bb9','0x8ef2db537ee1f33d109c83e654386f960ef2af69','0x2a5a6763797921bfa2eaa66699996f72d180281e','0x0678e012ae6b6d7cb9ea198b9dc54c8c51c632fe','0xd5ffa4cd397fb008d80a9461b162ffe9c3c82af3','0x69f7c32cc5b13e545ddb85b8db3e74460071c420','0x4955bf6ff2f19ba5ae65e8d0f5840112bd317e97','0x6942093d46081b2dd7097aa7879165ca37c6cb92','0xea1869c3975ab3dacfa124439d017e721186f638','0x610c04f69f8b3fde16c19c77a7dfed24b9a63853','0x2df17b8394d8cb4b8de6a534e27a217f15a91cf4','0xdc8dfd979293302568b82ec0036e815c7ee3953e','0x3d491f985b980818f27d980d629b8335bb06544a','0x05dc115b1076e3c656b5d80eeb55de7b4e9ac0a9','0x90846ee5ca3c06682c03e573601bd072dcdb08c6','0xcef3124cdc31813e4498198d3f503790fa3590bb','0x69d4e88aeda2ee5692fe95140e2b922c18eb8bad','0xc42ab7ae8ea32a950dbf18830bce5ce24b752908','0x9e77185a0cd45515ec5b7b6c1f60e6efe951136a','0x5c242d4f779ea0cd504250876d3abfd340cad1b5','0x69000dbc3870e0c32d891afd36f4f0c79210c4b0','0xd5bbba3aed191b06aace1928555ff5c1dea028e9','0x155788dd4b3ccd955a5b2d461c7d6504f83f71fa','0x7bae2b6cce95b30374925fab4f00d605a849f5ef','0xa6ce275663fdc31410de5518c304a669fd15050e','0xda2c4be5333871a175694b14484644bfe17420ee','0x5c16f1eaa09795a54f7ed177c192942648eb8e73','0x4a5ff0e4b85e4390ed5ef1b18b87fddd8c2cce55','0x7cb991b745d8179a03ef243de5ef1719e0b20b64','0x3ff8e398f65f32110301d845d3fbc348ed3ac790','0x002c33c3146cb58d02876740782a2cebb8728840','0xbc230bc6bf99170b3e6f598e751836e8f86b5962','0x8a36c73ad5dde9426e932955b0829b0a5db01809','0x694206013ff77315062188931d607b06fd7f8b33','0xcb6dcf1509c04aa5383883e67a9d1caa03a1c384','0x58c58a5dd042bd77c8990053bfc9a8ed58d2bace','0xd0ffe94681c2f7766d530a59489b7a8725d47ef5','0xd60551d82739cbeae5fa419f98f64144a25377d5','0x69420da6e645f2a6f6175b48d69cfd86fed97f52','0x6666666e0ffbcce9e59306fc0c2ab9aeb8d5f17a','0x69bab4d29b504d8a258e10d589518c225697e75a','0x1fb444169562d1fc2e1bb420e273e87270429a65','0x3558635734d98dce6eae526fff25777b08ea3b43','0x60420af435e1583e5c0da816a16fe6278c395274','0x696867660a84a3891b173f10a5d4ff102db6be5a','0x9258e3b9dfa9d2633cfb4b7eddd05bda8b2d4830','0xc9a7ea21ccc02e162c42c7f3f35c4310eca3ec50','0xc08d92bb9305a6a47a625436383cc4a069f74b57','0x6262f025997eb7f30244068e01ee4ad6d65efdea','0x7187131399923520e71fc35f5052a5940c5a472d','0x68aaa0d94ea163b9bbf659dc3766defb4c0ac7be','0x07040971246a73ebda9cf29ea1306bb47c7c4e76','0x314c4bd159949ed8f7bb02609a050abc138129b9','0xd2efed66e09368735580d8959976a9edcccb5365','0xa6412c78022011fe7b4a545ab9edc8e353aebcdc','0xa5807c2f5e01d6d59d1e8e0df79f94fd1174319f','0xc08dfad38da62215ccf58ae2551d7ceeb70e497a','0x141700a0cc1f8db7404b27113eafeef27f3469a6','0x070401b53ba716684d79e62daa4ac478004f6776','0x1a8589b36a7054fd4b0479ced6db0c7f1fd54a0f','0x070456ae26f19000adebc8fe2e0792d1ca751f76','0x07040fe2832395cefe556cb6a878fe5f7b487976','0xef5b3974ee8514fce3f63be7c48c77c8f9283050','0x0704d7c0759c09655b2956b7b136a82a7c157376','0xef019609913fa6dbc3179a42cb4e71deed65dd58','0x0704762ccc38b451325a31cfc6d059fc6ee72cdc','0x65ef8e7e1d7e49de33480b3979f120e05b630b05','0xf986e40f10603df78372483613de7872bf648b8c','0x8fc572f1e6dadc68c06827fa25bbe890a0f3187e','0xa68b41ff088f8e21a264bf7cf27249f26203227b','0xf19fb7c404c16ac17eee803397aacd049e6cf67c','0x8850c88a3fdc44ca237e28bd098fdfe8b222cae5','0x899f6678ac76b2dcae719de45db5de4dda6bbc11','0x089705f9c94d8d89e4954cc6c11ccb5e4025057a','0x6a7c309017197a57cd1c9fd1f6f17edbaee19548','0x364dc25d6f4465dd1b569e3e7344a93d38597f0e','0x70b7370af4b9dbe8820c8b626026ab6c25330113','0xdf8dfbeb823a2c161efaa8d4f09ee3466007f852','0xd177179a80d49049f3aa4be145a921abd5fa88f8','0x576f2a4db39cb015fef67abc11e84a45d58337ae','0x07ddacf367f0d40bd68b4b80b4709a37bdc9f847','0x3df012b9ecf56b847421292c0f80e87645875853','0xe0a236c5e26015f55c1e2522daa24d415d62a76f','0x6711c4b774544a5f72ab237b45f22e2109662453','0x218436996d3de16cecf6a5d731b5d683406a0bc1','0xb100d2e293291e426fceee8bece33b9def1a4379','0xd68352fac1699eb7ab45c37af466b25d1975ef38','0x1a06384664f26c4f103bbdad1a4c60f2f0e35b2b','0x6771f726ec22175eb15b8e67a3f3dbd53d51235d','0x2f71fa5ed21edcbae8029119434611a7703548f5','0xe6099adefc009466e602068f0fb54ffa87f55895','0xbdd6050c2a703125bbfda686d6153305c419e5fb','0x999059581e497b9c1bfdccd0590aef572ec147f1','0x00000003ad90aff5c178e29126514ef51c41db49','0x2005008f9b366f51612174e29568c07952f29190','0x89fd2d8fd8d937f55c89b7da3ceed44fa27e4a81','0x658aa86ba4bacb592663a92ff76cb9e94d66c358','0x407b574d34d1b967d1a6c927c58b6b76b7f9202f','0x80999b0276246db7e3869b7aaa23e349ddf9aba6','0x9db97cf3b55dc7f8fb1b96c2e4344f7443cffca6','0x420690b0736e88ea43373a6dd7e1c8f225050e7b','0xc859869655ba92d2dd146b7c4b87ba686d4647ba','0x45ebf789fdfb1476319b9403b40f63944b60aa28','0x319189b3ef7bdf4d6a93b89f1ba2a827a383b866','0x0d26dc03de1b8e70428836067ca3b7ebe465c3ad','0xc014810aa9262b865463a05be15610d0009cde63','0x975ef3607eee06e13094d4f3dc8359d7bbbd153e','0x908f82f44b23e68cddddba19e9e80633dfb2815d','0x1d76d9135c050386449f59bcb4c925b447cc8a1f','0x66638d7fc0cf3424aac47422060dd81790fef216','0xd24fccca7205a67cae8e7660218a16e043ecf561','0x099c2138751142fab2c0117d55e9a02f75c8cc64','0x65420f00fab946d3a467729cc6489a95f2babe73','0xbfa7cb34879167e982206fabf6ced5e2ba5cd496','0x89ef4bb349909d4d045db3042125e8b15c405915','0x5015f716f329893862442a7da4c5b08e54471782','0x0101013d11e4320d29759f40508c61110f525211','0x070722f1350c1f6b4c88ca1ebaa08a5acbded45b','0x7c5925771859a5cb9fd35097a39e7e8c7c65039b','0xd88d304b1cc5f302d7929e15d3f6babbb29036c6','0xb5fb7a46bcfe2009ca5dac1547abc912c4d426ea','0x04631e79df874c042bb37f40fa68879127ead6bd','0xeb15a54e462e1ec239c09f1e6aa322b45c8da225','0x69420522f46f4581d9607f504f947df1ddf9f944','0x738eea792dfb0598847780246c466a47da745305','0x19555cd03bb624c33466449cee3d3f5b8ab80d25','0x08980b25953d81947306b1b00a5ff4a41871f285','0xdf583b317f70935513a522324f07f9cd395af704','0x2022f2f160f10205699c77e211e24a347b8fe643','0x64b270da3656661d1dd06387113543a957617461','0x00c871fd61088d85ae55ac9829efa1b6bfc6279c','0x7fe18d4842a99e04acdb44634c9b97117b761aad','0x104829ff51b4660f500db04670971ed17e000001','0x12fe791b8b9151861390a4ce846c3f05a7dfc496','0xfb682f531b243bcb76655069621b61f03a68385a','0x6dc469a3ef387ad9619df7774388ae26439ac8d4','0xf75db03ef655cd599a8204743274c4da47dde9c2','0xafe2817bb409bd7b2855b8a803d2b24b9423f045','0x8478fe54d534bdf7cd11b966e3b00396f845544e','0xc4c9164d35d7f8ff2d750abd14a1c9102c2446ee','0x95a854e7d4e787c36db8d8c3b09dba98fb75e05c','0x907c84c948be6718add9a01d3c850524ba35b715','0x16c735ba30721074bfb132a584369639ed59082f','0xa8e4b2365ba39a4494b627db6f949a34caaa5743','0x762afa03f99482079db04ca4399528f2c58ba7ee','0x0b5506c310c0b818e4ad81984d1f33b02faada3f','0x2063516ba30d69f9075854a9f57d9c29f0c61b59','0x8c3fcec4866769ea2c31a5fba300671bfc7a78f7','0x647ba15d3067dbe7da1ec570d9a566937c8013da','0xe8cbe576d5b7d4fb37aee8f46a2d9637a792bd2e','0x8b95fe1c06e58c269f1267e0f0093b7b26b85481','0x0455967c2ed90726e9d52fec326ad578ed270073','0xfbbded467484406e09cbddd95a0a8d0e26c3ef67','0x21f720b7af3848ff4ee84f47499d18dc37f45d8e','0x20539ceae7b57ad65910a13220eb75412864637f','0xbb274c69c7114917d7f022c4ee90c1bd1f12884c','0x54e9e048fd2a68f8e169935840bbd748077dc8ea','0x11e339aa7bbd661056cb5c16fe53a907cf70ce5e','0xaaa444d69e48c17cc08718bb3ab4bdf206ad5673','0x9af6c757c9aab6ff01bfe0e83128e8d4978091d4','0x0e4f0965e1e1ebc7861698214f9725d9be9b5803','0xc8817102b25c9c88f88f0dc8fecd875ffb8e40b7','0x8e1707a336c1129bc020d99ce142d4fa73de9852','0xeb65739963cd920751b62ddfaceaaf0d4dfaef2d','0x07c9229087a046bd79052a53b63ac8c88b6e0aaa','0x721051634f954276990db6c2c5533e5e5b15681f','0x707cdd330d9f92e8948d48d372578d9244978cb7','0xd5085b30efc88c4f8820a61ac53f99da29211cc7','0x61c5cbfa3199e7d264a20aa2b1cf0e70e775a509','0x030c5379b31b9910dcad606a0f1baa480ae6fbe4','0x6215670c1121615087dcb8f842382c60454ddd7f','0x641e96c115be6194f7baf5fef2e8ac6fc22785d3','0x03d98a1f822503ba2a447494ac6fbaf3f0a24c0f','0xf04a6202e0dc9d1a4378b6ae2c881e776c10c31b','0xa7391a9ed64d42e14e30403fedef069341612b64','0x82178756599cb753ee74edc416ec173ae9494238','0xe9cdb1639bf18be40a231525c22844b3bd0eee66','0xbbd53218e75947da4ff16c059dd56eab97dd80a3','0x84eff5852aceac0ebd08e5dabcba459899c4d40e','0xec2d8209bdbda98109f6c7501a986c8579979b08','0x3a4d93f3f053ae3d4fb9174f94b168505c199893','0x0177c5050753ffa07610b982c7319b285ff0dab7','0xc38764fd24f980369097f68bf4442ca925e8554f','0xd0f3c2f0a63e000b3606d6561f5afd480ebbf767','0x79cd36e049f678ace70a58341ae751fda8d8665d','0x5a858d94011566f7d53f92feb54aff9ee3785db1','0xef5486baab18152f18cc6064dfd78b85d139840b','0xd85aa9e7fc2cf5e332270b68f938d5b2dc4b6d01','0x8e3a5f13fae8e1799df3746a6825da3ec979654f','0x76f4c5b746e50f8429b8473ef2fb3d1d81fc50f8','0xddc1f18958e3df0485a844d7a255650d3c16f9d2','0xb95eee411fe38acfd3c303932258d6848fa37b62','0x093c6979728a5815528e9fe70bddbbe0759d3e10','0xcbfad7f9dbaee37419e401626625f843a0dc6b91','0x44444e3196faef2fbe917622202cdd2dcccb021b','0x99fcfd719f256d73fd03d786f5385b26eb2f25bb','0x0dac56c1be040a1fe69652ae3e8038209ad96739','0x858dbaa689e4dc81d8da3677c4f9160c1aec2c7f','0x6d68015171eaa7af9a5a0a103664cf1e506ff699','0x2aef26eb142f3ceb547539f75d9362581d5a4dd5','0x8189e1df1555c21306bccf99ae17958ae7da6aee','0x029a013feeeec0ce845d78d03140fb14f0b10535','0x4666cf714104f52c5c0201f6e192c33c590741c2','0x4b5969641078c8cab3575d841a8c1c2200b598a2','0x035952001cba13d3cc5bec701aaea0edde67f3a3','0xeea19ce4bbcfb55d8ea37122c1df43ab207eeccb','0xf321353bad18f295a8de553972770a9759cee363','0xa5e3b17fc717dd81adf998f3881f3ae759dd665d','0xcec17bf53095078d3ba9a7865fae60e2fb47372e','0xcb57a9f0fac6e92e7eba47f302a6e1d5b9043209','0xa9a91c1036b4a4415eac523759009d2f6e0d72f2','0xe525ce2be712a8a8b3a8f59911bcbeae2428ede6','0xa1938abe92414b8c44ec9b98ddc197e49624de48','0xbbaf6e2db5e96d6de0f5d9fc8872d28e26566b84','0x57525e7e7b659cdf14d24d6665468f85b230862b','0xf7b60cf8db5b1a17e954ad3e7ca55e7483803e0d','0xa483fa8f281a7a7f7b209077278cc7eecc7e781c','0xf1c85b80bafbb1fb15ec09586f88c88045cb7be1','0x42069f488a707c86b97bc38038aa643091bfc798','0x34e72807d629af3d9fd74583ec5a7c4a838ad731','0xc71d5c393ec9fbcc989fce1589e27c12446f9f51','0x6314b7df907f30a1c3bc006dfc7205bcf7d782c7','0x8979ec7f1e0bbe982ed474c468cb9b06156ec9b2','0x5ef984a5c3273cb3a3d6c3f2f8230b6f3a261cca','0xb638bcc7644fe3de08580b5707aa9bbb41f42f6d','0x0e3855376e5db3dff1fa39fcd0b2d76142628426','0x32d89e3e91c314792a92d17ecfdffd881a558474','0xa4d12b15a693fc0534aa614a039044181baf92f5','0x292de8cc632bdfc2a6bc43ba922e410145cd97e5','0xceb805aed9bd1673a77a53dd579bbd2a2eea8478','0xa0ef99db97ca8f574a94ecd06bd78e55a4b2a1f3','0x86b69f38bea3e02f68ff88534bc61ec60e772b19','0x6987f2e1589a6d01b4535f481f8edac985ca6933','0xa13edd1a27ab4fb8982c033acb082cdb5f98b79b','0x997bc9e1d636b09fbf0eb75532abd499173cbefb','0xf4f94b5637190c3a4f033dc76ef2f02acc9765fb','0x548a59996ed03eab691ca0b1d3152754e274198e','0xee66872fd08b1dfd0cb55fbdd32aa0050c902e98','0x3e3d939d433e1ba99dd3e39d6d205754d191aeb5','0xa38e396de09318e159ce7122bb94f1c89edca398','0xbcdcc436501fba0934820a18a01c953415d648a3','0xeed068d0780de1528ccf2191a3dc5eeb4440ae1d','0xbd03a58edb7641c77773e3273c5f4ee00cf315ee','0xcf64217b56fb0b3ec7be6dfc5c94514174e5057f','0x1205cd7f75cacb75dc46ce1ba7530f11ed0652db','0x0f4dbc08b8917c4d683cb55511936989dee1f736','0xc1f557873bdd9c56a69b47ac191bfac26520c25c','0x63a80de7d6d2e079ad58dae23f9f891c9c0a2ee1','0x0dced35deffc5cb83f80d596725266b3e1fcab95','0x760fbbb1c7f8caf47841000da0657868497d888c','0x39c6d5065f6c9cbf7ea07e4e1be35d2ca3738ab7','0x2024e866287ef30ad56bcbe77351947e76fd6041','0xeb59a77a1465045cc901268111dfd48fe7018ff7','0x006beaf05ab75c848c92d0889165f4d0780d286c','0x0ed2ad0334cd512ddc94740b85032e6b9f8f19d7','0x134bb4ed7dd3c0dacb41619ca31bee5fc340c590','0x0f9b21e4ef45da7ab97dece6ec23a6fdf038c0af','0x728f9c3ebd38bfb66156fdcf8e43dc2134bf4abe','0x1171e6bec5fce0b06c2b73bc1096051c5650f1ed','0x6a8e32930b4058500082d922b6d005345294b537','0x53eb6ab0c12c8a8239f10b6f2eb2d30f3af517f5','0x0e6fb2b070ad1a4499e8bb1c9223024b186d1e6b','0x200500617e08dca0bc44984ed87795411df32fa6','0xc86bf42f991945d4b856779e8774400f1dfcec30','0x7328befc047ff4325c7a08d5a483edcf83a86e0a','0xb2ab1f22f5abaa1ff091a7200483038207ce5695','0x76ffe6d22d8de6e0a84cc77670b3f89d24791a58','0x6005279022955b3dee0a5be533199693e74bf8c1','0xe6b39a7e9ae24d4d54279ce3f6908d4616b232cd','0x73e52778c54ad76fe693f306eb4d1081cc196fee','0x0c50752e61c44528d417dd1be446da742d0c6a95','0x2449812c268da45b75cc724999f96c271c9c14a7','0x00bfe3df83e91bc9d51e851610975e5072387ff5','0x198629b09cab09fe19541764adc47a2a3bd70642','0xea67f8aa1ef86375bad5f4770276e1803a5adb8e','0x61f8223a6444aac71c97c13b0d113a41c1b4afb1','0xe434c7cb267998d302984418c5fac15e43caeab2','0x06141946112c78ac02f33f06d709d3ff758a045d','0x58739f8dd46748c7c674544f6c152fe4ea57381e','0xd338db236c11c3c6a2e3fe150d4ed0b49991a12b','0x43412511e0b1c29625c0fab100ccd42c46e0ac5a','0x7c6ab5d2c86ef04575e4dcf43f6e8ffad987870a','0xdeb33fee81f7c4dd964a9c90aa35f2e8dfa697f6','0x4f6882f1be3dacd21664fd9dce7c2ced17b2d3cf','0x514d05c0c8a6ac5dd0a73866b752aa414426a861','0x0301fe5d50250cffc9fc43afe651ecdcbf7cad98','0x15d1787be1553d05f7aaec2cec1da72ed72bbf44','0x45e53b15eaeb630b0df496bd7d9a6cb41a9bac19','0xb00ac5b6bd3ebf7ccec1ce561dfee297bcf7bac0','0xdda2b124d02e7052219c9b5fc32e7b3e43ab817b','0xf8ea778d3e5ade436ae4bc7c2b89ad18cf9d369c','0xf9e2cd82041bd8c643831f1e16fab52808136512','0x385d65ed9241e415cfc689c3e0bcf5ab2f0505c2','0x271b9df3cdab3ba223e9c9b2ff747f2cc27e86c4','0xe8697c293a53e5516687b8251a59e61c37f36378','0x6e5b707f9d6b224588db0b1afdad0653cf87c28c','0x36862ff1f4f2cc859a13a45d4921f8ab3664e22b','0x19b456a06d721848877e24590699365d12f0b86c','0xa39359605ac2d82111eff03ca36d2e44ed055dca','0x7f4dfa2fa46e10a88d30e0ac1355c57a64bf4cbd','0x0f5793ba5242234a5e3efe6e56b27fdf9c124727','0xbcabf7552754e57f185b9641308757ff9c08497d','0x0ee2f57c747b7f62a8b7be07a9d22623668613c5','0x5cf97a0a988b5ff035fea347472b8edbce80cd55','0x7b0255727ac6bbee3a6dc57d70b6a57c9817a573','0x3688e679b9b760480dd2457bd8c24348aac4c0cf','0x88e28064e89a1f8c9c6c63843d696903f0ae95ef','0x7b190f2bc65be37310d466ac710201c8c350fa88','0xb9eb6f357f040be1d2a3d6b4ba750d1ab8a4233c','0x5408d3883ec28c2de205064ae9690142b035fed2','0x8943148b4dd8b18f032adeef68a71753c4e4ea37','0xdca0d70c8948e84b792ac356e57871d16cd26017','0x6b6c723349b9811248929fbe840d18b4a3af75cc','0xbd32bec7c76d28aa054fc0c907d601b9263e22c7','0x828d5e355fbbbdd8a3fa27a4f331ca34bb687660','0x9573d45c16e9297412a5a405376fbb04105e723e','0x1bc887295817e568a09585e16f173f3c093b4e37','0x3485695a2d2b520a729e01b08c3b4e8972b7211f','0x56f1afaf632824684a1dd4bf2bc49ff484d0e415','0xa37d37c58da6ea3dfe9d4842ba237aeed12389ae','0x92f295fc01455a2fbfc880fc2f272e4eb2b7a2e5','0x8c0c91bf2a5a743f5aed728cd451b8b743895edf','0x57bd7fedef71ea3e01c746564800a4476190bfad','0xc14373c5d271ac8c7b02e2c237ccefe8a2b8631f','0x8808703dac0b9e2206a12dad0f37942f0ac6416c','0x565e340d34fc71083a2c7c4a065cd003cb9fa9f7','0x8aca7135b8c36c0d7f613908fa3e9039e4214e4c','0xf8c2c684d4d7279c7babc5d7f99d4f06a326b5fc','0x3ffeea07a27fab7ad1df5297fa75e77a43cb5790','0x3cf290ba26487121bf447e8b469fed691fb24665','0x00255e1947ab8d2eef26e8a9342042ba1db002ea','0x35993371d5819714c66c299d42927fb9b39e09b0','0xc60e0baac16dd2aa908c97a454b9f90bf2e61b6b','0x95ee34d87c0314620c9263f9c0ea177fd45b6d93','0x5c9438756c9347a5a2ca7efe8c81000db83eba78','0x208c7466388bb89f939963edc1daa928029d5c6a','0x13e36a188bce9dcb24ac8f9022af660d214d7d0d','0xff049d0f965c3fd2a314d0dab413beacc0641544','0xf1a83f6cfae6c1e96c0b94cd11b4ca902ab8eca8','0x224186fc537498d9b2ecec7ec1ddcb5f80a226b6','0xee650a1ba3c2917084cce4fb6babafeecb262f60','0xd149656c989b8298301e1e023204fd5daa8b1fea','0xeef2440a407879a2fcb13eb42c4c884c1ddce04b','0x77777296acfa95668305f8da0785205af595153c','0xdf4ac52ef799d79b29fd4045ff9eda85335b0325','0x0a2c375553e6965b42c135bb8b15a8914b08de0c','0xc1729dfe65e023d9d83d6254dc58d0455db5e4b7','0xf21b2c2a70019ff5156041ce2738437ca97a6c44','0xa949101be849184c77e5ac1405aaf3cdf41da1b2','0xc2abe14acf77c341532e0f79d20643c1f62b1e4c','0x81ed1d498d432e66057e9d9e8d7d1eb17d69f76f','0xe1114e0f8f0acd6b671af9f04c87a1c938baa656','0x921b01241db057d5a316fb27d46a11f52bc94fc3','0x1c89a5768a7b31152cc5d3189b2c736fb42ed18e','0x6d31d803f6bf9d300873be5fe0906bb5262ea141','0x1a8a39f2986cf9688f6dc9e5ee0cc0bc8d5edd67','0x356b735f00dc949c279389923b98d81f81421c41','0xef61670bfcbe40ac24a1c6975865eb328c4f21b1','0x1cf164a8ba56822853eadfe8c6d9a58c4acf1555','0xb4383afaec8ce6c4fe0fc6b715e37d3957520548','0x554fec3ba796ce9a7fdd37e9d8b3d3af032e7fd9','0xb36fb1b5ac1076bda54b2e5ca8586c642a7eff77','0x4d14f19bafd6a2159a152f8aab71172b2b16d7b0','0x19848077f45356b21164c412eff3d3e4ff6ebc31','0x98270c7d8e0d9c838d992efc7cc378120006329f','0x096799cec6b074bdcb11db622a173efdef7d48a1','0x6873c95307e13beb58fb8fcddf9a99667655c9e4','0xbe7f996174452f1269bba07e8afd5326522e332b','0x479bc8e910151cf8027dc6f8c5b08ecd7807acd3','0x8179dec9c048b9918ec63d3e0989119a324b7b2a','0x427ebe76c577719cbf117b873a08581c42ab7c8e','0xbea4526201020239c319dd4f30ff5594ebe996e6','0xfca13f773c021506b43018caa924671d813ff59a','0x0d7f9b10f4f8eda784f586cc613688cacf94d587','0x16c5d0144f470dcefd214d95fcbfb2a1a0d51705','0xb7734cc6122f847d801147204049279132aab239','0x1dc3d7a065254f53ef97437b7161f0d29f7352af','0x56b280b185563a3e62c41acde8962ef7074a3d40','0x3acd2e8397f2f45a4bab71e4291060fc33228c4f','0x9f5d0f27422c2e85a85bd9caf4db87778e2675fa','0xf9fcf8d53d72c550e6c15c0d0e40bd1685398d77','0xdf8858068ce429042c52d581c83855434587f6e8','0x541568d71e9f9c254d782b76c81a621d419bfe78','0xeb6bbf99ff82ab452bda2ea59e4f49064664481a','0xe985c92af297280760db7fc3aca36aa5b62e4267','0x66512481dcb0fdf031d6dc73682342c002bb31c6','0x67c4d14861f9c975d004cfb3ac305bee673e996e','0x2fd5da6c0700b07c28b204439016b921857c5afe','0xb0267aa7dd35cd09730ad1946436fce21a860cd7','0x6666613476ac9b295669abfe824ad7d8d968f57d','0x31080e73362b5a6a1ac62d57ae0964723d8f5bb0','0x4371cfe3ae6451325361db3be875d811a96a3bbb','0x1f33301c0b2b16cec53d15cd13af4be73f2be020','0x9cbefeec232cdbe428ec59ce310c6febc01d6163','0xc493f874d79265f983c935f3ba2ac4cd84dcf3aa','0xee3d502e853865e2074b577315922eb1060f9984','0xa2ff10af702e1ac0706fbc99cbcf0973b9ff088d','0x69420d7a9443036ebe40f965877b546b07434a5d','0x13194e6eddcb47285b0d1a885ba46957258c40b7','0xcfcb6f92b48fa1143ce2529e96029c71b82a7c78','0x11192e241058725ab0966002ee262b2111fa6bb5','0x520208e85d7f67545ed3c502a8b6fa726016cc08','0x137322eb6c1f131a3e4dc9d31bfee42e8fe8013a','0xce5579ea01d11816845f112bc9723f0ea981f8cd','0x8aeb4a7e99ea5852c6993d21b4e1b25d44f5080e','0xae53fa97bd0f5473d4947ffa9c76b274e1d6d621','0x5a1125e3e9add42c111346f28bdea42a582e634d','0x8c68a74a69e2cd2815cad9c0379fccb960f64cdd','0x70ba2b8367b6d62190f19c784d986d99d9f19555','0x866806c9c19cac818e311df11b90999cf7ea62b8','0x5f22adbcea5de210564e32f67d6a9d9a8acd8896','0x6e175a59fd946abf5aed7de2246656695f8186e2','0xf5d6f675a0340ed05275566615fe3de781949663','0x597bb10107e6f3bf55a8cc3d0c56d9a877c0b8b7','0x5a8e7d897c7edd504200e547c1ebb74abef9e5e1','0xa672b803e807ab9b7cb8514350523cd6d2e4d5cc','0x420695356f5077c838ff443df23a30a7aecddd6b','0x4f77707d29139dd3ff1f3734013e7b05554a10a2','0xb71cd0d74deb67d148e46b6e57df113c046fe7c1','0x37f2affbdae65cdda2c354e3197368c7cb41e33d','0xbcca9ce3ba9f719763d2dcf14017046d01b70344','0x23be7a6a80b0b9925eaf560df9833880de89b955','0xfe7103ebca8554740be85317669d5eafbbd7939a','0x4dc785423f68451382c94262fe0db8dc8529dbf3','0x64766392ad32a6c94b965b5bf655e07371c23a1d','0xd05f33b4fa630d6ba8a3ce75f7785439e6a3bb00','0x9777564d660ab4cf2f1140b2c147d0e587e8ba6f','0x4a07e03fcd697993633eb6647a7f53205d8a0626','0x6ce37efb39683602aa621fe3c080e8faffd7e98d','0x1735cbf7e1da60968dc90f968db0d530ef5fa8a2','0x4039a5b2e7db5bbec9a56701a49fe139987de54e','0x66c3d1072fc02959b871f4256da8729a2e6466cf','0xd89d1a30700675c02ca668292a339b59bbe5733b','0x8ef8216f2181f5ed37afd66f8a37bf0bb790e0e0','0xafd1552fad6cdb7fe5d5597bc3f5035ccefd98e3','0xfaa864fd50bd127ab4a78842fc8f2cd8434170c9','0xb90b468c161dabca6d0a4e202f506b8eccc50b2a','0xb6543a21070cbbb6446e54ec60fd88d4d67d40ea','0x010f78a9022da26d79dec49a82defee6d94385c2','0x361d6972d4c0bea1e4df25fb6f7a5b00c725692f','0x1cec8ccc3572e61c6f068059108404f8ea91318e','0xd105bb03397a6978159fda6cbfe5e0ee56a5ad73','0x1580eb65edb0b903e7b3624ca963219d0c5c565e','0x113103ff77db1c58b5afe7de0a60f2f1c05a72c8','0xeb2bde819a63db62c9f323b88a41e8892037dafe','0x817df30ff201eb721e7877c97c4a0aac1f0aa4ca','0x0b37f2b45b7af322021697907693a2d959747008','0x1f2afe6e54544001b723e54e4dbbfe3a959be166','0xf42b741b3ab2f1228b34b296c7b6bbd56496215b','0x0b4838fbdeb4b5f1b7760facec819becc3f0da55','0x712c257e4d8b7a76fe1c6ce2fc0469470047889a','0x03df73eb122d482fdd6a782d34f41c2d86725a30','0x9fd9278f04f01c6a39a9d1c1cd79f7782c6ade08','0x6b4256bcfff6d2a8141e6b08b95aee75f00de2bc','0x906900ac21d17f6f0502bffff6fb37a2085417a8','0x34f088b76ca6c765d296499da84d74326fc5c245','0x276105758dfb270f5cd845aa04a6ba09c88699ca','0x2da8e79496b4a0fc7da500c75b453c98e7f74c63','0xf5d84ea57db4665f9f398734721009b901dfe1ac','0xe11a479bc4052293c3470e8f38b744ec8996e71f','0xc5583253bfcef2429d0062a0760f18a10cf1b6c3','0xbd8496e51e1caa72296f378208354adc8d6a32e9','0x5c1bfab1f6bbd8f059c7d0c0124e5b8a7bff84fd','0xeb45be9a80bfbaca1aa3e8c4a48d387c56f049f6','0x3c259c709e14e4e31fe20f3b5cb2cc9443e8b692','0x182ebbb26bdb2699ed646a67fe19eb211c993bb7','0x68d57f179b808d12183b52785117365b8db11be1','0x71144d4826018aacd58562b0e1be199d3cc493ac','0x9e7427436d7d0cd6fd13e6e950dd81ea7dcc857b','0xcc61b897a22c8653e7af40998e1f67fff11da985','0x0b32615cd68d246fd9737382d5fafc40802a3816','0xcc20e4dde8717f7f3e0d781e2a1a0614057a7c26','0x9d2c813c6463ec93af1433e6a5bf02afc49f8d90','0x9e9607f740c9137ae853b3daf4d4cc3fe9000138','0x1946d29fc571a482059a939241e19053bdd6e49b','0x414860c042e6f38b912c373002c600ec91f14f49','0x327c6dded26ba9986ff592bc987116cca68b4856','0x368ecc49faaf400010f311b26f073345c981d25f','0x34f6a14b361d456671796f4923ec1c8b63ea5c8a','0x7c9f4212ab981952e21904aedea12e0013613e44','0x30ce31d1e29468052a9c9c2dc6294e3b4b6c3d83','0x234fa68cead4369bdec6162234259f3aa542b2a6','0xc0e361170232dd18313f4b9ec75a80aeb6e8cb4d','0x63abfe69ff662627560352dfb962493c65dc3f2f','0x8b03e088958f2e3593fe069c9e80c0f0ae1f44c3','0xcaf055fb9203911d940909543fd492a217a68a5e','0xb8e81259db6e68f00b5d850d409a6295e5398561','0xbba20dfa0b3f8956b32fc99ac7e9e81ce0c7efe3','0xe8a9e9846d44f8608e0f5f3849988155352bf00e','0xdee69b6ab6fe18cefb9b85f748b0312c75284ed9','0x3d8b5a5a8652957de845051a2988a2fc6a21a383','0xea1779920aa037a4b21912cadfde3a995d9fb1c5','0xbc1cc064a0abc6b034b9a55425f3d6f9a1499556','0x69420d95bbeffa6b461e723b92f07380dd9bac68','0x0095439adefc8c34c2d45be45d2d2f37fe7686c5','0x1c7bd8a56721e94ad3c45d131d51323db9eac8a9','0x6aa60deccfa289b58603bd79988fa5400bae5954','0xcab53a59bd75f49d98e9b144ef0a4282f9b27b47','0x1fb6d68f9bddb070013f8bd76341cb3d0cc0a9cf','0xf157b74ad26a1fd84484ce6a9b44a3907c65edf1','0x1ca03d09a4ae022ef155ff36ba46ef2209028f9a','0x3676a247f2fb2af9e1ae61209ffbb2bdd647df18','0x432e9fdf5d62c3db400b6060fa77eefe8c92c72c','0x59408ceaf8dbc6e91c5eee347d82ae87e7488c75','0xa953abaa1a3a1bf899fccafee7de6ef883d48a1f','0x5af612daeb5d31dacb4f8615ffd450d7cd1135fb','0x63ebcf98104a6c745f35fd7dcc8d31f6f3bb673f','0xbe93240e78d0c7a6e00e64a66516580877e20db8','0xc5903ced3c193b89cbbb5a0af584494c3d5d289d','0xaaa0734425f736a1aeaf53ec8957a243405567fa','0x1c43f4c10f23bd8f43badc34eadb5f7347f09243','0x9f95edd8c50224d5c698d81f7c4c091782be3ab3','0xc314823a7c69fac6fb136a5f150ffc16a2f8dbcb','0xb6c332ca5c2d4aeea80e9f05b632fa62b8f70621','0xcf87f9c5a48445224bb7f874e218dd5e533df646','0xa2884816c14f2c058a03f1619861df585cb7953e','0xa4a47e67b3f98ee5c6627020ca2e1d406dbc8335','0x070c26a851259211c51fcfdf56c8e2d6d4d0255c','0x99daf798969fadf31d34bb044552af86399def3e','0xf860332b4359e33ed25bc73d73a61a16eefb03ba','0x681db616399def061311cec847ef9c79d5c622ca','0x30841adf1fde3ce7bab04de56a968ada2599f8e0','0x54e50e0dad3747459df193937ee46f74a3cf37c5','0x3dc7d520fb61835c12150456ee38c9ef305b94cd','0x6defae64012324969a930c80d931ce0f1bb9078d','0x9c0fa37754057cd8f04d5cc94a399146123bd1f6','0x069e4aa272d17d9625aa3b6f863c7ef6cfb96713','0xa45a47a27a8652d80ebff41df405ec571da6cb65','0x3a91d5e5226f1abb7f0eaf56b49fbb54919ea8bd','0xe523a59430b2ed7b69764c6445072c52c3329c09','0x2cd40df1971a19ce1601828c1622da3bac17d0fd','0xa2a6c5ba49b282c253a35b77522aabb19ce4a0ee','0xfb8f50353eaa279d4874529c29304e0ff494e1e1','0x265d23c7044f39beea992c99758fd06d0e81855b','0xa7187e50d4d9c33aae0c9b09333568e8010ce7dc','0x0ed1bed69fff82f2cc05e073be6c88120aa344ab','0x3c189004e28e93ad990cb19d2793adf11417b863','0x2f847e23230f260bb0bbec06de73fadc292b9578','0x5dd87c4ea1f0890bf6f17e94ec384712eb65e3c2','0x6228a528c97ff6fb66eaf27443037cc2b318ff74','0x26555f39dad5e1a3b857eaf26d580b80d0f4fd99','0x9371a71b0f7ad6bfc3df958d1f133f50be8ce7f8','0x829eb267d309d1100273bd4aaa7c36808c29d305','0x20e3fd5ca2735a95560302f87b24545b44178e92','0x816c4a8d062a0cf4d11a6fcea29c37931690900d','0x28b7f370a2b0fd04a9f420c8863b12f35c0f791a','0x5c870fc9943c06abafdefe8f7d421869d5612c91','0x48e20ac22d8608181b8a969d6b31e475cd895d0c','0x6f057a5d7b7d70017a6fb6d90b2361258981e876','0x8eafee6f29a9d4130fe09d358e29618c2e4cf0ea','0xb66d224a8532ee2ff7ac3e2679f2c6887ac90e9d','0x53206bf5b6b8872c1bb0b3c533e06fde2f7e22e4','0xc61836c83178bcb17746101ba7598067d55d767f','0xea9c226da7d984ede7563dc58f8172c5c14e9ae0','0x66bff0d18513c648690edcdf5993c9da8c632ec8','0x3900d3e9ae0ed1264ebc0dee8a0312eb40bf4877','0xef29ca65c185ddda64bcbeb4fde07febffbb863d','0xd54f9caa3df84d1c5c6547f7fa1270a9b69654b6','0x48b30042902f3c7d5c7d6d81d03dc1adbae529e5','0x364e317656cbfd88ad01fde65cdb708b0e05a8df','0x2d048a8a75d6ba0919816850c5c6cf4abee73ddc','0x7cd7d349ea5aca2f986ab48b95ee702c3146affa','0x3fcf109d8c37d38e2626a670774df9a9acb33e62','0xa07849c62daa8361b1f26207645f3aa0d6e25d39','0x061410ce9c4fd25bcd075009dd5d0874fcc46b70','0xe41fbc4e83f9f702eb0ba53f0126a7df11c09c4e','0xdadd1e351a28573236e8cb144d28334c94085946','0x9c3d2a489f55b567a5feabd37632e637bd2219cb','0x7f0ee45f8a494400d33285d2f473a5b6bd832583','0xbeddd2508fd3622917bed698ce3327a3ba445dd4','0x916fd0958a9c9ec3170178824a89d13c5c82ebe6','0x7fad646e51245e55efe53a1f59e82eac5f9936b1','0x1eea5a172d5a8a7670476008fd19ccbd9297a0fe','0x5a33feef95a9cf7bfa9095d6cb4eb11a6c917874','0x8eccbae141d67b6d4b92bd63376c23c0ed9c52ea','0xa4b730bcbc6654b812797c42ccc5b7510edf6887','0xfb4d71ecbc4f6945f1ab0faff17c9bdeaf86b847','0x80034f803afb1c6864e3ca481ef1362c54d094b9','0xbcb93fb8ab2d33d33a257142bf590a061b50fa1f','0x47474daeba9221034431656a89df46842f573770','0x2f1ea376e52d2dd9dd9b1f00e17c90c301fe68d8','0x8744b6f123a69a75ea7aa33eaf2daaebd3811abe','0xe49c0b5b61f6e55e6d4fabc3e3034720339730e5','0x6ac50e51667f2ae02071052141d4b47d2c0d0cf6','0xdc09070a088465d35f802b6bdf22aec5b0f7cf3b','0x7a1609973e30deffe62a549947f72eae1b6f4e3a','0x63e83de9fec8aa5e22f79a8428d23781e2687b63','0x69876c027d6811c285cf0f05109d1efaff5adc33','0x8c1da9e220476943cf9d496687d0ddaa6e096a39','0x66babee1e404dda4b4df468e7782d18d93296c73','0xe4e9d448c683e7e761c4e14f4f4377830cfc4fc1','0x67babe21f1c5d488bdad21a87ebe6c80bc74be54','0xf5e9b3ed0f2f4200b9cc76705c01ccd3fcdaee0e','0xb287fc3e0f5b90706626bef3d4f4dc89f9ddbc92','0xd6fa0a7777cab55c8638823de4d067991611512a','0x5c168ce5e92a1826611d99b601172c64f6ca1272','0x9556012b0d73913fcc3030f2a64d9be019536195','0xf22a53f59f43559f98a3f43d8e433968bff4fbd6','0x0422a473244ba841a6133af6af9cd709ccbe091e','0xfb598cf36f48ded716b2349ef2f24500be1c2aba','0x414061ca9d248d26383f7e4caab40fae40252629','0x888888ae2c4a298efd66d162ffc53b3f2a869888','0x95af148dcdc6b36b977addf7ea2599c5e0483263','0x0731203e945576b0e2d4a2e3b8ac9b3eb3418ae8','0x63babe45bf4d998ad9150f98d2bf2bd29ca875c9','0x66babecb1fe6c45d7237ebcc0b73333ea68a0918','0x7a60c4ae6e9eb18aa1a228c6c057c76177a3ff89','0x118334d7c2387cdfea318f91c8fccf9600ca5994','0x896c2be4bb3804d6e96459bbb177d92d88c88ece','0x590246bfbf89b113d8ac36faeea12b7589f7fe5b','0x8598c5c22b3dab90748a1c3ba7a1a5df1435ac03','0x2a04dc2ca164387ff25669646150a38c4568b52d','0x548a4f5b94c35966bf4898ad32ccfb3bd0a25eb5','0xe6a80945803b2ee8d1b7ef3c784bd26d5dd2e54b','0xd048a7b725fda115fe7936fc6d40808d53e69468','0xdf584e32a0ab3fdae0a12a8885c32f14b522291f','0xf28f0101cc655f78e55942636ab7a7538127e1b5','0xf396de0ad252dbacc2dc0ffe9e02b39ca14ba31b','0x1e3370ee14d64d3291fd7c20e0ea0f769e773dc4','0x61aaa9db15560bd2a050fa844ff6ec97ddf19641','0x203e5b63394fd27180daf86ea80afe94253f8b94','0x700f6ce237c18711a73401e82090bbdb6605cacc','0x636bd98fc13908e475f56d8a38a6e03616ec5563','0x916ffb7c0a33e07b7c2b983964a4d10227039453','0x87efb24d3bae2325de53ef8cb2cb98d842d0289a','0x666666ba6bfc936e2d84a49c0df11b8fa2630a7e','0x420d9d607031dea0f60d51befdb272b872075069','0xdf82ad29f859e7c25d155d9e35d8533f46928ca9','0xd0b02967f84d8a299ab6a8ecc5ee8003f2173f55','0x65fa7efae6069d6e1fea303b8305c848b1d256ce','0x160bee3cb146a610256a3ea837d11a623e521423','0x281d0bbc59f96cbfab4ebe590c05a4d06b494ad6','0x65f13ee475ddbab5187617518d616258337485c3','0x009b816f8e051da2b1cd539cf7446f3df2fd3e01','0xc5629a50527a384bc2566ea97a0bd21ff4109e70','0x24a108d4a975595706e52a709f9b52237ea38c10','0x57babe77faab5b8bfdf3a4027e092a0dd57fc321','0x1839cad2f9b2be10df5a8f5b2f22ca1c2e9c8123','0x3e5d1e411a89cdfcfe85667c59eeb95a5a5ecff0','0xb15cbae718421e74deed41bf53eecd1cd2a7111b','0x7e744bbb1a49a44dfcc795014a4ba618e418fbbe','0x9ce03cec36d624e86bf3ce726070af59b5802195','0xfddc4e864445a389c11111476f0e9e866d461f7d','0xd39cb6aa6aa345c719b518d8efe2a92807f49cc1','0xb82850d943d7a89fb9718402625157b989186a80','0x4fa572979fc5f282c3c83a48e3e0453cf4b81111','0xfeda558d804a055363cd0f1e9f133e9515dd3078','0x0765422a37655686edee1b1733ccd2030c6d2365','0x29c42bd5d2812ea62bbb1962bfae4d79b5f2cc9b','0xf9e8fed35775d04c26ad818c8660ed919d46c004','0x2ce3f42a65531f8aa2d3be2053e38bfa11392f6e','0x6a987d6bcf6cc99c3de78a893abe2f23c3a569c6','0x1b08b0fcac915449814dd6802d273411b237fc00','0x7c7f3385464a21eee2884a0cf6411f5953b9f408','0xdead61337552d8572f3b229284a76086a293dead','0x46c921980d35f31d0a615f33335626f7e6396b88','0xa46e1e0cf3df66e3eb106ea831db67c582a616bd','0x6a698f5f692b6ef238793d402e25c2f313137138','0x604555a88d68e156aa52365080b8d8cba1987de3','0xcb07732abf1d551b6899087448b2b5c0ed3d4fa7','0x516f151a5dea4066cf7281011499c4ab12695b74','0x6969692088bff068d6f3e2c5eddc3dda8182870c','0x56fbbf5b6a0b7118dbc228fe5d3f4d63c2f9e6ae','0x1086acd443b25119e05a49b44010f800bc556d75','0xb27845fcf60f45a83ff73cf13c0023ad5aa18f1c','0xff681856005b25298c0af2499ac3c163f1be3922','0xfd9cf74750d06a76983cc3e94588843fa13b128e','0xf68bc2652d44d66e7ac864fedbf51bfd8179f757','0x1b80eb9afbf62499cc224c118e159aacc64f1f11','0xedc3343de83c35af2dc8ea284836c490f476372c','0x7c6f42c62f80d677c2a77f9e9ad02992c0bd99a6','0x894f8c4258bd09f3973c275c0df71775173437e3','0x69477b93b13f798e2cdcfd7e1cfc9b32db38e78c','0xd4d6af40329ccc59fe661283d0ed707e2761fdbc','0x2d2f2ccbbcc23d142e4a59154b3f5d8d0ea7cd55','0xc7d899ed34f506a76a2a25f23cee2f4a67db53ec','0x7d5d4fff14c285c96810336fc661df1bca2dd15c','0x2b7c0fa747611d4412b54076c62119926474edb3','0x286675fed193e3cfb20bfa3ba12db56e9231a0f7','0xa8e9d8fa0e0f0bc822383c2ab81d1dcefb1d74cd','0xc125ec4827620e2332046f9e0d30ec23f6084fd6','0x2005396cdf6de574d582cfb9e6b3e41f176014fc','0x1ba6b8144d471b7decc638fe794c0bf9ad278297','0xcb97c0c6998f365e096a2b8494704d6b455ad5a6','0x3c91cc527de238222496e95fed714b5de5726f10','0x1ab46d323d200eaa372d4860cf242c82b8301a96','0xa2b8e02ce95b54362f8db7273015478dd725d7e7','0xc3c3566a5b76958ebdd7b0a5c9daf92e362d1751','0x69420312905cf6ee33f3d0dea4aeef72a13f8473','0xeacad96537c0c5647f6b9e2f1ebc0ba9731a57f6','0xb033793679f43777b638bdb1f4cf50fe1d55c9f0','0x40bb1c8933b0cfef0ab5670814446a1ec1834d6f','0xc7215965beb89785b351c62761a1ffdee0ec31da','0x7d702984a9577472941756256595639d643db2c6','0xf373af20eddce50573d6ecc3bc207f87511bdfb4','0x7b28af7e9519684839bf2d804e50aa0c1ff43679','0xdb513a25d5e8342da083a0162e1cf1bcc760c353','0x77146a56bde1bbbdf7aea71b24f63362ff423e3e','0x3a074f5186c057047aea78d8a58bda8d88eee9a2','0xce813ee7d2542e22e2a3556e994ef81f13177156','0xafb698e325bb77e01dd4b74f9bbac561e55cf06f','0x53288aa471511fb0de8bfe17923f8eca64607a54','0x63b7ed40185a3e925b172e9be73f1549322b4d34','0xb6fe105ab68f9d38d61248f7b9042879a9e606c7','0xd966f70fa6f9e3b3b1c159f45c6e795054d034cc','0xead368e880156a37dec707dbf571567945f0a00d','0xbd2466046d22305dcb6dfe37a1ef9b4901440c82','0x2ae61ed5d4da30b40c8ebad2f88c5a2f9786d0ab','0x81725c66de2d405dce99c1c6c473fd8e64f1581f','0x039d4f77dd7ae619e09143cb225f2ac47198e774','0xe14b7a963925e451761166ad08dc6118eceddb9a','0x7366d7fe757e812a3136e39d590d1d6225113e0f','0xedd90e025b390442e1b6450f84031580c9dba53e','0xbaaa693046075ffc31f23637968b95a426c54006','0x420698cfdeddea6bc78d59bc17798113ad278f9d','0x5108f417288f87e8bd39786029b23f9f245f315a','0x4a4cd9c3f86e61ee87c64b63fdb4921564765ed1','0x78b9982f394bf699c3dcf07a693d0f1a0110ce9a','0xdf0669651b53c8e0f39e9a103eec23f8ffc1d4fb','0x9ce654b2046a3560da09238cee6effc97a3017f8','0xf0c59fd9d0c4d73f31f49a085442ec177952a317','0x8d661a27e0ed280f3870da12884e9fc359b2c9d8','0x60dc196cf14f802f25d7456ea3c8a6846943dfc4','0xd8c108d8b8076afcabc54399aad206148a81d68b','0x0ac9e8d8b3e87ace7938539c49cd7f7c6ceb3f20','0xe315c322f17fc12d23bebde1acd22854323d3c46','0x521af22156ad79547a2fd2bb5a00014526e7cf80','0x42102e43ac23a5d5a0c0044371228a577add0a47','0xb699c99063827a003dde7203678f1cbd4dd2596f','0xd62bbc1abdd0dfa1f33afffcf3891b9f10b22f31','0x56a187c49011bafb01e9c32ce90b5d259bee0c12','0x0000721475421300e285d0a33f660717da058c21','0x9d20731c8cb82cc6264711cf5a5f83196b9b1ce4','0x15f34c43b13913577e76f2d872d5373f8b9012a9','0x0dbb0b1cc1b4bbf414511b2a9be5ab7921189356','0x959440d319304bbb793dfe59b01b65184bcce7e3','0xc3f78ecaf481eae4eb32d43233b3a60469d5dfa9','0xe0ca9e4b62e4ae4d80f49315ae8479a5a76184ac','0x1d1ce9b1273d967a12ec4572e6a27eb7ac32a2db','0x5505fa45da599eab7fcc7b8c579c6d3509646fb7','0xda5a9d2267be1aaee97ea7045be4c297adda6224','0x1fb43088cd99908d49408ef6f2af8dba30c60595','0x3b991130eae3cca364406d718da22fa1c3e7c256','0xabb41328eb3b89465cc1d6638f6b6b828adcc96d','0x641cf8d6d42304eeb7fe4dcfbb18bb8a539389d1','0x7777770e1a291207c6fc886baa9ea85500767914','0x69691c7e9b7c055ccaa795ad4a8c2c1ba5d9af69','0x4206984834ca2001ca4b203d3a7dd3edd6185b1a','0x38b9cdf27192e838e1e549c58ace5387187a0737','0xca84898ab54c7d07cd9e7b179a512f1b462f6fbf','0x9b94c7c05a30005f46682c9edd29009210a61937','0x66c58510e15c25268c1aec890e6210e5bd4433ae','0x99979551aa8cfc7eae6c4fc1651046e0330e4dc0','0xb35dc8cc03bb0c240b66e2c17c3cfd08c972d4b1','0x99765236c134faf19f4384d95cf2bcdddb14af26','0xcaf4cb06beec11ff5ce3236c6d21308894e010bc','0xcab0a151152c8e30fb2016ce3dcbe5303e8cd6c0','0xf51b8f01d42736d9e622e6c0cc95e1f6faf80d26','0x69d094e731cb891ad4b43e620deef19b09a545a4','0x7a93edc5ad1ab34589c15dcf80bb842ba4bebf7b','0x7d3563a64f7909e47ddae5bf76fe6ce3fd83dc90','0x896da023ea3fca391f0180b10ceb1187f54ec0e9','0x08103879459ecd8b437d7f8adf80266d1a5ad1e7','0x9b8b742ef077d5a4c85742b3e9cd8169bb4ed9f7','0x5e67769ce24e950afa8830b9e06167280cff5f07','0xede2b2fdf0462d85a95713e05a5f90c225ff9c50','0x99999999900dc26b068b3c2ee971b39d1ccd69a0','0x7f301c0f0bb7ae145ac4c13f0cd29e85f5ab9bb9','0x8ef2db537ee1f33d109c83e654386f960ef2af69','0x2a5a6763797921bfa2eaa66699996f72d180281e','0x0678e012ae6b6d7cb9ea198b9dc54c8c51c632fe','0xd5ffa4cd397fb008d80a9461b162ffe9c3c82af3','0x69f7c32cc5b13e545ddb85b8db3e74460071c420','0x0f948158c5bb472e915995bca6efe1ee5f458409','0x1192e281cdc0f0b5f1557c0c4ebb751ca2b9e7ff','0x69420dbd873dedb25c86124cf6bfce5c932e46b9','0x9b465aab9e8f325418f222c37de474b1bd38ded2','0xfe3b7b0dc6a62436e39436649c5b0fb8a9a8f906','0x7e71942fe2b24e304abd172e4709139656fa2bbe','0x69825eb9061da7f82aecba3878f07c162c049d4c','0xc179e6235a9f9967b7b5aa236b240d4d74e2724a','0xba53d66ac20637f64b05f29402a899f08f80bb6e','0x1a7661cff5c31aa8774c53defbd2b632d643a610','0x6969550a22a29d1cdc8459fcdf638adbecf409e3','0x1a40bf4302f4177f20ace3a7d55cda31984ee773','0x29d280a8208cf2bdeba746868aa6745e09807d88','0xe334a54bcf21d88932ed88ed2452e0ee0e24f5bf','0xd2fa2a742156904ee436a354a56f6a50b08dd329','0x8b81f7bc94ae35067a940a9a760a9f5adbf09268','0x6c91e059387d9240d6b83be092831d58e96e0e6c','0x1da7e97f0b8a2c1ca46abdb0ba7b0046cb5be915','0xe6f713ef81d3ec5fb1f8cf2966ea5357da61723f','0x0511ffdf861ae1ce16246b6fce226fe66e9c4b1f','0x166769354205bcf47945d989572b6dbeaed791be','0xc3bd2f8c38593f6ac5c6b7f55ac43ec7b658ca5b','0xc35bd166e286d021b14118ae8dcbf691b30a8970','0xbad692a9fe0409518a0f719613feb1c42cb8bc36','0x7b91268f4ae255c06b2a6011e03f91bc4a426f48','0xc0f1daf4391e3a6ba3d959297c20d1159d582ec6','0x57226b3bad476a2247736f8791242a2be02c8024','0x767c24c71ce5e00e413fc30b41aa3074c8b221e4','0x0c04ff41b11065eed8c9eda4d461ba6611591395','0x20f85292824b41c48f2e2b73861a22ba7788f0d4','0x651b133a0ed8f869267ebbb549ef418fc1cb8491','0xdac2d261d2fc92fbb11e9eec8d5bbf06370eb321','0x3a22338595beac548e6e86ad95641da218a1f8b3','0x7d9cb11b51595df642e069660c2ae67c3c410a09','0xdeadca7048d4751b60f935adef327da9550e445a','0x90a66bb8f16a7829807efc3e74900c830f44d4bb','0x7445fc922e383bb61da62b7d68c6ec5d164af446','0x248d6cdf6207af27edd4d03e33f05bde29c232c0','0x541c02e59c7b2a4107427f9c02064d67cd7df92f','0xb40ddebcd4ad60c9f68b008ebf423cb28d11de62','0xce176825afc335d9759cb4e323ee8b31891de747','0x4d8b2febb878346389be70e9c199ef74467b166e','0xcba4d685c560b1aba46cd429ab2f4d41dbf3d65f','0x0fd28e1fc7feea029c77508dc775ac910c54c8ac','0xc7e3b4ccb7d9683b85b32ebc636310232b401bb9','0x472024566281fff51d2e6356f36a1e6239359845','0x9959d38d7dd5a5b92def3468fba4a80038ade4e9','0xcf384e0715c6eca371e2fdc368d7cff2240f3cfe','0x6b71b73b25f7e5cb5132e4542b7fbd90e38b80ef','0x90e0cbe916700ba537332d327c3020b8cf019c3b','0xa2400e2f0f4f70368fc00f339bd9bfc937e7d271','0x677a9bf12c7e1312526760146d4d6a0b496caf68','0x1f5eaac2f7ebca14f86882b7724bcce3fdfea2bd','0x509aad0ebb0027937a519414ad3023af77c75035','0x734be1702fda8a6cd3e68c6e6171e995d7531d65','0x79cfbba249657612d27c58f3b1811ac5d3ff6f89','0x69420ead81514b283d49594a35fcc7dfe0036fa1','0xe8baf4809f81cbffd8f3a8a89eb9f1de51d82846','0xdb0238975ce84f89212ffa56c64c0f2b47f8f153','0xd3f6efc975a47c38668ef98c39e5a0273afb5f18','0x59ff2f2673f86b78a8179048b117aebfeca46349','0x5b80d10959e58db8922bd6643f5a51023d665d68','0xedb40d2632b432173b48ee4d828afe10790a25e1','0x3fbabe48e137c9ef4f8cb517a4762add7e6242ae','0xea318541a292ab46e72722b02b3b060403cbbae0','0xababe51c38843f71e7b4b3139a67c39c909eb47f','0x47e3014d94401410e308505cb3e8970dea8074a6','0x69420034e8ea5797087a629e88c3ac1af3edc54e','0x6dce26b25cc024e343039f649694140b76974a1d','0xd8e75ad90928f81061c4c6f532e75c581023422d','0x661bdcee4749313e7b4386ed19b6675a1b392f43','0x69695c8427c01038cb11091f3820cb457668acba','0xab5738052669e9946a2cd6b635b40440af84996f','0x600d6777805f45e960c84726f721757a0bfd3125','0x179c5bfb1f9ddd3a6895f4f15b9e52616f13673d','0x37d47be72cc16a324533ac1b819fcc0f089cc672','0x6942016b8de9d18a5831eeda915e48b27cc8e23d','0xd05a02e4ecbd6c766a6f9dc96cedbdc1f1b591be','0x7ac126efa0758d0b199f07f3d1d66a4f82856df6','0x07e6d1a3839c5d037d66dabe4ffc59a1dab77631','0x888ca98a609e3e9b067259ffb6a9955de26c0068','0xd2aa2bf8ad812e6ae08560479a087ea484e76397','0x020b477193839289726d5042ea62b91121f0d5c0','0x37dd1cc64e0b995f21a682ec9e2c13ccba386aef','0xaa0181e6565346ad64094198ec854405ff54b1cd','0x2022cad0523b5928bced1f587de03112d5d44324','0xbea5bc325580d8e649bd8991b62b5caea39d3394','0x667bd45d6911ea696e33b1eea5022a8fbba1fe79','0x562b9166776938f8a842291d8b886d821ec9c3c2','0xb9bcc2abf5fe8cac12431ae19c10d85d14ef0319','0xa1ba34b1906c6706cee904b6d8bb08a6412d94a5','0x7c75b20c0a040e327753394cb00ef207878c4757','0x4955bf6ff2f19ba5ae65e8d0f5840112bd317e97','0x6942093d46081b2dd7097aa7879165ca37c6cb92','0xea1869c3975ab3dacfa124439d017e721186f638','0x610c04f69f8b3fde16c19c77a7dfed24b9a63853','0x2df17b8394d8cb4b8de6a534e27a217f15a91cf4','0xdc8dfd979293302568b82ec0036e815c7ee3953e','0x3d491f985b980818f27d980d629b8335bb06544a','0x05dc115b1076e3c656b5d80eeb55de7b4e9ac0a9','0x90846ee5ca3c06682c03e573601bd072dcdb08c6','0xcef3124cdc31813e4498198d3f503790fa3590bb','0x69d4e88aeda2ee5692fe95140e2b922c18eb8bad','0x81085e62f97527ead18ab640be4c6cd67a1a618a','0xa2f2fcf57249db7c72191768090b9f57e472aef8','0x553cf8bedceaa04648ecc19271ab3199e334d7d7','0x696969e69c81337d18133d245a6b8dbee7533694','0x513c569ef61d0af73ef6dda0d0ecaff8d1bd9f0e','0xcab1121a6c189852be5ccc73899ac79ba9d4a21d','0x7bee078e2366eda19f80b3724cb219820195b07d','0x1aba49b1724553a56984244b363a8cb48f9e0976','0xbf20963c84241d7c2bf4daf34c891a683012d941','0x626262530ab54e1b18dd587834d1874c6a693a87','0x0666b157e15930b80199b24d98859e99c896696a','0x2588cdb42c7cb543c48354b193d07a0fb40bb12d','0x7226b18885cac16f3f4aa7178c38a896dd534553','0x694200465963898a9fef06a5b778d9e65721685c','0x6942040b6d25d6207e98f8e26c6101755d67ac89','0x09027a55249f621f07bb2e922478d884fbb94256','0x7c90cc4310c187d1cf5951ed29fb1ac67b8d4590','0x943c77c1a212bb9f9890271dfeb3069d0e7667d7','0xc42ab7ae8ea32a950dbf18830bce5ce24b752908','0xd5bbba3aed191b06aace1928555ff5c1dea028e9','0x155788dd4b3ccd955a5b2d461c7d6504f83f71fa','0xda2c4be5333871a175694b14484644bfe17420ee','0x002c33c3146cb58d02876740782a2cebb8728840','0xbc230bc6bf99170b3e6f598e751836e8f86b5962','0xd0667d0618dc9b6d2a0a55f428b47c64bcf00416','0x694206013ff77315062188931d607b06fd7f8b33','0x58c58a5dd042bd77c8990053bfc9a8ed58d2bace','0xd0ffe94681c2f7766d530a59489b7a8725d47ef5','0x69420da6e645f2a6f6175b48d69cfd86fed97f52','0x44a023a4c32bdd2c89ee87ee76a2332b1a883012','0x69bab4d29b504d8a258e10d589518c225697e75a','0x3558635734d98dce6eae526fff25777b08ea3b43','0x9ad204436656b71ce217297768812bb1d4ffd356','0x9258e3b9dfa9d2633cfb4b7eddd05bda8b2d4830','0xc9a7ea21ccc02e162c42c7f3f35c4310eca3ec50','0x2fc72c4302a4e28c6073ffd30bbd1277baba78fb','0x6262f025997eb7f30244068e01ee4ad6d65efdea','0x7187131399923520e71fc35f5052a5940c5a472d','0x68aaa0d94ea163b9bbf659dc3766defb4c0ac7be','0x07040971246a73ebda9cf29ea1306bb47c7c4e76','0x314c4bd159949ed8f7bb02609a050abc138129b9','0xd2efed66e09368735580d8959976a9edcccb5365','0x946dad9b1e965d36660793933c6c0f1f01f2b54c','0xa6412c78022011fe7b4a545ab9edc8e353aebcdc','0xdeeeeed7744bee13d2ceaeb677025c1f045b7d98','0xc08dfad38da62215ccf58ae2551d7ceeb70e497a','0x141700a0cc1f8db7404b27113eafeef27f3469a6','0x6bf06b4c5039b4a2abbe224561c6fa276641506f','0x070401b53ba716684d79e62daa4ac478004f6776','0x1a8589b36a7054fd4b0479ced6db0c7f1fd54a0f','0x070456ae26f19000adebc8fe2e0792d1ca751f76','0x07040fe2832395cefe556cb6a878fe5f7b487976','0x0704d7c0759c09655b2956b7b136a82a7c157376','0xf19fb7c404c16ac17eee803397aacd049e6cf67c','0x899f6678ac76b2dcae719de45db5de4dda6bbc11','0x0704ded528f53c4af9c2c9544bbf09168cf81776','0x6a7c309017197a57cd1c9fd1f6f17edbaee19548','0x69602c3a33653cb7d2b8b0a4611d1fcce8c91152','0x364dc25d6f4465dd1b569e3e7344a93d38597f0e','0xd177179a80d49049f3aa4be145a921abd5fa88f8','0x576f2a4db39cb015fef67abc11e84a45d58337ae','0x07ddacf367f0d40bd68b4b80b4709a37bdc9f847','0x3df012b9ecf56b847421292c0f80e87645875853','0xe0a236c5e26015f55c1e2522daa24d415d62a76f','0x694201f10eed6c61ecb48efc99c16d6d9756675c','0x2f71fa5ed21edcbae8029119434611a7703548f5','0x999059581e497b9c1bfdccd0590aef572ec147f1','0xcb3d7baacec75fc058dbbb577b4cf2327f2e4a56','0x2005008f9b366f51612174e29568c07952f29190','0x420690b0736e88ea43373a6dd7e1c8f225050e7b','0x975ef3607eee06e13094d4f3dc8359d7bbbd153e','0x66638d7fc0cf3424aac47422060dd81790fef216','0x0000698c9721a1283e2dd3c63dddda082199307e','0x65420f00fab946d3a467729cc6489a95f2babe73','0x606162af732322df5bbbcd6e87e2b1215837d4fb','0x89ef4bb349909d4d045db3042125e8b15c405915','0x5015f716f329893862442a7da4c5b08e54471782','0x070722f1350c1f6b4c88ca1ebaa08a5acbded45b','0x7c5925771859a5cb9fd35097a39e7e8c7c65039b','0xeb15a54e462e1ec239c09f1e6aa322b45c8da225','0x738eea792dfb0598847780246c466a47da745305','0x08980b25953d81947306b1b00a5ff4a41871f285','0x0a348e0ea5e0ef5cdfa7801caf4dd6487343a0d6','0x2022f2f160f10205699c77e211e24a347b8fe643','0x69965c80640bc4dfc4abdd0fcec5005555812e24','0x92408e203da8bfc3947c0b3b066ac3a68bab24a1','0x69420dd0dc17e01b05a64e724842bf1ac349b7cf']

            for check_folder in ['_bMint','_b0','_b1','_b2','_b3','_b4','_b5','_step1','_step2','_step3','_step4','_step5','_step6']:
                #path_stats = f"{FILES_DIR}/temp/{TRAIN_FOLDER_MAIN}/{check_folder}"
                path_stats = f"{FILES_DIR}/temp/stats_22jun_2jul/{check_folder}"
                files = [f for f in listdir(path_stats) if isfile(join(path_stats, f))]
                data_stat = []
                for file in files:
                    with open(f"{path_stats}/{file}", "r") as f:
                        # print('file:', file)
                        token_address = file.split('.')[0].split('_')[0].lower()

                        # второе условие нужно только если будем делать проверку по X_val
                        if 1 or token_address in X_val_addresses:
                            try:
                                data_stat.append({'token_address': token_address} | json.load(f))
                            except json.decoder.JSONDecodeError:
                                print(f"file {file} loaded with JSONDecodeError")

                print(f"data_stat on {check_folder} len={len(data_stat)}")
                if not len(data_stat):
                    continue

                df_atl = pd.read_csv(f"{FILES_DIR}/temp/target-column_22jun_2jul.csv", header=0)
                target_dict = dict(zip(df_atl['token_address'], df_atl['TARGET']))

                list_addresses_not_scam = []
                for stat in data_stat:
                    if target_dict[stat['token_address']] == 1:
                        list_addresses_not_scam.append(stat['token_address'])
                # print('list_addresses_not_scam:')
                # print(list_addresses_not_scam)


                df = pd.DataFrame.from_records(data_stat)

                for var in ['']:
                    df.loc[df[f'{var}token1_address'] == utils.WETH, f'{var}token1_address'] = "WETH"
                    df.loc[df[f'{var}token1_address'] == utils.USDC, f'{var}token1_address'] = "USDC"
                    df.loc[df[f'{var}token1_address'] == utils.USDT, f'{var}token1_address'] = "USDT"

                    df_token1_address = pd.get_dummies(df[f'{var}token1_address'])
                    df = pd.concat([df, df_token1_address], axis=1).reindex(df.index)
                    df.drop(f'{var}token1_address', axis=1, inplace=True)

                    df_exchange = pd.get_dummies(df[f'{var}exchange'])
                    df = pd.concat([df, df_exchange], axis=1).reindex(df.index)
                    df.drop(f'{var}exchange', axis=1, inplace=True)

                if 'WETH' not in df.columns: df['WETH'] = 0
                if 'USDC' not in df.columns: df['USDC'] = 0
                if 'USDT' not in df.columns: df['USDT'] = 0
                if 'V2' not in df.columns: df['V2'] = 0
                if 'V3' not in df.columns: df['V3'] = 0


                for feature in model.feature_names_:
                    if feature not in df: df[feature] = None
                df = df[model.feature_names_]

                _predictions = model.predict(df)
                _predictions_proba = model.predict_proba(df)

                correct_0 = 0
                total_0 = 0
                correct_1 = 0
                total_1 = 0
                count_in_atlantes = 0
                
                for i, stat in enumerate(data_stat):

                    if target_dict[stat['token_address']] == 0: total_0 += 1
                    else: total_1 += 1

                    if _predictions[i] == 0 and target_dict[stat['token_address']] == 0:
                        correct_0 += 1
                    if _predictions[i] == 1 and target_dict[stat['token_address']] == 1:
                        correct_1 += 1

                    sign = '+'
                    if _predictions[i] != target_dict[stat['token_address']]: sign = '-'
                    if sign == '-' or sign == '+' and _predictions[i] == 1:
                        print(f"{stat['token_address']} target={target_dict[stat['token_address']]}, predict={_predictions[i]} {round(max(_predictions_proba[i])*100)}% {sign}")
                    
                    # in_atlantes = '-'
                    # if stat['token_address'] in altantes_tokens:
                    #     in_atlantes = '+'
                    #     if _predictions[i] == 1:
                    #         count_in_atlantes += 1
                    #         print(stat['token_address'])

                    # if _predictions[i] == 1:
                    #     print(f"{stat['token_address']},1,{round(max(_predictions_proba[i])*100)},{target_dict[stat['token_address']]},{sign},{in_atlantes}")

                print(f"folder {check_folder}")
                print(f"  correct_0: {correct_0} / {total_0} ({round(100*correct_0/total_0) if total_0 != 0 else ''}%) {total_0 - correct_0}")
                print(f"  correct_1: {correct_1} / {total_1} ({round(100*correct_1/total_1) if total_1 != 0 else ''}%)")
                # print(f"  count_in_atlantes: {count_in_atlantes}")
                print('---')





    if 0:
        
        # tensorflow==2.12.0
        # keras==3.1.1
        

        # top_words = 5000
        # max_review_length = 500
        # embedding_vecor_length = 32
        # model = Sequential()
        # model.add(Embedding(top_words, embedding_vecor_length, input_length=max_review_length))
        # model.add(Conv1D(filters=32, kernel_size=3, padding='same', activation='relu'))
        # model.add(MaxPooling1D(pool_size=2))
        # model.add(LSTM(100))
        # model.add(Dense(1, activation='sigmoid'))
        # model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
        # print(model.summary())
        # model.fit(X_train, y_train, epochs=3, batch_size=64)
        # # Final evaluation of the model
        # scores = model.evaluate(X_test, y_test, verbose=0)
        # print("Accuracy: %.2f%%" % (scores[1]*100))

        #opt = Adam(lr=0.01)
        # model = Sequential()
        # model.add(Embedding(len(df), 16, input_length=X_train.shape[1], mask_zero=True))
        # model.add(LSTM(12, dropout=0.7, recurrent_dropout=0.7))
        # model.add(Dense(6, kernel_regularizer=regularizers.l1_l2(0.3)))
        # model.add(Dropout(0.9))
        # model.add(Dense(1, activation='softmax'))
        # model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
        # es = EarlyStopping(monitor='val_loss', mode='min', patience=50)
        # history_lstm = model.fit(X_train, y_train, epochs=200, batch_size=64, validation_data=(X_test,y_test),shuffle=False)

        # preds = model.predict(X_test)
        # print(preds)


        # Dataset=pd.read_csv(f"{FILES_DIR}/temp/train_titanic.csv")
        # Dataset.dropna(inplace=True)
        # Dataset_droped = Dataset.drop(['PassengerId', 'Name', 'Ticket'], axis=1)
        # Dataset_one_hot = pd.get_dummies(Dataset_droped)

        # X = Dataset_one_hot.drop(['Survived'], axis=1)
        # y = Dataset_one_hot['Survived']
        # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # print('-----')
        # print('titanic')
        # print('X_train:', X_train)
        # print('X_test:', X_test)
        # print('y_train:', y_test)



        model = Sequential()
        
        #model.add(Embedding(input_dim = 188, output_dim = 50, input_length = len(X_train)))

        model.add(LSTM(units=64, input_shape=(X_train.shape[1], 1)))

        #model.add(LSTM(32, return_sequences=False, activation = 'sigmoid'))

        # model.add(LSTM(output_dim=256, activation='sigmoid', inner_activation='hard_sigmoid', return_sequences=True))
        # model.add(Dropout(0.5))
        # model.add(LSTM(output_dim=256, activation='sigmoid', inner_activation='hard_sigmoid'))

        #model.add(LSTM(50))

        model.add(Flatten())
        model.add(Dense(32, activation='relu'))
        model.add(Dense(8, activation='relu')) # linear
        
        model.add(Dropout(0.5))
        model.add(Dense(1, activation='softmax')) # activation='sigmoid'
        model.compile(loss='binary_crossentropy', optimizer='rmsprop', metrics=['accuracy']) # optimizer='rmsprop'
        #model.compile(loss='binary_crossentropy', optimizer=Adam(), metrics=['accuracy'])

        # https://stackoverflow.com/questions/55309566/keras-model-predict-only-returns-one-figure-for-binary-classification-task
        # https://github.com/tensorflow/tensorflow/issues/35876 Keras predict with sigmoid output returns probabilities instead of labels

        # https://colab.research.google.com/gist/ymodak/0b5ff42ac90f05e953848b1b6e0f2973/tf_forum.ipynb#scrollTo=0MJhxwMbD0KN Titanic dataset

        es = EarlyStopping(monitor='val_loss', mode='min', patience=3, restore_best_weights=True)
        hist = model.fit(X_train, y_train, batch_size=64, epochs=100, validation_split=0.1, shuffle=True, callbacks=[es], verbose=1)
        score, acc = model.evaluate(X_test, y_test) # batch_size=1
        print('Test score:', score)
        print('Test accuracy:', acc)
        score_val, acc_val = model.evaluate(X_val, y_val)
        print('Test score val:', score_val)
        print('Test accuracy val:', acc_val)


        model.save(f'{FILES_DIR}/lib/models/{TRAIN_FOLDER_MAIN}/ml{TRAIN_FOLDER_CAT}.keras')


        exit()





    if 0:

        print('.'*60)
        print('RandomForestClassifier()')

        rf = RandomForestClassifier(n_estimators=400, random_state=42) # max_depth=20
        rf.fit(X_train,y_train)
        rf_pred = rf.predict(X_test)
        print(classification_report(y_test,rf_pred))
        print(f"accuracy_score: {accuracy_score(y_test, rf_pred)}")

        rf_pred_val = rf.predict(X_val)
        print(f"accuracy_score val: {accuracy_score(y_val, rf_pred_val)}")
        

    if 0:

        print('.'*60)
        print('KNeighborsClassifier()')

        knn = KNeighborsClassifier(n_neighbors=400, n_jobs=-1)
        knn.fit(X_train,y_train)

        knn_pred = knn.predict(X_test)
        print(classification_report(y_test,knn_pred))
        print(f"accuracy_score: {accuracy_score(y_test, knn_pred)}")

        knn_pred_val = knn.predict(X_val)
        print(f"accuracy_score val: {accuracy_score(y_val, knn_pred_val)}")

    if 0:

        print('.'*60)
        print('SVC()')
        svm_model = SVC(probability=True)
        svm_model.fit(X_train, y_train)
        svm_pred = svm_model.predict(X_test)
        svm_pred_val = svm_model.predict(X_val)
        print(classification_report(y_test,svm_pred))
        print(f"accuracy_score: {accuracy_score(y_test, svm_pred)}")
        print(f"accuracy_score val: {accuracy_score(y_val, svm_pred_val)}")


    if 0:

        print('.'*60)
        print('AdaBoostClassifier()')
        ada_model = AdaBoostClassifier(DecisionTreeClassifier(max_depth=20),n_estimators=1000)
        ada_model.fit(X_train, y_train)
        ada_pred = ada_model.predict(X_test)
        ada_pred_val = ada_model.predict(X_val)
        print(classification_report(y_test,ada_pred))
        print(f"accuracy_score: {accuracy_score(y_test, ada_pred)}")
        print(f"accuracy_score val: {accuracy_score(y_val, ada_pred_val)}")


    if 0:

        print('.'*60)
        print('ExtraTreesClassifier()')
        et_model = ExtraTreesClassifier(n_estimators=500)
        et_model.fit(X_train, y_train)
        et_pred = et_model.predict(X_test)
        et_pred_val = et_model.predict(X_val)
        print(classification_report(y_test,et_pred))
        print(f"accuracy_score: {accuracy_score(y_test, et_pred)}")
        print(f"accuracy_score val: {accuracy_score(y_val, et_pred_val)}")

    
    if 0:

        print('.'*60)
        print('GradientBoostingClassifier()')
        gbrc_model = GradientBoostingClassifier(n_estimators=500) # max_depth=20
        gbrc_model.fit(X_train, y_train)
        gbrc_pred = gbrc_model.predict(X_test)
        gbrc_pred_val = gbrc_model.predict(X_val)
        print(classification_report(y_test,gbrc_pred))
        print(f"accuracy_score: {accuracy_score(y_test, gbrc_pred)}")
        print(f"accuracy_score val: {accuracy_score(y_val, gbrc_pred_val)}")



    # здесь StandartScalar()
    # https://www.kaggle.com/code/kaanboke/beginner-friendly-catboost-with-optuna

    # lr = LogisticRegression(solver='liblinear')
    # lda= LinearDiscriminantAnalysis()


        
    print("executied in %.3fs" % (time.time() - start_time_e))