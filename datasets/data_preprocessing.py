import itertools
import io
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
import os
import json
import numpy as np
import pandas as pd
import math
import codecs
from codecs import open
import requests
from google.cloud import bigquery, bigquery_storage, storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from sklearn.preprocessing import LabelEncoder


GCP_CREDENTIAL_PATH = "airflow-v2-keyfile.json"
GCP_CREDENTIAL_LOCAL_PATH = Path(GCP_CREDENTIAL_PATH).expanduser()

GCP_CREDENTIAL = service_account.Credentials.from_service_account_file(
    GCP_CREDENTIAL_LOCAL_PATH,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
BIGQUERY_CLIENT = bigquery.Client(
    credentials=GCP_CREDENTIAL, project=GCP_CREDENTIAL.project_id
)
BIGQUERY_STORAGE_CLIENT = bigquery_storage.BigQueryReadClient(
    credentials=GCP_CREDENTIAL
)
STORAGE_CLIENT = storage.Client(
    credentials=GCP_CREDENTIAL, project=GCP_CREDENTIAL.project_id
)


toeic_test = [date(2021, 6, 27), date(2021, 7, 11),
              date(2021, 7, 25), date(2021, 8, 8),
              date(2021, 8, 22), date(2021, 8, 29),
              date(2021, 9, 12), date(2021, 9, 26),
              date(2021, 10, 10), date(2021, 10, 24),
              date(2021, 11, 6), date(2021, 11, 21),
              date(2021, 12, 5), date(2021, 12, 19),
              date(2022, 1, 8), date(2022, 1, 23),
              date(2022, 2, 6), date(2022, 2, 20),
              date(2022, 2, 27), date(2022, 3, 13),
              date(2022, 3, 27), date(2022, 4, 10),
              date(2022, 4, 30), date(2022, 5, 15),
              date(2022, 5, 29), date(2022, 6, 12),
              date(2022, 6, 26), date(2022, 7, 10),
              date(2022, 7, 24), date(2022, 8, 7),
              date(2022, 8, 21), date(2022, 8, 28),
              date(2022, 9, 4), date(2022, 9, 25),
              date(2022, 10, 15), date(2022, 10, 30),
              date(2022, 11, 13), date(2022, 11, 27),
              date(2022, 12, 11), date(2022, 12, 25),
              date(2023, 1, 14), date(2023, 1, 29),
              date(2023, 2, 5), date(2023, 2, 19),
              date(2023, 2, 26), date(2023, 3, 12),
              date(2023, 3, 26), date(2023, 4, 15),
              date(2023, 4, 30), date(2023, 5, 14),
              date(2023, 5, 28), date(2023, 6, 11),
              date(2023, 6, 25), date(2023, 7, 9),
              date(2023, 7, 30), date(2023, 8, 6),
              date(2023, 8, 20), date(2023, 8, 27),
              date(2023, 9, 10), date(2023, 9, 24),
              date(2023, 10, 14), date(2023, 10, 29),
              date(2023, 11, 12), date(2023, 11, 26),
              date(2023, 12, 10), date(2023, 12, 24)
              ]


def user_data():
    sql = open('./sql/user.sql', mode='r', encoding='utf-8-sig').read()
    user_df = (
        BIGQUERY_CLIENT.query(
            sql
        )
            .result()
            .to_dataframe(bqstorage_client=BIGQUERY_STORAGE_CLIENT)
    )

    return user_df


def action_before_payment_data():
    sql = open('./sql/action_before_payment.sql', mode='r', encoding='utf-8-sig').read()
    abp_df = (
        BIGQUERY_CLIENT.query(
            sql
        )
            .result()
            .to_dataframe(bqstorage_client=BIGQUERY_STORAGE_CLIENT)
    )

    return abp_df


def arppu_data():
    sql = open('./sql/arppu.sql', mode='r', encoding='utf-8-sig').read()
    arppu_df = (
        BIGQUERY_CLIENT.query(
            sql
        )
            .result()
            .to_dataframe(bqstorage_client=BIGQUERY_STORAGE_CLIENT)
    )

    return arppu_df


def preprocessing():
    user_df = user_data()
    abp_df = action_before_payment_data()
    arppu_df = arppu_data()

    dataset = pd.merge(abp_df[abp_df.columns[:-4]], arppu_df, how='left',
                       left_on='registered_at', right_on='paid_at')
    dataset = pd.merge(dataset, user_df[['auth_id', 'agree_marketing', 'agree_push', 'last_activity_at']], how='left',
                       on='auth_id')
    dataset = dataset.rename(columns={'paid_at_x': 'paid_at'})
    dataset['paid'] = np.where(dataset['paid_at'].isnull(), 0, 1)

    # agree_marketing, agree_push를 선택조차 하지 않았으면 학습에서 제외
    # dataset['last_activity_at'] = dataset['last_activity_at'].fillna('no')
    dataset = dataset.dropna(axis=0, subset=['agree_marketing', 'agree_push']).reset_index(drop=True)

    l = [[], [], []]
    c = [[], [], []]
    for add_column, data in zip([l, c], [dataset['last_activity_at'], dataset['installed_at']]):
        for i in data:
            if (i == None) | (i == np.nan) | (type(i) == str) | (i == float('nan')):
                add_column[0].append(None)
                add_column[1].append(None)
                add_column[2].append(None)
            else:
                count = 0
                for j in toeic_test:
                    if count == 3:
                        break
                    if j == toeic_test[-1]:
                        if count == 0:
                            add_column[1].append(None)
                            add_column[2].append(None)
                            if i == date(2022, 12, 25):
                                add_column[0].append(None)
                        elif count == 1:
                            add_column[2].append(None)
                        elif count == 2:
                            pass
                    if i < j:
                        if count == 0:
                            add_column[0].append(j - i)
                        elif count == 1:
                            add_column[1].append(j - i)
                        elif count == 2:
                            add_column[2].append(j - i)
                        count += 1

    dataset['score_diff_td'] = dataset['target_score'] - dataset['diag_score']
    dataset['score_diff_ld'] = dataset['last_cell_score'] - dataset['diag_score']

    # 진단 안본 사람들 빼고 하기에는 구매자가 꽤 있음
    dataset_1 = dataset[['agree_marketing', 'agree_push',
                         'access_cnt', 'diag_score', 'target_score', 'score_diff_td', 'first_cell_type',
                         'first_cell_score', 'last_cell_score', 'score_diff_ld', 'cell_cnt', 'cycle_cnt',
                         'basics', 'lessons', 'mock_exams', 'my_note_quizzes', 'questions', 'reviews', 'vocab',
                         'self_lessons', 'self_questions', 'self_virt_exams', 'self_vocab', 'season', 'arppu',
                         'paid']]

    dataset_1['paid'] = dataset_1['paid'].fillna(0)

    for column, name in zip([l, c], ['l', 'c']):
        for num in range(len(column)):
            dataset_1[name + str(num)] = column[num]

    for i in dataset_1.columns[-6:]:
        dataset_1[i] = dataset_1[i].dt.days

    LE = LabelEncoder()
    dataset_1['first_cell_type'] = LE.fit_transform(dataset_1['first_cell_type'])
    dataset_1['agree_marketing'] = pd.to_numeric(dataset_1['agree_marketing'])
    dataset_1['agree_push'] = pd.to_numeric(dataset_1['agree_push'])
    dataset_1['arppu'] = pd.to_numeric(dataset_1['arppu'])
    dataset_1['paid_num'] = dataset_1['paid']
    del dataset_1['paid']

    return dataset_1


if __name__ == "__main__":
    a = preprocessing()
    a.to_csv('./datasets/data.csv', index=False)
