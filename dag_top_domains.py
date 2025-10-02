#!/usr/bin/env python
# coding: utf-8

# In[5]:


import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь показана запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_dom():# 10 доменных зон с наибольшим количеством доменов
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_exact'] = top_data_df['domain'].apply ( lambda x: x.split('.')[0])
    top_data_df['Domain_name'] = top_data_df['domain'].str.split('.').str[-1]
    top_data_df_1 = top_data_df.groupby(['Domain_name'], as_index=False).agg({'domain_exact': 'nunique'}).sort_values(['domain_exact'], ascending= False)
    top_data_df_10 = top_data_df_1.head(10)
    top_data_df_10 = top_data_df_10.reset_index()
    top_data_df_10 = top_data_df_10['Domain_name']
    with open('top_data_df_10.csv', 'w') as f:
        f.write(top_data_df_10.to_csv(index=False, header=False))


def get_max_len():# Самый длинный домен
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_exact'] = top_data_df['domain'].apply ( lambda x: x.split('.')[0])
    top_data_df ['len'] = top_data_df['domain_exact'].str.len()
    top_data_df_len = top_data_df.sort_values(['len'], ascending= False).reset_index()
    top_data_df_len_max =  top_data_df_len.drop(['domain','rank','index', 'Domain_name'], axis=1)
    top_data_df_len_max[top_data_df_len_max['len'] == top_data_df_len_max['len']. max ()]
    top_data_df_len_max = top_data_df_len_max.head(1)
    with open('top_data_df_len_max.csv', 'w') as f:
        f.write(top_data_df_len_max.to_csv(index=False, header=False))

def get_place_air():# Какое место занимает домен airflow.com
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df_len_max_air_1 = top_data_df[top_data_df['domain'] == 'airflow.com']
    top_data_df_len_max_air_2 = top_data_df_len_max_air_1.index[0]+1
    with open('top_data_df_len_max_air_2.csv', 'w') as f:
        f.write(top_data_df_len_max_air_2.to_csv(index=False, header=False))

def print_data(ds):
    with open('top_data_df_10.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_df_len_max.csv', 'r') as f:
        all_data_com = f.read()
    with open('top_data_df_len_max_air_2.csv', 'r') as f:
        all_data_air = f.read()   
    date = ds

    print(f'Top 10 count of domains zone for date {date}')
    print(all_data)

    print(f'Maximum domain length for date {date}')
    print(all_data_com)
    
    print(f'Place of domain Airflow.com for date {date}')
    print(all_data_air)

default_args = {
    'owner': 'j.hickova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 3, 4),
}
schedule_interval = '30 15 * * *'

dag = DAG('dag_top_domains', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_dom',
                    python_callable=get_top_dom,
                    dag=dag)

t3 = PythonOperator(task_id='get_max_len',
                        python_callable=get_max_len,
                        dag=dag)

t4 = PythonOperator(task_id='get_place_air',
                        python_callable=get_place_air,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t5




# In[ ]:




