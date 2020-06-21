import time
import random
import requests
import settings
import pandas as pd

from threading import Thread
from tqdm import tqdm
from datetime import datetime


HEADERS = {'Ocp-Apim-Subscription-Key': random.choice(settings.FUND_FACTSHEET_KEY)}
KEYS = settings.FUND_DAILY_INFO_KEY

def sec_list():
    url = 'https://api.sec.or.th/FundFactsheet/fund/amc'
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        return r.json()

def fund_list(unique_id):
    url = f'https://api.sec.or.th/FundFactsheet/fund/amc/{unique_id}'
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        return r.json()

def daily_nav(proj_id, nav_date, key):
    url = f'https://api.sec.or.th/FundDailyInfo/{proj_id}/dailynav/{nav_date}'
    r = requests.get(url, headers={'Ocp-Apim-Subscription-Key': key})
    if r.status_code == 200:
        return r.json()
    elif r.status_code == 204:
        return
    else:
        print(r.content)
        
def _task():
    key = KEYS.pop(0)
    while tasks:
        proj_id, date = tasks.pop(0)
        r = daily_nav(proj_id, date, key)
        if r:
            pd.DataFrame([[proj_id, date, r['last_val']]]).to_csv('out.csv', mode='a', header=False, index=False)
        time.sleep(0.2)

def agg():
    df = pd.read_csv('out.csv')
    df = df.drop_duplicates().pivot('date', 'proj_id', 'val').sort_index()
    col_map = {}
    for sec in sec_list():
        for fund in fund_list(sec['unique_id']):
            col_map[fund['proj_id']] = fund['proj_abbr_name']
    df.columns = [col_map[col] for col in df.columns]
    df.to_csv('agg.csv')

def scrape():
    tasks = []
    d = {'date': [], 'proj_name': [], 'val': []}
    for sec in sec_list():
        for fund in fund_list(sec['unique_id']):
            if fund['fund_status'] == 'RG':
                for date in pd.date_range(fund['regis_date'], datetime.now()).date:
                    tasks += [[fund['proj_id'], date]]

    threads = [Thread(target=_task) for _ in range(len(KEYS))]

    for thread in threads:
        thread.start()
    while tqdm(tasks):
        time.sleep(.5)
    for thread in threads:
        thread.join()

def filter_head_tail():
    df = pd.read_csv('agg.csv').set_index('date').fillna(method='ffill')
    df.index = pd.to_datetime(df.index)

    tail = df.groupby([df.index.month, df.index.year]).tail(1)
    head = df.groupby([df.index.month, df.index.year]).head(1)
    pd.concat([tail, head]).sort_index().to_csv('RG_filtered.csv')


if __name__ == "__main__":
    scrape()
    agg()
    filter_head_tail()