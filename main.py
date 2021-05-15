import urllib.request
import json,time,datetime,csv,requests,os
from lxml import etree
from datetime import date, timedelta
import pandas as pd

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'hk-border-traffic-12c98a1a477c.json'

from google.cloud import bigquery
client = bigquery.Client()

headers = {
    'authority': 'www.immd.gov.hk',
    'cache-control': 'max-age=0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
}

def get_one_day(date):
    datestr_path = date.strftime('%Y%m%d')
    url="https://www.immd.gov.hk/hkt/stat_"+datestr_path+".html"
    # headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
    res=requests.get(url=url, headers=headers,timeout=15)
    res.encoding = 'utf-8'
    html=res.text
    # print(html)
    html_data = etree.HTML(html).xpath('//div[@class="content"]//table/tbody/tr')
    # return html
    all_rows = []
    # print(html_data)
    datestr = date.strftime('%Y-%m-%d')
    for custom in html_data[0:-1]:
        line=custom.xpath("td")
        custom = line[3].text
        custom_rows = [
            [date,'入境','香港居民',custom,line[5].text.replace(',','').replace(' ','')],
            [date,'入境','內地訪客',custom,line[6].text.replace(',','').replace(' ','')],
            [date,'入境','其他訪客',custom,line[7].text.replace(',','').replace(' ','')],
            [date,'出境','香港居民',custom,line[10].text.replace(',','').replace(' ','')],
            [date,'出境','內地訪客',custom,line[11].text.replace(',','').replace(' ','')],
            [date,'出境','其他訪客',custom,line[12].text.replace(',','').replace(' ','')]
        ]
        all_rows.extend(custom_rows)

    df = pd.DataFrame(all_rows)
    # print(df)

    return all_rows

def main(*args, **kwargs):

    sql = """
        SELECT max(date) as max_date
        FROM `hk-border-traffic.traffic.raw`
    """

    df = client.query(sql).to_dataframe()
    # print(df)
    max_date = df['max_date'][0]
    print(max_date)

    today = datetime.datetime.now().date()
    print(today)

    d1day = timedelta(days=1)
    try_date = max_date + d1day
    if try_date < today:
        rows = get_one_day(try_date)
        re = client.insert_rows(
            client.get_table('hk-border-traffic.traffic.raw'), 
            rows)
        print(re)
        return len(rows)
    else:
        return None

if __name__ == '__main__':
    main()
    # get_one_day(datetime.date(2020, 11, 11))


'''
BQ Schema: 
date:DATE,direction:STRING,type:STRING,custom:STRING,value:INTEGER

'''
