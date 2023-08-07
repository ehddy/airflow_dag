import requests
from bs4 import BeautifulSoup
import pandas as pd
from airflow.models import XCom  


__all__ = ['get_generate_ai']

def get_generate_ai(**kwargs):
    url = 'https://news.google.com/search?q=생성형AI'

    response = requests.get(url)

    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
    
    news_list = soup.select('#yDmH0d > c-wiz > div > div.FVeGwb.CVnAc.Haq2Hf.bWfURe > div.ajwQHc.BL5WZb.RELBvb > div > main > c-wiz > div > div')[:-1]

    total_list = []

    for news in news_list:
        url = "https://news.google.com/" + news.select_one('a')['href'].replace('./', '')
        title = news.select_one('div > article > h3 > a').text
        press = news.select_one('div > article > div > img')['alt']
        posted_date = news.select_one('div > article > div > div > time')['datetime']
        total_list.append([title, press, posted_date, url])

    data = pd.DataFrame(total_list, columns=['title', 'press_name', 'post_date', 'link'])
    # DataFrame을 XCom에 저장하여 다른 작업으로 전달합니다.
    # kwargs['ti'].xcom_push(key='data', value=data)
    # data.to_csv('/opt/airflow/dags/crawl_data/generative_ai_data.csv', index=False, encoding='utf-8-sig')
    # print('저장 완료')
    # DataFrame을 XCom에 저장하여 다른 작업으로 전달합니다.
    kwargs['ti'].xcom_push(key='data', value=data)

