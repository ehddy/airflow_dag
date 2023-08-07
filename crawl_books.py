import requests
from bs4 import BeautifulSoup
import pandas as pd
from airflow.models import XCom  


__all__ = ['get_books']

def get_books(**kwargs):
    daily_top200_book_list = []
    rank = 1
    for i in range(1, 11):

        url = f'https://www.yes24.com/24/category/bestseller?CategoryNumber=001&sumgb=07&PageNumber={i}'

        response = requests.get(url)

        if response.status_code == 200:
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')

        else : 
            print(response.status_code)


        book_list = soup.select('table > tr')

        for i, book in enumerate(book_list):
            if i % 2 == 0:
                book.select('#category_layout > tbody > tr:nth-child(33) > td.goodsTxtInfo > div > a')
                try:
                    review_count = int(book.select_one('td.goodsTxtInfo > p.review > a').text.split('개')[0])
                except:
                    review_count = 0
                title = book.select_one('img')['alt']
                author_name = book.select_one('td.goodsTxtInfo > div > a:nth-child(1)').text
                publish_company = book.select_one('td.goodsTxtInfo > div').text.split('|')[1].strip()
                publish_date = book.select_one('td.goodsTxtInfo > div').text.split('|')[2].split('\r')[0].strip()
                price = int(book.select_one('td.goodsTxtInfo > p:nth-child(3) > span.priceB').text.split('원')[0].replace(',', ''))

                daily_top200_book_list.append([rank, title, author_name, price,publish_company, publish_date, review_count])
                rank += 1 

    data = pd.DataFrame(daily_top200_book_list, columns=['rank', 'title', 'author', 'price', 'publishing_house', 'publication_date', 'review_count'])
        # DataFrame을 XCom에 저장하여 다른 작업으로 전달합니다.
    # kwargs['ti'].xcom_push(key='data', value=data)
    # data.to_csv('/opt/airflow/dags/crawl_data/generative_ai_data.csv', index=False, encoding='utf-8-sig')
    # print('저장 완료')
    # DataFrame을 XCom에 저장하여 다른 작업으로 전달합니다.
    kwargs['ti'].xcom_push(key='data', value=data)

