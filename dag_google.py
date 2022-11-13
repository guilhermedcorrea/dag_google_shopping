
from datetime import datetime, timedelta
from sqlalchemy import text
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from includes.mssql.con_mssql import get_engine
from typing import Generator
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium import webdriver
import time
from selenium.webdriver.support import expected_conditions as EC
import re
import pandas as pd
from typing import Literal, List, Generator
from itertools import chain
from sqlalchemy import insert



default_args = {
    'owner':'airflow',
    'retries': 5,
    'retry_delay':timedelta(minutes=5)
}


options = webdriver.ChromeOptions() 
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--no-sandbox')
options.add_argument("--start-maximized")

driver = webdriver.Chrome(options=options, executable_path=r"/home/debian/Documentos/airflowapphausz/airflow/dags/includes/chromedriver/chromedriver")


def scroll_page() -> None:
    lenOfPage = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
    match=False
    while(match==False):
        lastCount = lenOfPage
        time.sleep(3)
        lenOfPage = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
        if lastCount==lenOfPage:
            match=True

def select_url_produtos_google(ti) -> Generator[list[dict], None, None]:
    seller_url = ti.xcom_pull(task_ids='get_urls', key='sellers')
    engine = get_engine()
    with engine.connect() as conn:
        try:
            query = (text("""SELECT  TOP(5) [marcaproduto],[paginaanuncio],[concorrente],[nomeproduto]
                          ,[ean],[sku],[urlgoogle],[marcaproduto]
                FROM [HauszMapaDev2].[Produtos].[MonitoramentoPrecos]
                WHERE concorrente LIKE '%MadeiraMadeira%' or concorrente LIKE '%Leroy%'"""))
            read_query = conn.execute(query)
            call = [{key: value for (key, value) in row.items()} for row in read_query]
            
            return call
        except Exception as e:
            print("erro", e)


def get_sellers(ti) -> (dict | None):
    lista_dicts: list = []
    driver.implicitly_wait(15)
    sellers = ti.xcom_push(key='seller_url', value='url')
    urls_sellers = ti.xcom_pull(task_ids='sellerurl', key='urls_seller')
    for seller in sellers:
        dict_produtos = {}
        time.sleep(3)
        
        driver.get(seller.get('paginaanuncio'))
        
        scroll_page()
        
        try:
            search = driver.find_element(By.XPATH,'//*[@id="REsRA"]')
            search.clear()
            search.send_keys(str(dict_produtos['EAN']))
        except:
            print("erro busca ean")
        try:
            busca = driver.find_element(By.XPATH,'//*[@id="kO001e"]/div/div/c-wiz/form/div[2]/div[1]/button/div/span').click()
        except:
            print("erro busca")
        time.sleep(1)
        try:
            google_url = driver.find_elements(By.XPATH,'//*[@id="rso"]/div/div[2]/div/div[1]/div[1]/div[2]/div[3]/div/a')
            urlgoogle = [url.get_dom_attribute('href') for url in google_url]
            if len(urlgoogle) <= 10:
                google_url = driver.find_elements(By.XPATH, '//*[@id="rso"]/div/div[2]/div/div/div[1]/div[2]/span/a')
                urlgoogle = [url.get_dom_attribute('href') for url in google_url]
        except:
            print("valor invalido")
            
        if next(map(lambda k: k if len(k) >=200 else(k if type(k) == str else 'valorinvalido'),urlgoogle),None):

            dict_item: dict = {}
            if urlgoogle !='valorinvalido' or urlgoogle != None:
                dict_item['urlgoogle'] = str('https://www.google.com/') + str(next(chain(urlgoogle)))
                dict_item['ean'] = str(dict_produtos['ean'])
                dict_item['nomeproduto'] = str(dict_produtos['nomeproduto'])
                dict_item['marcaproduto'] = dict_produtos['marcaproduto']
                dict_item['sku'] = str(dict_produtos['sku'])
                lista_dicts.append(dict_item)
                print(dict_item)
                return dict_item

                #insert_urls_google(referencia = dict_item['EAN'], loja = 'www.google.com.br'
                #                   , urlanuncio = str(dict_item['URLGOOGLE']))

def inser_urls_database():
    pass

with DAG(
    default_args=default_args,
    dag_id='google_shopping_urls',
    description='teste teste',
    start_date=datetime(2022, 10, 6),
    schedule_interval= '@daily'


) as dag:
    task1 = PythonOperator(
        task_id='select_url_produtos_google',
        python_callable=select_url_produtos_google,
        
    )

    task2 = PythonOperator(
        task_id = 'get_sellers',
        python_callable=get_sellers
    )
    
    task1 >> task2