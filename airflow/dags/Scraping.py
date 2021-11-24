from bs4 import BeautifulSoup
import requests
import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def _write_on_db(**context):

    task_instance = context['ti']
    df = task_instance.xcom_pull(task_ids='scrape_page')
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    postgres_hook.run(sql="DELETE FROM viviendas *")

    for i in range(len(df)):
        postgres_hook.run(sql="INSERT INTO viviendas VALUES({}, '{}', '{}', {},{},{},{});".format(df.loc[i,'Precio'],df.loc[i,'Ubicacion'],df.loc[i,'Barrio'],df.loc[i,'Metros'],df.loc[i,'Dorm'],df.loc[i,'Baños'],df.loc[i,'Años']))


def _create_table():

    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    postgres_hook.run(sql=" CREATE TABLE IF NOT EXISTS viviendas (precio INT, ubicacion VARCHAR(50), barrio VARCHAR(50), metros FLOAT, dorm INT, baños INT,años INT);")


def _get_num_pages():
    url = "https://www.argenprop.com/departamento-venta-region-capital-federal-1-o-más-cocheras-pagina-2"
    headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9     (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9'}
    r = requests.get(url,headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    paginas = soup.find_all('li', class_="pagination__page")
    paginas = int(paginas[-2].text)
    return paginas


def _scrape_page(**context):
    task_instance = context['ti']
    paginas = task_instance.xcom_pull(task_ids='get_num_pages')

    df=pd.DataFrame(columns=['Precio','Ubicacion','Barrio','Metros','Dorm','Baños','Años'])

    url = "https://api.webscrapingapi.com/v1"


    for j in range(2,paginas): # PARA MENOR TIEMPO reemplazar paginas por otro valor así no se trae la totalidad de páginas 
    
        params = {"api_key":"AD4b2btQq62KsBtRntfXFz0VCt0HglWu",
        "url":"https://www.argenprop.com/departamento-venta-region-capital-federal-1-o-más-cocheras-pagina-"+str(j)}
        
        r = requests.request("GET", url, params=params)
        
        soup = BeautifulSoup(r.text, 'lxml')

        precio = soup.find_all('p',class_="card__price")
        caract = soup.find_all('ul',class_="card__main-features")
        direccion = soup.find_all('p',class_="card__title--primary")

        try:
            ind = df.index[-1]+1
        except: 
            ind = 0

        for i in range(len(precio)):

            precios = precio[i].text.replace("USD ","").split()
            caracts = caract[i].text.split()
            direcciones = direccion[i].text.replace("Departamento en Venta en ","").split(", ")
            
            df.loc[ind+i,'Ubicacion'] = direcciones[0]
            df.loc[ind+i,'Barrio'] = direcciones[1]
            
            if precios[0] == 'Consultar' or precios[0] == '$' :
                df.loc[ind+i,'Precio'] = 999
            else:
                df.loc[ind+i,'Precio'] = int(precios[0].replace('.',''))

            if 'm²' in caracts:
                df.loc[ind+i,'Metros'] = float(caracts[caracts.index('m²')-1].replace(',','.'))

            if 'baño' in caracts:
                df.loc[ind+i,'Baños'] = int(caracts[caracts.index('baño')-1])

            if 'baños' in caracts:
                df.loc[ind+i,'Baños'] = int(caracts[caracts.index('baños')-1])

            if 'Monoam.' in caracts:
                df.loc[ind+i,'Dorm'] = 0
        
            if 'dorm.' in caracts:
                df.loc[ind+i,'Dorm'] = int(caracts[caracts.index('dorm.')-1])

            if 'años' in caracts:
                df.loc[ind+i,'Años'] = int(caracts[caracts.index('años')-1])

            if 'Estrenar' in caracts:
                df.loc[ind+i,'Años'] = 0
    df=df.fillna(999)
    return df


default_args = {'owner': 'Patricio/Lucas', 'retries': 0, 'start_date': datetime(2021, 11, 11)}

with DAG('scraping', default_args=default_args, schedule_interval='0 7 * * 1') as dag: # Se realaiza el scraping una vez por semana los días lunes a las 7 am

    get_num_pages = PythonOperator(
        task_id = "get_num_pages",
        python_callable = _get_num_pages,
    )

    scrape_page = PythonOperator(
        task_id = "scrape_page",
        python_callable = _scrape_page,
        provide_context=True,
    )

    create_table = PythonOperator(
        task_id="create_viviendas_table",
        python_callable = _create_table,
    )

    write_on_db = PythonOperator(
        task_id = "write_on_db",
        python_callable = _write_on_db,
        provide_context=True,
    )

    get_num_pages>>scrape_page>>create_table>>write_on_db
