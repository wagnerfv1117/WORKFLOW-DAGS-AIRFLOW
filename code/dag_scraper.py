from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def scrape_and_save_data():
    import os
    import sys
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup

    # Código para scraping y procesamiento de datos
    url = "https://www.larepublica.co/"
    respuesta = requests.get(url)
    print("La respuesta del servidor es:", respuesta)

    path = '/home/wagner/Documents/PROYECTOS DATASCIENCE/PROYECTOS PYTHON/WORKFLOW-DAG-AIRFLOW/data/scraped.txt'

    with open(path + 'extraccion.txt', 'wb') as output:
        output.write(respuesta.content)

    soup = BeautifulSoup(respuesta.content, 'html.parser')

    datos = []

    for enlace in soup.find_all('a'):
        texto = enlace.text.strip()
        url = enlace.get('href')
        datos.append({'Titulo': texto, 'Enlace URL': url})

    df = pd.DataFrame(datos)

    df.drop_duplicates()
    df = df[df['Titulo'] != '']

    df.to_excel('/home/wagner/Documents/PROYECTOS DATASCIENCE/PROYECTOS PYTHON/WORKFLOW-DAG-AIRFLOW/data/docbruto.xlsx')
    df.drop(index=[1, 34, 90, 157], axis=1, inplace=True)# se verificó y hay unas columnas que se deben eliminar, ya que no tienen información de link
    df.to_csv('/home/wagner/Documents/PROYECTOS DATASCIENCE/PROYECTOS PYTHON/WORKFLOW-DAG-AIRFLOW/data/links.csv', encoding="utf-8")

default_args = {
    'owner': 'python_scraper',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scraping_dag',
    default_args=default_args,
    description='DAG para el scraping de datos del diario la República',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

scrape_task = PythonOperator(
    task_id='scrape_task',
    python_callable=scrape_and_save_data,
    dag=dag,
)
