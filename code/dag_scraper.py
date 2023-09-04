#1- Importar las librerías necesarias para crear la tubería de datos que extraerá la información y exportarla a un documento delimitado por comas (.csv)
from datetime import datetime, timedelta # librería que integra Apache Airflow para la asignar tiempo para la ejecución de la tubiería de datos
from airflow import DAG # librería que integra con la API de Airflow y orquestar las tuberias de datos
from airflow.operators.python_operator import PythonOperator # orquestador de tareas para ejecutar tareas con codigo python

#2- Crear 
def scrape_and_save_data():
    import os #  para la gestión de parametros en el sistema como la ejecución adecuada de las rutas
    import sys
    import pandas as pd # para la manipulación y limpieza de datos
    import requests # hacer peticiones cliente servidor para poder hacer la extracción de datos
    from bs4 import BeautifulSoup # para la extracción de datos a partir de documentos en HTML

    # Código para scraping y procesamiento de datos
    url = "https://www.larepublica.co/" # pagina de la cual se va aextraer la información
    respuesta = requests.get(url)
    print("La respuesta del servidor es:", respuesta) # en la librería beautifulsoup si da 200, es porque la extracción es exitosa

    path = '/home/wagner/Documents/PROYECTOS DATASCIENCE/PROYECTOS PYTHON/WORKFLOW-DAG-AIRFLOW/data/scraped.txt' # ubicación en el nodo donde se va a almacenar los datos raspados

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
