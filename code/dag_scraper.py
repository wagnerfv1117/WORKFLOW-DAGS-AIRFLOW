#1- Importar las librerías necesarias para crear la tubería de datos que extraerá la información y exportarla a un documento delimitado por comas (.csv)
from datetime import datetime, timedelta # librería que integra Apache Airflow para la asignar tiempo para la ejecución de la tubiería de datos
from airflow import DAG # librería que integra con la API de Airflow y orquestar las tuberias de datos
from airflow.operators.python_operator import PythonOperator # orquestador de tareas para ejecutar tareas con codigo python

#2- Crear una función Python para orquestar el scraper a una pagina web, que extraerá los datos de la misma
def scrape_and_save_data():
    import os #  para la gestión de parametros en el sistema como la ejecución adecuada de las rutas para acceder a carpetas y archivos
    import sys # interacción del interprete de Python con el sistema operativo
    import pandas as pd # para la manipulación y limpieza de datos
    import requests # hacer peticiones cliente servidor para poder hacer la extracción de datos
    from bs4 import BeautifulSoup # para la extracción de datos a partir de documentos en HTML

    # Código para scraping y procesamiento de datos
    url = "https://www.larepublica.co/" # pagina de la cual se va aextraer la información
    respuesta = requests.get(url)
    print("La respuesta del servidor es:", respuesta) # en la librería beautifulsoup si da como respuesta 200, es porque la extracción ha sido exitosa

    path = '/home/wagner/Documents/PROYECTOS DATASCIENCE/PROYECTOS PYTHON/WORKFLOW-DAG-AIRFLOW/data/scraped.txt' # ubicación en el nodo donde se va a almacenar los datos raspados

    with open(path + 'extraccion.txt', 'wb') as output:
        output.write(respuesta.content) # guarda archivo en formato .txt de la pagina web extraída

    soup = BeautifulSoup(respuesta.content, 'html.parser') # se aplica el método "parse" de la librería BeautifulSoup, para limpiar las lineas de codigo HTML y extraiga solo los datos necesarios

    datos = [] # variable que se define para llamar a un DataFrame

    for enlace in soup.find_all('a'): #toma solo el esquema (a) donde están almacenados los titulos
        texto = enlace.text.strip()
        url = enlace.get('href') #toma el esquema "href" donde están almacenados los enlaces web
        datos.append({'Titulo': texto, 'Enlace URL': url}) # crea columnas con titulo y enlace URL

    df = pd.DataFrame(datos)# se prodece crear el dataframe con el metodo df de la librería pandas

    df.drop_duplicates() # metodo drop para borrar duplicados en el dataframe
    df = df[df['Titulo'] != '']# metodo de limpieza para borrar valores nulos o vacíos en el dataframe

    df.to_excel('/home/wagner/Documents/PROYECTOS DATASCIENCE/PROYECTOS PYTHON/WORKFLOW-DAG-AIRFLOW/data/docbruto.xlsx') # exportación del conjunto de datos o dataframe a un archivo excel para una mejor visualizacion 
    df.drop(index=[1, 34, 90, 157], axis=1, inplace=True)# se verificó y hay unas columnas que se deben eliminar, ya que no tienen información de link
    df.to_csv('/home/wagner/Documents/PROYECTOS DATASCIENCE/PROYECTOS PYTHON/WORKFLOW-DAG-AIRFLOW/data/links.csv', encoding="utf-8")# luego de hacer el proceso de verficación, se guarda en formato delimitado por comas (.csv)

# Crear las etiquetas del operador python que mostrará la información en el panel de Apache Airflow
default_args = {
    'owner': 'python_scraper', # nombre de la tarea
    'start_date': datetime(2023, 1, 1),
    'retries': 1, # numero de veces que se repite la tarea
    'retry_delay': timedelta(minutes=5), # duración máxima de la tarea
}

# Crear DAG con nombre, etiqueta e intervalo de ejecución
dag = DAG(
    'scraping_dag', # nombre del DAG
    default_args=default_args,
    description='DAG para el scraping de datos del diario la República', # etiqueta clave para ver en el panel de apache airflow
    schedule_interval=timedelta(days=1), # intervalo de ejecución, para este caso se decide cada día; de igual manera se puede ejecutar manual mente
    catchup=False,
)

# Crear el operador python que ejecutará la tarea, al dar clic en el panel de Apache airflow
scrape_task = PythonOperator(
    task_id='scrape_task', # nombre de la tarea 
    python_callable=scrape_and_save_data, # llama la función definida en el punto 2 que es extraer todos los datos de la pagina web del periodico
    dag=dag, 
) # se cierra la tarea orquestando el DAG
