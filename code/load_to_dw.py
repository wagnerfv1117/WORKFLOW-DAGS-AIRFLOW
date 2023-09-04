#Importar las librerías necesarias para crear el DAG
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Definir los parámetros del DAG
default_args = {
    'owner': 'wagner',
    'start_date': datetime(2023, 9, 3),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'cargar_csv_links_a_postgresql',
    default_args=default_args,
    schedule_interval=None,  # Aqui se puede configurar el desencadenamiento según las necesidades,en este caso se puede hacer manual o automaticamente
    catchup=False,
    tags=['cargar_csv', 'postgresql','Data_Warehouse'], # las etiquetas que se mostrarán en el panel de Airflow
)

# Definir una tarea que cargue el archivo CSV y lo escriba en en la base de datos PostgreSQL llamada wagner
def cargar_csv_links_a_postgresql():
    # Ruta al archivo CSV que se va a cargar
    csv_file_path = '/home/wagner/Documents/PROYECTOS DATASCIENCE/PROYECTOS PYTHON/WORKFLOW-DAG-AIRFLOW/data/links.csv'

    # Verificar si el archivo CSV existe en la ruta del host donde se aloja el archivo
    if os.path.exists(csv_file_path):
        # Cargar el CSV en un DataFrame, utilizando la librería pandas
        df = pd.read_csv(csv_file_path)

        # Conectar al motor PostgreSQL y escribir el DataFrame en la tabla 'links' de la base de datos "wagner"
        from sqlalchemy import create_engine

        #Parámetros de conexión a la base de datos 
        engine = create_engine('postgresql://wagner:1119@localhost/wagner')# parámetros de conexión
        df.to_sql('links', engine, if_exists='replace', index=False)# si existe la conexión, se hace el cargue de los datos en la tabla "links"
    else:
        raise FileNotFoundError(f'El archivo CSV {csv_file_path} no existe.')# en caso de que no exista el archivo 

cargar_csv_task = PythonOperator(
    task_id='cargar_csv_links_a_postgresql',
    python_callable=cargar_csv_links_a_postgresql,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
