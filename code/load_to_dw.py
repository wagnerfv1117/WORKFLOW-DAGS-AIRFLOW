<<<<<<< HEAD
#Importar las librerías necesarias para crear el DAG
from airflow import DAG # # librería que integra con la API de Airflow y orquestar las tuberias de datos
from airflow.providers.postgres.operators.postgres import PostgresOperator # Librería para que funcione insertar los datos desde la ubicación  
from airflow.operators.python import PythonOperator # orquestador de tareas para ejecutar tareas con codigo python
from datetime import datetime # para configurar y poner en marcha la fecha y hora 
import pandas as pd # para la manipulación y limpieza de datos
import os #  para la gestión de parametros en el sistema como la ejecución adecuada de las rutas para acceder a carpetas y archivos

# Definir los parámetros del DAG en cuanto a fecha de inicio y veces en que se ejecuta
=======
#1-Importar las librerías necesarias para crear el DAG
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# 2-Definir los parámetros del DAG
>>>>>>> 22e1c68 (comments updated)
default_args = {
    'owner': 'wagner', # etiqueta del DAG 
    'start_date': datetime(2023, 9, 3), # fecha de inicio del DAG
    'depends_on_past': False,
    'retries': 1, # numero de veces en las que se ejecuta el DAG en Airflow
}

<<<<<<< HEAD
#Nombre al que se le asigna al DAG
=======
#3-Crear el DAG que permitirá tomar el archivo .csv y cargarlo en la tabla del DW
>>>>>>> 22e1c68 (comments updated)
dag = DAG(
    'cargar_csv_links_a_postgresql', # nombre del DAG
    default_args=default_args,
    schedule_interval=None,  # Aqui se puede configurar el desencadenamiento según las necesidades,en este caso se puede hacer manual o automaticamente
    catchup=False,
    tags=['cargar_csv', 'postgresql','Data_Warehouse'], # las etiquetas que se mostrarán en el panel de Airflow
)

#4-Definir una tarea que cargue el archivo CSV y lo escriba en en la base de datos PostgreSQL llamada wagner
def cargar_csv_links_a_postgresql():
    # Ruta al archivo CSV que se va a cargar
    csv_file_path = '/home/wagner/Documents/PROYECTOS DATASCIENCE/PROYECTOS PYTHON/WORKFLOW-DAG-AIRFLOW/data/links.csv'

    # Verificar si el archivo CSV existe en la ruta del host donde se aloja el archivo
    if os.path.exists(csv_file_path):
        # Cargar el CSV en un DataFrame, utilizando la librería pandas
        df = pd.read_csv(csv_file_path)

        # Conectar al motor PostgreSQL y escribir el DataFrame en la tabla 'links' de la base de datos "wagner" que es el DW
        from sqlalchemy import create_engine

        #Parámetros de conexión a la base de datos 
        engine = create_engine('postgresql://wagner:1119@localhost/wagner')# parámetros de conexión
        df.to_sql('links', engine, if_exists='replace', index=False)# si existe la conexión, se hace el cargue de los datos en la tabla "links" del DW
    else:
        raise FileNotFoundError(f'El archivo CSV {csv_file_path} no existe.')# en caso de que no exista el archivo 

#5-Se desarrolla la tarea, que llamará la función del DAG en Apahe Airflow
cargar_csv_task = PythonOperator(
    task_id='cargar_csv_links_a_postgresql', # nombre de la tarea
    python_callable=cargar_csv_links_a_postgresql, # por parte del operador Python, se llama la función o nombre del DAG
    dag=dag, # instancia  que llama al DAG
)

#6-Se crea la función para la ejecución del DAG desde la linea de comandos
if __name__ == "__main__":
    dag.cli()
