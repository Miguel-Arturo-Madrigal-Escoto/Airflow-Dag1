
# * Miguel Arturo Madrigal Escoto

from datetime import timedelta
import hashlib
import mysql.connector
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

   
def set_data(**context):
    print('Ejecución de la primera funcion')
    username = 'Airflow Miguel'
    email = 'mig@airflow.com'
    password = 'Minecraft890'
    password = hashlib.new('sha256', bytes(password, 'utf-8')).hexdigest() 

    context['ti'].xcom_push(key='data', value={ 'username': username, 'email': email, 'password': password })
    

def insert_data(**context):
    print('Ejecución de la segunda funcion')
    user = context['ti'].xcom_pull(key='data')
    query = 'INSERT INTO usuario(username, email, password) VALUES(%s, %s, %s)'
    params = (user['username'], user['email'], user['password'])
    conn = mysql.connector.connect(
            db='ejemplo_mysql', 
            user='root', 
            password='', 
            host='127.0.0.1'
        )
    cursor = conn.cursor()
    cursor.execute(query, params)
    if cursor.rowcount:
        print('Insertado en la db')

default_args = {
    'owner': 'miguelmad123',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 13)
}

# 3. Instanciar la DAG
with DAG(
    dag_id='dag1.py',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
    ) as dag:
    
    primera_funcion = PythonOperator(
        task_id='set_data',
        python_callable=set_data,
        provide_context=True
    )

    segunda_funcion = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
        provide_context=True
    )

primera_funcion >> segunda_funcion