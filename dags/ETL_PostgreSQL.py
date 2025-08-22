# Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 1, 1)
}

# functions for tasks (it can be created outside the DAG, depending on your needs and conventions)
def _get_api():
    # the libreries must be inside the function, because each task is a different process executed by a different worker
    import requests
    url = "https://jsonplaceholder.typicode.com/posts/1"
    headers = {}
    response = requests.request("GET", url, headers=headers)
    # save the response in a csv file
    with open('/tmp/sales_db_py.csv', 'wb') as f:
        f.write(response.content)
        f.close()
    
def _join_trans():
    import pandas as pd
    
    df_py = pd.read_csv('/tmp/sales_db_py.csv')
    df_bash = pd.read_csv('/tmp/sales_db_bash.csv')
    df = pd.concat([df_py, df_bash], ignore_index=True)
    # group by 
    df = df.groupby('id').sum(
        numeric_only=True
    )
    
    # rename columns

    df.to_csv('/tmp/sales_db.csv', sep='\t', index=False, header=False)
    print(df.head())

with DAG(
    'ETL_PostgreSQL',
    default_args=default_args,
    description='ETL PostgreSQL',
    schedule=None,
    tags=['ETL', 'Ingenieria']
) as dag:
    
    get_api_python = PythonOperator(
        task_id='get_api_python',
        python_callable=_get_api
    )
    
    get_api_bash = BashOperator(
        task_id='get_api_bash',
        bash_command='curl "https://jsonplaceholder.typicode.com/posts/1" > /tmp/sales_db_bash.csv'
    )
    
    join_trans = PythonOperator(
        task_id='join_trans',
        python_callable=_join_trans
    )