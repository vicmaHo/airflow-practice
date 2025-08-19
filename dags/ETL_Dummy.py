from airflow import DAG
from airflow.operators.empty import EmptyOperator


# arguments for DAG 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['iHs2B@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': '2020-01-01'
}

# example DAG with dummy operators (EmptyOperator in new version of Airflow)
with DAG(
    'ETL_Dummy',
    default_args=default_args,
    description='ETL Dummy',
    tags=['ETL', 'Ingenieria'],
    schedule=None
) as dag:
    
    # tasks
    get_api_bash = EmptyOperator(
        task_id='get_api_bash'
    )
    
    get_api_python = EmptyOperator(task_id='get_api_python')
    
    join_transformation = EmptyOperator(task_id='join_transformation')
    
    load_postgreSQL = EmptyOperator(task_id='load_postgreSQL')
    
    # dependencies
    [get_api_bash, get_api_python] >> join_transformation >> load_postgreSQL