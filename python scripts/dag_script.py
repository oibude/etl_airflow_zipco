from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Extraction import run_extraction
from Transformation import run_transformation
from Loading import run_loading

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,6,18),
    'email': 'oriseibude@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retries_daily': timedelta(minutes=1)
}

dag= DAG(
    'zipco_foods_pipeline',
     default_args=default_args,
     description= "The represents Zipco Foods Pipeline"
)

extraction= PythonOperator(
    task_id= 'extraction_layer',
    python_callable=run_extraction,
    dag=dag,
)

transformation= PythonOperator(
    task_id= 'transformtion_layer',
    python_callable=run_transformation,
    dag=dag,
)

loading= PythonOperator(
    task_id= 'extraction_layer',
    python_callable=run_loading,
    dag=dag,
)

extraction >> transformation >> loading