from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from Extraction import extraction # Here we import function from ETL script
from Transformation import transformation
from Loading import loading

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,6,20),
    'email': 'oriseibude@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

dag= DAG(
    'zipco_pipeline',
     default_args=default_args,
     description= "This represents Zipco Foods Pipeline",
     schedule_interval=timedelta(hours=5),  # Schedule interval set to 5 minutes
)

extraction= PythonOperator(
    task_id= 'extraction_layer',
    python_callable=extraction,
    dag=dag,
)

transformation= PythonOperator(
    task_id= 'transformtion_layer',
    python_callable=transformation,
    dag=dag,
)

loading= PythonOperator(
    task_id= 'loading_layer',
    python_callable=loading,
    dag=dag,
)

extraction >> transformation >> loading
