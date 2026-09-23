from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator 
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 13),
}

dag = DAG(
    'orders_data_backfilling_dag',
    default_args=default_args,
    description='A DAG to run Spark job with input date parameter on Dataproc ',
    schedule_interval=None,
    catchup=False,
    tags=['dev'],
    params={
        'execution_date': Param(default='NA', type='string', description='Pass execution date in yyyymmdd format'),
    }
)

# Python function to get the execution date
def get_execution_date(ds_nodash, **kwargs):
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date

# PythonOperator to call the get_execution_date function
get_execution_date_task = PythonOperator(
    task_id='get_execution_date',
    python_callable=get_execution_date,
    provide_context=True,
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    dag=dag,
)

# Fetch configuration from Airflow variables
config = Variable.get("cluster_details", deserialize_json=True)
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
            "main_python_file_uri": "gs://airflow-test-projects-gds-dev/airflow-project-2/spark_code/orders_data_process.py",
            "args": ["--date={{ ti.xcom_pull(task_ids='get_execution_date') }}"],
        },
}

submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Set the task dependencies
get_execution_date_task >> submit_pyspark_job