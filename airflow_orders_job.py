from datetime import datetime, timedelta
from airflow import DAG
# Variable import allows fetching configuration data (like JSON) from Airflow UI
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator 
from airflow.operators.python import PythonOperator
# Param import allows parameterization to accept execution date from the Airflow UI
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

# Define the DAG
dag = DAG(
    'orders_data_backfilling_dag',
    default_args=default_args,
    description='A DAG to run Spark job with input date parameter on Dataproc ',
    # schedule_interval=None ensures the DAG is triggered manually
    schedule_interval=None,
    catchup=False,
    tags=['dev'],
    # params dictionary creates a popup in Airflow UI to enter execution_date in yyyymmdd format
    params={
        'execution_date': Param(default='NA', type='string', description='Pass execution date in yyyymmdd format'),
    }
)

# Python function to get the execution date
#  **kwargs collects the keyword arguments, including the Jinja templated params passed from the UI
def get_execution_date(ds_nodash, **kwargs):
     # Fetch the execution date entered by the user; defaults to 'NA' if nothing is entered
    execution_date = kwargs['params'].get('execution_date', 'NA')
    # If the UI variable is 'NA', use ds_nodash (the current system execution date)
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date

# Task 1: PythonOperator to call the get_execution_date function
get_execution_date_task = PythonOperator(
    task_id='get_execution_date',
    # References the python function defined above
    python_callable=get_execution_date,
    # provide_context=True is required so the function can access variables like ds_nodash or ti
    provide_context=True,
    # Passes the templated ds_nodash value to the function
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    dag=dag,
)

# Fetch configuration from Airflow variables
# deserialize_json=True parses the JSON stored in the 'cluster_details' variable
config = Variable.get("cluster_details", deserialize_json=True)
# Extract specific values from the deserialized JSON config
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']

# Define the PySpark job configuration
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
             # URI pointing to the spark job script in the GCS bucket
            "main_python_file_uri": "gs://airflow-test-projects-gds-dev/airflow-project-2/spark_code/orders_data_process.py",
            # args passes the date parameter
            # ti.xcom_pull fetches the intermediate date result returned by Task 1 (get_execution_date)
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
