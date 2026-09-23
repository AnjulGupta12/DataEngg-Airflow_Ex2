# Orders Data Backfilling Pipeline

## A) Overview

This project features an Apache Airflow DAG designed to support data backfilling via parameterized data inputs. It leverages Airflow variables and user interface configurations to trigger specific date-based data processing jobs.

## B) Explain Project

* **Business Need:** Process daily order files (e.g., `orders_20250913.csv`) to isolate and store only the completed orders.


* **Structure:** Utilizes Airflow's UI for parameterization, allowing users to manually input execution dates in `yyyymmdd` format.


* **Key Feature:** Seamlessly handles custom backfill dates or defaults to the current system date (`ds_nodash`) if no custom input is provided.



## C) Technology Stack

* **Orchestration:** Apache Airflow (Google Cloud Composer).


* **Processing Engine:** PySpark running on Dataproc.


* **Storage:** Google Cloud Storage (GCS).



## D) Project Architecture/Structure

1. **Input Layer:** Users interact with the Airflow UI to "Trigger DAG with config" and provide an `execution_date`.


2. **Parameter Evaluation:** A Python function determines if a custom date was provided; if not, it defaults to the current date.


3. **Processing Layer:** The evaluated date is passed via XCom to a PySpark job executing on a Dataproc cluster.


4. **Data Layer:** The PySpark job dynamically fetches the corresponding CSV file, filters the records, and writes the output back to GCS.



## E) Workflow

* **Task 1 (`get_execution_date`):** Retrieves the `execution_date` parameter from the UI kwargs. If the value is 'NA', it uses `ds_nodash` (current execution date).


* **Intermediary Step (XCom):** The date is stored as an intermediate result that the next task can pull using XCom.


* **Task 2 (`submit_pyspark_job`):** Pulls the date via XCom and passes it as a command-line argument (`--date`) to the PySpark script.


* **PySpark Execution:** Reads the specific `orders_{date}.csv`, filters for `order_status == "Completed"`, and writes the data to the output folder.



## F) Prerequisites

* **Airflow Environment:** A running Cloud Composer environment with an automatically generated DAGs bucket.


* **Storage Buckets:**
* Create a GCS bucket named `airflow-test-projects-gds-dev`.


* Inside it, create the folder structure: `airflow-project-2/` containing `data/`, `output/`, and `spark-job/`.




* **Airflow Variables:** In the Airflow Admin UI, create a variable named `cluster_details`. Store the values in JSON format containing `CLUSTER_NAME`, `PROJECT_ID`, and `REGION`.


* **File Deployment:** Upload `orders_data_process.py` to the `spark-job/` folder and `airflow_orders_job.py` to the Composer `dags/` folder.



## G) Explain All Files

* `airflow_orders_job.py`: The DAG definition orchestrating the Python date extraction and Dataproc job submission.


* `orders_data_process.py`: The PySpark script that parses command-line arguments, reads dynamic GCS paths, and filters the dataset.


* `Orders_{date}.csv`: Raw data files containing `order_id`, `product`, `order_status` (Completed, Pending, Failed), and `order_date`.



---

### `airflow_orders_job.py`

```python
from datetime import datetime, timedelta
from airflow import DAG
# Variable import allows fetching configuration data (like JSON) from Airflow UI[cite: 2]
from airflow.models import Variable 
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.python import PythonOperator
# Param import allows parameterization to accept execution date from the Airflow UI[cite: 2]
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

# Define the DAG[cite: 2]
dag = DAG(
    'orders_data_backfilling_dag',
    default_args=default_args,
    description="A DAG to run Spark job with input date parameter on Dataproc",
    # schedule_interval=None ensures the DAG is triggered manually[cite: 2]
    schedule_interval=None,
    catchup=False,
    tags=['dev'],
    # params dictionary creates a popup in Airflow UI to enter execution_date in yyyymmdd format[cite: 2]
    params={
        'execution_date': Param(default='NA', type='string', description="Pass execution date in yyyymmdd format")
    }
)

# Python function to evaluate and get the execution date[cite: 2]
# **kwargs collects the keyword arguments, including the Jinja templated params passed from the UI[cite: 2]
def get_execution_date(ds_nodash, **kwargs):
    # Fetch the execution date entered by the user; defaults to 'NA' if nothing is entered[cite: 2]
    execution_date = kwargs['params'].get('execution_date', 'NA')
    
    # If the UI variable is 'NA', use ds_nodash (the current system execution date)[cite: 2]
    if execution_date == 'NA':
        execution_date = ds_nodash
        
    return execution_date

# Task 1: PythonOperator to call the get_execution_date function[cite: 2]
get_execution_date_task = PythonOperator(
    task_id='get_execution_date',
    # References the python function defined above[cite: 2]
    python_callable=get_execution_date,
    # provide_context=True is required so the function can access variables like ds_nodash or ti[cite: 2]
    provide_context=True,
    # Passes the templated ds_nodash value to the function[cite: 2]
    op_kwargs={'ds_nodash': '{{ds_nodash}}'},
    dag=dag,
)

# Fetch configurations from Airflow variables[cite: 2]
# deserialize_json=True parses the JSON stored in the 'cluster_details' variable[cite: 2]
config = Variable.get("cluster_details", deserialize_json=True)

# Extract specific values from the deserialized JSON config[cite: 2]
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']

# Define the PySpark job configuration[cite: 2]
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        # URI pointing to the spark job script in the GCS bucket[cite: 2]
        "main_python_file_uri": "gs://airflow-test-projects-gds-dev/airflow-project-2/spark-code/order_data_process.py",
        # args passes the date parameter[cite: 2]
        # ti.xcom_pull fetches the intermediate date result returned by Task 1 (get_execution_date)[cite: 2]
        "args": ["--date={{ ti.xcom_pull(task_ids='get_execution_date') }}"],
    },
}

# Task 2: Submit the PySpark job to Dataproc[cite: 2]
submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Set the task dependencies[cite: 2]
get_execution_date_task >> submit_pyspark_job

```

### `orders_data_process.py`

```python
from pyspark.sql import SparkSession
import argparse

def data_processing(date):
    # Create Spark session[cite: 2]
    spark = SparkSession.builder.appName("Dataproc Order Processing").getOrCreate()
    
    # Define the dynamic path where the source CSV files are located, replacing {date} with the passed argument[cite: 2]
    # This prevents hardcoding and allows processing unique daily files[cite: 2]
    input_path = f"gs://airflow-test-projects-gds-dev/airflow-project-2/data/orders_{date}.csv"
    
    # Read CSV file with inferred schema[cite: 2]
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Filter the records to keep only those where order_status is 'Completed'[cite: 2]
    df_filtered = df.filter(df.order_status == "Completed")
    
    # Define dynamic output path to save the processed file[cite: 2]
    output_path = f"gs://airflow-test-projects-gds-dev/airflow-project-2/output/processed_orders_{date}"
    
    # Write back to GCS, overwriting any existing data for that day[cite: 2]
    df_filtered.write.csv(output_path, mode="overwrite", header=True)
    
    spark.stop()

if __name__ == "__main__":
    # Create parser object to read command-line inputs passed by Airflow[cite: 2]
    parser = argparse.ArgumentParser(description="Process date argument")
    
    # Defines the required '--date' flag. If the Airflow Dataproc task runs this script, it captures the provided date string[cite: 2]
    parser.add_argument('--date', type=str, required=True, help='Date in yyyymmdd format')
    
    # Parse the actual command executed[cite: 2]
    args = parser.parse_args()
    
    # Call data_processing function passing the captured date argument[cite: 2]
    data_processing(args.date)

```

Are there any specific edge cases with the `ds_nodash` format that you need configured for this backfilling pipeline?
