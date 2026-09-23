from pyspark.sql import SparkSession
import argparse

def data_processing(date):
    # Create a Spark session
    spark = SparkSession.builder.appName("DataprocOrderProcessing").getOrCreate()

    # Define the path where the files are located
    # replacing {date} with the passed argument, this prevents hardcoding and allows processing unique daily files
    input_path = f"gs://airflow-test-projects-gds-dev/airflow-project-2/data/orders_{date}.csv"
    
    # Read CSV files
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Filter the records which got completed
    df_filtered = df.filter(df.order_status == "Completed")

    # Write back to GCS with the date naming convention
    output_path = f"gs://airflow-test-projects-gds-dev/airflow-project-2/output/processed_orders_{date}"
    df_filtered.write.csv(output_path, mode="overwrite", header=True)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process date argument')      # Create parser object to read command-line inputs passed by Airflow
    parser.add_argument('--date', type=str, required=True, help='Date in yyyymmdd format') # Defines the required '--date' flag. If the Airflow Dataproc task runs this script, it captures the provided date string
    args = parser.parse_args()      # Parse the actual command executed


     # Call data_processing function passing the captured date argument
    data_processing(args.date)
