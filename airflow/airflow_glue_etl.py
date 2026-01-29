from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 3
}
# Create the DAG
with DAG('glue_etl_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    # Glue Job Operator
    run_glue_job = AwsGlueJobOperator(
        task_id='glue_job_trigger',
        job_name='glue_etl_name',
        aws_conn_id='aws_default',
        region_name='us-east-1'
    )
    run_glue_job