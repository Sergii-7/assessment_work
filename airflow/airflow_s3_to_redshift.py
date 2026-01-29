# Example of how to use S3ToRedshiftOperator

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Replace these with your own values. You will need to create a Connection
# in the Apache Airflow Admin section using the Amazon Redshift connection type
# in the extras field update the values for your setup
#
# { 
#  "iam": true,
#  "cluster_identifier": "{your_redshift_cluster}",
#  "port": 5439, 
#  "region": "{aws_region}",
#  "db_user": "{redshift_username}",
#  "database": "{redshfit_db}"
# }

# Change the S3_FILE to a file you have uploaded. This works for the sample db files

REDSHIFT_CONNECTION_ID = 'default_redshift_connection'
S3_BUCKET = 'mwaa-094459-redshift'
S3_FILE = 'sampletickit/allevents_pipe.txt'
REDSHIFT_SCHEMA = 'mwaa'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_to_redshift_dag',
    default_args=default_args,
    description='Load CSV from S3 to Redshift',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

check_s3_for_files = S3KeySensor(
        task_id='check_s3_for_files',
        bucket_name=S3_BUCKET,
        bucket_key=S3_FILE,
        dag=dag
    )

event_load_s3_to_redshift = S3ToRedshiftOperator(
    task_id='event_load_s3_to_redshift',
    s3_bucket=S3_BUCKET,
    s3_key= 'sampletickit/allevents_pipe.txt',
    schema=REDSHIFT_SCHEMA,
    table='public.event',
    copy_options=["CSV","DELIMITER '|'", "FILLRECORD", "IGNOREHEADER 1"],
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag,
)
cat_load_s3_to_redshift = S3ToRedshiftOperator(
    task_id='cat_load_s3_to_redshift',
    s3_bucket=S3_BUCKET,
    s3_key= 'sampletickit/category_pipe.txt',
    schema=REDSHIFT_SCHEMA,
    table='public.category',
    copy_options=["CSV","DELIMITER '|'", "FILLRECORD", "IGNOREHEADER 1"],
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag,
)

date_load_s3_to_redshift = S3ToRedshiftOperator(
    task_id='date_load_s3_to_redshift',
    s3_bucket=S3_BUCKET,
    s3_key= 'sampletickit/date2008_pipe.txt',
    schema=REDSHIFT_SCHEMA,
    table='public.date',
    copy_options=["CSV","DELIMITER '|'", "FILLRECORD", "IGNOREHEADER 1"],
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag,
)

listing_load_s3_to_redshift = S3ToRedshiftOperator(
    task_id='listing_load_s3_to_redshift',
    s3_bucket=S3_BUCKET,
    s3_key= 'sampletickit/listings_pipe.txt',
    schema=REDSHIFT_SCHEMA,
    table='public.listing',
    copy_options=["CSV","DELIMITER '|'", "MAXERROR 5", "IGNOREHEADER 1"],
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag,
)

sales_load_s3_to_redshift = S3ToRedshiftOperator(
    task_id='sales_load_s3_to_redshift',
    s3_bucket=S3_BUCKET,
    s3_key= 'sampletickit/sales_tab.txt',
    schema=REDSHIFT_SCHEMA,
    table='public.sales',
    copy_options=["CSV","DELIMITER '\t'", "TIMEFORMAT 'MM/DD/YYYY HH:MI:SS'" , "MAXERROR 100", "IGNOREHEADER 1"],
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag,
)

user_load_s3_to_redshift = S3ToRedshiftOperator(
    task_id='user_load_s3_to_redshift',
    s3_bucket=S3_BUCKET,
    s3_key= 'sampletickit/allusers_pipe.txt',
    schema=REDSHIFT_SCHEMA,
    table='public.users',
    copy_options=["CSV","DELIMITER '|'", "FILLRECORD", "IGNOREHEADER 1"],
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag,
)

venue_load_s3_to_redshift = S3ToRedshiftOperator(
    task_id='venue_load_s3_to_redshift',
    s3_bucket=S3_BUCKET,
    s3_key= 'sampletickit/venue_pipe.txt',
    schema=REDSHIFT_SCHEMA,
    table='public.venue',
    copy_options=["CSV","DELIMITER '|'", "FILLRECORD", "IGNOREHEADER 1"],
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag,
)

check_s3_for_files >> event_load_s3_to_redshift 
check_s3_for_files >> cat_load_s3_to_redshift 
check_s3_for_files >> date_load_s3_to_redshift
check_s3_for_files >> listing_load_s3_to_redshift
check_s3_for_files >> sales_load_s3_to_redshift
check_s3_for_files >> user_load_s3_to_redshift
check_s3_for_files >> venue_load_s3_to_redshift