from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator

from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY', None)
AWS_SECRET = os.environ.get('AWS_SECRET', None)

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12)
}

s3_staging_areas = {
    'events': 's3://udacity-dend/log_data',
    'songs': 's3://udacity-dend/song_data'
}

redshift_conn_id = 'redshift_conn_id'
aws_conn_id = 'aws_credentials'

with DAG(
    'sparkify_pipeline',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval=None
) as dag:

    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    staging_operators = {}
    for staging_area in s3_staging_areas.keys():
        staging_operators[staging_area] = StageToRedshiftOperator(
            task_id=f'Staging_{staging_area}_data',
            dag=dag,
            aws_conn_id=aws_conn_id,
            redshift_conn_id=redshift_conn_id,
            redshift_schema='public',
            redshift_table=f'staging_{staging_area}',
            s3_path=s3_staging_areas[staging_area],
            mode='REPLACE'
        )

        start_operator >> staging_operators[staging_area]
        

