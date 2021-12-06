from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator

from plugins.helpers import SqlQueriesInsert
from plugins.helpers import SqlQueriesCreate

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12)
}

staging_areas = {
    'staging_events': {
        's3_key': 'log_data', 
        'format': "JSON 's3://udacity-dend/log_json_path.json'"
        }, 
    'staging_songs': {
        's3_key': 'song_data',
        'format': "JSON 'auto' COMPUPDATE OFF"
        }
    }

s3_bucket = Variable.get('s3_bucket', 'udacity-dend')
redshift_conn_id = Variable.get('redshift_conn_id', 'redshift_conn_id')
aws_conn_id = Variable.get('aws_credentials', 'aws_credentials')

redshift_schema = 'public'

with DAG(
    'sparkify_pipeline',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval=None
) as dag:

    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    drop_staging_table = {}
    create_staging_table = {}
    copy_s3_data = {}
    for staging_area in staging_areas:
        drop_staging_table[staging_area] = PostgresOperator(
            task_id=f'Drop_{staging_area}',
            dag=dag,
            postgres_conn_id=redshift_conn_id,
            sql=SqlQueriesCreate.staging[staging_area]
        )

        create_staging_table[staging_area] = PostgresOperator(
            task_id=f'Create_{staging_area}',
            dag=dag,
            postgres_conn_id=redshift_conn_id,
            sql=SqlQueriesCreate.staging[staging_area]
        )

        copy_s3_data[staging_area] = StageToRedshiftOperator(
            task_id=f'S3_Copy_{staging_area}',
            dag=dag,
            aws_credentials_id=aws_conn_id,
            redshift_conn_id=redshift_conn_id,
            redshift_schema=redshift_schema,
            table=f'{staging_area}',
            s3_bucket=s3_bucket,
            s3_key=staging_areas[staging_area]['s3_key'],
            region="us-west-2",
            format=staging_areas[staging_area]['format']
        )
        
        start_operator >> drop_staging_table[staging_area] 
        drop_staging_table[staging_area] >> create_staging_table[staging_area] 
        create_staging_table[staging_area] >> copy_s3_data[staging_area]

