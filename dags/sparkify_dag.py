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

from plugins.helpers import SqlQueriesDrop
from plugins.helpers import SqlQueriesCreate
from plugins.helpers import SqlQueriesInsert

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12)
}

dwh_staging_tables = {
    'staging_events': {
        's3_key': 'log_data', 
        'format': "JSON 's3://udacity-dend/log_json_path.json'"
        }, 
    'staging_songs': {
        's3_key': 'song_data',
        'format': "JSON 'auto' COMPUPDATE OFF"
        }
    }

dwh_star_tables = {
    'fact': SqlQueriesCreate.fact.keys(),
    'dimension': SqlQueriesCreate.dimension.keys()
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

    start = DummyOperator(task_id='Begin_execution', dag=dag)

    end = DummyOperator(task_id='End_execution', dag=dag)

    create_fact_table = PostgresOperator(
        task_id=f'create_fact_table_songplays',
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SqlQueriesCreate.fact["songplays"]
    )

    load_fact_table = LoadFactOperator(
        task_id='Load_fact_table_songplays',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        redshift_schema=redshift_schema,
        redshift_table=dwh_star_tables['fact'],
        query=SqlQueriesInsert.fact['songplays']
    )

    run_quality_checks = DummyOperator(task_id='Run_quality_checks', dag=dag)

    drop_staging_table = {}
    create_staging_table = {}
    copy_s3_data = {}

    for staging_area in dwh_staging_tables:
        drop_staging_table[staging_area] = PostgresOperator(
            task_id=f'Drop_staging_table_{staging_area}',
            dag=dag,
            postgres_conn_id=redshift_conn_id,
            sql=SqlQueriesCreate.staging[staging_area]
        )

        create_staging_table[staging_area] = PostgresOperator(
            task_id=f'Create_staging_table_{staging_area}',
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
            s3_key=dwh_staging_tables[staging_area]['s3_key'],
            region="us-west-2",
            format=dwh_staging_tables[staging_area]['format']
        )
  
        start >> drop_staging_table[staging_area] 
        drop_staging_table[staging_area] >> create_staging_table[staging_area] 
        create_staging_table[staging_area] >> copy_s3_data[staging_area]

        copy_s3_data[staging_area] >> load_fact_table

    create_dimension_table = {}
    load_dimension_table = {}
    for dimension_table in dwh_star_tables['dimension']:
        create_dimension_table[dimension_table] = PostgresOperator(
            task_id=f'Create_dimension_table{staging_area}',
            dag=dag,
            postgres_conn_id=redshift_conn_id,
            sql=SqlQueriesCreate.dimensions[dimension_table]
            )

        load_dimension_table[dimension_table] = LoadDimensionOperator(
            task_id=f'Load_dimension_table_{dimension_table}',
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            redshift_schema=redshift_schema,
            redshift_table=dwh_star_tables['fact'],
            query=SqlQueriesInsert.dimensions[dimension_table]
        )

        load_fact_table >> create_dimension_table[dimension_table] 
        create_dimension_table[dimension_table] >> load_dimension_table[dimension_table] 
        load_dimension_table[dimension_table]  >> run_quality_checks

    run_quality_checks >> end



    


