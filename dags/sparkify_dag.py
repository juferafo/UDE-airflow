from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueriesCreate
from helpers import SqlQueriesInsert

from operators import StageToRedshiftOperator
from operators import LoadFactOperator
from operators import LoadDimensionOperator
from operators import DataQualityOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
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
    'fact': list(SqlQueriesCreate.fact.keys())[0],
    'dimensions': list(SqlQueriesCreate.dimensions.keys())
}

s3_bucket = Variable.get('s3_bucket', 'udacity-dend')
redshift_conn_id = Variable.get('redshift_conn_id', 'redshift_conn_id')
aws_conn_id = 'aws_credentials'

redshift_schema = 'public'

with DAG(
    'sparkify_pipeline',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
) as dag:

    start = DummyOperator(task_id='Start', dag=dag)

    end = DummyOperator(task_id='End', dag=dag)
    
    load_fact_table = LoadFactOperator(
        task_id='Load_fact_table_songplays',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        redshift_schema=redshift_schema,
        redshift_table=dwh_star_tables['fact'],
        query=SqlQueriesInsert.fact['songplays']
    )

    run_quality_check = DataQualityOperator(
        task_id=f'Data_quality_check',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        dq_checks=[
            { 
                'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 
                'expected_result': 0 
                }, 
            { 
                'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 
                'expected_result': 2 
                },
            { 
                'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 
                'expected_result': 0 
                },
            { 
                'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 
                'expected_result': 0 
                },
            { 
                'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 
                'expected_result': 0 
                },
            { 
                'check_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 
                'expected_result': 0 
                },
            { 
                'check_sql': 'SELECT COUNT(*) FROM public.songplays AS songplays LEFT OUTER JOIN public.users AS users ON songplays.userid = users.userid WHERE users.userid IS NULL',
                'expected_result': 0 
                }
            ]
    )
    
    copy_s3_data = {}

    for staging_area in dwh_staging_tables:

        copy_s3_data[staging_area] = StageToRedshiftOperator(
            task_id=f'S3_Copy_{staging_area}',
            dag=dag,
            aws_credentials_id=aws_conn_id,
            redshift_conn_id=redshift_conn_id,
            redshift_schema=redshift_schema,
            redshift_table=f'{staging_area}',
            s3_bucket=s3_bucket,
            s3_key=dwh_staging_tables[staging_area]['s3_key'],
            region="us-west-2",
            format=dwh_staging_tables[staging_area]['format']
        )

        start >> copy_s3_data[staging_area]
        copy_s3_data[staging_area] >> load_fact_table

    load_dimension_table = {}
    
    for dimension_table in dwh_star_tables['dimensions']:

        load_dimension_table[dimension_table] = LoadDimensionOperator(
            task_id=f'Load_dimension_table_{dimension_table}',
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            redshift_schema=redshift_schema,
            redshift_table=dimension_table,
            query=SqlQueriesInsert.dimensions[dimension_table],
            mode='truncate'
        )

        load_fact_table >> load_dimension_table[dimension_table] 
        load_dimension_table[dimension_table] >> run_quality_check
    
    run_quality_check >> end