from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY', None)
AWS_SECRET = os.environ.get('AWS_SECRET', None)

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'schedule_interval': None 
}

s3_log_data = 's3://udacity-dend/log_data'
s3_song_data = 's3://udacity-dend/song_data'

with DAG(
    'sparkify_pipeline',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
) as dag:

    start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag
    )

    data_staging = [stage_events_to_redshift, stage_songs_to_redshift]

    start_dwh_data_load = DummyOperator(task_id='Start_DWH_data_load', dag=dag)

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag
    )

    dwh_data_load = [
        load_songplays_table, 
        load_user_dimension_table, 
        load_song_dimension_table, 
        load_artist_dimension_table, 
        load_time_dimension_table
        ]

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag
    )

    end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

    # pipeline flow
    start_operator >> data_staging
    data_staging >> start_dwh_data_load
    start_dwh_data_load >> dwh_data_load
    dwh_data_load >> run_quality_checks
    end_operator

