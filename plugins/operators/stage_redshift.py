from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        aws_conn_id : str = None,
        redshift_conn_id : str = None,
        redshift_schema : str = None,
        redshift_table : str = None,
        s3_path : str = None,
        mode : str = None,
        *args, 
        **kwargs
    ):
        """
        This operator is designed to copy data from S3 into Redshift. It supports a replace mode that truncates the 
        table before inserting data. This feature complements the default append behavior of the COPY SQL statements
        
        :param aws_conn_id:
        :type aws_conn_id:
        :param redshift_conn_id:
        :type redshift_conn_id:
        :param redshift_schema:
        :type redshift_schema:
        :param redshift_table:
        :type redshift_table:
        :param s3_path:
        :type s3_path:
        :param mode:
        :type mode:
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.redshift_schema = redshift_schema
        self.redshift_table = redshift_table
        self.s3_path = s3_path
        self.mode = mode


    def execute(
        self, 
        context
    ):
        
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        access_key = s3_hook.get_credentials().access_key
        secret_key = s3_hook.get_credentials().secret_key
       
        
        if self.mode == 'REPLACE':
            # if method is set to REPLACE the table is truncated before inserting data
            self.log.info(f"REPLACE method selected. Truncating Redshift table {self.redshift_schema}.{self.redshift_table}")
            query_truncate = f"""TRUNCATE {self.redshift_schema}.{self.redshift_table};"""
            #redshift_hook.run(query_truncate, autocommit=self.autocommit)
    
        self.log.info(f'Executing COPY query. Ingesting data into {self.redshift_schema}.{self.redshift_table}')
        query_copy = f"""
                COPY {self.redshift_schema}.{self.redshift_table}
                FROM '{self.s3_path}'
                with credentials
                'aws_access_key_id={access_key};aws_secret_access_key={secret_key}';
            """
        self.log.info(query_copy)
        #redshift_hook.run(query_copy, autocommit=self.autocommit)
        self.log.info("COPY query completed")




