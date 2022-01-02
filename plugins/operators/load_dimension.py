from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id : str = None,
        redshift_schema : str = None,
        redshift_table : str = None,
        query : str = None,
        mode : str = 'append',
        *args,
        **kwargs
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_schema = redshift_schema
        self.redshift_table = redshift_table
        self.query = query
        self.mode = mode
            

    def execute(self, context):

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        sql_insert = f"""
            INSERT INTO {self.redshift_schema}.{self.redshift_table} 
            {self.query}
        """

        if self.mode == 'truncate':
            sql_truncate = f"""
                TRUNCATE TABLE {self.redshift_schema}.{self.redshift_table}
            """

            self.log.info(f"Truncate mode selected")
            self.log.info(f'Truncating {self.redshift_schema}.{self.redshift_table} dimension table before inserting data')
            self.log.info(f'{sql_truncate}')
            redshift_hook.run(sql_truncate)

        self.log.info(f'Loading {self.redshift_table} dimension table data into {self.redshift_schema}.{self.redshift_table}')
        self.log.info('Executing query')
        self.log.info(f'{self.query}')
        
        redshift_hook.run(sql_insert)
