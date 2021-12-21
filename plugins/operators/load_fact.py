from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id : str = None,
        redshift_schema : str = None,
        redshift_table : str = None,
        query : str = None,
        *args,
        **kwargs
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_schema = redshift_schema
        self.redshift_table = redshift_table
        self.query = query

    def execute(self, context):

        sql_insert = f"""
            INSERT INTO {self.redshift_schema}.{self.redshift_table} 
            {self.query}
        """

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Loading Songplays fact table data into {self.redshift_schema}.{self.redshift_table}')
        self.log.info('Executing query')
        self.log.info(f'{self.query}')
        
        redshift_hook.run(sql_insert)