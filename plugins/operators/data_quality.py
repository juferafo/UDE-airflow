from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        aws_credentials_id: str = None,
        redshift_conn_id: str = None,
        redshift_schema: str = None,
        redshift_table: str = None,
        *args, 
        **kwargs
    ) -> None:

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.redshift_schema = redshift_schema
        self.redshift_table = redshift_table
        self.query = f"""SELECT COUNT(*) FROM {self.redshift_schema}.{self.redshift_table}"""


    def execute(self, context):
        self.log.info("Running quality checks")
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        data = redshift_hook.get_records(self.query)
        
        if len(data) < 1 or len(data[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.redshift_schema}{self.redshift_table} returned no results")
            
        num_records = data[0][0]
        
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.redshift_schema}{self.redshift_table} contained 0 rows")
            
        logging.info(f"Data quality on table {self.redshift_schema}{self.redshift_table} check passed with {num_records[0][0]} records")
        
