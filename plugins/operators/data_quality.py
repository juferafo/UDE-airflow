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


    def execute(self, context):

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_conn = redshift_hook.get_conn()
        data = redshift_conn.cursor()

        self.log.info("Running quality checks")

        if len(data) == 0:
            pass