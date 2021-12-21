from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        aws_credentials_id: str = None,
        redshift_conn_id: str = None,
        redshift_schema: str = None,
        redshift_table: str = None,
        s3_bucket: str = None,
        s3_key: str = None,
        region: str = None,
        format: str = None,
        *args, 
        **kwargs
    ) -> None:

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.redshift_schema = redshift_schema
        self.redshift_table = redshift_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.format = format


    def execute(self, context):

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        rendered_key = self.s3_key.format(context['execution_date'])
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        self.log.info(f'Executing COPY query. Ingesting data into {self.redshift_schema}.{self.redshift_table}')
        copy_query = f"""
            COPY {self.redshift_schema}.{self.redshift_table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            FORMAT AS {self.format}
        """ 
        self.log.info(copy_query)
        redshift_hook.run(copy_query)
        self.log.info("COPY query completed")