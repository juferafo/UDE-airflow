from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str = None,
        dq_checks: list = None,
        *args, 
        **kwargs
    ) -> None:

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info("Running quality checks")
        
        if len(self.dq_checks) == 0:
            self.log.info("No data quality checks provided")
            return

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        tests_failed = []
        tests_passed = []

        for test in self.dq_checks:
            sql_query = test.get('check_sql')
            exp_result = test.get('expected_result')

            try:
                self.log.info(f"Executing query")
                self.log.info(f"{sql_query}")
                data = redshift_hook.get_records(sql_query)[0]

            except Exception as exception:
                self.log.info(f"Query failed: {exception}")

            if exp_result != data[0]:
                tests_failed.append(sql_query)

            else:
                tests_passed.append(sql_query)
            
        self.log.info(f"Tests failed: {len(tests_failed)}")
        self.log.info(f"Tests passed: {len(tests_passed)}")

        if len(tests_failed) == 0:
            self.log.info("All data quality tests passed")
