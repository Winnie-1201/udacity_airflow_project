from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        for table in self.tables:
            query = f"SELECT COUNT(*) FROM {table}"
            records = redshift_hook.get_records(query)

            if not records or not records[0] or records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results or contained 0 rows")
            
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")