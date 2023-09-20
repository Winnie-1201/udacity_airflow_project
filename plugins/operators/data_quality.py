from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    dq_checks = [
        {'tes_sql': "SELECT COUNT {*} FROM songplays", 'expected_result': 0, 'comparison': '>'}, 
        {'tes_sql': "SELECT COUNT {*} FROM users", 'expected_result': 0, 'comparison': '>'}, 
        {'tes_sql': "SELECT COUNT {*} FROM songs", 'expected_result': 0, 'comparison': '>'}, 
        {'tes_sql': "SELECT COUNT {*} FROM artists", 'expected_result': 0, 'comparison': '>'}, 
        {'tes_sql': "SELECT COUNT {*} FROM time", 'expected_result': 0, 'comparison': '>'}
    ]

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

        for i, dq_check in enumerate(self.dq_checks):
            test_sql = dq_check['test_sql']
            expected_result = dq_check['expected_result']
            comparison = dq_check.get('comparison', '==')
            records = redshift_hook.get_records(test_sql)
            
            if not self.compare_records(len(records), expected_result, comparison) or not self.compare_records(len(records[0]), expected_result, comparison) or not self.compare_records(len(records[0][0]), expected_result, comparison):
                raise ValueError(f"Data quality check #{i} failed. {self.get_error_message(test_sql, expected_result, comparison)}")
            
            self.log.info(f"Data quality check #{i} passed for: {test_sql}")

    def compare_records(self, actual, expected, comparison):
        if comparison == '==':
            return actual == expected
        elif comparison == '!=':
            return actual != expected
        elif comparison == '>':
            return actual > expected
        elif comparison == '>=':
            return actual >= expected
        elif comparison == '<':
            return actual < expected
        elif comparison == '<=':
            return actual <= expected
        else:
            raise ValueError(f"Invalid comparison operator: {comparison}")

    def get_error_message(self, test_sql, expected_result, comparison):
        if comparison == '==':
            return f"{test_sql} should be equal to {expected_result}"
        elif comparison == '!=':
            return f"{test_sql} should not be equal to {expected_result}"
        elif comparison == '>':
            return f"{test_sql} should be greater than {expected_result}"
        elif comparison == '>=':
            return f"{test_sql} should be greater than or equal to {expected_result}"
        elif comparison == '<':
            return f"{test_sql} should be less than {expected_result}"
        elif comparison == '<=':
            return f"{test_sql} should be less than or equal to {expected_result}"
        else:
            return f"Invalid comparison operator: {comparison}"