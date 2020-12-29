from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_queries=[],
                 expected_results=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_queries = test_queries
        self.expected_results = expected_results

    def execute(self, context):
        """
        Performs data quality checks based on the given parameters
        """        
        self.log.info("Running data quality checks")
        redshift = PostgresHook(self.redshift_conn_id)
        
        for query, exp_result in zip(self.test_queries, self.expected_results):
            rec_count = redshift.get_records(query)[0][0]
            print("rec_count: {} == {} exp_result".format(rec_count, exp_result))
            if (rec_count == exp_result):
                self.log.info("Data quality check passed")
            else:
                self.log.info("Data quality check failed")
                raise ValueError('Data quality check failed')
                
            self.log.info("Validation Status: All Tests Passed")
