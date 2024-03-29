from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    ui_color = '#358140'
    sql_statement_file='/home/workspace/airflow/plugins/operators/create_tables.sql'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        """
        Executes CREATE statements to generate fact/dimension tables
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Creating Staging and Dimension Tables")
        
        with open(CreateTablesOperator.sql_statement_file, 'r') as sql_con:
            sql_file = sql_con.read()
            
            sql_statements = sql_file.split(";")
            
            for statement in sql_statements:
                if statement.rstrip() != '':
                    redshift.run(statement)