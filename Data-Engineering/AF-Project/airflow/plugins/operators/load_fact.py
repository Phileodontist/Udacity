from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_statement = """INSERT into {} {}"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        """
        Inserts data from the staging tables into the fact table in Redshift
        """
        redshift = PostgresHook(self.redshift_conn_id)
        if self.truncate:
            redshift.run("TRUNCATE {}".format(self.table))
        redshift.run(self.insert_statement.format(self.table, self.sql_query))
        self.log.info("Loaded {} into redshift".format(self.table))
