from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        FORMAT AS {} '{}'
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 file_format="JSON",
                 jsonFilePath="",
                 addOns="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.jsonFilePath= jsonFilePath
        self.addOns = addOns
        self.execution_date = kwargs.get('execution_date')
        

    def execute(self, context):
        """
        Stages data from S3 into stage tables in Redshift
        """
        # Define hooks and retrieve credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clears any existing data from provided table
        self.log.info("Clearing data from destination Redshift Table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))
  
        
        # Constructs path to source of the data
        s3_path = "s3://{}".format(self.s3_bucket)
        if self.execution_date:
            # For backfill purposes
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%d")
            s3_path = '/'.join(s3_path, str(year), str(month), str(day))
            self.log.info("Retrieving Data from {}".format(s3_path))
        s3_path = s3_path + '/' + self.s3_key
        
        if self.file_format == 'CSV':
            self.addOns = " DELIMITER ',' IGNOREHEADER 1 " 
        
        self.log.info("Retrieving data for table {} from s3 bucket {}".format(self.table, s3_path))
        
        # Uses given jsonfile for column mapping, otherwise uses `auto`
        if self.jsonFilePath != "":
            copy_query = self.copy_sql.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.region, self.file_format, self.jsonFilePath, self.addOns) 
        else:
            copy_query = self.copy_sql.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.region, self.file_format, 'auto', self.addOns) 
            
        # Run COPY query
        self.log.info("Copying data from S3 to Redshift")
        redshift.run(copy_query)
        
        self.log.info("Success: {} staged successfully from S3 to Redshift".format(self.table))
        





