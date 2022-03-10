from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
    """
    copy_sql_csv = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        DELIMITER '{}'
        IGNOREHEADER {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 destination_table="",
                 input_file_type="",
                 delimiter=',',
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.destination_table = destination_table
        self.input_file_type = input_file_type
        
    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table (if it exists)")
        redshift_hook.run(f"DELETE FROM {self.destination_table}")
        self.log.info("Copying data from S3 to Redshift")
        # http://s3-us-west-2.amazonaws.com/udacity-dend/log-data/2018/11/2018-11-04-events.json
        #s3_path = f"s3://{self.s3_bucket}/{self.s3_key}/{ execution_date.strftime('%Y')}/{{ execution_date.strftime('%m') }}/{{ ds }}-events.json"
        if self.s3_key == 'song_data':
            s3_path = "s3://{bucket}/{key}".format(bucket=self.s3_bucket, key=self.s3_key)
        else:
            s3_key = '{key}/{{execution_date.year}}/{{execution_date.month}}/{{ds}}-events.json'.format(key=self.s3_key)
            s3_path = "s3://{bucket}/{key}".format(bucket=self.s3_bucket, key=self.s3_key)
        if self.input_file_type == 'csv':
            copy_sql = StageToRedshiftOperator.copy_sql_csv.format(self.destination_table,
                                                                   s3_path,
                                                                   credentials.access_key,
                                                                   credentials.secret_key,
                                                                   self.region,
                                                                   self.delimiter,
                                                                   self.ignore_headers)
        else: # json (default)
            if self.s3_key == 'log_data':
                jsonpath = 's3://{}/log_json_path.json'.format(self.s3_bucket)
            else:
                jsonpath = 'auto'
            copy_sql = StageToRedshiftOperator.copy_sql_json.format(self.destination_table,
                                                                    s3_path,
                                                                    credentials.access_key,
                                                                    credentials.secret_key,
                                                                    self.region,
                                                                    jsonpath
                                                                   )
        redshift_hook.run(copy_sql)