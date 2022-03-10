from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator


from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from load_fact import LoadFactOperator
from load_dimension import LoadDimensionOperator
from data_quality import DataQualityOperator

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'ashwath',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'email': ['ashwath92@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# hourly: cron is '0 * * * *': https://airflow.apache.org/docs/stable/scheduler.html
dag = DAG('sparkify_elt_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          # https://airflow.apache.org/docs/stable/scheduler.html
          schedule_interval='0 0 * * *'
          #schedule_interval=timedelta(days=1),
          #schedule_interval='0 * * * *'
        )
######################### Stage ###############################
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

######################### LOAD ###########################
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql=sql

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook.run(self.sql)




#########################################################
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    #sql in same directory
    sql='create_tables.sql',
    postgres_conn_id='redshift'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    destination_table='staging_events',
    input_file_type='json',
    start_date=datetime(2018, 11, 1),
    provide_context=True
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    destination_table='staging_songs',
    input_file_type='json',
    start_date=datetime(2018, 11, 1),
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql=SqlQueries.songplay_table_insert,
    provide_context=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    destination_table='users',
    mode='append',
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    destination_table='songs',
    mode='truncate',
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    destination_table='artists',
    mode='truncate',
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    destination_table='time',
    mode='append',
    sql=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task 
create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_artist_dimension_table, load_song_dimension_table, load_user_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
