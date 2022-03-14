import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import boto3
from io import StringIO



def S3_toRedShift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")

    day = {kwargs['ds']}.pop()
    day = datetime.strptime(day, '%Y-%m-%d').date() - timedelta(days=2)
    day = day.strftime("%m-%d-%Y")

    sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER 1
        DELIMITER ';'
        TIMEFORMAT 'auto'
        """
    redshift_hook.run(sql.format("log_review",
                                 "s3://data-raw-bucket/log_reviews.csv/",
                                 credentials.access_key,
                                 credentials.secret_key))
    return

def insertUSdata(*args, **kwargs):
    # aws_hook = AwsHook("aws_credentials")
    # credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")

    day = {kwargs['ds']}.pop()
    day = datetime.strptime(day, '%Y-%m-%d').date() - timedelta(days=2)
    day = day.strftime("%Y-%m-%d")

    sql = """
        INSERT INTO device
        SELECT 
            device
        FROM log_reviw """
    redshift_hook.run(sql.format("device"))
    return
    


default_args = {
    'owner': 'ashwath',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
  
}

# hourly: cron is '0 * * * *': https://airflow.apache.org/docs/stable/scheduler.html
dag = DAG('log_reviews',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          # https://airflow.apache.org/docs/stable/scheduler.html
          schedule_interval='0 0 * * *'
          #schedule_interval=timedelta(days=1),
          #schedule_interval='0 * * * *'


create_table_main = PostgresOperator(
    task_id="create_log",
    dag=dag,
    postgres_conn_id="redshift",
    sql=""" CREATE TABLE IF NOT EXISTS log_review (
            id_log INTEGER,
            device VARCHAR,
            location VARCHAR,
            os DATE,
            ip VARCHAR,
            phone_number VARCHAR,
            browser VARCHAR);""")
            
create_table_device = PostgresOperator(
    task_id="create_device",
    dag=dag,
    postgres_conn_id="redshift",
    sql=""" CREATE TABLE IF NOT EXISTS device (
            id_dim_devices INTEGER IDENTITY(1,1),
            device VARCHAR,
            );""")



MovetoRedShift = PythonOperator(
    task_id="S3_toRedShift",
    provide_context=True,
    python_callable=S3_toRedShift,
    dag=dag
)

insert_Devices = PythonOperator(
    task_id="insert_Devices",
    provide_context=True,
    python_callable=insertUSdata,
    dag=dag
)

create_table_main >> create_table_device >> MovetoRedShift >> insert_Devices
