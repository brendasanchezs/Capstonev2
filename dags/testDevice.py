import airflow
from airflow import DAG
from airflow.operators import PythonOperator
#from airflow.operators.python_operator import PythonOperator
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
    'owner': 'lzhou519',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id = 'LogReview',
    default_args = default_args,
    description = 'Desc',
    schedule_interval='0 6 * * *')

create_table_main = PostgresOperator(
    task_id="create_lob",
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
            
create_table_us = PostgresOperator(
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

insert_USdata = PythonOperator(
    task_id="insert_Devices",
    provide_context=True,
    python_callable=insertUSdata,
    dag=dag
)

create_table_main >> MovetoRedShift >> insert_USdata 
