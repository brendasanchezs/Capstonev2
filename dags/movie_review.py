from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

# Configurations


JOB_FLOW_OVERRIDES = {
    "Name": "Movie review classifier",
    "LogUri":"s3://staging-movie-bucket/",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}, {"Name": "Livy"} ], # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                   "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
             
                 }
            ],
        }  
    ],
    

     "BootstrapActions": [
       {
         "Name": "CustomBootStrapAction",
         "ScriptBootstrapAction": {
           "Path": "s3://raw-movie-data/xml_package.sh",
         }
        }
     ],
   
   "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}
##### Where the SPARK steps execute

SPARK_STEPS = [ 
    # Note the params values are supplied to the operator
    
    {
        "Name": "Classify movie and log reviews",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://raw-movie-data/transformations.py",
            ],
        },
    }
    
]


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 10, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}

dag = DAG(
    "data_pipeline",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="Init", dag=dag)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="transformation_movies",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
      params={ # these params are used to fill the paramterized values in SPARK_STEPS json
        "BUCKET_NAME":"raw-movie-data",
        "s3_script": "s3://raw-movie-data/transformations.py"
    },
    dag=dag,
)
step_adder2 = EmrAddStepsOperator(
    task_id="transformation_log_reviews",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
      params={ # these params are used to fill the paramterized values in SPARK_STEPS json
        "BUCKET_NAME":"raw-movie-data",
        "s3_script": "s3://raw-movie-data/transformations.py"
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="step_sensor",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='transformation_movies', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)
task1 = PostgresOperator(task_id = 'create_table_user_purchase',
                        sql="""
                        CREATE TABLE IF NOT EXISTS cities (    
                            LatD INTEGER,
                            LatM INTEGER,
                            LatS INTEGER,
                            NS VARCHAR(255),
                            LonD INTEGER,
                            LonM INTEGER,
                            LonS INTEGER,
                            EW VARCHAR(255), 
                            City VARCHAR(255),
                            State VARCHAR(255));
                            """,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)

task2 = PythonOperator(task_id='csv_to_database_postgres',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)

end_data_pipeline = DummyOperator(task_id="End", dag=dag)

start_data_pipeline >> task1 >> task2 >> create_emr_cluster >> [step_adder, step_adder2] >>step_checker >> terminate_emr_cluster >> end_data_pipeline
