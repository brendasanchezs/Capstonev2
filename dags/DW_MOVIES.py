import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 10, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False
}

dag = DAG(
    "DW_MOVIES",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    max_active_runs=1,
)



start = DummyOperator(task_id="start", dag=dag) 

create_table_main = DummyOperator(task_id="create_main_tables", dag=dag)
            
create_tables = DummyOperator(task_id="create_dims_and_fact", dag=dag)




MovetoRedShift = DummyOperator(task_id="Move_data_mains", dag=dag)

insert_Devices = DummyOperator(task_id="insert_dim_Device", dag=dag)
insert_os = DummyOperator(task_id="insert_dim_Os", dag=dag)
insert_date = DummyOperator(task_id="insert_dim_Date", dag=dag)
insert_location = DummyOperator(task_id="insert_dim_Location", dag=dag)
insert_browser = DummyOperator(task_id="insert_dim_Browser", dag=dag)

insert_fact = DummyOperator(task_id="insert_Fact", dag=dag) 


end_data_pipeline = DummyOperator(task_id="end", dag=dag)

start >> create_table_main >> create_tables >> MovetoRedShift >>  [insert_Devices,insert_os,insert_date,insert_location,insert_browser] >>insert_fact >> end_data_pipeline
