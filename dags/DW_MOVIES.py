import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG('DW_Movies',
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          # https://airflow.apache.org/docs/stable/scheduler.html
          schedule_interval='0 0 * * *'
          #schedule_interval=timedelta(days=1),
          #schedule_interval='0 * * * *'
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
