from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

import load_to_s3_from_nas
# import load_to_batchlayer_ticker

default_args = {
    'owner': 'mccho',
    'depends_on_past': False,
    'email': ['mccho8865@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
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

dag = DAG(
    'load_bithumb_ticker_hourly',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='5 * * * *',
    start_date=datetime(2021, 9, 15),
    tags=['load', 'bithumb'],
)

start = DummyOperator(task_id="start")
end = DummyOperator(task_id="end")

# merge files about hourly ticker and upload to s3 from nas
load_to_s3_from_nas = PythonOperator(
    task_id='load_to_s3_from_nas',
    python_callable=load_to_s3_from_nas.execute,
    op_kwargs={'dt': '{{ ds.strftime("%Y-%m-%d") }}',
               'hh': '{{ ts.strftime("%H")}}'},
    dag=dag
)

# load unique ticker item to warehouse
# load_to_batchlayer_ticker = PythonOperator(
#     task_id='load_to_batchlayer_ticker',
#     python_callable=load_to_batchlayer_ticker.execute,
#     op_kwargs={'dt': '{{ ds.strftime("%Y-%m-%d") }}',
#                'hh': '{{ ts.strftime("%H")}}'},
#     dag=dag
# )

start >> load_to_s3_from_nas >> end
