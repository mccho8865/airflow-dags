import pendulum

from airflow import DAG
from airflow.utils import timezone
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

local_tz = pendulum.timezone("Asia/Seoul")
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
    'load_ticker_from_nas_hourly',
    default_args=default_args,
    description='load_ticker_from_nas_hourly',
    schedule_interval='@daily',
    start_date=datetime(2022, 6, 13, tzinfo=local_tz),
    tags=['dw', 'daily'],
)

start = DummyOperator(task_id="start")
end = DummyOperator(task_id="end")

spark_config = Variable.get("spark_option", deserialize_json=True)

spark_op = SparkSubmitOperator(task_id='dw_bithumb_ticker', 
                               name = "airflow_dw_bithumb_ticker_{{execution_date}}",
#                                application_args = ["--date", "{{ (execution_date  + macros.timedelta(hours=9)).strftime('%Y-%m-%d') }}"],
                               application_args = ["--date", "{{execution_date}}"],
                               conf = spark_config,
                               conn_id = "spark_conn",
                               application = 'https://raw.githubusercontent.com/mccho8865/airflow-dags/main/dw_bithumb_ticker/daily_load_bithumb_ticker.py',
                               driver_memory = '4g',
                               executor_memory = '8g',
                               num_executors = 3,                               
                               dag=dag,)

start >> spark_op >> end