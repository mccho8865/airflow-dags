from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

import ftplib
import io

import boto
import boto.s3.connection
from boto.s3.key import Key

access_key = 'Z780FG2AP64YD0Y2EWS8'
secret_key = 'akGdNm3vY9xSCcyscq8StdTh6BMRGtt9FChidPgn'

conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = 'rook-ceph-rgw-my-store.rook-ceph.svc',
        is_secure=False,               # uncomment if you are not using ssl 
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
coin_bucket = conn.get_bucket("coin-bucket")

def load_to_s3_from_nas(ts):
    ts = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S+00:00')
    dt = ts.strftime("%Y-%m-%d")
    hh = ts.strftime("%H")
    read_dir = f"/raw/ticker/dt={dt}/hh={hh}"
    write_dir = f"/raw/ticker_merged/dt={dt}"

    item_list = []
    with ftplib.FTP() as ftp:
        ftp.connect("192.168.0.10", 21)
        ftp.login()
        ftp.cwd(read_dir)
        file_list = ftp.nlst()

        for file in file_list:
            item = []
            ftp.retrlines(f"RETR {file}", item.append)
            item_list.append("".join(item).replace(" ", ""))
        print(f"item count : {len(item_list)}")

    k = Key(coin_bucket)
    k.key = f"warehouse/raw/ticker/dt={dt}/hh_{hh}.txt"
    k.set_contents_from_string("\n".join(item_list))
    
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
    schedule_interval='5 * * * *',
    start_date=datetime(2021, 9, 15),
    tags=['load', 'bithumb', 'ticker', 'nas_to_s3'],
)

start = DummyOperator(task_id="start")
end = DummyOperator(task_id="end")

# merge files about hourly ticker and upload to s3 from nas
load_to_s3_from_nas = PythonOperator(
    task_id='load_to_s3_from_nas',
    python_callable=load_to_s3_from_nas,
    op_kwargs={'ts': '{{ ts }}'},
    dag=dag
)

start >> load_to_s3_from_nas >> end
