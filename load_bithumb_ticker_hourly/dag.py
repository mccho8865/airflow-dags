import io, ftplib
import pendulum, pytz
import os, socket

import boto
import boto.s3.connection
from boto.s3.key import Key
from datetime import timedelta, datetime

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

local_tz = pendulum.timezone("Asia/Seoul")
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
    kst = pytz.timezone("Asia/Seoul")
    ts = datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S+00:00')
    ts = ts.astimezone(kst)
    dt = ts.strftime("%Y-%m-%d")
    dt_nodash = ts.strftime("%Y%m%d")
    hh = ts.strftime("%H")
    read_dir = f"/raw/ticker/dt={dt_nodash}/hh={hh}"
    
    print("[INFO] ts:", ts)
    print("[INFO] read_dir:", read_dir)
    
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
    schedule_interval='0 * * * *',
    start_date=datetime(2021, 9, 21, tzinfo=local_tz),
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

# pod_ip = socket.gethostbyname(socket.gethostname())
spark_config = {"spark.kubernetes.container.image": "localhost:30580/spark-py:3.0.2",
                "spark.kubernetes.node.selector.spark": "",
                "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
                "spark.hadoop.fs.s3a.fast.upload": "true",
                "spark.hadoop.fs.s3a.endpoint": "rook-ceph-rgw-my-store.rook-ceph.svc.cluster.local",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.access.key": access_key,
                "spark.hadoop.fs.s3a.secret.key": secret_key,
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": "s3a://logs/spark-hs/",
                "spark.sql.session.timeZone": "Asia/Seoul",
                "spark.driver.extraJavaOptions": "'-Duser.timezone=Asmcchia/Seoul -Dio.netty.tryReflectionSetAccessible=true'",
                "spark.executor.extraJavaOptions": "'-Duser.timezone=Asia/Seoul -Dio.netty.tryReflectionSetAccessible=true'",
                "spark.sql.sources.partitionOverwriteMode": "dynamic"}

# packages = ['org.apache.hadoop:hadoop-aws:3.2.0',
#             'org.apache.hadoop:hadoop-common:3.2.0',
#             'com.amazonaws:aws-java-sdk:1.12.105']

load_batch = SparkSubmitOperator(task_id='load_to_batch_layer', 
                               name = "load_ticker_to_batch_{{ ts }}",
                               application_args = ["--date", "{{ ds }}"],
                               conf = spark_config,
#                                packages = ",".join(packages),
                               conn_id = "spark_conn",
#                                application = os.path.dirname(os.path.realpath(__file__)) + "/parquet_loader_pyudf.py",
                               application = 'https://raw.githubusercontent.com/mccho8865/airflow-dags/main/load_bithumb_ticker_hourly/parquet_loader_pyudf.py',
                               driver_memory = '4g',
                               executor_memory = '8g',
                               num_executors = 2,                               
                               dag=dag,)

start >> load_to_s3_from_nas >> load_batch >> end
