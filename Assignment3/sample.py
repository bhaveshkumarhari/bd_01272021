from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


from datetime import datetime, timedelta
#from airflow.operators.hive_operator import HiveOperator

#from airflow.operators.latest_only_operator import LatestOnlyOperator
#from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    #'start_date': datetime.now(),
    # 'end_date': datetime(2021, 2, 18),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }


dag = DAG(
    'My_Assignment_Demo',
    default_args=default_args,
    description='A simple tutorial DAG',
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1),
)

# t1, t2 and t3 are examples of tasks created by instantiating operators

sqoop_jobs_cmd="/home/hari/sqoop_jobs.sh "

t1 = BashOperator(
    task_id='sqoop_jobs',
    bash_command=sqoop_jobs_cmd,
    dag=dag,
)

external_cmd="/home/hari/external.sh "
#external_cmd="/home/hari/external_hive_script.hql"

t2 = BashOperator(
    task_id='external_jobs',
    depends_on_past=False,
    bash_command=external_cmd,
    dag=dag,
)


internal_hbase_cmd="/home/hari/internal_hbase.sh "
#internal_hbase_cmd="/home/hari/internal_hbase_hive_script.hql"

t3 = BashOperator(
    task_id='internal_hbase_jobs',
    depends_on_past=False,
    bash_command=internal_hbase_cmd,
    dag=dag
)

t1 >> t2 >> t3
