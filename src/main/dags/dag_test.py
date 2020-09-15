from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from utils.common import greet, respond, spark_task

dag = DAG(
    dag_id="my_simple_dag",
    start_date=datetime(year=2020, month=1, day=1, hour=12, minute=1, second=1),
    schedule_interval="@yearly",
    max_active_runs=1,
)
opr_hello = BashOperator(task_id="say_Hi", bash_command='echo "Hi!!"', dag=dag,)

opr_greet = PythonOperator(task_id="greet", python_callable=greet, dag=dag,)
opr_sleep = BashOperator(task_id="sleep_me", bash_command="sleep 5", dag=dag,)

opr_respond = PythonOperator(task_id="respond", python_callable=respond, dag=dag,)

opr_spark = PythonOperator(task_id="sparking", python_callable=spark_task, dag=dag,)

opr_hello >> opr_greet >> opr_sleep >> opr_respond >> opr_spark