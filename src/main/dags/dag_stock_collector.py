import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from dags.etl.daily_stock_collector import create_hourly_stock_etl, say_hi
from dags.etl.minutely_stock_collector import create_minuetly_stock_etl

logger = logging.getLogger(__name__)
logger.setLevel("WARNING")


args = {
    "owner": "stock-troy",
    "start_date": datetime(year=2020, month=9, day=10, hour=1, minute=0, second=0),
    "provide_context": True,
}


hi_dag = DAG(
    dag_id="for_say_hi",
    default_args=args,
    schedule_interval="@hourly",
    max_active_runs=1,
)

run_hi = PythonOperator(task_id="Say_HI", python_callable=say_hi, dag=hi_dag,)

run_hi


hourly_dag = DAG(
    dag_id="daily_gather_stock_data",
    default_args=args,
    schedule_interval="@daily",
    max_active_runs=1,
)


run_gather_stock = PythonOperator(
    task_id="create_hourly_stock_etl",
    python_callable=create_hourly_stock_etl,
    op_kwargs={
        "hdfs_master": "{{var.value.hdfs_master}}",
        "hdfs_path": "{{var.value.hdfs_path}}",
        "run_time": "{{yesterday_ds}}",
    },
    dag=hourly_dag,
)

run_gather_stock

minutely_dag = DAG(
    dag_id="minutely_gather_stock_data",
    default_args=args,
    schedule_interval="*/10 * * * *",
    max_active_runs=1,
)


run_gather_stock_mintuely = PythonOperator(
    task_id="create_minutely_stock_etl",
    python_callable=create_minuetly_stock_etl,
    op_kwargs={
        "hdfs_master": "{{var.value.hdfs_master}}",
        "hdfs_path": "{{var.value.hdfs_path}}",
        "run_time": "{{yesterday_ds}}",
    },
    dag=minutely_dag,
)

run_gather_stock_mintuely


# stock_etl = BashOperator(
#     task_id="stock_report",
#     bash_command="spark-submit --master spark://app:7077 "
#     "--deploy-mode client "
#     "--verbose "
#     "/opt/project/dags/etl/hourly_stock_collector.py "
#     "{{var.value.hdfs_master}} "
#     "{{var.value.hdfs_path}} "
#     "{{yesterday_ds}}",
#     dag=dag,
# )

# stock_etl
