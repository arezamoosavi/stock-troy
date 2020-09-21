import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from etl.hourly_stock_collector import create_hourly_stock_etl

logger = logging.getLogger(__name__)
logger.setLevel("WARNING")


args = {
    "owner": "stock-troy",
    "start_date": datetime(year=2020, month=9, day=10, hour=1, minute=0, second=0),
    "provide_context": True,
}

dag = DAG(
    dag_id="daily_gather_stock_data",
    default_args=args,
    schedule_interval="@daily",
    max_active_runs=10,
)


run_gather_stock = PythonOperator(
    task_id="create_hourly_stock_etl",
    python_callable=create_hourly_stock_etl,
    op_kwargs={
        "hdfs_master": "{{var.value.hdfs_master}}",
        "hdfs_path": "{{var.value.hdfs_path}}",
        "run_time": "{{yesterday_ds}}",
    },
    dag=dag,
)

run_gather_stock


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
