import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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

stock_etl = BashOperator(
    task_id="stock_report",
    bash_command="spark-submit --master spark://spark:7077 "
    "--deploy-mode client "
    "--verbose "
    "/opt/project/dags/etl/hourly_stock_collector.py "
    "{{var.value.hdfs_master}} "
    "{{var.value.hdfs_path}} "
    "{{yesterday_ds}}",
    dag=dag,
)

stock_etl
