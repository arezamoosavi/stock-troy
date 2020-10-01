import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from dags.etl.make_pred_model import develop_pred_model, develop_pred_model_v2

logger = logging.getLogger(__name__)
logger.setLevel("WARNING")


args = {
    "owner": "stock-troy",
    "start_date": datetime(year=2020, month=9, day=10, hour=1, minute=0, second=0),
    "provide_context": True,
}


ml_dag = DAG(
    dag_id="daily_ml_stock_data",
    default_args=args,
    schedule_interval="@daily",
    max_active_runs=1,
)


pred_stock_model = PythonOperator(
    task_id="create_hourly_stock_etl",
    python_callable=develop_pred_model,
    op_kwargs={
        "hdfs_master": "{{var.value.hdfs_master}}",
        "hdfs_path": "{{var.value.hdfs_path}}",
        "run_time": "{{yesterday_ds}}",
    },
    dag=ml_dag,
)

pred_stock_model


ml_dagv2 = DAG(
    dag_id="daily_ml_stock_datav2",
    default_args=args,
    schedule_interval="@daily",
    max_active_runs=1,
)


pred_stock_modelv2 = PythonOperator(
    task_id="create_hourly_stock_etl",
    python_callable=develop_pred_model_v2,
    op_kwargs={
        "hdfs_master": "{{var.value.hdfs_master}}",
        "hdfs_path": "{{var.value.hdfs_path}}",
        "run_time": "{{yesterday_ds}}",
    },
    dag=ml_dagv2,
)

pred_stock_modelv2
