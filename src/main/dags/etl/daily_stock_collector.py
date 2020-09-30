import sys
import os
import json
import logging
import requests
from datetime import datetime

from pyspark.sql.types import FloatType, TimestampType
from pyspark.sql import SparkSession
from pyspark import SparkConf


url = os.getenv("daily_url")


def make_url(_date):
    return f"https://financialmodelingprep.com/api/v3/historical-chart/30min/TSLA?from={_date}&to={_date}&apikey=71641ac9883a086f82d5e14f86025c0c"


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


def say_hi(**kwargs):
    logger.info("HIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")
    return "HIIIIIII"


def create_hourly_stock_etl(hdfs_master, hdfs_path, run_time, **kwargs):
    logger.info(f"HIIIIII {hdfs_master} {hdfs_path} {run_time}")

    conf = SparkConf()
    conf.setAppName("Hourly Stock")
    spark = (
        SparkSession.builder.master("spark://app:7077").config(conf=conf).getOrCreate()
    )
    logger.info("The report of " + run_time + " is started to generate!")

    response = requests.get(make_url(_date=run_time)).json()
    logger.info(response)
    _df = spark.createDataFrame([line for line in response])

    _df = _df.withColumn("close", _df["close"].cast("float").alias("close"))
    _df = _df.withColumn("open", _df["open"].cast("float").alias("open"))
    _df = _df.withColumn("date", _df["date"].cast(TimestampType()).alias("date"))

    _df.show()
    _df.printSchema()

    save_path = os.path.join(*[hdfs_master, hdfs_path, "tesla_stock_data"])
    # _df.write.csv(save_path, mode="overwrite", header=True, encoding="UTF-8")
    _df.write.format("csv").option("header", True).mode("append").save(save_path)

    return "Done!"


if __name__ == "__main__":

    hdfs_master = str(sys.argv[1])
    hdfs_path = str(sys.argv[2])
    run_time = str(sys.argv[3])

    create_hourly_stock_etl(
        hdfs_master=hdfs_master, hdfs_path=hdfs_path, run_time=run_time,
    )

