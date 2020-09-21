import sys
import os
import json
import logging
import subprocess

from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime

try:
    import requests
except ImportError:
    subprocess.check_call(["pip", "install", "requests"])
    import requests

url = "https://financialmodelingprep.com/api/v3/quote-short/TSLA?apikey=71641ac9883a086f82d5e14f86025c0c"

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

    response = requests.get(url).json()
    logger.info(response)
    _df = spark.createDataFrame([line for line in response])
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

