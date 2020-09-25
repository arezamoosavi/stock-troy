import sys
import os
import json
import logging
import requests

from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


def develop_pred_model(hdfs_master, hdfs_path, run_time, **kwargs):
    logger.info(f"PATH: {hdfs_master} {hdfs_path} {run_time}")

    conf = SparkConf()
    conf.setAppName("Daily Model Stock")
    spark = (
        SparkSession.builder.master("spark://app:7077").config(conf=conf).getOrCreate()
    )

    logger.info("READing from hdfs")
    _path = os.path.join(*[hdfs_master, hdfs_path, "tesla_stock_data"])
    _df = spark.read.format("csv").option("header", "true").csv(f"{_path}/*.csv")

    _df.show()
    _df.printSchema()

    return "Done!"


if __name__ == "__main__":

    hdfs_master = str(sys.argv[1])
    hdfs_path = str(sys.argv[2])
    run_time = str(sys.argv[3])

    develop_pred_model(
        hdfs_master=hdfs_master, hdfs_path=hdfs_path, run_time=run_time,
    )

