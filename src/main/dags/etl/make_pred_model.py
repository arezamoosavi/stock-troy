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

    logger.info("df to pandas:")
    pd_df = _df.toPandas()
    pd_df.dropna(inplace=True)
    # pd_df["date"] = pd_df.to_datetime(pd_df.date)
    pd_df.sort_values(by=["date"], inplace=True, ascending=True)
    pd_df.reset_index(inplace=True)

    logger.info("SHAPE is: ")
    logger.info(pd_df.shape)
    logger.info(pd_df)
    logger.info(pd_df.describe())

    return "Done!"


if __name__ == "__main__":

    hdfs_master = str(sys.argv[1])
    hdfs_path = str(sys.argv[2])
    run_time = str(sys.argv[3])

    develop_pred_model(
        hdfs_master=hdfs_master, hdfs_path=hdfs_path, run_time=run_time,
    )
