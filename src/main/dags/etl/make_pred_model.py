import sys
import os
import json
import logging
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from pyspark import SparkConf
from datetime import datetime

from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


def develop_pred_model_v2(hdfs_master, hdfs_path, run_time, **kwargs):
    logger.info(f"PATH: {hdfs_master} {hdfs_path} {run_time}")

    conf = SparkConf()
    conf.setAppName("Daily Model Stock")
    spark = (
        SparkSession.builder.master("spark://app:7077").config(conf=conf).getOrCreate()
    )

    logger.info("READing from hdfs")
    _path = os.path.join(*[hdfs_master, hdfs_path, "real_time_tesla_stock_data"])
    _df = spark.read.format("csv").option("header", "true").csv(f"{_path}/*.csv")

    _df.show()
    _df = _df.na.drop()

    # _df = _df.withColumn("date", unix_timestamp("date", format="yyyy-MM-dd HH:mm:ss"))

    _df = _df.withColumn("price", _df["price"].cast("float").alias("price"))
    _df = _df.withColumn("date", _df["date"].cast("timestamp").alias("date"))

    _df.printSchema()
    _df = _df.orderBy("date", ascending=True)
    _df.show()

    logger.info("df to pandas:")
    pd_df = _df.toPandas()
    pd_df.dropna(inplace=True)

    logger.info("SHAPE is: ")
    logger.info(pd_df.head(8))

    # _df.withColumn("new_column", lag("num", 1, 0).over(w)).show lag(col, count=1, default=None)

    return "Done!"


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

    # _df = _df.withColumn("date", unix_timestamp("date", format="yyyy-MM-dd HH:mm:ss"))

    _df = _df.withColumn("close", _df["close"].cast("float").alias("close"))
    _df = _df.withColumn("open", _df["open"].cast("float").alias("open"))
    _df = _df.withColumn("date", _df["date"].cast("timestamp").alias("date"))

    _df.printSchema()
    _df = _df.orderBy("date", ascending=True)
    _df.show()

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
