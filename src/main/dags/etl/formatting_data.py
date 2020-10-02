import sys
import os
import json
import logging
import requests
import io

from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from pyspark import SparkConf
from datetime import datetime

from utils.minio_connection import MinioClient

try:
    minio_obj = MinioClient()
    minio_client = minio_obj.client()
except Exception as e:
    print(e)


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


def develop_data_model(hdfs_master, hdfs_path, run_time, **kwargs):
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
    # _df.withColumn("new_column", lag("num", 1, 0).over(w)).show lag(col, count=1, default=None)

    _df = _df.withColumn("price", _df["price"].cast("float").alias("price"))
    _df = _df.withColumn("volume", _df["volume"].cast("float").alias("volume"))
    _df = _df.withColumn("date", _df["date"].cast("timestamp").alias("date"))

    _df.printSchema()
    _df = _df.orderBy("date", ascending=True)
    _df.show()

    logger.info("df to pandas:")
    pd_df = _df.toPandas()

    pd_df.drop(["symbol",], axis=1, inplace=True)

    pd_df["price_after_ten"] = pd_df["price"].shift(-10)
    pd_df["price_at_20"] = pd_df["price"].shift(20)

    pd_df.dropna(inplace=True)
    pd_df.reset_index(inplace=True, drop=True)

    logger.info("To spark: ")
    spark_df = spark.createDataFrame(pd_df)

    save_path = os.path.join(*[hdfs_master, hdfs_path, "tesla_pd_data"])
    spark_df.write.format("csv").option("header", True).mode("overwrite").save(
        save_path111111111111111111111111111111111



        
    )
    spark_df.printSchema()
    spark_df.show()

    logger.info("SHAPE is: ")
    logger.info(pd_df.shape)
    logger.info(f"\n {pd_df.head(8)}")

    
    return "Done!"
