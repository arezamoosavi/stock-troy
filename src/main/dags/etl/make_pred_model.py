import sys
import os
import json
import logging
import requests
import io

import tempfile
import joblib

from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from pyspark import SparkConf
from datetime import datetime

from pyspark.ml import Pipeline

from joblib import dump, load
from sklearn.ensemble import RandomForestRegressor

from utils.minio_connection import MinioClient

try:
    minio_obj = MinioClient()
    minio_client = minio_obj.client()
except Exception as e:
    print(e)


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
    _path = os.path.join(*[hdfs_master, hdfs_path, "tesla_pd_data"])
    _df = spark.read.format("csv").option("header", "true").csv(f"{_path}/*.csv")

    _df.show()
    _df = _df.na.drop()

    _df = _df.withColumn("price", _df["price"].cast("float").alias("price"))
    _df = _df.withColumn("volume", _df["volume"].cast("float").alias("volume"))
    _df = _df.withColumn(
        "price_after_ten",
        _df["price_after_ten"].cast("float").alias("price_after_ten"),
    )
    _df = _df.withColumn(
        "price_at_20", _df["price_at_20"].cast("float").alias("price_at_20")
    )
    _df = _df.withColumn("date", _df["date"].cast("timestamp").alias("date"))

    _df.printSchema()
    _df = _df.orderBy("date", ascending=True)
    _df.show()

    logger.info("df to pandas:")
    pd_df = _df.toPandas()

    logger.info("SHAPE is: ")
    logger.info(pd_df.shape)
    logger.info(f"\n {pd_df.head(8)}")

    logger.info("DOing ML: ")

    X = pd_df.drop(["price_after_ten", "date"], axis=1)
    y = pd_df["price_after_ten"]
    reg = RandomForestRegressor(n_estimators=200, random_state=101)
    reg.fit(X, y)

    accuracy = float("{0:.2f}".format(reg.score(X, y) * 100))
    print("\n" * 2, "Train Accuracy is: ", accuracy, "%", "\n" * 2)

    # WRITE
    logger.info("Write to minio: ")
    with tempfile.TemporaryFile() as fp:
        joblib.dump(reg, fp)
        fp.seek(0)
        _buffer = io.BytesIO(fp.read())
        _length = _buffer.getbuffer().nbytes
        minio_client.put_object(
            bucket_name="stock",
            object_name="models/tesla.joblib",
            data=_buffer,
            length=_length,
        )

    # READ
    logger.info("Read from minio: ")
    with tempfile.NamedTemporaryFile() as tmp:

        logger.info(f"model file {tmp.name}")
        minio_client.fget_object("stock", "models/tesla.joblib", tmp.name)
        model_pred = joblib.load(tmp.name)

    logger.info("Test with loaded model from minio: ")
    accuracy = float("{0:.2f}".format(model_pred.score(X, y) * 100))
    print("\n" * 2, "Train Accuracy is: ", accuracy, "%", "\n" * 2)

    return "Done!"
