import sys
import os
import json
import logging
import requests
import time
from datetime import datetime

from pyspark.sql.types import FloatType, TimestampType
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql import functions as F

# url = os.getenv("now_url")
url = "https://financialmodelingprep.com/api/v3/quote-short/TSLA?apikey=71641ac9883a086f82d5e14f86025c0c"

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


def create_minuetly_stock_etl(hdfs_master, hdfs_path, run_time, **kwargs):
    logger.info(f"HIIIIII {hdfs_master} {hdfs_path} {run_time}")

    conf = SparkConf()
    conf.setAppName("Minutely Stock")
    spark = (
        SparkSession.builder.master("spark://app:7077").config(conf=conf).getOrCreate()
    )
    logger.info("The report of " + run_time + " is started to generate!")

    response = requests.get(url).json()
    logger.info(response)
    _df = spark.createDataFrame([line for line in response])

    # 1
    timestamp = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
    _df = _df.withColumn(
        "date", unix_timestamp(lit(timestamp), "yyyy-MM-dd HH:mm:ss").cast("timestamp")
    )

    # 2: not very good
    # _df = _df.withColumn("date", F.current_timestamp())

    # 3 : just date
    # _df = _df.withColumn("date", F.current_date())

    _df = _df.withColumn("price", _df["price"].cast("float").alias("price"))

    _df.show()
    _df.printSchema()

    save_path = os.path.join(*[hdfs_master, hdfs_path, "real_time_tesla_stock_data"])
    _df.write.format("csv").option("header", True).mode("append").save(save_path)

    return "Done!"


if __name__ == "__main__":

    hdfs_master = str(sys.argv[1])
    hdfs_path = str(sys.argv[2])
    run_time = str(sys.argv[3])

    create_minuetly_stock_etl(
        hdfs_master=hdfs_master, hdfs_path=hdfs_path, run_time=run_time,
    )

