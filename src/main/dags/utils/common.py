import os
import datetime as dt
from pyspark.sql import SparkSession
from airflow.hooks import webhdfs_hook
import logging


logger = logging.getLogger(__name__)
logger.setLevel("WARNING")


def greet():
    print("Writing in file")
    with open("greet.txt", "a+", encoding="utf8") as f:
        now = dt.datetime.now()
        t = now.strftime("%Y-%m-%d %H:%M")
        f.write(str(t) + "\n")
    return "Greeted"


def respond():
    return "Greet Responded Again"


def spark_task():

    spark = SparkSession.builder.master("spark://app:7077").getOrCreate()
    sc = spark.sparkContext

    # Sum of the first 100 whole numbers
    rdd = sc.parallelize(range(100 + 1))
    suum = rdd.sum()
    print(suum)
    logger.info(suum)

    return "Done!"


def check_if_data_exist(ds, data_path, **kwargs):

    hd_hook = webhdfs_hook.WebHDFSHook()
    path = os.path.join(*["/", data_path])
    logger.info("Check wheater data is in path or not" + path)
    res = hd_hook.check_for_path(path)

    return res

