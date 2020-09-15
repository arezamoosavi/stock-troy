import datetime as dt
from pyspark.sql import SparkSession
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

    spark = SparkSession.builder.master("local").getOrCreate()
    sc = spark.sparkContext

    # Sum of the first 100 whole numbers
    rdd = sc.parallelize(range(100 + 1))
    suum = rdd.sum()
    print(suum)
    logger.info(suum)

    return "Done!"
