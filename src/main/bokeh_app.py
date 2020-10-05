import os
import logging
import tempfile
import pandas as pd
from datetime import datetime, timedelta
import time
import requests

import csv
from bokeh.io import output_file, show
from tornado.ioloop import IOLoop
from bokeh.plotting import figure, ColumnDataSource
from bokeh.layouts import column
from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.models.formatters import DatetimeTickFormatter

from pyspark.sql import SparkSession
from pyspark import SparkConf

import joblib
from utils.minio_connection import MinioClient


logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


try:
    minio_obj = MinioClient()
    minio_client = minio_obj.client()
except Exception as e:
    print(e)


PORT = 5555
BOKEH_INTERVAL_MS = 250000


def read_from_hdfs(hdfs_master, hdfs_path):

    conf = SparkConf()
    conf.setAppName("Daily Model Stock")
    spark = (
        SparkSession.builder.master("spark://app:7077").config(conf=conf).getOrCreate()
    )

    _path = os.path.join(*[hdfs_master, hdfs_path, "tesla_pd_data"])
    _df = spark.read.format("csv").option("header", "true").csv(f"{_path}/*.csv")

    logger.info("The data is here!")

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
    _df = _df.orderBy("date", ascending=True)

    logger.info("To pandas is done!")

    return _df.toPandas()


def get_model():
    with tempfile.NamedTemporaryFile() as tmp:
        logger.info(f"model file {tmp.name}")
        minio_client.fget_object("stock", "models/tesla.joblib", tmp.name)

        logger.info("The model is here!")

        return joblib.load(tmp.name)


def runner(doc):
    # initiate bokeh column data source
    real_data = ColumnDataSource({"price": [], "date": []})
    estimated_data = ColumnDataSource({"price": [], "date": []})

    # UPDATING FLIGHT DATA
    def update():
        # CONVERT TO PANDAS DATAFRAME
        real_df = read_from_hdfs(
            hdfs_master="hdfs://namenode:8020", hdfs_path="stock_data/"
        )

        logger.info(f"real_df is here! \n {real_df.head()}")

        clf = get_model()

        Pred_amount = clf.predict(
            [[real_df.loc[-1, ["price", "price_at_20", "volume"]]]]
        )[0]
        Pred_time = real_df.loc[-1, "date"] + timedelta(seconds=10)

        data = [[Pred_amount, Pred_time]]
        estimated_df = pd.DataFrame(data, columns=["price", "date"])

        n_roll_e = len(estimated_df.index)
        estimated_data.stream(estimated_df.to_dict(orient="list"), n_roll_e)
        n_roll = len(real_df.index)
        real_data.stream(
            real_df.loc[:, ["price", "date"]].to_dict(orient="list"), n_roll
        )

        logger.info("The data is streamed !")

    TOOLS = "pan,wheel_zoom,box_zoom,reset,undo,save,hover"
    p = figure(
        title="stock predictor",
        x_axis_type="datetime",
        y_axis_label="value",
        x_axis_label="Time",
        tools=TOOLS,
    )
    p.background_fill_color = "beige"
    p.background_fill_alpha = 0.5
    p.outline_line_width = 7
    p.outline_line_alpha = 0.3
    p.outline_line_color = "navy"

    p.sizing_mode = "stretch_both"
    p.line(
        source=real_data,
        x="date",
        y="price",
        legend_label="real ",
        line_color="#f46d43",
        line_width=1,
        line_alpha=0.6,
    )
    p.circle(
        source=real_data,
        x="date",
        y="price",
        legend_label="real",
        line_color="#3288bd",
        fill_color="white",
        line_width=5,
    )
    p.circle(
        source=estimated_data,
        x="date",
        y="price",
        legend_label="next 10 estimate",
        line_color="red",
        fill_color="red",
        line_width=6,
    )

    p.title.text_font_size = "20pt"
    p.xaxis.axis_label_text_font_size = "15pt"
    p.xaxis.major_label_text_font_size = "12pt"
    p.yaxis.axis_label_text_font_size = "15pt"
    p.yaxis.major_label_text_font_size = "12pt"
    p.legend.label_text_font_size = "12pt"
    p.title.align = "center"
    p.legend.location = "top_right"
    p.legend.click_policy = "hide"
    p.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d %H:%M:%S")

    doc.add_periodic_callback(update, BOKEH_INTERVAL_MS)
    doc.add_root(p)
    # output_file("tesla_stock.html")
    # show(p)


server = Server(
    {"/": runner},
    port=PORT,
    # address="0.0.0.0",
    # allow_websocket_origin=["*"],
    # use_xheaders=True,
)
server.start()
IOLoop.current().start()
