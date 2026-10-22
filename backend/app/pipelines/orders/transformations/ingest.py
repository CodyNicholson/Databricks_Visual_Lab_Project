from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Streaming table: each run reads only the JSON files that arrived since the last run
@dp.table(name="orders_raw", comment="Raw orders, ingested incrementally from the landing folder")
def orders_raw():
    return (
        spark.readStream.schema("order_id INT, customer STRING, store STRING, amount DOUBLE")
        .json("/lakehouse/landing/sdp_orders")
        .withColumn("ingested_at", F.current_timestamp())
    )
