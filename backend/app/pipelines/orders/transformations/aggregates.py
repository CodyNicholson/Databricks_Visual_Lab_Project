from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Materialized views: recomputed from their inputs on every run
@dp.materialized_view(name="revenue_by_store")
def revenue_by_store():
    return spark.read.table("orders_clean").groupBy("store").agg(
        F.count("*").alias("orders"), F.round(F.sum("amount"), 2).alias("revenue"))


@dp.materialized_view(name="top_customers")
def top_customers():
    return (spark.read.table("orders_clean").groupBy("customer")
            .agg(F.round(F.sum("amount"), 2).alias("spent")).orderBy(F.desc("spent")).limit(5))
