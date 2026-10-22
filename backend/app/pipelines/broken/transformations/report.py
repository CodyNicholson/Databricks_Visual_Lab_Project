from pyspark import pipelines as dp


# Mistake: reads a dataset that no file in this pipeline defines (a typo of orders_clean)
@dp.materialized_view(name="daily_report")
def daily_report():
    return spark.read.table("orders_cleaned").groupBy("store").count()
