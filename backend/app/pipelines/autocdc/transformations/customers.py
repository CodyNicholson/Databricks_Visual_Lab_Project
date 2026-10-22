from pyspark import pipelines as dp


@dp.temporary_view(name="customer_changes")
def customer_changes():
    return spark.readStream.table("main.sdp_cdc.cdc_feed")


# On Databricks this keeps `customers` as SCD type 1. On Spark 4.2 + Delta 4.4 it fails:
# Delta tables do not expose the DSv2 row-level operations Auto CDC needs.
dp.create_streaming_table("customers")
dp.create_auto_cdc_flow(
    target="customers",
    source="customer_changes",
    keys=["id"],
    sequence_by="seq",
    apply_as_deletes="op = 'DELETE'",
    except_column_list=["op", "seq"],
)
