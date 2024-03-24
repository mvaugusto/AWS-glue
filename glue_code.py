from datetime import datetime
from pyspark.context import SparkContext
import pyspark.sql.functions as f

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

glue_db = "demo"
glue_tbl = "sample_data_csv"

s3_write_path = "s3://whizlabs66477874"

dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_start)

dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl)

data_frame = dynamic_frame_read.toDF()

decode_col = f.floor(data_frame["year"]/10)*10
data_frame = data_frame.withColumn("decade", decode_col)


data_frame_aggregated = data_frame.groupby("decade").agg(
    f.count(f.col("movie")).alias('movie_count'),
    f.mean(f.col("rating")).alias('rating_mean'),
    )

data_frame_aggregated = data_frame_aggregated.orderBy(f.desc("movie_count"))

data_frame_aggregated.show(10)

data_frame_aggregated = data_frame_aggregated.repartition(1)

dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glue_context, "dynamic_frame_write")

glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path,
    },
    format = "csv"
)

dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_end)