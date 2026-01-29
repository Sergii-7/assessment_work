
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
df = glueContext.create_dynamic_frame.from_catalog(
             database="my_df",
             table_name="my_table")
print("Count: ", df.count())
df.printSchema()

glueContext.write_dynamic_frame.from_options(frame = df,
          connection_type = "s3",
          connection_options = {"path": "s3://my-target-bucket/output-dir/df"},
          format = "parquet")

# equivalent to above
df2 = df.toDF().repartition(1)
df2.write.parquet("s3://my-target-bucket/output-dir/df")