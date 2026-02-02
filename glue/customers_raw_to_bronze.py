import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

DB = "data-platform_database"
SRC_TABLE = "customers"
BRONZE_PATH = "s3://data-platform-data-lake-eu-north-1-998169516591/bronze/customers/"

df = spark.table(f"`{DB}`.`{SRC_TABLE}`")

# bronze: всі колонки STRING, назви як в CSV
out = df.select(*[F.col(c).cast("string").alias(c) for c in df.columns])

out.write.mode("overwrite").format("parquet").save(BRONZE_PATH)

job.commit()