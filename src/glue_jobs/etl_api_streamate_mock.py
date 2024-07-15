import sys
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, explode, lit, current_timestamp

# Set Up Job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# S3 options
s3_path = "s3://data-lake-demo/mock_data/streamate"
data_format = "json"

# Dynamic Frame
data_source = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format=data_format
)

# Convert DynamicFrame to DataFrame for processing
df = data_source.toDF()

# Add the current timestamp as a column
df_with_timestamp = df.withColumn("data_timestamp", lit(current_timestamp()))

# Extract performers and add timestamp
performers_df = df_with_timestamp.select(
    explode(col("studios.performers")).alias("performer"),
    col("data_timestamp")
).select(
    col("performer.performerId").alias("performer_id"),
    col("performer.nickname"),
    col("performer.emailAddress").alias("email_address"),
    col("data_timestamp")
)

# Store performers data in S3
performers_dynamic_frame = DynamicFrame.fromDF(
    performers_df, glueContext, "performers_dynamic_frame")
performers_output_path = "s3://your-bucket-name/performers/"
glueContext.write_dynamic_frame.from_options(
    frame=performers_dynamic_frame,
    connection_type="s3",
    connection_options={"path": performers_output_path},
    format="json"
)

# Extract earnings data and add timestamp
earnings_df = df_with_timestamp.select(
    explode(col("studios.performers")).alias("performer"),
    col("data_timestamp")
).select(
    col("performer.performerId").alias("performer_id"),
    explode(col("performer.earnings")).alias("earning"),
    col("data_timestamp")
).select(
    col("performer_id"),
    col("earning.date").alias("date"),
    col("earning.onlineSeconds").alias("online_seconds"),
    col("earning.payableAmount").alias("payable_amount"),
    col("data_timestamp")
)

# Create and store performers_earnings table
performers_earnings_dynamic_frame = DynamicFrame.fromDF(
    earnings_df, glueContext, "performers_earnings_dynamic_frame")
performers_earnings_output_path = "s3://your-bucket-name/performers_earnings/"
glueContext.write_dynamic_frame.from_options(
    frame=performers_earnings_dynamic_frame,
    connection_type="s3",
    connection_options={"path": performers_earnings_output_path},
    format="json"
)

# Commit Job
job.commit()
