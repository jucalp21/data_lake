import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import json

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define input and output paths directly in the script
input_path = "s3://data-lake-demo/bronze/streamate/"
output_path = "s3://data-lake-demo/silver/studios_earnings/"

# Read the Parquet files from S3 into a DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="parquet"
)

# Convert the DynamicFrame to a DataFrame
df = dynamic_frame.toDF()

# Extract the relevant data from the nested JSON structure
records = []

# Collect the data from the DataFrame
data = df.collect()

# Assuming the data is structured with one root JSON object containing the studios array
for row in data:
    studios = row.studios
    for studio in studios:
        studio_id = studio['studioId']
        email_address = studio['emailAddress']
        earnings = studio['earnings']
        for earning in earnings:
            record = {
                'studioId': studio_id,
                'emailAddress': email_address,
                'date': earning['date'],
                'payableAmount': earning['payableAmount']
            }
            records.append(record)

# Convert the records to a DataFrame
processed_df = spark.createDataFrame(records)

# Convert DataFrame to DynamicFrame
dynamic_dframe = DynamicFrame.fromDF(
    processed_df, glueContext, "dynamic_dframe")

# Write the DynamicFrame to S3 as JSON
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_dframe,
    connection_type="s3",
    connection_options={"path": output_path},
    format="json"
)

job.commit()
