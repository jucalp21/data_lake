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
user_input_path = "s3://data-lake-demo/bronze/users/data.parquet/"
studio_output_path = "s3://data-lake-demo/silver/studios_earnings/"
performer_output_path = "s3://data-lake-demo/silver/earnings_by_performer/"

# Read the Parquet files from S3 into DynamicFrames
streamate_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="parquet"
)

user_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [user_input_path]},
    format="parquet"
)

# Convert the DynamicFrames to DataFrames
streamate_df = streamate_dynamic_frame.toDF()
user_df = user_dynamic_frame.toDF()

# Extract the relevant data from the nested JSON structure
studio_records = []
performer_records = []

# Process studio and performer earnings
if not streamate_df.rdd.isEmpty():
    for row in streamate_df.collect():
        if 'studios' in row:
            studios = row['studios']
            for studio in studios:
                studio_id = studio['studioId']
                studio_email = studio['emailAddress']

                # Process studio earnings
                earnings = studio['earnings']
                for earning in earnings:
                    studio_record = {
                        'studioId': studio_id,
                        'emailAddress': studio_email,
                        'date': earning['date'],
                        'payableAmount': earning['payableAmount']
                    }
                    studio_records.append(studio_record)

                # Process performer earnings
                if 'performers' in studio:
                    performers = studio['performers']
                    for performer in performers:
                        performer_id = performer['performerId']
                        performer_nickname = performer['nickname']
                        performer_email = performer['emailAddress']
                        performer_earnings = performer['earnings']
                        for p_earning in performer_earnings:
                            performer_record = {
                                'performerId': performer_id,
                                'nickname': performer_nickname,
                                'emailAddress': performer_email,
                                'date': p_earning['date'],
                                'onlineSeconds': p_earning['onlineSeconds'],
                                'payableAmount': p_earning['payableAmount']
                            }
                            performer_records.append(performer_record)

# Convert the records to DataFrames
studio_df = spark.createDataFrame(studio_records)
performer_df = spark.createDataFrame(performer_records)

# Join performer_df and user_df on emailAddress and streamateUser
joined_df = performer_df.join(user_df, performer_df.emailAddress == user_df.streamateUser, "left_outer")\
                        .select(performer_df["performerId"], performer_df["nickname"], performer_df["emailAddress"], performer_df["date"], performer_df["onlineSeconds"], performer_df["payableAmount"], user_df["_id"])

# Convert DataFrames to DynamicFrames
studio_dynamic_dframe = DynamicFrame.fromDF(
    studio_df, glueContext, "studio_dynamic_dframe")
performer_dynamic_dframe = DynamicFrame.fromDF(
    joined_df, glueContext, "performer_dynamic_dframe")

# Write the DynamicFrames to S3 as JSON
glueContext.write_dynamic_frame.from_options(
    frame=studio_dynamic_dframe,
    connection_type="s3",
    connection_options={"path": studio_output_path},
    format="json"
)

glueContext.write_dynamic_frame.from_options(
    frame=performer_dynamic_dframe,
    connection_type="s3",
    connection_options={"path": performer_output_path},
    format="json"
)

job.commit()
