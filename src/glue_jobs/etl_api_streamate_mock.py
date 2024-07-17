import sys
import json
import requests
from awsglue.job import Job
from datetime import datetime
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3_output_path = "s3://data-lake-demo/bronze/"
api_url = "https://devstreamatemock.omgworldwidegroup.com/api/v1/studio/earningsdailysummary"
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbEFkZHJlc3MiOiJ0ZXN0c3R1ZGlvMTIzNEBzdHJlYW1hdGVtb2RlbHMuY29tIiwiaWF0IjoxNzIxMjMyODE3LCJleHAiOjE3NTI3OTA0MTd9.wXwM86yl9w-PiQVCVNW-9ljGCudPBki77YSC7esmuwI"

try:
    payload = {
        "token": token
    }

    response = requests.post(api_url, json=payload)
    response.raise_for_status()
    data = response.json()

    if 'studios' not in data:
        raise ValueError("No se encontró el campo studios")

    studios = data['studios']

    studios_df = []
    earnings_by_studio_df = []
    performers_df = []
    earnings_by_performer_df = []

    for studio in studios:
        studio_id = studio["studioId"]
        email_address = studio["emailAddress"]

        studios_df.append((studio_id, email_address))

        for earning in studio['earnings']:
            earnings_by_studio_df.append(
                (earning['date'], float(earning['payableAmount']), studio_id))

        for performer in studio['performers']:
            performers_df.append(
                (performer["performerId"], performer["nickname"], performer["emailAddress"]))

            for earning in performer['earnings']:
                earnings_by_performer_df.append((earning['date'], int(earning['onlineSeconds']), float(
                    earning['payableAmount']), performer["performerId"]))

    df_studios = spark.createDataFrame(
        studios_df, ["studioId", "emailAddress"])
    df_earnings_by_studio = spark.createDataFrame(
        earnings_by_studio_df, ["date", "payableAmount", "studioId"])
    df_performers = spark.createDataFrame(
        performers_df, ["performerId", "nickname", "emailAddress"])
    df_earnings_by_performer = spark.createDataFrame(earnings_by_performer_df, [
                                                     "date", "onlineSeconds", "payableAmount", "performerId"])

    df_studios.write.mode("overwrite").json(
        f"{s3_output_path}/streamatemock/studios/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json")

    df_earnings_by_studio.write.mode("overwrite").json(
        f"{s3_output_path}/streamatemock/earnings_by_studio/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json")

    df_performers.write.mode("overwrite").json(
        f"{s3_output_path}/streamatemock/performers/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json")

    df_earnings_by_performer.write.mode("overwrite").json(
        f"{s3_output_path}/streamatemock/earnings_by_performer/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json")

except Exception as e:
    raise e

job.commit()
