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
users_api_url = "https://devstreamatemock.omgworldwidegroup.com/api/v1/user"
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbEFkZHJlc3MiOiJ0ZXN0c3R1ZGlvMTIzNEBzdHJlYW1hdGVtb2RlbHMuY29tIiwiaWF0IjoxNzIxMjMyODE3LCJleHAiOjE3NTI3OTA0MTd9.wXwM86yl9w-PiQVCVNW-9ljGCudPBki77YSC7esmuwI"

try:
    payload = {
        "token": token
    }

    # Consultar datos de estudios y performadores
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
                    earning['payableAmount']), performer["emailAddress"]))

    # Consultar datos de modelos
    response = requests.get(users_api_url)
    response.raise_for_status()
    users_data = response.json()

    if not users_data.get('success', False):
        raise ValueError("La consulta de usuarios no fue exitosa")

    users = users_data['users']

    models_df = []

    for user in users:
        models_df.append((
            user["artisticName"],
            user["jasminUser"],
            user["office"],
            user["room"],
            user["streamateUser"],
            user["_id"],
            user["city"],
            user["user"]
        ))

    # Crear DataFrames de Spark a partir de las listas
    df_studios = spark.createDataFrame(
        studios_df, ["studioId", "emailAddress"])
    df_earnings_by_studio = spark.createDataFrame(
        earnings_by_studio_df, ["date", "payableAmount", "studioId"])
    df_performers = spark.createDataFrame(
        performers_df, ["performerId", "nickname", "emailAddress"])
    df_earnings_by_performer = spark.createDataFrame(earnings_by_performer_df, [
                                                     "date", "onlineSeconds", "payableAmount", "emailAddress"])
    df_models = spark.createDataFrame(models_df, [
        "artisticName", "jasminUser", "office", "room", "streamateUser", "_id", "city", "user"])

    # Unir performers con models usando el streamateUser
    df_performers_with_id = df_performers.join(
        df_models, df_performers.emailAddress == df_models.streamateUser, "left").select(
            df_performers["*"], df_models["_id"].alias("model_id"))

    # Unir earnings_by_performers con models usando el streamateUser y el emailAddress
    df_earnings_by_performer_with_id = df_earnings_by_performer.join(
        df_models, df_earnings_by_performer.emailAddress == df_models.streamateUser, "left").select(
            df_earnings_by_performer["*"], df_models["_id"].alias("model_id"))

    # Almacenar studios en S3
    df_studios.write.mode("overwrite").json(
        f"{s3_output_path}/streamatemock/studios/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json")

    # Almacenar earnings_by_studio en S3
    df_earnings_by_studio.write.mode("overwrite").json(
        f"{s3_output_path}/streamatemock/earnings_by_studio/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json")

    # Almacenar performers en S3
    df_performers_with_id.write.mode("overwrite").json(
        f"{s3_output_path}/streamatemock/performers/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json")

    # Almacenar earnings_by_performer en S3
    df_earnings_by_performer_with_id.write.mode("overwrite").json(
        f"{s3_output_path}/streamatemock/earnings_by_performer/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json")

    # Almacenar models en S3
    df_models.write.mode("overwrite").json(
        f"{s3_output_path}/streamatemock/models/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json")

except Exception as e:
    raise e

job.commit()
