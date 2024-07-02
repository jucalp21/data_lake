import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def get_secret(secret_name):
    region_name = "us-east-2"

    client = boto3.client('secretsmanager', region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'secret-name'])
secret_name = args['secret-name']
secret = get_secret(secret_name)
mongo_uri_base = secret['mongo_uri']

s3_output_path = "654654288333-dl-bronze"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

database = "myspace1a_rc"
collections = ["users", "cities", "offices", "rooms"]

for collection in collections:
    mongo_uri = f"{mongo_uri_base}&database={database}&collection={collection}"
    df = spark.read.format("mongo").option("uri", mongo_uri).load()

    output_path = f"s3://{s3_output_path}/mongo/{collection}/"
    df.write.format("parquet").mode("overwrite").save(output_path)

job.commit()
