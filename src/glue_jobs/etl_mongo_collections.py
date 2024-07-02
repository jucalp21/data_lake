import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
s3_output_path = "654654288333-dl-bronze"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

database = "myspace1a_rc"
collections = ["users", "cities", "offices", "rooms"]
mongo_uri_base = "mongodb://root:oneG-TrYh4Ck%2aTh15@3.210.8.173:27017/?authSource=admin&readPreference=primary&directConnection=true&ssl=false"

for collection in collections:
    mongo_uri = f"{mongo_uri_base}&database={database}&collection={collection}"
    df = spark.read.format("mongo").option("uri", mongo_uri).load()

    output_path = f"s3://{s3_output_path}/mongo/{collection}/"
    df.write.format("parquet").mode("overwrite").save(output_path)

job.commit()
