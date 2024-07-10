import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from datetime import datetime

# Configuración del logger para registrar eventos
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Obtener los argumentos del trabajo de Glue
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Definición de variables
    s3_output_path = "data-lake-demo/bronze/dbUsers"
    database = "myspace1a_rc"
    target_collection = "users"
    mongo_uri_base = "mongodb://root:oneG-TrYh4Ck*Th15@3.210.8.173:27017/?authSource=admin&readPreference=primary&directConnection=true&ssl=false"
    mongo_uri = f"{mongo_uri_base}&database={database}&collection={target_collection}"

    logger.info(f"Conectando a MongoDB con URI: {mongo_uri}")

    # Cargar datos de MongoDB
    df = spark.read.format("mongo").option("uri", mongo_uri).load()

    if df is None or df.rdd.isEmpty():
        raise ValueError("No se encontraron datos en la colección de MongoDB.")

    output_path = f"s3://{s3_output_path}/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.parquet"
    df.write.parquet(output_path)

    logger.info(f"Datos escritos en: {output_path}")

    job.commit()

except Exception as e:
    logger.error(f"Error en el trabajo de Glue: {str(e)}")
    job.commit()
    raise

finally:
    sc.stop()
