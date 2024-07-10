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

    # Configurar SLF4J para suprimir logs innecesarios
    spark._jsc.hadoopConfiguration().set("log4j.logger.org.apache.spark", "ERROR")
    spark._jsc.hadoopConfiguration().set("log4j.logger.org.spark-project", "ERROR")

    # Definición de variables
    s3_output_path = "data-lake-demo/bronze/dbOffices"
    database = "myspace1a_rc"
    target_collection = "offices"
    mongo_uri_base = "mongodb://root:oneG-TrYh4Ck*Th15@3.210.8.173:27017"
    mongo_uri = f"{mongo_uri_base}/{database}.{target_collection}?authSource=admin&readPreference=primary&directConnection=true&ssl=false"

    logger.info(f"Conectando a MongoDB con URI: {mongo_uri}")

    # Cargar datos de MongoDB
    df = spark.read.format("mongo") \
        .option("uri", mongo_uri_base) \
        .option("database", database) \
        .option("collection", target_collection) \
        .option("authSource", "admin") \
        .option("readPreference", "primary") \
        .option("directConnection", "true") \
        .option("ssl", "false") \
        .load()

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
