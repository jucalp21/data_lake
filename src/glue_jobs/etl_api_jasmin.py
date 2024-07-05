import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import requests
import json
from pyspark.sql import Row
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


s3_output_path = "590184094373-dl-bronze"
api_url = "https://rickandmortyapi.com/api/character"

try:
    logger.info(f"Llamando a la API: {api_url}")
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    logger.info("Datos obtenidos de la API correctamente")

    if 'results' not in data:
        raise ValueError(
            "La respuesta de la API no contiene el campo 'results'")
    results = data['results']
    logger.info(
        f"Datos extraídos de 'results': {results[:2]}... (mostrando solo los dos primeros elementos)")

    rdd = sc.parallelize(results)
    df = spark.read.json(rdd)
    logger.info("Datos convertidos a DataFrame")

    output_path = f"s3://{s3_output_path}/data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    logger.info(f"Guardando datos en S3: {output_path}")
    df.write.json(output_path)
    logger.info("Datos guardados en S3 correctamente")

except Exception as e:
    logger.error(f"Error al ejecutar el job: {e}")
    raise

job.commit()
