import sys
import requests
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, lit, to_date
from datetime import datetime

# Obtener argumentos del trabajo
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
api_url = "https://devstreamatemock.omgworldwidegroup.com/api/v1/studio/earningsdailysummary"
token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbEFkZHJlc3MiOiJ0ZXN0c3R1ZGlvMTIzNEBzdHJlYW1hdGVtb2RlbHMuY29tIiwiaWF0IjoxNzIxMjMyODE3LCJleHAiOjE3NTI3OTA0MTd9.wXwM86yl9w-PiQVCVNW-9ljGCudPBki77YSC7esmuwI"
s3_output_path = "s3://data-lake-demo/bronze/streamate/"
payload = {"token": token}

# Inicializar el contexto de Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extract: Obtener datos desde la API
response = requests.post(api_url, json=payload)
data = response.json()

# Convertir a DataFrame de Spark
sqlContext = SQLContext(spark)
df = sqlContext.read.json(sc.parallelize([json.dumps(data)]))

# Añadir columna de fecha de procesamiento
df = df.withColumn("processing_date", lit(datetime.now().strftime("%Y-%m-%d")))

# Ruta del archivo de salida
output_path = s3_output_path

# Cargar datos previos si existen
try:
    existing_df = spark.read.parquet(output_path)
    combined_df = existing_df.unionByName(
        df).dropDuplicates(['studioId', 'date'])
except Exception as e:
    # Si el archivo no existe, usar solo los nuevos datos
    combined_df = df

# Guardar datos combinados en S3 en formato Parquet, particionados por processing_date
combined_df.write.mode('overwrite').partitionBy(
    'processing_date').parquet(output_path)

# Finalizar el trabajo
job.commit()
