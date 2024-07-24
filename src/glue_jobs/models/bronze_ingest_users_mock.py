import sys
import requests
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, lit, coalesce
from datetime import datetime

# Obtener argumentos del trabajo
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
api_url = "https://devstreamatemock.omgworldwidegroup.com/api/v1/user"
s3_output_path = "s3://data-lake-demo/bronze/users/"
current_date = datetime.now().strftime("%Y-%m-%d")

# Inicializar el contexto de Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extract: Obtener datos desde la API
response = requests.get(api_url)
data = response.json()

# Convertir a DataFrame de Spark
sqlContext = SQLContext(spark)
df_new = sqlContext.read.json(sc.parallelize([json.dumps(data['users'])]))

# Añadir columna de fecha de procesamiento
df_new = df_new.withColumn("processing_date", lit(current_date))

# Ruta del archivo de salida
output_path = f'{s3_output_path}'

# Cargar datos previos si existen
try:
    existing_df = spark.read.parquet(output_path)
    # Unir los datos existentes con los nuevos
    combined_df = existing_df.alias("existing").join(
        df_new.alias("new"), col("existing._id") == col("new._id"), "outer"
    ).select(
        coalesce(col("new._id"), col("existing._id")).alias("_id"),
        coalesce(col("new.artisticName"), col(
            "existing.artisticName")).alias("artisticName"),
        coalesce(col("new.jasminUser"), col(
            "existing.jasminUser")).alias("jasminUser"),
        coalesce(col("new.office"), col("existing.office")).alias("office"),
        coalesce(col("new.room"), col("existing.room")).alias("room"),
        coalesce(col("new.streamateUser"), col(
            "existing.streamateUser")).alias("streamateUser"),
        coalesce(col("new.city"), col("existing.city")).alias("city"),
        coalesce(col("new.user"), col("existing.user")).alias("user"),
        lit(current_date).alias("processing_date")
    )
except Exception as e:
    # Si el archivo no existe, usar solo los nuevos datos
    combined_df = df_new

# Guardar datos combinados en S3 en formato Parquet, particionados por processing_date
combined_df.write.mode('overwrite').partitionBy(
    'processing_date').parquet(output_path)

# Finalizar el trabajo
job.commit()
