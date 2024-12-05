import pandas as pd
import requests
from datetime import datetime
import boto3
import io
import pyarrow.parquet as pq
import pyarrow as pa

# URL de la API
api_url = "https://1astats.models1a.com/api/v1/user"

# Ruta base de S3 para almacenar los datos particionados
s3_bucket = "data-lake-prd-og"
s3_prefix = "bronze/users/"

headers = {
    "x-signature": "c1456aed7e5060dc0426b6f04e2efd2cf22838e7792f28f01a5ba76e5188398a"
}

# Función para extraer datos desde la API


def extract_data(api_url):
    response = requests.get(api_url, headers=headers)
    data = response.json()
    return pd.DataFrame(data['users'])

# Función para agregar columnas de fecha y hora


def add_datetime_columns(df, timestamp):
    df['year'] = timestamp.year
    df['month'] = f"{timestamp.month:02d}"  # Formatear el mes con dos dígitos
    df['day'] = timestamp.day
    df['hour'] = timestamp.hour
    df['minute'] = timestamp.minute
    return df

# Función para cargar datos desde S3 en formato Parquet


def load_existing_data(s3_bucket, s3_prefix, year, month):
    s3_client = boto3.client('s3')
    s3_key = f"{s3_prefix}{year}{month}/data.parquet"
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        data = response['Body'].read()
        table = pq.read_table(io.BytesIO(data))
        return table.to_pandas()
    except s3_client.exceptions.NoSuchKey:
        # Si no existe el archivo, devolver un DataFrame vacío
        return pd.DataFrame()

# Función para guardar datos en S3 en formato Parquet


def save_data_to_s3(df, s3_bucket, s3_prefix, year, month):
    s3_client = boto3.client('s3')
    s3_key = f"{s3_prefix}{year}{month}/data.parquet"
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=buffer.getvalue())


# Obtener la fecha y hora actual
now = datetime.now()

# Extraer nueva data desde la API
new_df = extract_data(api_url)

# Agregar las columnas de fecha y hora a la nueva data
new_df = add_datetime_columns(new_df, now)

# Manejo de nulos: convertir valores nulos de la API a NaN de pandas
new_df = new_df.where(pd.notnull(new_df), None)

# Cargar la data existente desde el bucket S3
existing_df = load_existing_data(
    s3_bucket, s3_prefix, now.year, f"{now.month:02d}")

# Combinar la data existente con la nueva data
combined_df = pd.concat([existing_df, new_df])

# Asegurarse de que los tipos de datos sean hashables (convertir listas o diccionarios en cadenas)
for col in combined_df.columns:
    if combined_df[col].apply(lambda x: isinstance(x, (dict, list))).any():
        print(
            f"Warning: La columna {col} tiene valores no hashables. Convertir a cadenas.")
        combined_df[col] = combined_df[col].apply(
            lambda x: str(x) if isinstance(x, (dict, list)) else x)

# Eliminar registros duplicados basados en las columnas relevantes
subset = [col for col in combined_df.columns if col not in [
    'year', 'month', 'day', 'hour', 'minute']]
combined_df = combined_df.drop_duplicates(subset=subset)

# Guardar la data combinada y validada en el bucket S3 en formato Parquet
save_data_to_s3(combined_df, s3_bucket, s3_prefix,
                now.year, f"{now.month:02d}")

# Mostrar el DataFrame resultante
print(combined_df)
