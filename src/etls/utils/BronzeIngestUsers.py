import pandas as pd
import requests
from datetime import datetime
import boto3
import io
import pyarrow.parquet as pq
import pyarrow as pa

# URL de la API
api_url = "https://devstreamatemock.omgworldwidegroup.com/api/v1/user"

# Ruta base de S3 para almacenar los datos particionados
s3_bucket = "data-lake-demo"
s3_prefix = "bronze/users/"

headers = {
    "x-signature": "c1456aed7e5060dc0426b6f04e2efd2cf22838e7792f28f01a5ba76e5188398a"
}

# Función para extraer datos desde la API


def extract_data(api_url):
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()  # Asegurar que la solicitud fue exitosa
    data = response.json()
    return pd.DataFrame(data['users'])

# Función para cargar datos existentes desde S3 en formato Parquet


def load_existing_data(s3_bucket, s3_prefix, year, month):
    s3_client = boto3.client('s3')
    s3_key = f"{s3_prefix}{year}{month}/data.parquet"
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        data = response['Body'].read()
        table = pq.read_table(io.BytesIO(data))
        return table.to_pandas()
    except s3_client.exceptions.NoSuchKey:
        return pd.DataFrame()  # Si no existe el archivo, devolver un DataFrame vacío

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

# Manejo de nulos: convertir valores nulos de la API a NaN de pandas
new_df = new_df.where(pd.notnull(new_df), None)

# Cargar la data existente desde el bucket S3
existing_df = load_existing_data(
    s3_bucket, s3_prefix, now.year, f"{now.month:02d}")

# Unir datos existentes con nuevos, reemplazando valores para usuarios existentes
if not existing_df.empty:
    # Asegurar que ambas tablas tienen las mismas columnas
    new_df = new_df.reindex(columns=existing_df.columns, fill_value=None)
    # Indexar por `_id` para facilitar reemplazos
    existing_df.set_index('_id', inplace=True)
    new_df.set_index('_id', inplace=True)

    # Actualizar registros existentes y agregar nuevos
    combined_df = new_df.combine_first(existing_df).reset_index()
else:
    combined_df = new_df  # Si no hay datos existentes, usar los nuevos directamente

# Asegurarse de que los tipos de datos sean hashables
for col in combined_df.columns:
    if combined_df[col].apply(lambda x: isinstance(x, (dict, list))).any():
        print(
            f"Warning: La columna {col} tiene valores no hashables. Convertir a cadenas.")
        combined_df[col] = combined_df[col].apply(
            lambda x: str(x) if isinstance(x, (dict, list)) else x)

# Validar si el DataFrame resultante tiene duplicados adicionales (diagnóstico)
duplicate_check = combined_df.duplicated(subset=['_id']).sum()
if duplicate_check > 0:
    print(
        f"Advertencia: Hay {duplicate_check} duplicados restantes en la columna '_id'.")
else:
    print("Duplicados eliminados exitosamente.")

# Guardar la data combinada y validada en el bucket S3 en formato Parquet
save_data_to_s3(combined_df, s3_bucket, s3_prefix,
                now.year, f"{now.month:02d}")

# Mostrar el DataFrame resultante
print(combined_df)
