import json
import boto3
from datetime import datetime

s3_client = boto3.client('s3')
athena_client = boto3.client('athena')

BUCKET_NAME = "data-lake-demo"
PREFIX = "silver/jasmin_model_performance/"
ATHENA_OUTPUT = "s3://data-lake-demo/athena-results-validation/"
DATABASE_NAME = "data_lake_db"
TABLE_NAME = "silver_jasmin_model_performance"


def query_athena(query):
    """
    Ejecuta una consulta en Athena y retorna los resultados.
    """
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": DATABASE_NAME},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        query_execution_id = response['QueryExecutionId']
        # Log del ID para rastreo
        print(f"QueryExecutionId: {query_execution_id}")

        # Esperar a que la consulta termine
        while True:
            query_status = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id)
            status = query_status['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

        if status != "SUCCEEDED":
            error_message = query_status['QueryExecution']['Status'].get(
                'StateChangeReason', 'Unknown error')
            raise Exception(f"Query failed: {error_message}")

        # Obtener los resultados
        result = athena_client.get_query_results(
            QueryExecutionId=query_execution_id)
        rows = result['ResultSet']['Rows']
        return rows
    except Exception as e:
        print(f"Error executing query: {query}")
        raise Exception(f"Athena query error: {str(e)}")


def check_and_update(data):
    """
    Verifica si existe un registro duplicado o diferente en Athena.
    Reemplaza el archivo en S3 si es necesario.
    """
    _id = data["_id"]
    date = data["date"]
    query = f"""
        SELECT total_earnings, online_seconds, file 
        FROM {TABLE_NAME} 
        WHERE _id = '{_id}' AND date = '{date}'
    """
    try:
        rows = query_athena(query)
        if len(rows) > 1:  # Ignorar encabezado
            existing_row = rows[1]['Data']
            total_earnings = existing_row[0]['VarCharValue']
            online_seconds = existing_row[1]['VarCharValue']
            file_name = existing_row[2]['VarCharValue']

            if total_earnings != data['total_earnings'] or online_seconds != data['online_seconds']:
                s3_client.delete_object(Bucket=BUCKET_NAME, Key=file_name)

                # Guardar el nuevo archivo
                return False  # Indica que el archivo debe reemplazarse

            return True  # Registro duplicado exacto, no actualizar

        return False  # Nuevo registro, guardar
    except Exception as e:
        print(f"Error checking or updating record: {str(e)}")
        raise


def lambda_handler(event, context):
    try:
        body = event.get("body")
        if not body:
            return {"statusCode": 400, "body": json.dumps({"message": "No data provided"})}

        data = json.loads(body)

        required_keys = ["total_earnings", "online_seconds", "_id", "date"]
        for key in required_keys:
            if key not in data:
                return {"statusCode": 400, "body": json.dumps({"message": f"Missing key: {key}"})}

        # Verificar duplicados o necesidad de actualización
        if check_and_update(data):
            return {"statusCode": 409, "body": json.dumps({"message": "Duplicate data detected, no update needed"})}

        # Guardar datos en S3
        file_name = f"{PREFIX}{data['_id']}_{data['date']}.json"

        # Agregar el nombre del archivo al registro
        data["file"] = file_name

        # Subir el nuevo archivo
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        return {"statusCode": 200, "body": json.dumps({"message": "Data successfully updated or saved", "file_name": file_name})}

    except Exception as e:
        print(f"Error in Lambda handler: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"message": str(e)})}
