import json
import boto3
from datetime import datetime

s3_client = boto3.client('s3')
athena_client = boto3.client('athena')

BUCKET_NAME = "data-lake-demo"
PREFIX = "silver/streamate_model_performance/"
ATHENA_OUTPUT = "s3://data-lake-demo/athena-results-validation/"
DATABASE_NAME = "data_lake_db"
TABLE_NAME = "silver_streamate_model_performance"


def query_athena(query):
    """
    Ejecuta una consulta en Athena y retorna los resultados.
    Elimina los archivos generados en S3 después de usarlos.
    """
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": DATABASE_NAME},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        query_execution_id = response['QueryExecutionId']
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

        # Eliminar los resultados de Athena de S3
        try:
            athena_result_path = f"athena-results-validation/{query_execution_id}.csv"
            athena_result_path_meta = f"athena-results-validation/{query_execution_id}.csv.metadata"
            s3_client.delete_object(
                Bucket=BUCKET_NAME,
                Key=athena_result_path
            )
            s3_client.delete_object(
                Bucket=BUCKET_NAME,
                Key=athena_result_path_meta
            )
            print(f"Athena result file deleted: {athena_result_path}")
            print(f"Athena result file deleted: {athena_result_path_meta}")
        except Exception as delete_error:
            print(f"Failed to delete Athena result file: {delete_error}")

        return rows

    except Exception as e:
        print(f"Error executing query: {query}")
        raise Exception(f"Athena query error: {str(e)}")


def check_and_update(data):
    """
    Verifica si existe un registro duplicado o diferente en Athena.
    Si el nuevo valor de total_earnings es menor, guarda los detalles en otra ubicación en S3 para trazabilidad.
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
            prev_total_earnings = float(existing_row[0]['VarCharValue'])
            prev_online_seconds = float(existing_row[1]['VarCharValue'])
            file_name = existing_row[2].get('VarCharValue')

            new_total_earnings = float(data['total_earnings'])
            new_online_seconds = float(data['online_seconds'])

            # Si el registro es idéntico, no hay que actualizar
            if prev_total_earnings == new_total_earnings and prev_online_seconds == new_online_seconds:
                return True  # Registro duplicado exacto, no actualizar

            # Si el nuevo valor de total_earnings es menor que el anterior, almacenar trazabilidad
            if new_total_earnings < prev_total_earnings:
                trace_file_name = f"silver/traceability/streamate_model_performance_traceability/{_id}_{date}_trace.json"
                trace_data = {
                    "date": date,
                    "_id": _id,
                    "prev_total_earnings": prev_total_earnings,
                    "new_total_earnings": new_total_earnings,
                    "prev_online_seconds": prev_online_seconds,
                    "new_online_seconds": new_online_seconds,
                    "processed_at": datetime.now().isoformat()  # Agregar timestamp de procesamiento
                }

                # Subir el archivo de trazabilidad a una ubicación separada en S3
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=trace_file_name,
                    Body=json.dumps(trace_data),
                    ContentType="application/json"
                )
                print(f"Trazability data stored at: {trace_file_name}")

            # Si los valores son diferentes, eliminar el archivo viejo
            if file_name:  # Si file_name no está vacío
                s3_client.delete_object(Bucket=BUCKET_NAME, Key=file_name)
                print(f"Deleted old file: {file_name}")
            else:
                print(f"No file to delete for ID: {_id}, Date: {date}")

            return False  # Indica que el archivo debe reemplazarse

        return False  # Si no existe el registro, es un nuevo registro

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
        return {"statusCode": 500, "body": json.dumps({"message": f"custom error: {str(e)}"})}
