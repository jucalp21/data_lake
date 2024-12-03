import boto3
import json
import time
import math
from datetime import datetime


def safe_float(value, default=0.0):
    try:
        f_value = float(value)
        if math.isnan(f_value):
            return default
        return f_value
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    return int(safe_float(value, default))


def lambda_handler(event, context):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization'
    }

    if event['httpMethod'] == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps('Preflight OK')
        }

    try:
        body = json.loads(event['body'])
    except Exception:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Error al procesar el cuerpo de la solicitud.')
        }

    start_date = body.get('start_date')
    end_date = body.get('end_date')
    locations = body.get('locations')
    page = int(body.get('page', 1))
    limit = int(body.get('limit', 10))

    if not start_date or not end_date:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Debe proporcionar start_date y end_date en el cuerpo.')
        }

    try:
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Formato de fecha inválido. Use YYYY-MM-DD.')
        }

    if start_date_obj > end_date_obj:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('start_date no puede ser posterior a end_date.')
        }

    filters_main = []
    if locations:
        for loc in locations:
            if 'officeName' in loc and loc['officeName']:
                office_filter = loc['officeName'].replace("'", "''")
                filters_main.append(f"bu.office = '{office_filter}'")
            elif 'cityName' in loc and loc['cityName']:
                city_filter = loc['cityName'].replace("'", "''")
                filters_main.append(f"bu.city = '{city_filter}'")

    filters_main_str = f" AND ({' OR '.join(filters_main)})" if filters_main else ""

    query = f"""
    WITH jasmin_data AS (
        SELECT
            bu._id,
            bu.artisticname,
            bu.city,
            bu.office,
            bu.room,
            'Jasmin' AS platform,
            SUM(CAST(sjmp.total_earnings AS DOUBLE)) AS sales,
            SUM(CAST(sjmp.online_seconds AS INTEGER)) AS time,
            bu.picture
        FROM silver_jasmin_model_performance sjmp
        INNER JOIN bronze_users bu ON sjmp._id = bu._id
        WHERE CAST(sjmp."date" AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        {filters_main_str}
        GROUP BY bu._id, bu.artisticname, bu.city, bu.office, bu.room, bu.picture
    ),
    streamate_data AS (
        SELECT
            bu._id,
            bu.artisticname,
            bu.city,
            bu.office,
            bu.room,
            'Streamate' AS platform,
            SUM(CAST(sjmp.total_earnings AS DOUBLE)) AS sales,
            SUM(CAST(sjmp.online_seconds AS INTEGER)) AS time,
            bu.picture
        FROM silver_streamate_model_performance sjmp
        INNER JOIN bronze_users bu ON sjmp._id = bu._id
        WHERE CAST(sjmp."date" AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        {filters_main_str}
        GROUP BY bu._id, bu.artisticname, bu.city, bu.office, bu.room, bu.picture
    )
    SELECT
        COALESCE(jd._id, sd._id) AS _id,
        COALESCE(jd.artisticname, sd.artisticname) AS artisticname,
        COALESCE(jd.city, sd.city) AS city,
        COALESCE(jd.office, sd.office) AS office,
        COALESCE(jd.room, sd.room) AS room,
        jd.picture AS picture,
        COALESCE(jd.sales, 0) AS jasmin_sales,
        COALESCE(jd.time, 0) AS jasmin_time,
        COALESCE(sd.sales, 0) AS streamate_sales,
        COALESCE(sd.time, 0) AS streamate_time,
        ROUND(COALESCE(jd.sales, 0) / (COALESCE(jd.sales, 0) + COALESCE(sd.sales, 0)) * 100, 2) AS jasmin_percentage,
        ROUND(COALESCE(sd.sales, 0) / (COALESCE(jd.sales, 0) + COALESCE(sd.sales, 0)) * 100, 2) AS streamate_percentage,
        (COALESCE(jd.sales, 0) + COALESCE(sd.sales, 0)) AS total_sales
    FROM jasmin_data jd
    FULL OUTER JOIN streamate_data sd ON jd._id = sd._id
    ORDER BY total_sales DESC, artisticname ASC
    """

    athena_client = boto3.client('athena')
    database = 'data_lake_pdn_og'
    output_location = 's3://data-lake-prd-og/gold/'

    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )

        query_execution_id = response['QueryExecutionId']

        max_wait_time = 60
        waited_time = 0
        sleep_time = 2

        while waited_time < max_wait_time:
            query_status = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id)
            query_state = query_status['QueryExecution']['Status']['State']

            if query_state == 'SUCCEEDED':
                break
            elif query_state in ['FAILED', 'CANCELLED']:
                return {
                    'statusCode': 500,
                    'headers': headers,
                    'body': json.dumps(f"Query {query_state} con motivo: {query_status['QueryExecution']['Status']['StateChangeReason']}"),
                }

            time.sleep(sleep_time)
            waited_time += sleep_time

        if waited_time >= max_wait_time:
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps('Timeout al esperar que la consulta se ejecute.'),
            }

        result = athena_client.get_query_results(
            QueryExecutionId=query_execution_id)
        rows = result['ResultSet']['Rows']

        if len(rows) <= 1:
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps('No se encontraron resultados para el rango de fechas especificado.'),
            }

        output = []
        for row in rows[1:]:
            data = {
                "_id": row['Data'][0].get('VarCharValue', None),
                "model": {
                    "artisticName": row['Data'][1].get('VarCharValue', None),
                    "user": row['Data'][1].get('VarCharValue', None),
                    "_id": row['Data'][1].get('VarCharValue', None),
                    "role": "model",
                    "city": row['Data'][2].get('VarCharValue', None),
                    "office": row['Data'][3].get('VarCharValue', None),
                    "picture": row['Data'][5].get('VarCharValue', None),
                },
                "jasmin": {
                    "sales": safe_float(row['Data'][6].get('VarCharValue', 0)),
                    "time": safe_int(row['Data'][7].get('VarCharValue', 0)),
                    "percentage": safe_float(row['Data'][10].get('VarCharValue', 0)),
                },
                "streamate": {
                    "sales": safe_float(row['Data'][8].get('VarCharValue', 0)),
                    "time": safe_int(row['Data'][9].get('VarCharValue', 0)),
                    "percentage": safe_float(row['Data'][11].get('VarCharValue', 0)),
                },
                "total": safe_float(row['Data'][12].get('VarCharValue', 0)),
            }
            output.append(data)

        filtered_output = [entry for entry in output if entry['jasmin']
                           ['sales'] > 0 or entry['streamate']['sales'] > 0]

        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_results = filtered_output[start_idx:end_idx]

        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                "page": page,
                "limit": limit,
                "total_results": len(filtered_output),
                "hasMore": end_idx < len(filtered_output),
                "results": paginated_results,
            }),
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps(f'Error ejecutando la consulta: {str(e)}'),
        }
