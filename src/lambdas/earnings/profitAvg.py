import boto3
import json
import time
from datetime import datetime


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

    if not start_date or not end_date:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Debe proporcionar start_date y end_date en el formato YYYY-MM-DD')
        }

    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Formato de fecha inválido. Use YYYY-MM-DD.')
        }

    filters_main = []

    if locations:
        for loc in locations:
            if 'officeName' in loc and loc['officeName']:
                office_filter = loc['officeName'].replace("'", "''")
                filters_main.append(f"us.office = '{office_filter}'")
            elif 'cityName' in loc and loc['cityName']:
                city_filter = loc['cityName'].replace("'", "''")
                filters_main.append(f"us.city = '{city_filter}'")

    filters_main_str = f" AND ({' OR '.join(filters_main)})" if filters_main else ""

    query = f"""
    SELECT base.transmissionType AS id,
           base.transmissionType AS label,
           COALESCE(AVG(earnings.payableamount), 0) AS value,
           CASE base.transmissionType
               WHEN 'Toy' THEN '#BD0909'
               WHEN 'Privada' THEN '#EB8326'
               WHEN 'Otros' THEN '#C9370F'
               ELSE '#000000'
           END AS color
    FROM (
        SELECT 'Toy' AS transmissionType
        UNION ALL
        SELECT 'Privada' AS transmissionType
        UNION ALL
        SELECT 'Otros' AS transmissionType
    ) AS base
    LEFT JOIN (
        SELECT 'Otros' AS transmissionType, SUM(COALESCE(eap.payableamount, 0)) AS payableamount
        FROM "data_lake_db"."silver_earnings_by_performer" eap
        INNER JOIN "data_lake_db"."bronze_users" us
            ON (eap.emailaddress = us.streamateuser OR eap.emailaddress = us.jasminuser)
        WHERE CAST(eap."date" AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        {filters_main_str}
        GROUP BY us.office
    ) AS earnings ON base.transmissionType = earnings.transmissionType
    GROUP BY base.transmissionType
    ORDER BY 
        CASE 
            WHEN base.transmissionType = 'Toy' THEN 1
            WHEN base.transmissionType = 'Privada' THEN 2
            WHEN base.transmissionType = 'Otros' THEN 3
            ELSE 4
        END;
"""

    athena_client = boto3.client('athena')
    database = 'data_lake_db'
    output_location = 's3://data-lake-demo/gold/'

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
                    'body': json.dumps(f"Query {query_state} con motivo: {query_status['QueryExecution']['Status']['StateChangeReason']}")
                }

            time.sleep(sleep_time)
            waited_time += sleep_time

        if waited_time >= max_wait_time:
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps('Timeout al esperar que la consulta se ejecute.')
            }

        result = athena_client.get_query_results(
            QueryExecutionId=query_execution_id)
        rows = result['ResultSet']['Rows']

        if len(rows) <= 1:
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps([
                    {"id": "Toy", "label": "Toy", "value": 0.0, "color": "#BD0909"},
                    {"id": "Privada", "label": "Privada",
                        "value": 0.0, "color": "#EB8326"},
                    {"id": "Otros", "label": "Otros",
                        "value": 0.0, "color": "#C9370F"}
                ])
            }

        output = []
        for row in rows[1:]:
            output.append({
                'id': row['Data'][0]['VarCharValue'],
                'label': row['Data'][1]['VarCharValue'],
                'value': float(row['Data'][2]['VarCharValue']),
                'color': row['Data'][3]['VarCharValue'],
            })

        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(output)
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps(f"Error al ejecutar la consulta: {str(e)}")
        }
