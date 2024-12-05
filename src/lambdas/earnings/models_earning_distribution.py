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
    user_selected = body.get('userSelected')
    sort_key = body.get('sort_key', 'DESC').upper()
    platform = body.get('platform', '').lower()

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

    if sort_key not in ['ASC', 'DESC']:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('sort_key debe ser "ASC" o "DESC".')
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

    if user_selected:
        user_selected = user_selected.replace("'", "''")
        filters_main.append(f"us._id = '{user_selected}'")

    filters_main_str = f" AND ({' OR '.join(filters_main)})" if filters_main else ""

    if platform == 'jasmin':
        table_name = 'silver_jasmin_model_performance sjmp'
        amount_column = 'sjmp.total_earnings'
    elif platform == 'streamate':
        table_name = 'silver_streamate_model_performance sjmp'
        amount_column = 'sjmp.total_earnings'
    else:
        table_name = '''(
            SELECT date, total_earnings, _id FROM silver_jasmin_model_performance
            UNION ALL
            SELECT date, total_earnings, _id FROM silver_streamate_model_performance
        ) sjmp'''
        amount_column = 'sjmp.total_earnings'

    query = f"""
        WITH ranked_artists AS (
            SELECT      us.artisticname,
                        ROUND(SUM(CAST({amount_column} AS DOUBLE)), 2) AS total_earnings,
                        ROW_NUMBER() OVER (ORDER BY SUM(CAST({amount_column} AS DOUBLE)) {sort_key}) AS ranking
            FROM        {table_name}
            INNER JOIN  "data_lake_pdn_og"."bronze_users" us ON us._id = sjmp._id
            WHERE       CAST(sjmp."date" AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
                        {filters_main_str}
            GROUP BY    us.artisticname
        )
        SELECT      CASE WHEN ranking <= 5 THEN artisticname ELSE 'General' END AS artisticname,
                    ROUND(SUM(total_earnings), 2) AS total_earnings
        FROM        ranked_artists
        GROUP BY    CASE WHEN ranking <= 5 THEN artisticname ELSE 'General' END
        ORDER BY    total_earnings {sort_key};
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
                'body': json.dumps('No se encontraron resultados para el rango de fechas especificado.')
            }

        # Calcular el total global de ganancias
        total_global = 0
        output = []
        for row in rows[1:]:
            artisticname = row['Data'][0]['VarCharValue']
            total_earnings = float(row['Data'][1]['VarCharValue'])
            output.append({
                'artisticname': artisticname,
                'total_earnings': total_earnings
            })
            total_global += total_earnings

        # Calcular el porcentaje
        for artist in output:
            artist['percentage'] = round(
                (artist['total_earnings'] / total_global) * 100, 2)

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
