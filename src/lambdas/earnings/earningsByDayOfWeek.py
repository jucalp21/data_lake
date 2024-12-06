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
    platform = body.get('platform')

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
    user_filter = ""

    if locations:
        for loc in locations:
            if 'officeName' in loc and loc['officeName']:
                office_filter = loc['officeName'].replace("'", "''")
                filters_main.append(f"office = '{office_filter}'")
            elif 'cityName' in loc and loc['cityName']:
                city_filter = loc['cityName'].replace("'", "''")
                filters_main.append(f"city = '{city_filter}'")

    if user_selected:
        user_selected_filter = user_selected.replace("'", "''")
        user_filter = f" AND us._id = '{user_selected_filter}'"

    filters_main_str = f" AND ({' OR '.join(filters_main)})" if filters_main else ""

    filter_platform_table = {
        'jasmin': 'data_lake_pdn_og.silver_jasmin_model_performance',
        'streamate': 'data_lake_pdn_og.silver_streamate_model_performance'
    }

    platform = platform.lower() if platform else None

    if platform and platform not in filter_platform_table:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps(f"Plataforma '{platform}' no reconocida.")
        }

    tables_to_query = []
    if platform:
        tables_to_query.append(filter_platform_table[platform])
    else:
        tables_to_query.extend(
            [filter_platform_table['jasmin'], filter_platform_table['streamate']])

    select_queries = []
    for tbl in tables_to_query:
        select_query = f"""
        SELECT
            eap._id,
            eap."date",
            us.city,
            us._id,
            CAST(eap.total_earnings AS DOUBLE) AS total_earnings
        FROM {tbl} eap
        INNER JOIN "data_lake_pdn_og"."bronze_users" us
            ON eap._id = us._id
        WHERE CAST(eap."date" AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            {filters_main_str}
            {user_filter}
        """
        select_queries.append(select_query.strip())

    if len(select_queries) > 1:
        union_all_query = " UNION ALL ".join(select_queries)
    else:
        union_all_query = select_queries[0]

    query = f"""
    WITH combined AS (
        {union_all_query}
    )

    SELECT  
        CASE 
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 1 THEN 'Lun'
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 2 THEN 'Mar'
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 3 THEN 'Mié'
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 4 THEN 'Jue'
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 5 THEN 'Vie'
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 6 THEN 'Sáb'
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 7 THEN 'Dom'
        END AS DOW,
        ROUND(SUM(combined.total_earnings), 2) AS TOTAL,
        ROUND(
            (SUM(combined.total_earnings) / (
                SELECT SUM(combined.total_earnings)
                FROM combined
            )) * 100, 2
        ) AS percentage 
    FROM combined 
    WHERE 1=1 {filters_main_str}
    GROUP BY day_of_week(CAST(combined."date" AS DATE))
    ORDER BY
        CASE 
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 7 THEN 1  -- Domingo
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 1 THEN 2  -- Lunes
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 2 THEN 3  -- Martes
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 3 THEN 4  -- Miércoles
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 4 THEN 5  -- Jueves
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 5 THEN 6  -- Viernes
            WHEN day_of_week(CAST(combined."date" AS DATE)) = 6 THEN 7  -- Sábado
        END ASC;
    """

    athena_client = boto3.client('athena')
    database = 'data_lake_pdn_og'
    output_location = "s3://data-lake-prd-og/gold/"

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

        output = []
        for row in rows[1:]:
            output.append({
                'day': row['Data'][0]['VarCharValue'],
                'totalAmount': row['Data'][1]['VarCharValue'],
                'percentage': row['Data'][2]['VarCharValue'],
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
