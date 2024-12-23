import boto3
import json
import time
from datetime import datetime


def deduce_time_unit(start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    delta_days = (end_date - start_date).days

    if delta_days == 0:
        return 'day'
    elif delta_days <= 7:
        return 'week'
    elif delta_days <= 15:
        return 'biweek'
    elif delta_days <= 30:
        return 'month'
    elif delta_days <= 90:
        return 'quarter'
    else:
        return 'year'


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
            'body': json.dumps('Invalid request body format.')
        }

    start_date = body.get('start_date')
    end_date = body.get('end_date')
    locations = body.get('locations')
    user_selected = body.get('userSelected')
    platform = body.get('platform')

    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Invalid date format. Use YYYY-MM-DD.')
        }

    time_unit = deduce_time_unit(start_date, end_date)

    user_filter = ""
    if user_selected:
        user_filter = f" AND us._id = '{user_selected}'"

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

    # Modificación en la lógica para tomar ambos plataformas cuando no se especifica una
    if platform == 'jasmin':
        platform_table = 'data_lake_pdn_og.silver_jasmin_model_performance'
    elif platform == 'streamate':
        platform_table = 'data_lake_pdn_og.silver_streamate_model_performance'
    else:
        platform_table = "(SELECT * FROM data_lake_pdn_og.silver_jasmin_model_performance UNION ALL SELECT * FROM data_lake_pdn_og.silver_streamate_model_performance)"

    query = f"""
    WITH current_value AS (
        SELECT 
            SUM(CAST(platform_data.total_earnings AS DECIMAL(10,2))) AS current_value
        FROM 
            "data_lake_pdn_og"."bronze_users" us
        LEFT JOIN 
            {platform_table} platform_data ON platform_data._id = us._id
        WHERE 
            CAST(platform_data.date AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            {user_filter}
            {filters_main_str}
    ),
    historical_values AS (
        SELECT 
            SUM(CAST(platform_data.total_earnings AS DECIMAL(10,2))) AS total_earnings
        FROM 
            "data_lake_pdn_og"."bronze_users" us
        LEFT JOIN 
            {platform_table} platform_data ON platform_data._id = us._id
        WHERE 
            CASE 
                WHEN '{time_unit}' = 'day' THEN CAST(platform_data.date AS DATE) BETWEEN DATE_ADD('day', -1, DATE('{start_date}')) AND DATE_ADD('day', -1, DATE('{end_date}'))
                WHEN '{time_unit}' = 'week' THEN CAST(platform_data.date AS DATE) BETWEEN DATE_ADD('week', -1, DATE('{start_date}')) AND DATE_ADD('week', -1, DATE('{end_date}'))
                WHEN '{time_unit}' = 'biweek' THEN CAST(platform_data.date AS DATE) BETWEEN DATE_ADD('day', -14, DATE('{start_date}')) AND DATE_ADD('day', -14, DATE('{end_date}'))
                WHEN '{time_unit}' = 'month' THEN CAST(platform_data.date AS DATE) BETWEEN DATE_ADD('month', -1, DATE('{start_date}')) AND DATE_ADD('month', -1, DATE('{end_date}'))
                WHEN '{time_unit}' = 'quarter' THEN CAST(platform_data.date AS DATE) BETWEEN DATE_ADD('month', -3, DATE('{start_date}')) AND DATE_ADD('month', -3, DATE('{end_date}'))
                ELSE CAST(platform_data.date AS DATE) BETWEEN DATE_ADD('year', -1, DATE('{start_date}')) AND DATE_ADD('year', -1, DATE('{end_date}'))
            END
        {user_filter}
        {filters_main_str}
        GROUP BY 
            CASE 
                WHEN '{time_unit}' = 'day' THEN EXTRACT(DAY FROM CAST(platform_data.date AS DATE))
                WHEN '{time_unit}' = 'week' THEN EXTRACT(WEEK FROM CAST(platform_data.date AS DATE))
                WHEN '{time_unit}' = 'biweek' THEN EXTRACT(WEEK FROM CAST(platform_data.date AS DATE)) / 2
                WHEN '{time_unit}' = 'month' THEN EXTRACT(MONTH FROM CAST(platform_data.date AS DATE))
                WHEN '{time_unit}' = 'quarter' THEN EXTRACT(QUARTER FROM CAST(platform_data.date AS DATE))
                ELSE EXTRACT(YEAR FROM CAST(platform_data.date AS DATE))
            END
    )
    SELECT 
        (SELECT current_value FROM current_value) AS current_value,
        AVG(total_earnings) AS target_value
    FROM 
        historical_values;
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
                    'body': json.dumps(f"Query {query_state} with reason: {query_status['QueryExecution']['Status']['StateChangeReason']}")
                }

            time.sleep(sleep_time)
            waited_time += sleep_time

        if waited_time >= max_wait_time:
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps('Query timed out.')
            }

        result = athena_client.get_query_results(
            QueryExecutionId=query_execution_id)
        rows = result['ResultSet']['Rows']

        if len(rows) <= 1:
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps('No results found for the specified filters.')
            }

        try:
            current_value = rows[1]['Data'][0].get('VarCharValue', '0')
            target_value = rows[1]['Data'][1].get('VarCharValue', '0')
        except IndexError:
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps('Unexpected query result format.')
            }

        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'current_value': current_value,
                'target_value': target_value
            })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps(f"Error executing query: {str(e)}")
        }
