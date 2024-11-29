import boto3
import json
import time
import urllib3
from datetime import datetime

# Inicializar el cliente de urllib3
http = urllib3.PoolManager()


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

    authorization = body.get('authorization')
    status = body.get('status', 'enabled')

    start_date = body.get('start_date')
    end_date = body.get('end_date')
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Invalid date format. Use YYYY-MM-DD.')
        }

    if authorization is None:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Authorization token is required.')
        }

    try:
        # Usamos urllib3 para hacer la petición HTTP
        response = http.request(
            'GET',
            "https://1astats.omgworldwidegroup.com/api/v1/user",
            headers={"Authorization": authorization}
        )

        if response.status != 200:
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps(f"Error fetching external API: {response.data}")
            }

        users_data = json.loads(response.data.decode('utf-8'))
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps(f"Error fetching external API: {str(e)}")
        }

    user_ids = []
    for user in users_data.get('users', []):
        if (status == 'enabled' and user['isEnable']) or (status == 'disabled' and not user['isEnable']):
            user_ids.append(user['streamateUser'])

    if not user_ids:
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(f'No users found with status "{status}".')
        }

    # Aquí corregimos el error de sintaxis en el f-string
    user_filter = f" AND (us.streamateuser IN ({', '.join([f'\'{user_id}\'' for user_id in user_ids])}) OR us.jasminuser IN ({', '.join([f'\'{user_id}\'' for user_id in user_ids])}))"

    query = f"""
    WITH current_value AS (
        SELECT 
            SUM(eap.payableamount) AS current_value
        FROM 
            "data_lake_db"."silver_earnings_by_performer" eap
        INNER JOIN 
            "data_lake_db"."bronze_users" us 
            ON eap.emailaddress = us.streamateuser OR eap.emailaddress = us.jasminuser
        WHERE 
            CAST(eap."date" AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            {user_filter}
    ),
    historical_values AS (
        SELECT 
            SUM(eap.payableamount) AS total_earnings
        FROM 
            "data_lake_db"."silver_earnings_by_performer" eap
        INNER JOIN 
            "data_lake_db"."bronze_users" us 
            ON eap.emailaddress = us.streamateuser OR eap.emailaddress = us.jasminuser
        WHERE 
            CAST(eap."date" AS DATE) BETWEEN 
                DATE_ADD('day', -30, DATE('{start_date}')) AND DATE_ADD('day', -30, DATE('{end_date}'))
            {user_filter}
        GROUP BY 
            EXTRACT(YEAR FROM CAST(eap."date" AS DATE)), EXTRACT(MONTH FROM CAST(eap."date" AS DATE))
    )
    SELECT 
        (SELECT current_value FROM current_value) AS current_value,
        AVG(total_earnings) AS target_value
    FROM 
        historical_values;
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
