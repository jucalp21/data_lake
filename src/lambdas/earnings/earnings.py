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
    city = body.get('city')
    office = body.get('office')
    artisticName = body.get('artisticName')

    if not start_date or not end_date:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps('Debe proporcionar al menos start_date y end_date en el formato YYYY-MM-DD')
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

    query = f"""
    SELECT      eap.date,
                SUM(eap.payableamount) AS totalAmount
    FROM        "data_lake_db"."silver_earnings_by_performer" eap
    INNER JOIN  "data_lake_db"."bronze_users" us
        ON      (eap.emailaddress = us.streamateuser OR eap.emailaddress = us.jasminuser)
    WHERE       CAST(eap.date AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    """

    if city:
        query += f" AND us.city = '{city.replace('\'', '\'\'')}'"
    
    if office:
        query += f" AND us.office = '{office.replace('\'', '\'\'')}'"

    if artisticName:
        query += f" AND us.artisticName = '{artisticName.replace('\'', '\'\'')}'"

    query += """
        GROUP BY eap.date
        ORDER BY eap.date;
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
        while waited_time < max_wait_time:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            query_state = query_status['QueryExecution']['Status']['State']

            if query_state == 'SUCCEEDED':
                break
            elif query_state in ['FAILED', 'CANCELLED']:
                return {
                    'statusCode': 500,
                    'headers': headers,
                    'body': json.dumps(f"Query {query_state} con motivo: {query_status['QueryExecution']['Status']['StateChangeReason']}")
                }
            time.sleep(2)
            waited_time += 2

        if waited_time >= max_wait_time:
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps('Timeout al esperar que la consulta se ejecute.')
            }

        result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        rows = result['ResultSet']['Rows']
        
        output = []
        for row in rows[1:]:
            output.append({
                'date': row['Data'][0]['VarCharValue'],
                'totalAmount': row['Data'][1]['VarCharValue'],
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
