import boto3
import json
import time
from datetime import datetime


def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps('Error al procesar el cuerpo de la solicitud.')
        }

    start_date = body.get('start_date')
    end_date = body.get('end_date')

    if not start_date or not end_date:
        return {
            'statusCode': 400,
            'body': json.dumps('Debe proporcionar start_date y end_date en el formato YYYY-MM-DD')
        }

    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return {
            'statusCode': 400,
            'body': json.dumps('Formato de fecha inválido. Use YYYY-MM-DD.')
        }

    query = f"""
        SELECT  eap.date,
                SUM(eap.payableamount) AS totalAmount
        FROM    "data_lake_db"."silver_earnings_by_performer" eap
        WHERE   CAST(eap.date AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        GROUP BY eap.date
        ORDER BY eap.date;
    """

    athena_client = boto3.client('athena')
    database = 'data_lake_db'
    output_location = 's3://data-lake-demo/gold/'

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )

    query_execution_id = response['QueryExecutionId']

    while True:
        query_status = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id)
        query_state = query_status['QueryExecution']['Status']['State']

        if query_state == 'SUCCEEDED':
            break
        elif query_state in ['FAILED', 'CANCELLED']:
            return {
                'statusCode': 500,
                'body': json.dumps(f"Query {query_state} with reason: {query_status['QueryExecution']['Status']['StateChangeReason']}")
            }
        time.sleep(2)

    result = athena_client.get_query_results(
        QueryExecutionId=query_execution_id)

    rows = result['ResultSet']['Rows']
    output = []
    for row in rows[1:]:
        output.append({
            'date': row['Data'][0]['VarCharValue'],
            'totalAmount': row['Data'][1]['VarCharValue'],
        })

    return {
        'statusCode': 200,
        'body': json.dumps(output)
    }
