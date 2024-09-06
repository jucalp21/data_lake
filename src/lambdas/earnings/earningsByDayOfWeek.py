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

    # Consulta SQL para Athena
    query = f"""
        SELECT  CASE 
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 1 THEN 'Lun'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 2 THEN 'Mar'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 3 THEN 'Mié'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 4 THEN 'Jue'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 5 THEN 'Vie'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 6 THEN 'Sáb'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 7 THEN 'Dom'
                END AS DOW,
                ROUND(SUM(eap.payableamount), 2) AS TOTAL,
                ROUND((SUM(eap.payableamount) / (SELECT SUM(eap_inner.payableamount) 
                                                 FROM "data_lake_db"."silver_earnings_by_performer" eap_inner
                                                 INNER JOIN "data_lake_db"."bronze_users" us_inner 
                                                 ON (eap_inner.emailaddress = us_inner.streamateuser OR eap_inner.emailaddress = us_inner.jasminuser))
                ) * 100, 2) AS percentage
        FROM        "data_lake_db"."silver_earnings_by_performer" eap
        INNER JOIN  "data_lake_db"."bronze_users" us
            ON (eap.emailaddress = us.streamateuser OR eap.emailaddress = us.jasminuser)
        WHERE   CAST(eap.date AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        GROUP BY    day_of_week(CAST(eap.date AS DATE))
        ORDER BY    day_of_week(CAST(eap.date AS DATE)) ASC;
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

        # Verificar estado de la consulta
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
            time.sleep(5)

        result = athena_client.get_query_results(
            QueryExecutionId=query_execution_id)
        rows = result['ResultSet']['Rows']

        # Validar si hay resultados
        if len(rows) <= 1:
            return {
                'statusCode': 200,
                'body': json.dumps('No se encontraron resultados para el rango de fechas especificado.')
            }

        output = []
        for row in rows[1:]:
            output.append({
                'day': row['Data'][0]['VarCharValue'],
                'totalAmount': row['Data'][1]['VarCharValue'],
                'percentaje': row['Data'][2]['VarCharValue'],
            })

        return {
            'statusCode': 200,
            'body': json.dumps(output)
        }

    except Exception as e:
        # Captura y log de errores inesperados
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error al ejecutar la consulta: {str(e)}")
        }


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

    # Consulta SQL para Athena
    query = f"""
        SELECT  CASE 
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 1 THEN 'Lun'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 2 THEN 'Mar'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 3 THEN 'Mié'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 4 THEN 'Jue'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 5 THEN 'Vie'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 6 THEN 'Sáb'
                    WHEN day_of_week(CAST(eap.date AS DATE)) = 7 THEN 'Dom'
                END AS DOW,
                ROUND(SUM(eap.payableamount), 2) AS TOTAL,
                ROUND((SUM(eap.payableamount) / (SELECT SUM(eap_inner.payableamount) 
                                                 FROM "data_lake_db"."silver_earnings_by_performer" eap_inner
                                                 INNER JOIN "data_lake_db"."bronze_users" us_inner 
                                                 ON (eap_inner.emailaddress = us_inner.streamateuser OR eap_inner.emailaddress = us_inner.jasminuser))
                ) * 100, 2) AS percentage
        FROM        "data_lake_db"."silver_earnings_by_performer" eap
        INNER JOIN  "data_lake_db"."bronze_users" us
            ON (eap.emailaddress = us.streamateuser OR eap.emailaddress = us.jasminuser)
        WHERE   CAST(eap.date AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        GROUP BY    day_of_week(CAST(eap.date AS DATE))
        ORDER BY    day_of_week(CAST(eap.date AS DATE)) ASC;
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

        # Verificar estado de la consulta
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
            time.sleep(5)

        result = athena_client.get_query_results(
            QueryExecutionId=query_execution_id)
        rows = result['ResultSet']['Rows']

        # Validar si hay resultados
        if len(rows) <= 1:
            return {
                'statusCode': 200,
                'body': json.dumps('No se encontraron resultados para el rango de fechas especificado.')
            }

        output = []
        for row in rows[1:]:
            output.append({
                'day': row['Data'][0]['VarCharValue'],
                'totalAmount': row['Data'][1]['VarCharValue'],
                'percentaje': row['Data'][2]['VarCharValue'],
            })

        return {
            'statusCode': 200,
            'body': json.dumps(output)
        }

    except Exception as e:
        # Captura y log de errores inesperados
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error al ejecutar la consulta: {str(e)}")
        }
