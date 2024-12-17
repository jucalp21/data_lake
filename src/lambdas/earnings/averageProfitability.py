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

    if locations:
        for loc in locations:
            if 'officeName' in loc and loc['officeName']:
                office_filter = loc['officeName'].replace("'", "''")
                filters_main.append(f"bu.office = '{office_filter}'")
            elif 'cityName' in loc and loc['cityName']:
                city_filter = loc['cityName'].replace("'", "''")
                filters_main.append(f"bu.city = '{city_filter}'")

    if user_selected:
        user_selected_filter = user_selected.replace("'", "''")
        filters_main.append(f"bu._id = '{user_selected_filter}'")

    filters_main_str = f" AND ({' OR '.join(filters_main)})" if filters_main else ""

    if platform == "streamate":
        query = f"""
        SELECT base.transmissionType AS id,
               base.transmissionType AS label,
               COALESCE(AVG(COALESCE(CAST(earnings_streamate.payableamount AS DECIMAL), 0)), 0) AS total_value,
               COALESCE(AVG(COALESCE(CAST(earnings_streamate.online_seconds AS DECIMAL), 0)) / 3600, 0) AS total_hours,
               CASE base.transmissionType
                   WHEN 'Toy' THEN '#21619A'
                   WHEN 'Privada' THEN '#EB933D'
                   WHEN 'Total' THEN '#219E0D'
                   ELSE '#000000'
               END AS color
        FROM (
            SELECT 'Toy' AS transmissionType
            UNION ALL
            SELECT 'Privada' AS transmissionType
            UNION ALL
            SELECT 'Total' AS transmissionType
        ) AS base
        LEFT JOIN (
            SELECT 'Total' AS transmissionType, 
                   SUM(COALESCE(CAST(ssmp.total_earnings AS DECIMAL), 0)) AS payableamount,
                   SUM(COALESCE(CAST(ssmp.online_seconds AS DECIMAL), 0)) AS online_seconds
            FROM "data_lake_db"."silver_streamate_model_performance" ssmp
            INNER JOIN "data_lake_db"."bronze_users" bu
                ON ssmp._id = bu._id
            WHERE CAST(ssmp.date AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            {filters_main_str}
            GROUP BY bu.office
        ) AS earnings_streamate ON base.transmissionType = earnings_streamate.transmissionType
        GROUP BY base.transmissionType
        ORDER BY 
            CASE 
                WHEN base.transmissionType = 'Total' THEN 1
                WHEN base.transmissionType = 'Privada' THEN 2
                WHEN base.transmissionType = 'Toy' THEN 3
                ELSE 4
            END;
        """
    elif platform == "jasmin":
        query = f"""
        SELECT base.transmissionType AS id,
               base.transmissionType AS label,
               COALESCE(AVG(COALESCE(CAST(earnings_jasmin.payableamount AS DECIMAL), 0)), 0) AS total_value,
               COALESCE(AVG(COALESCE(CAST(earnings_jasmin.online_seconds AS DECIMAL), 0)) / 3600, 0) AS total_hours,
               CASE base.transmissionType
                   WHEN 'Toy' THEN '#21619A'
                   WHEN 'Privada' THEN '#EB933D'
                   WHEN 'Total' THEN '#219E0D'
                   ELSE '#000000'
               END AS color
        FROM (
            SELECT 'Toy' AS transmissionType
            UNION ALL
            SELECT 'Privada' AS transmissionType
            UNION ALL
            SELECT 'Total' AS transmissionType
        ) AS base
        LEFT JOIN (
            SELECT 'Total' AS transmissionType, 
                   SUM(COALESCE(CAST(sjmp.total_earnings AS DECIMAL), 0)) AS payableamount,
                   SUM(COALESCE(CAST(sjmp.online_seconds AS DECIMAL), 0)) AS online_seconds
            FROM "data_lake_db"."silver_jasmin_model_performance" sjmp
            INNER JOIN "data_lake_db"."bronze_users" bu
                ON sjmp._id = bu._id
            WHERE CAST(sjmp.date AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            {filters_main_str}
            GROUP BY bu.office
        ) AS earnings_jasmin ON base.transmissionType = earnings_jasmin.transmissionType
        GROUP BY base.transmissionType
        ORDER BY 
            CASE 
                WHEN base.transmissionType = 'Total' THEN 1
                WHEN base.transmissionType = 'Privada' THEN 2
                WHEN base.transmissionType = 'Toy' THEN 3
                ELSE 4
            END;
        """
    else:  # If platform is empty or unspecified, use both tables
        query = f"""
        SELECT base.transmissionType AS id,
               base.transmissionType AS label,
               COALESCE(AVG(COALESCE(CAST(earnings_jasmin.payableamount AS DECIMAL), 0)), 0) +
               COALESCE(AVG(COALESCE(CAST(earnings_streamate.payableamount AS DECIMAL), 0)), 0) AS total_value,
               COALESCE(AVG(COALESCE(CAST(earnings_jasmin.online_seconds AS DECIMAL), 0)) / 3600, 0) +
               COALESCE(AVG(COALESCE(CAST(earnings_streamate.online_seconds AS DECIMAL), 0)) / 3600, 0) AS total_hours,
               CASE base.transmissionType
                   WHEN 'Toy' THEN '#21619A'
                   WHEN 'Privada' THEN '#EB933D'
                   WHEN 'Total' THEN '#219E0D'
                   ELSE '#000000'
               END AS color
        FROM (
            SELECT 'Toy' AS transmissionType
            UNION ALL
            SELECT 'Privada' AS transmissionType
            UNION ALL
            SELECT 'Total' AS transmissionType
        ) AS base
        LEFT JOIN (
            SELECT 'Total' AS transmissionType, 
                   SUM(COALESCE(CAST(sjmp.total_earnings AS DECIMAL), 0)) AS payableamount,
                   SUM(COALESCE(CAST(sjmp.online_seconds AS DECIMAL), 0)) AS online_seconds
            FROM "data_lake_db"."silver_jasmin_model_performance" sjmp
            INNER JOIN "data_lake_db"."bronze_users" bu
                ON sjmp._id = bu._id
            WHERE CAST(sjmp.date AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            {filters_main_str}
            GROUP BY bu.office
        ) AS earnings_jasmin ON base.transmissionType = earnings_jasmin.transmissionType
        LEFT JOIN (
            SELECT 'Total' AS transmissionType, 
                   SUM(COALESCE(CAST(ssmp.total_earnings AS DECIMAL), 0)) AS payableamount,
                   SUM(COALESCE(CAST(ssmp.online_seconds AS DECIMAL), 0)) AS online_seconds
            FROM "data_lake_db"."silver_streamate_model_performance" ssmp
            INNER JOIN "data_lake_db"."bronze_users" bu
                ON ssmp._id = bu._id
            WHERE CAST(ssmp.date AS DATE) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            {filters_main_str}
            GROUP BY bu.office
        ) AS earnings_streamate ON base.transmissionType = earnings_streamate.transmissionType
        GROUP BY base.transmissionType
        ORDER BY 
            CASE 
                WHEN base.transmissionType = 'Total' THEN 1
                WHEN base.transmissionType = 'Privada' THEN 2
                WHEN base.transmissionType = 'Toy' THEN 3
                ELSE 4
            END;
        """

    # Athena query execution
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
                'body': json.dumps([  # Default values if no data
                    {"id": "Toy", "label": "Toy", "value": 0.0,
                        "value_per_hour": 0.0, "color": "#21619A"},
                    {"id": "Privada", "label": "Privada", "value": 0.0,
                        "value_per_hour": 0.0, "color": "#EB933D"},
                    {"id": "Total", "label": "Total", "value": 0.0,
                        "value_per_hour": 0.0, "color": "#219E0D"}
                ])
            }

        output = []
        for row in rows[1:]:
            total_value = float(row['Data'][2]['VarCharValue'])
            total_hours = float(row['Data'][3]['VarCharValue'])
            value_per_hour = total_value / total_hours if total_hours > 0 else 0

            output.append({
                'id': row['Data'][0]['VarCharValue'],
                'label': row['Data'][1]['VarCharValue'],
                'value': total_value,
                'value_per_hour': value_per_hour,  # Valor por hora
                'color': row['Data'][4]['VarCharValue'],
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
